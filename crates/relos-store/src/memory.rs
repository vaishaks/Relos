use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use relos_core::{RelosError, Result};

use crate::traits::{LocalStore, ReadTransaction, WriteTransaction};

type StoreData = BTreeMap<Vec<u8>, Vec<u8>>;

/// In-memory [`LocalStore`] backed by a `BTreeMap`.
///
/// Uses an `Arc<RwLock<BTreeMap>>` for concurrent access.
/// Write transactions clone the map, mutate the clone, and swap it back on commit.
/// Read transactions clone the map for snapshot isolation.
#[derive(Clone)]
pub struct MemoryStore {
    data: Arc<RwLock<StoreData>>,
}

impl MemoryStore {
    /// Create a new, empty in-memory store.
    pub fn new() -> Self {
        Self {
            data: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }
}

impl Default for MemoryStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl LocalStore for MemoryStore {
    fn begin_write_txn(&self) -> Result<Box<dyn WriteTransaction>> {
        let data = self
            .data
            .read()
            .map_err(|e| RelosError::Store(format!("lock poisoned: {e}")))?;
        Ok(Box::new(MemoryWriteTx {
            store: Arc::clone(&self.data),
            working: data.clone(),
            committed: false,
        }))
    }

    fn begin_read_txn(&self) -> Result<Box<dyn ReadTransaction>> {
        let data = self
            .data
            .read()
            .map_err(|e| RelosError::Store(format!("lock poisoned: {e}")))?;
        Ok(Box::new(MemoryReadTx {
            snapshot: data.clone(),
        }))
    }

    async fn flush(&self) -> Result<()> {
        // No-op for in-memory store.
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Read-only transaction
// ---------------------------------------------------------------------------

struct MemoryReadTx {
    snapshot: StoreData,
}

impl ReadTransaction for MemoryReadTx {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(self.snapshot.get(key).cloned())
    }

    fn prefix_scan(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        Ok(scan_prefix(&self.snapshot, prefix))
    }
}

// ---------------------------------------------------------------------------
// Read-write transaction
// ---------------------------------------------------------------------------

struct MemoryWriteTx {
    store: Arc<RwLock<StoreData>>,
    /// Working copy — all mutations happen here.
    working: StoreData,
    committed: bool,
}

impl ReadTransaction for MemoryWriteTx {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(self.working.get(key).cloned())
    }

    fn prefix_scan(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        Ok(scan_prefix(&self.working, prefix))
    }
}

impl WriteTransaction for MemoryWriteTx {
    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.working.insert(key.to_vec(), value.to_vec());
        Ok(())
    }

    fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.working.remove(key);
        Ok(())
    }

    fn commit(mut self: Box<Self>) -> Result<()> {
        let mut data = self
            .store
            .write()
            .map_err(|e| RelosError::Store(format!("lock poisoned: {e}")))?;
        // Swap the working copy into the store.
        std::mem::swap(&mut *data, &mut self.working);
        self.committed = true;
        Ok(())
    }

    fn rollback(mut self: Box<Self>) -> Result<()> {
        // Simply drop the working copy without writing back.
        self.working.clear();
        self.committed = true;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Scan a BTreeMap for all entries whose key starts with `prefix`.
fn scan_prefix(map: &StoreData, prefix: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
    map.range(prefix.to_vec()..)
        .take_while(|(k, _)| k.starts_with(prefix))
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_put_and_get() {
        let store = MemoryStore::new();

        let mut txn = store.begin_write_txn().unwrap();
        txn.put(b"key1", b"value1").unwrap();
        txn.put(b"key2", b"value2").unwrap();
        txn.commit().unwrap();

        let txn = store.begin_read_txn().unwrap();
        assert_eq!(txn.get(b"key1").unwrap(), Some(b"value1".to_vec()));
        assert_eq!(txn.get(b"key2").unwrap(), Some(b"value2".to_vec()));
        assert_eq!(txn.get(b"missing").unwrap(), None);
    }

    #[test]
    fn test_put_overwrites() {
        let store = MemoryStore::new();

        let mut txn = store.begin_write_txn().unwrap();
        txn.put(b"k", b"v1").unwrap();
        txn.commit().unwrap();

        let mut txn = store.begin_write_txn().unwrap();
        txn.put(b"k", b"v2").unwrap();
        txn.commit().unwrap();

        let txn = store.begin_read_txn().unwrap();
        assert_eq!(txn.get(b"k").unwrap(), Some(b"v2".to_vec()));
    }

    #[test]
    fn test_delete() {
        let store = MemoryStore::new();

        // Insert a key.
        let mut txn = store.begin_write_txn().unwrap();
        txn.put(b"key1", b"value1").unwrap();
        txn.commit().unwrap();

        // Delete it.
        let mut txn = store.begin_write_txn().unwrap();
        txn.delete(b"key1").unwrap();
        // Should be gone within the same transaction.
        assert_eq!(txn.get(b"key1").unwrap(), None);
        txn.commit().unwrap();

        // Confirm it's gone after commit.
        let txn = store.begin_read_txn().unwrap();
        assert_eq!(txn.get(b"key1").unwrap(), None);
    }

    #[test]
    fn test_delete_nonexistent_key() {
        let store = MemoryStore::new();

        // Deleting a key that was never inserted should succeed.
        let mut txn = store.begin_write_txn().unwrap();
        txn.delete(b"nope").unwrap();
        txn.commit().unwrap();
    }

    #[test]
    fn test_commit() {
        let store = MemoryStore::new();

        let mut txn = store.begin_write_txn().unwrap();
        txn.put(b"a", b"1").unwrap();
        txn.commit().unwrap();

        // Data should be visible in a new read transaction.
        let txn = store.begin_read_txn().unwrap();
        assert_eq!(txn.get(b"a").unwrap(), Some(b"1".to_vec()));
    }

    #[test]
    fn test_rollback() {
        let store = MemoryStore::new();

        let mut txn = store.begin_write_txn().unwrap();
        txn.put(b"key1", b"value1").unwrap();
        txn.rollback().unwrap();

        // Data should NOT be visible after rollback.
        let txn = store.begin_read_txn().unwrap();
        assert_eq!(txn.get(b"key1").unwrap(), None);
    }

    #[test]
    fn test_rollback_preserves_existing_data() {
        let store = MemoryStore::new();

        // Insert initial data.
        let mut txn = store.begin_write_txn().unwrap();
        txn.put(b"keep", b"me").unwrap();
        txn.commit().unwrap();

        // Start a new write txn, mutate, then rollback.
        let mut txn = store.begin_write_txn().unwrap();
        txn.put(b"new", b"data").unwrap();
        txn.delete(b"keep").unwrap();
        txn.rollback().unwrap();

        // Original data should still be intact.
        let txn = store.begin_read_txn().unwrap();
        assert_eq!(txn.get(b"keep").unwrap(), Some(b"me".to_vec()));
        assert_eq!(txn.get(b"new").unwrap(), None);
    }

    #[test]
    fn test_prefix_scan() {
        let store = MemoryStore::new();

        let mut txn = store.begin_write_txn().unwrap();
        txn.put(b"users/1", b"alice").unwrap();
        txn.put(b"users/2", b"bob").unwrap();
        txn.put(b"users/3", b"carol").unwrap();
        txn.put(b"orders/1", b"order1").unwrap();
        txn.put(b"orders/2", b"order2").unwrap();
        txn.commit().unwrap();

        let txn = store.begin_read_txn().unwrap();

        let users = txn.prefix_scan(b"users/").unwrap();
        assert_eq!(users.len(), 3);
        assert_eq!(users[0], (b"users/1".to_vec(), b"alice".to_vec()));
        assert_eq!(users[1], (b"users/2".to_vec(), b"bob".to_vec()));
        assert_eq!(users[2], (b"users/3".to_vec(), b"carol".to_vec()));

        let orders = txn.prefix_scan(b"orders/").unwrap();
        assert_eq!(orders.len(), 2);

        let empty = txn.prefix_scan(b"nonexistent/").unwrap();
        assert!(empty.is_empty());
    }

    #[test]
    fn test_prefix_scan_within_write_txn() {
        let store = MemoryStore::new();

        let mut txn = store.begin_write_txn().unwrap();
        txn.put(b"a/1", b"v1").unwrap();
        txn.put(b"a/2", b"v2").unwrap();
        txn.put(b"b/1", b"v3").unwrap();

        // Scan should see uncommitted writes within the same transaction.
        let results = txn.prefix_scan(b"a/").unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].0, b"a/1".to_vec());
        assert_eq!(results[1].0, b"a/2".to_vec());

        txn.rollback().unwrap();
    }

    #[test]
    fn test_snapshot_isolation() {
        let store = MemoryStore::new();

        // Insert initial data.
        let mut txn = store.begin_write_txn().unwrap();
        txn.put(b"key1", b"value1").unwrap();
        txn.commit().unwrap();

        // Take a read snapshot.
        let snapshot = store.begin_read_txn().unwrap();

        // Write more data after the snapshot was taken.
        let mut txn = store.begin_write_txn().unwrap();
        txn.put(b"key2", b"value2").unwrap();
        txn.put(b"key1", b"updated").unwrap();
        txn.commit().unwrap();

        // The snapshot should NOT see the new writes.
        assert_eq!(
            snapshot.get(b"key1").unwrap(),
            Some(b"value1".to_vec()),
            "snapshot should see original value"
        );
        assert_eq!(
            snapshot.get(b"key2").unwrap(),
            None,
            "snapshot should not see key2"
        );

        // A new read transaction SHOULD see the updates.
        let fresh = store.begin_read_txn().unwrap();
        assert_eq!(fresh.get(b"key1").unwrap(), Some(b"updated".to_vec()));
        assert_eq!(fresh.get(b"key2").unwrap(), Some(b"value2".to_vec()));
    }

    #[test]
    fn test_write_txn_isolation_from_concurrent_reads() {
        let store = MemoryStore::new();

        let mut txn = store.begin_write_txn().unwrap();
        txn.put(b"k", b"v").unwrap();

        // Before commit, a read txn should not see the write.
        let ro = store.begin_read_txn().unwrap();
        assert_eq!(ro.get(b"k").unwrap(), None);

        txn.commit().unwrap();

        // After commit, a NEW read txn should see it.
        let ro2 = store.begin_read_txn().unwrap();
        assert_eq!(ro2.get(b"k").unwrap(), Some(b"v".to_vec()));
    }

    #[tokio::test]
    async fn test_flush_is_noop() {
        let store = MemoryStore::new();
        // Should succeed without error.
        store.flush().await.unwrap();
    }

    #[test]
    fn test_empty_prefix_scan_returns_all() {
        let store = MemoryStore::new();

        let mut txn = store.begin_write_txn().unwrap();
        txn.put(b"a", b"1").unwrap();
        txn.put(b"b", b"2").unwrap();
        txn.put(b"c", b"3").unwrap();
        txn.commit().unwrap();

        let txn = store.begin_read_txn().unwrap();
        let all = txn.prefix_scan(b"").unwrap();
        assert_eq!(all.len(), 3);
    }

    #[test]
    fn test_multiple_commits() {
        let store = MemoryStore::new();

        let mut txn = store.begin_write_txn().unwrap();
        txn.put(b"k1", b"v1").unwrap();
        txn.commit().unwrap();

        let mut txn = store.begin_write_txn().unwrap();
        txn.put(b"k2", b"v2").unwrap();
        txn.commit().unwrap();

        let mut txn = store.begin_write_txn().unwrap();
        txn.put(b"k3", b"v3").unwrap();
        txn.commit().unwrap();

        let txn = store.begin_read_txn().unwrap();
        assert_eq!(txn.get(b"k1").unwrap(), Some(b"v1".to_vec()));
        assert_eq!(txn.get(b"k2").unwrap(), Some(b"v2".to_vec()));
        assert_eq!(txn.get(b"k3").unwrap(), Some(b"v3".to_vec()));
    }
}
