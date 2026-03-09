use async_trait::async_trait;
use relos_core::Result;

/// A read-only transaction / snapshot
pub trait ReadTransaction: Send {
    /// Get a value by key.
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;

    /// Scan all key-value pairs whose keys start with the given prefix,
    /// returned in sorted (lexicographic) order.
    fn prefix_scan(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>>;
}

/// A read-write transaction
pub trait WriteTransaction: ReadTransaction {
    /// Insert or update a key-value pair.
    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()>;

    /// Delete a key. No-op if the key does not exist.
    fn delete(&mut self, key: &[u8]) -> Result<()>;

    /// Commit the transaction, making all writes visible to future transactions.
    fn commit(self: Box<Self>) -> Result<()>;

    /// Rollback the transaction, discarding all writes.
    fn rollback(self: Box<Self>) -> Result<()>;
}

/// The local store abstraction.
///
/// Every replica in Delos maintains local state in a LocalStore.
/// This trait provides transactional read and write access to the store.
#[async_trait]
pub trait LocalStore: Send + Sync {
    /// Begin a read-write transaction.
    fn begin_write_txn(&self) -> Result<Box<dyn WriteTransaction>>;

    /// Begin a read-only transaction (snapshot).
    fn begin_read_txn(&self) -> Result<Box<dyn ReadTransaction>>;

    /// Flush any buffered data to durable storage.
    async fn flush(&self) -> Result<()>;
}
