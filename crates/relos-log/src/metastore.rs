use async_trait::async_trait;
use tokio::sync::RwLock;

use relos_core::Result;

use crate::chain::LogChain;

/// MetaStore — versioned register for log chain configuration.
///
/// The MetaStore stores the current LogChain and a version number.
/// Updates use compare-and-swap on the version for consistency.
#[async_trait]
pub trait MetaStore: Send + Sync {
    /// Read current chain and its version.
    async fn read(&self) -> Result<(LogChain, u64)>;

    /// Conditionally write new chain (CAS on version). Returns true if successful.
    async fn write(&self, chain: LogChain, expected_version: u64) -> Result<bool>;
}

/// In-memory MetaStore implementation.
pub struct MemoryMetaStore {
    inner: RwLock<(LogChain, u64)>,
}

impl MemoryMetaStore {
    pub fn new(initial_chain: LogChain) -> Self {
        Self {
            inner: RwLock::new((initial_chain, 0)),
        }
    }
}

#[async_trait]
impl MetaStore for MemoryMetaStore {
    async fn read(&self) -> Result<(LogChain, u64)> {
        let inner = self.inner.read().await;
        Ok((inner.0.clone(), inner.1))
    }

    async fn write(&self, chain: LogChain, expected_version: u64) -> Result<bool> {
        let mut inner = self.inner.write().await;
        if inner.1 != expected_version {
            return Ok(false);
        }
        inner.0 = chain;
        inner.1 += 1;
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_read_initial() {
        let store = MemoryMetaStore::new(LogChain::new("loglet-0".to_string()));
        let (chain, version) = store.read().await.unwrap();
        assert_eq!(version, 0);
        assert_eq!(chain.segments.len(), 1);
        assert_eq!(chain.segments[0].loglet_id, "loglet-0");
    }

    #[tokio::test]
    async fn test_write_success() {
        let store = MemoryMetaStore::new(LogChain::new("loglet-0".to_string()));

        let mut new_chain = LogChain::new("loglet-0".to_string());
        new_chain.extend(5, "loglet-1".to_string());

        let success = store.write(new_chain, 0).await.unwrap();
        assert!(success);

        let (chain, version) = store.read().await.unwrap();
        assert_eq!(version, 1);
        assert_eq!(chain.segments.len(), 2);
    }

    #[tokio::test]
    async fn test_write_cas_failure() {
        let store = MemoryMetaStore::new(LogChain::new("loglet-0".to_string()));

        // First write succeeds
        let mut chain1 = LogChain::new("loglet-0".to_string());
        chain1.extend(5, "loglet-1".to_string());
        assert!(store.write(chain1, 0).await.unwrap());

        // Second write with stale version fails
        let mut chain2 = LogChain::new("loglet-0".to_string());
        chain2.extend(3, "loglet-2".to_string());
        let success = store.write(chain2, 0).await.unwrap();
        assert!(!success);

        // Version should still be 1
        let (_, version) = store.read().await.unwrap();
        assert_eq!(version, 1);
    }

    #[tokio::test]
    async fn test_sequential_writes() {
        let store = MemoryMetaStore::new(LogChain::new("loglet-0".to_string()));

        let mut chain = LogChain::new("loglet-0".to_string());
        chain.extend(5, "loglet-1".to_string());
        assert!(store.write(chain.clone(), 0).await.unwrap());

        chain.extend(10, "loglet-2".to_string());
        assert!(store.write(chain, 1).await.unwrap());

        let (final_chain, version) = store.read().await.unwrap();
        assert_eq!(version, 2);
        assert_eq!(final_chain.segments.len(), 3);
    }
}
