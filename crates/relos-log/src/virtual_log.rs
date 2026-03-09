use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use relos_core::{Entry, LogPos, RelosError, Result};
use tokio::sync::RwLock;
use tracing::{debug, warn};

use crate::chain::LogChain;
use crate::loglet::Loglet;
use crate::metastore::MetaStore;

/// Factory for creating loglet instances by ID.
#[async_trait]
pub trait LogletFactory: Send + Sync {
    async fn create(&self, loglet_id: &str) -> Result<Arc<dyn Loglet>>;
}

/// The VirtualLog chains loglets into a single shared log.
///
/// It uses a MetaStore to persist chain configuration and a LogletFactory
/// to instantiate loglets on demand. On seal, it performs the 3-step
/// reconfiguration protocol from the Delos paper.
pub struct VirtualLog {
    meta_store: Arc<dyn MetaStore>,
    factory: Arc<dyn LogletFactory>,
    chain: RwLock<LogChain>,
    chain_version: RwLock<u64>,
    loglets: RwLock<HashMap<String, Arc<dyn Loglet>>>,
}

impl VirtualLog {
    /// Create a new VirtualLog by reading the chain from the MetaStore and
    /// instantiating all loglets via the factory.
    pub async fn new(
        meta_store: Arc<dyn MetaStore>,
        factory: Arc<dyn LogletFactory>,
    ) -> Result<Self> {
        let (chain, version) = meta_store.read().await?;
        let mut loglets = HashMap::new();
        for segment in &chain.segments {
            let loglet = factory.create(&segment.loglet_id).await?;
            loglets.insert(segment.loglet_id.clone(), loglet);
        }

        Ok(Self {
            meta_store,
            factory,
            chain: RwLock::new(chain),
            chain_version: RwLock::new(version),
            loglets: RwLock::new(loglets),
        })
    }

    /// Append an entry to the active segment's loglet.
    /// On Sealed error, triggers reconfiguration and retries.
    pub async fn append(&self, entry: &Entry) -> Result<LogPos> {
        loop {
            let (loglet, start_pos) = self.get_active_loglet().await?;
            match loglet.append(entry).await {
                Ok(local_pos) => {
                    let global_pos = self.local_to_global(start_pos, local_pos);
                    return Ok(global_pos);
                }
                Err(RelosError::Sealed(_)) => {
                    debug!("active loglet sealed, triggering reconfiguration");
                    self.reconfigure().await?;
                    // Retry with new active loglet
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// Check the tail of the virtual log (global position).
    pub async fn check_tail(&self) -> Result<LogPos> {
        let (loglet, start_pos) = self.get_active_loglet().await?;
        let local_tail = loglet.check_tail().await?;
        Ok(self.local_to_global(start_pos, local_tail))
    }

    /// Read the entry at the given global position.
    pub async fn read(&self, global_pos: LogPos) -> Result<Option<Entry>> {
        let chain = self.chain.read().await;
        let segment = chain.find_segment(global_pos).ok_or_else(|| {
            RelosError::Internal(format!("no segment found for position {}", global_pos))
        })?;
        let local_pos = self.global_to_local(segment.start_pos, global_pos);
        let loglet_id = segment.loglet_id.clone();
        drop(chain);

        let loglets = self.loglets.read().await;
        let loglet = loglets.get(&loglet_id).ok_or_else(|| {
            RelosError::Internal(format!("loglet {} not found", loglet_id))
        })?;
        loglet.read(local_pos).await
    }

    /// Trim all entries before the given global position.
    pub async fn trim(&self, global_pos: LogPos) -> Result<()> {
        let chain = self.chain.read().await;
        let segments: Vec<_> = chain.segments.clone();
        drop(chain);

        for segment in &segments {
            let seg_end = segment.end_pos.unwrap_or(LogPos::MAX);
            if global_pos > segment.start_pos {
                let trim_global = global_pos.min(seg_end);
                let local_trim = self.global_to_local(segment.start_pos, trim_global);
                let loglets = self.loglets.read().await;
                if let Some(loglet) = loglets.get(&segment.loglet_id) {
                    loglet.trim(local_trim).await?;
                }
            }
        }
        Ok(())
    }

    /// The 3-step reconfiguration protocol:
    /// 1. Seal the active loglet to get the final tail.
    /// 2. Write a new chain to MetaStore with CAS.
    /// 3. If CAS fails, re-read chain from MetaStore and sync.
    async fn reconfigure(&self) -> Result<()> {
        // Step 1: Seal the active loglet
        let (loglet, start_pos) = self.get_active_loglet().await?;
        let local_tail = loglet.seal().await?;
        let global_seal_pos = self.local_to_global(start_pos, local_tail);
        debug!(global_seal_pos, "sealed active loglet");

        // Step 2: Build new chain and try to CAS it into MetaStore
        let version = *self.chain_version.read().await;
        let mut new_chain = self.chain.read().await.clone();
        let new_loglet_id = self.generate_loglet_id(&new_chain);
        new_chain.extend(global_seal_pos, new_loglet_id.clone());

        let success = self
            .meta_store
            .write(new_chain.clone(), version)
            .await?;

        if success {
            debug!(new_loglet_id, "reconfiguration CAS succeeded");
            // Create new loglet and update local state
            let new_loglet = self.factory.create(&new_loglet_id).await?;
            {
                let mut loglets = self.loglets.write().await;
                loglets.insert(new_loglet_id, new_loglet);
            }
            {
                let mut chain = self.chain.write().await;
                *chain = new_chain;
            }
            {
                let mut v = self.chain_version.write().await;
                *v = version + 1;
            }
        } else {
            // Step 3: CAS failed, someone else reconfigured. Re-read.
            warn!("reconfiguration CAS failed, re-reading chain");
            self.sync_chain().await?;
        }

        Ok(())
    }

    /// Re-read chain from MetaStore and instantiate any missing loglets.
    async fn sync_chain(&self) -> Result<()> {
        let (new_chain, new_version) = self.meta_store.read().await?;
        let mut loglets = self.loglets.write().await;
        for segment in &new_chain.segments {
            if !loglets.contains_key(&segment.loglet_id) {
                let loglet = self.factory.create(&segment.loglet_id).await?;
                loglets.insert(segment.loglet_id.clone(), loglet);
            }
        }
        drop(loglets);

        *self.chain.write().await = new_chain;
        *self.chain_version.write().await = new_version;
        Ok(())
    }

    /// Get the active loglet and its segment's global start position.
    async fn get_active_loglet(&self) -> Result<(Arc<dyn Loglet>, LogPos)> {
        let chain = self.chain.read().await;
        let segment = chain.active_segment().ok_or_else(|| {
            RelosError::Internal("no active segment in chain".to_string())
        })?;
        let start_pos = segment.start_pos;
        let loglet_id = segment.loglet_id.clone();
        drop(chain);

        let loglets = self.loglets.read().await;
        let loglet = loglets.get(&loglet_id).ok_or_else(|| {
            RelosError::Internal(format!("active loglet {} not found", loglet_id))
        })?;
        Ok((loglet.clone(), start_pos))
    }

    /// Convert a local loglet position to a global position.
    /// global_pos = segment.start_pos + local_pos - 1
    fn local_to_global(&self, segment_start: LogPos, local_pos: LogPos) -> LogPos {
        segment_start + local_pos - 1
    }

    /// Convert a global position to a local loglet position.
    /// local_pos = global_pos - segment.start_pos + 1
    fn global_to_local(&self, segment_start: LogPos, global_pos: LogPos) -> LogPos {
        global_pos - segment_start + 1
    }

    /// Generate a new loglet ID based on the current chain length.
    fn generate_loglet_id(&self, chain: &LogChain) -> String {
        format!("loglet-{}", chain.segments.len())
    }
}

#[async_trait]
impl Loglet for VirtualLog {
    async fn append(&self, entry: &Entry) -> Result<LogPos> {
        VirtualLog::append(self, entry).await
    }

    async fn check_tail(&self) -> Result<LogPos> {
        VirtualLog::check_tail(self).await
    }

    async fn read(&self, pos: LogPos) -> Result<Option<Entry>> {
        VirtualLog::read(self, pos).await
    }

    async fn trim(&self, pos: LogPos) -> Result<()> {
        VirtualLog::trim(self, pos).await
    }

    async fn seal(&self) -> Result<LogPos> {
        // Seal the active loglet and return the global tail
        let (loglet, start_pos) = self.get_active_loglet().await?;
        let local_tail = loglet.seal().await?;
        Ok(self.local_to_global(start_pos, local_tail))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory_loglet::MemoryLoglet;
    use crate::metastore::MemoryMetaStore;

    /// A simple factory that creates MemoryLoglets.
    struct MemoryLogletFactory;

    #[async_trait]
    impl LogletFactory for MemoryLogletFactory {
        async fn create(&self, _loglet_id: &str) -> Result<Arc<dyn Loglet>> {
            Ok(Arc::new(MemoryLoglet::new()))
        }
    }

    async fn make_virtual_log() -> VirtualLog {
        let chain = LogChain::new("loglet-0".to_string());
        let meta_store = Arc::new(MemoryMetaStore::new(chain)) as Arc<dyn MetaStore>;
        let factory = Arc::new(MemoryLogletFactory) as Arc<dyn LogletFactory>;
        VirtualLog::new(meta_store, factory).await.unwrap()
    }

    #[tokio::test]
    async fn test_append_and_read() {
        let vlog = make_virtual_log().await;

        let pos1 = vlog.append(&Entry::new(b"hello".to_vec())).await.unwrap();
        let pos2 = vlog.append(&Entry::new(b"world".to_vec())).await.unwrap();

        assert_eq!(pos1, 1);
        assert_eq!(pos2, 2);

        let e1 = vlog.read(pos1).await.unwrap().unwrap();
        assert_eq!(e1.payload, b"hello");

        let e2 = vlog.read(pos2).await.unwrap().unwrap();
        assert_eq!(e2.payload, b"world");
    }

    #[tokio::test]
    async fn test_check_tail() {
        let vlog = make_virtual_log().await;

        assert_eq!(vlog.check_tail().await.unwrap(), 1);
        vlog.append(&Entry::new(b"a".to_vec())).await.unwrap();
        assert_eq!(vlog.check_tail().await.unwrap(), 2);
    }

    #[tokio::test]
    async fn test_read_beyond_tail() {
        let vlog = make_virtual_log().await;
        let result = vlog.read(1).await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_trim() {
        let vlog = make_virtual_log().await;

        vlog.append(&Entry::new(b"a".to_vec())).await.unwrap();
        vlog.append(&Entry::new(b"b".to_vec())).await.unwrap();
        vlog.append(&Entry::new(b"c".to_vec())).await.unwrap();

        vlog.trim(3).await.unwrap();

        assert!(matches!(vlog.read(1).await, Err(RelosError::Trimmed(1))));
        assert!(matches!(vlog.read(2).await, Err(RelosError::Trimmed(2))));
        let e3 = vlog.read(3).await.unwrap().unwrap();
        assert_eq!(e3.payload, b"c");
    }

    #[tokio::test]
    async fn test_reconfigure_on_seal() {
        let chain = LogChain::new("loglet-0".to_string());
        let meta_store = Arc::new(MemoryMetaStore::new(chain)) as Arc<dyn MetaStore>;
        let factory = Arc::new(MemoryLogletFactory) as Arc<dyn LogletFactory>;
        let vlog = VirtualLog::new(meta_store.clone(), factory).await.unwrap();

        // Write some entries
        vlog.append(&Entry::new(b"a".to_vec())).await.unwrap();
        vlog.append(&Entry::new(b"b".to_vec())).await.unwrap();

        // Manually seal the active loglet to trigger reconfiguration on next append
        {
            let (loglet, _) = vlog.get_active_loglet().await.unwrap();
            loglet.seal().await.unwrap();
        }

        // Next append should trigger reconfiguration and succeed
        let pos = vlog.append(&Entry::new(b"c".to_vec())).await.unwrap();
        assert_eq!(pos, 3);

        // Should be able to read all entries across segments
        let e1 = vlog.read(1).await.unwrap().unwrap();
        assert_eq!(e1.payload, b"a");
        let e2 = vlog.read(2).await.unwrap().unwrap();
        assert_eq!(e2.payload, b"b");
        let e3 = vlog.read(3).await.unwrap().unwrap();
        assert_eq!(e3.payload, b"c");

        // Check that MetaStore was updated
        let (chain, version) = meta_store.read().await.unwrap();
        assert_eq!(version, 1);
        assert_eq!(chain.segments.len(), 2);
    }

    #[tokio::test]
    async fn test_position_mapping_across_segments() {
        let chain = LogChain::new("loglet-0".to_string());
        let meta_store = Arc::new(MemoryMetaStore::new(chain)) as Arc<dyn MetaStore>;
        let factory = Arc::new(MemoryLogletFactory) as Arc<dyn LogletFactory>;
        let vlog = VirtualLog::new(meta_store, factory).await.unwrap();

        // Write 3 entries in first segment
        for i in 0..3 {
            let pos = vlog
                .append(&Entry::new(format!("entry-{}", i).into_bytes()))
                .await
                .unwrap();
            assert_eq!(pos, (i + 1) as u64);
        }

        // Seal and reconfigure
        {
            let (loglet, _) = vlog.get_active_loglet().await.unwrap();
            loglet.seal().await.unwrap();
        }

        // Write entry in second segment — should get global pos 4
        let pos = vlog
            .append(&Entry::new(b"entry-3".to_vec()))
            .await
            .unwrap();
        assert_eq!(pos, 4);

        // Verify tail
        assert_eq!(vlog.check_tail().await.unwrap(), 5);
    }

    #[tokio::test]
    async fn test_multiple_reconfigurations() {
        let chain = LogChain::new("loglet-0".to_string());
        let meta_store = Arc::new(MemoryMetaStore::new(chain)) as Arc<dyn MetaStore>;
        let factory = Arc::new(MemoryLogletFactory) as Arc<dyn LogletFactory>;
        let vlog = VirtualLog::new(meta_store.clone(), factory).await.unwrap();

        // First segment: write 2 entries
        vlog.append(&Entry::new(b"a".to_vec())).await.unwrap();
        vlog.append(&Entry::new(b"b".to_vec())).await.unwrap();

        // Seal and reconfigure
        {
            let (loglet, _) = vlog.get_active_loglet().await.unwrap();
            loglet.seal().await.unwrap();
        }

        // Second segment: write 2 entries
        vlog.append(&Entry::new(b"c".to_vec())).await.unwrap();
        vlog.append(&Entry::new(b"d".to_vec())).await.unwrap();

        // Seal and reconfigure again
        {
            let (loglet, _) = vlog.get_active_loglet().await.unwrap();
            loglet.seal().await.unwrap();
        }

        // Third segment: write 1 entry
        let pos = vlog.append(&Entry::new(b"e".to_vec())).await.unwrap();
        assert_eq!(pos, 5);

        // Read all entries
        for (i, expected) in [b"a", b"b", b"c", b"d", b"e"].iter().enumerate() {
            let entry = vlog.read((i + 1) as u64).await.unwrap().unwrap();
            assert_eq!(&entry.payload, expected);
        }

        let (chain, version) = meta_store.read().await.unwrap();
        assert_eq!(version, 2);
        assert_eq!(chain.segments.len(), 3);
    }
}
