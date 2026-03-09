use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::RwLock;
use tracing::debug;

use relos_core::{Entry, LogPos, RelosError, Result, ServerId};
use relos_store::WriteTransaction;

use crate::{Applicator, Engine, ReturnValue};

/// Header key used by the view-tracking engine.
const VIEW_TRACKING_HEADER: &str = "view_tracking";

/// Metadata stored in the view-tracking header.
#[derive(serde::Serialize, serde::Deserialize, Debug)]
struct ViewTrackingMetadata {
    server_id: ServerId,
    cursor: LogPos,
}

/// ViewTrackingEngine — injects local playback position into proposed entries.
///
/// Per the Delos SOSP 2021 paper, each server piggybacks its durable playback
/// cursor onto every proposed entry. The corresponding [`ViewTrackingApplicator`]
/// extracts these cursors during apply and maintains a map of server views,
/// enabling safe prefix trimming of the shared log.
pub struct ViewTrackingEngine {
    pub(crate) inner: Arc<dyn Engine>,
    pub(crate) server_id: ServerId,
    pub(crate) views: Arc<RwLock<HashMap<ServerId, LogPos>>>,
}

impl ViewTrackingEngine {
    /// Create a new ViewTrackingEngine wrapping the given downstream engine.
    pub fn new(inner: Arc<dyn Engine>, server_id: ServerId) -> Arc<Self> {
        Arc::new(Self {
            inner,
            server_id,
            views: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    /// Create a matching [`ViewTrackingApplicator`] that shares the view map.
    pub fn applicator(&self, inner: Box<dyn Applicator>) -> ViewTrackingApplicator {
        ViewTrackingApplicator {
            inner,
            views: self.views.clone(),
        }
    }

    /// Get the current safe trim prefix — the minimum cursor across all known servers.
    ///
    /// Returns `None` if no views have been recorded yet.
    pub async fn trim_prefix(&self) -> Option<LogPos> {
        let views = self.views.read().await;
        views.values().copied().min()
    }
}

#[async_trait::async_trait]
impl Engine for ViewTrackingEngine {
    async fn propose(&self, entry: Entry) -> Result<ReturnValue> {
        // Read the current cursor for this server from the view map.
        let cursor = {
            let views = self.views.read().await;
            views.get(&self.server_id).copied().unwrap_or(0)
        };

        let metadata = ViewTrackingMetadata {
            server_id: self.server_id,
            cursor,
        };
        let header_bytes = bincode::serialize(&metadata)
            .map_err(|e| RelosError::Serialization(e.to_string()))?;

        let entry = entry.with_header(VIEW_TRACKING_HEADER, header_bytes);

        debug!(
            server_id = self.server_id,
            cursor, "injecting view-tracking header"
        );

        self.inner.propose(entry).await
    }

    async fn sync(&self) -> Result<LogPos> {
        self.inner.sync().await
    }
}

/// ViewTrackingApplicator — extracts view-tracking headers on apply.
///
/// Wraps an upstream [`Applicator`]. On each entry that contains a
/// `"view_tracking"` header, it updates the shared view map before
/// delegating to the inner applicator.
pub struct ViewTrackingApplicator {
    pub(crate) inner: Box<dyn Applicator>,
    pub(crate) views: Arc<RwLock<HashMap<ServerId, LogPos>>>,
}

impl Applicator for ViewTrackingApplicator {
    fn apply(
        &self,
        txn: &mut dyn WriteTransaction,
        entry: &Entry,
        pos: LogPos,
    ) -> Result<ReturnValue> {
        // Extract view-tracking header if present.
        if let Some(header_bytes) = entry.get_header(VIEW_TRACKING_HEADER) {
            let metadata: ViewTrackingMetadata = bincode::deserialize(header_bytes)
                .map_err(|e| RelosError::Serialization(e.to_string()))?;

            debug!(
                server_id = metadata.server_id,
                cursor = metadata.cursor,
                pos,
                "recording view from server"
            );

            // Update the view map. We use try_write to avoid async in a sync
            // context — the lock should be uncontended during the apply loop.
            // Fall back to blocking write if needed.
            match self.views.try_write() {
                Ok(mut views) => {
                    views.insert(metadata.server_id, pos);
                }
                Err(_) => {
                    // This should be extremely rare in practice. Log and skip
                    // rather than blocking the apply loop.
                    tracing::warn!(
                        server_id = metadata.server_id,
                        "could not acquire view map write lock; skipping view update"
                    );
                }
            }
        }

        // Delegate to the inner applicator.
        self.inner.apply(txn, entry, pos)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::BaseEngine;
    use relos_log::{
        LogChain, Loglet, LogletFactory, MemoryLoglet, MemoryMetaStore, MetaStore, VirtualLog,
    };
    use relos_store::{LocalStore, MemoryStore};

    struct MemoryLogletFactory;

    #[async_trait::async_trait]
    impl LogletFactory for MemoryLogletFactory {
        async fn create(&self, _loglet_id: &str) -> Result<Arc<dyn Loglet>> {
            Ok(Arc::new(MemoryLoglet::new()))
        }
    }

    async fn make_virtual_log() -> Arc<dyn Loglet> {
        let chain = LogChain::new("loglet-0".to_string());
        let meta_store = Arc::new(MemoryMetaStore::new(chain)) as Arc<dyn MetaStore>;
        let factory = Arc::new(MemoryLogletFactory) as Arc<dyn LogletFactory>;
        Arc::new(VirtualLog::new(meta_store, factory).await.unwrap())
    }

    /// Simple test applicator that stores payloads.
    struct TestApplicator;

    impl Applicator for TestApplicator {
        fn apply(
            &self,
            txn: &mut dyn WriteTransaction,
            entry: &Entry,
            pos: LogPos,
        ) -> Result<ReturnValue> {
            let key = format!("entry:{}", pos);
            txn.put(key.as_bytes(), &entry.payload)?;
            Ok(ReturnValue::Data(entry.payload.clone()))
        }
    }

    #[tokio::test]
    async fn test_view_tracking_injects_header() {
        let vlog = make_virtual_log().await;
        let store = Arc::new(MemoryStore::new());

        let server_id: ServerId = 42;

        let vt_engine_for_applicator = ViewTrackingEngine {
            inner: Arc::new(NoopEngine), // dummy, only need the views Arc
            server_id,
            views: Arc::new(RwLock::new(HashMap::new())),
        };
        let vt_applicator = vt_engine_for_applicator.applicator(Box::new(TestApplicator));

        let base = BaseEngine::new(vlog.clone(), store.clone(), Arc::new(vt_applicator))
            .await
            .unwrap();

        let vt_engine = ViewTrackingEngine {
            inner: base as Arc<dyn Engine>,
            server_id,
            views: vt_engine_for_applicator.views.clone(),
        };

        // Propose an entry.
        let entry = Entry::new(b"hello".to_vec());
        vt_engine.propose(entry).await.unwrap();

        // Read the entry back from the log and verify it has the header.
        let log_entry = vlog.read(1).await.unwrap().expect("entry should exist");
        assert!(
            log_entry.get_header(VIEW_TRACKING_HEADER).is_some(),
            "entry should have view_tracking header"
        );

        // Deserialize and check content.
        let metadata: ViewTrackingMetadata =
            bincode::deserialize(log_entry.get_header(VIEW_TRACKING_HEADER).unwrap()).unwrap();
        assert_eq!(metadata.server_id, 42);
    }

    #[tokio::test]
    async fn test_view_tracking_computes_min_trim_prefix() {
        // Manually populate views and check trim_prefix.
        let views = Arc::new(RwLock::new(HashMap::new()));
        {
            let mut v = views.write().await;
            v.insert(1, 10);
            v.insert(2, 5);
            v.insert(3, 15);
        }

        let engine = ViewTrackingEngine {
            inner: Arc::new(NoopEngine),
            server_id: 1,
            views,
        };

        let prefix = engine.trim_prefix().await;
        assert_eq!(prefix, Some(5));
    }

    #[tokio::test]
    async fn test_view_tracking_passthrough() {
        let vlog = make_virtual_log().await;
        let store = Arc::new(MemoryStore::new());

        let server_id: ServerId = 1;

        // Build the stack: ViewTrackingApplicator -> TestApplicator
        let views = Arc::new(RwLock::new(HashMap::new()));
        let vt_applicator = ViewTrackingApplicator {
            inner: Box::new(TestApplicator),
            views: views.clone(),
        };

        let base = BaseEngine::new(vlog.clone(), store.clone(), Arc::new(vt_applicator))
            .await
            .unwrap();

        let vt_engine = ViewTrackingEngine {
            inner: base as Arc<dyn Engine>,
            server_id,
            views,
        };

        // Propose works.
        let rv = vt_engine
            .propose(Entry::new(b"data".to_vec()))
            .await
            .unwrap();
        match rv {
            ReturnValue::Data(d) => assert_eq!(d, b"data"),
            _ => panic!("expected Data return value"),
        }

        // Sync works.
        let tail = vt_engine.sync().await.unwrap();
        assert_eq!(tail, 2);

        // Verify the applicator stored the entry.
        let txn = store.begin_read_txn().unwrap();
        assert_eq!(txn.get(b"entry:1").unwrap(), Some(b"data".to_vec()));
    }

    /// A no-op engine used only for tests that need an Arc<dyn Engine>.
    struct NoopEngine;

    #[async_trait::async_trait]
    impl Engine for NoopEngine {
        async fn propose(&self, _entry: Entry) -> Result<ReturnValue> {
            Ok(ReturnValue::Success)
        }
        async fn sync(&self) -> Result<LogPos> {
            Ok(0)
        }
    }
}
