use std::sync::Arc;
use std::time::Instant;

use relos_core::{Entry, LogPos, Result};
use relos_store::WriteTransaction;

use crate::{Applicator, Engine, ReturnValue};

/// Trait for reporting engine operation latencies.
pub trait MetricsReporter: Send + Sync {
    /// Record the latency of a propose operation.
    fn record_propose_latency(&self, duration: std::time::Duration);
    /// Record the latency of a sync operation.
    fn record_sync_latency(&self, duration: std::time::Duration);
    /// Record the latency of an apply operation.
    fn record_apply_latency(&self, duration: std::time::Duration);
}

/// A simple [`MetricsReporter`] that logs latencies via `tracing`.
pub struct LoggingMetrics;

impl MetricsReporter for LoggingMetrics {
    fn record_propose_latency(&self, duration: std::time::Duration) {
        tracing::info!(duration_us = duration.as_micros(), "propose latency");
    }

    fn record_sync_latency(&self, duration: std::time::Duration) {
        tracing::info!(duration_us = duration.as_micros(), "sync latency");
    }

    fn record_apply_latency(&self, duration: std::time::Duration) {
        tracing::info!(duration_us = duration.as_micros(), "apply latency");
    }
}

/// ObserverEngine — lightweight pass-through that measures operation latencies.
///
/// Wraps a downstream [`Engine`] and records propose/sync latencies via a
/// pluggable [`MetricsReporter`].
pub struct ObserverEngine {
    inner: Arc<dyn Engine>,
    metrics: Arc<dyn MetricsReporter>,
}

impl ObserverEngine {
    /// Create a new ObserverEngine wrapping the given downstream engine.
    pub fn new(inner: Arc<dyn Engine>, metrics: Arc<dyn MetricsReporter>) -> Arc<Self> {
        Arc::new(Self { inner, metrics })
    }

    /// Create a matching [`ObserverApplicator`] that shares the metrics reporter.
    pub fn applicator(&self, inner: Box<dyn Applicator>) -> ObserverApplicator {
        ObserverApplicator {
            inner,
            metrics: self.metrics.clone(),
        }
    }
}

#[async_trait::async_trait]
impl Engine for ObserverEngine {
    async fn propose(&self, entry: Entry) -> Result<ReturnValue> {
        let start = Instant::now();
        let result = self.inner.propose(entry).await;
        self.metrics.record_propose_latency(start.elapsed());
        result
    }

    async fn sync(&self) -> Result<LogPos> {
        let start = Instant::now();
        let result = self.inner.sync().await;
        self.metrics.record_sync_latency(start.elapsed());
        result
    }
}

/// ObserverApplicator — measures apply latencies.
///
/// Wraps an upstream [`Applicator`] and records apply latencies via a
/// pluggable [`MetricsReporter`].
pub struct ObserverApplicator {
    inner: Box<dyn Applicator>,
    metrics: Arc<dyn MetricsReporter>,
}

impl Applicator for ObserverApplicator {
    fn apply(
        &self,
        txn: &mut dyn WriteTransaction,
        entry: &Entry,
        pos: LogPos,
    ) -> Result<ReturnValue> {
        let start = Instant::now();
        let result = self.inner.apply(txn, entry, pos);
        self.metrics.record_apply_latency(start.elapsed());
        result
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
    use std::sync::atomic::{AtomicU64, Ordering};

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

    /// A test metrics reporter that counts calls.
    struct CountingMetrics {
        propose_count: AtomicU64,
        sync_count: AtomicU64,
        apply_count: AtomicU64,
    }

    impl CountingMetrics {
        fn new() -> Self {
            Self {
                propose_count: AtomicU64::new(0),
                sync_count: AtomicU64::new(0),
                apply_count: AtomicU64::new(0),
            }
        }
    }

    impl MetricsReporter for CountingMetrics {
        fn record_propose_latency(&self, _duration: std::time::Duration) {
            self.propose_count.fetch_add(1, Ordering::SeqCst);
        }
        fn record_sync_latency(&self, _duration: std::time::Duration) {
            self.sync_count.fetch_add(1, Ordering::SeqCst);
        }
        fn record_apply_latency(&self, _duration: std::time::Duration) {
            self.apply_count.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[tokio::test]
    async fn test_observer_records_latencies() {
        let vlog = make_virtual_log().await;
        let store = Arc::new(MemoryStore::new());
        let metrics = Arc::new(CountingMetrics::new());

        let observer_applicator = ObserverApplicator {
            inner: Box::new(TestApplicator),
            metrics: metrics.clone(),
        };

        let base = BaseEngine::new(vlog.clone(), store.clone(), Arc::new(observer_applicator))
            .await
            .unwrap();

        let observer = ObserverEngine::new(base as Arc<dyn Engine>, metrics.clone());

        // Propose two entries.
        observer
            .propose(Entry::new(b"a".to_vec()))
            .await
            .unwrap();
        observer
            .propose(Entry::new(b"b".to_vec()))
            .await
            .unwrap();

        // Sync once.
        observer.sync().await.unwrap();

        assert_eq!(metrics.propose_count.load(Ordering::SeqCst), 2);
        assert_eq!(metrics.sync_count.load(Ordering::SeqCst), 1);
        assert_eq!(metrics.apply_count.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn test_observer_passthrough() {
        let vlog = make_virtual_log().await;
        let store = Arc::new(MemoryStore::new());
        let metrics = Arc::new(CountingMetrics::new());

        let observer_applicator = ObserverApplicator {
            inner: Box::new(TestApplicator),
            metrics: metrics.clone(),
        };

        let base = BaseEngine::new(vlog.clone(), store.clone(), Arc::new(observer_applicator))
            .await
            .unwrap();

        let observer = ObserverEngine::new(base as Arc<dyn Engine>, metrics.clone());

        // Propose works and returns correct data.
        let rv = observer
            .propose(Entry::new(b"passthrough".to_vec()))
            .await
            .unwrap();
        match rv {
            ReturnValue::Data(d) => assert_eq!(d, b"passthrough"),
            _ => panic!("expected Data return value"),
        }

        // Sync works.
        let tail = observer.sync().await.unwrap();
        assert_eq!(tail, 2);

        // Data was persisted correctly.
        let txn = store.begin_read_txn().unwrap();
        assert_eq!(
            txn.get(b"entry:1").unwrap(),
            Some(b"passthrough".to_vec())
        );
    }
}
