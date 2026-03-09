use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, oneshot};
use tracing::debug;

use relos_core::{Entry, LogPos, RelosError, Result};
use relos_store::WriteTransaction;

use crate::{Applicator, Engine, ReturnValue};

/// Default maximum number of entries to batch before flushing.
const DEFAULT_MAX_BATCH_SIZE: usize = 100;

/// Default maximum time to wait before flushing a partial batch.
const DEFAULT_BATCH_TIMEOUT: Duration = Duration::from_millis(1);

/// Header key used by the batching engine.
const BATCHING_HEADER: &str = "batching";

/// Metadata stored in the batching header.
#[derive(serde::Serialize, serde::Deserialize, Debug)]
struct BatchMetadata {
    /// Number of sub-entries in this batch.
    count: usize,
}

/// A pending propose request waiting to be batched.
struct PendingPropose {
    payload: Vec<u8>,
    result_tx: oneshot::Sender<Result<ReturnValue>>,
}

/// BatchingEngine — group commit optimization.
///
/// Accumulates multiple propose calls and flushes them as a single batched
/// entry to the downstream engine. This amortises the per-entry overhead of
/// log appends across many proposals.
pub struct BatchingEngine {
    submit_tx: mpsc::Sender<BatchRequest>,
    /// Handle to the background flush task.
    _task: tokio::task::JoinHandle<()>,
}

enum BatchRequest {
    Propose(PendingPropose),
    Sync {
        result_tx: oneshot::Sender<Result<LogPos>>,
    },
}

impl BatchingEngine {
    /// Create a new BatchingEngine wrapping a downstream engine.
    pub fn new(
        downstream: Arc<dyn Engine>,
        max_batch_size: Option<usize>,
        batch_timeout: Option<Duration>,
    ) -> Arc<Self> {
        let max_batch = max_batch_size.unwrap_or(DEFAULT_MAX_BATCH_SIZE);
        let timeout = batch_timeout.unwrap_or(DEFAULT_BATCH_TIMEOUT);

        let (submit_tx, submit_rx) = mpsc::channel::<BatchRequest>(1024);

        let downstream_clone = downstream.clone();
        let task = tokio::spawn(Self::flush_loop(
            submit_rx,
            downstream_clone,
            max_batch,
            timeout,
        ));

        Arc::new(Self {
            submit_tx,
            _task: task,
        })
    }

    /// Background task that accumulates proposals and flushes batches.
    async fn flush_loop(
        mut rx: mpsc::Receiver<BatchRequest>,
        downstream: Arc<dyn Engine>,
        max_batch_size: usize,
        batch_timeout: Duration,
    ) {
        let mut pending: Vec<PendingPropose> = Vec::new();

        loop {
            // If we have pending items, wait with a timeout for more.
            // If we have nothing, block until we get something.
            let request = if pending.is_empty() {
                match rx.recv().await {
                    Some(req) => Some(req),
                    None => break, // channel closed
                }
            } else {
                match tokio::time::timeout(batch_timeout, rx.recv()).await {
                    Ok(Some(req)) => Some(req),
                    Ok(None) => {
                        // Channel closed, flush remaining.
                        Self::flush_batch(&downstream, &mut pending).await;
                        break;
                    }
                    Err(_) => {
                        // Timeout — flush what we have.
                        None
                    }
                }
            };

            match request {
                Some(BatchRequest::Propose(p)) => {
                    pending.push(p);
                    if pending.len() >= max_batch_size {
                        Self::flush_batch(&downstream, &mut pending).await;
                    }
                }
                Some(BatchRequest::Sync { result_tx }) => {
                    // Flush any pending proposals first, then sync.
                    if !pending.is_empty() {
                        Self::flush_batch(&downstream, &mut pending).await;
                    }
                    let result = downstream.sync().await;
                    let _ = result_tx.send(result);
                }
                None => {
                    // Timeout: flush partial batch.
                    if !pending.is_empty() {
                        Self::flush_batch(&downstream, &mut pending).await;
                    }
                }
            }
        }
    }

    /// Flush accumulated proposals as a single batched entry.
    async fn flush_batch(downstream: &Arc<dyn Engine>, pending: &mut Vec<PendingPropose>) {
        if pending.is_empty() {
            return;
        }

        let batch: Vec<PendingPropose> = pending.drain(..).collect();
        let count = batch.len();

        debug!(count, "flushing batch");

        // Serialize all payloads into a single batched payload.
        let payloads: Vec<Vec<u8>> = batch.iter().map(|p| p.payload.clone()).collect();
        let batched_payload = match bincode::serialize(&payloads) {
            Ok(bytes) => bytes,
            Err(e) => {
                let err = RelosError::Serialization(e.to_string());
                for p in batch {
                    let _ = p.result_tx.send(Err(err.clone()));
                }
                return;
            }
        };

        // Create batch metadata header.
        let metadata = BatchMetadata { count };
        let header_bytes = match bincode::serialize(&metadata) {
            Ok(bytes) => bytes,
            Err(e) => {
                let err = RelosError::Serialization(e.to_string());
                for p in batch {
                    let _ = p.result_tx.send(Err(err.clone()));
                }
                return;
            }
        };

        let entry = Entry::new(batched_payload).with_header(BATCHING_HEADER, header_bytes);

        // Propose the batched entry to the downstream engine.
        match downstream.propose(entry).await {
            Ok(rv) => {
                // All sub-entries in the batch succeed together.
                for p in batch {
                    let _ = p.result_tx.send(Ok(rv.clone()));
                }
            }
            Err(e) => {
                for p in batch {
                    let _ = p.result_tx.send(Err(e.clone()));
                }
            }
        }
    }
}

#[async_trait::async_trait]
impl Engine for BatchingEngine {
    async fn propose(&self, entry: Entry) -> Result<ReturnValue> {
        let (result_tx, result_rx) = oneshot::channel();
        self.submit_tx
            .send(BatchRequest::Propose(PendingPropose {
                payload: entry.payload,
                result_tx,
            }))
            .await
            .map_err(|_| RelosError::Internal("batching flush loop has shut down".into()))?;
        result_rx
            .await
            .map_err(|_| RelosError::Internal("batching flush loop dropped result".into()))?
    }

    async fn sync(&self) -> Result<LogPos> {
        let (result_tx, result_rx) = oneshot::channel();
        self.submit_tx
            .send(BatchRequest::Sync { result_tx })
            .await
            .map_err(|_| RelosError::Internal("batching flush loop has shut down".into()))?;
        result_rx
            .await
            .map_err(|_| RelosError::Internal("batching flush loop dropped result".into()))?
    }
}

/// BatchingApplicator — unwraps batched entries and applies each sub-entry.
///
/// Wraps an upstream [`Applicator`]. When it encounters an entry with a
/// `"batching"` header, it deserializes the batch and calls the wrapped
/// applicator for each sub-entry individually. Non-batched entries are
/// passed through directly.
pub struct BatchingApplicator {
    inner: Arc<dyn Applicator>,
}

impl BatchingApplicator {
    pub fn new(inner: Arc<dyn Applicator>) -> Self {
        Self { inner }
    }
}

impl Applicator for BatchingApplicator {
    fn apply(
        &self,
        txn: &mut dyn WriteTransaction,
        entry: &Entry,
        pos: LogPos,
    ) -> Result<ReturnValue> {
        // Check if this is a batched entry.
        if let Some(header_bytes) = entry.get_header(BATCHING_HEADER) {
            let metadata: BatchMetadata = bincode::deserialize(header_bytes)
                .map_err(|e| RelosError::Serialization(e.to_string()))?;

            let payloads: Vec<Vec<u8>> = bincode::deserialize(&entry.payload)
                .map_err(|e| RelosError::Serialization(e.to_string()))?;

            if payloads.len() != metadata.count {
                return Err(RelosError::Internal(format!(
                    "batch metadata count {} != actual payload count {}",
                    metadata.count,
                    payloads.len()
                )));
            }

            let mut last_rv = ReturnValue::Success;
            for payload in &payloads {
                let sub_entry = Entry::new(payload.clone());
                last_rv = self.inner.apply(txn, &sub_entry, pos)?;
            }
            Ok(last_rv)
        } else {
            // Not a batched entry — pass through directly.
            self.inner.apply(txn, entry, pos)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::BaseEngine;
    use relos_log::{LogChain, LogletFactory, Loglet, MemoryLoglet, MemoryMetaStore, MetaStore, VirtualLog};
    use relos_store::{LocalStore, MemoryStore};
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// A simple factory that creates MemoryLoglets.
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

    /// Test applicator that counts how many times apply is called
    /// and stores payloads.
    struct CountingApplicator {
        count: AtomicUsize,
    }

    impl CountingApplicator {
        fn new() -> Self {
            Self {
                count: AtomicUsize::new(0),
            }
        }

        fn applied_count(&self) -> usize {
            self.count.load(Ordering::SeqCst)
        }
    }

    impl Applicator for CountingApplicator {
        fn apply(
            &self,
            txn: &mut dyn WriteTransaction,
            entry: &Entry,
            _pos: LogPos,
        ) -> Result<ReturnValue> {
            let n = self.count.fetch_add(1, Ordering::SeqCst);
            let key = format!("applied:{}", n);
            txn.put(key.as_bytes(), &entry.payload)?;
            Ok(ReturnValue::Data(entry.payload.clone()))
        }
    }

    #[tokio::test]
    async fn test_batching_applicator_unbatched() {
        let inner: Arc<dyn Applicator> = Arc::new(CountingApplicator::new());
        let batching_app = BatchingApplicator::new(inner.clone());

        let store = MemoryStore::new();
        let mut txn = store.begin_write_txn().unwrap();

        // A normal (non-batched) entry should pass through.
        let entry = Entry::new(b"hello".to_vec());
        let rv = batching_app.apply(&mut *txn, &entry, 1).unwrap();
        txn.commit().unwrap();

        match rv {
            ReturnValue::Data(d) => assert_eq!(d, b"hello"),
            _ => panic!("expected Data"),
        }
    }

    #[tokio::test]
    async fn test_batching_applicator_batched() {
        let counting = Arc::new(CountingApplicator::new());
        let inner: Arc<dyn Applicator> = counting.clone();
        let batching_app = BatchingApplicator::new(inner);

        let store = MemoryStore::new();
        let mut txn = store.begin_write_txn().unwrap();

        // Create a batched entry manually.
        let payloads: Vec<Vec<u8>> = vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()];
        let batched_payload = bincode::serialize(&payloads).unwrap();
        let metadata = BatchMetadata { count: 3 };
        let header_bytes = bincode::serialize(&metadata).unwrap();
        let entry = Entry::new(batched_payload).with_header(BATCHING_HEADER, header_bytes);

        batching_app.apply(&mut *txn, &entry, 1).unwrap();
        txn.commit().unwrap();

        // The inner applicator should have been called 3 times.
        assert_eq!(counting.applied_count(), 3);

        // Check that payloads were stored.
        let rtxn = store.begin_read_txn().unwrap();
        assert_eq!(rtxn.get(b"applied:0").unwrap(), Some(b"a".to_vec()));
        assert_eq!(rtxn.get(b"applied:1").unwrap(), Some(b"b".to_vec()));
        assert_eq!(rtxn.get(b"applied:2").unwrap(), Some(b"c".to_vec()));
    }

    #[tokio::test]
    async fn test_batching_engine_end_to_end() {
        let vlog = make_virtual_log().await;
        let store = Arc::new(MemoryStore::new());

        let counting = Arc::new(CountingApplicator::new());
        let batching_app = Arc::new(BatchingApplicator::new(counting.clone()));

        let base = BaseEngine::new(vlog, store.clone(), batching_app)
            .await
            .unwrap();

        // Use a large batch timeout so entries accumulate, small batch size.
        let batching = BatchingEngine::new(
            base as Arc<dyn Engine>,
            Some(3),
            Some(Duration::from_secs(10)),
        );

        // Send 3 proposals concurrently — they should be batched together.
        let engine = batching.clone();
        let handles: Vec<_> = (0..3u8)
            .map(|i| {
                let e = engine.clone();
                tokio::spawn(async move {
                    e.propose(Entry::new(vec![i])).await
                })
            })
            .collect();

        for h in handles {
            h.await.unwrap().unwrap();
        }

        // All 3 sub-entries should have been applied.
        assert_eq!(counting.applied_count(), 3);
    }

    #[tokio::test]
    async fn test_batching_engine_timeout_flush() {
        let vlog = make_virtual_log().await;
        let store = Arc::new(MemoryStore::new());

        let counting = Arc::new(CountingApplicator::new());
        let batching_app = Arc::new(BatchingApplicator::new(counting.clone()));

        let base = BaseEngine::new(vlog, store.clone(), batching_app)
            .await
            .unwrap();

        // Small timeout, large batch size — should flush on timeout.
        let batching = BatchingEngine::new(
            base as Arc<dyn Engine>,
            Some(1000),
            Some(Duration::from_millis(10)),
        );

        // Send a single entry — should be flushed after timeout.
        batching
            .propose(Entry::new(b"timeout_flush".to_vec()))
            .await
            .unwrap();

        assert_eq!(counting.applied_count(), 1);
    }

    #[tokio::test]
    async fn test_batching_engine_sync() {
        let vlog = make_virtual_log().await;
        let store = Arc::new(MemoryStore::new());

        let counting = Arc::new(CountingApplicator::new());
        let batching_app = Arc::new(BatchingApplicator::new(counting.clone()));

        let base = BaseEngine::new(vlog, store.clone(), batching_app)
            .await
            .unwrap();

        let batching = BatchingEngine::new(
            base as Arc<dyn Engine>,
            Some(100),
            Some(Duration::from_millis(1)),
        );

        batching
            .propose(Entry::new(b"before_sync".to_vec()))
            .await
            .unwrap();

        let tail = batching.sync().await.unwrap();
        assert!(tail >= 2); // At least the one entry we proposed.
    }
}
