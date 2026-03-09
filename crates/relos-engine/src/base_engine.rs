use std::sync::Arc;

use tokio::sync::{mpsc, oneshot};
use tracing::debug;

use relos_core::{Entry, LogPos, RelosError, Result, LOG_POS_BEGIN};
use relos_log::Loglet;
use relos_store::LocalStore;

use crate::{Applicator, Engine, ReturnValue};

/// Key used to persist the apply cursor position in the LocalStore.
const CURSOR_KEY: &[u8] = b"__cursor__";

/// Messages sent to the apply loop.
enum ApplyRequest {
    /// Propose: append then apply up to the new position.
    Propose {
        entry: Entry,
        result_tx: oneshot::Sender<Result<ReturnValue>>,
    },
    /// Sync: apply up to the current tail.
    Sync {
        result_tx: oneshot::Sender<Result<LogPos>>,
    },
}

/// The BaseEngine — bottom of the engine stack.
///
/// Drives a single-threaded apply loop that reads entries from the shared log
/// and applies them to the local store via an [`Applicator`].
pub struct BaseEngine {
    apply_tx: mpsc::Sender<ApplyRequest>,
    /// Handle to the background task so it is aborted on drop.
    _task: tokio::task::JoinHandle<()>,
}

impl BaseEngine {
    /// Create a new BaseEngine.
    ///
    /// Reads the persisted cursor from `local_store`, spawns the apply loop,
    /// and returns the engine wrapped in an `Arc`.
    pub async fn new(
        loglet: Arc<dyn Loglet>,
        local_store: Arc<dyn LocalStore>,
        applicator: Arc<dyn Applicator>,
    ) -> Result<Arc<Self>> {
        // Read the persisted cursor (next position to apply).
        let cursor = {
            let txn = local_store.begin_read_txn()?;
            match txn.get(CURSOR_KEY)? {
                Some(bytes) => {
                    let arr: [u8; 8] = bytes
                        .try_into()
                        .map_err(|_| RelosError::Store("invalid cursor bytes".into()))?;
                    u64::from_le_bytes(arr)
                }
                None => LOG_POS_BEGIN,
            }
        };

        let (apply_tx, apply_rx) = mpsc::channel::<ApplyRequest>(256);

        let task = tokio::spawn(Self::apply_loop(
            apply_rx,
            loglet,
            local_store,
            applicator,
            cursor,
        ));

        Ok(Arc::new(Self {
            apply_tx,
            _task: task,
        }))
    }

    /// The single-threaded apply loop.
    ///
    /// Sequentially processes `ApplyRequest`s. Maintains a `cursor` pointing
    /// to the next log position to apply.
    async fn apply_loop(
        mut rx: mpsc::Receiver<ApplyRequest>,
        loglet: Arc<dyn Loglet>,
        local_store: Arc<dyn LocalStore>,
        applicator: Arc<dyn Applicator>,
        mut cursor: LogPos,
    ) {
        debug!(cursor, "apply loop starting");

        while let Some(req) = rx.recv().await {
            match req {
                ApplyRequest::Propose { entry, result_tx } => {
                    let result =
                        Self::handle_propose(&loglet, &local_store, &applicator, &mut cursor, entry)
                            .await;
                    let _ = result_tx.send(result);
                }
                ApplyRequest::Sync { result_tx } => {
                    let result =
                        Self::handle_sync(&loglet, &local_store, &applicator, &mut cursor).await;
                    let _ = result_tx.send(result);
                }
            }
        }

        debug!("apply loop exiting");
    }

    /// Handle a Propose request: append to log, then play forward to the new position.
    async fn handle_propose(
        loglet: &Arc<dyn Loglet>,
        local_store: &Arc<dyn LocalStore>,
        applicator: &Arc<dyn Applicator>,
        cursor: &mut LogPos,
        entry: Entry,
    ) -> Result<ReturnValue> {
        // 1. Append to the shared log.
        let target_pos = loglet.append(&entry).await?;
        debug!(target_pos, cursor = *cursor, "proposed entry appended");

        // 2. Play forward from cursor to target_pos (inclusive).
        let mut target_return = ReturnValue::Success;
        while *cursor <= target_pos {
            let rv = Self::apply_one(loglet, local_store, applicator, *cursor).await?;
            if *cursor == target_pos {
                target_return = rv;
            }
            *cursor += 1;
        }

        Ok(target_return)
    }

    /// Handle a Sync request: play forward to the current tail.
    async fn handle_sync(
        loglet: &Arc<dyn Loglet>,
        local_store: &Arc<dyn LocalStore>,
        applicator: &Arc<dyn Applicator>,
        cursor: &mut LogPos,
    ) -> Result<LogPos> {
        let tail = loglet.check_tail().await?;
        debug!(tail, cursor = *cursor, "sync: playing forward");

        // Play forward from cursor to tail - 1 (tail is the next-to-be-written position).
        while *cursor < tail {
            Self::apply_one(loglet, local_store, applicator, *cursor).await?;
            *cursor += 1;
        }

        Ok(tail)
    }

    /// Apply a single log entry at the given position.
    ///
    /// Reads the entry, begins a write transaction, calls the applicator,
    /// persists the new cursor, and commits — all atomically.
    async fn apply_one(
        loglet: &Arc<dyn Loglet>,
        local_store: &Arc<dyn LocalStore>,
        applicator: &Arc<dyn Applicator>,
        pos: LogPos,
    ) -> Result<ReturnValue> {
        let entry = loglet.read(pos).await?.ok_or_else(|| {
            RelosError::Internal(format!("expected entry at position {pos} but got None"))
        })?;

        let mut txn = local_store.begin_write_txn()?;

        let rv = applicator.apply(&mut *txn, &entry, pos)?;

        // Persist the cursor: next position to apply is pos + 1.
        let next_cursor = pos + 1;
        txn.put(CURSOR_KEY, &next_cursor.to_le_bytes())?;

        txn.commit()?;

        debug!(pos, "applied entry");
        Ok(rv)
    }
}

#[async_trait::async_trait]
impl Engine for BaseEngine {
    async fn propose(&self, entry: Entry) -> Result<ReturnValue> {
        let (result_tx, result_rx) = oneshot::channel();
        self.apply_tx
            .send(ApplyRequest::Propose { entry, result_tx })
            .await
            .map_err(|_| RelosError::Internal("apply loop has shut down".into()))?;
        result_rx
            .await
            .map_err(|_| RelosError::Internal("apply loop dropped result channel".into()))?
    }

    async fn sync(&self) -> Result<LogPos> {
        let (result_tx, result_rx) = oneshot::channel();
        self.apply_tx
            .send(ApplyRequest::Sync { result_tx })
            .await
            .map_err(|_| RelosError::Internal("apply loop has shut down".into()))?;
        result_rx
            .await
            .map_err(|_| RelosError::Internal("apply loop dropped result channel".into()))?
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use relos_log::{LogChain, LogletFactory, Loglet, MemoryLoglet, MemoryMetaStore, MetaStore, VirtualLog};
    use relos_store::MemoryStore;

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

    /// A simple test applicator that stores each entry's payload under a key
    /// derived from its log position: `entry:<pos>`.
    struct TestApplicator;

    impl Applicator for TestApplicator {
        fn apply(
            &self,
            txn: &mut dyn relos_store::WriteTransaction,
            entry: &Entry,
            pos: LogPos,
        ) -> Result<ReturnValue> {
            let key = format!("entry:{}", pos);
            txn.put(key.as_bytes(), &entry.payload)?;
            Ok(ReturnValue::Data(entry.payload.clone()))
        }
    }

    async fn setup() -> (Arc<dyn Loglet>, Arc<MemoryStore>, Arc<BaseEngine>) {
        let vlog = make_virtual_log().await;
        let store = Arc::new(MemoryStore::new());
        let applicator: Arc<dyn Applicator> = Arc::new(TestApplicator);
        let engine = BaseEngine::new(vlog.clone(), store.clone(), applicator)
            .await
            .unwrap();
        (vlog, store, engine)
    }

    #[tokio::test]
    async fn test_propose_single_entry() {
        let (_loglet, store, engine) = setup().await;

        let entry = Entry::new(b"hello".to_vec());
        let rv = engine.propose(entry).await.unwrap();

        // Should return the payload data.
        match rv {
            ReturnValue::Data(d) => assert_eq!(d, b"hello"),
            _ => panic!("expected Data return value"),
        }

        // Verify the entry was persisted in the store.
        let txn = store.begin_read_txn().unwrap();
        assert_eq!(txn.get(b"entry:1").unwrap(), Some(b"hello".to_vec()));
    }

    #[tokio::test]
    async fn test_propose_multiple_entries() {
        let (_loglet, store, engine) = setup().await;

        for i in 0..5u8 {
            let entry = Entry::new(vec![i]);
            engine.propose(entry).await.unwrap();
        }

        let txn = store.begin_read_txn().unwrap();
        for i in 0..5u8 {
            let key = format!("entry:{}", i + 1);
            assert_eq!(txn.get(key.as_bytes()).unwrap(), Some(vec![i]));
        }
    }

    #[tokio::test]
    async fn test_sync_catches_up() {
        let (loglet, store, engine) = setup().await;

        // Append entries directly to the log (bypassing the engine).
        loglet
            .append(&Entry::new(b"external1".to_vec()))
            .await
            .unwrap();
        loglet
            .append(&Entry::new(b"external2".to_vec()))
            .await
            .unwrap();

        // Sync should apply those entries.
        let tail = engine.sync().await.unwrap();
        assert_eq!(tail, 3); // 2 entries => tail is at position 3

        // Verify both entries were applied.
        let txn = store.begin_read_txn().unwrap();
        assert_eq!(
            txn.get(b"entry:1").unwrap(),
            Some(b"external1".to_vec())
        );
        assert_eq!(
            txn.get(b"entry:2").unwrap(),
            Some(b"external2".to_vec())
        );
    }

    #[tokio::test]
    async fn test_sync_idempotent() {
        let (_loglet, _store, engine) = setup().await;

        // Sync with nothing in the log.
        let tail1 = engine.sync().await.unwrap();
        let tail2 = engine.sync().await.unwrap();
        assert_eq!(tail1, tail2);
    }

    #[tokio::test]
    async fn test_propose_then_sync() {
        let (_loglet, store, engine) = setup().await;

        engine
            .propose(Entry::new(b"first".to_vec()))
            .await
            .unwrap();
        let tail = engine.sync().await.unwrap();
        assert_eq!(tail, 2);

        let txn = store.begin_read_txn().unwrap();
        assert_eq!(txn.get(b"entry:1").unwrap(), Some(b"first".to_vec()));
    }

    #[tokio::test]
    async fn test_cursor_persisted() {
        let vlog = make_virtual_log().await;
        let store = Arc::new(MemoryStore::new());
        let applicator: Arc<dyn Applicator> = Arc::new(TestApplicator);

        // Create engine, propose an entry, then drop.
        {
            let engine = BaseEngine::new(vlog.clone(), store.clone(), applicator.clone())
                .await
                .unwrap();
            engine
                .propose(Entry::new(b"persisted".to_vec()))
                .await
                .unwrap();
        }

        // Verify cursor was persisted.
        let txn = store.begin_read_txn().unwrap();
        let cursor_bytes = txn.get(CURSOR_KEY).unwrap().expect("cursor should exist");
        let cursor = u64::from_le_bytes(cursor_bytes.try_into().unwrap());
        assert_eq!(cursor, 2); // next position after pos 1

        // Create a new engine — it should pick up the cursor and NOT re-apply.
        // Append another entry directly.
        vlog.append(&Entry::new(b"second".to_vec()))
            .await
            .unwrap();

        let engine2 = BaseEngine::new(vlog.clone(), store.clone(), applicator)
            .await
            .unwrap();
        engine2.sync().await.unwrap();

        let txn = store.begin_read_txn().unwrap();
        // entry:1 should still be "persisted" (not re-applied).
        assert_eq!(txn.get(b"entry:1").unwrap(), Some(b"persisted".to_vec()));
        // entry:2 should be "second".
        assert_eq!(txn.get(b"entry:2").unwrap(), Some(b"second".to_vec()));
    }

    #[tokio::test]
    async fn test_entries_applied_in_order() {
        let (loglet, store, engine) = setup().await;

        // Append entries directly to the log to ensure ordering.
        loglet
            .append(&Entry::new(b"a".to_vec()))
            .await
            .unwrap();
        loglet
            .append(&Entry::new(b"b".to_vec()))
            .await
            .unwrap();
        loglet
            .append(&Entry::new(b"c".to_vec()))
            .await
            .unwrap();

        engine.sync().await.unwrap();

        let txn = store.begin_read_txn().unwrap();
        assert_eq!(txn.get(b"entry:1").unwrap(), Some(b"a".to_vec()));
        assert_eq!(txn.get(b"entry:2").unwrap(), Some(b"b".to_vec()));
        assert_eq!(txn.get(b"entry:3").unwrap(), Some(b"c".to_vec()));
    }
}
