use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::{mpsc, oneshot};
use tracing::debug;

use relos_core::{Entry, LogPos, RelosError, Result, LOG_POS_BEGIN};

use crate::loglet::Loglet;
use crate::virtual_log::LogletFactory;

// ---------------------------------------------------------------------------
// Sequencer
// ---------------------------------------------------------------------------

enum SequencerRequest {
    /// Get the next position to assign. Returns None if sealed.
    NextPos {
        reply: oneshot::Sender<Option<LogPos>>,
    },
    /// Seal the sequencer. Returns the next position (tail) at seal time.
    Seal {
        reply: oneshot::Sender<LogPos>,
    },
    /// Query the current tail without advancing it.
    CheckTail {
        reply: oneshot::Sender<LogPos>,
    },
}

struct SequencerHandle {
    tx: mpsc::Sender<SequencerRequest>,
}

impl SequencerHandle {
    fn spawn() -> Self {
        let (tx, mut rx) = mpsc::channel::<SequencerRequest>(256);
        tokio::spawn(async move {
            let mut next_pos: LogPos = LOG_POS_BEGIN;
            let mut sealed = false;

            while let Some(req) = rx.recv().await {
                match req {
                    SequencerRequest::NextPos { reply } => {
                        if sealed {
                            let _ = reply.send(None);
                        } else {
                            let pos = next_pos;
                            next_pos += 1;
                            let _ = reply.send(Some(pos));
                        }
                    }
                    SequencerRequest::Seal { reply } => {
                        sealed = true;
                        let _ = reply.send(next_pos);
                    }
                    SequencerRequest::CheckTail { reply } => {
                        let _ = reply.send(next_pos);
                    }
                }
            }
        });
        Self { tx }
    }

    async fn next_pos(&self) -> Result<Option<LogPos>> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(SequencerRequest::NextPos { reply })
            .await
            .map_err(|_| RelosError::Internal("sequencer task gone".into()))?;
        rx.await
            .map_err(|_| RelosError::Internal("sequencer reply dropped".into()))
    }

    async fn seal(&self) -> Result<LogPos> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(SequencerRequest::Seal { reply })
            .await
            .map_err(|_| RelosError::Internal("sequencer task gone".into()))?;
        rx.await
            .map_err(|_| RelosError::Internal("sequencer reply dropped".into()))
    }

    async fn check_tail(&self) -> Result<LogPos> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(SequencerRequest::CheckTail { reply })
            .await
            .map_err(|_| RelosError::Internal("sequencer task gone".into()))?;
        rx.await
            .map_err(|_| RelosError::Internal("sequencer reply dropped".into()))
    }
}

// ---------------------------------------------------------------------------
// LogServer
// ---------------------------------------------------------------------------

enum LogServerRequest {
    /// Store an entry at the given position. Returns Ok(true) if stored,
    /// Ok(false) if already occupied, Err if sealed.
    Store {
        pos: LogPos,
        entry: Entry,
        reply: oneshot::Sender<std::result::Result<bool, LogPos>>,
    },
    /// Read the entry at the given position.
    Read {
        pos: LogPos,
        reply: oneshot::Sender<ReadResult>,
    },
    /// Seal this log server. Returns the local tail (next expected position).
    Seal {
        reply: oneshot::Sender<LogPos>,
    },
    /// Trim all entries before the given position.
    Trim {
        pos: LogPos,
        reply: oneshot::Sender<()>,
    },
}

enum ReadResult {
    Entry(Entry),
    Trimmed,
    NotFound,
}

struct LogServerHandle {
    tx: mpsc::Sender<LogServerRequest>,
}

impl LogServerHandle {
    fn spawn(id: usize) -> Self {
        let (tx, mut rx) = mpsc::channel::<LogServerRequest>(256);
        tokio::spawn(async move {
            let mut entries: BTreeMap<LogPos, Entry> = BTreeMap::new();
            let mut sealed = false;
            let mut trim_pos: LogPos = LOG_POS_BEGIN;
            // Track the highest position stored + 1 for tail reporting.
            let mut tail: LogPos = LOG_POS_BEGIN;

            while let Some(req) = rx.recv().await {
                match req {
                    LogServerRequest::Store { pos, entry, reply } => {
                        if sealed {
                            let _ = reply.send(Err(tail));
                        } else if entries.contains_key(&pos) {
                            let _ = reply.send(Ok(false));
                        } else {
                            entries.insert(pos, entry);
                            if pos >= tail {
                                tail = pos + 1;
                            }
                            debug!(server = id, pos, "stored entry");
                            let _ = reply.send(Ok(true));
                        }
                    }
                    LogServerRequest::Read { pos, reply } => {
                        if pos < trim_pos {
                            let _ = reply.send(ReadResult::Trimmed);
                        } else if let Some(entry) = entries.get(&pos) {
                            let _ = reply.send(ReadResult::Entry(entry.clone()));
                        } else {
                            let _ = reply.send(ReadResult::NotFound);
                        }
                    }
                    LogServerRequest::Seal { reply } => {
                        sealed = true;
                        debug!(server = id, tail, "sealed");
                        let _ = reply.send(tail);
                    }
                    LogServerRequest::Trim { pos, reply } => {
                        if pos > trim_pos {
                            trim_pos = pos;
                            // Remove trimmed entries from storage.
                            let to_remove: Vec<LogPos> =
                                entries.range(..pos).map(|(&k, _)| k).collect();
                            for k in to_remove {
                                entries.remove(&k);
                            }
                        }
                        let _ = reply.send(());
                    }
                }
            }
        });
        Self { tx }
    }

    async fn read(&self, pos: LogPos) -> Result<ReadResult> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(LogServerRequest::Read { pos, reply })
            .await
            .map_err(|_| RelosError::Internal("log server task gone".into()))?;
        rx.await
            .map_err(|_| RelosError::Internal("log server reply dropped".into()))
    }
}

// ---------------------------------------------------------------------------
// NativeLoglet
// ---------------------------------------------------------------------------

/// A Delos-style NativeLoglet that uses a sequencer and replicated log servers
/// with majority quorum writes.
///
/// This follows the design from the Delos OSDI 2020 paper:
/// - A **sequencer** assigns monotonic positions to append requests.
/// - **Log servers** store entries at assigned positions.
/// - Appends fan out to all log servers and succeed when a majority (quorum) ack.
/// - Seal propagates to all servers; the loglet is sealed when a quorum seals.
/// - Reads try each log server until one returns the entry.
pub struct NativeLoglet {
    sequencer: SequencerHandle,
    log_servers: Vec<LogServerHandle>,
    quorum_size: usize,
    sealed: AtomicBool,
}

impl NativeLoglet {
    /// Create a new NativeLoglet with the given number of log servers.
    ///
    /// The quorum size is `num_servers / 2 + 1`.
    pub fn new(num_servers: usize) -> Self {
        assert!(num_servers > 0, "need at least one log server");
        let quorum_size = num_servers / 2 + 1;
        let sequencer = SequencerHandle::spawn();
        let log_servers = (0..num_servers)
            .map(|id| LogServerHandle::spawn(id))
            .collect();

        Self {
            sequencer,
            log_servers,
            quorum_size,
            sealed: AtomicBool::new(false),
        }
    }

    /// Create a NativeLoglet with the default configuration of 3 log servers.
    pub fn with_defaults() -> Self {
        Self::new(3)
    }

    /// Fan out a store request to all log servers concurrently.
    /// Returns the oneshot receivers for each server's response.
    async fn fan_out_store(
        &self,
        pos: LogPos,
        entry: &Entry,
    ) -> Result<Vec<oneshot::Receiver<std::result::Result<bool, LogPos>>>> {
        let mut receivers = Vec::with_capacity(self.log_servers.len());
        for server in &self.log_servers {
            let (reply, rx) = oneshot::channel();
            server
                .tx
                .send(LogServerRequest::Store {
                    pos,
                    entry: entry.clone(),
                    reply,
                })
                .await
                .map_err(|_| RelosError::Internal("log server task gone".into()))?;
            receivers.push(rx);
        }
        Ok(receivers)
    }

    /// Fan out a seal request to all log servers concurrently.
    async fn fan_out_seal(&self) -> Result<Vec<oneshot::Receiver<LogPos>>> {
        let mut receivers = Vec::with_capacity(self.log_servers.len());
        for server in &self.log_servers {
            let (reply, rx) = oneshot::channel();
            server
                .tx
                .send(LogServerRequest::Seal { reply })
                .await
                .map_err(|_| RelosError::Internal("log server task gone".into()))?;
            receivers.push(rx);
        }
        Ok(receivers)
    }

    /// Fan out a trim request to all log servers concurrently.
    async fn fan_out_trim(&self, pos: LogPos) -> Result<Vec<oneshot::Receiver<()>>> {
        let mut receivers = Vec::with_capacity(self.log_servers.len());
        for server in &self.log_servers {
            let (reply, rx) = oneshot::channel();
            server
                .tx
                .send(LogServerRequest::Trim { pos, reply })
                .await
                .map_err(|_| RelosError::Internal("log server task gone".into()))?;
            receivers.push(rx);
        }
        Ok(receivers)
    }
}

#[async_trait]
impl Loglet for NativeLoglet {
    async fn append(&self, entry: &Entry) -> Result<LogPos> {
        // Fast path: check if already sealed.
        if self.sealed.load(Ordering::Acquire) {
            let tail = self.sequencer.check_tail().await?;
            return Err(RelosError::Sealed(tail));
        }

        // Step 1: Get next position from sequencer.
        let pos = match self.sequencer.next_pos().await? {
            Some(pos) => pos,
            None => {
                self.sealed.store(true, Ordering::Release);
                let tail = self.sequencer.check_tail().await?;
                return Err(RelosError::Sealed(tail));
            }
        };

        // Step 2: Fan out store to all log servers and collect replies.
        let receivers = self.fan_out_store(pos, entry).await?;

        // Step 3: Count successful acks.
        let mut ack_count = 0;
        let mut seal_tail = None;
        for rx in receivers {
            match rx.await {
                Ok(Ok(true)) => ack_count += 1,
                Ok(Ok(false)) => {
                    // Already had entry at this position — still counts as durable.
                    ack_count += 1;
                }
                Ok(Err(tail)) => {
                    // Server is sealed.
                    seal_tail = Some(tail);
                }
                Err(_) => {
                    // Server reply dropped, skip.
                }
            }
        }

        if ack_count >= self.quorum_size {
            Ok(pos)
        } else if let Some(tail) = seal_tail {
            self.sealed.store(true, Ordering::Release);
            Err(RelosError::Sealed(tail))
        } else {
            Err(RelosError::Internal(
                "failed to reach quorum for append".into(),
            ))
        }
    }

    async fn check_tail(&self) -> Result<LogPos> {
        self.sequencer.check_tail().await
    }

    async fn read(&self, pos: LogPos) -> Result<Option<Entry>> {
        // Try each log server until one has the entry.
        for server in &self.log_servers {
            match server.read(pos).await? {
                ReadResult::Entry(entry) => return Ok(Some(entry)),
                ReadResult::Trimmed => return Err(RelosError::Trimmed(pos)),
                ReadResult::NotFound => continue,
            }
        }
        Ok(None)
    }

    async fn trim(&self, pos: LogPos) -> Result<()> {
        // Send trim to all log servers and await replies.
        let receivers = self.fan_out_trim(pos).await?;
        for rx in receivers {
            rx.await
                .map_err(|_| RelosError::Internal("log server reply dropped".into()))?;
        }
        Ok(())
    }

    async fn seal(&self) -> Result<LogPos> {
        // Step 1: Seal the sequencer to prevent new position assignments.
        let sequencer_tail = self.sequencer.seal().await?;
        debug!(sequencer_tail, "sequencer sealed");

        // Step 2: Seal all log servers and collect their tails.
        let receivers = self.fan_out_seal().await?;

        let mut sealed_count = 0;
        let mut max_tail: LogPos = LOG_POS_BEGIN;
        for rx in receivers {
            match rx.await {
                Ok(tail) => {
                    sealed_count += 1;
                    if tail > max_tail {
                        max_tail = tail;
                    }
                }
                Err(_) => {
                    // Server reply dropped, skip.
                }
            }
        }

        if sealed_count < self.quorum_size {
            return Err(RelosError::Internal(
                "failed to reach quorum for seal".into(),
            ));
        }

        // The final tail is the max of the sequencer tail and the server tails.
        // The sequencer tail should be authoritative, but we take the max for safety.
        let final_tail = sequencer_tail.max(max_tail);

        self.sealed.store(true, Ordering::Release);
        debug!(final_tail, "native loglet sealed");
        Ok(final_tail)
    }
}

// ---------------------------------------------------------------------------
// NativeLogletFactory
// ---------------------------------------------------------------------------

/// Factory that creates NativeLoglet instances with a configurable number
/// of log servers.
pub struct NativeLogletFactory {
    num_servers: usize,
}

impl NativeLogletFactory {
    pub fn new(num_servers: usize) -> Self {
        Self { num_servers }
    }
}

impl Default for NativeLogletFactory {
    fn default() -> Self {
        Self { num_servers: 3 }
    }
}

#[async_trait]
impl LogletFactory for NativeLogletFactory {
    async fn create(&self, _loglet_id: &str) -> Result<Arc<dyn Loglet>> {
        Ok(Arc::new(NativeLoglet::new(self.num_servers)))
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_append_and_read() {
        let loglet = NativeLoglet::with_defaults();
        let entry = Entry::new(b"hello".to_vec());

        let pos = loglet.append(&entry).await.unwrap();
        assert_eq!(pos, 1);

        let read_entry = loglet.read(pos).await.unwrap().unwrap();
        assert_eq!(read_entry.payload, b"hello");
    }

    #[tokio::test]
    async fn test_append_multiple() {
        let loglet = NativeLoglet::with_defaults();

        let pos1 = loglet.append(&Entry::new(b"a".to_vec())).await.unwrap();
        let pos2 = loglet.append(&Entry::new(b"b".to_vec())).await.unwrap();
        let pos3 = loglet.append(&Entry::new(b"c".to_vec())).await.unwrap();

        assert_eq!(pos1, 1);
        assert_eq!(pos2, 2);
        assert_eq!(pos3, 3);

        assert_eq!(loglet.read(1).await.unwrap().unwrap().payload, b"a");
        assert_eq!(loglet.read(2).await.unwrap().unwrap().payload, b"b");
        assert_eq!(loglet.read(3).await.unwrap().unwrap().payload, b"c");
    }

    #[tokio::test]
    async fn test_seal_blocks_appends() {
        let loglet = NativeLoglet::with_defaults();
        loglet.append(&Entry::new(b"a".to_vec())).await.unwrap();

        let seal_pos = loglet.seal().await.unwrap();
        assert_eq!(seal_pos, 2);

        let result = loglet.append(&Entry::new(b"b".to_vec())).await;
        assert!(matches!(result, Err(RelosError::Sealed(2))));
    }

    #[tokio::test]
    async fn test_seal_returns_correct_tail() {
        let loglet = NativeLoglet::with_defaults();
        loglet.append(&Entry::new(b"x".to_vec())).await.unwrap();
        loglet.append(&Entry::new(b"y".to_vec())).await.unwrap();

        let tail = loglet.seal().await.unwrap();
        assert_eq!(tail, 3);
    }

    #[tokio::test]
    async fn test_read_after_seal() {
        let loglet = NativeLoglet::with_defaults();
        loglet.append(&Entry::new(b"a".to_vec())).await.unwrap();
        loglet.seal().await.unwrap();

        // Reading should still work after sealing.
        assert_eq!(loglet.read(1).await.unwrap().unwrap().payload, b"a");
    }

    #[tokio::test]
    async fn test_quorum_write() {
        // With 3 servers and quorum of 2, entries should be readable
        // even though not all servers may respond in the same order.
        let loglet = NativeLoglet::new(3);

        let pos = loglet
            .append(&Entry::new(b"quorum".to_vec()))
            .await
            .unwrap();
        assert_eq!(pos, 1);

        // The entry should be readable (at least one server has it).
        let entry = loglet.read(pos).await.unwrap().unwrap();
        assert_eq!(entry.payload, b"quorum");
    }

    #[tokio::test]
    async fn test_trim() {
        let loglet = NativeLoglet::with_defaults();
        loglet.append(&Entry::new(b"a".to_vec())).await.unwrap();
        loglet.append(&Entry::new(b"b".to_vec())).await.unwrap();
        loglet.append(&Entry::new(b"c".to_vec())).await.unwrap();

        // Trim positions < 3 (i.e., positions 1 and 2).
        loglet.trim(3).await.unwrap();

        // Positions 1 and 2 should return Trimmed.
        assert!(matches!(
            loglet.read(1).await,
            Err(RelosError::Trimmed(1))
        ));
        assert!(matches!(
            loglet.read(2).await,
            Err(RelosError::Trimmed(2))
        ));

        // Position 3 should still be readable.
        assert_eq!(loglet.read(3).await.unwrap().unwrap().payload, b"c");
    }

    #[tokio::test]
    async fn test_concurrent_appends() {
        let loglet = Arc::new(NativeLoglet::with_defaults());
        let num_tasks = 10;
        let entries_per_task = 10;

        let mut handles = Vec::new();
        for task_id in 0..num_tasks {
            let loglet = loglet.clone();
            handles.push(tokio::spawn(async move {
                let mut positions = Vec::new();
                for i in 0..entries_per_task {
                    let payload = format!("task-{}-entry-{}", task_id, i).into_bytes();
                    let pos = loglet.append(&Entry::new(payload)).await.unwrap();
                    positions.push(pos);
                }
                positions
            }));
        }

        let mut all_positions = Vec::new();
        for handle in handles {
            let positions = handle.await.unwrap();
            all_positions.extend(positions);
        }

        // All positions should be unique.
        all_positions.sort();
        all_positions.dedup();
        assert_eq!(
            all_positions.len(),
            num_tasks * entries_per_task,
            "all positions must be unique"
        );

        // All entries should be readable.
        for &pos in &all_positions {
            let entry = loglet.read(pos).await.unwrap();
            assert!(entry.is_some(), "entry at pos {} should exist", pos);
        }
    }

    #[tokio::test]
    async fn test_check_tail() {
        let loglet = NativeLoglet::with_defaults();
        assert_eq!(loglet.check_tail().await.unwrap(), 1);

        loglet.append(&Entry::new(b"a".to_vec())).await.unwrap();
        assert_eq!(loglet.check_tail().await.unwrap(), 2);

        loglet.append(&Entry::new(b"b".to_vec())).await.unwrap();
        assert_eq!(loglet.check_tail().await.unwrap(), 3);
    }

    #[tokio::test]
    async fn test_read_beyond_tail_returns_none() {
        let loglet = NativeLoglet::with_defaults();
        assert!(loglet.read(1).await.unwrap().is_none());
        assert!(loglet.read(100).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_entry_with_headers() {
        let loglet = NativeLoglet::with_defaults();
        let entry =
            Entry::new(b"payload".to_vec()).with_header("engine1", b"header_data".to_vec());

        let pos = loglet.append(&entry).await.unwrap();
        let read_entry = loglet.read(pos).await.unwrap().unwrap();

        assert_eq!(read_entry.payload, b"payload");
        assert_eq!(
            read_entry.get_header("engine1"),
            Some(b"header_data".as_ref())
        );
    }

    #[tokio::test]
    async fn test_single_server() {
        // With 1 server, quorum is 1.
        let loglet = NativeLoglet::new(1);

        let pos = loglet
            .append(&Entry::new(b"solo".to_vec()))
            .await
            .unwrap();
        assert_eq!(pos, 1);

        let entry = loglet.read(pos).await.unwrap().unwrap();
        assert_eq!(entry.payload, b"solo");

        let tail = loglet.seal().await.unwrap();
        assert_eq!(tail, 2);
    }

    #[tokio::test]
    async fn test_five_servers() {
        // With 5 servers, quorum is 3.
        let loglet = NativeLoglet::new(5);

        loglet.append(&Entry::new(b"a".to_vec())).await.unwrap();
        loglet.append(&Entry::new(b"b".to_vec())).await.unwrap();

        assert_eq!(loglet.read(1).await.unwrap().unwrap().payload, b"a");
        assert_eq!(loglet.read(2).await.unwrap().unwrap().payload, b"b");

        let tail = loglet.seal().await.unwrap();
        assert_eq!(tail, 3);
    }

    #[tokio::test]
    async fn test_factory_creates_distinct_loglets() {
        let factory = NativeLogletFactory::default();

        let loglet_a = factory.create("loglet-a").await.unwrap();
        let loglet_b = factory.create("loglet-b").await.unwrap();

        loglet_a
            .append(&Entry::new(b"in-a".to_vec()))
            .await
            .unwrap();

        // loglet_b should be independent.
        assert_eq!(loglet_b.check_tail().await.unwrap(), 1);
        assert!(loglet_b.read(1).await.unwrap().is_none());
    }
}
