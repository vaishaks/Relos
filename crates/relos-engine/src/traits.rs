use async_trait::async_trait;
use relos_core::{Entry, LogPos, Result};
use relos_store::WriteTransaction;

/// Return value from engine operations.
#[derive(Clone, Debug)]
pub enum ReturnValue {
    /// The operation succeeded with no data to return.
    Success,
    /// The operation succeeded and produced data.
    Data(Vec<u8>),
}

/// The Engine trait — propose entries and sync state.
///
/// An engine sits between the shared log and the application. It drives a
/// replicated state machine by reading entries from the log, applying them
/// to a local store, and exposing `propose` / `sync` to callers.
#[async_trait]
pub trait Engine: Send + Sync {
    /// Propose an entry to be appended to the shared log.
    /// Blocks until the entry is applied locally, then returns the result.
    async fn propose(&self, entry: Entry) -> Result<ReturnValue>;

    /// Sync: ensure all committed entries up to the current tail have been
    /// applied locally. Returns the tail position.
    async fn sync(&self) -> Result<LogPos>;
}

/// The Applicator trait — called by the engine when applying log entries.
///
/// Implementations define how each log entry mutates local state.
pub trait Applicator: Send + Sync {
    /// Apply a log entry within the given write transaction.
    /// Called by the engine's apply loop for each entry in log order.
    fn apply(
        &self,
        txn: &mut dyn WriteTransaction,
        entry: &Entry,
        pos: LogPos,
    ) -> Result<ReturnValue>;
}
