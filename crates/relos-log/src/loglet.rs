use async_trait::async_trait;
use relos_core::{Entry, LogPos, Result};

/// The Loglet trait — minimal shared log interface.
///
/// A loglet is a pluggable log segment implementation. Each loglet maintains
/// its own position space starting at LOG_POS_BEGIN (1).
#[async_trait]
pub trait Loglet: Send + Sync {
    /// Append an entry. Returns the assigned position.
    /// Returns Err(Sealed) if the loglet has been sealed.
    async fn append(&self, entry: &Entry) -> Result<LogPos>;

    /// Returns the next position that would be assigned (tail = last_written + 1).
    async fn check_tail(&self) -> Result<LogPos>;

    /// Read the entry at exactly `pos`. Returns None if no entry at that position yet.
    async fn read(&self, pos: LogPos) -> Result<Option<Entry>>;

    /// Trim all entries before `pos` (exclusive). Trimmed positions return Err(Trimmed).
    async fn trim(&self, pos: LogPos) -> Result<()>;

    /// Seal the loglet. After sealing, all appends return Err(Sealed).
    /// Returns the final tail position (the position after the last entry).
    async fn seal(&self) -> Result<LogPos>;
}
