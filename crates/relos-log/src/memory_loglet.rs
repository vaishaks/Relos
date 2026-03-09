use async_trait::async_trait;
use tokio::sync::RwLock;

use relos_core::{Entry, LogPos, RelosError, Result, LOG_POS_BEGIN};

use crate::loglet::Loglet;

struct MemoryLogletInner {
    entries: Vec<Option<Entry>>,
    sealed: bool,
    trim_pos: LogPos,
}

/// In-memory loglet implementation for testing.
pub struct MemoryLoglet {
    inner: RwLock<MemoryLogletInner>,
}

impl MemoryLoglet {
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(MemoryLogletInner {
                entries: Vec::new(),
                sealed: false,
                trim_pos: LOG_POS_BEGIN,
            }),
        }
    }
}

impl Default for MemoryLoglet {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Loglet for MemoryLoglet {
    async fn append(&self, entry: &Entry) -> Result<LogPos> {
        let mut inner = self.inner.write().await;
        let tail = inner.entries.len() as LogPos + LOG_POS_BEGIN;
        if inner.sealed {
            return Err(RelosError::Sealed(tail));
        }
        inner.entries.push(Some(entry.clone()));
        Ok(tail)
    }

    async fn check_tail(&self) -> Result<LogPos> {
        let inner = self.inner.read().await;
        Ok(inner.entries.len() as LogPos + LOG_POS_BEGIN)
    }

    async fn read(&self, pos: LogPos) -> Result<Option<Entry>> {
        let inner = self.inner.read().await;
        if pos < inner.trim_pos {
            return Err(RelosError::Trimmed(pos));
        }
        let tail = inner.entries.len() as LogPos + LOG_POS_BEGIN;
        if pos >= tail {
            return Ok(None);
        }
        let index = (pos - LOG_POS_BEGIN) as usize;
        Ok(inner.entries[index].clone())
    }

    async fn trim(&self, pos: LogPos) -> Result<()> {
        let mut inner = self.inner.write().await;
        if pos > inner.trim_pos {
            inner.trim_pos = pos;
            // Replace trimmed entries with None
            let tail = inner.entries.len() as LogPos + LOG_POS_BEGIN;
            let end = if pos < tail { pos } else { tail };
            for p in LOG_POS_BEGIN..end {
                let index = (p - LOG_POS_BEGIN) as usize;
                inner.entries[index] = None;
            }
        }
        Ok(())
    }

    async fn seal(&self) -> Result<LogPos> {
        let mut inner = self.inner.write().await;
        inner.sealed = true;
        Ok(inner.entries.len() as LogPos + LOG_POS_BEGIN)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_append_and_read() {
        let loglet = MemoryLoglet::new();
        let entry = Entry::new(b"hello".to_vec());

        let pos = loglet.append(&entry).await.unwrap();
        assert_eq!(pos, 1);

        let read_entry = loglet.read(pos).await.unwrap().unwrap();
        assert_eq!(read_entry.payload, b"hello");
    }

    #[tokio::test]
    async fn test_append_multiple() {
        let loglet = MemoryLoglet::new();

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
    async fn test_check_tail() {
        let loglet = MemoryLoglet::new();
        assert_eq!(loglet.check_tail().await.unwrap(), 1);

        loglet.append(&Entry::new(b"a".to_vec())).await.unwrap();
        assert_eq!(loglet.check_tail().await.unwrap(), 2);

        loglet.append(&Entry::new(b"b".to_vec())).await.unwrap();
        assert_eq!(loglet.check_tail().await.unwrap(), 3);
    }

    #[tokio::test]
    async fn test_read_beyond_tail_returns_none() {
        let loglet = MemoryLoglet::new();
        assert!(loglet.read(1).await.unwrap().is_none());
        assert!(loglet.read(100).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_seal_blocks_appends() {
        let loglet = MemoryLoglet::new();
        loglet.append(&Entry::new(b"a".to_vec())).await.unwrap();

        let seal_pos = loglet.seal().await.unwrap();
        assert_eq!(seal_pos, 2);

        let result = loglet.append(&Entry::new(b"b".to_vec())).await;
        assert!(matches!(result, Err(RelosError::Sealed(2))));
    }

    #[tokio::test]
    async fn test_seal_returns_tail() {
        let loglet = MemoryLoglet::new();
        loglet.append(&Entry::new(b"x".to_vec())).await.unwrap();
        loglet.append(&Entry::new(b"y".to_vec())).await.unwrap();

        let tail = loglet.seal().await.unwrap();
        assert_eq!(tail, 3);
    }

    #[tokio::test]
    async fn test_trim() {
        let loglet = MemoryLoglet::new();
        loglet.append(&Entry::new(b"a".to_vec())).await.unwrap();
        loglet.append(&Entry::new(b"b".to_vec())).await.unwrap();
        loglet.append(&Entry::new(b"c".to_vec())).await.unwrap();

        // Trim positions < 3 (i.e., positions 1 and 2)
        loglet.trim(3).await.unwrap();

        // Positions 1 and 2 should return Trimmed
        assert!(matches!(loglet.read(1).await, Err(RelosError::Trimmed(1))));
        assert!(matches!(loglet.read(2).await, Err(RelosError::Trimmed(2))));

        // Position 3 should still be readable
        assert_eq!(loglet.read(3).await.unwrap().unwrap().payload, b"c");
    }

    #[tokio::test]
    async fn test_trim_idempotent() {
        let loglet = MemoryLoglet::new();
        loglet.append(&Entry::new(b"a".to_vec())).await.unwrap();
        loglet.append(&Entry::new(b"b".to_vec())).await.unwrap();

        loglet.trim(2).await.unwrap();
        loglet.trim(2).await.unwrap(); // should be fine
        loglet.trim(1).await.unwrap(); // lower trim_pos should be ignored

        assert!(matches!(loglet.read(1).await, Err(RelosError::Trimmed(1))));
        assert_eq!(loglet.read(2).await.unwrap().unwrap().payload, b"b");
    }

    #[tokio::test]
    async fn test_read_after_seal() {
        let loglet = MemoryLoglet::new();
        loglet.append(&Entry::new(b"a".to_vec())).await.unwrap();
        loglet.seal().await.unwrap();

        // Reading should still work after sealing
        assert_eq!(loglet.read(1).await.unwrap().unwrap().payload, b"a");
    }

    #[tokio::test]
    async fn test_entry_with_headers() {
        let loglet = MemoryLoglet::new();
        let entry = Entry::new(b"payload".to_vec())
            .with_header("engine1", b"header_data".to_vec());

        let pos = loglet.append(&entry).await.unwrap();
        let read_entry = loglet.read(pos).await.unwrap().unwrap();

        assert_eq!(read_entry.payload, b"payload");
        assert_eq!(read_entry.get_header("engine1"), Some(b"header_data".as_ref()));
    }
}
