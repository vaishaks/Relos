use thiserror::Error;

#[derive(Error, Debug, Clone)]
pub enum RelosError {
    #[error("log sealed at position {0}")]
    Sealed(crate::LogPos),

    #[error("position {0} has been trimmed")]
    Trimmed(crate::LogPos),

    #[error("end of log reached")]
    EndOfLog,

    #[error("version mismatch: expected {expected}, got {actual}")]
    VersionMismatch { expected: u64, actual: u64 },

    #[error("serialization error: {0}")]
    Serialization(String),

    #[error("store error: {0}")]
    Store(String),

    #[error("not found: {0}")]
    NotFound(String),

    #[error("invalid operation: {0}")]
    InvalidOperation(String),

    #[error("io error: {0}")]
    Io(String),

    #[error("internal error: {0}")]
    Internal(String),
}

pub type Result<T> = std::result::Result<T, RelosError>;
