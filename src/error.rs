use std::io;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum KvError {
    #[error("Serialization error: {0}")]
    SerdeError(#[from] serde_json::Error),
    #[error("I/O error: {0}")]
    IoError(#[from] io::Error),
    #[error("Failed to write to or remove log")]
    LogError,
    #[error("Failed to open file or get file path")]
    FileError,
}
