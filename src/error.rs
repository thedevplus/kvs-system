use std::io;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum KvError {
    #[error("Serialization error: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
    #[error("Logger error: {0}")]
    Logger(#[from] log::SetLoggerError),
    #[error("Failed to write to or remove log")]
    Log,
    #[error("Failed to open file or get file path")]
    File,
}
