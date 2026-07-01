use std::{io, str, sync};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum KvError {
    #[error("Serialization error: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
    #[error("Logger error: {0}")]
    Logger(#[from] log::SetLoggerError),
    #[error("Sled error: {0}")]
    Sled(#[from] sled::Error),
    #[error("Str convert error: {0}")]
    Utf8(#[from] str::Utf8Error),
    #[error("Failed to write to or remove log")]
    Log,
    #[error("Failed to open file or get file path")]
    File,
    #[error("Network stream error")]
    Network,
    #[error("Failed to get lock")]
    RwLock,
}
