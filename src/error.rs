use std::io;
use thiserror::Error;

pub type Result<T> = std::result::Result<T, KvError>;

#[derive(Error, Debug)]
pub enum KvError {
    #[error("serialization error: {0}")]
    SerdeError(#[from] serde_json::Error),

    #[error("I/O error: {0}")]
    IoError(#[from] io::Error),

    #[error("key not found or operation failed")]
    Other,
}
