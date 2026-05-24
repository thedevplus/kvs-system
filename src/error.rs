use std::io;
use thiserror::Error;

pub type Result<T> = std::result::Result<T, KvError>;

#[derive(Error, Debug)]
#[error("kvs error")]
pub enum KvError {
    SerdeError(#[from] serde_json::Error),
    IoError(#[from] io::Error),
    Other,
}
