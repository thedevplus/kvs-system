use crate::error::KvError;

mod kvs;
mod engine;
mod error;

pub type Result<T> = std::result::Result<T, KvError>;

pub use crate::engine::KvsEngine;
pub use crate::kvs::KvStore;