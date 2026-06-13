use crate::error::KvError;

mod engine;
mod error;
mod kvs;

pub type Result<T> = std::result::Result<T, KvError>;

pub use crate::engine::KvsEngine;
pub use crate::kvs::KvStore;
