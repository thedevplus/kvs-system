use crate::error::KvError;

mod engine;
mod error;
pub mod kvs;
pub mod protocol;
pub mod logger;

pub type Result<T> = std::result::Result<T, KvError>;

pub use crate::engine::KvsEngine;
pub use crate::kvs::KvStore;
