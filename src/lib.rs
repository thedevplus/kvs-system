use crate::error::KvError;

mod engine;
pub mod error;
pub mod kvs;
pub mod logger;
pub mod protocol;

pub type Result<T> = std::result::Result<T, KvError>;

pub use crate::engine::KvsEngine;
pub use crate::kvs::KvStore;
