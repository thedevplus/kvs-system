use crate::error::KvError;

mod engine;
pub mod error;
pub mod kvs;
pub mod protocol;
pub mod sled;
pub mod thread_pool;

pub type Result<T> = std::result::Result<T, KvError>;

pub use crate::engine::{Engine, KvsEngine};
pub use crate::kvs::KvStore;
pub use crate::sled::SledKvsEngine;
