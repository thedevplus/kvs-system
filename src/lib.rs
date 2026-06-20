use crate::error::KvError;

mod engine;
pub mod error;
pub mod kvs;
pub mod protocol;
pub mod sled;

pub type Result<T> = std::result::Result<T, KvError>;

pub use crate::engine::KvsEngine;
pub use crate::kvs::KvStore;
pub use crate::sled::SledKvsEngine;
