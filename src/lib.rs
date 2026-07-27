use crate::error::KvError;

pub mod client;
mod engine;
pub mod error;
pub mod kvs;
pub mod protocol;
pub mod server;
pub mod sled;
pub mod thread_pool;

pub type Result<T> = std::result::Result<T, KvError>;

pub use crate::client::KvsClient;
pub use crate::engine::KvsEngine;
pub use crate::kvs::KvStore;
pub use crate::server::KvsServer;
pub use crate::sled::SledKvsEngine;
pub use crate::thread_pool::ThreadPool;
