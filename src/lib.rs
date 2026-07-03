use crate::error::KvError;

mod engine;
pub mod error;
pub mod kvs;
pub mod protocol;
pub mod sled;
pub mod thread_pool;

pub type Result<T> = std::result::Result<T, KvError>;

pub use crate::engine::KvsEngine;
pub use crate::kvs::KvStore;
pub use crate::sled::SledKvsEngine;

pub enum Engine {
    Kvs(KvStore),
    Sled(SledKvsEngine),
}

impl Engine {
    pub fn set(&self, key: String, value: String) -> Result<()> {
        match self {
            Engine::Kvs(kvs) => kvs.set(key, value),
            Engine::Sled(sled) => sled.set(key, value),
        }
    }

    pub fn get(&self, key: String) -> Result<Option<String>> {
        match self {
            Engine::Kvs(kvs) => kvs.get(key),
            Engine::Sled(sled) => sled.get(key),
        }
    }

    pub fn remove(&self, key: String) -> Result<()> {
        match self {
            Engine::Kvs(kvs) => kvs.remove(key),
            Engine::Sled(sled) => sled.remove(key),
        }
    }
}
