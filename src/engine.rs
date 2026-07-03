use crate::Result;
use crate::{KvStore, SledKvsEngine};

pub trait KvsEngine: Clone + Send + 'static {
    fn set(&self, key: String, value: String) -> Result<()>;

    fn get(&self, key: String) -> Result<Option<String>>;

    fn remove(&self, key: String) -> Result<()>;
}

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

impl Clone for Engine {
    fn clone(&self) -> Self {
        match self {
            Engine::Kvs(kvs) => Engine::Kvs(KvStore::clone(kvs)),
            Engine::Sled(sled) => Engine::Sled(SledKvsEngine::clone(sled)),
        }
    }
}
