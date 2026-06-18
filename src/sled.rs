use sled::Db;
use std::{path::Path, process};
use crate::{KvsEngine, Result};
use crate::error::KvError;

struct SledKvsEngine {
    sled: Db,
}

impl SledKvsEngine {
    fn open<P: AsRef<Path>>(path: P) -> Self {
        if let Ok(sled) = sled::open(path) {
            Self { sled }
        } else {
            process::exit(1);
        }
    }
}

impl KvsEngine for SledKvsEngine {
    fn set(&mut self, key: String, value: String) -> Result<()> {
        self.sled.insert(key, value.as_bytes())?;
        Ok(())
    }

    fn get(&mut self, key: String) -> Result<Option<String>> {
        match self.sled.get(key) {
            Ok(Some(v)) => Ok(Some(std::str::from_utf8(v.trim_ascii())?.to_string())),
            Ok(None) => Ok(None),
            Err(e) => Err(KvError::Sled(e)),
        }
    }

    fn remove(&mut self, key: String) -> Result<()> {
        self.sled.remove(key)?;
        Ok(())
    }
}
