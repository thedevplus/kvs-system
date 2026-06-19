use crate::error::KvError;
use crate::{KvsEngine, Result};
use sled::Db;
use std::fs::DirBuilder;
use std::path::PathBuf;
use std::process;

pub struct SledKvsEngine {
    sled: Db,
}

impl SledKvsEngine {
    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();
        if !path.is_dir() {
            DirBuilder::new().create(&path)?;
        }
        if let Ok(sled) = sled::open(&path) {
            Ok(Self { sled })
        } else {
            process::exit(1);
        }
    }
}

impl KvsEngine for SledKvsEngine {
    fn set(&mut self, key: String, value: String) -> Result<()> {
        self.sled.insert(key, value.as_bytes())?;
        self.sled.flush()?;
        Ok(())
    }

    fn get(&mut self, key: String) -> Result<Option<String>> {
        self.sled.flush()?;
        match self.sled.get(key) {
            Ok(Some(v)) => Ok(Some(str::from_utf8(v.trim_ascii())?.to_string())),
            Ok(None) => Ok(None),
            Err(e) => Err(KvError::Sled(e)),
        }
    }

    fn remove(&mut self, key: String) -> Result<()> {
        if self.sled.remove(key)?.is_some() {
            self.sled.flush()?;
            Ok(())
        } else {
            Err(KvError::Log)
        }
    }
}
