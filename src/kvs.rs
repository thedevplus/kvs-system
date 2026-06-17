//! # kvs
//!
//! A log-structured key-value store inspired by the Bitcask paper.
//!
//! # Features
//!
//! - Append-only log for sequential I/O performance
//! - In-memory HashMap index for O(1) lookups
//! - Automatic compaction for garbage collection
//! - Crash recovery with durable writes

use crate::Result;
use crate::engine::KvsEngine;
use crate::error::KvError;
use clap::ValueEnum;
use log::debug;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{self, DirBuilder, File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::time::SystemTime;

/// Directory name for storing log files
const LOG_FILE_DIR: &str = "database";
/// File extension for log files
const LOG_FILE_EXT: &str = "log";
/// Maximum size per log file
const LOG_FILE_SIZE: u64 = 1024 * 1024;
/// Threshold for triggering compaction
const LOG_UNCOMPACT: u64 = 1000;

pub struct KvStore {
    path: PathBuf,
    active: KvPointer,
    buffer: BufWriter<File>,
    map: HashMap<String, KvPointer>,
    uncompact: u64,
    flag: bool,
}

#[derive(Copy, Clone, Deserialize, Serialize, Debug, ValueEnum)]
pub enum KvCommand {
    Set,
    Get,
    Rm,
}

#[derive(Deserialize, Serialize, Clone, Debug)]
struct KvLog {
    pub command: KvCommand,
    time: SystemTime,
    pub key: String,
    pub value: Option<String>,
}

#[derive(Clone, Deserialize, Serialize, Debug)]
struct KvPointer {
    log: u64,
    pos: u64,
    sz: u64,
}

impl KvsEngine for KvStore {
    /// Sets a key-value pair in the store.
    ///
    /// If the key already exists, the value will be updated.
    /// Triggers compaction when the uncompact count exceeds the threshold.
    fn set(&mut self, key: String, value: String) -> Result<()> {
        let kv_log = KvLog::build_from(KvCommand::Set, key.clone(), Some(value));
        self.write_log(&kv_log)?;
        self.add_index(key)?;
        self.start_compact()
    }

    /// Gets the value associated with the given key.
    ///
    /// Returns `Some(value)` if the key exists, `None` otherwise.
    fn get(&mut self, key: String) -> Result<Option<String>> {
        if self.flag {
            self.buffer.flush()?;
            self.flag = false;
        }
        match self.read_log(&key) {
            Ok(log) => {
                println!("{}", log.value.as_ref().unwrap());
                Ok(log.value)
            }
            Err(_) => {
                println!("Key not found");
                Ok(None)
            }
        }
    }

    /// Removes the key-value pair associated with the given key.
    ///
    /// Returns an error if the key does not exist.
    fn remove(&mut self, key: String) -> Result<()> {
        if self.map.contains_key(&key) {
            let kv_log = KvLog::build_from(KvCommand::Rm, key.clone(), None);
            self.write_log(&kv_log)?;
            self.delete_index(key)?;
            self.start_compact()
        } else {
            println!("Key not found");
            Err(KvError::Log)
        }
    }
}

impl KvStore {
    /// Opens or creates a key-value store at the given path.
    ///
    /// Creates the database directory and log files if they do not exist.
    /// Builds the in-memory index from existing log files on startup.
    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let mut path = path.into();
        path.push(LOG_FILE_DIR);
        debug!("Initialize path ok.");
        directory_initial(&path)?;
        let mut kvs = Self {
            buffer: BufWriter::new(
                OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(number_convert_to_log_path(&path, 0))?,
            ),
            map: HashMap::new(),
            path: path.clone(),
            active: KvPointer::new(),
            uncompact: 0,
            flag: false,
        };
        debug!("Initialize file ok.");
        kvs.map = kvs.start_build_index()?;
        kvs.buffer = BufWriter::new(
            OpenOptions::new()
                .append(true)
                .open(number_convert_to_log_path(&path, kvs.active.log))?,
        );
        debug!("Open pointer file ok.");
        Ok(kvs)
    }

    fn write_log(&mut self, log: &KvLog) -> Result<()> {
        let stream = self.stream_serialize(log)?;
        let sz = stream.len() as u64 + 1;
        if self.active.pos + self.active.sz + sz > LOG_FILE_SIZE {
            self.active.log += 1;
            self.active.pos = 0;
            self.active.sz = 0;
            self.buffer = BufWriter::new(
                OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(number_convert_to_log_path(&self.path, self.active.log))?,
            );
        }
        self.buffer.write_all(&stream)?;
        self.buffer.write_all(b"\n")?;
        self.active = self.active.build_from(sz);
        self.flag = true;
        Ok(())
    }

    fn read_log(&self, key: &String) -> Result<KvLog> {
        let Some(p) = self.map.get(key) else {
            return Err(KvError::Log);
        };
        let mut file = BufReader::new(File::open(number_convert_to_log_path(&self.path, p.log))?);
        let mut data = vec![0u8; p.sz as usize];
        file.seek(SeekFrom::Start(p.pos))?;
        file.read_exact(&mut data)?;
        let log: KvLog = self.stream_deserialize(&data)?;
        Ok(log)
    }

    fn stream_serialize<T: Serialize>(&mut self, data: T) -> Result<Vec<u8>> {
        Ok(serde_json::to_vec(&data)?)
    }

    fn stream_deserialize<'a, T: Deserialize<'a>>(&self, data: &'a [u8]) -> Result<T> {
        Ok(serde_json::from_slice(data)?)
    }

    fn add_index(&mut self, key: String) -> Result<()> {
        let None = self.map.insert(key.clone(), self.active.clone()) else {
            self.uncompact += 1;
            return Ok(());
        };
        Ok(())
    }

    fn delete_index(&mut self, key: String) -> Result<()> {
        self.map.remove(&key);
        self.uncompact += 1;
        Ok(())
    }

    fn read_directory_and_sort(&self) -> Result<Vec<(u64, PathBuf)>> {
        let mut dir_files = Vec::new();
        for entry in fs::read_dir(&self.path)?.flatten() {
            let entry = entry.path();
            let file = entry
                .file_prefix()
                .ok_or(KvError::File)?
                .to_str()
                .ok_or(KvError::File)?;
            if !(file.contains("._") || file.contains(".DS_")) {
                dir_files.push((file.parse().unwrap(), entry));
            }
        }
        dir_files.sort();
        Ok(dir_files)
    }

    fn start_build_index(&mut self) -> Result<HashMap<String, KvPointer>> {
        let mut map = HashMap::new();
        for e in self.read_directory_and_sort()?.iter() {
            let log = e.0;
            let mut pos = 0u64;
            let mut sz = 0u64;
            let file = &e.1;
            let reader = BufReader::new(File::open(file)?);
            serde_json::Deserializer::from_reader(reader)
                .into_iter::<KvLog>()
                .flatten()
                .for_each(|e| {
                    sz = get_size(&e);
                    let pointer = KvPointer { log, pos, sz };
                    if let KvCommand::Set = e.command {
                        if map.contains_key(&e.key) {
                            self.uncompact += 1;
                        }
                        map.insert(e.key, pointer.clone());
                    } else {
                        map.remove(&e.key);
                        self.uncompact += 1;
                    }
                    self.active = pointer;
                    pos += sz;
                });
        }
        Ok(map)
    }

    fn start_compact(&mut self) -> Result<()> {
        if self.uncompact > LOG_UNCOMPACT {
            let compact_bound = self.active.log;
            for e in self.read_directory_and_sort()?.iter() {
                let log = e.0;
                if log < compact_bound {
                    let mut pos = 0u64;
                    let file = &e.1;
                    let reader = BufReader::new(File::open(file)?);
                    let stream_iter = serde_json::Deserializer::from_reader(reader)
                        .into_iter::<KvLog>()
                        .flatten();
                    for e in stream_iter {
                        let sz = get_size(&e);
                        let pointer = KvPointer { log, pos, sz };
                        if pointer
                            == *self.map.get(&e.key).unwrap_or(&KvPointer {
                                log: 0,
                                pos: 0,
                                sz: 0,
                            })
                        {
                            self.write_log(&e)?;
                            self.add_index(e.key.clone())?;
                        }
                        pos += sz;
                    }
                    fs::remove_file(file)?;
                }
            }
            self.uncompact = 0;
        }
        Ok(())
    }
}

impl KvPointer {
    fn new() -> Self {
        Self {
            log: 0,
            pos: 0,
            sz: 0,
        }
    }

    fn build_from(&self, sz: u64) -> Self {
        Self {
            log: self.log,
            pos: self.pos + self.sz,
            sz,
        }
    }
}

impl PartialEq for KvPointer {
    fn eq(&self, other: &Self) -> bool {
        self.log == other.log && self.pos == other.pos && self.sz == other.sz
    }
}

impl Eq for KvPointer {}

impl KvLog {
    fn build_from(command: KvCommand, key: String, value: Option<String>) -> Self {
        Self {
            command,
            time: SystemTime::now(),
            key,
            value,
        }
    }
}

fn number_convert_to_log_path(path: impl Into<PathBuf>, log: u64) -> PathBuf {
    let mut path = path.into();
    path.push(log.to_string());
    path.set_extension(LOG_FILE_EXT);
    path
}

fn get_size(log: &KvLog) -> u64 {
    serde_json::to_vec(log)
        .expect("Unable to get size from convert log to stream")
        .len() as u64
        + 1
}

fn directory_initial(dir: &PathBuf) -> Result<()> {
    if !dir.is_dir() {
        DirBuilder::new().create(dir)?;
        debug!("Creation path ok.");
        File::create(number_convert_to_log_path(dir, 0))?;
    }
    Ok(())
}
