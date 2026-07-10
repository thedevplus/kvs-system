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
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{self, DirBuilder, File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::{Arc, Mutex, RwLock};
use std::thread::{self, JoinHandle};
use std::time::Duration;
// use std::time::SystemTime;

/// File extension for log files
const LOG_FILE_EXT: &str = "log";
/// Maximum size per log file
const LOG_FILE_SIZE: u64 = 1024 * 1024;
/// Threshold for triggering compaction
const LOG_UNCOMPACT: u64 = 1000;

type MultiSafeHashMap = Arc<RwLock<HashMap<u64, Arc<RwLock<BufReader<File>>>>>>;

pub struct KvStore {
    path: Arc<RwLock<PathBuf>>,
    active: Arc<RwLock<KvPointer>>,
    writer: Arc<RwLock<BufWriter<File>>>,
    reader: MultiSafeHashMap,
    map: Arc<RwLock<HashMap<String, KvPointer>>>,
    uncompact: Arc<RwLock<u64>>,
    maintain: Arc<Mutex<Option<JoinHandle<()>>>>,
    // flag: bool,
}

#[derive(Copy, Clone, Deserialize, Serialize, Debug, ValueEnum)]
pub enum KvCommand {
    Set,
    Get,
    Rm,
}

#[derive(Deserialize, Serialize, Clone, Debug)]
struct KvLog {
    command: KvCommand,
    // time: SystemTime,
    key: String,
    value: Option<String>,
}

#[derive(Clone, Deserialize, Serialize, Debug)]
struct KvPointer {
    log: u64,
    pos: u64,
    sz: u64,
}

impl Clone for KvStore {
    fn clone(&self) -> Self {
        Self {
            path: Arc::clone(&self.path),
            active: Arc::clone(&self.active),
            writer: Arc::clone(&self.writer),
            reader: Arc::clone(&self.reader),
            map: Arc::clone(&self.map),
            uncompact: Arc::clone(&self.uncompact),
            maintain: Arc::clone(&self.maintain),
        }
    }
}

impl KvsEngine for KvStore {
    /// Sets a key-value pair in the store.
    ///
    /// If the key already exists, the value will be updated.
    /// Triggers compaction when the uncompact count exceeds the threshold.
    fn set(&self, key: String, value: String) -> Result<()> {
        let kv_log = KvLog::build_from(KvCommand::Set, key.clone(), Some(value));
        let pointer = self.write_log(&kv_log)?;
        self.add_index(key, pointer)?;
        // self.start_compact()?;
        self.writer
            .try_write()
            .map_err(|_| KvError::Lock)?
            .flush()?;
        Ok(())
    }

    /// Gets the value associated with the given key.
    ///
    /// Returns `Some(value)` if the key exists, `None` otherwise.
    fn get(&self, key: String) -> Result<Option<String>> {
        /* Not needed if set and rm command flush themselves
        if self.flag {
            self.buffer.flush()?;
            self.flag = false;
        }
        */
        match self.read_log(&key) {
            Ok(log) => {
                // println!("{}", log.value.as_ref().unwrap());
                Ok(log.value)
            }
            Err(_) => {
                // eprintln!("Key not found");
                Ok(None)
            }
        }
    }

    /// Removes the key-value pair associated with the given key.
    ///
    /// Returns an error if the key does not exist.
    fn remove(&self, key: String) -> Result<()> {
        let execute = {
            if let Ok(map) = self.map.try_read().map_err(|_| KvError::Lock) {
                map.contains_key(&key)
            } else {
                true
            }
        };

        if execute {
            let kv_log = KvLog::build_from(KvCommand::Rm, key.clone(), None);
            self.write_log(&kv_log)?;
            self.delete_index(key)?;
            // self.start_compact()?;
            self.writer
                .try_write()
                .map_err(|_| KvError::Lock)?
                .flush()?;
        } else {
            return Err(KvError::Log);
        }

        Ok(())
    }
}

impl Drop for KvStore {
    fn drop(&mut self) {
        if Arc::strong_count(&self.maintain) == 1 {
            if let Ok(mut uncompact) = self.uncompact.write() {
                *uncompact = u64::MAX;
            }
            if let Ok(mut thread) = self.maintain.lock()
                && let Some(handle) = thread.take()
            {
                handle.join().unwrap();
            };
        }
    }
}

impl KvStore {
    /// Opens or creates a key-value store at the given path.
    ///
    /// Creates the database directory and log files if they do not exist.
    /// Builds the in-memory index from existing log files on startup.
    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();
        directory_initial(&path)?;
        let mut kvs = Self {
            writer: Arc::new(RwLock::new(BufWriter::new(
                OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(number_convert_to_log_path(&path, 0))?,
            ))),
            reader: Arc::new(RwLock::new(HashMap::new())),
            map: Arc::new(RwLock::new(HashMap::new())),
            path: Arc::new(RwLock::new(path.clone())),
            active: Arc::new(RwLock::new(KvPointer::new())),
            uncompact: Arc::new(RwLock::new(0)),
            maintain: Arc::new(Mutex::new(None)),
            // flag: false,
        };
        kvs.start_build_map_and_reader()?;
        kvs.writer = Arc::new(RwLock::new(BufWriter::new(
            OpenOptions::new()
                .append(true)
                .open(number_convert_to_log_path(
                    &path,
                    kvs.active.read().map_err(|_| KvError::Lock)?.log,
                ))?,
        )));
        let compact_kvs = kvs.clone();
        kvs.maintain = Arc::new(Mutex::new(Some(thread::spawn(move || {
            if compact_kvs.start_compact().is_ok() {}
        }))));
        Ok(kvs)
    }

    fn write_log(&self, log: &KvLog) -> Result<KvPointer> {
        let stream = self.stream_serialize(log)?;
        let sz = stream.len() as u64 + 1;
        let mut active = self.active.write().map_err(|_| KvError::Lock)?;
        let mut writer = self.writer.write().map_err(|_| KvError::Lock)?;
        if active.pos + active.sz + sz > LOG_FILE_SIZE {
            active.log += 1;
            active.pos = 0;
            active.sz = 0;
            *writer = BufWriter::new(OpenOptions::new().create(true).append(true).open(
                number_convert_to_log_path(
                    self.path.read().map_err(|_| KvError::Lock)?.as_path(),
                    active.log,
                ),
            )?);
        }
        writer.write_all(&stream)?;
        writer.write_all(b"\n")?;
        *active = active.build_from(sz);
        // self.flag = true;
        Ok(active.clone())
    }

    fn read_log(&self, key: &String) -> Result<KvLog> {
        let current_map = self.map.read().map_err(|_| KvError::Lock)?;
        let Some(p) = current_map.get(key) else {
            return Err(KvError::Log);
        };
        let current_reader = self.reader.read().map_err(|_| KvError::Lock)?;
        let mut reader = current_reader
            .get(&p.log)
            .ok_or(KvError::File)?
            .write()
            .map_err(|_| KvError::Lock)?;
        let mut data = vec![0u8; p.sz as usize];
        reader.seek(SeekFrom::Start(p.pos))?;
        reader.read_exact(&mut data)?;
        let log: KvLog = self.stream_deserialize(&data)?;
        Ok(log)
    }

    fn stream_serialize<T: Serialize>(&self, data: T) -> Result<Vec<u8>> {
        Ok(serde_json::to_vec(&data)?)
    }

    fn stream_deserialize<'a, T: Deserialize<'a>>(&self, data: &'a [u8]) -> Result<T> {
        Ok(serde_json::from_slice(data)?)
    }

    fn add_index(&self, key: String, pointer: KvPointer) -> Result<()> {
        let None = self
            .map
            .write()
            .map_err(|_| KvError::Lock)?
            .insert(key, pointer)
        else {
            *self.uncompact.write().map_err(|_| KvError::Lock)? += 1;
            return Ok(());
        };
        Ok(())
    }

    fn delete_index(&self, key: String) -> Result<()> {
        let mut map = self.map.write().map_err(|_| KvError::Lock)?;
        let mut uncompact = self.uncompact.write().map_err(|_| KvError::Lock)?;
        *uncompact += 1;
        if map.remove(&key).is_some() {
            Ok(())
        } else {
            // eprintln!("Key not found");
            Err(KvError::Log)
        }
    }

    fn read_directory_and_sort(&self) -> Result<Vec<(u64, PathBuf)>> {
        let mut dir_files = Vec::new();
        for entry in fs::read_dir(self.path.read().map_err(|_| KvError::Lock)?.as_path())?.flatten()
        {
            let entry = entry.path();
            let file = entry
                .file_prefix()
                .ok_or(KvError::File)?
                .to_str()
                .ok_or(KvError::File)?;
            if !(file.contains("._") || file.contains(".DS_"))
                && let Ok(file) = file.parse()
            {
                dir_files.push((file, entry));
            }
        }
        dir_files.sort();
        Ok(dir_files)
    }

    fn start_build_map_and_reader(&self) -> Result<()> {
        for e in self.read_directory_and_sort()?.iter() {
            let log = e.0;
            let mut pos = 0u64;
            let mut sz = 0u64;
            let file = &e.1;
            let mut active = self.active.write().map_err(|_| KvError::Lock)?;
            let mut map = self.map.write().map_err(|_| KvError::Lock)?;
            let mut reader = self.reader.write().map_err(|_| KvError::Lock)?;
            let mut uncompact = self.uncompact.write().map_err(|_| KvError::Lock)?;
            reader.insert(
                log,
                Arc::new(RwLock::new(BufReader::new(File::open(file)?))),
            );
            serde_json::Deserializer::from_reader(
                reader
                    .get(&log)
                    .ok_or(KvError::File)?
                    .read()
                    .map_err(|_| KvError::Lock)?
                    .get_ref(),
            )
            .into_iter::<KvLog>()
            .flatten()
            .for_each(|e| {
                sz = get_size(&e);
                let pointer = KvPointer { log, pos, sz };
                if let KvCommand::Set = e.command {
                    if map.contains_key(&e.key) {
                        *uncompact += 1;
                    }
                    map.insert(e.key, pointer.clone());
                } else {
                    map.remove(&e.key);
                    *uncompact += 1;
                }
                *active = pointer;
                pos += sz;
            });
        }
        Ok(())
    }

    fn start_compact(&self) -> Result<()> {
        loop {
            let uncompact = {
                let Ok(uncompact) = self.uncompact.try_read().map_err(|_| KvError::Lock) else {
                    continue;
                };
                *uncompact
            };
            if uncompact == u64::MAX {
                break;
            } else if uncompact >= LOG_UNCOMPACT {
                let compact_bound = {
                    let Ok(compuact_bound_lock) = self.active.try_read().map_err(|_| KvError::Lock)
                    else {
                        continue;
                    };
                    compuact_bound_lock.log
                };
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
                            let mut map_pointer = KvPointer {
                                log: 0,
                                pos: 0,
                                sz: 0,
                            };
                            loop {
                                if let Ok(map) = self.map.try_read().map_err(|_| KvError::Lock) {
                                    if let Some(pointer) = map.get(&e.key) {
                                        map_pointer = pointer.clone();
                                    }
                                    break;
                                }
                            }
                            if pointer == map_pointer {
                                self.write_log(&e)?;
                                self.add_index(e.key.clone(), pointer)?;
                            }
                            pos += sz;
                        }
                        fs::remove_file(file)?;
                    }
                }
                *self.uncompact.write().map_err(|_| KvError::Lock)? = 0;
            }

            thread::sleep(Duration::from_secs(1));
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
            // time: SystemTime::now(),
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
        File::create(number_convert_to_log_path(dir, 0))?;
    }
    Ok(())
}
