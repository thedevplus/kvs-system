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
use std::io::{BufReader, BufWriter, Read, Write};
use std::os::unix::fs::FileExt;
use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::{Relaxed, SeqCst};
use std::sync::{Arc, Mutex, RwLock};
use std::thread::{self, JoinHandle};
use std::time::Duration;
// use log::{LevelFilter, debug};
// use std::time::SystemTime;

/// File extension for log files
const LOG_FILE_EXT: &str = "log";
/// Maximum size per log file
const LOG_FILE_SIZE: u64 = 1024 * 1024;
/// Threshold for triggering compaction
const LOG_UNCOMPACT: u64 = 1000;
const CMD_EXE_RATIO: u64 = 1;
const LOG_UNCOMPACT_SLEEP: u64 = LOG_UNCOMPACT * 500;

pub struct KvStore {
    path: Arc<RwLock<PathBuf>>,
    reader: Arc<RwLock<HashMap<u64, File>>>,
    shared: Arc<Mutex<SharedKvStore>>,
    map: Arc<RwLock<HashMap<String, KvPointer>>>,
    uncompact: Arc<AtomicU64>,
    maintain: Arc<Mutex<Option<JoinHandle<()>>>>,
    // flag: bool,
}

struct SharedKvStore {
    active: KvPointer,
    writer: BufWriter<File>,
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
            reader: Arc::clone(&self.reader),
            shared: Arc::clone(&self.shared),
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
        if self.uncompact.load(Relaxed) > LOG_UNCOMPACT_SLEEP {
            thread::sleep(Duration::from_nanos(CMD_EXE_RATIO));
        } else if self.uncompact.load(Relaxed) > LOG_UNCOMPACT {
            thread::yield_now();
        }
        let kv_log = KvLog::build_from(KvCommand::Set, key.clone(), Some(value));
        self.write_log(&kv_log)?;
        // self.start_compact()?;
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
            if self.uncompact.load(Relaxed) > LOG_UNCOMPACT_SLEEP {
                thread::sleep(Duration::from_nanos(CMD_EXE_RATIO));
            } else if self.uncompact.load(Relaxed) > LOG_UNCOMPACT {
                thread::yield_now();
            }
            let kv_log = KvLog::build_from(KvCommand::Rm, key.clone(), None);
            self.write_log(&kv_log)?;
            // self.start_compact()?;
        } else {
            return Err(KvError::Log);
        }

        Ok(())
    }
}

impl Drop for KvStore {
    fn drop(&mut self) {
        if Arc::strong_count(&self.maintain) == 1 {
            self.uncompact.store(u64::MAX, SeqCst);
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

        let shared = SharedKvStore {
            active: KvPointer::new(),
            writer: BufWriter::new(
                OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(number_convert_to_log_path(&path, 0, LOG_FILE_EXT))?,
            ),
        };

        let mut kvs = Self {
            path: Arc::new(RwLock::new(path.clone())),
            reader: Arc::new(RwLock::new(HashMap::new())),
            shared: Arc::new(Mutex::new(shared)),
            map: Arc::new(RwLock::new(HashMap::new())),
            uncompact: Arc::new(AtomicU64::new(0)),
            maintain: Arc::new(Mutex::new(None)),
            // flag: false,
        };

        kvs.start_build_map_and_reader()?;
        let log_path = BufWriter::new(OpenOptions::new().append(true).open(
            number_convert_to_log_path(
                &path,
                kvs.shared.lock().map_err(|_| KvError::Lock)?.active.log,
                LOG_FILE_EXT,
            ),
        )?);
        kvs.shared.lock().map_err(|_| KvError::Lock)?.writer = log_path;
        let compact_kvs = kvs.clone();
        kvs.maintain = Arc::new(Mutex::new(Some(thread::spawn(move || {
            if compact_kvs.start_compact().is_ok() {}
        }))));

        Ok(kvs)
    }

    fn write_log(&self, log: &KvLog) -> Result<()> {
        let stream = self.stream_serialize(log)?;
        let sz = stream.len() as u64 + 9;
        let mut shared = self.shared.lock().map_err(|_| KvError::Lock)?;

        if shared.active.pos + shared.active.sz + sz > LOG_FILE_SIZE {
            shared.active.log += 1;
            shared.active.pos = 0;
            shared.active.sz = 0;
            let file = number_convert_to_log_path(
                self.path.read().map_err(|_| KvError::Lock)?.as_path(),
                shared.active.log,
                LOG_FILE_EXT,
            );
            shared.writer =
                BufWriter::new(OpenOptions::new().create(true).append(true).open(&file)?);
            self.reader
                .write()
                .map_err(|_| KvError::Lock)?
                .insert(shared.active.log, File::open(file)?);
        }

        shared.writer.write_all(sz.to_le_bytes().as_slice())?;
        shared.writer.write_all(&stream)?;
        shared.writer.write_all(b"\n")?;
        shared.writer.flush()?;
        shared.active = shared.active.build_from(sz);
        let active = shared.active.clone();
        drop(shared);

        let mut map = self.map.write().map_err(|_| KvError::Lock)?;
        if let KvCommand::Set = log.command {
            if map.insert(log.key.clone(), active).is_some() {
                self.uncompact.fetch_add(1, Relaxed);
            };
        } else {
            map.remove(&log.key).ok_or(KvError::Log)?;
            self.uncompact.fetch_add(1, Relaxed);
        }
        // self.flag = true;
        Ok(())
    }

    fn read_log(&self, key: &String) -> Result<KvLog> {
        let p = self
            .map
            .read()
            .map_err(|_| KvError::Lock)?
            .get(key)
            .ok_or(KvError::Log)?
            .clone();
        let mut data = vec![0u8; p.sz as usize - 8];
        let reader = self
            .reader
            .read()
            .map_err(|_| KvError::Lock)?
            .get(&p.log)
            .ok_or(KvError::File)?
            .try_clone()?;
        reader.read_at(&mut data, p.pos + 8)?;
        let log = self.stream_deserialize(&data)?;

        Ok(log)
    }

    fn stream_serialize<T: Serialize>(&self, data: T) -> Result<Vec<u8>> {
        Ok(serde_json::to_vec(&data)?)
    }

    fn stream_deserialize<'a, T: Deserialize<'a>>(&self, data: &'a [u8]) -> Result<T> {
        Ok(serde_json::from_slice(data)?)
    }

    fn read_directory_and_sort(&self, reverse: bool) -> Result<Vec<(u64, PathBuf)>> {
        let mut dir_files = Vec::new();
        for entry in fs::read_dir(self.path.read().map_err(|_| KvError::Lock)?.as_path())?.flatten()
        {
            let entry = entry.path();
            let file = entry
                .file_stem()
                .ok_or(KvError::File)?
                .to_str()
                .ok_or(KvError::File)?;
            if !(file.contains("._") || file.contains(".DS_"))
                && let Ok(file) = file.parse()
            {
                dir_files.push((file, entry));
            } else {
                continue;
            }
        }
        if reverse {
            dir_files.sort_by(|a, b| b.cmp(a));
        } else {
            dir_files.sort();
        }
        Ok(dir_files)
    }

    fn start_build_map_and_reader(&self) -> Result<()> {
        let mut shared = self.shared.lock().map_err(|_| KvError::Lock)?;
        let mut map = self.map.write().map_err(|_| KvError::Lock)?;
        let mut reader = self.reader.write().map_err(|_| KvError::Lock)?;
        for e in self.read_directory_and_sort(false)? {
            let log = e.0;
            let file = &e.1;
            reader.insert(log, File::open(file)?);

            let mut reader = BufReader::new(OpenOptions::new().read(true).open(file)?);
            let mut pointer = KvPointer { log, pos: 0, sz: 0 };
            let mut read_sz = [0u8; 8];
            let mut sz;
            let mut read_log = Vec::with_capacity(1024 * 1024);

            loop {
                if reader.read_exact(&mut read_sz).is_err() {
                    break;
                }
                sz = u64::from_le_bytes(read_sz);
                read_log.resize(sz as usize - 8, 0);
                if reader.read_exact(&mut read_log).is_err() {
                    break;
                }
                let kv_log = self.stream_deserialize::<KvLog>(&read_log)?;
                pointer = pointer.build_from(sz);
                if let KvCommand::Set = kv_log.command {
                    if map.insert(kv_log.key, pointer.clone()).is_some() {
                        self.uncompact.fetch_add(1, Relaxed);
                    }
                } else {
                    map.remove(&kv_log.key);
                    self.uncompact.fetch_add(1, Relaxed);
                }
            }

            shared.active = pointer.clone();
        }
        Ok(())
    }

    fn start_compact(&self) -> Result<()> {
        loop {
            let uncompact = self.uncompact.load(Relaxed);
            if uncompact == u64::MAX {
                break;
            } else if uncompact >= LOG_UNCOMPACT {
                let path = self.path.read().map_err(|_| KvError::Lock)?;
                let mut entry = self.read_directory_and_sort(false)?;
                let mut compact_file_index = 0u64;
                let mut writer =
                    BufWriter::new(OpenOptions::new().create(true).append(true).open(
                        number_convert_to_log_path(&*path, compact_file_index, "compact"),
                    )?);
                let mut compact = 0u64;
                let mut remove_file_success = 0u64;

                entry.pop();
                for e in &entry {
                    let log = e.0;
                    let file = &e.1;

                    let mut reader = BufReader::new(OpenOptions::new().read(true).open(file)?);
                    let mut pointer = KvPointer { log, pos: 0, sz: 0 };
                    let mut read_sz = [0u8; 8];
                    let mut sz;
                    let mut read_log = Vec::with_capacity(1024 * 1024);
                    let mut map = self.map.write().map_err(|_| KvError::Lock)?;
                    let mut file_reader = self.reader.write().map_err(|_| KvError::Lock)?;

                    loop {
                        if reader.read_exact(&mut read_sz).is_err() {
                            break;
                        }
                        sz = u64::from_le_bytes(read_sz);
                        read_log.resize(sz as usize - 8, 0);
                        if reader.read_exact(&mut read_log).is_err() {
                            break;
                        }
                        let kv_log = self.stream_deserialize::<KvLog>(&read_log)?;

                        if let Some(map_store_log) = map.get(&kv_log.key)
                            && map_store_log.log == log
                        {
                            if pointer.pos + pointer.sz + sz > LOG_FILE_SIZE {
                                pointer.log += 1;
                                pointer.pos = 0;
                                pointer.sz = 0;

                                let log_file_before = number_convert_to_log_path(
                                    &*path,
                                    compact_file_index,
                                    LOG_FILE_EXT,
                                );
                                let log_file_after = number_convert_to_log_path(
                                    &*path,
                                    compact_file_index,
                                    "compact",
                                );
                                if fs::remove_file(&log_file_before).is_ok() {
                                    remove_file_success += 1;
                                };
                                fs::rename(&log_file_after, &log_file_before)?;
                                file_reader
                                    .insert(compact_file_index, File::open(&log_file_before)?)
                                    .ok_or(KvError::File)?;
                                compact_file_index += 1;
                                writer = BufWriter::new(
                                    OpenOptions::new().create(true).append(true).open(
                                        number_convert_to_log_path(
                                            &*path,
                                            compact_file_index,
                                            "compact",
                                        ),
                                    )?,
                                );
                            }

                            writer.write_all(&read_sz)?;
                            writer.write_all(&read_log)?;
                            writer.write_all(b"\n")?;

                            pointer = pointer.build_from(sz);
                            map.insert(kv_log.key, pointer.clone())
                                .ok_or(KvError::Log)?;

                            compact += 1;
                        }
                    }
                }
                let log_file_before =
                    number_convert_to_log_path(&*path, compact_file_index, LOG_FILE_EXT);
                let log_file_after =
                    number_convert_to_log_path(&*path, compact_file_index, "compact");
                if fs::remove_file(&log_file_before).is_ok() {
                    remove_file_success += 1;
                };
                fs::rename(&log_file_after, &log_file_before)?;
                self.reader
                    .write()
                    .map_err(|_| KvError::Lock)?
                    .insert(compact_file_index, File::open(&log_file_before)?)
                    .ok_or(KvError::File)?;

                if entry.len() >= remove_file_success as usize {
                    entry[remove_file_success as usize..]
                        .iter()
                        .for_each(|(i, _)| {
                            if fs::remove_file(number_convert_to_log_path(&*path, *i, LOG_FILE_EXT))
                                .is_err()
                            {}
                        });
                }

                self.uncompact.fetch_sub(compact, SeqCst);
            }
            thread::sleep(Duration::from_nanos(CMD_EXE_RATIO * 1000));
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

fn number_convert_to_log_path(path: impl Into<PathBuf>, log: u64, ext: &str) -> PathBuf {
    let mut path = path.into();
    path.push(log.to_string());
    path.set_extension(ext);
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
        File::create(number_convert_to_log_path(dir, 0, LOG_FILE_EXT))?;
    }
    Ok(())
}
