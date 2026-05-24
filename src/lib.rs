use crate::error::KvError;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{self, DirBuilder, File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::time::SystemTime;

pub use crate::error::Result;

pub mod error;

const LOG_FILE_DIR: &str = "database";
const LOG_FILE_EXT: &str = "log";
const LOG_FILE_SIZE: u64 = 1024 * 1024;
const LOG_UNCOMPACT: u64 = 1000;

#[derive(Debug)]
pub struct KvStore {
    path: PathBuf,
    active: KvPointer,
    buffer: BufWriter<File>,
    map: HashMap<String, KvPointer>,
    uncompact: u64,
    flag: bool,
}

#[derive(Copy, Clone, Deserialize, Serialize, Debug)]
enum KvCommand {
    Set,
    Remove,
}

impl std::fmt::Display for KvCommand {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            KvCommand::Set => write!(f, "Set"),
            KvCommand::Remove => write!(f, "Remove"),
        }
    }
}

#[derive(Clone, Deserialize, Serialize, Debug)]
struct KvPointer {
    log: u64,
    pos: u64,
    sz: u64,
}

impl std::fmt::Display for KvPointer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "KvPointer {{ log: {}, pos: {}, sz: {} }}",
            self.log, self.pos, self.sz
        )
    }
}

#[derive(Deserialize, Serialize, Clone, Debug)]
struct KvLog {
    command: KvCommand,
    time: SystemTime,
    key: String,
    value: Option<String>,
}

impl std::fmt::Display for KvLog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "KvLog {{ command: {}, key: {}, value: {:?} }}",
            self.command, self.key, self.value
        )
    }
}

impl KvStore {
    #[doc = "Sets a key-value pair in the store."]
    #[doc = ""]
    #[doc = "# Errors"]
    #[doc = ""]
    #[doc = "Returns an error if the write to the log file fails."]
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let kv_log = KvLog::build_from(KvCommand::Set, key.clone(), Some(value));
        self.write_log(&kv_log)?;
        self.add_index(key)?;
        self.start_compact()
    }

    #[doc = "Retrieves a value by key from the store."]
    #[doc = ""]
    #[doc = "# Errors"]
    #[doc = ""]
    #[doc = "Returns an error if the read from the log file fails."]
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        if self.flag {
            self.buffer.flush()?;
            self.flag = false;
        }
        match self.read_log(&key) {
            Ok(log) => {
                println!("{}", log.value.as_ref().unwrap_or(&String::new()));
                Ok(log.value)
            }
            Err(_) => {
                println!("Key not found");
                Ok(None)
            }
        }
    }

    #[doc = "Removes a key-value pair from the store."]
    #[doc = ""]
    #[doc = "# Errors"]
    #[doc = ""]
    #[doc = "Returns an error if the key does not exist or if the write fails."]
    pub fn remove(&mut self, key: String) -> Result<()> {
        if self.map.contains_key(&key) {
            let kv_log = KvLog::build_from(KvCommand::Remove, key.clone(), None);
            self.write_log(&kv_log)?;
            self.delete_index(key)?;
            self.start_compact()
        } else {
            println!("Key not found");
            Err(KvError::Other)
        }
    }

    #[doc = "Opens or creates a new key-value store at the given path."]
    #[doc = ""]
    #[doc = "The store is backed by a log-structured file system where each"]
    #[doc = "key-value pair is appended to an append-only log file."]
    #[doc = ""]
    #[doc = "# Errors"]
    #[doc = ""]
    #[doc = "Returns an error if the directory cannot be created or if"]
    #[doc = "a log file cannot be opened for writing."]
    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let mut path = path.into();
        path.push(LOG_FILE_DIR);
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
        kvs.map = kvs.start_build_index()?;
        kvs.buffer = BufWriter::new(
            OpenOptions::new()
                .append(true)
                .open(number_convert_to_log_path(&path, kvs.active.log))?,
        );
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
            return Err(KvError::Other);
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
                .ok_or(KvError::Other)?
                .to_str()
                .ok_or(KvError::Other)?;
            if !(file.contains("._") || file.contains(".DS_")) {
                let log_num: u64 = file.parse().map_err(|_| KvError::Other)?;
                dir_files.push((log_num, entry));
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
            let file = &e.1;
            let reader = BufReader::new(File::open(file)?);
            let log_iter = serde_json::Deserializer::from_reader(reader)
                .into_iter::<KvLog>()
                .flatten();
            for e in log_iter {
                let sz = get_size(&e)?;
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
            }
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
                        let sz = get_size(&e)?;
                        let pointer = KvPointer { log, pos, sz };
                        if let Some(expected) = self.map.get(&e.key)
                            && pointer == *expected
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

fn get_size(log: &KvLog) -> Result<u64> {
    Ok(serde_json::to_vec(log)?.len() as u64 + 1)
}

fn directory_initial(dir: &PathBuf) -> Result<()> {
    if !dir.is_dir() {
        DirBuilder::new().create(dir)?;
        File::create(number_convert_to_log_path(dir, 0))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_set_and_get() {
        let temp_dir = TempDir::new().unwrap();
        let mut store = KvStore::open(temp_dir.path()).unwrap();

        store.set("key1".to_string(), "value1".to_string()).unwrap();
        store.set("key2".to_string(), "value2".to_string()).unwrap();

        let result = store.get("key1".to_string()).unwrap();
        assert_eq!(result, Some("value1".to_string()));

        let result = store.get("key2".to_string()).unwrap();
        assert_eq!(result, Some("value2".to_string()));
    }

    #[test]
    fn test_remove() {
        let temp_dir = TempDir::new().unwrap();
        let mut store = KvStore::open(temp_dir.path()).unwrap();

        store.set("key1".to_string(), "value1".to_string()).unwrap();
        assert_eq!(store.get("key1".to_string()).unwrap(), Some("value1".to_string()));

        store.remove("key1".to_string()).unwrap();
        assert_eq!(store.get("key1".to_string()).unwrap(), None);
    }

    #[test]
    fn test_get_nonexistent_key() {
        let temp_dir = TempDir::new().unwrap();
        let mut store = KvStore::open(temp_dir.path()).unwrap();

        let result = store.get("nonexistent".to_string()).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_remove_nonexistent_key() {
        let temp_dir = TempDir::new().unwrap();
        let mut store = KvStore::open(temp_dir.path()).unwrap();

        let result = store.remove("nonexistent".to_string());
        assert!(result.is_err());
    }

    #[test]
    fn test_update_existing_key() {
        let temp_dir = TempDir::new().unwrap();
        let mut store = KvStore::open(temp_dir.path()).unwrap();

        store.set("key1".to_string(), "value1".to_string()).unwrap();
        assert_eq!(store.get("key1".to_string()).unwrap(), Some("value1".to_string()));

        store.set("key1".to_string(), "value2".to_string()).unwrap();
        assert_eq!(store.get("key1".to_string()).unwrap(), Some("value2".to_string()));
    }

    #[test]
    fn test_empty_value() {
        let temp_dir = TempDir::new().unwrap();
        let mut store = KvStore::open(temp_dir.path()).unwrap();

        store.set("key1".to_string(), "".to_string()).unwrap();
        assert_eq!(store.get("key1".to_string()).unwrap(), Some("".to_string()));
    }

    #[test]
    fn test_kv_pointer_display() {
        let pointer = KvPointer {
            log: 1,
            pos: 100,
            sz: 50,
        };
        assert_eq!(format!("{}", pointer), "KvPointer { log: 1, pos: 100, sz: 50 }");
    }

    #[test]
    fn test_kv_command_display() {
        assert_eq!(format!("{}", KvCommand::Set), "Set");
        assert_eq!(format!("{}", KvCommand::Remove), "Remove");
    }
}
