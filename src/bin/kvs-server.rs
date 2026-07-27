#![deny(clippy::all)]
#![deny(missing_docs)]

//! Key-value store server.
//!
//! This server listens on a TCP socket and handles client requests for
//! key-value operations (Set, Get, Remove). It supports two storage engines:
//! - `kvs`: A custom log-structured key-value store
//! - `sled`: An embedded database using the sled library
//!

use clap::{Parser, ValueEnum};
use kvs::error::KvError;
use kvs::thread_pool::{SharedQueueThreadPool, ThreadPool};
use kvs::{KvStore, KvsServer, Result, SledKvsEngine};
use log::{LevelFilter, debug};
use std::net::{IpAddr, Ipv4Addr, SocketAddr, TcpListener};
use std::path::PathBuf;
use std::{fs, process};

/// Directory name for storing log files
const LOG_FILE_DIR: &str = "database";

/// The argument(s) for running
#[derive(Parser)]
#[command(version, name="kvs server", about = "A key-value store server", long_about = None)]
struct Args {
    /// Socket address to listen on
    #[arg(long, default_value_t = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 4000))]
    addr: SocketAddr,
    /// Storage engine to use (kvs or sled)
    #[arg(long)]
    engine: Option<EngineType>,
}

/// Storage engine type selection.
#[derive(Clone, ValueEnum)]
enum EngineType {
    /// log-structured key-value store
    Kvs,
    /// Sled embedded database
    Sled,
}

enum DefineKvsServer<T>
where
    T: ThreadPool,
{
    Kvs(KvsServer<KvStore, T>),
    Sled(KvsServer<SledKvsEngine, T>),
}

impl<T> DefineKvsServer<T>
where
    T: ThreadPool,
{
    fn work(&self, listener: TcpListener) -> Result<()> {
        match self {
            DefineKvsServer::Kvs(kvs) => kvs.work(listener),
            DefineKvsServer::Sled(sled) => sled.work(listener),
        }
    }
}

/// Entry point for the key-value store server.
///
/// This function:
/// 1. Parses command-line arguments
/// 2. Initializes logging
/// 3. Detects and opens the appropriate storage engine
/// 4. Listens for TCP connections and handles client requests
fn main() -> Result<()> {
    stderrlog::new()
        .module(module_path!())
        .module("kvs")
        .show_module_names(true)
        .verbosity(LevelFilter::Debug)
        .init()?;
    let args = Args::parse();

    // Prepare database directory path
    let mut path = PathBuf::from("./");
    path.push(LOG_FILE_DIR);

    // Detect existing storage engine by checking file types
    // (db files indicate sled, .log files indicate kvs)
    let mut engine_exist = (false, false);
    for file in path.read_dir().unwrap_or(fs::read_dir("./")?).flatten() {
        let file = file.path().to_str().ok_or(KvError::File)?.to_owned();
        if file.contains("db") {
            engine_exist.0 = true; // sled engine exists
            if engine_exist.1 {
                break;
            }
        } else if file.contains(".log") {
            engine_exist.1 = true; // kvs engine exists
            if engine_exist.0 {
                break;
            }
        }
    }

    let cpu_num = num_cpus::get();
    if cpu_num < 2 {
        eprintln!("Your hardware currently is not available for running server process.");
        process::exit(1);
    }
    let workers = SharedQueueThreadPool::new(cpu_num as u32)?;

    // Initialize the appropriate storage engine
    let kvs = match args.engine {
        Some(EngineType::Kvs) | None if !engine_exist.0 => {
            DefineKvsServer::Kvs(KvsServer::new(KvStore::open(path)?, workers)?)
        }
        Some(EngineType::Sled) | None if !engine_exist.1 => {
            DefineKvsServer::Sled(KvsServer::new(SledKvsEngine::open(path)?, workers)?)
        }
        _ => process::exit(1),
    };

    debug!(
        "program: kvs-server, version: {}, address: {}, engine: {}, threads: {}",
        env!("CARGO_PKG_VERSION"),
        args.addr,
        match kvs {
            DefineKvsServer::Kvs(_) => "kvs",
            DefineKvsServer::Sled(_) => "sled",
        },
        cpu_num
    );

    // Main server loop: accept and handle TCP connections
    let listener = TcpListener::bind(args.addr)?;
    kvs.work(listener)
}
