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
use kvs::protocol::{KvStream, StreamCommand};
use kvs::thread_pool::{SharedQueueThreadPool, ThreadPool};
use kvs::{Engine, KvStore, Result, SledKvsEngine, protocol};
use log::{LevelFilter, debug, info};
use num_cpus;
use std::io::{BufRead, BufReader, Write};
use std::net::{IpAddr, Ipv4Addr, SocketAddr, TcpListener, TcpStream};
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

    let listener = TcpListener::bind(args.addr)?;

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

    // Initialize the appropriate storage engine
    let kvs = match args.engine {
        Some(EngineType::Kvs) | None if !engine_exist.0 => Engine::Kvs(KvStore::open(&path)?),
        Some(EngineType::Sled) | None if !engine_exist.1 => {
            Engine::Sled(SledKvsEngine::open(&path)?)
        }
        _ => {
            process::exit(1);
        }
    };

    let cpu_num = num_cpus::get();
    if cpu_num < 2 {
        eprintln!("Your hardware currently is not available for running server process.");
        process::exit(1);
    }
    let workers = SharedQueueThreadPool::new(cpu_num as u32)?;

    debug!(
        "program: kvs-server, version: {}, address: {}, engine: {}, threads: {}",
        env!("CARGO_PKG_VERSION"),
        args.addr,
        match kvs {
            Engine::Kvs(_) => "kvs",
            Engine::Sled(_) => "sled",
        },
        cpu_num
    );

    // Main server loop: accept and handle TCP connections
    while let Some(Ok(tcp_stream)) = listener.incoming().next() {
        info!("TCP connected: ok.");
        // server_worker(&kvs, &tcp_stream)?;
        let kvs = kvs.clone();
        workers.spawn(move || {
            let _ = server_worker(&kvs, &tcp_stream);
        });
    }

    Ok(())
}

fn server_worker(kvs: &Engine, tcp_stream: &TcpStream) -> Result<()> {
    let stream = BufReader::new(tcp_stream);
    let mut iter = stream.lines();
    let mut tcp_stream = tcp_stream.try_clone()?;

    // Process each command from the client
    while let Some(Ok(stream)) = iter.next() {
        let v = stream.as_bytes().to_owned();
        if let Ok(kv_stream) = protocol::parse_protocol_stream(&v) {
            // Handle command and prepare response
            let stream =
                match kv_stream.command {
                    // Set command: store key-value pair
                    StreamCommand::St
                        if kvs
                            .set(
                                kv_stream.key.clone(),
                                kv_stream.value.ok_or(KvError::Network)?,
                            )
                            .is_ok() =>
                    {
                        protocol::create_protocol_stream(&KvStream::build_from(
                            StreamCommand::Rm,
                            "".to_string(),
                            None,
                        ))?
                    }
                    // Get command: retrieve value by key
                    StreamCommand::Gt => match kvs.get(kv_stream.key) {
                        Ok(Some(value)) => protocol::create_protocol_stream(
                            &KvStream::build_from(StreamCommand::Gt, value, None),
                        )?,
                        Ok(None) => protocol::create_protocol_stream(&KvStream::build_from(
                            StreamCommand::Gn,
                            "Key not found".to_string(),
                            None,
                        ))?,
                        _ => {
                            tcp_stream.write_all(b"\n")?;
                            continue;
                        }
                    },
                    // Remove command: delete key-value pair
                    StreamCommand::Rm => {
                        if kvs.remove(kv_stream.key).is_ok() {
                            protocol::create_protocol_stream(&KvStream::build_from(
                                StreamCommand::Rm,
                                "".to_string(),
                                None,
                            ))?
                        } else {
                            protocol::create_protocol_stream(&KvStream::build_from(
                                StreamCommand::Re,
                                "Key not found".to_string(),
                                None,
                            ))?
                        }
                    }
                    _ => {
                        tcp_stream.write_all(b"\n")?;
                        continue;
                    }
                };
            // Send response back to client
            tcp_stream.write_all(&stream)?;
            tcp_stream.write_all(b"\n")?;
            // info!("Respone: ok.");
        }
    }
    Ok(())
}
