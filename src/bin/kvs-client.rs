#![deny(clippy::all)]
#![deny(missing_docs)]

//! Key-value store client.
//!
//! This client connects to a kvs-server over TCP and supports three operations:
//! - `set <key> <value>`: Store a key-value pair
//! - `get <key>`: Retrieve the value for a key
//! - `rm <key>`: Remove a key-value pair

use clap::Parser;
use kvs::kvs::KvCommand;
use kvs::thread_pool::SharedQueueThreadPool;
use kvs::{KvsClient, Result, ThreadPool};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
//use log::{LevelFilter, info};

/// Command-line arguments for the key-value store client.
#[derive(Parser)]
#[command(version, name="kvs client", about = "A key-value store client", long_about = None)]
struct Args {
    /// Command to execute
    command: KvCommand,
    /// Key to operate on
    key: String,
    /// Value to set (required for set command)
    value: Option<String>,
    /// Server address to connect to
    #[arg(long, default_value_t = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 4000))]
    addr: SocketAddr,
}

/// Entry point for the key-value store client.
///
/// This function:
/// 1. Parses command-line arguments
/// 2. Connects to the server via TCP
/// 3. Sends the command request
/// 4. Receives and processes the response
fn main() -> Result<()> {
    let args = Args::parse();

    let threads = SharedQueueThreadPool::new(1)?;
    let client = KvsClient::new(threads)?;
    client.run(args.command, args.key, args.value, args.addr, None)
}
