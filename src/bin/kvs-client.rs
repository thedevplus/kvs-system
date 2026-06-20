#![deny(clippy::all)]
#![deny(missing_docs)]

//! Key-value store client.
//!
//! This client connects to a kvs-server over TCP and supports three operations:
//! - `set <key> <value>`: Store a key-value pair
//! - `get <key>`: Retrieve the value for a key
//! - `rm <key>`: Remove a key-value pair

use clap::Parser;
use kvs::Result;
use kvs::kvs::KvCommand;
use kvs::protocol::{self, KvStream, StreamCommand};
use log::{LevelFilter, info};
use std::io::{BufRead, BufReader, Write};
use std::net::{IpAddr, Ipv4Addr, SocketAddr, TcpStream};
use std::process;

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
    stderrlog::new()
        .module(module_path!())
        .show_module_names(true)
        .verbosity(LevelFilter::Info)
        .init()?;
    let args = Args::parse();

    // Connect to the server
    let mut tcp_stream = TcpStream::connect(args.addr)?;
    info!("Connect to {}: ok.", args.addr);

    // Build the protocol stream based on the command
    let stream = match args.command {
        KvCommand::Set if let Some(value) = args.value => protocol::create_protocol_stream(
            &KvStream::build_from(StreamCommand::St, args.key, Some(value)),
        ),
        KvCommand::Get if args.value.is_none() => protocol::create_protocol_stream(
            &KvStream::build_from(StreamCommand::Gt, args.key, None),
        ),
        KvCommand::Rm if args.value.is_none() => protocol::create_protocol_stream(
            &KvStream::build_from(StreamCommand::Rm, args.key, None),
        ),
        _ => process::exit(1),
    }?;

    // Send the request to the server
    tcp_stream.write_all(&stream)?;
    tcp_stream.write_all(b"\n")?;

    // Read and process the server response
    let stream = BufReader::new(&tcp_stream);
    if let Some(Ok(kv_stream)) = stream.lines().next() {
        let stream = protocol::parse_protocol_stream(kv_stream.as_bytes())?;
        match stream.command {
            // Set/Remove success: no output
            StreamCommand::St | StreamCommand::Rm => (),
            // Get success: print the value
            StreamCommand::Gt => println!("{}", stream.key),
            // Key not found: print the message
            StreamCommand::Gn => println!("{}", stream.key),
            // Remove error: print error and exit
            StreamCommand::Re => {
                eprintln!("{}", stream.key);
                process::exit(1);
            }
            _ => process::exit(1),
        }
    }

    Ok(())
}
