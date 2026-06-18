use clap::Parser;
use kvs::kvs::KvCommand;
use kvs::protocol::{self, KvStream};
use kvs::{Result, logger};
use log::debug;
use std::io::{BufRead, BufReader, Write};
use std::net::{IpAddr, Ipv4Addr, SocketAddr, TcpStream};
use std::process;

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

fn main() -> Result<()> {
    logger::init()?;
    let args = Args::parse();
    debug!(
        "program: kvs-server, version: {}, address: {}",
        env!("CARGO_PKG_VERSION"),
        args.addr
    );

    let mut tcp_stream = TcpStream::connect(args.addr)?;

    let stream = match args.command {
        KvCommand::Set if let Some(value) = args.value => protocol::create_protocol_stream(
            &KvStream::build_from(args.command, args.key, Some(value)),
        ),
        KvCommand::Get | KvCommand::Rm if args.value.is_none() => {
            protocol::create_protocol_stream(&KvStream::build_from(args.command, args.key, None))
        }
        _ => process::exit(1),
    }?;

    debug!("{stream:?}");
    tcp_stream.write_all(&stream)?;
    tcp_stream.write_all(b"\r\n")?;

    let stream = BufReader::new(&tcp_stream);
    if let Some(Ok(kv_stream)) = stream.lines().next() {
        println!(
            "{}",
            protocol::parse_protocol_stream(kv_stream.as_bytes())?.key
        );
    }

    Ok(())
}
