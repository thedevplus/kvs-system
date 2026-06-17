use clap::Parser;
use kvs::{Result, logger};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::process;
use kvs::kvs::KvCommand;
use kvs::protocol::{self, KvStream};
use log::debug;

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

    let stream = match args.command {
        KvCommand::Set if let Some(value) = args.value => {
            protocol::create_protocol_stream(&KvStream::build_from(args.command, args.key, Some(value)))
        }
        KvCommand::Get | KvCommand::Rm if args.value.is_none() => {
            protocol::create_protocol_stream(&KvStream::build_from(args.command, args.key, None))
        }
        _ => process::exit(1),
    }?;

    debug!("{stream:?}");

    Ok(())
}
