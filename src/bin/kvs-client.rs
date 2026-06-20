use clap::Parser;
use kvs::Result;
use kvs::kvs::KvCommand;
use kvs::protocol::{self, KvStream, StreamCommand};
use log::{LevelFilter, info};
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
    stderrlog::new()
        .module(module_path!())
        .show_module_names(true)
        .verbosity(LevelFilter::Info)
        .init()?;
    let args = Args::parse();

    let mut tcp_stream = TcpStream::connect(args.addr)?;
    info!("Connect to {}: ok.", args.addr);

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

    tcp_stream.write_all(&stream)?;
    tcp_stream.write_all(b"\n")?;

    let stream = BufReader::new(&tcp_stream);
    if let Some(Ok(kv_stream)) = stream.lines().next() {
        let stream = protocol::parse_protocol_stream(kv_stream.as_bytes())?;
        match stream.command {
            StreamCommand::St | StreamCommand::Rm => (),
            StreamCommand::Gt => println!("{}", stream.key),
            StreamCommand::Gn => println!("{}", stream.key),
            StreamCommand::Re => {
                eprintln!("{}", stream.key);
                process::exit(1);
            }
            _ => process::exit(1),
        }
    }

    Ok(())
}
