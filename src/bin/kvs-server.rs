use clap::{Parser, ValueEnum};
use kvs::error::KvError;
use kvs::kvs::KvCommand;
use kvs::protocol::KvStream;
use kvs::{KvStore, KvsEngine, Result};
use kvs::{logger, protocol};
use log::{debug, info};
use std::io::{BufRead, BufReader, Write};
use std::net::{IpAddr, Ipv4Addr, SocketAddr, TcpListener};
use std::process;

#[derive(Parser)]
#[command(version, name="kvs server", about = "A key-value store server", long_about = None)]
struct Args {
    /// Socket address to listen on
    #[arg(long, default_value_t = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 4000))]
    addr: SocketAddr,
    /// Storage engine to use (kvs or sled)
    #[arg(long)]
    engine: Option<Engine>,
}

#[derive(Clone, ValueEnum)]
enum Engine {
    Kvs,
    Sled,
}

fn main() -> Result<()> {
    logger::init()?;
    let args = Args::parse();
    debug!(
        "program: kvs-server, version: {}, address: {}, engine: {}",
        env!("CARGO_PKG_VERSION"),
        args.addr,
        match args.engine {
            Some(Engine::Kvs) => "kvs",
            Some(Engine::Sled) => "sled",
            _ => "kvs",
        }
    );

    let listener = TcpListener::bind(args.addr)?;
    let mut kvs = KvStore::open("./")?;

    while let Some(tcp_stream) = listener.incoming().map_while(|x| x.ok()).next() {
        info!("TCP connected: ok.");
        debug!("connetion created, status: {:?}", tcp_stream);
        let stream = BufReader::new(&tcp_stream);
        let mut iter = stream.lines();
        let mut tcp_stream = tcp_stream.try_clone()?;
        while let Some(Ok(stream)) = iter.next() {
            debug!("Stream read, status: {:?}", stream.as_bytes());
            let v = stream.as_bytes().to_owned();
            if let Ok(kv_stream) = protocol::parse_protocol_stream(&v) {
                let stream = match kv_stream.command {
                    KvCommand::Set => {
                        if kvs
                            .set(kv_stream.key, kv_stream.value.ok_or(KvError::Network)?)
                            .is_ok()
                        {
                            protocol::create_protocol_stream(&KvStream::build_from(
                                KvCommand::Get,
                                "Status: set ok.".to_string(),
                                None,
                            ))?
                        } else {
                            process::exit(1);
                        }
                    }
                    KvCommand::Get => {
                        if let Ok(Some(value)) = kvs.get(kv_stream.key) {
                            protocol::create_protocol_stream(&KvStream::build_from(
                                KvCommand::Get,
                                value,
                                None,
                            ))?
                        } else {
                            protocol::create_protocol_stream(&KvStream::build_from(
                                KvCommand::Get,
                                "Status: key not found.".to_string(),
                                None,
                            ))?
                        }
                    }
                    KvCommand::Rm => {
                        if kvs.remove(kv_stream.key).is_ok() {
                            protocol::create_protocol_stream(&KvStream::build_from(
                                KvCommand::Get,
                                "Status: remove ok.".to_string(),
                                None,
                            ))?
                        } else {
                            process::exit(1);
                        }
                    }
                };
                tcp_stream.write_all(&stream)?;
                tcp_stream.write_all(b"\r\n")?;
                info!("Respone: ok.");
            }
        }
    }

    Ok(())
}
