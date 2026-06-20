use clap::{Parser, ValueEnum};
use kvs::error::KvError;
use kvs::protocol::{KvStream, StreamCommand};
use kvs::sled::SledKvsEngine;
use kvs::{KvStore, KvsEngine, Result, protocol};
use log::{LevelFilter, debug, info};
use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::net::{IpAddr, Ipv4Addr, SocketAddr, TcpListener};
use std::path::PathBuf;
use std::process;

/// Directory name for storing log files
const LOG_FILE_DIR: &str = "database";

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
    stderrlog::new()
        .module(module_path!())
        .show_module_names(true)
        .verbosity(LevelFilter::Debug)
        .init()?;
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
    let mut path = PathBuf::from("./");
    path.push(LOG_FILE_DIR);
    let mut engine_exist = (false, false);
    for file in path.read_dir().unwrap_or(fs::read_dir("./")?).flatten() {
        let file = file.path().to_str().ok_or(KvError::File)?.to_owned();
        if file.contains("db") {
            engine_exist.0 = true;
            if engine_exist.1 {
                break;
            }
        } else if file.contains(".log") {
            engine_exist.1 = true;
            if engine_exist.0 {
                break;
            }
        }
    }
    let mut kvs: Box<dyn KvsEngine> = match args.engine {
        Some(Engine::Kvs) | None if !engine_exist.0 => Box::new(KvStore::open(&path)?),
        Some(Engine::Sled) | None if !engine_exist.1 => Box::new(SledKvsEngine::open(&path)?),
        _ => {
            eprintln!("Data was previously persisted with a different engine");
            process::exit(1);
        }
    };

    while let Some(tcp_stream) = listener.incoming().map_while(|x| x.ok()).next() {
        info!("TCP connected: ok.");
        let stream = BufReader::new(&tcp_stream);
        let mut iter = stream.lines();
        let mut tcp_stream = tcp_stream.try_clone()?;
        while let Some(Ok(stream)) = iter.next() {
            let v = stream.as_bytes().to_owned();
            if let Ok(kv_stream) = protocol::parse_protocol_stream(&v) {
                let stream = match kv_stream.command {
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
                tcp_stream.write_all(&stream)?;
                tcp_stream.write_all(b"\n")?;
                // info!("Respone: ok.");
            }
        }
    }

    Ok(())
}
