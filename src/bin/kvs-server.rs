use clap::{Parser, ValueEnum};
use kvs::{Result, logger};
use std::net::{IpAddr, Ipv4Addr, SocketAddr, TcpListener};
use log::debug;

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
        "version: {}, address: {}, engine: {}",
        env!("CARGO_PKG_VERSION"),
        args.addr,
        match args.engine {
            Some(Engine::Kvs) => "kvs",
            Some(Engine::Sled) => "sled",
            _ => "kvs",
        }
    );

    let listener = TcpListener::bind(args.addr)?;
    for stream in listener.incoming() {
        debug!("connetion created, status: {:?}", stream?);
        loop {
            
        }
    }

    Ok(())
}
