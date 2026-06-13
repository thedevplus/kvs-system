use clap::{Parser, ValueEnum};
use kvs::Result;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};

#[derive(Parser)]
#[command(version, name="kvs server", about = "The server for key/value storage", long_about = None)]
struct Args {
    /// Listen to IPv4 or IPv6 with a custom port
    #[arg(long, default_value_t = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 4000))]
    addr: SocketAddr,
    /// Choose kvs or sled engine to run
    #[arg(long)]
    engine: Option<Engine>,
}

#[derive(Clone, ValueEnum)]
enum Engine {
    Kvs,
    Sled,
}

fn main() -> Result<()> {
    let args = Args::parse();
    Ok(())
}
