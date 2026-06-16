use clap::{Parser, ValueEnum};
use kvs::Result;
use log::{Level, LevelFilter, Log, Metadata, Record, debug};
use std::net::{IpAddr, Ipv4Addr, SocketAddr, TcpListener};

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

struct SimpleLog;

impl Log for SimpleLog {
    fn enabled(&self, metadata: &Metadata) -> bool {
        metadata.level() <= Level::Debug
    }

    fn log(&self, record: &Record) {
        if self.enabled(record.metadata()) {
            println!("{} - {}", record.level(), record.args())
        }
    }

    fn flush(&self) {}
}

static LOGGER: SimpleLog = SimpleLog;

fn init() -> Result<()> {
    log::set_logger(&LOGGER).map(|()| log::set_max_level(LevelFilter::Debug))?;
    Ok(())
}

fn main() -> Result<()> {
    init()?;
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
