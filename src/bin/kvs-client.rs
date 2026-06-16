use clap::Parser;
use kvs::{KvStore, KvsEngine, Result};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::process;
use kvs::kvs::KvCommand;

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
    let args = Args::parse();
    let mut kvs = KvStore::open("./databse")?;

    match args.command {
        KvCommand::Set => {
            if let Some(value) = args.value {
                kvs.set(args.key, value)?;
            } else {
                process::exit(1);
            }
        }
        KvCommand::Get => {
            if args.value.is_some() {
                process::exit(1);
                //return Err(kvs::error::KvError::Other);
            } else {
                let Ok(Some(_)) = kvs.get(args.key) else {
                    process::exit(0);
                };
            }
        }
        KvCommand::Rm => {
            if args.value.is_some() {
                process::exit(1);
            } else {
                let Ok(_) = kvs.remove(args.key) else {
                    process::exit(1);
                };
            }
        }
    }

    Ok(())
}
