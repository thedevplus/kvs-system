use clap::Parser;
use kvs::KvStore;
use kvs::Result;
use std::env;
use std::process;

#[derive(Parser)]
#[command(version)]
struct Args {
    command: Option<String>,
    key: Option<String>,
    value: Option<String>,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let mut kvs = KvStore::open(&env::current_dir()?)?;

    match args.command.unwrap().as_ref() {
        "-V" => println!("{}", env!("CARGO_PKG_VERSION")),
        "set" => {
            if let Some(value) = args.value {
                kvs.set(args.key.unwrap(), value)?;
            } else {
                process::exit(1);
            }
        }
        "get" => {
            if args.value.is_some() {
                process::exit(1);
                //return Err(kvs::error::KvError::Other);
            } else {
                let Ok(Some(_)) = kvs.get(args.key.unwrap()) else {
                    process::exit(0);
                };
            }
        }
        "rm" => {
            if args.value.is_some() {
                process::exit(1);
            } else {
                let Ok(_) = kvs.remove(args.key.unwrap()) else {
                    process::exit(1);
                };
            }
        }
        _ => process::exit(1),
    }

    Ok(())
}
