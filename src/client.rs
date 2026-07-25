use crate::Result;
use crate::kvs::KvCommand;
use crate::protocol::{self, KvStream, StreamCommand};
use crate::thread_pool::ThreadPool;
use std::io::{BufRead, BufReader, Write};
use std::net::SocketAddr;
use std::net::TcpStream;
use std::process;

pub struct KvsClient<T>
where
    T: ThreadPool,
{
    thread: T,
}

impl<T> KvsClient<T>
where
    T: ThreadPool,
{
    pub fn new(thread: T) -> Result<Self> {
        Ok(Self { thread })
    }

    pub fn run(
        &self,
        command: KvCommand,
        key: String,
        value: Option<String>,
        address: SocketAddr,
    ) -> Result<()> {
        self.thread.spawn(move || {
            let _ = deal(command, key, value, address);
        });

        Ok(())
    }
}

fn deal(command: KvCommand, key: String, value: Option<String>, address: SocketAddr) -> Result<()> {
    let mut tcp_stream = TcpStream::connect(address)?;
    let stream = match command {
        KvCommand::Set if let Some(value) = value => protocol::create_protocol_stream(
            &KvStream::build_from(StreamCommand::St, key, Some(value)),
        ),
        KvCommand::Get if value.is_none() => {
            protocol::create_protocol_stream(&KvStream::build_from(StreamCommand::Gt, key, None))
        }
        KvCommand::Rm if value.is_none() => {
            protocol::create_protocol_stream(&KvStream::build_from(StreamCommand::Rm, key, None))
        }
        _ => process::exit(1),
    }?;

    // Send the request to the server
    tcp_stream.write_all(&stream)?;
    tcp_stream.write_all(b"\n")?;

    // Read and process the server response
    let stream = BufReader::new(&tcp_stream);
    if let Some(Ok(kv_stream)) = stream.lines().next() {
        let stream = protocol::parse_protocol_stream(kv_stream.as_bytes())?;
        match stream.command {
            // Set/Remove success: no output
            StreamCommand::St | StreamCommand::Rm => (),
            // Get success: print the value
            StreamCommand::Gt => println!("{}", stream.key),
            // Key not found: print the message
            StreamCommand::Gn => println!("{}", stream.key),
            // Remove error: print error and exit
            StreamCommand::Re => {
                eprintln!("{}", stream.key);
                process::exit(1);
            }
            _ => process::exit(1),
        }
    }

    Ok(())
}
