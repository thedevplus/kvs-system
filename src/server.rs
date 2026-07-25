use crate::protocol::{self, KvStream, StreamCommand};
use crate::{KvError, KvsEngine, Result, thread_pool::ThreadPool};
use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};

pub struct KvsServer<S, T>
where
    S: KvsEngine,
    T: ThreadPool,
{
    store: S,
    thread: T,
}

impl<S, T> KvsServer<S, T>
where
    S: KvsEngine,
    T: ThreadPool,
{
    pub fn new(store: S, thread: T) -> Result<Self> {
        Ok(KvsServer { store, thread })
    }

    pub fn worker(&self, listener: TcpListener) -> Result<()> {
        while let Some(Ok(tcpstream)) = listener.incoming().next() {
            let tcp_stream = tcpstream.try_clone()?;
            let stream = BufReader::new(tcpstream);

            let kvs = self.store.clone();
            self.spawn(move || {
                let _ = deal(kvs, stream, tcp_stream);
            });
        }
        Ok(())
    }

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.thread.spawn(job);
    }
}

fn deal<U: KvsEngine>(
    kvs: U,
    mut stream: BufReader<TcpStream>,
    mut tcp_stream: TcpStream,
) -> Result<()> {
    // Process each command from the client
    let stream = &mut stream;
    while let Some(Ok(stream)) = stream.lines().next() {
        let v = stream.as_bytes().to_owned();
        if let Ok(kv_stream) = protocol::parse_protocol_stream(&v) {
            // Handle command and prepare response
            let stream =
                match kv_stream.command {
                    // Set command: store key-value pair
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
                    // Get command: retrieve value by key
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
                    // Remove command: delete key-value pair
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
            // Send response back to client
            tcp_stream.write_all(&stream)?;
            tcp_stream.write_all(b"\n")?;
            // info!("Respone: ok.");
        }
    }

    Ok(())
}
