use crate::protocol::{self, KvStream, StreamCommand};
use crate::{KvError, KvsEngine, Result, thread_pool::ThreadPool};
use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering::{Relaxed, SeqCst};

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

    pub fn work(&self, listener: TcpListener) -> Result<()> {
        let exit_status = Arc::new(AtomicBool::new(false));

        while !exit_status.load(Relaxed)
            && let Some(Ok(tcpstream)) = listener.incoming().next()
        {
            let exit_status_thread = Arc::clone(&exit_status);
            let tcp_stream = tcpstream.try_clone()?;
            let stream = BufReader::new(tcpstream);

            let kvs = self.store.clone();

            self.spawn(move || {
                if let Err(KvError::Thread) = deal(kvs, stream, tcp_stream) {
                    exit_status_thread.store(true, SeqCst);
                }
            });
        }

        Ok(())
    }

    pub fn spawn<F>(&self, job: F)
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
                    StreamCommand::St => {
                        if kvs
                            .set(
                                kv_stream.key.clone(),
                                kv_stream.value.ok_or(KvError::Network)?,
                            )
                            .is_ok()
                        {
                            protocol::create_protocol_stream(&KvStream::build_from(
                                StreamCommand::St,
                                String::new(),
                                None,
                            ))?
                        } else {
                            protocol::create_protocol_stream(&KvStream::build_from(
                                StreamCommand::Se,
                                String::new(),
                                None,
                            ))?
                        }
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
                        _ => protocol::create_protocol_stream(&KvStream::build_from(
                            StreamCommand::Ge,
                            String::new(),
                            None,
                        ))?,
                    },
                    // Remove command: delete key-value pair
                    StreamCommand::Rm => {
                        if kvs.remove(kv_stream.key).is_ok() {
                            protocol::create_protocol_stream(&KvStream::build_from(
                                StreamCommand::Rm,
                                String::new(),
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
                    StreamCommand::Sd => {
                        return Err(KvError::Thread);
                    }
                    _ => Vec::new(),
                };
            // Send response back to client
            tcp_stream.write_all(&stream)?;
            tcp_stream.write_all(b"\n")?;
            // info!("Respone: ok.");
        }
    }

    Ok(())
}
