use crate::Result;
use crate::error::KvError;
use crossbeam_channel::{self, Sender};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::{panic, process};

pub trait ThreadPool {
    fn new(threads: u32) -> Result<impl ThreadPool>;

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static;

    fn join(&mut self) -> Result<()> {
        Ok(())
    }

    fn shutdown(&self) {}
}

pub struct NaiveThreadPool {}

impl ThreadPool for NaiveThreadPool {
    fn new(_threads: u32) -> Result<impl ThreadPool> {
        Ok(Self {})
    }

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        thread::spawn(job);
    }
}

pub struct SharedQueueThreadPool {
    send: Option<Sender<ThreadPoolMessage>>,
    work: Option<Vec<JoinHandle<()>>>,
}

enum ThreadPoolMessage {
    RunJob(Box<dyn FnOnce() + Send + 'static>),
    Shutdown,
}

impl ThreadPool for SharedQueueThreadPool {
    fn new(threads: u32) -> Result<impl ThreadPool> {
        let (sender, receiver) = crossbeam_channel::bounded(threads as usize);
        let mut thread_pool = Self {
            send: Some(sender),
            work: Some(Vec::new()),
        };
        let shutdown = Arc::new(Mutex::new(false));
        (0..threads).for_each(|_| {
            let receiver = receiver.clone();
            let shutdown = Arc::clone(&shutdown);
            if let Some(work) = thread_pool.work.as_mut() {
                work.push(thread::spawn(move || {
                    while !*shutdown.lock().unwrap() {
                        if panic::catch_unwind(|| match receiver.recv() {
                            Ok(ThreadPoolMessage::RunJob(thread)) => {
                                thread();
                            }
                            _ => {
                                *shutdown.lock().unwrap() = true;
                            }
                        })
                        .is_err()
                        {
                            continue;
                        };
                    }
                }))
            };
        });

        Ok(thread_pool)
    }

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        if let Some(sender) = &self.send
            && sender
                .send(ThreadPoolMessage::RunJob(Box::new(job)))
                .is_err()
        {
            eprintln!("Threads try to shutdown and process is aborted");
        };
    }

    fn join(&mut self) -> Result<()> {
        let handle = self.work.take().ok_or(KvError::Thread)?;
        for e in handle {
            if e.join().is_ok() {}
        }

        Ok(())
    }

    fn shutdown(&self) {
        let mut busy = false;
        let Some(work) = self.work.as_ref() else {
            process::exit(1)
        };
        for _ in 0..work.len() {
            while let Some(sender) = &self.send
                && sender.try_send(ThreadPoolMessage::Shutdown).is_err()
            {
                busy = true;
            }
            if busy {
                break;
            }
        }
    }
}

impl Drop for SharedQueueThreadPool {
    fn drop(&mut self) {
        let sender = self.send.take();
        if let Some(sender) = sender {
            drop(sender);
        };
        if let Some(handles) = self.work.as_mut() {
            while let Some(handle) = handles.pop() {
                let _ = handle.join();
            }
        }
    }
}

pub struct RayonThreadPool {
    threadpool: rayon::ThreadPool,
}

impl ThreadPool for RayonThreadPool {
    fn new(threads: u32) -> Result<impl ThreadPool> {
        let threadpool = rayon::ThreadPoolBuilder::new()
            .num_threads(threads as usize)
            .build()?;
        Ok(RayonThreadPool { threadpool })
    }

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.threadpool.spawn(job);
    }
}
