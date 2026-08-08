use crate::Result;
use crate::error::KvError;
use crossbeam_channel::{self, Sender};
use std::panic;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering::{Relaxed, SeqCst};
use std::thread::{self, JoinHandle};
use crate::config::CHANNEL_BOUND;

pub trait ThreadPool {
    fn new(threads: u32) -> Result<Self>
    where
        Self: Sized;

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
    fn new(_threads: u32) -> Result<Self> {
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
    fn new(threads: u32) -> Result<Self> {
        let (sender, receiver) = crossbeam_channel::bounded(CHANNEL_BOUND);
        let mut thread_pool = Self {
            send: Some(sender),
            work: Some(Vec::new()),
        };
        let shutdown = Arc::new(AtomicBool::new(false));
        (0..threads).for_each(|_| {
            let receiver = receiver.clone();
            let shutdown = Arc::clone(&shutdown);
            if let Some(work) = thread_pool.work.as_mut() {
                work.push(thread::spawn(move || {
                    while !shutdown.load(Relaxed) {
                        if panic::catch_unwind(|| match receiver.recv() {
                            Ok(ThreadPoolMessage::RunJob(thread)) => {
                                thread();
                            }
                            _ => {
                                shutdown.store(true, SeqCst);
                            }
                        })
                        .is_err()
                        {
                            eprintln!("Status: thread panicked");
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
        if let Some(sender) = self.send.as_ref()
            && sender
                .send(ThreadPoolMessage::RunJob(Box::new(job)))
                .is_err()
        {
            eprintln!("Threads try to shutdown and process is aborted");
        };
    }

    fn join(&mut self) -> Result<()> {
        let sender = self.send.take().ok_or(KvError::Thread)?;
        drop(sender);
        let handle = self.work.take().ok_or(KvError::Thread)?;
        for e in handle {
            if e.join().is_ok() {}
        }

        Ok(())
    }

    fn shutdown(&self) {
        if let Some(work) = self.work.as_ref() {
            for _ in 0..work.len() {
                if let Some(sender) = self.send.as_ref()
                    && sender.send(ThreadPoolMessage::Shutdown).is_ok()
                {}
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
            while let Some(handle) = handles.pop()
                && handle.join().is_ok()
            {}
        }

        println!("Thread droppppp success");
    }
}

pub struct RayonThreadPool {
    threadpool: rayon::ThreadPool,
}

impl ThreadPool for RayonThreadPool {
    fn new(threads: u32) -> Result<Self> {
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
