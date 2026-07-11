use crate::Result;
use crossbeam_channel::{self, Sender};
use std::panic;
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};

pub trait ThreadPool {
    fn new(threads: u32) -> Result<impl ThreadPool>;

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static;

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
    send: Sender<ThreadPoolMessage>,
    work: Vec<JoinHandle<()>>,
}

enum ThreadPoolMessage {
    RunJob(Box<dyn FnOnce() + Send + 'static>),
    Shutdown,
}

impl ThreadPool for SharedQueueThreadPool {
    fn new(threads: u32) -> Result<impl ThreadPool> {
        let (sender, receiver) = crossbeam_channel::bounded(threads as usize);
        let mut thread_pool = Self {
            send: sender,
            work: Vec::new(),
        };
        let shutdown = Arc::new(Mutex::new(false));
        (0..threads).for_each(|_| {
            let receiver = receiver.clone();
            let shutdown = Arc::clone(&shutdown);
            thread_pool.work.push(thread::spawn(move || {
                while !*shutdown.lock().unwrap() {
                    let Ok(_) = panic::catch_unwind(|| match receiver.recv() {
                        Ok(ThreadPoolMessage::RunJob(thread)) => {
                            thread();
                        }
                        Ok(ThreadPoolMessage::Shutdown) => {
                            *shutdown.lock().unwrap() = true;
                        }
                        Err(_) => (),
                    }) else {
                        continue;
                    };
                }
            }));
        });

        Ok(thread_pool)
    }

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.send
            .send(ThreadPoolMessage::RunJob(Box::new(job)))
            .unwrap();
    }

    fn shutdown(&self) {
        self.send.send(ThreadPoolMessage::Shutdown).unwrap();
    }
}

impl Drop for SharedQueueThreadPool {
    fn drop(&mut self) {
        let (sender, _) = crossbeam_channel::unbounded();
        self.send = sender;
        while let Some(handle) = self.work.pop() {
            handle.join().unwrap();
        }
    }
}
