use std::thread::{self, JoinHandle};
use crate::Result;
use crossbeam_channel::{self, Sender};
pub trait ThreadPool {
    fn new(threads: u32) -> Result<impl ThreadPool>;

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static;
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
    (0..threads).into_iter().for_each(|_| {
        let r = receiver.clone();
        thread_pool.work.push(thread::spawn(move || {
        while let Ok(ThreadPoolMessage::RunJob(thread)) = r.recv() {
            thread();
        }
    }))});

    Ok(thread_pool)
    }

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static
    {
        self.send.send(ThreadPoolMessage::RunJob(Box::new(job))).unwrap();
    }
}
