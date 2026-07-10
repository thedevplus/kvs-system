use crate::Result;
use crossbeam_channel::{self, Sender};
use std::panic;
use std::rc::Rc;
use std::sync::Mutex;
use std::thread::{self, JoinHandle};

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

pub enum ThreadPoolMessage {
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
        (0..threads).for_each(|_| {
            let receiver = receiver.clone();
            thread_pool.work.push(thread::spawn(move || {
                let shutdown = Rc::new(Mutex::new(false));
                while !*shutdown.lock().unwrap() {
                    let Ok(_) = panic::catch_unwind(|| {
                        if let Ok(ThreadPoolMessage::RunJob(thread)) = receiver.recv() {
                            thread();
                        } else {
                            *shutdown.lock().unwrap() = true;
                        }
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
