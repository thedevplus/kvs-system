use std::thread;

use crate::Result;
pub trait ThreadPool {
    fn new(threads: u32) -> Result<impl ThreadPool>;

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        thread::spawn(|| job());
    }
}

pub struct NaiveThreadPool {}

impl ThreadPool for NaiveThreadPool {
    fn new(threads: u32) -> Result<impl ThreadPool> {
        Ok(Self {})
    }
}
