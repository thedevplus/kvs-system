use log::{Level, LevelFilter, Log, Metadata, Record};
use crate::Result;

struct SimpleLog;

impl Log for SimpleLog {
    fn enabled(&self, metadata: &Metadata) -> bool {
        metadata.level() <= Level::Debug
    }

    fn log(&self, record: &Record) {
        if self.enabled(record.metadata()) {
            println!("{} - {}", record.level(), record.args())
        }
    }

    fn flush(&self) {}
}

static LOGGER: SimpleLog = SimpleLog;

pub fn init() -> Result<()> {
    log::set_logger(&LOGGER).map(|()| log::set_max_level(LevelFilter::Debug))?;
    Ok(())
}