/// For kvs.rs configuration
/// File extension for log files
pub const LOG_FILE_EXT: &str = "log";
/// Maximum size per log file
pub const LOG_FILE_SIZE: u64 = 1024 * 1024;
/// Threshold for triggering compaction
pub const LOG_UNCOMPACT: u64 = 1000;
pub const LOG_UNCOMPACT_SLEEP: u64 = LOG_UNCOMPACT * 10;
pub const CMD_EXE_RATIO: u64 = 2;

/// For client.rs configuration
/// 
pub const TRY_SEND: u8 = 1;
pub const TRY_SEND_MAX: u8 = 3;
pub const SHUTDOWN_SERVER_CMD: &str = "019fcd63-9284-70a5-8c5d-c3e0f1cbc573";

/// For thread_pool.rs configuration
/// 
pub const CHANNEL_BOUND: usize = 500000000;

/// For kvs-server.rs configuration
/// Directory name for storing log files
pub const LOG_FILE_DIR: &str = "database";