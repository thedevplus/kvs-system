# kvs (`thedevplus`)

A log-structured key-value store implemented in Rust.

[![Rust](https://img.shields.io/badge/Rust-1.95+-orange.svg)](https://www.rust-lang.org)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

> Designed and implemented based on the industrial-grade technical specifications and test suites of the [PingCAP Talent Plan](https://github.com/pingcap/talent-plan) (Practical Networked Applications in Rust).

---

## Overview

This project implements a log-structured key-value store inspired by the Bitcask design pattern. It demonstrates core systems programming concepts in Rust.

## Features

- **Append-only Log**: Sequential writes for optimal I/O performance
- **In-memory Index**: O(1) read complexity via HashMap
- **Automatic Compaction**: Background garbage collection
- **CLI Interface**: Interactive command-line tool

## Project Roadmap & Progression

- [x] **Milestone 1**: In-memory prototype with `clap` CLI parser.
- [x] **Milestone 2**: Log-structured engine (Bitcask pattern) with compaction. *(Fully implemented in one sprint; 100% tests passed)*
- [x] **Milestone 3**: Custom TCP networking stack with line-based framing. *(Completed and Verified with 5 rounds of Criterion benchmarks)*
- [ ] **Milestone 4**: Thread-pool concurrency engine (`Send + Sync` optimization). — *In Progress*
- [ ] **Milestone 5**: Full asynchronous migration via Tokio runtime.

## Benchmark Results

### Optimizations Applied

Performance comparison after implementing read performance optimizations in KvStore engine.

| Optimization | Description | Impact |
|--------------|-------------|--------|
| **BufReader Caching** | Maintain a dedicated BufReader instance for each log file instead of creating new ones on each read operation. | Reduces file I/O overhead and improves read consistency. |

### Benchmark Results (5 Rounds Summary)

| Operation | Engine | Round 1 (median) | Round 5 (median) | Change |
|-----------|--------|------------------|------------------|--------|
| **Write** (set-rm-set) | KvStore | 203.20 ms | 337.46 ms | +66% |
| **Write** (set-rm-set) | Sled | 290.59 ms | 666.80 ms | +129% |
| **Read** (get-order) | KvStore | 59.33 ms | 69.83 ms | +18% |
| **Read** (get-order) | Sled | 18.18 ms | 20.54 ms | +13% |
| **Read** (get-disorder) | KvStore | 58.71 ms | 69.43 ms | +18% |
| **Read** (get-disorder) | Sled | 15.79 ms | 19.19 ms | +22% |

### Key Findings

1. **Write Performance**: KvStore consistently outperforms Sled by ~1.5-2x in write operations (203-337 ms vs 291-667 ms).
2. **Read Performance**: KvStore's ordered and disordered read performance remains comparable (~59-70 ms), while Sled maintains ~3-4x faster reads.
3. **Stability**: Sled demonstrates superior stability with fewer performance outliers across all test rounds.
4. **Scalability**: Both engines show increased latency in later rounds, likely due to accumulated data size affecting disk I/O.

### Benchmark Output

<details>
<summary>Round 1 - Full Benchmark Results</summary>

```bash
Benchmarking write_group/kvs/set-rm-set
Benchmarking write_group/kvs/set-rm-set: Warming up for 3.0000 s
Warning: Unable to complete 100 samples in 5.0s. You may wish to increase target time to 19.5s, or reduce sample count to 20.
Benchmarking write_group/kvs/set-rm-set: Collecting 100 samples in estimated 19.508 s (100 iterations)
Benchmarking write_group/kvs/set-rm-set: Analyzing
write_group/kvs/set-rm-set
                        time:   [165.05 ms 203.20 ms 244.59 ms]
Found 22 outliers among 100 measurements (22.00%)
  22 (22.00%) high severe

Benchmarking write_group/sled/set-rm-set
Benchmarking write_group/sled/set-rm-set: Warming up for 3.0000 s
Warning: Unable to complete 100 samples in 5.0s. You may wish to increase target time to 33.2s, or reduce sample count to 10.
Benchmarking write_group/sled/set-rm-set: Collecting 100 samples in estimated 33.206 s (100 iterations)
Benchmarking write_group/sled/set-rm-set: Analyzing
write_group/sled/set-rm-set
                        time:   [286.07 ms 290.59 ms 295.43 ms]
Found 1 outliers among 100 measurements (1.00%)
  1 (1.00%) high severe

Benchmarking read_group/kvs/get-order
Benchmarking read_group/kvs/get-order: Warming up for 3.0000 s
Warning: Unable to complete 100 samples in 5.0s. You may wish to increase target time to 6.2s, or reduce sample count to 80.
Benchmarking read_group/kvs/get-order: Collecting 100 samples in estimated 6.1754 s (100 iterations)
Benchmarking read_group/kvs/get-order: Analyzing
read_group/kvs/get-order
                        time:   [59.093 ms 59.330 ms 59.586 ms]
Found 9 outliers among 100 measurements (9.00%)
  7 (7.00%) high mild
  2 (2.00%) high severe

Benchmarking read_group/sled/get-order
Benchmarking read_group/sled/get-order: Warming up for 3.0000 s
Benchmarking read_group/sled/get-order: Collecting 100 samples in estimated 5.7966 s (300 iterations)
Benchmarking read_group/sled/get-order: Analyzing
read_group/sled/get-order
                        time:   [18.044 ms 18.183 ms 18.332 ms]
Found 2 outliers among 100 measurements (2.00%)
  2 (2.00%) high mild

Benchmarking read_group/kvs/get-disorder
Benchmarking read_group/kvs/get-disorder: Warming up for 3.0000 s
Warning: Unable to complete 100 samples in 5.0s. You may wish to increase target time to 5.9s, or reduce sample count to 80.
Benchmarking read_group/kvs/get-disorder: Collecting 100 samples in estimated 5.9250 s (100 iterations)
Benchmarking read_group/kvs/get-disorder: Analyzing
read_group/kvs/get-disorder
                        time:   [58.377 ms 58.713 ms 59.070 ms]
Found 4 outliers among 100 measurements (4.00%)
  4 (4.00%) high mild

Benchmarking read_group/sled/get-disorder
Benchmarking read_group/sled/get-disorder: Warming up for 3.0000 s
Benchmarking read_group/sled/get-disorder: Collecting 100 samples in estimated 6.3688 s (400 iterations)
Benchmarking read_group/sled/get-disorder: Analyzing
read_group/sled/get-disorder
                        time:   [15.636 ms 15.789 ms 15.977 ms]
Found 11 outliers among 100 measurements (11.00%)
  7 (7.00%) high mild
  4 (4.00%) high severe
```

</details>

<details>
<summary>Round 5 - Full Benchmark Results</summary>

```bash
Benchmarking write_group/kvs/set-rm-set
Benchmarking write_group/kvs/set-rm-set: Warming up for 3.0000 s
Warning: Unable to complete 100 samples in 5.0s. You may wish to increase target time to 30.0s, or reduce sample count to 10.
Benchmarking write_group/kvs/set-rm-set: Collecting 100 samples in estimated 30.001 s (100 iterations)
Benchmarking write_group/kvs/set-rm-set: Analyzing
write_group/kvs/set-rm-set
                        time:   [230.51 ms 337.46 ms 488.04 ms]
                        change: [−20.102% +22.976% +90.847%] (p = 0.43 > 0.05)
                        No change in performance detected.
Found 21 outliers among 100 measurements (21.00%)
  1 (1.00%) high mild
  20 (20.00%) high severe

Benchmarking write_group/sled/set-rm-set
Benchmarking write_group/sled/set-rm-set: Warming up for 3.0000 s
Warning: Unable to complete 100 samples in 5.0s. You may wish to increase target time to 54.3s, or reduce sample count to 10.
Benchmarking write_group/sled/set-rm-set: Collecting 100 samples in estimated 54.284 s (100 iterations)
Benchmarking write_group/sled/set-rm-set: Analyzing
write_group/sled/set-rm-set
                        time:   [564.46 ms 666.80 ms 821.27 ms]
                        change: [−1.8367% +30.955% +74.057%] (p = 0.06 > 0.05)
                        No change in performance detected.
Found 10 outliers among 100 measurements (10.00%)
  1 (1.00%) high mild
  9 (9.00%) high severe

Benchmarking read_group/kvs/get-order
Benchmarking read_group/kvs/get-order: Warming up for 3.0000 s
Warning: Unable to complete 100 samples in 5.0s. You may wish to increase target time to 8.1s, or reduce sample count to 60.
Benchmarking read_group/kvs/get-order: Collecting 100 samples in estimated 8.0992 s (100 iterations)
Benchmarking read_group/kvs/get-order: Analyzing
read_group/kvs/get-order
                        time:   [68.984 ms 69.830 ms 70.746 ms]
                        change: [+2.7254% +4.1130% +5.6178%] (p = 0.00 < 0.05)
                        Performance has regressed.
Found 3 outliers among 100 measurements (3.00%)
  3 (3.00%) high mild

Benchmarking read_group/sled/get-order
Benchmarking read_group/sled/get-order: Warming up for 3.0000 s
Benchmarking read_group/sled/get-order: Collecting 100 samples in estimated 6.5839 s (300 iterations)
Benchmarking read_group/sled/get-order: Analyzing
read_group/sled/get-order
                        time:   [20.391 ms 20.538 ms 20.702 ms]
                        change: [−6.4812% −1.7898% +1.3303%] (p = 0.54 > 0.05)
                        No change in performance detected.
Found 14 outliers among 100 measurements (14.00%)
  5 (5.00%) high mild
  9 (9.00%) high severe

Benchmarking read_group/kvs/get-disorder
Benchmarking read_group/kvs/get-disorder: Warming up for 3.0000 s
Warning: Unable to complete 100 samples in 5.0s. You may wish to increase target time to 7.2s, or reduce sample count to 60.
Benchmarking read_group/kvs/get-disorder: Collecting 100 samples in estimated 7.2227 s (100 iterations)
Benchmarking read_group/kvs/get-disorder: Analyzing
read_group/kvs/get-disorder
                        time:   [69.105 ms 69.430 ms 69.781 ms]
                        change: [+5.5796% +6.5955% +7.5232%] (p = 0.00 < 0.05)
                        Performance has regressed.
Found 13 outliers among 100 measurements (13.00%)
  10 (10.00%) high mild
  3 (3.00%) high severe

Benchmarking read_group/sled/get-disorder
Benchmarking read_group/sled/get-disorder: Warming up for 3.0000 s
Benchmarking read_group/sled/get-disorder: Collecting 100 samples in estimated 6.0871 s (300 iterations)
Benchmarking read_group/sled/get-disorder: Analyzing
read_group/sled/get-disorder
                        time:   [19.060 ms 19.189 ms 19.326 ms]
                        change: [−0.0224% +4.2574% +7.4986%] (p = 0.02 < 0.05)
                        Change within noise threshold.
Found 12 outliers among 100 measurements (12.00%)
  10 (10.00%) high mild
  2 (2.00%) high severe
```

</details>

## Usage

### Server

Start the key-value store server:

```bash
# Start with default settings (kvs engine, 127.0.0.1:4000)
kvs-server

# Start with KvStore engine
kvs-server --engine kvs

# Start on a custom address
kvs-server --addr 192.168.1.100:4000

# Start with KvStore engine on custom address
kvs-server --engine kvs --addr 0.0.0.0:4000
```

**Server Options:**
- `--addr <SOCKET_ADDR>`: Socket address to listen on (default: `127.0.0.1:4000`)
- `--engine <ENGINE>`: Storage engine to use (`kvs` or `sled`, default: `kvs`)

**Storage Engines:**
- `kvs`: Custom log-structured key-value store (Bitcask pattern)
- `sled`: Embedded database using the sled library

### Client

Connect to the server and perform operations:

```bash
# Set a key-value pair
kvs-client set key value

# Set with custom server address
kvs-client set key value --addr 127.0.0.1:4000

# Get a value by key
kvs-client get key

# Remove a key
kvs-client rm key
```

**Client Options:**
- `<COMMAND>`: Operation to perform (`set`, `get`, or `rm`)
- `<KEY>`: Key to operate on
- `[VALUE]`: Value to set (required for `set` command)
- `--addr <SOCKET_ADDR>`: Server address to connect to (default: `127.0.0.1:4000`)

### Library API

Use as a Rust library:

```rust
use kvs::{KvStore, SledKvsEngine, KvsEngine, Result};

// Open a KvStore in current directory
let mut store = KvStore::open("./")?;
store.set("key".to_string(), "value".to_string())?;
let value = store.get("key".to_string())?; // Returns Option<String>
store.remove("key".to_string())?;

// Or use SledKvsEngine in current directory
let mut sled = SledKvsEngine::open("./")?;
sled.set("key".to_string(), "value".to_string())?;
let value = sled.get("key".to_string())?;
sled.remove("key".to_string())?;
```

### Configuration Constants

The following constants are defined in the source code and can be modified for customization:

**Log File Settings (`src/kvs.rs`):**
- `LOG_FILE_EXT`: File extension for log files (default: `"log"`)
- `LOG_FILE_SIZE`: Maximum size per log file before rotating (default: `1 MB` = 1024 * 1024 bytes)
- `LOG_UNCOMPACT`: Threshold of stale entries before triggering compaction (default: `1000`)

**Server Settings (`src/bin/kvs-server.rs`):**
- `LOG_FILE_DIR`: Directory name for storing log files (default: `"database"`)

**Example Customization:**

To change the maximum log file size to 10MB:
```rust
const LOG_FILE_SIZE: u64 = 10 * 1024 * 1024; // 10 MB
```

## Ongoing Optimizations

- **Write Performance Analysis**: The write time of Sled engine is roughly 1.5 times that of KvStore engine, but KvStore engine exhibits less stability, with significant performance fluctuations exceeding 20%. This is likely related to forced compaction during writes, leaving substantial room for optimization.

- **Read Performance Optimization**: The read time of KvStore engine is approximately 5-6 times that of Sled engine. While stability is comparable, there is still room for improvement. The slow read speed is likely because key/value pairs are not cached in memory—each read requires file I/O. Currently, only index pointers are kept in memory, representing a potential optimization direction.

## License

MIT