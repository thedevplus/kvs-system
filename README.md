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
- [ ] **Milestone 3**: Custom TCP networking stack (Handling sticky/half packets) — *In Progress*
- [ ] **Milestone 4**: Thread-pool concurrency engine (`Send + Sync` optimization).
- [ ] **Milestone 5**: Full asynchronous migration via Tokio runtime.

## Usage

```rust
use kvs::KvStore;

let mut store = KvStore::open("./folder")?;
store.set("key".to_string(), "value".to_string())?;
let value = store.get("key".to_string())?;
```

```bash
kvs set key value
kvs get key
kvs rm key
```

## Ongoing Optimizations

- **Error Handling**: Improving error handling and consistency.
- **Documentation**: Expanding inline rustdoc comments for core APIs.

## License

MIT