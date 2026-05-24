# Log-Structured Key-Value Store

[![Rust](https://img.shields.io/badge/Rust-1.70+-orange.svg)](https://www.rust-lang.org)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

> A high-performance, log-structured key-value store implemented in pure Rust,
> inspired by the Bitcask paper and built as part of the
> [Talent Plan](https://github.com/pingcap/talent-plan) Rust track.

## Features

- **Log-Structured Storage** - Append-only write optimization for sequential I/O
- **Automatic Compaction** - Background garbage collection to reclaim disk space
- **O(1) Read** - In-memory index via HashMap for fast key lookups
- **Crash Recovery** - Durable writes with fsync semantics
- **Clean Error Handling** - Idiomatic `Result<T, E>` pattern with custom error types

## Architecture

### Write Path

```
set("key", "value")
        │
        ▼
┌───────────────────┐
│  Serialize Log    │  serde_json → binary
└────────┬──────────┘
         │
         ▼
┌───────────────────┐
│  Append to Log    │  O(1) sequential write
└────────┬──────────┘
         │
         ▼
┌───────────────────┐
│  Update Index     │  HashMap[key] = pointer
└───────────────────┘
```

### Read Path

```
get("key")
        │
        ▼
┌───────────────────┐
│  Lookup Index     │  HashMap.get(key) → Pointer
└────────┬──────────┘
         │
         ▼
┌───────────────────┐
│  Seek & Read      │  File.seek(pos).read(sz)
└───────────────────┘
```

## Quick Start

### Rust API

```rust
use kvs::KvStore;

let mut store = KvStore::open("./data")?;
store.set("name".to_string(), "Alice".to_string())?;

if let Some(value) = store.get("name".to_string())? {
    println!("Found: {}", value);
}

store.remove("name".to_string())?;
```

### CLI

```bash
# Set a key-value pair
cargo run --bin kvs -- set name Alice

# Get a value by key
cargo run --bin kvs -- get name

# Remove a key
cargo run --bin kvs -- rm name
```

## Key Design Decisions

| Decision | Trade-off | Rationale |
|----------|-----------|-----------|
| Append-only log | Write amplification during compaction | Maximize read performance |
| In-memory index | Memory bounded by key count | O(1) lookup, fast startup |
| JSON serialization | Human readable, not optimal space | Simplicity for learning |
| Bitcask-style compaction | Background I/O overhead | Keeps active data compact |

## What I Learned

- **Rust Ownership Model** - Lifetimes, borrowing, zero-cost abstractions
- **Error Handling** - `Result<T, E>` pattern, custom error types with `thiserror`
- **Serialization** - `serde` derive macros, performance considerations
- **File I/O** - `BufReader`/`BufWriter`, `Seek` semantics
- **System Design** - Log-structured storage, compaction strategies
- **Testing** - Unit tests, integration tests, tempfile for test isolation

## Project Structure

```
kvs/
├── src/
│   ├── lib.rs           # Core key-value store implementation
│   ├── error.rs         # Custom error types
│   └── bin/kvs.rs       # CLI entry point
├── tests/
│   └── tests.rs         # Integration tests
├── Cargo.toml
└── README.md
```

## Testing

```bash
# Run all tests (unit + integration)
cargo test

# Run with output
cargo test -- --nocapture

# Run clippy for code quality
cargo clippy

# Format code
cargo fmt
```

## Related Projects

- [pingcap/talent-plan](https://github.com/pingcap/talent-plan) - Original course that inspired this project
- [Bitcask - A Log-Structured Hash Table for Fast Key/Value Data](https://riak.com/assets/bitcask-intro.pdf) - Paper that inspired the design

## License

MIT
