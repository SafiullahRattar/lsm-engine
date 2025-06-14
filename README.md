# lsm-engine

A Log-Structured Merge-tree (LSM-tree) storage engine written in Rust.

LSM-trees are the data structure behind many of the most widely used storage systems in the world: [LevelDB](https://github.com/google/leveldb), [RocksDB](https://rocksdb.org/), [Apache Cassandra](https://cassandra.apache.org/), [ScyllaDB](https://www.scylladb.com/), [CockroachDB](https://www.cockroachlabs.com/) (via Pebble), and many more. They trade read amplification for write throughput by buffering writes in memory and periodically flushing sorted runs to disk.

This project implements the core components of an LSM storage engine from scratch, with no dependencies beyond `thiserror` and `crc32fast`.

## Architecture

```
                 WRITE PATH                              READ PATH
                 ──────────                              ─────────
                  Client                                  Client
                    │                                       │
                    ▼                                       ▼
               ┌─────────┐                            ┌──────────┐
               │   WAL   │ ← append for durability    │ MemTable │ ← check newest data first
               └────┬────┘                            └────┬─────┘
                    │                                      │ miss
                    ▼                                      ▼
               ┌──────────┐                        ┌──────────────┐
               │ MemTable │ ← sorted in-memory     │ Bloom Filter │ ← skip if key absent
               └────┬─────┘   (BTreeMap)           └──────┬───────┘
                    │ threshold exceeded                   │ may contain
                    ▼                                      ▼
               ┌──────────┐                        ┌──────────────┐
               │ SSTable  │ ← flush sorted run     │   SSTable    │ ← binary search index,
               │  (disk)  │   to immutable file    │   (disk)     │   then binary search block
               └────┬─────┘                        └──────────────┘
                    │ too many tables
                    ▼
              ┌────────────┐
              │ Compaction │ ← merge SSTables,
              │            │   drop tombstones
              └────────────┘
```

## Features

- **Memtable** -- in-memory sorted storage backed by `BTreeMap`, with configurable size threshold for flushing
- **Write-Ahead Log (WAL)** -- append-only log with CRC32 checksums for crash recovery
- **SSTables** -- immutable on-disk sorted files with block-based layout (~4 KiB blocks), binary-searchable index, and per-table bloom filter
- **Bloom filters** -- implemented from scratch using double hashing (FNV-1a), ~0.82% false positive rate at 10 bits/key
- **Size-tiered compaction** -- merges SSTables when the count exceeds a threshold, with tombstone garbage collection
- **Manifest** -- tracks live SSTables across restarts using a simple append-only log
- **Range scans** -- merge iterators across memtable and all SSTables with proper deduplication
- **Crash recovery** -- WAL replay on startup reconstructs the in-flight memtable

## API

```rust
use lsm_engine::{Db, DbOptions};
use std::path::Path;

// Open (or create) a database
let mut db = Db::open_default(Path::new("/tmp/my_db"))?;

// Put
db.put(b"user:1", b"Alice")?;

// Get
let value = db.get(b"user:1")?;
assert_eq!(value, Some(b"Alice".to_vec()));

// Delete
db.delete(b"user:1")?;
assert_eq!(db.get(b"user:1")?, None);

// Range scan [start, end)
db.put(b"key:01", b"a")?;
db.put(b"key:02", b"b")?;
db.put(b"key:03", b"c")?;
let results = db.scan(b"key:01", b"key:03")?;
// results = [("key:01", "a"), ("key:02", "b")]
```

### Configuration

```rust
let options = DbOptions {
    memtable_size_threshold: 8 * 1024 * 1024, // 8 MiB (default: 4 MiB)
    ..Default::default()
};
let mut db = Db::open(Path::new("/tmp/my_db"), options)?;
```

## SSTable Binary Format

```
┌─────────────────────────────────────┐
│  Data Block 0                       │
│   [num_entries: u32]                │
│   [offsets: u32 * num_entries]      │
│   [entries: (key_len, key,          │
│              val_len, val) ...]     │
│   [crc32: u32]                      │
├─────────────────────────────────────┤
│  Data Block 1 ...                   │
├─────────────────────────────────────┤
│  Data Block N                       │
├─────────────────────────────────────┤
│  Index Block                        │
│   [num_blocks: u32]                 │
│   for each block:                   │
│     [first_key_len: u32]            │
│     [first_key: bytes]              │
│     [block_offset: u64]             │
│     [block_length: u64]             │
├─────────────────────────────────────┤
│  Bloom Filter Block                 │
│   [bit_vector: bytes]               │
│   [num_hashes: u32]                 │
├─────────────────────────────────────┤
│  Footer (40 bytes)                  │
│   [index_offset:  u64]              │
│   [index_length:  u64]              │
│   [bloom_offset:  u64]              │
│   [bloom_length:  u64]              │
│   [magic: "LSMT"]                   │
│   [version: u32]                    │
└─────────────────────────────────────┘
```

## Design Decisions

### Why BTreeMap for the memtable?

A skip list (as used in LevelDB/RocksDB) would allow concurrent reads during writes. However, `BTreeMap` is:
- Part of the standard library -- no external dependencies
- Cache-friendly -- contiguous node layout vs. pointer-chasing in skip lists
- O(log n) for inserts, lookups, and ordered iteration
- Sufficient for a single-writer design

The trade-off is that readers must wait for writers. A production engine would use a read-write lock or lock-free skip list.

### Block size (4 KiB)

4 KiB aligns with typical OS page sizes and SSD page sizes, balancing:
- **Seek granularity**: smaller blocks mean less wasted I/O on point reads
- **Index overhead**: larger blocks mean fewer index entries
- **Compression potential**: larger blocks compress better (not yet implemented)

### Size-tiered compaction

| Property            | Size-Tiered     | Leveled         |
|---------------------|-----------------|-----------------|
| Write amplification | Lower           | Higher          |
| Space amplification | Higher          | Lower           |
| Read amplification  | Higher          | Lower           |
| Implementation      | Simpler         | More complex    |

Size-tiered compaction is simpler to implement correctly and favors write-heavy workloads. Leveled compaction (as in LevelDB) would be the natural next step for read-heavy workloads.

### WAL format

Each record is `[CRC32][type][key_len][key][value_len][value]`. CRC32 covers everything after the checksum, detecting:
- Partial writes (crash mid-record)
- Bit flips on disk
- Truncated files

On recovery, the first corrupted record marks the end of valid data.

### Bloom filter parameters

- **10 bits per key** with **7 hash functions** gives ~0.82% false positive rate
- Uses **double hashing** (Kirsch & Mitzenmacher, 2006) to derive `k` hashes from two FNV-1a base hashes
- The enhanced double hashing formula `h1 + i*h2 + i*i` improves uniformity for higher-order hashes

## Benchmarks

Run benchmarks with:

```bash
cargo bench
```

| Benchmark                  | Throughput / Latency |
|----------------------------|---------------------|
| Sequential write (1K ops)  | ~1.2M ops/sec       |
| Point read (memtable hit)  | ~5M ops/sec         |
| Point read (SSTable hit)   | ~800K ops/sec       |
| Point read (miss)          | ~6M ops/sec         |
| Range scan (100 keys)      | ~15 us              |
| Range scan (1000 keys)     | ~120 us             |
| Write with flushes (10K)   | ~200K ops/sec       |

*Numbers are approximate and vary by hardware. Run `cargo bench` for your machine.*

## Build & Test

```bash
# Build
cargo build

# Run tests
cargo test

# Run with verbose output
cargo test -- --nocapture

# Run clippy lints
cargo clippy -- -D warnings

# Format code
cargo fmt

# Run the example
cargo run --example basic_usage

# Run benchmarks
cargo bench
```

## Project Structure

```
src/
├── lib.rs          Public API and module declarations
├── db.rs           Main DB struct -- coordinates all components
├── memtable.rs     In-memory sorted storage (BTreeMap)
├── wal.rs          Write-ahead log with CRC32 checksums
├── sstable/
│   ├── mod.rs      Module re-exports
│   ├── builder.rs  SSTable writer (data blocks + index + bloom + footer)
│   ├── reader.rs   SSTable reader (point lookup + range scan)
│   ├── block.rs    Data block format (sorted entries + binary search)
│   └── iterator.rs SSTable and merge iterators
├── compaction.rs   Size-tiered compaction strategy and execution
├── bloom.rs        Bloom filter (double hashing, FNV-1a)
├── manifest.rs     Tracks live SSTables across restarts
└── error.rs        Error types (thiserror)
```

## Comparison with Other Implementations

| Feature                | lsm-engine       | LevelDB          | RocksDB          |
|------------------------|-------------------|-------------------|-------------------|
| Language               | Rust              | C++               | C++               |
| Memtable               | BTreeMap          | Skip list         | Skip list / hash  |
| Compaction             | Size-tiered       | Leveled           | Multiple options  |
| Concurrency            | Single-threaded   | Single writer     | Multi-threaded    |
| Compression            | None              | Snappy            | Multiple          |
| Bloom filter           | FNV double hash   | Built-in          | Built-in          |
| WAL                    | CRC32             | CRC32             | CRC32             |
| Block cache            | None              | LRU               | LRU / Clock       |
| Column families        | No                | No                | Yes               |
| Transactions           | No                | No                | Yes               |

This engine intentionally omits features like compression, block caching, column families, and transactions to keep the codebase focused and readable. Each of these would be a natural extension.

## License

MIT
