//! # lsm-engine
//!
//! A Log-Structured Merge-tree (LSM-tree) storage engine written in Rust.
//!
//! LSM-trees are the foundation of many modern storage systems including
//! LevelDB, RocksDB, Apache Cassandra, and ScyllaDB. They trade read
//! performance for write throughput by buffering writes in memory and
//! periodically flushing sorted runs to disk.
//!
//! ## Quick Start
//!
//! ```no_run
//! use lsm_engine::{Db, DbOptions};
//! use std::path::Path;
//!
//! let mut db = Db::open_default(Path::new("/tmp/my_db")).unwrap();
//!
//! // Write
//! db.put(b"name", b"Alice").unwrap();
//!
//! // Read
//! let value = db.get(b"name").unwrap();
//! assert_eq!(value, Some(b"Alice".to_vec()));
//!
//! // Delete
//! db.delete(b"name").unwrap();
//! assert_eq!(db.get(b"name").unwrap(), None);
//!
//! // Range scan
//! db.put(b"user:1", b"Alice").unwrap();
//! db.put(b"user:2", b"Bob").unwrap();
//! db.put(b"user:3", b"Carol").unwrap();
//! let users = db.scan(b"user:1", b"user:3").unwrap();
//! // Returns [("user:1", "Alice"), ("user:2", "Bob")]
//! ```
//!
//! ## Components
//!
//! - **Memtable**: In-memory `BTreeMap` that absorbs writes before flushing.
//! - **WAL**: Write-ahead log with CRC32 checksums for crash recovery.
//! - **SSTable**: Sorted String Table with block-based layout, index, and bloom filter.
//! - **Bloom filter**: Probabilistic filter to skip SSTables during point lookups.
//! - **Compaction**: Size-tiered strategy to merge SSTables and reclaim space.
//! - **Manifest**: Tracks the set of live SSTables across restarts.

pub mod bloom;
pub mod compaction;
pub mod db;
pub mod error;
pub mod manifest;
pub mod memtable;
pub mod sstable;
pub mod wal;

// Re-export the main public types at the crate root for convenience.
pub use db::{Db, DbOptions};
pub use error::{Error, Result};
