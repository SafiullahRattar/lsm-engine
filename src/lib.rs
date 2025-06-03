//! # lsm-engine
//!
//! A Log-Structured Merge-tree (LSM-tree) storage engine written in Rust.

pub mod error;
pub mod memtable;
pub mod wal;

pub use error::{Error, Result};
