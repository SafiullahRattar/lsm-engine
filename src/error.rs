use thiserror::Error;

/// All errors that can occur within the LSM storage engine.
#[derive(Error, Debug)]
pub enum Error {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Corruption detected: {0}")]
    Corruption(String),

    #[error("Invalid SSTable: {0}")]
    InvalidSsTable(String),

    #[error("WAL error: {0}")]
    Wal(String),

    #[error("Manifest error: {0}")]
    Manifest(String),

    #[error("Key not found")]
    KeyNotFound,
}

pub type Result<T> = std::result::Result<T, Error>;
