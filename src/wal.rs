use crate::error::{Error, Result};
use crate::memtable::MemTable;
use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

/// Record type tags for WAL entries.
const RECORD_PUT: u8 = 1;
const RECORD_DELETE: u8 = 2;

/// A decoded WAL record: (record_type, key, value).
type WalRecord = (u8, Vec<u8>, Vec<u8>);

/// Write-Ahead Log for crash recovery.
///
/// Every mutation (put or delete) is appended to the WAL *before* the
/// corresponding memtable write. On crash recovery the WAL is replayed to
/// reconstruct the memtable that was in memory at the time of the crash.
///
/// # On-Disk Format
///
/// Each record is:
/// ```text
/// [crc32:  4 bytes, little-endian]   CRC32 of (type ++ key_len ++ key ++ value_len ++ value)
/// [type:   1 byte ]                  1 = Put, 2 = Delete
/// [key_len:   4 bytes, LE]
/// [key:    key_len bytes]
/// [value_len: 4 bytes, LE]           (0 for Delete records)
/// [value:  value_len bytes]
/// ```
///
/// CRC32 is computed over the payload (everything after the checksum field)
/// so that any corruption -- partial writes, bit flips -- is detected during
/// recovery.
pub struct Wal {
    writer: BufWriter<File>,
    path: PathBuf,
}

impl Wal {
    /// Opens (or creates) a WAL file at the given path.
    pub fn open(path: &Path) -> Result<Self> {
        let file = OpenOptions::new().create(true).append(true).open(path)?;

        Ok(Wal {
            writer: BufWriter::new(file),
            path: path.to_path_buf(),
        })
    }

    /// Appends a `put` record and flushes it to stable storage.
    pub fn write_put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.write_record(RECORD_PUT, key, value)
    }

    /// Appends a `delete` record and flushes it to stable storage.
    pub fn write_delete(&mut self, key: &[u8]) -> Result<()> {
        self.write_record(RECORD_DELETE, key, &[])
    }

    /// Ensures all buffered data has been written to the OS.
    pub fn sync(&mut self) -> Result<()> {
        self.writer.flush()?;
        self.writer.get_ref().sync_all()?;
        Ok(())
    }

    /// Replays a WAL file into a fresh memtable.
    ///
    /// Corrupted records at the tail are silently skipped (they indicate an
    /// incomplete write before a crash). Any corruption in the middle of the
    /// file is also treated as the end of valid data, since we cannot trust
    /// records after a corrupted one.
    pub fn recover(path: &Path) -> Result<MemTable> {
        let mut memtable = MemTable::new();

        if !path.exists() {
            return Ok(memtable);
        }

        let file = File::open(path)?;
        let mut reader = BufReader::new(file);

        loop {
            match Self::read_record(&mut reader) {
                Ok(Some((record_type, key, value))) => match record_type {
                    RECORD_PUT => memtable.put(key, value),
                    RECORD_DELETE => memtable.delete(key),
                    _ => {
                        // Unknown record type -- treat as corruption at the tail.
                        break;
                    }
                },
                Ok(None) => break, // EOF
                Err(_) => break,   // Corruption at tail
            }
        }

        Ok(memtable)
    }

    /// Removes the WAL file from disk. Called after a successful memtable flush.
    pub fn discard(path: &Path) -> Result<()> {
        if path.exists() {
            fs::remove_file(path)?;
        }
        Ok(())
    }

    /// Returns the file system path of this WAL.
    pub fn path(&self) -> &Path {
        &self.path
    }

    // ── internal helpers ──────────────────────────────────────────────

    fn write_record(&mut self, record_type: u8, key: &[u8], value: &[u8]) -> Result<()> {
        let payload = Self::encode_payload(record_type, key, value);
        let crc = crc32fast::hash(&payload);

        self.writer.write_all(&crc.to_le_bytes())?;
        self.writer.write_all(&payload)?;
        self.writer.flush()?;
        Ok(())
    }

    fn encode_payload(record_type: u8, key: &[u8], value: &[u8]) -> Vec<u8> {
        let mut buf = Vec::with_capacity(1 + 4 + key.len() + 4 + value.len());
        buf.push(record_type);
        buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
        buf.extend_from_slice(key);
        buf.extend_from_slice(&(value.len() as u32).to_le_bytes());
        buf.extend_from_slice(value);
        buf
    }

    fn read_record(reader: &mut BufReader<File>) -> Result<Option<WalRecord>> {
        // Read CRC
        let mut crc_buf = [0u8; 4];
        match reader.read_exact(&mut crc_buf) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                return Ok(None);
            }
            Err(e) => return Err(Error::Io(e)),
        }
        let expected_crc = u32::from_le_bytes(crc_buf);

        // Read record type
        let mut type_buf = [0u8; 1];
        reader.read_exact(&mut type_buf)?;
        let record_type = type_buf[0];

        // Read key
        let mut len_buf = [0u8; 4];
        reader.read_exact(&mut len_buf)?;
        let key_len = u32::from_le_bytes(len_buf) as usize;
        let mut key = vec![0u8; key_len];
        reader.read_exact(&mut key)?;

        // Read value
        reader.read_exact(&mut len_buf)?;
        let value_len = u32::from_le_bytes(len_buf) as usize;
        let mut value = vec![0u8; value_len];
        reader.read_exact(&mut value)?;

        // Verify CRC
        let payload = Self::encode_payload(record_type, &key, &value);
        let actual_crc = crc32fast::hash(&payload);
        if actual_crc != expected_crc {
            return Err(Error::Corruption(format!(
                "WAL CRC mismatch: expected {expected_crc:#010x}, got {actual_crc:#010x}"
            )));
        }

        Ok(Some((record_type, key, value)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_wal_write_and_recover() {
        let dir = TempDir::new().unwrap();
        let wal_path = dir.path().join("test.wal");

        // Write some records
        {
            let mut wal = Wal::open(&wal_path).unwrap();
            wal.write_put(b"key1", b"value1").unwrap();
            wal.write_put(b"key2", b"value2").unwrap();
            wal.write_delete(b"key1").unwrap();
            wal.sync().unwrap();
        }

        // Recover
        let memtable = Wal::recover(&wal_path).unwrap();
        assert_eq!(
            memtable.get(b"key1"),
            Some(&crate::memtable::Value::Tombstone)
        );
        assert_eq!(
            memtable.get(b"key2"),
            Some(&crate::memtable::Value::Put(b"value2".to_vec()))
        );
    }

    #[test]
    fn test_wal_recover_empty() {
        let dir = TempDir::new().unwrap();
        let wal_path = dir.path().join("nonexistent.wal");
        let memtable = Wal::recover(&wal_path).unwrap();
        assert!(memtable.is_empty());
    }

    #[test]
    fn test_wal_recover_truncated() {
        let dir = TempDir::new().unwrap();
        let wal_path = dir.path().join("test.wal");

        // Write a valid record then append garbage
        {
            let mut wal = Wal::open(&wal_path).unwrap();
            wal.write_put(b"key1", b"value1").unwrap();
            wal.sync().unwrap();
        }

        // Append some garbage bytes to simulate a partial write
        {
            let mut f = OpenOptions::new().append(true).open(&wal_path).unwrap();
            f.write_all(&[0xff, 0xfe, 0xfd]).unwrap();
        }

        // Should recover the valid record and skip the garbage
        let memtable = Wal::recover(&wal_path).unwrap();
        assert_eq!(
            memtable.get(b"key1"),
            Some(&crate::memtable::Value::Put(b"value1".to_vec()))
        );
    }

    #[test]
    fn test_wal_discard() {
        let dir = TempDir::new().unwrap();
        let wal_path = dir.path().join("test.wal");

        {
            let mut wal = Wal::open(&wal_path).unwrap();
            wal.write_put(b"key", b"val").unwrap();
            wal.sync().unwrap();
        }

        assert!(wal_path.exists());
        Wal::discard(&wal_path).unwrap();
        assert!(!wal_path.exists());
    }
}
