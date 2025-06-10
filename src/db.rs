use crate::compaction::{self, CompactionStrategy, TOMBSTONE_VALUE};
use crate::error::Result;
use crate::manifest::Manifest;
use crate::memtable::{MemTable, Value};
use crate::sstable::builder::SsTableBuilder;
use crate::sstable::reader::SsTableReader;
use crate::wal::Wal;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

/// Configuration for the LSM database.
pub struct DbOptions {
    /// Maximum memtable size (in bytes) before flushing to an SSTable.
    /// Default: 4 MiB.
    pub memtable_size_threshold: usize,

    /// Compaction strategy parameters.
    pub compaction_strategy: CompactionStrategy,
}

impl Default for DbOptions {
    fn default() -> Self {
        DbOptions {
            memtable_size_threshold: 4 * 1024 * 1024, // 4 MiB
            compaction_strategy: CompactionStrategy::default(),
        }
    }
}

/// A Log-Structured Merge-tree storage engine.
///
/// # Architecture
///
/// ```text
///  Write path:                          Read path:
///  ──────────                          ─────────
///  Client                              Client
///    │                                   │
///    ▼                                   ▼
///  ┌──────┐                           ┌──────────┐
///  │ WAL  │  (crash recovery)         │ MemTable │ ◄── check first (newest data)
///  └──┬───┘                           └────┬─────┘
///     │                                    │ miss
///     ▼                                    ▼
///  ┌──────────┐                       ┌──────────────┐
///  │ MemTable │                       │ Bloom Filter │ ◄── skip SSTable if key absent
///  └──────────┘                       └──────┬───────┘
///     │ full                                 │ may contain
///     ▼                                      ▼
///  ┌──────────┐                       ┌──────────┐
///  │ SSTable  │ (flush to disk)       │ SSTable  │ ◄── binary search index + block
///  └──────────┘                       └──────────┘
///     │ too many
///     ▼
///  ┌────────────┐
///  │ Compaction │ (merge SSTables)
///  └────────────┘
/// ```
///
/// # Concurrency
///
/// This implementation is single-threaded. All operations hold exclusive
/// access through `&mut self`. A production engine would use a read-write
/// lock on the memtable and background threads for flushing/compaction.
pub struct Db {
    dir: PathBuf,
    memtable: MemTable,
    wal: Wal,
    manifest: Manifest,
    sstables: Vec<SsTableReader>,
    options: DbOptions,
    next_sst_id: u64,
}

impl Db {
    /// Opens or creates a database in the given directory.
    ///
    /// On startup:
    /// 1. Replays the manifest to discover live SSTables.
    /// 2. Opens each SSTable.
    /// 3. Replays the WAL to recover any memtable state from before a crash.
    pub fn open(dir: &Path, options: DbOptions) -> Result<Self> {
        fs::create_dir_all(dir)?;

        // Load manifest
        let manifest_path = dir.join("MANIFEST");
        let manifest = Manifest::open(&manifest_path)?;

        // Open SSTables listed in manifest
        let mut sstables = Vec::new();
        for name in manifest.sstables() {
            let sst_path = dir.join(name);
            if sst_path.exists() {
                sstables.push(SsTableReader::open(&sst_path)?);
            }
        }

        // Recover memtable from WAL
        let wal_path = dir.join("wal.log");
        let memtable = Wal::recover(&wal_path)?;
        let wal = Wal::open(&wal_path)?;

        // Determine next SST ID from existing files
        let next_sst_id = Self::find_next_sst_id(manifest.sstables());

        Ok(Db {
            dir: dir.to_path_buf(),
            memtable,
            wal,
            manifest,
            sstables,
            options,
            next_sst_id,
        })
    }

    /// Opens a database with default options.
    pub fn open_default(dir: &Path) -> Result<Self> {
        Self::open(dir, DbOptions::default())
    }

    /// Inserts or updates a key-value pair.
    ///
    /// The write is first appended to the WAL for durability, then applied
    /// to the in-memory table. If the memtable exceeds its size threshold,
    /// it is flushed to a new SSTable on disk.
    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.wal.write_put(key, value)?;
        self.memtable.put(key.to_vec(), value.to_vec());

        if self.memtable.approximate_size() >= self.options.memtable_size_threshold {
            self.flush_memtable()?;
        }

        Ok(())
    }

    /// Retrieves the value for a key.
    ///
    /// Search order:
    /// 1. Active memtable (most recent writes)
    /// 2. SSTables from newest to oldest, using bloom filters to skip
    ///    tables that definitely do not contain the key
    ///
    /// Returns `None` if the key does not exist or has been deleted.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        // Check memtable first
        match self.memtable.get(key) {
            Some(Value::Put(v)) => return Ok(Some(v.clone())),
            Some(Value::Tombstone) => return Ok(None),
            None => {}
        }

        // Check SSTables from newest to oldest
        for reader in self.sstables.iter().rev() {
            if let Some(value) = reader.get(key)? {
                if value == TOMBSTONE_VALUE {
                    return Ok(None);
                }
                return Ok(Some(value));
            }
        }

        Ok(None)
    }

    /// Deletes a key by writing a tombstone.
    ///
    /// The tombstone shadows any existing value in older SSTables. It will
    /// be removed during compaction once all older versions are merged.
    pub fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.wal.write_delete(key)?;
        self.memtable.delete(key.to_vec());

        if self.memtable.approximate_size() >= self.options.memtable_size_threshold {
            self.flush_memtable()?;
        }

        Ok(())
    }

    /// Scans keys in the range `[start, end)` and returns them in sorted order.
    ///
    /// Results are merged from the memtable and all SSTables, with newer
    /// values taking precedence. Tombstoned keys are excluded.
    pub fn scan(&self, start: &[u8], end: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        // Collect from memtable
        let mut sources: Vec<Vec<(Vec<u8>, Vec<u8>)>> = Vec::new();

        let mem_entries: Vec<(Vec<u8>, Vec<u8>)> = self
            .memtable
            .scan(start, end)
            .map(|(k, v)| {
                let value = match v {
                    Value::Put(data) => data.clone(),
                    Value::Tombstone => TOMBSTONE_VALUE.to_vec(),
                };
                (k.clone(), value)
            })
            .collect();
        sources.push(mem_entries);

        // Collect from SSTables, newest first
        for reader in self.sstables.iter().rev() {
            let entries = reader.scan_range(start, end)?;
            sources.push(entries);
        }

        // Merge and deduplicate
        let mut merge = crate::sstable::iterator::MergeIterator::from_entries(sources);
        let all = merge.collect_remaining();

        // Filter out tombstones
        let results = all
            .into_iter()
            .filter(|(_, v)| v.as_slice() != TOMBSTONE_VALUE)
            .collect();

        Ok(results)
    }

    /// Forces the current memtable to be flushed to an SSTable.
    ///
    /// Normally this happens automatically when the memtable exceeds the
    /// size threshold. Exposed publicly for testing and manual control.
    pub fn flush(&mut self) -> Result<()> {
        if !self.memtable.is_empty() {
            self.flush_memtable()?;
        }
        Ok(())
    }

    /// Forces a compaction of all SSTables, if the strategy permits it.
    pub fn maybe_compact(&mut self) -> Result<bool> {
        if !self
            .options
            .compaction_strategy
            .should_compact(self.sstables.len())
        {
            return Ok(false);
        }

        self.run_compaction()?;
        Ok(true)
    }

    /// Forces a compaction regardless of the strategy threshold.
    pub fn force_compact(&mut self) -> Result<()> {
        if self.sstables.len() < 2 {
            return Ok(());
        }
        self.run_compaction()
    }

    /// Returns the number of live SSTables on disk.
    pub fn num_sstables(&self) -> usize {
        self.sstables.len()
    }

    /// Returns the database directory path.
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    // ── internal ──────────────────────────────────────────────────────

    fn flush_memtable(&mut self) -> Result<()> {
        let entries = self.memtable.drain();
        if entries.is_empty() {
            return Ok(());
        }

        let sst_name = self.next_sst_name();
        let sst_path = self.dir.join(&sst_name);

        let mut builder = SsTableBuilder::new(&sst_path, entries.len())?;

        for (key, value) in &entries {
            let v = match value {
                Value::Put(data) => data.as_slice(),
                Value::Tombstone => TOMBSTONE_VALUE,
            };
            builder.add(key, v)?;
        }
        builder.finish()?;

        // Update manifest
        self.manifest.add_sstable(&sst_name)?;

        // Open the new SSTable for reads
        self.sstables.push(SsTableReader::open(&sst_path)?);

        // Discard the old WAL and start a fresh one
        let wal_path = self.dir.join("wal.log");
        self.wal.sync()?;
        Wal::discard(&wal_path)?;
        self.wal = Wal::open(&wal_path)?;

        // Trigger compaction if needed
        let _ = self.maybe_compact();

        Ok(())
    }

    fn run_compaction(&mut self) -> Result<()> {
        let input_paths: Vec<PathBuf> = self
            .sstables
            .iter()
            .map(|r| r.path().to_path_buf())
            .collect();
        let input_names: Vec<String> = input_paths
            .iter()
            .map(|p| p.file_name().unwrap().to_string_lossy().to_string())
            .collect();

        let output_name = self.next_sst_name();
        let output_path = self.dir.join(&output_name);

        // Compact all SSTables into one, dropping tombstones since we are
        // merging everything (no older data can be shadowed).
        compaction::compact(&input_paths, &output_path, true)?;

        // Update manifest: add output, remove inputs
        self.manifest.add_sstable(&output_name)?;
        self.manifest.remove_sstables(&input_names)?;
        self.manifest.compact()?;

        // Replace SSTable readers
        self.sstables.clear();
        self.sstables.push(SsTableReader::open(&output_path)?);

        // Remove old SSTable files
        for path in &input_paths {
            if path.exists() {
                let _ = fs::remove_file(path);
            }
        }

        Ok(())
    }

    fn next_sst_name(&mut self) -> String {
        let id = self.next_sst_id;
        self.next_sst_id += 1;
        format!("{id:06}.sst")
    }

    fn find_next_sst_id(existing: &[String]) -> u64 {
        let max_id = existing
            .iter()
            .filter_map(|name| {
                name.strip_suffix(".sst")
                    .and_then(|s| s.parse::<u64>().ok())
            })
            .max()
            .unwrap_or(0);

        // Add a time-based component to avoid collisions after crashes
        let time_component = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64 % 1_000_000)
            .unwrap_or(0);

        (max_id + 1).max(time_component)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn open_test_db() -> (TempDir, Db) {
        let dir = TempDir::new().unwrap();
        let db = Db::open_default(dir.path()).unwrap();
        (dir, db)
    }

    #[test]
    fn test_put_get() {
        let (_dir, mut db) = open_test_db();
        db.put(b"hello", b"world").unwrap();
        assert_eq!(db.get(b"hello").unwrap(), Some(b"world".to_vec()));
    }

    #[test]
    fn test_get_missing() {
        let (_dir, db) = open_test_db();
        assert_eq!(db.get(b"nope").unwrap(), None);
    }

    #[test]
    fn test_overwrite() {
        let (_dir, mut db) = open_test_db();
        db.put(b"k", b"old").unwrap();
        db.put(b"k", b"new").unwrap();
        assert_eq!(db.get(b"k").unwrap(), Some(b"new".to_vec()));
    }

    #[test]
    fn test_delete() {
        let (_dir, mut db) = open_test_db();
        db.put(b"k", b"v").unwrap();
        db.delete(b"k").unwrap();
        assert_eq!(db.get(b"k").unwrap(), None);
    }

    #[test]
    fn test_scan() {
        let (_dir, mut db) = open_test_db();
        db.put(b"a", b"1").unwrap();
        db.put(b"b", b"2").unwrap();
        db.put(b"c", b"3").unwrap();
        db.put(b"d", b"4").unwrap();

        let results = db.scan(b"b", b"d").unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0], (b"b".to_vec(), b"2".to_vec()));
        assert_eq!(results[1], (b"c".to_vec(), b"3".to_vec()));
    }

    #[test]
    fn test_flush_and_read_from_sstable() {
        let (_dir, mut db) = open_test_db();
        db.put(b"key1", b"val1").unwrap();
        db.put(b"key2", b"val2").unwrap();
        db.flush().unwrap();

        assert_eq!(db.num_sstables(), 1);
        assert_eq!(db.get(b"key1").unwrap(), Some(b"val1".to_vec()));
        assert_eq!(db.get(b"key2").unwrap(), Some(b"val2".to_vec()));
    }

    #[test]
    fn test_delete_across_flush() {
        let (_dir, mut db) = open_test_db();
        db.put(b"k", b"v").unwrap();
        db.flush().unwrap();
        db.delete(b"k").unwrap();

        // Should be deleted even though it lives in an SSTable
        assert_eq!(db.get(b"k").unwrap(), None);

        // Flush the delete tombstone
        db.flush().unwrap();
        assert_eq!(db.get(b"k").unwrap(), None);
    }

    #[test]
    fn test_wal_recovery() {
        let dir = TempDir::new().unwrap();

        // Write some data and drop without flushing
        {
            let mut db = Db::open_default(dir.path()).unwrap();
            db.put(b"survive", b"crash").unwrap();
            // Don't flush -- data is only in memtable + WAL
        }

        // Re-open: WAL should be replayed
        let db = Db::open_default(dir.path()).unwrap();
        assert_eq!(db.get(b"survive").unwrap(), Some(b"crash".to_vec()));
    }

    #[test]
    fn test_compaction() {
        let dir = TempDir::new().unwrap();
        let options = DbOptions {
            memtable_size_threshold: 64, // Very small, to force frequent flushes
            compaction_strategy: CompactionStrategy {
                min_tables_to_compact: 3,
            },
        };
        let mut db = Db::open(dir.path(), options).unwrap();

        // Write enough data to trigger multiple flushes and compaction
        for i in 0..100u32 {
            let key = format!("key_{i:04}");
            let val = format!("val_{i}");
            db.put(key.as_bytes(), val.as_bytes()).unwrap();
        }

        // Verify data is still correct after compaction
        for i in 0..100u32 {
            let key = format!("key_{i:04}");
            let val = format!("val_{i}");
            assert_eq!(
                db.get(key.as_bytes()).unwrap(),
                Some(val.into_bytes()),
                "Failed to read back key_{i:04}"
            );
        }
    }

    #[test]
    fn test_many_writes_and_scan() {
        let dir = TempDir::new().unwrap();
        let options = DbOptions {
            memtable_size_threshold: 256,
            ..Default::default()
        };
        let mut db = Db::open(dir.path(), options).unwrap();

        for i in 0..50u32 {
            let key = format!("{i:04}");
            let val = format!("v{i}");
            db.put(key.as_bytes(), val.as_bytes()).unwrap();
        }

        let results = db.scan(b"0010", b"0020").unwrap();
        assert_eq!(results.len(), 10);
        assert_eq!(results[0].0, b"0010");
        assert_eq!(results[9].0, b"0019");
    }
}
