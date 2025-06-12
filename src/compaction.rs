use crate::error::Result;
use crate::sstable::builder::SsTableBuilder;
use crate::sstable::iterator::MergeIterator;
use crate::sstable::reader::SsTableReader;
use std::path::{Path, PathBuf};

/// Tombstone marker value stored in SSTables.
///
/// When a key is deleted, we write a tombstone value to shadow older entries.
/// During compaction, tombstones can be dropped once we are certain no older
/// SSTable contains the key (i.e., during a full compaction of all levels).
pub const TOMBSTONE_VALUE: &[u8] = b"__TOMBSTONE__";

/// Size-tiered compaction strategy.
///
/// # Strategy
///
/// When the number of SSTables at a given "tier" exceeds a threshold, they
/// are merged into a single larger SSTable. This is the simplest compaction
/// strategy and the one used by Apache Cassandra's `SizeTieredCompactionStrategy`.
///
/// ## Trade-offs vs. Leveled Compaction
///
/// | Property           | Size-Tiered        | Leveled             |
/// |--------------------|--------------------|---------------------|
/// | Write amplification| Lower              | Higher              |
/// | Space amplification| Higher             | Lower               |
/// | Read amplification | Higher             | Lower               |
/// | Implementation     | Simpler            | More complex        |
///
/// Size-tiered is a good default for write-heavy workloads.
pub struct CompactionStrategy {
    /// Number of SSTables that triggers a compaction.
    pub min_tables_to_compact: usize,
}

impl Default for CompactionStrategy {
    fn default() -> Self {
        CompactionStrategy {
            min_tables_to_compact: 4,
        }
    }
}

impl CompactionStrategy {
    /// Returns `true` if compaction should be triggered.
    pub fn should_compact(&self, num_sstables: usize) -> bool {
        num_sstables >= self.min_tables_to_compact
    }
}

/// Performs the actual compaction: merges multiple SSTables into one.
///
/// # Algorithm
///
/// 1. Open all input SSTables and extract their entries.
/// 2. Feed them into a `MergeIterator` which sorts and deduplicates by key,
///    preferring entries from newer SSTables.
/// 3. Optionally drop tombstones (when `drop_tombstones` is true -- safe only
///    when compacting *all* SSTables, since older tables might have a live
///    value that the tombstone is shadowing).
/// 4. Write the merged entries into a new SSTable.
///
/// # Returns
///
/// The path to the newly created SSTable.
pub fn compact(
    input_paths: &[PathBuf],
    output_path: &Path,
    drop_tombstones: bool,
) -> Result<PathBuf> {
    // Read all entries from each input SSTable, newest first
    let mut all_entries: Vec<Vec<(Vec<u8>, Vec<u8>)>> = Vec::new();
    for path in input_paths.iter().rev() {
        let reader = SsTableReader::open(path)?;
        let entries = reader.scan_all()?;
        all_entries.push(entries);
    }

    // Merge
    let mut merge_iter = MergeIterator::from_entries(all_entries);
    let merged = merge_iter.collect_remaining();

    // Filter tombstones if requested
    // NOTE: Only drop tombstones when compacting the full set of SSTables.
    // Otherwise we risk exposing a previously-shadowed value from an older SSTable.
    let entries: Vec<(Vec<u8>, Vec<u8>)> = if drop_tombstones {
        merged
            .into_iter()
            .filter(|(_, v)| v.as_slice() != TOMBSTONE_VALUE)
            .collect()
    } else {
        merged
    };

    // Build the output SSTable
    let mut builder = SsTableBuilder::new(output_path, entries.len())?;
    for (key, value) in &entries {
        builder.add(key, value)?;
    }
    builder.finish()?;

    Ok(output_path.to_path_buf())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sstable::builder::SsTableBuilder;
    use tempfile::TempDir;

    fn build_sstable(dir: &Path, name: &str, entries: &[(&[u8], &[u8])]) -> PathBuf {
        let path = dir.join(name);
        let mut builder = SsTableBuilder::new(&path, entries.len()).unwrap();
        for (k, v) in entries {
            builder.add(k, v).unwrap();
        }
        builder.finish().unwrap();
        path
    }

    #[test]
    fn test_compact_merge() {
        let dir = TempDir::new().unwrap();

        let sst1 = build_sstable(dir.path(), "001.sst", &[(b"a", b"1"), (b"c", b"3")]);
        let sst2 = build_sstable(dir.path(), "002.sst", &[(b"b", b"2"), (b"c", b"new_3")]);

        let output = dir.path().join("003.sst");
        // sst2 is newer, so its "c" -> "new_3" wins
        compact(&[sst1, sst2], &output, false).unwrap();

        let reader = SsTableReader::open(&output).unwrap();
        assert_eq!(reader.get(b"a").unwrap(), Some(b"1".to_vec()));
        assert_eq!(reader.get(b"b").unwrap(), Some(b"2".to_vec()));
        assert_eq!(reader.get(b"c").unwrap(), Some(b"new_3".to_vec()));
    }

    #[test]
    fn test_compact_drop_tombstones() {
        let dir = TempDir::new().unwrap();

        let sst1 = build_sstable(dir.path(), "001.sst", &[(b"a", b"1"), (b"b", b"2")]);
        let sst2 = build_sstable(dir.path(), "002.sst", &[(b"a", TOMBSTONE_VALUE)]);

        let output = dir.path().join("003.sst");
        compact(&[sst1, sst2], &output, true).unwrap();

        let reader = SsTableReader::open(&output).unwrap();
        // "a" was deleted, should be gone
        assert_eq!(reader.get(b"a").unwrap(), None);
        // "b" is still alive
        assert_eq!(reader.get(b"b").unwrap(), Some(b"2".to_vec()));
    }

    #[test]
    fn test_compaction_strategy() {
        let strat = CompactionStrategy::default();
        assert!(!strat.should_compact(2));
        assert!(!strat.should_compact(3));
        assert!(strat.should_compact(4));
        assert!(strat.should_compact(10));
    }
}
