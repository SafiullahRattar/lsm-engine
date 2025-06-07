use crate::error::Result;
use crate::sstable::reader::SsTableReader;

/// Iterator over all key-value pairs in an SSTable, in sorted order.
///
/// Loads entries eagerly into memory. For production use you would want a
/// lazy block-at-a-time iterator, but eager loading simplifies the merge
/// logic and is fine for the data volumes this engine targets.
pub struct SsTableIterator {
    entries: Vec<(Vec<u8>, Vec<u8>)>,
    position: usize,
}

impl SsTableIterator {
    /// Creates an iterator over the entire SSTable.
    pub fn new(reader: &SsTableReader) -> Result<Self> {
        let entries = reader.scan_all()?;
        Ok(SsTableIterator {
            entries,
            position: 0,
        })
    }

    /// Creates an iterator over a key range `[start, end)`.
    pub fn range(reader: &SsTableReader, start: &[u8], end: &[u8]) -> Result<Self> {
        let entries = reader.scan_range(start, end)?;
        Ok(SsTableIterator {
            entries,
            position: 0,
        })
    }

    /// Returns `true` if the iterator has more entries.
    pub fn is_valid(&self) -> bool {
        self.position < self.entries.len()
    }

    /// Returns the current key, or `None` if exhausted.
    pub fn key(&self) -> Option<&[u8]> {
        self.entries.get(self.position).map(|(k, _)| k.as_slice())
    }

    /// Returns the current value, or `None` if exhausted.
    pub fn value(&self) -> Option<&[u8]> {
        self.entries.get(self.position).map(|(_, v)| v.as_slice())
    }

    /// Advances to the next entry.
    pub fn next(&mut self) {
        if self.position < self.entries.len() {
            self.position += 1;
        }
    }
}

/// Merges multiple sorted iterators into a single sorted stream.
///
/// When the same key appears in multiple iterators the value from the
/// iterator with the *lowest index* wins (i.e., the most recent source).
/// This is used during compaction and cross-SSTable scans.
pub struct MergeIterator {
    entries: Vec<(Vec<u8>, Vec<u8>)>,
    position: usize,
}

impl MergeIterator {
    /// Builds a merged, deduplicated stream from the given iterators.
    ///
    /// `iterators` should be ordered from newest to oldest so that the
    /// first occurrence of each key is the authoritative one.
    pub fn new(iterators: Vec<SsTableIterator>) -> Self {
        // Collect all (key, value) pairs with their source priority
        let mut all: Vec<(Vec<u8>, Vec<u8>, usize)> = Vec::new();
        for (priority, iter) in iterators.into_iter().enumerate() {
            for (k, v) in iter.entries {
                all.push((k, v, priority));
            }
        }

        // Sort by key, then by priority (lower = newer = wins)
        all.sort_by(|a, b| a.0.cmp(&b.0).then(a.2.cmp(&b.2)));

        // Deduplicate: keep only the first (lowest priority) entry per key
        let mut entries: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        let mut last_key: Option<Vec<u8>> = None;
        for (k, v, _) in all {
            if last_key.as_ref() == Some(&k) {
                continue;
            }
            last_key = Some(k.clone());
            entries.push((k, v));
        }

        MergeIterator {
            entries,
            position: 0,
        }
    }

    /// Builds a merged stream from raw entry vectors (used by compaction to
    /// merge memtable entries alongside SSTable entries).
    pub fn from_entries(sources: Vec<Vec<(Vec<u8>, Vec<u8>)>>) -> Self {
        let mut all: Vec<(Vec<u8>, Vec<u8>, usize)> = Vec::new();
        for (priority, entries) in sources.into_iter().enumerate() {
            for (k, v) in entries {
                all.push((k, v, priority));
            }
        }

        all.sort_by(|a, b| a.0.cmp(&b.0).then(a.2.cmp(&b.2)));

        let mut entries: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        let mut last_key: Option<Vec<u8>> = None;
        for (k, v, _) in all {
            if last_key.as_ref() == Some(&k) {
                continue;
            }
            last_key = Some(k.clone());
            entries.push((k, v));
        }

        MergeIterator {
            entries,
            position: 0,
        }
    }

    pub fn is_valid(&self) -> bool {
        self.position < self.entries.len()
    }

    pub fn key(&self) -> Option<&[u8]> {
        self.entries.get(self.position).map(|(k, _)| k.as_slice())
    }

    pub fn value(&self) -> Option<&[u8]> {
        self.entries.get(self.position).map(|(_, v)| v.as_slice())
    }

    pub fn next(&mut self) {
        if self.position < self.entries.len() {
            self.position += 1;
        }
    }

    /// Collects all remaining entries into a vector.
    pub fn collect_remaining(&mut self) -> Vec<(Vec<u8>, Vec<u8>)> {
        let rest = self.entries[self.position..].to_vec();
        self.position = self.entries.len();
        rest
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_iter(pairs: Vec<(&[u8], &[u8])>) -> SsTableIterator {
        let entries = pairs
            .into_iter()
            .map(|(k, v)| (k.to_vec(), v.to_vec()))
            .collect();
        SsTableIterator {
            entries,
            position: 0,
        }
    }

    #[test]
    fn test_merge_iterator_dedup() {
        // Newer source (priority 0) should win
        let iter1 = make_iter(vec![(b"a", b"new_a"), (b"b", b"new_b")]);
        let iter2 = make_iter(vec![(b"a", b"old_a"), (b"c", b"old_c")]);

        let mut merged = MergeIterator::new(vec![iter1, iter2]);
        let entries = merged.collect_remaining();

        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0], (b"a".to_vec(), b"new_a".to_vec()));
        assert_eq!(entries[1], (b"b".to_vec(), b"new_b".to_vec()));
        assert_eq!(entries[2], (b"c".to_vec(), b"old_c".to_vec()));
    }

    #[test]
    fn test_merge_iterator_step_by_step() {
        let iter1 = make_iter(vec![(b"x", b"1")]);
        let iter2 = make_iter(vec![(b"y", b"2")]);

        let mut merged = MergeIterator::new(vec![iter1, iter2]);

        assert!(merged.is_valid());
        assert_eq!(merged.key(), Some(b"x".as_slice()));
        merged.next();
        assert_eq!(merged.key(), Some(b"y".as_slice()));
        merged.next();
        assert!(!merged.is_valid());
    }
}
