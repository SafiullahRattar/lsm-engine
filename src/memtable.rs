use std::collections::BTreeMap;
use std::ops::Bound;

/// An entry in the memtable. `Value(bytes)` represents a live key-value pair;
/// `Tombstone` marks a deletion that must propagate to SSTables during compaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Value {
    /// A live value.
    Put(Vec<u8>),
    /// A deletion marker.
    Tombstone,
}

/// In-memory sorted key-value store backed by a `BTreeMap`.
///
/// # Why BTreeMap?
///
/// A BTreeMap gives us:
/// - O(log n) point lookups, inserts, and deletes
/// - Efficient ordered iteration (needed for SSTable flushes and range scans)
/// - Cache-friendly node layout compared to a skip list
///
/// A skip list (used in LevelDB/RocksDB) would allow concurrent readers during
/// writes, but BTreeMap is simpler and sufficient for a single-writer design.
///
/// # Size Tracking
///
/// `approximate_size` tracks the memory footprint of keys and values to decide
/// when the memtable should be flushed to an SSTable. The tracking is approximate
/// because it does not account for BTreeMap node overhead.
pub struct MemTable {
    map: BTreeMap<Vec<u8>, Value>,
    approximate_size: usize,
}

impl MemTable {
    /// Creates a new, empty memtable.
    pub fn new() -> Self {
        MemTable {
            map: BTreeMap::new(),
            approximate_size: 0,
        }
    }

    /// Inserts or updates a key-value pair.
    ///
    /// If the key already exists, its old value is replaced and the size
    /// estimate is adjusted accordingly.
    pub fn put(&mut self, key: Vec<u8>, value: Vec<u8>) {
        let added_size = key.len() + value.len();
        if let Some(old) = self.map.insert(key, Value::Put(value)) {
            // Subtract the old value's contribution
            let old_size = match &old {
                Value::Put(v) => v.len(),
                Value::Tombstone => 0,
            };
            self.approximate_size = self.approximate_size + added_size - old_size;
        } else {
            self.approximate_size += added_size;
        }
    }

    /// Marks a key as deleted by writing a tombstone.
    ///
    /// Tombstones are necessary because a simple removal from the memtable
    /// would allow older SSTable entries to resurface during reads.
    pub fn delete(&mut self, key: Vec<u8>) {
        let key_len = key.len();
        if let Some(old) = self.map.insert(key, Value::Tombstone) {
            let old_size = match &old {
                Value::Put(v) => v.len(),
                Value::Tombstone => 0,
            };
            self.approximate_size -= old_size;
        } else {
            self.approximate_size += key_len;
        }
    }

    /// Retrieves the value associated with a key.
    ///
    /// Returns `Some(Value::Put(..))` for live entries, `Some(Value::Tombstone)`
    /// for deleted entries, and `None` if the key is absent from this memtable.
    pub fn get(&self, key: &[u8]) -> Option<&Value> {
        self.map.get(key)
    }

    /// Returns an iterator over all entries in sorted key order.
    ///
    /// Used when flushing the memtable to an SSTable.
    pub fn iter(&self) -> impl Iterator<Item = (&Vec<u8>, &Value)> {
        self.map.iter()
    }

    /// Returns an iterator over entries in the key range `[start, end)`.
    pub fn scan<'a>(
        &'a self,
        start: &[u8],
        end: &[u8],
    ) -> impl Iterator<Item = (&'a Vec<u8>, &'a Value)> {
        self.map
            .range::<[u8], _>((Bound::Included(start), Bound::Excluded(end)))
    }

    /// Returns the approximate memory footprint of stored keys and values in bytes.
    pub fn approximate_size(&self) -> usize {
        self.approximate_size
    }

    /// Returns the number of entries (including tombstones).
    pub fn len(&self) -> usize {
        self.map.len()
    }

    /// Returns `true` if the memtable has no entries.
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    /// Drains all entries from the memtable, returning them in sorted order.
    ///
    /// After this call the memtable is empty and its size estimate is zero.
    pub fn drain(&mut self) -> Vec<(Vec<u8>, Value)> {
        self.approximate_size = 0;
        std::mem::take(&mut self.map).into_iter().collect()
    }
}

impl Default for MemTable {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_put_and_get() {
        let mut mt = MemTable::new();
        mt.put(b"key1".to_vec(), b"value1".to_vec());
        mt.put(b"key2".to_vec(), b"value2".to_vec());

        assert_eq!(mt.get(b"key1"), Some(&Value::Put(b"value1".to_vec())));
        assert_eq!(mt.get(b"key2"), Some(&Value::Put(b"value2".to_vec())));
        assert_eq!(mt.get(b"key3"), None);
    }

    #[test]
    fn test_overwrite() {
        let mut mt = MemTable::new();
        mt.put(b"key".to_vec(), b"old".to_vec());
        mt.put(b"key".to_vec(), b"new".to_vec());

        assert_eq!(mt.get(b"key"), Some(&Value::Put(b"new".to_vec())));
        assert_eq!(mt.len(), 1);
    }

    #[test]
    fn test_delete_creates_tombstone() {
        let mut mt = MemTable::new();
        mt.put(b"key".to_vec(), b"value".to_vec());
        mt.delete(b"key".to_vec());

        assert_eq!(mt.get(b"key"), Some(&Value::Tombstone));
    }

    #[test]
    fn test_scan_range() {
        let mut mt = MemTable::new();
        mt.put(b"a".to_vec(), b"1".to_vec());
        mt.put(b"b".to_vec(), b"2".to_vec());
        mt.put(b"c".to_vec(), b"3".to_vec());
        mt.put(b"d".to_vec(), b"4".to_vec());

        let results: Vec<_> = mt.scan(b"b", b"d").map(|(k, _)| k.clone()).collect();
        assert_eq!(results, vec![b"b".to_vec(), b"c".to_vec()]);
    }

    #[test]
    fn test_sorted_iteration() {
        let mut mt = MemTable::new();
        mt.put(b"zebra".to_vec(), b"1".to_vec());
        mt.put(b"apple".to_vec(), b"2".to_vec());
        mt.put(b"mango".to_vec(), b"3".to_vec());

        let keys: Vec<_> = mt.iter().map(|(k, _)| k.clone()).collect();
        assert_eq!(
            keys,
            vec![b"apple".to_vec(), b"mango".to_vec(), b"zebra".to_vec()]
        );
    }

    #[test]
    fn test_approximate_size() {
        let mut mt = MemTable::new();
        assert_eq!(mt.approximate_size(), 0);

        mt.put(b"key".to_vec(), b"value".to_vec()); // 3 + 5 = 8
        assert_eq!(mt.approximate_size(), 8);

        mt.put(b"key".to_vec(), b"v".to_vec()); // replace: 8 - 5 + (3 + 1) = 7
        assert_eq!(mt.approximate_size(), 7);
    }

    #[test]
    fn test_drain() {
        let mut mt = MemTable::new();
        mt.put(b"a".to_vec(), b"1".to_vec());
        mt.put(b"b".to_vec(), b"2".to_vec());

        let entries = mt.drain();
        assert_eq!(entries.len(), 2);
        assert!(mt.is_empty());
        assert_eq!(mt.approximate_size(), 0);
    }
}
