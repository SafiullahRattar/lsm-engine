use lsm_engine::memtable::{MemTable, Value};

#[test]
fn test_memtable_insert_and_lookup() {
    let mut mt = MemTable::new();
    mt.put(b"foo".to_vec(), b"bar".to_vec());
    assert_eq!(mt.get(b"foo"), Some(&Value::Put(b"bar".to_vec())));
    assert_eq!(mt.get(b"baz"), None);
}

#[test]
fn test_memtable_tombstone() {
    let mut mt = MemTable::new();
    mt.put(b"x".to_vec(), b"1".to_vec());
    mt.delete(b"x".to_vec());
    assert_eq!(mt.get(b"x"), Some(&Value::Tombstone));
}

#[test]
fn test_memtable_ordered_iteration() {
    let mut mt = MemTable::new();
    mt.put(b"c".to_vec(), b"3".to_vec());
    mt.put(b"a".to_vec(), b"1".to_vec());
    mt.put(b"b".to_vec(), b"2".to_vec());

    let keys: Vec<_> = mt.iter().map(|(k, _)| k.clone()).collect();
    assert_eq!(keys, vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);
}

#[test]
fn test_memtable_drain_clears_state() {
    let mut mt = MemTable::new();
    mt.put(b"a".to_vec(), b"1".to_vec());
    mt.put(b"b".to_vec(), b"2".to_vec());

    let entries = mt.drain();
    assert_eq!(entries.len(), 2);
    assert!(mt.is_empty());
    assert_eq!(mt.approximate_size(), 0);
}

#[test]
fn test_memtable_scan_bounds() {
    let mut mt = MemTable::new();
    for c in b'a'..=b'z' {
        mt.put(vec![c], vec![c]);
    }

    let results: Vec<_> = mt.scan(b"d", b"g").map(|(k, _)| k[0]).collect();
    assert_eq!(results, vec![b'd', b'e', b'f']);
}
