use lsm_engine::sstable::builder::SsTableBuilder;
use lsm_engine::sstable::iterator::SsTableIterator;
use lsm_engine::sstable::reader::SsTableReader;
use tempfile::TempDir;

fn build_test_sstable(dir: &std::path::Path, entries: &[(&[u8], &[u8])]) -> std::path::PathBuf {
    let path = dir.join("test.sst");
    let mut builder = SsTableBuilder::new(&path, entries.len()).unwrap();
    for (k, v) in entries {
        builder.add(k, v).unwrap();
    }
    builder.finish().unwrap();
    path
}

#[test]
fn test_sstable_roundtrip() {
    let dir = TempDir::new().unwrap();
    let entries: Vec<(&[u8], &[u8])> = vec![(b"alpha", b"1"), (b"beta", b"2"), (b"gamma", b"3")];
    let path = build_test_sstable(dir.path(), &entries);

    let reader = SsTableReader::open(&path).unwrap();
    assert_eq!(reader.get(b"alpha").unwrap(), Some(b"1".to_vec()));
    assert_eq!(reader.get(b"beta").unwrap(), Some(b"2".to_vec()));
    assert_eq!(reader.get(b"gamma").unwrap(), Some(b"3".to_vec()));
    assert_eq!(reader.get(b"delta").unwrap(), None);
}

#[test]
fn test_sstable_many_entries() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("big.sst");

    let n = 1000;
    let mut builder = SsTableBuilder::new(&path, n).unwrap();
    for i in 0..n {
        let key = format!("key_{i:06}");
        let val = format!("val_{i:06}");
        builder.add(key.as_bytes(), val.as_bytes()).unwrap();
    }
    builder.finish().unwrap();

    let reader = SsTableReader::open(&path).unwrap();
    assert!(reader.num_blocks() > 1, "Should have multiple blocks");

    // Spot-check some keys
    for i in [0, 1, 499, 500, 998, 999] {
        let key = format!("key_{i:06}");
        let val = format!("val_{i:06}");
        assert_eq!(
            reader.get(key.as_bytes()).unwrap(),
            Some(val.into_bytes()),
            "Failed for key_{i:06}"
        );
    }

    // Check a missing key
    assert_eq!(reader.get(b"key_999999").unwrap(), None);
}

#[test]
fn test_sstable_iterator() {
    let dir = TempDir::new().unwrap();
    let entries: Vec<(&[u8], &[u8])> = vec![(b"a", b"1"), (b"b", b"2"), (b"c", b"3")];
    let path = build_test_sstable(dir.path(), &entries);

    let reader = SsTableReader::open(&path).unwrap();
    let mut iter = SsTableIterator::new(&reader).unwrap();

    assert!(iter.is_valid());
    assert_eq!(iter.key(), Some(b"a".as_slice()));
    assert_eq!(iter.value(), Some(b"1".as_slice()));

    iter.next();
    assert_eq!(iter.key(), Some(b"b".as_slice()));

    iter.next();
    assert_eq!(iter.key(), Some(b"c".as_slice()));

    iter.next();
    assert!(!iter.is_valid());
}

#[test]
fn test_sstable_range_scan() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("range.sst");

    let mut builder = SsTableBuilder::new(&path, 26).unwrap();
    for c in b'a'..=b'z' {
        builder.add(&[c], &[c]).unwrap();
    }
    builder.finish().unwrap();

    let reader = SsTableReader::open(&path).unwrap();
    let results = reader.scan_range(b"d", b"h").unwrap();

    let keys: Vec<u8> = results.iter().map(|(k, _)| k[0]).collect();
    assert_eq!(keys, vec![b'd', b'e', b'f', b'g']);
}

#[test]
fn test_sstable_bloom_filter_effectiveness() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("bloom.sst");

    let n = 500;
    let mut builder = SsTableBuilder::new(&path, n).unwrap();
    for i in 0..n {
        let key = format!("present_{i:06}");
        builder.add(key.as_bytes(), b"v").unwrap();
    }
    builder.finish().unwrap();

    let reader = SsTableReader::open(&path).unwrap();

    // All present keys should be found
    for i in 0..n {
        let key = format!("present_{i:06}");
        assert!(
            reader.get(key.as_bytes()).unwrap().is_some(),
            "Missing: {key}"
        );
    }

    // Absent keys should mostly return None (bloom filter skips them)
    let mut found = 0;
    for i in 0..1000 {
        let key = format!("absent_{i:06}");
        if reader.get(key.as_bytes()).unwrap().is_some() {
            found += 1;
        }
    }
    assert_eq!(found, 0, "Absent keys should never be 'found'");
}
