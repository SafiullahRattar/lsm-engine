use lsm_engine::compaction::{compact, TOMBSTONE_VALUE};
use lsm_engine::sstable::builder::SsTableBuilder;
use lsm_engine::sstable::reader::SsTableReader;
use std::path::{Path, PathBuf};
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
fn test_compaction_merges_disjoint_ranges() {
    let dir = TempDir::new().unwrap();

    let sst1 = build_sstable(dir.path(), "001.sst", &[(b"a", b"1"), (b"b", b"2")]);
    let sst2 = build_sstable(dir.path(), "002.sst", &[(b"c", b"3"), (b"d", b"4")]);

    let output = dir.path().join("merged.sst");
    compact(&[sst1, sst2], &output, false).unwrap();

    let reader = SsTableReader::open(&output).unwrap();
    assert_eq!(reader.get(b"a").unwrap(), Some(b"1".to_vec()));
    assert_eq!(reader.get(b"b").unwrap(), Some(b"2".to_vec()));
    assert_eq!(reader.get(b"c").unwrap(), Some(b"3".to_vec()));
    assert_eq!(reader.get(b"d").unwrap(), Some(b"4".to_vec()));
}

#[test]
fn test_compaction_newer_wins() {
    let dir = TempDir::new().unwrap();

    // sst1 is older, sst2 is newer
    let sst1 = build_sstable(dir.path(), "001.sst", &[(b"key", b"old_value")]);
    let sst2 = build_sstable(dir.path(), "002.sst", &[(b"key", b"new_value")]);

    let output = dir.path().join("merged.sst");
    compact(&[sst1, sst2], &output, false).unwrap();

    let reader = SsTableReader::open(&output).unwrap();
    assert_eq!(reader.get(b"key").unwrap(), Some(b"new_value".to_vec()));
}

#[test]
fn test_compaction_tombstone_removal() {
    let dir = TempDir::new().unwrap();

    let sst1 = build_sstable(
        dir.path(),
        "001.sst",
        &[(b"alive", b"yes"), (b"dead", b"old_value")],
    );
    let sst2 = build_sstable(dir.path(), "002.sst", &[(b"dead", TOMBSTONE_VALUE)]);

    // With drop_tombstones = true
    let output = dir.path().join("merged.sst");
    compact(&[sst1, sst2], &output, true).unwrap();

    let reader = SsTableReader::open(&output).unwrap();
    assert_eq!(reader.get(b"alive").unwrap(), Some(b"yes".to_vec()));
    assert_eq!(reader.get(b"dead").unwrap(), None);
}

#[test]
fn test_compaction_preserves_tombstones_when_asked() {
    let dir = TempDir::new().unwrap();

    let sst1 = build_sstable(dir.path(), "001.sst", &[(b"key", TOMBSTONE_VALUE)]);

    let output = dir.path().join("merged.sst");
    compact(&[sst1], &output, false).unwrap();

    let reader = SsTableReader::open(&output).unwrap();
    assert_eq!(reader.get(b"key").unwrap(), Some(TOMBSTONE_VALUE.to_vec()));
}

#[test]
fn test_compaction_many_tables() {
    let dir = TempDir::new().unwrap();

    let mut paths = Vec::new();
    for t in 0..5 {
        let mut entries = Vec::new();
        let keys: Vec<String> = (0..20).map(|i| format!("key_{:04}", t * 20 + i)).collect();
        let vals: Vec<String> = (0..20).map(|i| format!("val_t{t}_{i}")).collect();
        for (k, v) in keys.iter().zip(vals.iter()) {
            entries.push((k.as_bytes(), v.as_bytes()));
        }
        let path = build_sstable(dir.path(), &format!("{t:03}.sst"), &entries);
        paths.push(path);
    }

    let output = dir.path().join("merged.sst");
    compact(&paths, &output, false).unwrap();

    let reader = SsTableReader::open(&output).unwrap();
    let all = reader.scan_all().unwrap();
    assert_eq!(all.len(), 100);

    // Verify sorted order
    for window in all.windows(2) {
        assert!(window[0].0 < window[1].0);
    }
}
