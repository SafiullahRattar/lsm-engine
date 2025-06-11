use lsm_engine::{Db, DbOptions};
use tempfile::TempDir;

#[test]
fn test_basic_crud() {
    let dir = TempDir::new().unwrap();
    let mut db = Db::open_default(dir.path()).unwrap();

    // Insert
    db.put(b"key1", b"value1").unwrap();
    db.put(b"key2", b"value2").unwrap();

    // Read
    assert_eq!(db.get(b"key1").unwrap(), Some(b"value1".to_vec()));
    assert_eq!(db.get(b"key2").unwrap(), Some(b"value2".to_vec()));

    // Update
    db.put(b"key1", b"updated").unwrap();
    assert_eq!(db.get(b"key1").unwrap(), Some(b"updated".to_vec()));

    // Delete
    db.delete(b"key1").unwrap();
    assert_eq!(db.get(b"key1").unwrap(), None);

    // key2 still accessible
    assert_eq!(db.get(b"key2").unwrap(), Some(b"value2".to_vec()));
}

#[test]
fn test_persistence_across_reopen() {
    let dir = TempDir::new().unwrap();

    // Write and flush
    {
        let mut db = Db::open_default(dir.path()).unwrap();
        db.put(b"persistent", b"data").unwrap();
        db.flush().unwrap();
    }

    // Reopen and verify
    {
        let db = Db::open_default(dir.path()).unwrap();
        assert_eq!(db.get(b"persistent").unwrap(), Some(b"data".to_vec()));
    }
}

#[test]
fn test_wal_recovery_on_reopen() {
    let dir = TempDir::new().unwrap();

    // Write without flushing (data only in WAL)
    {
        let mut db = Db::open_default(dir.path()).unwrap();
        db.put(b"wal_key", b"wal_value").unwrap();
        // Drop without flush -- simulates a crash
    }

    // Reopen -- WAL should be replayed
    {
        let db = Db::open_default(dir.path()).unwrap();
        assert_eq!(db.get(b"wal_key").unwrap(), Some(b"wal_value".to_vec()));
    }
}

#[test]
fn test_scan_with_mixed_sources() {
    let dir = TempDir::new().unwrap();
    let options = DbOptions {
        memtable_size_threshold: 128,
        ..Default::default()
    };
    let mut db = Db::open(dir.path(), options).unwrap();

    // Write enough to trigger a flush
    for i in 0..30u32 {
        let key = format!("scan_{i:03}");
        let val = format!("v_{i}");
        db.put(key.as_bytes(), val.as_bytes()).unwrap();
    }

    // Some data in SSTable, some in memtable
    let results = db.scan(b"scan_005", b"scan_015").unwrap();
    assert_eq!(results.len(), 10);
    for (i, (k, _v)) in results.iter().enumerate() {
        let expected_key = format!("scan_{:03}", i + 5);
        assert_eq!(k, expected_key.as_bytes());
    }
}

#[test]
fn test_large_values() {
    let dir = TempDir::new().unwrap();
    let mut db = Db::open_default(dir.path()).unwrap();

    let big_value = vec![0xAB_u8; 100_000];
    db.put(b"big", &big_value).unwrap();
    db.flush().unwrap();

    let result = db.get(b"big").unwrap().unwrap();
    assert_eq!(result.len(), 100_000);
    assert!(result.iter().all(|&b| b == 0xAB));
}

#[test]
fn test_empty_value() {
    let dir = TempDir::new().unwrap();
    let mut db = Db::open_default(dir.path()).unwrap();

    db.put(b"empty", b"").unwrap();
    assert_eq!(db.get(b"empty").unwrap(), Some(b"".to_vec()));

    db.flush().unwrap();
    assert_eq!(db.get(b"empty").unwrap(), Some(b"".to_vec()));
}

#[test]
fn test_sequential_writes_maintain_order() {
    let dir = TempDir::new().unwrap();
    let mut db = Db::open_default(dir.path()).unwrap();

    for i in 0..1000u32 {
        let key = format!("{i:06}");
        db.put(key.as_bytes(), key.as_bytes()).unwrap();
    }

    db.flush().unwrap();

    let results = db.scan(b"000100", b"000200").unwrap();
    assert_eq!(results.len(), 100);
    for (i, (k, v)) in results.iter().enumerate() {
        let expected = format!("{:06}", 100 + i);
        assert_eq!(k, expected.as_bytes());
        assert_eq!(v, expected.as_bytes());
    }
}
