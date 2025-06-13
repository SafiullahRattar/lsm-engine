use criterion::{black_box, criterion_group, criterion_main, Criterion};
use lsm_engine::{Db, DbOptions};
use tempfile::TempDir;

fn bench_sequential_writes(c: &mut Criterion) {
    c.bench_function("sequential_write_1000", |b| {
        b.iter_with_setup(
            || {
                let dir = TempDir::new().unwrap();
                let db = Db::open_default(dir.path()).unwrap();
                (dir, db)
            },
            |(_dir, mut db)| {
                for i in 0..1000u32 {
                    let key = format!("key_{i:08}");
                    let val = format!("value_{i:08}");
                    db.put(black_box(key.as_bytes()), black_box(val.as_bytes()))
                        .unwrap();
                }
            },
        );
    });
}

fn bench_random_reads(c: &mut Criterion) {
    // Setup: create a database with some data
    let dir = TempDir::new().unwrap();
    let mut db = Db::open_default(dir.path()).unwrap();
    for i in 0..5000u32 {
        let key = format!("key_{i:08}");
        let val = format!("value_{i:08}");
        db.put(key.as_bytes(), val.as_bytes()).unwrap();
    }
    db.flush().unwrap();

    c.bench_function("point_read_memtable_hit", |b| {
        b.iter(|| {
            let key = format!("key_{:08}", 2500);
            black_box(db.get(black_box(key.as_bytes())).unwrap());
        });
    });

    c.bench_function("point_read_sstable_hit", |b| {
        b.iter(|| {
            let key = format!("key_{:08}", 100);
            black_box(db.get(black_box(key.as_bytes())).unwrap());
        });
    });

    c.bench_function("point_read_miss", |b| {
        b.iter(|| {
            black_box(db.get(black_box(b"nonexistent_key")).unwrap());
        });
    });
}

fn bench_scan(c: &mut Criterion) {
    let dir = TempDir::new().unwrap();
    let mut db = Db::open_default(dir.path()).unwrap();
    for i in 0..10_000u32 {
        let key = format!("{i:08}");
        let val = format!("v{i}");
        db.put(key.as_bytes(), val.as_bytes()).unwrap();
    }
    db.flush().unwrap();

    c.bench_function("scan_100_keys", |b| {
        b.iter(|| {
            black_box(db.scan(b"00001000", b"00001100").unwrap());
        });
    });

    c.bench_function("scan_1000_keys", |b| {
        b.iter(|| {
            black_box(db.scan(b"00001000", b"00002000").unwrap());
        });
    });
}

fn bench_write_with_flushes(c: &mut Criterion) {
    c.bench_function("write_10000_with_flushes", |b| {
        b.iter_with_setup(
            || {
                let dir = TempDir::new().unwrap();
                let options = DbOptions {
                    memtable_size_threshold: 32 * 1024, // 32 KiB to trigger frequent flushes
                    ..Default::default()
                };
                let db = Db::open(dir.path(), options).unwrap();
                (dir, db)
            },
            |(_dir, mut db)| {
                for i in 0..10_000u32 {
                    let key = format!("key_{i:08}");
                    let val = format!("value_{i:08}");
                    db.put(black_box(key.as_bytes()), black_box(val.as_bytes()))
                        .unwrap();
                }
            },
        );
    });
}

criterion_group!(
    benches,
    bench_sequential_writes,
    bench_random_reads,
    bench_scan,
    bench_write_with_flushes,
);
criterion_main!(benches);
