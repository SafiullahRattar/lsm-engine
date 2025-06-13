use lsm_engine::{Db, DbOptions};
use std::path::Path;

fn main() -> lsm_engine::Result<()> {
    let db_path = Path::new("/tmp/lsm_example_db");

    // Clean up from previous runs
    if db_path.exists() {
        std::fs::remove_dir_all(db_path)?;
    }

    println!("=== LSM-Tree Storage Engine Demo ===\n");

    // Open with default options (4 MiB memtable threshold)
    let mut db = Db::open_default(db_path)?;

    // ── Writes ────────────────────────────────────────────────────────
    println!("Writing key-value pairs...");
    db.put(b"user:1:name", b"Alice")?;
    db.put(b"user:1:email", b"alice@example.com")?;
    db.put(b"user:2:name", b"Bob")?;
    db.put(b"user:2:email", b"bob@example.com")?;
    db.put(b"user:3:name", b"Carol")?;
    db.put(b"user:3:email", b"carol@example.com")?;
    println!("  Wrote 6 entries.\n");

    // ── Point reads ──────────────────────────────────────────────────
    println!("Point lookups:");
    if let Some(name) = db.get(b"user:1:name")? {
        println!("  user:1:name = {}", String::from_utf8_lossy(&name));
    }
    if let Some(email) = db.get(b"user:2:email")? {
        println!("  user:2:email = {}", String::from_utf8_lossy(&email));
    }
    match db.get(b"user:99:name")? {
        Some(_) => println!("  user:99:name = found (unexpected)"),
        None => println!("  user:99:name = not found (expected)"),
    }
    println!();

    // ── Range scan ───────────────────────────────────────────────────
    println!("Range scan [user:1, user:3):");
    let results = db.scan(b"user:1", b"user:3")?;
    for (key, value) in &results {
        println!(
            "  {} = {}",
            String::from_utf8_lossy(key),
            String::from_utf8_lossy(value)
        );
    }
    println!();

    // ── Delete ───────────────────────────────────────────────────────
    println!("Deleting user:2:name...");
    db.delete(b"user:2:name")?;
    match db.get(b"user:2:name")? {
        Some(_) => println!("  Still exists (bug!)"),
        None => println!("  Confirmed deleted."),
    }
    println!();

    // ── Flush to disk ────────────────────────────────────────────────
    println!("Flushing memtable to SSTable...");
    db.flush()?;
    println!("  SSTables on disk: {}", db.num_sstables());

    // Reads still work after flush
    if let Some(name) = db.get(b"user:1:name")? {
        println!(
            "  Read after flush: user:1:name = {}",
            String::from_utf8_lossy(&name)
        );
    }
    println!();

    // ── Bulk writes to trigger compaction ────────────────────────────
    println!("Writing 5000 entries with small memtable to demo compaction...");
    // Reopen with a tiny memtable to trigger compaction
    drop(db);
    std::fs::remove_dir_all(db_path)?;

    let options = DbOptions {
        memtable_size_threshold: 1024, // 1 KiB
        ..Default::default()
    };
    let mut db = Db::open(db_path, options)?;

    for i in 0..5000u32 {
        let key = format!("item:{i:05}");
        let val = format!("data for item {i}");
        db.put(key.as_bytes(), val.as_bytes())?;
    }

    println!("  SSTables after bulk write: {}", db.num_sstables());

    // Verify a sample
    let val = db.get(b"item:02500")?.expect("Should exist");
    println!("  Verified: item:02500 = {}", String::from_utf8_lossy(&val));

    // Cleanup
    std::fs::remove_dir_all(db_path)?;
    println!("\nDone. Cleaned up {}", db_path.display());

    Ok(())
}
