use crate::error::{Error, Result};
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};

/// Tracks the current set of SSTable files that make up the database.
///
/// The manifest is a simple text file where each line is a command:
/// - `ADD <filename>` -- an SSTable was created (flush or compaction output)
/// - `REMOVE <filename>` -- an SSTable was deleted (compaction input)
///
/// On startup the manifest is replayed to reconstruct the set of live
/// SSTables. This is much simpler than LevelDB's binary manifest format
/// but sufficient for our purposes.
///
/// # Crash Safety
///
/// Each mutation is flushed and synced before the corresponding file system
/// operation, ensuring that the manifest is always a superset of files that
/// exist on disk. Recovery can safely skip `ADD` entries whose files are
/// missing (they indicate an incomplete flush) and `REMOVE` entries whose
/// files are already gone (they indicate an incomplete compaction cleanup).
pub struct Manifest {
    path: PathBuf,
    /// The currently live SSTable filenames, in order from oldest to newest.
    sstables: Vec<String>,
}

impl Manifest {
    /// Opens or creates a manifest file and replays it to discover live SSTables.
    pub fn open(path: &Path) -> Result<Self> {
        let sstables = if path.exists() {
            Self::replay(path)?
        } else {
            Vec::new()
        };

        Ok(Manifest {
            path: path.to_path_buf(),
            sstables,
        })
    }

    /// Records that a new SSTable has been added.
    pub fn add_sstable(&mut self, filename: &str) -> Result<()> {
        self.append_line(&format!("ADD {filename}"))?;
        self.sstables.push(filename.to_string());
        Ok(())
    }

    /// Records that SSTables have been removed (after compaction).
    pub fn remove_sstables(&mut self, filenames: &[String]) -> Result<()> {
        for f in filenames {
            self.append_line(&format!("REMOVE {f}"))?;
        }
        self.sstables.retain(|s| !filenames.contains(s));
        Ok(())
    }

    /// Returns the list of live SSTable filenames, oldest first.
    pub fn sstables(&self) -> &[String] {
        &self.sstables
    }

    /// Returns the number of live SSTables.
    pub fn num_sstables(&self) -> usize {
        self.sstables.len()
    }

    /// Rewrites the manifest file compactly (only ADD lines for live tables).
    ///
    /// Prevents the manifest from growing without bound after many compaction
    /// cycles.
    pub fn compact(&mut self) -> Result<()> {
        let tmp_path = self.path.with_extension("tmp");
        {
            let mut f = File::create(&tmp_path)?;
            for name in &self.sstables {
                writeln!(f, "ADD {name}")
                    .map_err(|e| Error::Manifest(format!("Failed to write: {e}")))?;
            }
            f.sync_all()?;
        }
        fs::rename(&tmp_path, &self.path)?;
        Ok(())
    }

    fn append_line(&self, line: &str) -> Result<()> {
        let mut f = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        writeln!(f, "{line}").map_err(|e| Error::Manifest(format!("Failed to append: {e}")))?;
        f.sync_all()?;
        Ok(())
    }

    fn replay(path: &Path) -> Result<Vec<String>> {
        let file = File::open(path)?;
        let reader = BufReader::new(file);
        let mut live: Vec<String> = Vec::new();

        for line in reader.lines() {
            let line = line?;
            let line = line.trim();
            if line.is_empty() {
                continue;
            }

            if let Some(filename) = line.strip_prefix("ADD ") {
                live.push(filename.to_string());
            } else if let Some(filename) = line.strip_prefix("REMOVE ") {
                live.retain(|s| s != filename);
            }
            // Unknown lines are silently ignored for forward compatibility.
        }

        Ok(live)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_manifest_add_and_list() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("MANIFEST");

        let mut m = Manifest::open(&path).unwrap();
        m.add_sstable("000001.sst").unwrap();
        m.add_sstable("000002.sst").unwrap();

        assert_eq!(m.sstables(), &["000001.sst", "000002.sst"]);
    }

    #[test]
    fn test_manifest_remove() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("MANIFEST");

        let mut m = Manifest::open(&path).unwrap();
        m.add_sstable("000001.sst").unwrap();
        m.add_sstable("000002.sst").unwrap();
        m.remove_sstables(&["000001.sst".to_string()]).unwrap();

        assert_eq!(m.sstables(), &["000002.sst"]);
    }

    #[test]
    fn test_manifest_replay() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("MANIFEST");

        // Write some entries
        {
            let mut m = Manifest::open(&path).unwrap();
            m.add_sstable("a.sst").unwrap();
            m.add_sstable("b.sst").unwrap();
            m.add_sstable("c.sst").unwrap();
            m.remove_sstables(&["a.sst".to_string(), "b.sst".to_string()])
                .unwrap();
        }

        // Re-open and verify
        let m = Manifest::open(&path).unwrap();
        assert_eq!(m.sstables(), &["c.sst"]);
    }

    #[test]
    fn test_manifest_compact() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("MANIFEST");

        let mut m = Manifest::open(&path).unwrap();
        m.add_sstable("a.sst").unwrap();
        m.add_sstable("b.sst").unwrap();
        m.remove_sstables(&["a.sst".to_string()]).unwrap();
        m.compact().unwrap();

        // Re-open and verify only "b.sst" is present
        let m2 = Manifest::open(&path).unwrap();
        assert_eq!(m2.sstables(), &["b.sst"]);

        // Verify the file is compact (no REMOVE lines)
        let contents = fs::read_to_string(&path).unwrap();
        assert!(!contents.contains("REMOVE"));
    }
}
