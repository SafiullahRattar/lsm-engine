use crate::bloom::BloomFilter;
use crate::error::Result;
use crate::sstable::block::BlockBuilder;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;

/// Magic bytes identifying an SSTable file ("LSMT" in ASCII).
pub const MAGIC: [u8; 4] = [0x4C, 0x53, 0x4D, 0x54];
/// Current format version.
pub const VERSION: u32 = 1;
/// Footer size: index_offset(8) + index_len(8) + bloom_offset(8) + bloom_len(8) + magic(4) + version(4) = 40
pub const FOOTER_SIZE: usize = 40;

/// Entry in the SSTable index.
/// Stores the first key of each data block together with the block's offset
/// and length within the file, enabling binary search across blocks.
#[derive(Debug, Clone)]
pub struct IndexEntry {
    pub first_key: Vec<u8>,
    pub offset: u64,
    pub length: u64,
}

/// Builds an SSTable file incrementally from sorted key-value pairs.
///
/// # File Layout
///
/// ```text
/// ┌─────────────────────────────┐
/// │  Data Block 0               │
/// ├─────────────────────────────┤
/// │  Data Block 1               │
/// ├─────────────────────────────┤
/// │  ...                        │
/// ├─────────────────────────────┤
/// │  Data Block N               │
/// ├─────────────────────────────┤
/// │  Index Block                │
/// │   (first_key, offset, len)  │
/// │   for each data block       │
/// ├─────────────────────────────┤
/// │  Bloom Filter Block         │
/// ├─────────────────────────────┤
/// │  Footer (40 bytes)          │
/// │   index_offset  : u64 LE    │
/// │   index_length  : u64 LE    │
/// │   bloom_offset  : u64 LE    │
/// │   bloom_length  : u64 LE    │
/// │   magic         : 4 bytes   │
/// │   version       : u32 LE    │
/// └─────────────────────────────┘
/// ```
pub struct SsTableBuilder {
    writer: BufWriter<File>,
    block_builder: BlockBuilder,
    index_entries: Vec<IndexEntry>,
    bloom: BloomFilter,
    current_offset: u64,
    entry_count: usize,
}

impl SsTableBuilder {
    /// Creates a new SSTable builder that writes to the given file path.
    ///
    /// `expected_entries` is used to size the bloom filter. A rough estimate
    /// is fine -- overshooting wastes a small amount of memory, undershooting
    /// increases the false-positive rate.
    pub fn new(path: &Path, expected_entries: usize) -> Result<Self> {
        let file = File::create(path)?;
        Ok(SsTableBuilder {
            writer: BufWriter::new(file),
            block_builder: BlockBuilder::new(),
            index_entries: Vec::new(),
            bloom: BloomFilter::new(expected_entries.max(1)),
            current_offset: 0,
            entry_count: 0,
        })
    }

    /// Adds a key-value pair. **Keys must be added in sorted order.**
    ///
    /// When the current data block reaches its target size the block is
    /// flushed to disk and a new block is started.
    pub fn add(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.bloom.insert(key);
        self.entry_count += 1;

        if !self.block_builder.add(key, value) {
            // Block is full -- flush it and start a new one.
            self.flush_block()?;
            // The new key goes into the fresh block.
            assert!(
                self.block_builder.add(key, value),
                "Single entry must fit in an empty block"
            );
        }

        Ok(())
    }

    /// Finalizes the SSTable: flushes the last data block, writes the index
    /// block, bloom filter block, and footer, then syncs to disk.
    pub fn finish(mut self) -> Result<()> {
        // Flush the last data block
        if !self.block_builder.is_empty() {
            self.flush_block()?;
        }

        // Write index block
        let index_offset = self.current_offset;
        let index_data = self.encode_index();
        self.writer.write_all(&index_data)?;
        let index_length = index_data.len() as u64;
        self.current_offset += index_length;

        // Write bloom filter block
        let bloom_offset = self.current_offset;
        let bloom_data = self.bloom.to_bytes();
        self.writer.write_all(&bloom_data)?;
        let bloom_length = bloom_data.len() as u64;
        self.current_offset += bloom_length;

        // Write footer
        self.writer.write_all(&index_offset.to_le_bytes())?;
        self.writer.write_all(&index_length.to_le_bytes())?;
        self.writer.write_all(&bloom_offset.to_le_bytes())?;
        self.writer.write_all(&bloom_length.to_le_bytes())?;
        self.writer.write_all(&MAGIC)?;
        self.writer.write_all(&VERSION.to_le_bytes())?;

        self.writer.flush()?;
        self.writer.get_ref().sync_all()?;

        Ok(())
    }

    /// Returns the number of key-value entries added so far.
    pub fn entry_count(&self) -> usize {
        self.entry_count
    }

    // ── internal helpers ──────────────────────────────────────────────

    fn flush_block(&mut self) -> Result<()> {
        let first_key = self
            .block_builder
            .first_key()
            .expect("flush_block called on empty block")
            .to_vec();

        let block_data = self.block_builder.finish();
        let block_len = block_data.len() as u64;

        self.index_entries.push(IndexEntry {
            first_key,
            offset: self.current_offset,
            length: block_len,
        });

        self.writer.write_all(&block_data)?;
        self.current_offset += block_len;

        Ok(())
    }

    /// Serializes the index entries.
    ///
    /// Format:
    /// ```text
    /// [num_entries: u32 LE]
    /// for each entry:
    ///   [key_len: u32 LE][key][offset: u64 LE][length: u64 LE]
    /// ```
    fn encode_index(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&(self.index_entries.len() as u32).to_le_bytes());
        for entry in &self.index_entries {
            buf.extend_from_slice(&(entry.first_key.len() as u32).to_le_bytes());
            buf.extend_from_slice(&entry.first_key);
            buf.extend_from_slice(&entry.offset.to_le_bytes());
            buf.extend_from_slice(&entry.length.to_le_bytes());
        }
        buf
    }
}
