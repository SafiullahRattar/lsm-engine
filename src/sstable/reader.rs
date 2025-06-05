use crate::bloom::BloomFilter;
use crate::error::{Error, Result};
use crate::sstable::block::BlockReader;
use crate::sstable::builder::{IndexEntry, FOOTER_SIZE, MAGIC, VERSION};
use std::fs;
use std::path::{Path, PathBuf};

/// Read-only handle to an SSTable file.
///
/// On open the reader memory-maps the footer and parses the index and bloom
/// filter into memory. Data blocks are loaded on demand during point lookups
/// and scans.
pub struct SsTableReader {
    data: Vec<u8>,
    index: Vec<IndexEntry>,
    bloom: BloomFilter,
    path: PathBuf,
}

impl SsTableReader {
    /// Opens an SSTable file, verifying the footer magic and version, then
    /// loading the index and bloom filter into memory.
    pub fn open(path: &Path) -> Result<Self> {
        let data = fs::read(path)?;

        if data.len() < FOOTER_SIZE {
            return Err(Error::InvalidSsTable(
                "File too small for footer".to_string(),
            ));
        }

        let footer_start = data.len() - FOOTER_SIZE;
        let footer = &data[footer_start..];

        // Verify magic
        let magic = &footer[32..36];
        if magic != MAGIC {
            return Err(Error::InvalidSsTable(format!(
                "Bad magic: expected {:?}, got {magic:?}",
                MAGIC
            )));
        }

        // Verify version
        let version = u32::from_le_bytes(
            footer[36..40]
                .try_into()
                .map_err(|_| Error::InvalidSsTable("Bad version bytes".to_string()))?,
        );
        if version != VERSION {
            return Err(Error::InvalidSsTable(format!(
                "Unsupported version: {version}"
            )));
        }

        let index_offset = u64::from_le_bytes(footer[0..8].try_into().unwrap()) as usize;
        let index_length = u64::from_le_bytes(footer[8..16].try_into().unwrap()) as usize;
        let bloom_offset = u64::from_le_bytes(footer[16..24].try_into().unwrap()) as usize;
        let bloom_length = u64::from_le_bytes(footer[24..32].try_into().unwrap()) as usize;

        // Parse index
        let index_data = &data[index_offset..index_offset + index_length];
        let index = Self::decode_index(index_data)?;

        // Parse bloom filter
        let bloom_data = &data[bloom_offset..bloom_offset + bloom_length];
        let bloom = BloomFilter::from_bytes(bloom_data)
            .ok_or_else(|| Error::InvalidSsTable("Invalid bloom filter data".to_string()))?;

        Ok(SsTableReader {
            data,
            index,
            bloom,
            path: path.to_path_buf(),
        })
    }

    /// Point lookup: returns the value for an exact key match, or `None`.
    ///
    /// The lookup first checks the bloom filter to avoid unnecessary I/O,
    /// then binary-searches the index to find the candidate data block,
    /// then binary-searches within the block.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        // Bloom filter early-out
        if !self.bloom.may_contain(key) {
            return Ok(None);
        }

        // Binary search the index to find the right block.
        // We want the last block whose first_key <= target key.
        let block_idx = match self
            .index
            .binary_search_by(|entry| entry.first_key.as_slice().cmp(key))
        {
            Ok(i) => i,
            Err(0) => return Ok(None), // key < first key in SSTable
            Err(i) => i - 1,
        };

        let entry = &self.index[block_idx];
        let block_data = &self.data[entry.offset as usize..(entry.offset + entry.length) as usize];
        let block = BlockReader::open(block_data)?;
        block.search(key)
    }

    /// Returns all key-value pairs in sorted order from all blocks.
    ///
    /// This is the foundation for the SSTable iterator and for merge
    /// operations during compaction.
    pub fn scan_all(&self) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut results = Vec::new();
        for entry in &self.index {
            let block_data =
                &self.data[entry.offset as usize..(entry.offset + entry.length) as usize];
            let block = BlockReader::open(block_data)?;
            for i in 0..block.num_entries() {
                results.push(block.get_entry(i)?);
            }
        }
        Ok(results)
    }

    /// Returns key-value pairs in the range `[start, end)`.
    pub fn scan_range(&self, start: &[u8], end: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut results = Vec::new();

        // Find the first block that could contain keys >= start
        let first_block = match self
            .index
            .binary_search_by(|e| e.first_key.as_slice().cmp(start))
        {
            Ok(i) => i,
            Err(0) => 0,
            Err(i) => i - 1,
        };

        for idx in first_block..self.index.len() {
            let entry = &self.index[idx];

            // If the first key of this block is >= end, we are done.
            if entry.first_key.as_slice() >= end {
                break;
            }

            let block_data =
                &self.data[entry.offset as usize..(entry.offset + entry.length) as usize];
            let block = BlockReader::open(block_data)?;
            for i in 0..block.num_entries() {
                let (k, v) = block.get_entry(i)?;
                if k.as_slice() >= end {
                    return Ok(results);
                }
                if k.as_slice() >= start {
                    results.push((k, v));
                }
            }
        }

        Ok(results)
    }

    /// Returns the file path of this SSTable.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Returns the number of data blocks in this SSTable.
    pub fn num_blocks(&self) -> usize {
        self.index.len()
    }

    /// Returns a reference to the index entries (used by compaction).
    pub fn index(&self) -> &[IndexEntry] {
        &self.index
    }

    /// Returns the smallest key in this SSTable, or `None` if empty.
    pub fn first_key(&self) -> Option<&[u8]> {
        self.index.first().map(|e| e.first_key.as_slice())
    }

    /// Returns the file size in bytes.
    pub fn file_size(&self) -> u64 {
        self.data.len() as u64
    }

    fn decode_index(data: &[u8]) -> Result<Vec<IndexEntry>> {
        if data.len() < 4 {
            return Err(Error::InvalidSsTable("Index too small".to_string()));
        }

        let num_entries = u32::from_le_bytes(data[..4].try_into().unwrap()) as usize;
        let mut entries = Vec::with_capacity(num_entries);
        let mut pos = 4;

        for _ in 0..num_entries {
            if pos + 4 > data.len() {
                return Err(Error::InvalidSsTable("Index truncated".to_string()));
            }
            let key_len = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
            pos += 4;

            if pos + key_len + 16 > data.len() {
                return Err(Error::InvalidSsTable("Index entry truncated".to_string()));
            }
            let first_key = data[pos..pos + key_len].to_vec();
            pos += key_len;

            let offset = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
            pos += 8;

            let length = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
            pos += 8;

            entries.push(IndexEntry {
                first_key,
                offset,
                length,
            });
        }

        Ok(entries)
    }
}
