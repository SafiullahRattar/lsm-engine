use crate::error::{Error, Result};

/// Target size for each data block before it is flushed.
///
/// 4 KiB aligns with typical filesystem and SSD page sizes, giving a good
/// balance between seek granularity and compression ratio.
pub const BLOCK_SIZE: usize = 4096;

/// A data block containing sorted key-value pairs.
///
/// # On-Disk Format
///
/// ```text
/// [num_entries:  4 bytes, LE]
/// [offsets:      num_entries * 4 bytes, LE]   — byte offset of each entry within the data section
/// [data section]
///   for each entry:
///     [key_len:   4 bytes, LE]
///     [key:       key_len bytes]
///     [value_len: 4 bytes, LE]
///     [value:     value_len bytes]
/// [crc32:        4 bytes, LE]                 — CRC of everything before it
/// ```
///
/// The offset array enables binary search within a block: read the offset,
/// jump to the entry, compare the key.
///
/// Builds a block incrementally and serializes it to bytes.
pub struct BlockBuilder {
    entries: Vec<(Vec<u8>, Vec<u8>)>,
    size_estimate: usize,
}

impl BlockBuilder {
    pub fn new() -> Self {
        BlockBuilder {
            entries: Vec::new(),
            // 4 bytes for num_entries + 4 bytes for crc
            size_estimate: 8,
        }
    }

    /// Adds a key-value pair. Returns `false` without adding if the block
    /// is already at capacity (unless it is the first entry -- a block must
    /// contain at least one entry).
    pub fn add(&mut self, key: &[u8], value: &[u8]) -> bool {
        let entry_size = 4 + key.len() + 4 + value.len() + 4; // +4 for offset slot
        if !self.entries.is_empty() && self.size_estimate + entry_size > BLOCK_SIZE {
            return false;
        }
        self.size_estimate += entry_size;
        self.entries.push((key.to_vec(), value.to_vec()));
        true
    }

    /// Returns the number of entries currently in the builder.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Returns `true` if the builder has no entries.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Returns the first key in the block, or `None` if empty.
    pub fn first_key(&self) -> Option<&[u8]> {
        self.entries.first().map(|(k, _)| k.as_slice())
    }

    /// Returns the last key in the block, or `None` if empty.
    pub fn last_key(&self) -> Option<&[u8]> {
        self.entries.last().map(|(k, _)| k.as_slice())
    }

    /// Serializes the block to bytes and resets the builder.
    pub fn finish(&mut self) -> Vec<u8> {
        let num_entries = self.entries.len() as u32;
        let mut buf = Vec::with_capacity(self.size_estimate);

        // Number of entries
        buf.extend_from_slice(&num_entries.to_le_bytes());

        // Compute offsets (relative to the start of the data section)
        let offsets_size = self.entries.len() * 4;
        let data_section_start = 4 + offsets_size;
        let _ = data_section_start; // used only conceptually

        // First pass: compute offsets
        let mut offsets = Vec::with_capacity(self.entries.len());
        let mut offset: u32 = 0;
        for (key, value) in &self.entries {
            offsets.push(offset);
            offset += (4 + key.len() + 4 + value.len()) as u32;
        }

        // Write offsets
        for o in &offsets {
            buf.extend_from_slice(&o.to_le_bytes());
        }

        // Write entries
        for (key, value) in &self.entries {
            buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
            buf.extend_from_slice(key);
            buf.extend_from_slice(&(value.len() as u32).to_le_bytes());
            buf.extend_from_slice(value);
        }

        // CRC over everything so far
        let crc = crc32fast::hash(&buf);
        buf.extend_from_slice(&crc.to_le_bytes());

        // Reset
        self.entries.clear();
        self.size_estimate = 8;

        buf
    }
}

impl Default for BlockBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Read-only view of a serialized block.
pub struct BlockReader<'a> {
    data: &'a [u8],
    num_entries: u32,
    offsets_start: usize,
    data_start: usize,
}

impl<'a> BlockReader<'a> {
    /// Parses a block from raw bytes, verifying the CRC checksum.
    pub fn open(data: &'a [u8]) -> Result<Self> {
        if data.len() < 8 {
            return Err(Error::Corruption("Block too small".to_string()));
        }

        // Verify CRC (last 4 bytes)
        let payload = &data[..data.len() - 4];
        let stored_crc = u32::from_le_bytes(
            data[data.len() - 4..]
                .try_into()
                .map_err(|_| Error::Corruption("Invalid CRC bytes".to_string()))?,
        );
        let computed_crc = crc32fast::hash(payload);
        if stored_crc != computed_crc {
            return Err(Error::Corruption(format!(
                "Block CRC mismatch: stored {stored_crc:#010x}, computed {computed_crc:#010x}"
            )));
        }

        let num_entries = u32::from_le_bytes(
            data[..4]
                .try_into()
                .map_err(|_| Error::Corruption("Bad entry count".to_string()))?,
        );

        let offsets_start = 4;
        let data_start = offsets_start + (num_entries as usize) * 4;

        Ok(BlockReader {
            data,
            num_entries,
            offsets_start,
            data_start,
        })
    }

    /// Returns the number of key-value entries in the block.
    pub fn num_entries(&self) -> u32 {
        self.num_entries
    }

    /// Reads the key-value pair at the given index.
    pub fn get_entry(&self, index: u32) -> Result<(Vec<u8>, Vec<u8>)> {
        if index >= self.num_entries {
            return Err(Error::InvalidSsTable(format!(
                "Entry index {index} out of range (num_entries={})",
                self.num_entries
            )));
        }

        let offset_pos = self.offsets_start + (index as usize) * 4;
        let entry_offset = u32::from_le_bytes(
            self.data[offset_pos..offset_pos + 4]
                .try_into()
                .map_err(|_| Error::Corruption("Bad offset".to_string()))?,
        ) as usize;

        let pos = self.data_start + entry_offset;
        let (key, value) = Self::decode_entry(&self.data[pos..])?;
        Ok((key, value))
    }

    /// Binary searches for a key within the block.
    ///
    /// Returns `Some((key, value))` on exact match, `None` otherwise.
    pub fn search(&self, target: &[u8]) -> Result<Option<Vec<u8>>> {
        let mut lo: u32 = 0;
        let mut hi: u32 = self.num_entries;

        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let (key, _) = self.get_entry(mid)?;
            match key.as_slice().cmp(target) {
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Equal => {
                    let (_, value) = self.get_entry(mid)?;
                    return Ok(Some(value));
                }
                std::cmp::Ordering::Greater => hi = mid,
            }
        }
        Ok(None)
    }

    fn decode_entry(data: &[u8]) -> Result<(Vec<u8>, Vec<u8>)> {
        if data.len() < 4 {
            return Err(Error::Corruption("Entry truncated at key_len".to_string()));
        }
        let key_len = u32::from_le_bytes(data[..4].try_into().unwrap()) as usize;
        let key_end = 4 + key_len;
        if data.len() < key_end + 4 {
            return Err(Error::Corruption(
                "Entry truncated at value_len".to_string(),
            ));
        }
        let key = data[4..key_end].to_vec();

        let value_len = u32::from_le_bytes(data[key_end..key_end + 4].try_into().unwrap()) as usize;
        let value_end = key_end + 4 + value_len;
        if data.len() < value_end {
            return Err(Error::Corruption("Entry truncated at value".to_string()));
        }
        let value = data[key_end + 4..value_end].to_vec();

        Ok((key, value))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_block_roundtrip() {
        let mut builder = BlockBuilder::new();
        builder.add(b"apple", b"red");
        builder.add(b"banana", b"yellow");
        builder.add(b"cherry", b"dark_red");
        let data = builder.finish();

        let reader = BlockReader::open(&data).unwrap();
        assert_eq!(reader.num_entries(), 3);

        let (k, v) = reader.get_entry(0).unwrap();
        assert_eq!(k, b"apple");
        assert_eq!(v, b"red");

        let (k, v) = reader.get_entry(2).unwrap();
        assert_eq!(k, b"cherry");
        assert_eq!(v, b"dark_red");
    }

    #[test]
    fn test_block_search() {
        let mut builder = BlockBuilder::new();
        for i in 0..20u32 {
            let key = format!("key_{i:04}");
            let val = format!("val_{i}");
            builder.add(key.as_bytes(), val.as_bytes());
        }
        let data = builder.finish();

        let reader = BlockReader::open(&data).unwrap();
        let result = reader.search(b"key_0010").unwrap();
        assert_eq!(result, Some(b"val_10".to_vec()));

        let result = reader.search(b"missing").unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_block_crc_corruption() {
        let mut builder = BlockBuilder::new();
        builder.add(b"key", b"value");
        let mut data = builder.finish();

        // Corrupt a byte
        data[5] ^= 0xff;

        let result = BlockReader::open(&data);
        assert!(result.is_err());
    }

    #[test]
    fn test_block_capacity() {
        let mut builder = BlockBuilder::new();
        // First entry always fits
        assert!(builder.add(&[0u8; 2000], &[0u8; 2000]));
        // Second large entry should be rejected
        assert!(!builder.add(&[0u8; 2000], &[0u8; 2000]));
    }
}
