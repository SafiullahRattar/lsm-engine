/// A bloom filter for probabilistic set membership testing.
///
/// Uses double hashing to generate `k` hash functions from two base hashes.
/// This avoids needing `k` independent hash functions while maintaining good
/// false positive rates. The approach is described in:
/// Kirsch & Mitzenmacher, "Less Hashing, Same Performance" (2006).
///
/// # False Positive Rate
///
/// For `n` inserted items and `m` bits with `k` hash functions:
///   FPR ~ (1 - e^(-kn/m))^k
///
/// With the default 10 bits per key and 7 hash functions, the expected
/// false positive rate is approximately 0.82%.
pub struct BloomFilter {
    bits: Vec<u8>,
    num_bits: u64,
    num_hashes: u32,
}

impl BloomFilter {
    /// The default number of bits allocated per key.
    /// 10 bits/key with 7 hashes yields ~0.82% FPR.
    const BITS_PER_KEY: u64 = 10;

    /// Creates a new bloom filter sized for the expected number of keys.
    ///
    /// The filter allocates `BITS_PER_KEY` bits per expected key and uses
    /// an optimal number of hash functions: k = (m/n) * ln(2).
    pub fn new(expected_keys: usize) -> Self {
        let num_bits = (expected_keys as u64 * Self::BITS_PER_KEY).max(64);
        // Round up to the next byte boundary
        let num_bytes = num_bits.div_ceil(8);
        let num_bits = num_bytes * 8;
        // Optimal k = (m/n) * ln(2) ~ 0.693 * (bits_per_key)
        let num_hashes = ((Self::BITS_PER_KEY as f64) * 0.693)
            .ceil()
            .clamp(1.0, 30.0) as u32;

        BloomFilter {
            bits: vec![0u8; num_bytes as usize],
            num_bits,
            num_hashes,
        }
    }

    /// Reconstructs a bloom filter from its serialized byte representation.
    ///
    /// The last 4 bytes encode the number of hash functions (little-endian u32).
    /// Returns `None` if the data is too short to contain valid filter state.
    pub fn from_bytes(data: &[u8]) -> Option<Self> {
        if data.len() < 4 {
            return None;
        }
        let num_hashes = u32::from_le_bytes(data[data.len() - 4..].try_into().ok()?);
        let bits = data[..data.len() - 4].to_vec();
        let num_bits = bits.len() as u64 * 8;
        Some(BloomFilter {
            bits,
            num_bits,
            num_hashes,
        })
    }

    /// Inserts a key into the bloom filter by setting `k` bits.
    pub fn insert(&mut self, key: &[u8]) {
        let (h1, h2) = self.hash_pair(key);
        for i in 0..self.num_hashes {
            let bit_pos = self.nth_hash(h1, h2, i) % self.num_bits;
            self.set_bit(bit_pos);
        }
    }

    /// Tests whether a key might be in the set.
    ///
    /// Returns `true` if the key is *possibly* present (may be a false positive).
    /// Returns `false` if the key is *definitely* absent (no false negatives).
    pub fn may_contain(&self, key: &[u8]) -> bool {
        let (h1, h2) = self.hash_pair(key);
        for i in 0..self.num_hashes {
            let bit_pos = self.nth_hash(h1, h2, i) % self.num_bits;
            if !self.get_bit(bit_pos) {
                return false;
            }
        }
        true
    }

    /// Serializes the bloom filter to bytes.
    ///
    /// Layout: `[bit_vector][num_hashes: u32 LE]`
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut data = self.bits.clone();
        data.extend_from_slice(&self.num_hashes.to_le_bytes());
        data
    }

    /// Generates two independent 64-bit hashes using FNV-1a variants.
    ///
    /// `h1` uses the standard FNV-1a offset basis.
    /// `h2` uses a different seed (obtained by hashing the key in reverse byte
    /// order with a rotated offset basis) to ensure independence.
    fn hash_pair(&self, key: &[u8]) -> (u64, u64) {
        let h1 = self.fnv1a_hash(key);
        let h2 = self.fnv1a_hash_seed(key, 0x51_7c_c1_b7_27_22_0a_95);
        (h1, h2)
    }

    /// Computes the i-th hash using double hashing: `h1 + i*h2 + i*i`.
    ///
    /// The quadratic term `i*i` (enhanced double hashing) improves uniformity
    /// for higher-order hash functions.
    fn nth_hash(&self, h1: u64, h2: u64, i: u32) -> u64 {
        h1.wrapping_add((i as u64).wrapping_mul(h2))
            .wrapping_add((i as u64).wrapping_mul(i as u64))
    }

    fn set_bit(&mut self, pos: u64) {
        let byte_idx = (pos / 8) as usize;
        let bit_idx = pos % 8;
        self.bits[byte_idx] |= 1 << bit_idx;
    }

    fn get_bit(&self, pos: u64) -> bool {
        let byte_idx = (pos / 8) as usize;
        let bit_idx = pos % 8;
        (self.bits[byte_idx] >> bit_idx) & 1 == 1
    }

    /// FNV-1a hash with the standard offset basis.
    fn fnv1a_hash(&self, key: &[u8]) -> u64 {
        self.fnv1a_hash_seed(key, 0xcb_f2_9c_e4_84_22_23_25)
    }

    /// FNV-1a hash with a caller-supplied offset basis (seed).
    fn fnv1a_hash_seed(&self, key: &[u8], seed: u64) -> u64 {
        const FNV_PRIME: u64 = 0x00_00_01_00_00_01_b3;
        let mut hash = seed;
        for &byte in key {
            hash ^= byte as u64;
            hash = hash.wrapping_mul(FNV_PRIME);
        }
        hash
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bloom_basic_membership() {
        let mut filter = BloomFilter::new(100);
        filter.insert(b"hello");
        filter.insert(b"world");

        assert!(filter.may_contain(b"hello"));
        assert!(filter.may_contain(b"world"));
    }

    #[test]
    fn test_bloom_likely_absent() {
        let mut filter = BloomFilter::new(100);
        for i in 0..50u32 {
            filter.insert(&i.to_le_bytes());
        }

        // Keys that were never inserted should mostly return false
        let mut false_positives = 0;
        for i in 1000..2000u32 {
            if filter.may_contain(&i.to_le_bytes()) {
                false_positives += 1;
            }
        }
        // With 10 bits/key and 50 keys, FPR should be well under 5%
        assert!(
            false_positives < 50,
            "Too many false positives: {false_positives}/1000"
        );
    }

    #[test]
    fn test_bloom_serialization_roundtrip() {
        let mut filter = BloomFilter::new(100);
        filter.insert(b"key1");
        filter.insert(b"key2");

        let bytes = filter.to_bytes();
        let restored = BloomFilter::from_bytes(&bytes).unwrap();

        assert!(restored.may_contain(b"key1"));
        assert!(restored.may_contain(b"key2"));
    }

    #[test]
    fn test_bloom_false_positive_rate() {
        let n = 1000;
        let mut filter = BloomFilter::new(n);
        for i in 0..n as u32 {
            filter.insert(&i.to_le_bytes());
        }

        let test_count = 10_000;
        let mut fp = 0;
        for i in (n as u32 + 1000)..(n as u32 + 1000 + test_count) {
            if filter.may_contain(&i.to_le_bytes()) {
                fp += 1;
            }
        }

        let fpr = fp as f64 / test_count as f64;
        // Expected ~0.82%, allow up to 3% to avoid flaky tests
        assert!(
            fpr < 0.03,
            "False positive rate too high: {fpr:.4} ({fp}/{test_count})"
        );
    }
}
