//! BloomFilter — 64B (512-bit) Cache-line Blocked Bloom Filter, 编码内嵌 CRC32.
//!
//! # 哈希与分桶方案
//!
//! ```text
//! h = hash64(key) (FNV-1a 64-bit + SplitMix64 混合)
//! block_idx = ((h >> 32) as usize) % num_blocks   // 定位单个 64B cache line
//! h1 = h as u32;  h2 = ((h >> 17) as u32) | 1
//! bit_i = (h1 + i × h2) & 511,  i ∈ [0, num_hashes)
//! ```
//!
//! bits 向上对齐到 512 bits 的整数倍; 磁盘编码 `[num_hashes:4][num_bits:8][bits][crc32:4]`,
//! `decode` 校验长度, 512 对齐约束与 CRC.
//!
//! # Invariant
//!
//! - 仅索引 user_key (`add(extract_user_key(key))`), 不带 sequence.
//! - miss 即"确定不存在" (免 I/O); 解码失败由调用方降级为无 filter, 不影响正确性.

use std::sync::atomic::{AtomicU64, Ordering};

use crate::error::{Error, Result};

const CACHE_LINE_BITS: usize = 512;
const CACHE_LINE_BYTES: usize = 64;

static BLOOM_FALSE_POSITIVES: AtomicU64 = AtomicU64::new(0);

/// 返回自进程启动以来 bloom filter 判断"可能存在"但实际 SSTable 读取未找到的累计次数.
pub fn bloom_false_positive_count() -> u64 {
    BLOOM_FALSE_POSITIVES.load(Ordering::Relaxed)
}

pub(crate) fn record_bloom_false_positive() {
    BLOOM_FALSE_POSITIVES.fetch_add(1, Ordering::Relaxed);
}

/// Filter 抽象, 便于未来扩展 Ribbon 等.
pub trait Filter {
    fn may_contain(&self, key: &[u8]) -> bool;
    fn add(&mut self, key: &[u8]);
    fn encode(&self) -> Vec<u8>;
    fn decode(data: &[u8]) -> Result<Self>
    where
        Self: Sized;
}

#[derive(Debug, Clone)]
pub struct BloomFilter {
    bits: Vec<u8>,
    num_hashes: u32,
    num_bits: usize,
    num_blocks: usize,
}

impl BloomFilter {
    pub fn new(expected_keys: usize, false_positive_rate: f64) -> Self {
        if expected_keys == 0 {
            return Self::with_bits_and_hashes(CACHE_LINE_BITS, 1);
        }
        let fp = false_positive_rate.clamp(0.0001, 0.9999);
        let num_bits = Self::optimal_num_bits(expected_keys, fp);
        let num_hashes = Self::optimal_num_hashes(num_bits, expected_keys);
        Self::with_bits_and_hashes(num_bits, num_hashes)
    }

    pub fn default_with_keys(num_keys: usize) -> Self {
        Self::new(num_keys, 0.01)
    }

    fn with_bits_and_hashes(raw_bits: usize, num_hashes: u32) -> Self {
        let num_blocks = raw_bits.div_ceil(CACHE_LINE_BITS).max(1);
        let num_bits = num_blocks * CACHE_LINE_BITS;
        Self {
            bits: vec![0u8; num_blocks * CACHE_LINE_BYTES],
            num_hashes,
            num_bits,
            num_blocks,
        }
    }

    fn optimal_num_bits(expected_keys: usize, false_positive_rate: f64) -> usize {
        let n = expected_keys as f64;
        let p = false_positive_rate;
        let raw_bits = (-n * p.ln() / (2.0_f64.ln().powi(2))).ceil() as usize;
        raw_bits.div_ceil(CACHE_LINE_BITS).max(1) * CACHE_LINE_BITS
    }

    fn optimal_num_hashes(num_bits: usize, expected_keys: usize) -> u32 {
        if expected_keys == 0 {
            return 1;
        }
        let k = ((num_bits as f64 / expected_keys as f64) * 2.0_f64.ln()).round() as u32;
        k.clamp(1, 30)
    }

    #[inline]
    fn block_and_seeds(&self, key: &[u8]) -> (usize, u32, u32) {
        let h = hash64(key);
        let block_idx = ((h >> 32) as usize) % self.num_blocks;
        let h1 = h as u32;
        let h2 = ((h >> 17) as u32) | 1;
        (block_idx * CACHE_LINE_BYTES, h1, h2)
    }

    pub fn num_hashes(&self) -> u32 {
        self.num_hashes
    }

    pub fn num_bits(&self) -> usize {
        self.num_bits
    }
}

#[inline]
fn hash64(key: &[u8]) -> u64 {
    const FNV_OFFSET: u64 = 0xcbf29ce484222325;
    const FNV_PRIME: u64 = 0x00000100000001b3;
    let mut z = FNV_OFFSET;
    for &b in key {
        z ^= b as u64;
        z = z.wrapping_mul(FNV_PRIME);
    }
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d049bb133111eb);
    z ^ (z >> 31)
}

impl Filter for BloomFilter {
    #[inline]
    fn may_contain(&self, key: &[u8]) -> bool {
        let (base, h1, h2) = self.block_and_seeds(key);
        let block = &self.bits[base..base + CACHE_LINE_BYTES];
        let mut h = h1;
        for _ in 0..self.num_hashes {
            let bit_pos = (h & 511) as usize;
            if block[bit_pos >> 3] & (1 << (bit_pos & 7)) == 0 {
                return false;
            }
            h = h.wrapping_add(h2);
        }
        true
    }

    #[inline]
    fn add(&mut self, key: &[u8]) {
        let (base, h1, h2) = self.block_and_seeds(key);
        let block = &mut self.bits[base..base + CACHE_LINE_BYTES];
        let mut h = h1;
        for _ in 0..self.num_hashes {
            let bit_pos = (h & 511) as usize;
            block[bit_pos >> 3] |= 1 << (bit_pos & 7);
            h = h.wrapping_add(h2);
        }
    }

    fn encode(&self) -> Vec<u8> {
        let mut raw = Vec::with_capacity(12 + self.bits.len() + 4);
        raw.extend_from_slice(&self.num_hashes.to_le_bytes());
        raw.extend_from_slice(&(self.num_bits as u64).to_le_bytes());
        raw.extend_from_slice(&self.bits);
        let crc = crc32fast::hash(&raw);
        raw.extend_from_slice(&crc.to_le_bytes());
        raw
    }

    fn decode(data: &[u8]) -> Result<Self> {
        if data.len() < 16 {
            return Err(Error::Corruption("bloom filter data too short".into()));
        }
        let num_hashes = u32::from_le_bytes(data[0..4].try_into().unwrap());
        let num_bits = u64::from_le_bytes(data[4..12].try_into().unwrap()) as usize;
        if num_hashes == 0 || num_hashes > 30 {
            return Err(Error::Corruption("invalid num_hashes".into()));
        }
        if num_bits < CACHE_LINE_BITS || !num_bits.is_multiple_of(CACHE_LINE_BITS) {
            return Err(Error::Corruption(
                "num_bits must be a positive multiple of 512".into(),
            ));
        }
        let expected_bytes = num_bits / 8;
        if data.len() != 16 + expected_bytes {
            return Err(Error::Corruption(
                "bloom filter data length mismatch".into(),
            ));
        }
        let body_end = 12 + expected_bytes;
        let expected_crc = u32::from_le_bytes(data[body_end..body_end + 4].try_into().unwrap());
        let computed_crc = crc32fast::hash(&data[..body_end]);
        if expected_crc != computed_crc {
            return Err(Error::Corruption("bloom filter CRC mismatch".into()));
        }
        Ok(Self {
            bits: data[12..body_end].to_vec(),
            num_hashes,
            num_bits,
            num_blocks: num_bits / CACHE_LINE_BITS,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 验证 BloomFilter 基础插入与查询功能
    #[test]
    fn test_bloom_filter_basic() {
        let mut f = BloomFilter::new(100, 0.01);
        f.add(b"hello");
        assert!(f.may_contain(b"hello"));
        assert!(!f.may_contain(b"missing"));
    }

    /// 验证 BloomFilter 保证零假阴性 (No False Negatives)
    #[test]
    fn test_bloom_filter_no_false_negatives() {
        let mut f = BloomFilter::new(1000, 0.01);
        let keys: Vec<Vec<u8>> = (0..1000).map(|i| format!("key_{i}").into_bytes()).collect();
        for k in &keys {
            f.add(k);
        }
        for k in &keys {
            assert!(
                f.may_contain(k),
                "false negative for {:?}",
                String::from_utf8_lossy(k)
            );
        }
    }

    /// 验证 BloomFilter 假阳性率符合预期参数
    #[test]
    fn test_bloom_filter_false_positive_rate() {
        let mut f = BloomFilter::new(10000, 0.01);
        for i in 0..10000 {
            f.add(format!("key_{i}").as_bytes());
        }
        let mut false_positives = 0;
        for i in 10000..20000 {
            if f.may_contain(format!("key_{i}").as_bytes()) {
                false_positives += 1;
            }
        }
        let rate = false_positives as f64 / 10000.0;
        assert!(rate < 0.02, "FPR {rate} >= 2%");
    }

    /// 验证 BloomFilter 序列化与反序列化 Roundtrip
    #[test]
    fn test_bloom_filter_encode_decode() {
        let mut f = BloomFilter::new(50, 0.01);
        f.add(b"a");
        f.add(b"b");
        let encoded = f.encode();
        let decoded = BloomFilter::decode(&encoded).unwrap();
        assert!(decoded.may_contain(b"a"));
        assert!(decoded.may_contain(b"b"));
        assert!(!decoded.may_contain(b"z"));
    }

    /// 验证空 BloomFilter 的安全查询行为
    #[test]
    fn test_bloom_filter_empty() {
        let f = BloomFilter::new(0, 0.01);
        assert!(!f.may_contain(b"any"));
    }

    /// 验证 BloomFilter 解码非法字节数据容错
    #[test]
    fn test_bloom_filter_decode_invalid() {
        assert!(BloomFilter::decode(&[]).is_err());
        assert!(BloomFilter::decode(&[0u8; 8]).is_err());
        let mut bad = BloomFilter::new(10, 0.01).encode();
        bad[0] = 0; // num_hashes = 0
        assert!(BloomFilter::decode(&bad).is_err());
    }

    /// 验证 BloomFilter 哈希溢出安全性
    #[test]
    fn test_bloom_filter_hash_overflow_safe() {
        let mut f = BloomFilter::new(5000, 0.01);
        for i in 0..5000u64 {
            f.add(&i.to_le_bytes());
        }
        for i in 0..5000u64 {
            assert!(f.may_contain(&i.to_le_bytes()));
        }
    }

    /// 验证 64-bit 哈希分散性与 512-bit 对齐
    #[test]
    fn test_cacheline_blocked_bloom_alignment_and_hash64() {
        let f = BloomFilter::new(10_000, 0.01);
        assert_eq!(f.num_bits() % CACHE_LINE_BITS, 0);
        let a = hash64(b"key");
        let b = hash64(b"other");
        assert_eq!(a, hash64(b"key"));
        assert_ne!(a, b);
    }
}
