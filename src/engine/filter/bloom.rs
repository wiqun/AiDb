//! BloomFilter — SSTable 级布隆过滤器: FNV-1a 双哈希, 编码内嵌 CRC32.
//!
//! # 哈希方案
//!
//! ```text
//! h1 = fnv1a_like(key, 0xbc9f1d34);  h2 = fnv1a_like(key, 0xd0e89c7b) (强制奇数)
//! pos_i = (h1 + i × h2) % num_bits,  i ∈ [0, num_hashes)
//! ```
//!
//! bits/hashes 按期望 key 数与目标 false positive rate 取最优; 磁盘编码
//! `[num_hashes:4][num_bits:8][bits][crc32:4]`, `decode` 校验长度与 CRC.
//!
//! # Invariant
//!
//! - 仅索引 user_key (`add(extract_user_key(key))`), 不带 sequence.
//! - miss 即"确定不存在" (免 I/O); 解码失败由调用方降级为无 filter, 不影响正确性.

use std::sync::atomic::{AtomicU64, Ordering};

use crate::error::{Error, Result};

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
}

impl BloomFilter {
    pub fn new(expected_keys: usize, false_positive_rate: f64) -> Self {
        if expected_keys == 0 {
            return Self::with_bits_and_hashes(64, 1);
        }
        let fp = false_positive_rate.clamp(0.0001, 0.9999);
        let num_bits = Self::optimal_num_bits(expected_keys, fp);
        let num_hashes = Self::optimal_num_hashes(num_bits, expected_keys);
        Self::with_bits_and_hashes(num_bits, num_hashes)
    }

    pub fn default_with_keys(num_keys: usize) -> Self {
        Self::new(num_keys, 0.01)
    }

    fn with_bits_and_hashes(num_bits: usize, num_hashes: u32) -> Self {
        let num_bytes = num_bits.div_ceil(8);
        Self {
            bits: vec![0u8; num_bytes],
            num_hashes,
            num_bits,
        }
    }

    fn optimal_num_bits(expected_keys: usize, false_positive_rate: f64) -> usize {
        let n = expected_keys as f64;
        let p = false_positive_rate;
        let num_bits = (-n * p.ln() / (2.0_f64.ln().powi(2))).ceil() as usize;
        num_bits.max(64)
    }

    fn optimal_num_hashes(num_bits: usize, expected_keys: usize) -> u32 {
        if expected_keys == 0 {
            return 1;
        }
        let k = ((num_bits as f64 / expected_keys as f64) * 2.0_f64.ln()).round() as u32;
        k.clamp(1, 30)
    }

    fn hash_positions(&self, key: &[u8]) -> impl Iterator<Item = usize> + '_ {
        let h1 = fnv1a_like(key, 0xbc9f1d34);
        let mut h2 = fnv1a_like(key, 0xd0e89c7b);
        if h2.is_multiple_of(2) {
            h2 = h2.wrapping_add(1);
        }
        (0..self.num_hashes).map(move |i| {
            let hash = h1.wrapping_add(i.wrapping_mul(h2));
            (hash as usize) % self.num_bits
        })
    }

    pub fn num_hashes(&self) -> u32 {
        self.num_hashes
    }

    pub fn num_bits(&self) -> usize {
        self.num_bits
    }
}

fn fnv1a_like(key: &[u8], seed: u32) -> u32 {
    const FNV_OFFSET: u32 = 0x811c9dc5;
    const FNV_PRIME: u32 = 0x01000193;
    let mut hash = FNV_OFFSET ^ seed;
    for &b in key {
        hash ^= b as u32;
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

impl Filter for BloomFilter {
    fn may_contain(&self, key: &[u8]) -> bool {
        for pos in self.hash_positions(key) {
            if self.bits[pos / 8] & (1 << (pos % 8)) == 0 {
                return false;
            }
        }
        true
    }

    fn add(&mut self, key: &[u8]) {
        let num_hashes = self.num_hashes;
        let num_bits = self.num_bits;
        let h1 = fnv1a_like(key, 0xbc9f1d34);
        let mut h2 = fnv1a_like(key, 0xd0e89c7b);
        if h2.is_multiple_of(2) {
            h2 = h2.wrapping_add(1);
        }
        for i in 0..num_hashes {
            let hash = h1.wrapping_add(i.wrapping_mul(h2));
            let pos = (hash as usize) % num_bits;
            self.bits[pos / 8] |= 1 << (pos % 8);
        }
    }

    fn encode(&self) -> Vec<u8> {
        let mut raw = Vec::with_capacity(12 + self.bits.len());
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
        if num_bits < 64 {
            return Err(Error::Corruption("num_bits too small".into()));
        }
        let expected_bytes = num_bits.div_ceil(8);
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

    /// 验证 FNV-1a 种子哈希算法分散性
    #[test]
    fn test_fnv_hasher() {
        let a = fnv1a_like(b"key", 0xbc9f1d34);
        let b = fnv1a_like(b"key", 0xd0e89c7b);
        let c = fnv1a_like(b"other", 0xbc9f1d34);
        assert_eq!(a, fnv1a_like(b"key", 0xbc9f1d34));
        assert_ne!(a, b);
        assert_ne!(a, c);
    }
}
