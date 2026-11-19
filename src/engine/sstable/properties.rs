//! SSTable Properties Block — 统计信息.

use crate::error::{Error, Result};

/// SSTable 统计属性, 小端编码 (56B 定长头 + 变长首尾 key).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SstProperties {
    pub num_entries: u64,
    pub raw_key_size: u64,
    pub raw_value_size: u64,
    pub data_size: u64,
    pub index_size: u64,
    pub filter_size: u64,
    pub num_data_blocks: u64,
    pub smallest_key: Vec<u8>,
    pub largest_key: Vec<u8>,
}

impl SstProperties {
    /// 7 * u64 (56B) + smallest_len (4B) + largest_len (4B) = 64B.
    pub const MIN_ENCODED_SIZE: usize = 64;

    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(
            Self::MIN_ENCODED_SIZE + self.smallest_key.len() + self.largest_key.len(),
        );
        buf.extend_from_slice(&self.num_entries.to_le_bytes());
        buf.extend_from_slice(&self.raw_key_size.to_le_bytes());
        buf.extend_from_slice(&self.raw_value_size.to_le_bytes());
        buf.extend_from_slice(&self.data_size.to_le_bytes());
        buf.extend_from_slice(&self.index_size.to_le_bytes());
        buf.extend_from_slice(&self.filter_size.to_le_bytes());
        buf.extend_from_slice(&self.num_data_blocks.to_le_bytes());
        buf.extend_from_slice(&(self.smallest_key.len() as u32).to_le_bytes());
        buf.extend_from_slice(&self.smallest_key);
        buf.extend_from_slice(&(self.largest_key.len() as u32).to_le_bytes());
        buf.extend_from_slice(&self.largest_key);
        buf
    }

    pub fn decode(buf: &[u8]) -> Result<Self> {
        if buf.len() < Self::MIN_ENCODED_SIZE {
            return Err(Error::Corruption("SSTable properties too short".into()));
        }
        let num_entries = u64::from_le_bytes(buf[0..8].try_into().unwrap());
        let raw_key_size = u64::from_le_bytes(buf[8..16].try_into().unwrap());
        let raw_value_size = u64::from_le_bytes(buf[16..24].try_into().unwrap());
        let data_size = u64::from_le_bytes(buf[24..32].try_into().unwrap());
        let index_size = u64::from_le_bytes(buf[32..40].try_into().unwrap());
        let filter_size = u64::from_le_bytes(buf[40..48].try_into().unwrap());
        let num_data_blocks = u64::from_le_bytes(buf[48..56].try_into().unwrap());

        let mut pos = 56;
        let smallest_len = u32::from_le_bytes(buf[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;
        if pos + smallest_len + 4 > buf.len() {
            return Err(Error::Corruption(
                "SSTable properties smallest_key truncated".into(),
            ));
        }
        let smallest_key = buf[pos..pos + smallest_len].to_vec();
        pos += smallest_len;

        let largest_len = u32::from_le_bytes(buf[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;
        if pos + largest_len > buf.len() {
            return Err(Error::Corruption(
                "SSTable properties largest_key truncated".into(),
            ));
        }
        let largest_key = buf[pos..pos + largest_len].to_vec();

        Ok(Self {
            num_entries,
            raw_key_size,
            raw_value_size,
            data_size,
            index_size,
            filter_size,
            num_data_blocks,
            smallest_key,
            largest_key,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_properties_roundtrip() {
        let p = SstProperties {
            num_entries: 42,
            raw_key_size: 1024,
            raw_value_size: 4096,
            data_size: 8192,
            index_size: 256,
            filter_size: 128,
            num_data_blocks: 2,
            smallest_key: b"a\x00\x00\x00\x00\x00\x00\x00\x01".to_vec(),
            largest_key: b"z\x00\x00\x00\x00\x00\x00\x00\x01".to_vec(),
        };
        let e = p.encode();
        let d = SstProperties::decode(&e).unwrap();
        assert_eq!(d, p);
    }

    #[test]
    fn test_properties_decode_too_short() {
        assert!(SstProperties::decode(&[0u8; 8]).is_err());
    }
}
