//! WAL Record 格式: 磁盘上的物理记录单元, 也是 WAL 文件的最小读写单位.
//! Record 由 Header (7B) + Data 组成, Data 承载编码后的 `WalEntry`.
//!
//! # 布局
//!
//! ```text
//! Record   = [CRC32:4][Length:2 LE][Type:1][Data: Length B]
//! 分片链   = Full 单条承载 / First + Middle* + Last 跨 Record 承载超大 entry
//! WalEntry = [sequence:8 BE][op_type:1][has_value:1][key_len:2 LE][key][value_len:4 LE][value]
//! ```
//!
//! # Invariant
//!
//! - 单条 Record 的 Data 不超过 `BLOCK_SIZE - HEADER_SIZE` (32KB block 内, 超出自动分片).
//! - CRC32 覆盖 `Length + Type + Data` (不含 CRC 自身), 与 Reader / Writer 两侧一致.
//! - `OpType` 决定语义: `TypePut` / `TypeDelete` / `TypeDeleteRange` / `BatchStart` / `FileHeader`.

use crate::error::{Error, Result};

// WAL_HEADER_SIZE = 7 bytes (CRC32 4B + Length 2B + Type 1B)
pub const HEADER_SIZE: usize = 7;

/// Record 类型 (1 Byte)
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordType {
    Full = 1,   // 一条 Record 承载完整数据
    First = 2,  // 数据分片的第一片
    Middle = 3, // 数据分片的中间片
    Last = 4,   // 数据分片的最后一片
}

impl TryFrom<u8> for RecordType {
    type Error = Error;

    fn try_from(value: u8) -> Result<Self> {
        match value {
            1 => Ok(RecordType::Full),
            2 => Ok(RecordType::First),
            3 => Ok(RecordType::Middle),
            4 => Ok(RecordType::Last),
            _ => Err(Error::Corruption(format!("invalid record type: {}", value))),
        }
    }
}

/// 操作类型 (1 Byte)
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpType {
    TypePut = 0,         // put(key, value), has_value=true
    TypeDelete = 1,      // delete(key), has_value=false
    BatchStart = 2,      // WriteBatch 开始标记
    FileHeader = 3,      // WAL 文件头
    TypeDeleteRange = 4, // delete_range(start, end), has_value=true
}

impl TryFrom<u8> for OpType {
    type Error = Error;

    fn try_from(value: u8) -> Result<Self> {
        match value {
            0 => Ok(OpType::TypePut),
            1 => Ok(OpType::TypeDelete),
            2 => Ok(OpType::BatchStart),
            3 => Ok(OpType::FileHeader),
            4 => Ok(OpType::TypeDeleteRange),
            _ => Err(Error::Corruption(format!("invalid op type: {}", value))),
        }
    }
}

/// 编码后的 WalEntry (磁盘格式)
///
/// 磁盘编码:
/// ┌──────────────────────────────────────────────────────┐
/// │ sequence (8B BE) │ op_type (1B) │ has_value (1B)    │
/// │ key_len (2B LE) │ key ... │ [value_len (4B LE) │ v] │
/// └──────────────────────────────────────────────────────┘
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalEntry {
    pub sequence: u64,
    pub op_type: OpType,
    pub has_value: bool,
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
}

impl WalEntry {
    /// 校验 sequence 等不变式 (BatchStart/FileHeader 的 sequence=0 占位除外).
    pub fn validate(&self) -> Result<()> {
        match self.op_type {
            OpType::BatchStart | OpType::FileHeader => Ok(()),
            _ => crate::engine::memtable::check_sequence(self.sequence),
        }
    }

    /// 编码 WalEntry 为字节序列
    pub fn encode(&self) -> Vec<u8> {
        let key_len = self.key.len() as u16;
        let value_len = self.value.as_ref().map(|v| v.len() as u32).unwrap_or(0);

        let mut buf = Vec::with_capacity(
            8 + 1
                + 1
                + 2
                + self.key.len()
                + if self.has_value {
                    4 + value_len as usize
                } else {
                    0
                },
        );

        buf.extend_from_slice(&self.sequence.to_be_bytes()); // 8B BE
        buf.push(self.op_type as u8); // 1B
        buf.push(self.has_value as u8); // 1B
        buf.extend_from_slice(&key_len.to_le_bytes()); // 2B LE
        buf.extend_from_slice(&self.key); // key

        if self.has_value {
            if let Some(ref val) = self.value {
                buf.extend_from_slice(&value_len.to_le_bytes()); // 4B LE
                buf.extend_from_slice(val);
            } else {
                buf.extend_from_slice(&0u32.to_le_bytes());
            }
        }

        buf
    }

    /// 零堆分配序列化 WalEntry 到指定的输出缓冲区中 (借用切片, 无所有权转移)
    pub fn encode_to_buf(
        sequence: u64,
        op_type: OpType,
        has_value: bool,
        key: &[u8],
        value: Option<&[u8]>,
        buf: &mut Vec<u8>,
    ) {
        let key_len = key.len() as u16;
        let value_len = value.map(|v| v.len() as u32).unwrap_or(0);

        let needed = 8 + 1 + 1 + 2 + key.len() + if has_value { 4 + value_len as usize } else { 0 };
        buf.reserve(needed);

        buf.extend_from_slice(&sequence.to_be_bytes()); // 8B BE
        buf.push(op_type as u8); // 1B
        buf.push(has_value as u8); // 1B
        buf.extend_from_slice(&key_len.to_le_bytes()); // 2B LE
        buf.extend_from_slice(key); // key

        if has_value {
            if let Some(val) = value {
                buf.extend_from_slice(&value_len.to_le_bytes()); // 4B LE
                buf.extend_from_slice(val);
            } else {
                buf.extend_from_slice(&0u32.to_le_bytes());
            }
        }
    }

    /// 从字节序列解码 WalEntry
    pub fn decode(data: &[u8]) -> Result<Self> {
        if data.len() < 12 {
            return Err(Error::Corruption("WalEntry too short".into()));
        }

        let mut offset = 0;

        // sequence (8B BE)
        if offset + 8 > data.len() {
            return Err(Error::Corruption("missing sequence".into()));
        }
        let sequence = u64::from_be_bytes(data[offset..offset + 8].try_into().unwrap());
        offset += 8;

        // op_type (1B)
        if offset + 1 > data.len() {
            return Err(Error::Corruption("missing op_type".into()));
        }
        let op_type = OpType::try_from(data[offset])?;
        offset += 1;

        // has_value (1B)
        if offset + 1 > data.len() {
            return Err(Error::Corruption("missing has_value".into()));
        }
        let has_value = data[offset] != 0;
        offset += 1;

        // key_len (2B LE)
        if offset + 2 > data.len() {
            return Err(Error::Corruption("missing key_len".into()));
        }
        let key_len = u16::from_le_bytes(data[offset..offset + 2].try_into().unwrap()) as usize;
        offset += 2;

        // key
        if offset + key_len > data.len() {
            return Err(Error::Corruption("key extends beyond data".into()));
        }
        let key = data[offset..offset + key_len].to_vec();
        offset += key_len;

        // value (optional)
        let value = if has_value {
            if offset + 4 > data.len() {
                return Err(Error::Corruption("missing value_len".into()));
            }
            let value_len =
                u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
            offset += 4;
            if offset + value_len > data.len() {
                return Err(Error::Corruption("value extends beyond data".into()));
            }
            Some(data[offset..offset + value_len].to_vec())
        } else {
            None
        };

        // 验证 op_type 不变式
        match op_type {
            OpType::TypePut if !has_value => {
                return Err(Error::Corruption("TypePut must have value".into()));
            }
            OpType::TypeDelete if has_value => {
                return Err(Error::Corruption("TypeDelete must not have value".into()));
            }
            OpType::TypeDeleteRange if !has_value => {
                return Err(Error::Corruption("TypeDeleteRange must have value".into()));
            }
            OpType::BatchStart => {
                if !has_value {
                    return Err(Error::Corruption("BatchStart must have value".into()));
                }
                if !key.is_empty() {
                    return Err(Error::Corruption("BatchStart key must be empty".into()));
                }
                if let Some(ref v) = value {
                    if v.len() != 4 {
                        return Err(Error::Corruption(format!(
                            "BatchStart value len {} != 4",
                            v.len()
                        )));
                    }
                }
            }
            OpType::FileHeader => {
                if !has_value {
                    return Err(Error::Corruption("FileHeader must have value".into()));
                }
                if key != b"WAL" {
                    return Err(Error::Corruption("FileHeader key must be 'WAL'".into()));
                }
                if let Some(ref v) = value {
                    if v.len() != 25 {
                        return Err(Error::Corruption(format!(
                            "FileHeader value len {} != 25",
                            v.len()
                        )));
                    }
                }
            }
            _ => {}
        }

        let entry = WalEntry {
            sequence,
            op_type,
            has_value,
            key,
            value,
        };
        entry.validate()?;
        Ok(entry)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_entry_encode_decode_put() {
        let entry = WalEntry {
            sequence: 42,
            op_type: OpType::TypePut,
            has_value: true,
            key: b"hello".to_vec(),
            value: Some(b"world".to_vec()),
        };

        let encoded = entry.encode();
        let decoded = WalEntry::decode(&encoded).unwrap();

        assert_eq!(decoded.sequence, 42);
        assert_eq!(decoded.op_type, OpType::TypePut);
        assert!(decoded.has_value);
        assert_eq!(decoded.key, b"hello");
        assert_eq!(decoded.value, Some(b"world".to_vec()));
    }

    #[test]
    fn test_entry_encode_decode_delete() {
        let entry = WalEntry {
            sequence: 1,
            op_type: OpType::TypeDelete,
            has_value: false,
            key: b"to_delete".to_vec(),
            value: None,
        };

        let encoded = entry.encode();
        let decoded = WalEntry::decode(&encoded).unwrap();

        assert_eq!(decoded.sequence, 1);
        assert_eq!(decoded.op_type, OpType::TypeDelete);
        assert!(!decoded.has_value);
        assert_eq!(decoded.key, b"to_delete");
        assert_eq!(decoded.value, None);
    }

    #[test]
    fn test_entry_sequence_big_endian() {
        let entry = WalEntry {
            sequence: 0x0102030405060708,
            op_type: OpType::TypePut,
            has_value: true,
            key: b"k".to_vec(),
            value: Some(b"v".to_vec()),
        };

        let encoded = entry.encode();
        // 前 8 字节应该是 Big-Endian 的 sequence
        assert_eq!(
            &encoded[..8],
            &[0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08]
        );
    }

    #[test]
    fn test_entry_decode_too_short() {
        // 空数据
        assert!(WalEntry::decode(&[]).is_err());
        // 只有 sequence (8B), 缺后面的字段
        assert!(WalEntry::decode(&[0u8; 8]).is_err());
        // 刚好 11 字节 (不足 12 的最小长度)
        assert!(WalEntry::decode(&[0u8; 11]).is_err());
        // 12 字节足够最小结构, 但后续 key_len 超出范围
        let mut data = 0u64.to_be_bytes().to_vec();
        data.push(0x00); // op_type = TypePut
        data.push(0x00); // has_value = false
        data.extend_from_slice(&5u16.to_le_bytes()); // key_len = 5
                                                     // 但 data 中没有 key 数据 → 解码失败
        assert!(WalEntry::decode(&data).is_err());
    }

    #[test]
    fn test_entry_decode_unknown_op_type() {
        // 构造合法的 sequence + 非法 op_type (跳过长度检查)
        let mut data = Vec::new();
        data.extend_from_slice(&0u64.to_be_bytes()); // 8B sequence
        data.push(0xFF); // 非法 op_type
        data.push(0x00); // has_value = false
        data.extend_from_slice(&0u16.to_le_bytes()); // key_len = 0
        let result = WalEntry::decode(&data);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("invalid op type"));
    }

    #[test]
    fn test_batch_entry() {
        // WriteBatch batch start 标记
        let batch_entry = WalEntry {
            sequence: 0,
            op_type: OpType::BatchStart,
            has_value: true,
            key: vec![],
            value: Some(2u32.to_le_bytes().to_vec()), // batch_size = 2
        };
        let encoded = batch_entry.encode();
        let decoded = WalEntry::decode(&encoded).unwrap();
        assert_eq!(decoded.op_type, OpType::BatchStart);
        assert_eq!(decoded.value.unwrap().as_slice(), &2u32.to_le_bytes());

        // batch 中的 put
        let put = WalEntry {
            sequence: 100,
            op_type: OpType::TypePut,
            has_value: true,
            key: b"a".to_vec(),
            value: Some(b"1".to_vec()),
        };
        let encoded = put.encode();
        let decoded = WalEntry::decode(&encoded).unwrap();
        assert_eq!(decoded.sequence, 100);
        assert_eq!(decoded.op_type, OpType::TypePut);

        // batch 中的 delete
        let del = WalEntry {
            sequence: 101,
            op_type: OpType::TypeDelete,
            has_value: false,
            key: b"b".to_vec(),
            value: None,
        };
        let encoded = del.encode();
        let decoded = WalEntry::decode(&encoded).unwrap();
        assert_eq!(decoded.op_type, OpType::TypeDelete);
        assert_eq!(decoded.key, b"b");
    }

    #[test]
    fn test_entry_sequence_overflow_rejected() {
        let entry = WalEntry {
            sequence: 1 << 56,
            op_type: OpType::TypePut,
            has_value: true,
            key: b"k".to_vec(),
            value: Some(b"v".to_vec()),
        };
        assert!(entry.validate().is_err());
        let encoded = entry.encode();
        assert!(WalEntry::decode(&encoded).is_err());
    }

    #[test]
    fn test_wal_delete_range_encode_decode() {
        let entry = WalEntry {
            sequence: 42,
            op_type: OpType::TypeDeleteRange,
            has_value: true,
            key: b"start_key".to_vec(),
            value: Some(b"end_key".to_vec()),
        };
        let encoded = entry.encode();
        let decoded = WalEntry::decode(&encoded).unwrap();
        assert_eq!(decoded, entry);
    }

    #[test]
    fn test_max_key_value_length() {
        // key = 65535 (u16 上限)
        let large_key = vec![0xAB; 65535];
        let entry = WalEntry {
            sequence: 1,
            op_type: OpType::TypePut,
            has_value: true,
            key: large_key.clone(),
            value: Some(b"v".to_vec()),
        };
        let encoded = entry.encode();
        let decoded = WalEntry::decode(&encoded).unwrap();
        assert_eq!(decoded.key.len(), 65535);
        assert_eq!(decoded.key, large_key);

        // value 超过 65535 (通过 Record 分片写入)
        // WalEntry 编码本身无上限, 只要内存能容纳
        let large_val = vec![0xCD; 100000];
        let entry = WalEntry {
            sequence: 2,
            op_type: OpType::TypePut,
            has_value: true,
            key: b"k".to_vec(),
            value: Some(large_val.clone()),
        };
        let encoded = entry.encode();
        let decoded = WalEntry::decode(&encoded).unwrap();
        let val = decoded.value.unwrap();
        assert_eq!(val.len(), 100000);
        assert_eq!(val, large_val);
    }

    #[test]
    fn test_encode_to_buf_matches_encode() {
        let entry = WalEntry {
            sequence: 12345678,
            op_type: OpType::TypePut,
            has_value: true,
            key: b"test_key".to_vec(),
            value: Some(b"test_value".to_vec()),
        };
        let expected = entry.encode();
        let mut buf = Vec::new();
        WalEntry::encode_to_buf(
            entry.sequence,
            entry.op_type,
            entry.has_value,
            &entry.key,
            entry.value.as_deref(),
            &mut buf,
        );
        assert_eq!(buf, expected);

        // Delete test
        let del_entry = WalEntry {
            sequence: 98765432,
            op_type: OpType::TypeDelete,
            has_value: false,
            key: b"del_key".to_vec(),
            value: None,
        };
        let del_expected = del_entry.encode();
        buf.clear();
        WalEntry::encode_to_buf(
            del_entry.sequence,
            del_entry.op_type,
            del_entry.has_value,
            &del_entry.key,
            None,
            &mut buf,
        );
        assert_eq!(buf, del_expected);
    }
}
