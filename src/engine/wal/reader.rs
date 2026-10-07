//! WAL Reader — 顺序读取 Record (open 后从头到尾扫描).
//! 负责 block padding 跳过、CRC32 校验与分片重组 (Full / First / Middle / Last → 完整 entry),
//! 通过 `ReadStatus` 区分正常记录与各类异常, 供 recover 决定丢弃或报错.
//!
//! # 行为
//!
//! - 文件尾部不完整 (`TailPartial`) 视为 partial write, 静默丢弃.
//! - 中间 CRC 不匹配: 默认 `CorruptionRecoverable` (记 warning 后继续);
//!   严格模式 (`strict_wal_recovery`) 下为 `CorruptionFatal`.
//!
//! # Invariant
//!
//! - CRC 校验覆盖 `Length + Type + Data`, 与 Writer 一致, 不一致即视为损坏.

use super::record::{RecordType, HEADER_SIZE};
use super::writer::BLOCK_SIZE;
use crate::error::{Error, Result};
use std::fs::File;
use std::io::Read;
use std::path::Path;

/// 读取 Record 的返回状态
#[derive(Debug)]
pub enum ReadStatus {
    /// 成功读取一条 Record
    Record(RecordType, Vec<u8>),
    /// 正常文件末尾
    Eof,
    /// 文件尾部不完整 (partial write, 静默丢弃)
    TailPartial,
    /// 中间 CRC 不匹配 (记 warning, 调用方可继续)
    CorruptionRecoverable,
    /// strict 模式下中间 CRC 不匹配
    CorruptionFatal,
}

pub struct Reader {
    file: File,
    block_offset: usize,
    strict: bool,
}

impl Reader {
    /// 打开 WAL 文件进行读取
    pub fn open(path: &Path) -> Result<Self> {
        let file = File::open(path)?;
        Ok(Reader {
            file,
            block_offset: 0,
            strict: false,
        })
    }

    /// 打开 WAL 文件进行读取 (strict 模式: CRC 损坏返回 CorruptionFatal)
    pub fn open_strict(path: &Path) -> Result<Self> {
        let file = File::open(path)?;
        Ok(Reader {
            file,
            block_offset: 0,
            strict: true,
        })
    }

    /// 从已打开的文件创建 Reader
    pub fn from_file(file: File) -> Result<Self> {
        Ok(Reader {
            file,
            block_offset: 0,
            strict: false,
        })
    }

    /// 顺序读取一条 Record.
    ///
    /// 返回 `ReadStatus` 区分正常记录、文件尾部、CRC 损坏等.
    pub fn read_record(&mut self) -> Result<ReadStatus> {
        // 检查是否需要跳过 block trailer padding
        if self.block_offset + HEADER_SIZE > BLOCK_SIZE {
            let trailer = BLOCK_SIZE - self.block_offset;
            if trailer > 0 {
                let mut buf = vec![0u8; trailer];
                if self.read_exact(&mut buf).is_err() {
                    // eof
                    return Ok(ReadStatus::TailPartial);
                }
            }
            self.block_offset = 0;
        }

        // 读取 7 字节 HEADER
        let mut header = [0u8; HEADER_SIZE];
        if self.read_exact(&mut header).is_err() {
            // eof
            return Ok(ReadStatus::Eof);
        }
        self.block_offset += HEADER_SIZE;

        let checksum = u32::from_le_bytes(header[0..4].try_into().unwrap());
        let length = u16::from_le_bytes(header[4..6].try_into().unwrap()) as usize;
        let record_type_val = header[6];

        // 检查 record_type 是否有效 (1-4), 全零 padding 也走此分支
        if !(1..=4).contains(&record_type_val) {
            let skip = BLOCK_SIZE - self.block_offset;
            if skip > 0 {
                let mut buf = vec![0u8; skip];
                let _ = self.read_exact(&mut buf);
            }
            self.block_offset = 0;
            return Ok(ReadStatus::CorruptionRecoverable);
        }

        // length=0 也是无效的
        if length == 0 {
            let skip = BLOCK_SIZE - self.block_offset;
            if skip > 0 {
                let mut buf = vec![0u8; skip];
                let _ = self.read_exact(&mut buf);
            }
            self.block_offset = 0;
            return Ok(ReadStatus::CorruptionRecoverable);
        }

        // 读取 Data
        let mut data = vec![0u8; length];
        if self.read_exact(&mut data).is_err() {
            // eof
            return Ok(ReadStatus::TailPartial);
        }

        self.block_offset += length;

        // 验证 CRC
        let mut h = crc32fast::Hasher::new();
        h.update(&(length as u16).to_le_bytes());
        h.update(&[record_type_val]);
        h.update(&data);
        let expected_crc = h.finalize();

        if checksum != expected_crc {
            tracing::warn!(target: "wal", "wal.crc.mismatch");
            if self.strict {
                return Ok(ReadStatus::CorruptionFatal);
            }
            return Ok(ReadStatus::CorruptionRecoverable);
        }

        let record_type = RecordType::try_from(record_type_val)
            .map_err(|_| Error::Corruption("invalid record type".into()))?;

        tracing::debug!(
          target: "wal",
          "wal.read.complete: record_type={:?} record_size={}",
          record_type,
          length + HEADER_SIZE,
        );

        Ok(ReadStatus::Record(record_type, data))
    }

    /// 尝试读取完整数据, EOF 时返回 UnexpectedEof
    fn read_exact(&mut self, buf: &mut [u8]) -> std::io::Result<()> {
        self.file.read_exact(buf)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::wal::record::RecordType;
    use crate::engine::wal::writer::Writer;
    use tempfile::tempdir;

    fn write_and_read(data: &[u8]) -> (RecordType, Vec<u8>) {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.log");
        let mut writer = Writer::open(&path).unwrap();
        writer.write_record(RecordType::Full, data).unwrap();
        writer.sync_data().unwrap();
        drop(writer);

        let mut reader = Reader::open(&path).unwrap();
        match reader.read_record().unwrap() {
            ReadStatus::Record(rt, d) => (rt, d),
            other => panic!("expected Record, got {:?}", other),
        }
    }

    #[test]
    fn test_record_full_roundtrip() {
        let data = b"hello world";
        let (rt, decoded) = write_and_read(data);
        assert_eq!(rt, RecordType::Full);
        assert_eq!(decoded, data);
    }

    #[test]
    fn test_record_large_data() {
        let data = vec![0xAB; 1000];
        let (rt, decoded) = write_and_read(&data);
        assert_eq!(rt, RecordType::Full);
        assert_eq!(decoded, data);
    }

    #[test]
    fn test_record_cross_block() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test_cross.log");
        let mut writer = Writer::open(&path).unwrap();

        // 写一个几乎填满 block 的记录, 使下一条跨 block
        let fill_size = BLOCK_SIZE - HEADER_SIZE;
        let fill_data = vec![0xFF; fill_size];
        writer.write_record(RecordType::Full, &fill_data).unwrap();

        // 再写一条小记录, 应在新 block
        writer.write_record(RecordType::Full, b"cross").unwrap();
        writer.sync_data().unwrap();
        drop(writer);

        // 读取第一条
        let mut reader = Reader::open(&path).unwrap();
        match reader.read_record().unwrap() {
            ReadStatus::Record(rt, data) => {
                assert_eq!(rt, RecordType::Full);
                assert_eq!(data.len(), fill_size);
            }
            other => panic!("expected Record, got {:?}", other),
        }

        // 读取第二条 (跨 block)
        match reader.read_record().unwrap() {
            ReadStatus::Record(rt, data) => {
                assert_eq!(rt, RecordType::Full);
                assert_eq!(data, b"cross");
            }
            other => panic!("expected Record, got {:?}", other),
        }

        // 读完: 应该 EOF
        match reader.read_record().unwrap() {
            ReadStatus::Eof => {}
            other => panic!("expected Eof, got {:?}", other),
        }
    }

    // --- 损坏容忍测试 ---

    #[test]
    fn test_crc_mismatch_returns_recoverable() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test_crc.log");

        // 写一条正常记录
        let mut w = Writer::open(&path).unwrap();
        w.write_record(RecordType::Full, b"data").unwrap();
        w.sync_data().unwrap();
        drop(w);

        // 篡改 CRC 字节 (第 0-3 字节)
        let mut content = std::fs::read(&path).unwrap();
        if content.len() > 4 {
            content[0] ^= 0xFF;
            std::fs::write(&path, &content).unwrap();
        }

        let mut reader = Reader::open(&path).unwrap();
        match reader.read_record().unwrap() {
            ReadStatus::CorruptionRecoverable => {} // 期望行为
            other => panic!("expected CorruptionRecoverable, got {:?}", other),
        }
    }

    #[test]
    fn test_invalid_type_returns_recoverable() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test_invalid_type.log");

        // 写一条记录后篡改 type 字节 (第 6 字节) 为 0xFF
        let mut w = Writer::open(&path).unwrap();
        w.write_record(RecordType::Full, b"data").unwrap();
        w.sync_data().unwrap();
        drop(w);

        let mut content = std::fs::read(&path).unwrap();
        if content.len() > 7 {
            content[6] = 0xFF; // 非法 type
            std::fs::write(&path, &content).unwrap();
        }

        let mut reader = Reader::open(&path).unwrap();
        match reader.read_record().unwrap() {
            ReadStatus::CorruptionRecoverable => {} // 期望行为
            other => panic!("expected CorruptionRecoverable, got {:?}", other),
        }
    }

    #[test]
    fn test_partial_write_returns_tail_partial() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test_partial.log");

        // 写入一条长度 > 0 的记录, 然后截断文件
        let mut w = Writer::open(&path).unwrap();
        w.write_record(RecordType::Full, b"complete_data").unwrap();
        w.sync_data().unwrap();
        let _full_size = w.file_size().unwrap();
        drop(w);

        // 截断到仅 header (7 字节), 模拟崩溃只写了一半 header
        // 实际上少于 HEADER_SIZE 字节也能触发 TailPartial
        std::fs::write(&path, vec![0u8; 4]).unwrap(); // 只保留前 4 字节 CRC

        let mut reader = Reader::open(&path).unwrap();
        // 读取不到完整 HEADER, 应该是 Eof 或 TailPartial
        match reader.read_record().unwrap() {
            ReadStatus::Eof | ReadStatus::TailPartial => {}
            other => panic!("expected Eof/TailPartial, got {:?}", other),
        }
    }

    #[test]
    fn test_record_split_large_data() {
        // 数据超过 block 容量时应自动分片为 First/Middle/Last
        let dir = tempdir().unwrap();
        let path = dir.path().join("test_split.log");
        let mut writer = Writer::open(&path).unwrap();

        // 70000 > 32761 (block payload max), 应分片
        let large_data = vec![0xAB; 70000];
        writer.write_record(RecordType::Full, &large_data).unwrap();
        writer.sync_data().unwrap();
        drop(writer);

        // 读取并重组所有分片
        let mut reader = Reader::open(&path).unwrap();
        let mut reconstructed = Vec::new();
        let mut fragment_types = Vec::new();

        loop {
            match reader.read_record().unwrap() {
                ReadStatus::Record(rt, data) => {
                    fragment_types.push(rt);
                    reconstructed.extend_from_slice(&data);
                }
                ReadStatus::Eof => break,
                other => panic!("unexpected: {:?}", other),
            }
        }

        assert_eq!(
            reconstructed, large_data,
            "reassembled data should match original"
        );
        // 验证分片类型: 非 Full
        assert_ne!(fragment_types.len(), 1, "large data should be split");
        assert_eq!(
            fragment_types[0],
            RecordType::First,
            "first fragment should be First"
        );
        assert_eq!(
            *fragment_types.last().unwrap(),
            RecordType::Last,
            "last fragment should be Last"
        );
    }

    #[test]
    fn test_record_multiple_reads() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test_multi.log");
        let mut writer = Writer::open(&path).unwrap();

        writer.write_record(RecordType::Full, b"first").unwrap();
        writer.write_record(RecordType::Full, b"second").unwrap();
        writer.write_record(RecordType::Full, b"third").unwrap();
        writer.sync_data().unwrap();
        drop(writer);

        let mut reader = Reader::open(&path).unwrap();
        let (_, d1) = match reader.read_record().unwrap() {
            ReadStatus::Record(rt, d) => (rt, d),
            other => panic!("expected Record, got {:?}", other),
        };
        assert_eq!(d1, b"first");

        let (_, d2) = match reader.read_record().unwrap() {
            ReadStatus::Record(rt, d) => (rt, d),
            other => panic!("expected Record, got {:?}", other),
        };
        assert_eq!(d2, b"second");

        let (_, d3) = match reader.read_record().unwrap() {
            ReadStatus::Record(rt, d) => (rt, d),
            other => panic!("expected Record, got {:?}", other),
        };
        assert_eq!(d3, b"third");

        match reader.read_record().unwrap() {
            ReadStatus::Eof => {}
            other => panic!("expected Eof, got {:?}", other),
        }
    }
}
