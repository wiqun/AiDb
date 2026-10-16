//! WAL Writer — 追加写入 Record: 分片、32KB block padding、CRC32 计算、磁盘预分配与 sync.
//!
//! # 布局
//!
//! ```text
//! Record = [CRC32:4][Length:2 LE][Type:1][Data]
//! Block  = 32KB (BLOCK_SIZE), Record 不跨 block:
//!          当前 block 剩余空间不足时以 0x00 填充 padding 后切到新 block
//! 超大 data (> 65535 或超出 block 容量) 分片为 First / Middle / Last
//! ```
//!
//! Linux 下通过 `fallocate` 预分配磁盘空间 (不改变文件大小), 避免写放大.
//!
//! # Invariant
//!
//! - 单片 Record (header + data) 必须 <= `BLOCK_SIZE`, 分片链由 Reader 重组.
//! - CRC32 覆盖 `Length(2B) + Type(1B) + Data`.

use super::record::{RecordType, HEADER_SIZE};
use crate::error::Result;
use std::io::{Seek, SeekFrom, Write};
use std::path::Path;

// `fallocate` 系统调用, 用于预分配磁盘空间.
// `FALLOC_FL_KEEP_SIZE = 0x01` 确保分配空间但不改变文件大小.
#[cfg(target_os = "linux")]
extern "C" {
    fn fallocate(fd: i32, mode: i32, offset: i64, len: i64) -> i32;
}

#[cfg(target_os = "linux")]
const FALLOC_FL_KEEP_SIZE: i32 = 0x01;

/// 预分配文件磁盘空间, 不改变文件大小.
/// 非 Linux 平台无操作.
#[cfg(target_os = "linux")]
fn preallocate_file(file: &std::fs::File, size: u64) -> Result<()> {
    use std::os::unix::io::AsRawFd;
    let ret = unsafe { fallocate(file.as_raw_fd(), FALLOC_FL_KEEP_SIZE, 0, size as i64) };
    if ret != 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn preallocate_file(_file: &std::fs::File, _size: u64) -> Result<()> {
    Ok(())
}

/// WAL 默认 block 大小
pub const BLOCK_SIZE: usize = 32768;

/// 预分配零填充 buffer, 避免每次 padding 分配 vec (F-014).
static ZEROS: [u8; BLOCK_SIZE] = [0u8; BLOCK_SIZE];

pub struct Writer {
    file: std::fs::File,
    block_offset: usize,
}

impl Writer {
    pub fn open(path: &Path) -> Result<Self> {
        Self::open_inner(path, None)
    }

    /// 打开 WAL 文件并预分配 `preallocate_bytes` 字节磁盘空间.
    /// `preallocate_bytes` 为 0 或 None 时跳过预分配.
    pub fn open_with_preallocate(path: &Path, preallocate_bytes: u64) -> Result<Self> {
        let pre = if preallocate_bytes > 0 {
            Some(preallocate_bytes)
        } else {
            None
        };
        Self::open_inner(path, pre)
    }

    /// 兼容旧接口: sync_wal 由上层 WALManager 管理, Writer 不再负责 syncing.
    pub fn open_with_sync(path: &Path, _sync_wal: bool) -> Result<Self> {
        Self::open(path)
    }

    /// 兼容旧接口: sync_wal 由上层管理, 同时支持预分配.
    pub fn open_with_sync_preallocate(
        path: &Path,
        _sync_wal: bool,
        preallocate_bytes: u64,
    ) -> Result<Self> {
        Self::open_with_preallocate(path, preallocate_bytes)
    }

    fn open_inner(path: &Path, preallocate: Option<u64>) -> Result<Self> {
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(path)?;

        if let Some(size) = preallocate {
            preallocate_file(&file, size)?;
        }

        let block_offset = (file.metadata()?.len() % BLOCK_SIZE as u64) as usize;
        Ok(Writer { file, block_offset })
    }

    /// 单条 Record 的 data 最大长度 (Length 字段为 u16)
    const MAX_RECORD_DATA: usize = 65535;

    /// 追加一条 Record.
    ///
    /// `data` 是已编码的 WalEntry 字节.
    /// 如果 data 长度超过 block 容量或 65535, 自动分片为 First/Middle/Last.
    /// 每片写入前检查 block 剩余空间, 不足时填充 0x00 padding 后切到新 block.
    pub fn write_record(&mut self, record_type: RecordType, data: &[u8]) -> Result<()> {
        let max_payload = BLOCK_SIZE - HEADER_SIZE;
        let total = data.len();
        let mut offset = 0;
        let mut fragment_index = 0;

        while offset < total {
            // 当前 block 剩余空间不足以放下最小 HEADER 时, 填充 padding
            if self.block_offset + HEADER_SIZE >= BLOCK_SIZE {
                let remaining = BLOCK_SIZE - self.block_offset;
                if remaining > 0 {
                    self.file.write_all(&ZEROS[..remaining])?;
                }
                self.block_offset = 0;
            }

            // 当前 block 剩余容量
            let block_avail = BLOCK_SIZE - self.block_offset - HEADER_SIZE;
            // 单片最大长度: 受 block 剩余空间和 u16 Length 字段双重约束
            let max_chunk = max_payload.min(Self::MAX_RECORD_DATA);
            let chunk_size = (total - offset).min(max_chunk).min(block_avail);

            let end = offset + chunk_size;
            let chunk = &data[offset..end];
            offset = end;

            // 确定 RecordType: 根据分片位置
            let is_first_fragment = fragment_index == 0;
            let is_last_fragment = offset >= total;

            let frag_type = if is_first_fragment && is_last_fragment {
                record_type // 整个数据一次性写入, 使用调用方指定的类型
            } else if is_first_fragment {
                RecordType::First
            } else if is_last_fragment {
                RecordType::Last
            } else {
                RecordType::Middle
            };

            self.write_one_record(frag_type, chunk)?;
            fragment_index += 1;
        }

        Ok(())
    }

    /// 写一条完整的单块 Record (自动处理 block padding).
    /// data 长度必须 ≤ BLOCK_SIZE - HEADER_SIZE.
    fn write_one_record(&mut self, record_type: RecordType, data: &[u8]) -> Result<()> {
        tracing::debug!(target: "wal", "wal.write.start: record_type={:?} record_size={}", record_type, data.len() + HEADER_SIZE);
        let data_len = data.len();
        let needed = HEADER_SIZE + data_len;

        // 数据必须能放进一个 block
        assert!(
            needed <= BLOCK_SIZE,
            "record {} bytes > block size {}",
            needed,
            BLOCK_SIZE
        );

        // 当前 block 剩余空间不足时, 填充 padding 并重置 block_offset
        if BLOCK_SIZE - self.block_offset < needed {
            let remaining = BLOCK_SIZE - self.block_offset;
            if remaining > 0 {
                self.file.write_all(&ZEROS[..remaining])?;
            }
            self.block_offset = 0;
        }

        // 计算 CRC32: 覆盖 Length(2B) + Type(1B) + Data
        let mut h = crc32fast::Hasher::new();
        h.update(&(data_len as u16).to_le_bytes());
        h.update(&[record_type as u8]);
        h.update(data);
        let crc = h.finalize();

        // 合并 header + data 为单次 write (F-012): 4 次 write_all → 1 次 syscall
        let mut buf = Vec::with_capacity(HEADER_SIZE + data_len);
        buf.extend_from_slice(&crc.to_le_bytes());
        buf.extend_from_slice(&(data_len as u16).to_le_bytes());
        buf.extend_from_slice(&[record_type as u8]);
        buf.extend_from_slice(data);
        self.file.write_all(&buf)?;

        self.block_offset += HEADER_SIZE + data_len;

        tracing::debug!(
          target: "wal",
          "wal.write.complete: record_type={:?} record_size={}",
          record_type,
          data_len + HEADER_SIZE,
        );

        Ok(())
    }

    /// Flush 文件缓冲区
    pub fn flush(&mut self) -> Result<()> {
        tracing::debug!(target: "wal", "wal.flush.start");
        self.file.flush()?;
        tracing::debug!(target: "wal", "wal.flush.complete");
        Ok(())
    }

    /// fdatasync — 仅 data+size, 跳过 mtime 元数据同步 (F-013)
    pub fn sync_data(&mut self) -> Result<()> {
        tracing::debug!(target: "wal", "wal.sync.start");
        self.file.sync_data()?;
        tracing::debug!(target: "wal", "wal.sync.complete");
        Ok(())
    }

    /// 当前 block 偏移
    pub fn block_offset(&self) -> usize {
        self.block_offset
    }

    /// 获取文件大小
    pub fn file_size(&self) -> Result<u64> {
        Ok(self.file.metadata()?.len())
    }

    /// 估算一条 Record 落盘字节数 (含 block padding), 以及写入后的 block_offset.
    pub(crate) fn estimated_record_disk_bytes(data: &[u8], block_offset: usize) -> (u64, usize) {
        let max_payload = BLOCK_SIZE - HEADER_SIZE;
        let total = data.len();
        let mut offset = 0;
        let mut block_off = block_offset;
        let mut disk = 0u64;

        while offset < total {
            if block_off + HEADER_SIZE >= BLOCK_SIZE {
                disk += (BLOCK_SIZE - block_off) as u64;
                block_off = 0;
            }
            let block_avail = BLOCK_SIZE - block_off - HEADER_SIZE;
            let max_chunk = max_payload.min(Self::MAX_RECORD_DATA);
            let chunk_size = (total - offset).min(max_chunk).min(block_avail);
            disk += (HEADER_SIZE + chunk_size) as u64;
            block_off += HEADER_SIZE + chunk_size;
            offset += chunk_size;
        }
        (disk, block_off)
    }

    /// 获取底层文件句柄 (用于 Reader)
    pub fn file(&self) -> &std::fs::File {
        &self.file
    }

    /// 重建 Reader 用
    pub fn try_clone_file(&self) -> Result<std::fs::File> {
        // 需要读权限, 以 read-write 方式打开
        // 但这里我们只是尝试副本
        Ok(self.file.try_clone()?)
    }
}

impl Write for Writer {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.file.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.file.flush()
    }
}

impl Seek for Writer {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        let new_pos = self.file.seek(pos)?;
        // 重新计算 block_offset (仅在确定性 seek 后有效)
        self.block_offset = (new_pos as usize) % BLOCK_SIZE;
        Ok(new_pos)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::wal::record::RecordType;
    use tempfile::tempdir;

    #[test]
    fn test_writer_creates_file() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("wal_test.log");
        let mut writer = Writer::open(&path).unwrap();
        assert!(path.exists());
        writer.write_record(RecordType::Full, b"hello").unwrap();
        writer.sync_data().unwrap();
        assert!(writer.file_size().unwrap() > 0);
    }

    #[test]
    fn test_writer_write_and_file_size() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test_write.log");
        let mut writer = Writer::open(&path).unwrap();
        // 一个 Full record: 7B header + 5B data = 12 bytes
        writer.write_record(RecordType::Full, b"hello").unwrap();
        assert_eq!(writer.file_size().unwrap(), (HEADER_SIZE + 5) as u64);
    }

    #[test]
    fn test_writer_block_padding() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test_padding.log");
        let mut writer = Writer::open(&path).unwrap();
        // 填满当前 block
        let data = vec![0u8; BLOCK_SIZE - HEADER_SIZE]; // 刚好塞满一个 block
        writer.write_record(RecordType::Full, &data).unwrap();
        // block 已满, block_offset 应指向 block 末尾
        assert!(writer.block_offset >= BLOCK_SIZE);
        // 再写一条, 应产生 padding (32768 - 32768 = 0, 即填充一个完整的 block trailer)
        writer.write_record(RecordType::Full, b"x").unwrap();
        // 第二条记录的 HEADER + 1 byte data 在新 block 中
        // 文件大小 = 32768 (第一个block, 含 padding) + HEADER_SIZE + 1
        assert_eq!(
            writer.file_size().unwrap(),
            (BLOCK_SIZE + HEADER_SIZE + 1) as u64
        );
    }
}
