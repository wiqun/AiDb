//! Block 落盘: compression type + CRC32 trailer.

use std::io::{Read, Seek, SeekFrom, Write};
use std::sync::atomic::Ordering;
use std::sync::Arc;

use bytes::Bytes;
use crc32fast::Hasher as Crc32Hasher;

use crate::config::CompressionType;
use crate::engine::cache::{BlockCache, CacheKey};
use crate::error::{Error, Result};
use crate::statistics::Statistics;

use super::handle::BlockHandle;
use super::BLOCK_TRAILER_SIZE;

pub const COMPRESSION_NONE: u8 = 0;

fn skip_checksum() -> bool {
    std::env::var("AIDB_SKIP_CHECKSUM").ok().as_deref() == Some("1")
}

/// 落盘一个 block, 返回 **实际写入磁盘的 payload 长度** (不含 trailer).
///
/// 压缩启用时 payload 是压缩后数据, 长度可能小于 `block_data.len()`; 调用方
/// 必须用这个返回值 (而非 `block_data.len()`) 计算 `BlockHandle.size`/累加
/// `data_block_offset`, 否则读侧会按错误的偏移拆 trailer, 读到垃圾
/// `compression_type`/`crc` 并报 `Corruption`.
pub fn write_block<W: Write>(
    writer: &mut W,
    block_data: &[u8],
    compression: CompressionType,
) -> Result<u64> {
    let compression_byte = compression_to_byte(compression)?;
    let payload_len = if compression == CompressionType::None {
        writer.write_all(block_data)?;
        block_data.len() as u64
    } else {
        let data = compress_block(block_data, compression)?;
        writer.write_all(&data)?;
        data.len() as u64
    };
    writer.write_all(&[compression_byte])?;
    let mut hasher = Crc32Hasher::new();
    hasher.update(block_data);
    writer.write_all(&hasher.finalize().to_le_bytes())?;
    Ok(payload_len)
}

/// 带 BlockCache 的 Data Block 读取; miss 时读盘并 insert.
pub fn read_block_cached(
    file: &std::fs::File,
    file_number: u64,
    handle: &BlockHandle,
    block_cache: Option<&Arc<BlockCache>>,
    stats: Option<&Arc<Statistics>>,
) -> Result<Bytes> {
    let key = CacheKey {
        file_number,
        offset: handle.offset,
    };
    // 触碰计数: hit 与 miss 双路径均计 1, 与 block_read_bytes (仅 miss 落盘字节) 互补,
    // 前者为经典读放大 RA 分子, 后者为读穿透字节比分子
    if let Some(s) = stats {
        s.block_seek_count.fetch_add(1, Ordering::Relaxed);
    }
    if let Some(cache) = block_cache {
        if let Some(bytes) = cache.get(key.clone()) {
            return Ok(bytes);
        }
    }

    let data = read_block_from_file_traced(file, handle)?;
    if let Some(s) = stats {
        s.block_read_bytes.fetch_add(handle.size, Ordering::Relaxed);
    }
    let bytes = Bytes::from(data);
    if let Some(cache) = block_cache {
        cache.insert(key, bytes.clone());
    }
    Ok(bytes)
}

/// 从 `File` 以 pread 语义读取 block, 支持 `Arc<File>` 安全并发读.
pub fn read_block_from_file(file: &std::fs::File, handle: &BlockHandle) -> Result<Vec<u8>> {
    use std::os::unix::fs::FileExt;
    let mut raw = vec![0u8; handle.size as usize];
    file.read_at(&mut raw, handle.offset)?;
    parse_block_bytes(&raw, handle)
}

fn read_block_from_file_traced(file: &std::fs::File, handle: &BlockHandle) -> Result<Vec<u8>> {
    read_block_from_file(file, handle)
}

/// 通用 `Read + Seek` reader 的 block 读取 (非并发安全).
pub fn read_block_bytes<R: Read + Seek>(reader: &mut R, handle: &BlockHandle) -> Result<Vec<u8>> {
    reader.seek(SeekFrom::Start(handle.offset))?;
    let mut raw = vec![0u8; handle.size as usize];
    reader.read_exact(&mut raw)?;
    parse_block_bytes(&raw, handle)
}

/// 解析 raw block 数据: 校验 size、CRC、解压.
fn parse_block_bytes(raw: &[u8], handle: &BlockHandle) -> Result<Vec<u8>> {
    if handle.size < BLOCK_TRAILER_SIZE as u64 {
        return Err(Error::Corruption(format!(
            "block size {} < trailer",
            handle.size
        )));
    }
    let data_len = raw.len() - BLOCK_TRAILER_SIZE;
    let raw_data = &raw[..data_len];
    let compression_type = raw[data_len];
    let stored_crc = u32::from_le_bytes(raw[data_len + 1..].try_into().unwrap());

    // `raw_data` 是已压缩的数据 (`compress_block` 的输出), 需解压后再校验 CRC
    let decompressed = if compression_type == COMPRESSION_NONE {
        raw_data.to_vec()
    } else {
        let ct = byte_to_compression(compression_type).map_err(Error::Corruption)?;
        decompress_block(raw_data, ct)?
    };

    if !skip_checksum() {
        let mut hasher = Crc32Hasher::new();
        hasher.update(&decompressed);
        let computed = hasher.finalize();
        if computed != stored_crc {
            return Err(Error::Corruption(format!(
                "block CRC mismatch: {computed:#x} != {stored_crc:#x}"
            )));
        }
    }

    Ok(decompressed)
}

/// 压缩 block 数据.
fn compress_block(data: &[u8], compression: CompressionType) -> Result<Vec<u8>> {
    match compression {
        CompressionType::None => Ok(data.to_vec()),
        #[cfg(feature = "compression")]
        CompressionType::Snap => snap::raw::Encoder::new()
            .compress_vec(data)
            .map_err(|e| Error::InvalidArgument(format!("Snap compress failed: {e}"))),
        // `prepend_size = true`: 把原始长度写进压缩数据头部, 使解压端能在不
        // 知道 uncompressed_size 的情况下正确分配缓冲区 (见 decompress_block).
        #[cfg(feature = "compression")]
        CompressionType::Lz4 => lz4::block::compress(data, None, true)
            .map_err(|e| Error::InvalidArgument(format!("LZ4 compress failed: {e}"))),
        #[cfg(not(feature = "compression"))]
        CompressionType::Snap | CompressionType::Lz4 => Err(Error::InvalidArgument(
            "Snap/LZ4 compression requires 'compression' feature".into(),
        )),
    }
}

/// 解压 block 数据.
fn decompress_block(data: &[u8], compression: CompressionType) -> Result<Vec<u8>> {
    match compression {
        CompressionType::None => Ok(data.to_vec()),
        #[cfg(feature = "compression")]
        CompressionType::Snap => snap::raw::Decoder::new()
            .decompress_vec(data)
            .map_err(|e| Error::Corruption(format!("Snap decompress failed: {e}"))),
        // `None` 让 lz4 从 compress_block 写入的前缀头里读原始长度; 与
        // compress_block 的 `prepend_size = true` 配套, 缺一不可.
        #[cfg(feature = "compression")]
        CompressionType::Lz4 => lz4::block::decompress(data, None)
            .map_err(|e| Error::Corruption(format!("LZ4 decompress failed: {e}"))),
        #[cfg(not(feature = "compression"))]
        CompressionType::Snap | CompressionType::Lz4 => Err(Error::Corruption(
            "Snap/LZ4 decompression requires 'compression' feature".into(),
        )),
    }
}

fn compression_to_byte(c: CompressionType) -> Result<u8> {
    match c {
        CompressionType::None => Ok(COMPRESSION_NONE),
        CompressionType::Snap => Ok(1),
        CompressionType::Lz4 => Ok(2),
    }
}

fn byte_to_compression(b: u8) -> std::result::Result<CompressionType, String> {
    match b {
        0 => Ok(CompressionType::None),
        1 => Ok(CompressionType::Snap),
        2 => Ok(CompressionType::Lz4),
        _ => Err(format!("unknown compression type byte {b}")),
    }
}
