//! Meta Block 辅助: Bloom 等裸字节块.

use std::fs::File;
use std::io::{Read, Seek, SeekFrom};

use crate::error::Result;

use super::handle::BlockHandle;
use super::index::{IndexBlock, IndexEntry};

pub const BLOOM_META_NAME: &[u8] = b"bloom";
pub const PROPERTIES_META_NAME: &[u8] = b"properties";
pub const RANGE_TOMBSTONES_META_NAME: &[u8] = b"range_tombstones";

pub type RawRangeTombstone = (Vec<u8>, Vec<u8>, u64);

pub fn encode_range_tombstones(entries: &[RawRangeTombstone]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(4 + entries.len() * 24);
    buf.extend_from_slice(&(entries.len() as u32).to_le_bytes());
    for (start, end, seq) in entries {
        buf.extend_from_slice(&(start.len() as u32).to_le_bytes());
        buf.extend_from_slice(start);
        buf.extend_from_slice(&(end.len() as u32).to_le_bytes());
        buf.extend_from_slice(end);
        buf.extend_from_slice(&seq.to_le_bytes());
    }
    buf
}

pub fn decode_range_tombstones(data: &[u8]) -> Result<Vec<RawRangeTombstone>> {
    use crate::error::Error;
    if data.len() < 4 {
        return Err(Error::Corruption(
            "range_tombstones meta block truncated".into(),
        ));
    }
    let count = u32::from_le_bytes(data[..4].try_into().unwrap()) as usize;
    let mut pos = 4;
    let mut out = Vec::with_capacity(count);
    for _ in 0..count {
        if pos + 4 > data.len() {
            return Err(Error::Corruption(
                "range_tombstones start_len truncated".into(),
            ));
        }
        let start_len = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;
        if pos + start_len + 4 > data.len() {
            return Err(Error::Corruption("range_tombstones start truncated".into()));
        }
        let start = data[pos..pos + start_len].to_vec();
        pos += start_len;
        let end_len = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;
        if pos + end_len + 8 > data.len() {
            return Err(Error::Corruption(
                "range_tombstones end/seq truncated".into(),
            ));
        }
        let end = data[pos..pos + end_len].to_vec();
        pos += end_len;
        let seq = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
        pos += 8;
        out.push((start, end, seq));
    }
    Ok(out)
}

pub fn find_meta_block_handle(meta_index: &IndexBlock, name: &[u8]) -> Result<Option<BlockHandle>> {
    for entry in meta_index.entries()? {
        if entry.key == name {
            return Ok(Some(entry.handle));
        }
    }
    Ok(None)
}

pub fn read_raw_bytes(file: &File, offset: u64, size: u64) -> Result<Vec<u8>> {
    let mut buf = vec![0u8; size as usize];
    let mut f = file.try_clone()?;
    f.seek(SeekFrom::Start(offset))?;
    f.read_exact(&mut buf)?;
    Ok(buf)
}

pub fn write_raw_block(
    writer: &mut impl std::io::Write,
    data_block_offset: &mut u64,
    data: &[u8],
) -> Result<BlockHandle> {
    let offset = *data_block_offset;
    writer.write_all(data)?;
    let handle = BlockHandle {
        offset,
        size: data.len() as u64,
    };
    *data_block_offset += handle.size;
    Ok(handle)
}

pub fn index_entry_for_bloom(handle: BlockHandle) -> IndexEntry {
    IndexEntry {
        key: BLOOM_META_NAME.to_vec(),
        handle,
    }
}
