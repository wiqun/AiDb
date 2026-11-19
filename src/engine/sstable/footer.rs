//! SSTable 文件尾 48B Footer.

use crate::error::{Error, Result};

use super::handle::BlockHandle;

pub const FOOTER_SIZE: usize = 48;
pub const MAGIC_NUMBER: u64 = 0x5f454c4241545353;
pub const SST_FORMAT_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Footer {
    pub meta_index_handle: BlockHandle,
    pub index_handle: BlockHandle,
    pub format_version: u32,
    pub flags: u32,
}

impl Footer {
    pub fn new(meta_index_handle: BlockHandle, index_handle: BlockHandle) -> Self {
        Self {
            meta_index_handle,
            index_handle,
            format_version: SST_FORMAT_VERSION,
            flags: 0,
        }
    }

    pub fn encode(self) -> [u8; FOOTER_SIZE] {
        let mut buf = [0u8; FOOTER_SIZE];
        buf[..16].copy_from_slice(&self.meta_index_handle.encode());
        buf[16..32].copy_from_slice(&self.index_handle.encode());
        buf[32..36].copy_from_slice(&self.format_version.to_le_bytes());
        buf[36..40].copy_from_slice(&self.flags.to_le_bytes());
        buf[40..].copy_from_slice(&MAGIC_NUMBER.to_le_bytes());
        buf
    }

    pub fn decode(buf: &[u8]) -> Result<Self> {
        if buf.len() != FOOTER_SIZE {
            return Err(Error::Corruption(format!(
                "footer size {} != {FOOTER_SIZE}",
                buf.len()
            )));
        }
        let magic = u64::from_le_bytes(buf[40..48].try_into().unwrap());
        if magic != MAGIC_NUMBER {
            return Err(Error::Corruption(format!("bad SSTable magic: {magic:#x}")));
        }
        let format_version = u32::from_le_bytes(buf[32..36].try_into().unwrap());
        if format_version != SST_FORMAT_VERSION {
            return Err(Error::Corruption(format!(
                "unsupported SSTable format_version: {format_version} != {SST_FORMAT_VERSION}"
            )));
        }
        let flags = u32::from_le_bytes(buf[36..40].try_into().unwrap());
        Ok(Self {
            meta_index_handle: BlockHandle::decode(&buf[..16])?,
            index_handle: BlockHandle::decode(&buf[16..32])?,
            format_version,
            flags,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn footer_encodes_and_validates_format_version() {
        let f = Footer::new(
            BlockHandle {
                offset: 10,
                size: 20,
            },
            BlockHandle {
                offset: 30,
                size: 40,
            },
        );
        let mut raw = f.encode();
        let decoded = Footer::decode(&raw).unwrap();
        assert_eq!(decoded.format_version, SST_FORMAT_VERSION);
        assert_eq!(decoded.flags, 0);

        raw[32..36].copy_from_slice(&99u32.to_le_bytes());
        assert!(Footer::decode(&raw).is_err());
    }
}
