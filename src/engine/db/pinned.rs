//! PinnableSlice: 零拷贝或共享所有权的字节切片封装 (ADR-007).
//! 对应 RocksDB PinnableSlice, 消除点查路径中高频的堆内存分配与数据克隆.

use bytes::Bytes;
use std::ops::Deref;

/// 零拷贝或共享所有权的切片借用封装 (对应 RocksDB PinnableSlice).
#[derive(Debug, Clone)]
pub enum PinnableSlice<'a> {
    Borrowed(&'a [u8]),
    Shared(Bytes),
    Owned(Vec<u8>),
}

impl<'a> Deref for PinnableSlice<'a> {
    type Target = [u8];

    #[inline]
    fn deref(&self) -> &Self::Target {
        match self {
            Self::Borrowed(s) => s,
            Self::Shared(b) => b.as_ref(),
            Self::Owned(v) => v.as_slice(),
        }
    }
}

impl<'a> AsRef<[u8]> for PinnableSlice<'a> {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        self.deref()
    }
}

impl<'a> PartialEq for PinnableSlice<'a> {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.deref() == other.deref()
    }
}

impl<'a> Eq for PinnableSlice<'a> {}

impl<'a> PinnableSlice<'a> {
    #[inline]
    pub fn as_slice(&self) -> &[u8] {
        self.deref()
    }

    #[inline]
    pub fn into_bytes(self) -> Bytes {
        match self {
            Self::Borrowed(s) => Bytes::copy_from_slice(s),
            Self::Shared(b) => b,
            Self::Owned(v) => Bytes::from(v),
        }
    }

    #[inline]
    pub fn into_vec(self) -> Vec<u8> {
        match self {
            Self::Borrowed(s) => s.to_vec(),
            Self::Shared(b) => b.to_vec(),
            Self::Owned(v) => v,
        }
    }
}
