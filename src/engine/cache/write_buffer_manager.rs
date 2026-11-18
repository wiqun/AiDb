//! WriteBufferManager: 跨多个 DB 实例共享的全局 MemTable 内存预算管理器.
//!
//! 用于多 Raft Group (例如 256 个独立 DB 实例) 场景下限制所有活跃 + 冻结
//! MemTable 的总内存占用, 在总量超过上限时触发当前或后续写入路径冻结并刷盘.

use std::sync::atomic::{AtomicUsize, Ordering};

/// 跨 DB 共享的写入缓冲区全局内存管理器.
#[derive(Debug)]
pub struct WriteBufferManager {
    buffer_size: usize,
    memory_used: AtomicUsize,
}

impl WriteBufferManager {
    /// 创建指定全局内存上限 (字节) 的 `WriteBufferManager` (`0` 表示不限制).
    pub fn new(buffer_size: usize) -> Self {
        Self {
            buffer_size,
            memory_used: AtomicUsize::new(0),
        }
    }

    /// 全局内存上限 (字节).
    pub fn buffer_size(&self) -> usize {
        self.buffer_size
    }

    /// 当前已占用的 MemTable 总字节数.
    pub fn memory_usage(&self) -> usize {
        self.memory_used.load(Ordering::Relaxed)
    }

    /// 记账新增 MemTable 内存占用.
    pub fn reserve_mem(&self, bytes: usize) {
        if bytes > 0 {
            self.memory_used.fetch_add(bytes, Ordering::Relaxed);
        }
    }

    /// 释放已刷盘 Immutable MemTable 的内存占用.
    pub fn free_mem(&self, bytes: usize) {
        if bytes > 0 {
            let _ = self
                .memory_used
                .try_update(Ordering::Relaxed, Ordering::Relaxed, |cur| {
                    Some(cur.saturating_sub(bytes))
                });
        }
    }

    /// 当前全局内存占用是否已达到或超过预算上限 (`buffer_size == 0` 时恒为 `false`).
    pub fn should_flush(&self) -> bool {
        self.buffer_size > 0 && self.memory_used.load(Ordering::Relaxed) >= self.buffer_size
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn write_buffer_manager_tracks_reserve_free_and_should_flush() {
        let wbm = WriteBufferManager::new(1024);
        assert!(!wbm.should_flush());
        wbm.reserve_mem(600);
        assert_eq!(wbm.memory_usage(), 600);
        assert!(!wbm.should_flush());

        wbm.reserve_mem(500);
        assert_eq!(wbm.memory_usage(), 1100);
        assert!(wbm.should_flush());

        wbm.free_mem(600);
        assert_eq!(wbm.memory_usage(), 500);
        assert!(!wbm.should_flush());

        // Underflow protection
        wbm.free_mem(10_000);
        assert_eq!(wbm.memory_usage(), 0);
    }
}
