//! WriteThread: 基于无锁队列与 Leader-Follower 协作状态机的高性能 Group Commit 写入管线.
//! 依据 ADR-006: 彻底废除写路径单核互斥锁, 实现动态 WAL 批量合并与全核心无锁 MemTable 并发写入.

use std::sync::atomic::{AtomicU8, Ordering};
use std::time::Duration;
use crossbeam::queue::SegQueue;
use crossbeam::sync::{Parker, Unparker};
use parking_lot::{Mutex, MutexGuard};

use crate::engine::db::WriteBatch;
use crate::error::Result;

pub const STATE_INIT: u8 = 0;
pub const STATE_PARKED: u8 = 1;
pub const STATE_LEADER: u8 = 2;
pub const STATE_MEMTABLE: u8 = 3;
pub const STATE_MEMTABLE_DONE: u8 = 4;
pub const STATE_COMPLETED: u8 = 5;
pub const STATE_FAILED: u8 = 6;

pub struct Writer {
    pub batch: *const WriteBatch,
    pub seq: u64,
    pub state: AtomicU8,
    pub unparker: Unparker,
    pub use_wal: bool,
    pub sync: bool,
    pub error: Option<String>,
}

unsafe impl Send for Writer {}
unsafe impl Sync for Writer {}

impl Writer {
    pub fn new(batch: &WriteBatch, unparker: Unparker, use_wal: bool, sync: bool) -> Self {
        Self {
            batch: batch as *const WriteBatch,
            seq: 0,
            state: AtomicU8::new(STATE_INIT),
            unparker,
            use_wal,
            sync,
            error: None,
        }
    }
}

pub struct WriteGroupLeader<'a> {
    pub group: Vec<*mut Writer>,
    _guard: MutexGuard<'a, ()>,
}

pub enum WriteGroupStatus<'a> {
    Leader(WriteGroupLeader<'a>),
    FollowerMemtable(u64),
    Completed,
}

pub struct WriteThread {
    leader_lock: Mutex<()>,
    queue: SegQueue<*mut Writer>,
}

unsafe impl Send for WriteThread {}
unsafe impl Sync for WriteThread {}

impl Default for WriteThread {
    fn default() -> Self {
        Self::new()
    }
}

impl WriteThread {
    pub fn new() -> Self {
        Self {
            leader_lock: Mutex::new(()),
            queue: SegQueue::new(),
        }
    }

    /// 加入写入协作组:
    /// - 登记至无锁队列后竞争 Leader 角色;
    /// - Leader 负责提取当前队列中所有并发写请求, 执行批量分配与 Group Commit WAL;
    /// - Followers 挂起等待 Leader 广播分配的专属 sequence 并并发并行写入 MemTable.
    pub fn join_batch_group<'a>(
        &'a self,
        writer: &mut Writer,
        parker: &Parker,
    ) -> Result<WriteGroupStatus<'a>> {
        let w_ptr = writer as *mut Writer;
        self.queue.push(w_ptr);

        loop {
            match writer.state.load(Ordering::Acquire) {
                STATE_MEMTABLE => {
                    return Ok(WriteGroupStatus::FollowerMemtable(writer.seq));
                }
                STATE_COMPLETED => {
                    return Ok(WriteGroupStatus::Completed);
                }
                STATE_FAILED => {
                    let msg = writer
                        .error
                        .clone()
                        .unwrap_or_else(|| "write group failed".to_string());
                    return Err(crate::error::Error::InvalidState(msg));
                }
                _ => {}
            }

            // 尝试竞选为当前批次的 Leader
            if let Some(guard) = self.leader_lock.try_lock() {
                // 再次检查自身状态是否在争锁前已被上一个 Leader 收集并处理
                let curr_state = writer.state.load(Ordering::Acquire);
                if curr_state != STATE_INIT && curr_state != STATE_PARKED {
                    drop(guard);
                    continue;
                }

                // 成功当选 Leader: 提取当前就绪批次中所有的 Writers
                let mut group = Vec::with_capacity(32);
                while let Some(item_ptr) = self.queue.pop() {
                    group.push(item_ptr);
                }

                // 保证 Leader 自身严格位于 group[0]
                if let Some(pos) = group.iter().position(|&p| p == w_ptr) {
                    group.swap(0, pos);
                } else {
                    group.insert(0, w_ptr);
                }

                writer.state.store(STATE_LEADER, Ordering::Release);
                return Ok(WriteGroupStatus::Leader(WriteGroupLeader {
                    group,
                    _guard: guard,
                }));
            }

            // 竞选未果: 仅在当前状态仍为 STATE_INIT 时 CAS 标记为 STATE_PARKED, 决不覆盖 Leader 分配的 STATE_MEMTABLE
            let _ = writer.state.compare_exchange(
                STATE_INIT,
                STATE_PARKED,
                Ordering::AcqRel,
                Ordering::Acquire,
            );
            if writer.state.load(Ordering::Acquire) == STATE_MEMTABLE {
                return Ok(WriteGroupStatus::FollowerMemtable(writer.seq));
            }
            parker.park_timeout(Duration::from_micros(50));
        }
    }

    /// Leader 通知批次中所有 Followers 失败并唤醒
    pub fn fail_group(&self, group: &[*mut Writer], err_msg: &str) {
        for (idx, w_ptr) in group.iter().enumerate() {
            if idx > 0 {
                unsafe {
                    let w = &mut **w_ptr;
                    w.error = Some(err_msg.to_string());
                    w.state.store(STATE_FAILED, Ordering::Release);
                    w.unparker.unpark();
                }
            }
        }
    }
}
