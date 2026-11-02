//! WriteThread: 基于无锁队列与 Leader-Follower 协作状态机的高性能 Group Commit 写入管线.
//! 依据 ADR-006: 彻底废除写路径单核互斥锁, 实现动态 WAL 批量合并与全核心无锁 MemTable 并发写入.

use crossbeam::queue::SegQueue;
use crossbeam::sync::{Parker, Unparker};
use parking_lot::{Mutex, MutexGuard};
use std::sync::atomic::{AtomicU8, Ordering};
use std::time::Duration;

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

impl WriteGroupLeader<'_> {
    /// 向除 Leader 自身外的全部 Followers 发布 `STATE_COMPLETED` 并唤醒.
    pub fn complete(&self) {
        for &w_ptr in self.group.iter().skip(1) {
            // SAFETY: group 中的 Followers 在收到终态前不会返回, 指针在此刻有效.
            unsafe { publish_follower_state(w_ptr, STATE_COMPLETED) };
        }
    }

    /// 向除 Leader 自身外的全部 Followers 发布 `STATE_FAILED` (附错误信息) 并唤醒.
    pub fn fail(&self, err_msg: &str) {
        for &w_ptr in self.group.iter().skip(1) {
            // SAFETY: 同 `complete`.
            unsafe {
                (*w_ptr).error = Some(err_msg.to_string());
                publish_follower_state(w_ptr, STATE_FAILED);
            }
        }
    }
}

#[cfg(test)]
thread_local! {
    /// 测试钩子: 在 `publish_follower_state` 发布终态之后, 唤醒之前执行 (仅当前线程).
    static AFTER_PUBLISH_HOOK: std::cell::RefCell<Option<Box<dyn Fn()>>> =
        const { std::cell::RefCell::new(None) };
}

/// 向一个正在等待终态的 Follower 发布终态并唤醒它.
///
/// # Safety
/// `w_ptr` 必须指向一个尚未返回的 Follower `Writer` (它在收到终态前一直阻塞).
/// 发布终态之后 Follower 随时可能返回并销毁 `*w_ptr`, 因此发布之后**不得**再访问 `*w_ptr`.
pub unsafe fn publish_follower_state(w_ptr: *mut Writer, state: u8) {
    // 必须在发布终态之前克隆: `Unparker` 是 `Arc`, 克隆后即使 Follower 的 `Writer` (及其中的
    // 那份 `Unparker`) 被销毁, `Parker::Inner` 仍被我们持有, `unpark()` 对已返回的 Follower 无害.
    let unparker = (*w_ptr).unparker.clone();
    (*w_ptr).state.store(state, Ordering::Release);
    #[cfg(test)]
    AFTER_PUBLISH_HOOK.with(|h| {
        if let Some(hook) = h.borrow().as_ref() {
            hook();
        }
    });
    unparker.unpark();
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
        for &w_ptr in group.iter().skip(1) {
            // SAFETY: 同 `WriteGroupLeader::complete`.
            unsafe {
                (*w_ptr).error = Some(err_msg.to_string());
                publish_follower_state(w_ptr, STATE_FAILED);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::mem::ManuallyDrop;
    use std::sync::atomic::{AtomicBool, AtomicUsize};
    use std::sync::Arc;

    fn new_batch() -> WriteBatch {
        let mut b = WriteBatch::new();
        b.put(b"k".to_vec(), b"v".to_vec());
        b
    }

    /// 回归 (写路径 use-after-free, 表现为服务整体卡死):
    /// Leader 把 Follower 置为终态后, Follower 立即返回并销毁自己栈上的 `Writer`,
    /// 旧实现随后仍通过 `w.unparker.unpark()` 读取该已销毁对象, 读到被复用的内存,
    /// 在垃圾 `Arc<Parker::Inner>` 的 mutex 上永久阻塞 (线上 gdb 现场: `Mutex::lock_contended`).
    /// 期望: 发布终态之后 Leader 不再访问 Follower 的 `Writer`.
    #[test]
    fn leader_must_not_touch_follower_writer_after_publishing_terminal_state() {
        let wt = Arc::new(WriteThread::new());
        let scribbled = Arc::new(AtomicBool::new(false));

        // 主线程先当选 Leader 并持有 leader_lock, Follower 只能排队等待.
        let leader_batch = new_batch();
        let leader_parker = Parker::new();
        let mut leader_writer =
            Writer::new(&leader_batch, leader_parker.unparker().clone(), true, false);
        let WriteGroupStatus::Leader(mut leader) = wt
            .join_batch_group(&mut leader_writer, &leader_parker)
            .expect("leader joins")
        else {
            panic!("first writer must become leader");
        };

        let follower = {
            let wt = Arc::clone(&wt);
            let scribbled = Arc::clone(&scribbled);
            std::thread::spawn(move || {
                let batch = new_batch();
                let parker = Parker::new();
                // ManuallyDrop: 下面会把对象内存整体覆写, 不能再运行 Drop.
                let mut writer =
                    ManuallyDrop::new(Writer::new(&batch, parker.unparker().clone(), true, false));
                let status = wt
                    .join_batch_group(&mut writer, &parker)
                    .expect("follower joins");
                assert!(matches!(status, WriteGroupStatus::Completed));
                // 模拟栈帧销毁后被其他对象复用: 把 Writer 的内存整体覆写成垃圾.
                unsafe {
                    std::ptr::write_bytes(
                        &mut *writer as *mut Writer as *mut u8,
                        0xFF,
                        std::mem::size_of::<Writer>(),
                    );
                }
                scribbled.store(true, Ordering::Release);
            })
        };

        // 等 Follower 入队后, 把它并入 Leader 的 group.
        while wt.queue.is_empty() {
            std::thread::yield_now();
        }
        while let Some(p) = wt.queue.pop() {
            leader.group.push(p);
        }
        assert_eq!(leader.group.len(), 2);

        // 钩子: 终态已发布、尚未唤醒的这一刻, 等 Follower 完成覆写, 再让 Leader 继续.
        let flag = Arc::clone(&scribbled);
        AFTER_PUBLISH_HOOK.with(|h| {
            *h.borrow_mut() = Some(Box::new(move || {
                while !flag.load(Ordering::Acquire) {
                    std::hint::spin_loop();
                }
            }));
        });
        leader.complete();
        AFTER_PUBLISH_HOOK.with(|h| *h.borrow_mut() = None);

        follower.join().expect("follower thread must finish");
    }

    /// 多线程反复走完整的 Leader/Follower 协议, 每个 Writer 都必须在限定时间内完成.
    #[test]
    fn concurrent_writers_all_complete() {
        const THREADS: usize = 16;
        const ITERS: usize = 3000;
        let wt = Arc::new(WriteThread::new());
        let completed = Arc::new(AtomicUsize::new(0));

        let handles: Vec<_> = (0..THREADS)
            .map(|_| {
                let wt = Arc::clone(&wt);
                let completed = Arc::clone(&completed);
                std::thread::spawn(move || {
                    for _ in 0..ITERS {
                        let batch = new_batch();
                        let parker = Parker::new();
                        let mut writer =
                            Writer::new(&batch, parker.unparker().clone(), true, false);
                        if let WriteGroupStatus::Leader(leader) =
                            wt.join_batch_group(&mut writer, &parker).expect("join")
                        {
                            leader.complete();
                        }
                        completed.fetch_add(1, Ordering::Relaxed);
                    }
                })
            })
            .collect();

        let deadline = std::time::Instant::now() + Duration::from_secs(60);
        while completed.load(Ordering::Relaxed) < THREADS * ITERS {
            assert!(
                std::time::Instant::now() < deadline,
                "writers hung: {}/{}",
                completed.load(Ordering::Relaxed),
                THREADS * ITERS
            );
            std::thread::sleep(Duration::from_millis(10));
        }
        for h in handles {
            h.join().expect("writer thread");
        }
    }
}
