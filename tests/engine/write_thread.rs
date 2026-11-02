//! WriteThread Leader-Follower Group Commit 并发协作测试
//! @component aidb-engine

use crossbeam::sync::Parker;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use aidb::engine::db::write_thread::{
    WriteGroupStatus, WriteThread, Writer, STATE_COMPLETED, STATE_MEMTABLE, STATE_MEMTABLE_DONE,
};
use aidb::WriteBatch;

#[test]
fn test_write_thread_concurrency_group_commit() {
    let write_thread = Arc::new(WriteThread::new());
    let leader_count = Arc::new(AtomicUsize::new(0));
    let total_collected = Arc::new(AtomicUsize::new(0));
    let completed_count = Arc::new(AtomicUsize::new(0));

    let num_threads = 50;
    let mut handles = Vec::with_capacity(num_threads);

    for i in 0..num_threads {
        let wt = Arc::clone(&write_thread);
        let leaders = Arc::clone(&leader_count);
        let collected = Arc::clone(&total_collected);
        let completed = Arc::clone(&completed_count);

        handles.push(std::thread::spawn(move || {
            let parker = Parker::new();
            let unparker = parker.unparker().clone();

            let mut batch = WriteBatch::new();
            batch.put(format!("k_{i}").into_bytes(), format!("v_{i}").into_bytes());

            let mut writer = Box::new(Writer::new(&batch, unparker, true, false));

            match wt
                .join_batch_group(&mut writer, &parker)
                .expect("join group")
            {
                WriteGroupStatus::Leader(leader) => {
                    let group = &leader.group;
                    leaders.fetch_add(1, Ordering::Relaxed);
                    collected.fetch_add(group.len(), Ordering::Relaxed);

                    // 1. Leader 分配 sequence 并唤醒 Followers
                    for (idx, w_ptr) in group.iter().enumerate() {
                        unsafe {
                            let w = &mut **w_ptr;
                            w.seq = 1000 + idx as u64;
                            if idx > 0 {
                                w.state.store(STATE_MEMTABLE, Ordering::Release);
                                w.unparker.unpark();
                            }
                        }
                    }

                    // 2. Leader 等待所有 Followers 完成 MemTable 插入
                    for (idx, w_ptr) in group.iter().enumerate() {
                        if idx > 0 {
                            unsafe {
                                let w = &**w_ptr;
                                while w.state.load(Ordering::Acquire) != STATE_MEMTABLE_DONE {
                                    std::hint::spin_loop();
                                }
                            }
                        }
                    }

                    // 3. 标记所有 Followers 已完成并唤醒 (发布终态后 Follower 随时可能销毁自己的 Writer,
                    //    必须走 `complete()`, 不能自己在 store 之后再访问 w.unparker)
                    leader.complete();
                    completed.fetch_add(1, Ordering::Relaxed);
                }
                WriteGroupStatus::FollowerMemtable(seq) => {
                    assert!(
                        seq >= 1000,
                        "Follower 必须获取到 Leader 分配的自增 sequence"
                    );
                    // 模拟 Follower 插入 MemTable 完成
                    writer.state.store(STATE_MEMTABLE_DONE, Ordering::Release);

                    while writer.state.load(Ordering::Acquire) != STATE_COMPLETED {
                        parker.park_timeout(Duration::from_micros(50));
                    }
                    completed.fetch_add(1, Ordering::Relaxed);
                }
                WriteGroupStatus::Completed => {
                    completed.fetch_add(1, Ordering::Relaxed);
                }
            }
        }));
    }

    for h in handles {
        h.join().expect("thread join");
    }

    assert_eq!(
        completed_count.load(Ordering::SeqCst),
        num_threads,
        "全部 50 个线程必须完成写入"
    );
    assert!(
        leader_count.load(Ordering::SeqCst) >= 1,
        "至少有 1 个 Leader 当选"
    );
    assert_eq!(
        total_collected.load(Ordering::SeqCst),
        num_threads,
        "收集到的总写入数必须为 50"
    );
}
