//! 分区/脑裂 failover 集成测试 (cluster-test-util).
//! @component aidb-cluster
//!
//! 复用 `ClusterTestHarness` (3 节点 linearizable), 用网络黑洞 (failpoint) +
//! per-node server abort 组合模拟真实双向分区, 验证三层防线:
//! 1. aidb 读侧 LeaseRead 快速失败 (Task 3)
//! 2. LeaderChangeWatcher 探活判定 (Task 4)
//! 3. aikv cluster_state:fail 拒绝门控 (e2e 覆盖)
//!
//! 黑洞按 (源节点, 目标 addr) 键控: 只丢弃"本节点 → 目标"方向的 Raft RPC,
//! 多数派侧 (N2↔N3) 互连保持, 分区窗口内可正常选举.

use std::collections::HashSet;
use std::time::Duration;

use aidb::cluster::failpoint::{blackhole, blackhole_active};
use aidb::cluster::leader_watcher::LeaderChangeWatcher;
use aidb::cluster::RaftNetworkClient;
use aidb::error::{ClusterError, Error};
use openraft::error::RPCError;
use openraft::network::RPCOption;
use openraft::raft::AppendEntriesRequest;
use openraft::RaftNetworkV2;
use serial_test::serial;
use tokio::time::sleep;

use super::harness::ClusterTestHarness;

const KEY: &[u8] = b"partition-k1";
const VALUE: &[u8] = b"partition-v1";

/// `judge_leader_quorum` 纯函数规则表 (注入 elapsed 时长, 确定性覆盖 fresh/stale):
/// - 本节点是 leader + 距 quorum ack 流逝 ≤ lease → Some(true)
/// - 本节点是 leader + 距 quorum ack 流逝 > lease → Some(false)
/// - 本节点是 leader + 从未有 ack (None) → Some(true) (新 leader 不误判)
/// - 本节点是 leader + 单节点 group (self-quorum) → Some(true) (恒有效)
/// - 本节点非 leader → None (不判定)
#[test]
fn judge_leader_quorum_rules() {
    let lease = Duration::from_secs(1);

    // leader + fresh ack → ok
    assert_eq!(
        LeaderChangeWatcher::judge_leader_quorum(
            Some(1),
            1,
            Some(Duration::from_millis(1)),
            lease,
            false
        ),
        Some(true)
    );
    // leader + stale ack → 失效
    assert_eq!(
        LeaderChangeWatcher::judge_leader_quorum(
            Some(1),
            1,
            Some(Duration::from_secs(2)),
            lease,
            false
        ),
        Some(false)
    );
    // leader + None (新 leader 首 ack 前) → 有效
    assert_eq!(
        LeaderChangeWatcher::judge_leader_quorum(Some(1), 1, None, lease, false),
        Some(true)
    );
    // leader + 单节点 group → 即使 ack 陈旧也有效 (openraft ≥alpha.32 单节点 leader
    // 无 follower 回复, last_quorum_acked 会停滞, 不能按 elapsed 判定)
    assert_eq!(
        LeaderChangeWatcher::judge_leader_quorum(
            Some(1),
            1,
            Some(Duration::from_secs(2)),
            lease,
            true
        ),
        Some(true)
    );
    // 非 leader → 不判定
    assert_eq!(
        LeaderChangeWatcher::judge_leader_quorum(
            Some(2),
            1,
            Some(Duration::from_millis(1)),
            lease,
            false
        ),
        None
    );
}

/// 黑洞命中时 `append_entries` 返回 `RPCError::Network` 且错误内容含 "blackhole";
/// guard drop 后黑洞清除.
#[tokio::test]
#[serial]
async fn network_blackhole_drops_rpc() {
    let src = 1u64;
    let target = "http://127.0.0.1:20349".to_string();

    let mut client = RaftNetworkClient::new(
        src,
        2u64,
        target.clone(),
        ClusterTestHarness::DEFAULT_GROUP_ID,
        100,
        1024,
        None,
        None,
        None,
    );

    let request = AppendEntriesRequest {
        vote: openraft::Vote::new_committed(1, src),
        prev_log_id: None,
        entries: Vec::new(),
        leader_commit: None,
    };

    assert!(!blackhole_active(src, &target));

    let guard = blackhole(src, HashSet::from([target.clone()]));
    assert!(blackhole_active(src, &target));

    let result = client
        .append_entries(request, RPCOption::new(Duration::from_millis(100)))
        .await;
    match result {
        Err(RPCError::Network(e)) => {
            assert!(
                e.to_string().contains("blackhole"),
                "错误内容应含 blackhole, 实际: {e}"
            );
        }
        other => panic!("黑洞命中应返回 RPCError::Network, 实际: {other:?}"),
    }
    drop(guard);

    assert!(!blackhole_active(src, &target));
}

/// 分区后 LeaseRead 快速失败: 旧 leader 双向隔离 (黑洞断出站 + abort 断入站),
/// lease 过期后 `get()` 返回 `NotLeader { leader: None, leader_addr: None }`;
/// 多数派侧选出新 leader, put/get 成功 (分区窗口内 N2↔N3 互连保持).
#[tokio::test]
#[serial]
async fn lease_read_fails_after_leader_isolated() {
    let harness = ClusterTestHarness::new_3node_linearizable().await;
    harness.bootstrap().await;
    let leader = harness.leader().await;
    let leader_id = leader.node_id();

    leader.put(KEY.to_vec(), VALUE.to_vec()).await.unwrap();

    let leader_idx = harness
        .nodes
        .iter()
        .position(|n| n.node_id() == leader_id)
        .unwrap();
    let leader_rpc_addr = format!("http://{}", harness.addrs[leader_idx]);

    let follower_rpc_addrs: HashSet<String> = harness
        .nodes
        .iter()
        .enumerate()
        .filter(|(_, n)| n.node_id() != leader_id)
        .map(|(i, _)| format!("http://{}", harness.addrs[i]))
        .collect();

    // 双向分区: leader 出站 (→followers) 全断 + followers 出站 (→leader) 全断 +
    // abort leader server 断其入站. guards 保持存活至测试结束.
    let _leader_guard = blackhole(leader_id, follower_rpc_addrs.clone());
    let mut follower_guards = Vec::new();
    for node in &harness.nodes {
        if node.node_id() != leader_id {
            follower_guards.push(blackhole(
                node.node_id(),
                HashSet::from([leader_rpc_addr.clone()]),
            ));
        }
    }
    harness.abort_server(leader_id);

    // 等 leader_lease (election_timeout_max=1000ms) 过期 + margin
    sleep(Duration::from_millis(1600)).await;

    // 旧 leader get() 快速失败为 NotLeader { leader: None } (LeaseRead 过期 →
    // ForwardToLeader::empty() → mapper 新分支)
    match leader.get(KEY.to_vec()).await {
        Err(Error::Cluster(ClusterError::NotLeader {
            leader: l,
            leader_addr,
            ..
        })) => {
            assert_eq!(l, None, "旧 leader 分区后应无已知 leader 可转发");
            assert_eq!(leader_addr, None);
        }
        other => panic!("旧 leader get 应返回 NotLeader {{ leader: None }}, 实际: {other:?}"),
    }

    // 多数派侧选出新 leader, put/get 成功 (轮询 ≤2s)
    let majority: Vec<_> = harness
        .nodes
        .iter()
        .filter(|n| n.node_id() != leader_id)
        .cloned()
        .collect();
    let mut put_ok = false;
    for _ in 0..20 {
        for node in &majority {
            if node
                .put(KEY.to_vec(), b"after-failover".to_vec())
                .await
                .is_ok()
            {
                put_ok = true;
                break;
            }
        }
        if put_ok {
            break;
        }
        sleep(Duration::from_millis(100)).await;
    }
    assert!(put_ok, "多数派侧 failover 后 put 应成功 (新 leader 已选出)");

    let mut read_back = None;
    for node in &majority {
        if let Ok(Some(v)) = node.get(KEY.to_vec()).await {
            read_back = Some(v);
            break;
        }
    }
    assert_eq!(
        read_back.as_deref(),
        Some(b"after-failover".as_slice()),
        "多数派侧应读到 failover 后的新值"
    );
}
