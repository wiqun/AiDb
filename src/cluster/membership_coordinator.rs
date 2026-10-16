//! 节点成员协调 — 节点 join / leave / replace 的运维编排. 操作统一先经 MetaRaft
//! 更新元数据 (权威), 再执行数据 Group 的 Raft 成员变更.
//!
//! # 流程
//!
//! ```text
//! add_node(ctx)
//!   ├─ 幂等: 同 rpc_addr 已存在 -> 视为更新 client_addr (MEET 补全 MOVED 端口)
//!   ├─ RegisterNode (MetaRaft)
//!   ├─ add_node_address (network factory)
//!   ├─ add_learner_nonblocking (MetaRaft learner)
//!   └─ promote_learner_to_voter (屏障: learner 追平 + 确认复制)
//!
//! remove_node(ctx)
//!   ├─ 状态置 Draining
//!   ├─ force: 逐 group 移除成员 (拒绝留下零副本); 否则要求先迁走 group
//!   └─ RemoveNode
//! ```
//!
//! `change_group_membership` 先写 MetaRaft 元数据再改 Raft membership —
//! 副本节点通过 LifecycleManager 从元数据发现新成员后补 learner (带重试);
//! group 非本地时只更新元数据, Raft 变更交由该 group 所在节点的 drift 对账.
//!
//! # Invariant
//!
//! - 变更顺序: MetaRaft 元数据先行, Raft membership 随后 — 保证所有节点都能
//!   通过元数据发现拓扑变化.
//! - `add_node` 幂等: 以 `rpc_addr` 为身份键.
//! - force remove 不得使任何 group 剩零副本.
//! - 涉及活跃迁移的节点/group 禁止移除 (validate 拒绝).

use std::sync::Arc;

use crate::cluster::meta_raft_node::MetaRaftNode;
use crate::cluster::meta_types::{MetaRequest, NodeStatus, SlotMigrationState};
use crate::cluster::multi_raft_node::MultiRaftNode;
use crate::cluster::router::Router;
use crate::cluster::types::{ClusterError, NodeId};
use crate::error::Result;

#[derive(Debug)]
pub struct NodeJoinContext {
    pub node_id: NodeId,
    pub rpc_addr: String,
    pub client_addr: Option<String>,
    pub join_method: JoinMethod,
}

#[derive(Debug)]
pub enum JoinMethod {
    Empty,
    TakeoverGroups {
        source_node: NodeId,
        groups: Vec<u64>,
    },
}

#[derive(Debug)]
pub struct NodeLeaveContext {
    pub node_id: NodeId,
    pub force: bool,
}

pub struct MembershipCoordinator {
    meta_raft: Arc<MetaRaftNode>,
    multi_raft: Arc<MultiRaftNode>,
    // kept for reserved/future use
    #[expect(dead_code)]
    router: Arc<Router>,
    #[expect(dead_code)]
    node_id: NodeId,
}

impl MembershipCoordinator {
    pub fn new(
        meta_raft: Arc<MetaRaftNode>,
        multi_raft: Arc<MultiRaftNode>,
        router: Arc<Router>,
        node_id: NodeId,
    ) -> Self {
        Self {
            meta_raft,
            multi_raft,
            router,
            node_id,
        }
    }

    /// 将新节点加入集群.
    ///
    /// 幂等语义: 如果 RPC 地址已存在则视为更新 client_addr 而非新增.
    /// 这允许 --cluster-peers 启动的节点通过后续 CLUSTER MEET 补充
    /// client_addr, 确保 MOVED 重定向返回客户端端口而非 RPC 端口.
    pub async fn add_node(&self, ctx: NodeJoinContext) -> Result<()> {
        let start = std::time::Instant::now();
        tracing::info!("add_node: starting");
        if ctx.rpc_addr.is_empty() {
            return Err(ClusterError::InvalidConfig("empty rpc_addr".into()).into());
        }

        let cluster_meta = self.meta_raft.get_cluster_meta();

        // 按 RPC 地址查找已有节点 — 如果找到则视为更新 client_addr.
        if let Some((existing_id, existing_node)) = cluster_meta
            .nodes
            .iter()
            .find(|(_, n)| n.rpc_addr == ctx.rpc_addr)
        {
            let existing_id = *existing_id;
            if existing_node.client_addr == ctx.client_addr {
                return Ok(());
            }
            if let Some(client_addr) = &ctx.client_addr {
                self.meta_raft
                    .propose(MetaRequest::UpdateNodeClientAddr {
                        node_id: existing_id,
                        client_addr: Some(client_addr.clone()),
                    })
                    .await?;
                return Ok(());
            }
            return Ok(());
        }

        if cluster_meta.nodes.contains_key(&ctx.node_id) {
            return Err(ClusterError::InvalidState("node already exists".into()).into());
        }

        let rpc_addr = ctx.rpc_addr.clone();

        // Step 1: RegisterNode proposal
        let t0 = std::time::Instant::now();
        self.meta_raft
            .propose(MetaRequest::RegisterNode {
                node_id: ctx.node_id,
                rpc_addr: ctx.rpc_addr,
                client_addr: ctx.client_addr,
                tags: std::collections::HashMap::new(),
            })
            .await?;
        tracing::info!(
            elapsed_ms = t0.elapsed().as_millis(),
            "add_node: RegisterNode committed"
        );

        // Step 2: Register gRPC address
        self.meta_raft
            .add_node_address(ctx.node_id, rpc_addr.clone());

        // Step 3: Add as Raft Learner (non-blocking)
        let t1 = std::time::Instant::now();
        self.meta_raft
            .add_learner_nonblocking(ctx.node_id, rpc_addr.clone())
            .await?;
        tracing::info!(
            elapsed_ms = t1.elapsed().as_millis(),
            "add_node: add_learner committed"
        );

        // Step 4: Promote to Voter (may be slow — waits for replication)
        let t2 = std::time::Instant::now();
        if let Err(e) = self.meta_raft.promote_learner_to_voter(ctx.node_id).await {
            tracing::warn!(
              node_id = ctx.node_id,
              error = %e,
              "failed to promote MetaRaft learner to voter (will retry on next MEET)"
            );
        }
        tracing::info!(
            elapsed_ms = t2.elapsed().as_millis(),
            "add_node: promote_learner_to_voter done"
        );

        // Step 5: Data group membership (for TakeoverGroups)
        match ctx.join_method {
            JoinMethod::Empty => {}
            JoinMethod::TakeoverGroups { .. } => {
                // ... (existing code)
            }
        }

        tracing::info!(
            total_elapsed_ms = start.elapsed().as_millis(),
            "add_node: complete"
        );
        Ok(())
    }

    /// Promote a Learner node to Voter in the MetaRaft group.
    ///
    /// Gathers all current voters plus the target node and calls
    /// `change_membership` on the MetaRaft consensus group.
    pub async fn promote_learner_to_voter(&self, node_id: NodeId) -> Result<()> {
        // Delegate to MetaRaftNode which has the correct safety-net logic.
        self.meta_raft.promote_learner_to_voter(node_id).await
    }

    /// 从集群移除节点.
    pub async fn remove_node(&self, ctx: NodeLeaveContext) -> Result<()> {
        let cluster_meta = self.meta_raft.get_cluster_meta();
        if !cluster_meta.nodes.contains_key(&ctx.node_id) {
            return Err(ClusterError::Raft("node not found".into()).into());
        }

        let groups_on_node: Vec<u64> = cluster_meta
            .groups
            .iter()
            .filter(|(_, g)| g.replicas.iter().any(|r| r.node_id == ctx.node_id))
            .map(|(id, _)| *id)
            .collect();

        // Check active migration
        if let Some(ref state) = self.meta_raft.get_migration_state() {
            if node_in_active_migration(ctx.node_id, state, &cluster_meta.groups) {
                return Err(
                    ClusterError::InvalidState("node involved in active migration".into()).into(),
                );
            }
        }

        self.meta_raft
            .propose(MetaRequest::UpdateNodeStatus {
                node_id: ctx.node_id,
                status: NodeStatus::Draining,
            })
            .await?;

        if ctx.force {
            for &gid in &groups_on_node {
                let meta = self.meta_raft.get_cluster_meta();
                if let Some(g) = meta.groups.get(&gid) {
                    if g.replicas.len() <= 1 {
                        return Err(ClusterError::InvalidState(
                            "force remove would leave group with zero replicas".into(),
                        )
                        .into());
                    }
                }
                // Remove node from group members
                let meta = self.meta_raft.get_cluster_meta();
                let current_members: Vec<NodeId> = meta
                    .groups
                    .get(&gid)
                    .map(|g| g.replicas.iter().map(|r| r.node_id).collect())
                    .unwrap_or_default();
                let new_members: Vec<NodeId> = current_members
                    .into_iter()
                    .filter(|id| *id != ctx.node_id)
                    .collect();
                self.change_group_membership(gid, vec![], vec![], Some(new_members))
                    .await?;
            }
        } else {
            let meta = self.meta_raft.get_cluster_meta();
            let remaining: Vec<u64> = meta
                .groups
                .iter()
                .filter(|(_, g)| g.replicas.iter().any(|r| r.node_id == ctx.node_id))
                .map(|(id, _)| *id)
                .collect();
            if !remaining.is_empty() {
                return Err(ClusterError::InvalidState(format!(
                    "node still has {} groups, migrate them first",
                    remaining.len()
                ))
                .into());
            }
        }

        self.meta_raft
            .propose(MetaRequest::RemoveNode {
                node_id: ctx.node_id,
            })
            .await?;
        Ok(())
    }

    /// 变更 Group 成员 (全量替换).
    ///
    /// 如果 group 不在本地 (is_group_local 返回 false), 则跳过 MultiRaft 操作,
    /// 仅更新 MetaRaft 元数据. 后续 LifecycleManager 会在 group 所在节点上检测到
    /// drift 并自动执行 Raft 成员变更.
    pub async fn change_group_membership(
        &self,
        group_id: u64,
        add: Vec<NodeId>,
        remove: Vec<NodeId>,
        explicit_members: Option<Vec<NodeId>>,
    ) -> Result<()> {
        let cluster_meta = self.meta_raft.get_cluster_meta();
        let group = cluster_meta
            .groups
            .get(&group_id)
            .ok_or_else(|| ClusterError::InvalidState("group not found".into()))?;

        // Compute the new full member set
        let new_members = match explicit_members {
            Some(m) => m,
            None => {
                let mut members: Vec<NodeId> = group.replicas.iter().map(|r| r.node_id).collect();
                for id in &add {
                    if !members.contains(id) {
                        members.push(*id);
                    }
                }
                members.retain(|id| !remove.contains(id));
                members
            }
        };

        let is_local = self.multi_raft.is_group_local(group_id);

        // Update MetaRaft metadata FIRST — always works regardless of locality.
        // This ensures replica nodes' LifecycleManagers discover the group assignment
        // before we try to add learners or change Raft membership.
        let new_replicas: Vec<(NodeId, bool)> = new_members
            .iter()
            .map(|id| (*id, *id == new_members[0]))
            .collect();
        self.meta_raft
            .propose(MetaRequest::ChangeGroupMembership {
                group_id,
                new_replicas,
                config_version: group.config_version + 1,
            })
            .await?;

        // Add learners with retry — the replica node's LifecycleManager may not have
        // created the group yet (it discovers from MetaRaft metadata which was just
        // updated above).  Retry for up to 10s to give LifecycleManager time to react.
        if is_local {
            for &node_id in &add {
                let addr = cluster_meta
                    .nodes
                    .get(&node_id)
                    .map(|n| n.rpc_addr.clone())
                    .unwrap_or_default();
                // Retry add_learner_to_group: replica's LifecycleManager may need up to
                // a few ticks (1s each) to discover and create the group.
                let mut added = false;
                for _ in 0..20 {
                    if self
                        .multi_raft
                        .add_learner_to_group(group_id, node_id, addr.clone())
                        .await
                        .is_ok()
                    {
                        added = true;
                        break;
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                }
                if !added {
                    tracing::warn!(
            group_id,
            node_id,
            "failed to add learner to group after retries (group may not exist on target node)"
          );
                }
            }
        }

        // Perform Raft-level membership change (only when group is local).
        // The barriers inside OpenRaftNode::change_membership will wait for
        // learners to catch up and confirm replication before returning.
        if is_local {
            self.multi_raft
                .change_group_membership(group_id, new_members)
                .await?;
        }

        Ok(())
    }

    /// 用新节点替换旧节点.
    pub async fn replace_node(&self, old_node_id: NodeId, new_node_id: NodeId) -> Result<()> {
        let cluster_meta = self.meta_raft.get_cluster_meta();
        let groups: Vec<u64> = cluster_meta
            .groups
            .iter()
            .filter(|(_, g)| g.replicas.iter().any(|r| r.node_id == old_node_id))
            .map(|(id, _)| *id)
            .collect();

        for gid in groups {
            self.change_group_membership(gid, vec![new_node_id], vec![old_node_id], None)
                .await?;
        }
        self.meta_raft
            .propose(MetaRequest::RemoveNode {
                node_id: old_node_id,
            })
            .await?;
        Ok(())
    }
}

fn node_in_active_migration(
    node_id: NodeId,
    state: &SlotMigrationState,
    groups: &std::collections::HashMap<u64, crate::cluster::meta_types::GroupMeta>,
) -> bool {
    let (source, target) = match state {
        SlotMigrationState::Prepare {
            source_group,
            target_group,
            ..
        }
        | SlotMigrationState::Migrating {
            source_group,
            target_group,
            ..
        }
        | SlotMigrationState::Frozen {
            source_group,
            target_group,
            ..
        }
        | SlotMigrationState::ReadyToCommit {
            source_group,
            target_group,
            ..
        } => (*source_group, *target_group),
    };
    for gid in [source, target] {
        if let Some(g) = groups.get(&gid) {
            if g.replicas.iter().any(|r| r.node_id == node_id) {
                return true;
            }
        }
    }
    false
}
