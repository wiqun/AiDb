use std::sync::Arc;

use openraft::RaftNetworkFactory;

use crate::cluster::network::RaftNetworkClient;
use crate::cluster::node::OpenRaftNode;
use crate::cluster::types::{ClusterError, NodeId, Request, Response};
use crate::error::Result;

use super::MultiRaftNode;

/// 半开用户 key 区间内字典序最大的一条. 空区间返回 `None`.
fn last_sm_user_pair(
    db: &crate::DB,
    group_id: u64,
    start_user: &[u8],
    end_user: &[u8],
) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
    use crate::cluster::storage::keys::{sm_key, user_key_from_sm_key};

    let start = sm_key(group_id, start_user);
    let end = sm_key(group_id, end_user);
    let mut iter = db.scan(Some(start.as_slice()), Some(end.as_slice()))?;
    iter.seek_to_last();
    let (Some(key), Some(value)) = (iter.key(), iter.value()) else {
        return Ok(None);
    };
    let Some(user_key) = user_key_from_sm_key(group_id, key) else {
        return Ok(None);
    };
    Ok(Some((user_key, value.to_vec())))
}

/// 按用户 key 的半开区间读取 group 状态机. `end_user == None` 扫到该 group 末尾.
/// `visit` 返回 false 时停止, 不再读后面的 key.
pub fn visit_sm_user_pairs(
    db: &crate::DB,
    group_id: u64,
    start_user: &[u8],
    end_user: Option<&[u8]>,
    mut visit: impl FnMut(Vec<u8>, Vec<u8>) -> bool,
) -> Result<()> {
    use crate::cluster::storage::keys::{sm_key, sm_range_end, user_key_from_sm_key};

    let start = sm_key(group_id, start_user);
    let end = match end_user {
        Some(end) => sm_key(group_id, end),
        None => sm_range_end(group_id),
    };
    let iter = db.scan(Some(start.as_slice()), Some(end.as_slice()))?;
    for item in iter {
        let (k, v) = item?;
        let Some(user_key) = user_key_from_sm_key(group_id, &k) else {
            continue;
        };
        if !visit(user_key, v) {
            break;
        }
    }
    Ok(())
}

fn collect_sm_user_pairs(
    db: &crate::DB,
    group_id: u64,
    start_user: &[u8],
    end_user: Option<&[u8]>,
) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
    let mut out = Vec::new();
    visit_sm_user_pairs(db, group_id, start_user, end_user, |key, value| {
        out.push((key, value));
        true
    })?;
    Ok(out)
}

impl MultiRaftNode {
    // ---- 读写操作 ----

    /// 向指定 Group 提交提案 (propose).
    ///
    /// Group 在本地时直接走本地 `OpenRaftNode::propose`; 否则 RPC 到该
    /// group 当前已知的 leader (`RemotePropose`, 与 Raft RPC 同一数据面
    /// gRPC 通道) —— 在线 slot 迁移的 `PutConditional` / `MigrationBarrier`
    /// 需要跨节点落到持有 target group 的节点. 超时/失败原样返回 Err.
    pub async fn propose_group(&self, group_id: u64, request: Request) -> Result<Response> {
        let group = self.groups.read().get(&group_id).cloned();
        match group {
            Some(node) => node.propose(request).await,
            None => self.propose_group_remote(group_id, request).await,
        }
    }

    /// `propose_group` 的跨节点 fallback: RPC 到目标 group 当前 leader 节点
    /// 执行 propose.
    async fn propose_group_remote(&self, group_id: u64, request: Request) -> Result<Response> {
        let mut client = self.remote_leader_client(group_id).await?;
        client.remote_propose(group_id, &request).await
    }

    /// 按 key 路由并提交提案 (单 key SET/DEL 入口).
    pub async fn propose_key(&self, key: Vec<u8>, value: Option<Vec<u8>>) -> Result<Response> {
        let (gid, _status) = self.router.route_key(&key)?;
        let request = match value {
            Some(v) => Request::Put { key, value: v },
            None => Request::Delete { key },
        };
        self.propose_group(gid, request).await
    }

    /// 按 key 路由并本地读取 (单 key GET 入口).
    pub async fn get_key(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>> {
        let (gid, _status) = self.router.route_key(&key)?;
        let group = self.groups.read().get(&gid).cloned();
        match group {
            Some(node) => node.get(key).await,
            None => Err(ClusterError::Raft(format!("group {} not found locally", gid)).into()),
        }
    }

    // ---- Phase 15 转发方法 ----

    /// 从指定 Group 的状态机直接读取 key (绕过路由).
    ///
    /// Group 在本地时直接走本地 leader-check / linearizable 读; 非本地时
    /// fallback 到 `get_key_from_group_remote` (RPC 到该 group leader) —
    /// slot 迁移 executor 在源节点上执行 `verify_migration` 时会对目标
    /// group 读取 key, 此时目标 group 不在本地, 必须跨节点读取.
    pub async fn get_key_from_group(&self, group_id: u64, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if !self.is_group_local(group_id) {
            return self.get_key_from_group_remote(group_id, key).await;
        }
        self.get_key_local(group_id, key).await
    }

    /// 本地 group 直读 (不检查是否本地, 由调用方保证).
    async fn get_key_local(&self, group_id: u64, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let group = self.groups.read().get(&group_id).cloned();
        match group {
            Some(node) => node.get(key.to_vec()).await,
            None => Err(ClusterError::Raft(format!("group {} not found locally", group_id)).into()),
        }
    }

    /// FIX-0056-A1"读导向"点 3: `get_key_from_group` 的跨节点合并读 fallback.
    ///
    /// Group 在本地时直接走本地 leader-check / linearizable 读. 否则 RPC 到
    /// 该 group 当前已知的 leader (`GetKey`, 与 Raft RPC 同一数据面 gRPC
    /// 通道), 超时/失败原样返回 Err —— 调用方必须将其映射为 `TRYAGAIN`,
    /// 禁止静默 fallback 到可能陈旧的本地视图.
    pub async fn get_key_from_group_remote(
        &self,
        group_id: u64,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>> {
        if self.is_group_local(group_id) {
            return self.get_key_local(group_id, key).await;
        }
        let mut client = self.remote_leader_client(group_id).await?;
        client.get_key(key).await
    }

    /// FIX-0056-A1: 读取指定 Group 在 `epoch` 下的迁移 oplog tip.
    ///
    /// 与 `get_key_from_group` 共用同一 leader / linearizable 语义 (A1
    /// "合并读线性点" 硬约束: tombstone/tip 必须在 target group leader 上读,
    /// 不允许落后 follower 冒充最新). 若 group 不在本节点: RPC 到该 group
    /// 当前已知的 leader (`GetMigrationTip`, 与 Raft RPC 同一数据面 gRPC
    /// 通道) —— "读导向"点 3 (`tip 跨节点读取`).
    pub async fn read_migration_tip(&self, group_id: u64, epoch: u64) -> Result<u64> {
        let group = self.groups.read().get(&group_id).cloned();
        match group {
            Some(node) => node.get_migration_tip(epoch).await,
            None => self.read_migration_tip_remote(group_id, epoch).await,
        }
    }

    async fn read_migration_tip_remote(&self, group_id: u64, epoch: u64) -> Result<u64> {
        let mut client = self.remote_leader_client(group_id).await?;
        client.get_migration_tip(epoch).await
    }

    /// FIX-0056-A1 合并读线性点第 1 步: 读取 target group 上 `epoch` 内 `key`
    /// 的迁移 tombstone (供 aikv 合并读判断 target miss 是"从未拷贝"还是
    /// "已被客户端 Del"). Group 本地时走本地 leader-check / linearizable 读
    /// (`OpenRaftNode::get_migration_tombstone`); 否则 RPC 到该 group 当前
    /// 已知的 leader (`GetMigrationTombstone`), 与 `get_key_from_group_remote`
    /// 同一超时/失败语义 (Err 必须映射为 TRYAGAIN, 不得当作"无 tombstone").
    pub async fn get_migration_tombstone_remote(
        &self,
        group_id: u64,
        epoch: u64,
        key: &[u8],
    ) -> Result<Option<crate::cluster::migration_oplog::MigOp>> {
        let group = self.groups.read().get(&group_id).cloned();
        match group {
            Some(node) => node.get_migration_tombstone(epoch, key.to_vec()).await,
            None => {
                let mut client = self.remote_leader_client(group_id).await?;
                client.get_migration_tombstone(epoch, key).await
            }
        }
    }

    /// FIX-0056-A1: 构造指向 `group_id` 当前已知 leader 的 `RaftNetworkClient`
    /// (与本地 Raft 对等通信同一 gRPC 通道/连接池). Leader 未知时返回 Err ——
    /// 调用方 (`get_key_from_group_remote` / `read_migration_tip` /
    /// `propose_group_remote`) 不得把"找不到 leader"悄悄当成"key 不存在"
    /// 或 tip=0.
    ///
    /// 目标地址解析顺序:
    /// 1. **MetaRaft 元数据中的 `rpc_addr`** — 跨 group 节点从未参与本地
    ///    Raft 对等通信, `network_factory` 缓存中没有其地址; 而元数据里
    ///    保存的是容器内可达的 Raft 对等地址, 是唯一可靠来源.
    /// 2. 元数据缺失 (如未接线 lifecycle 的测试环境) 时回退到 factory 缓存的
    ///    rpc_addr (`new_client` 收到空 `BasicNode.addr` 时自动回退).
    ///
    /// 不能用 `router.node_addrs` —— 后者优先 `client_addr` (外部可达的
    /// client 端口), 容器内跨节点 RPC 连它必然 Connection refused.
    async fn remote_leader_client(&self, group_id: u64) -> Result<RaftNetworkClient> {
        let leader = self
            .router
            .get_group_leader(group_id)
            .ok_or_else(|| ClusterError::Raft(format!("no known leader for group {group_id}")))?;
        let mut factory = self.network_factory.read().clone().with_group_id(group_id);
        let rpc_addr = self
            .lifecycle
            .meta_raft()
            .get_cluster_meta()
            .nodes
            .get(&leader)
            .map(|n| n.rpc_addr.clone());
        let basic_node = openraft::BasicNode {
            addr: rpc_addr.unwrap_or_default(),
        };
        Ok(factory.new_client(leader, &basic_node).await)
    }

    pub fn get_local_sync(&self, group_id: u64, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let node = self.groups.read().get(&group_id).cloned();
        match node {
            Some(node) => {
                let storage = node.storage();
                storage.get_state_machine_value(key)
            }
            None => Err(ClusterError::Raft(format!("group {group_id} not found locally")).into()),
        }
    }

    /// 直接读取本地 group 状态机 (不要求 leader).
    ///
    /// 与 `get_key_from_group` 不同, 本方法不检查 leader 身份, 直接读本地
    /// 状态机, 供数据面读路径 (leader 直读 / 只读副本) 使用.
    pub async fn get_local(&self, group_id: u64, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.get_local_sync(group_id, key)
    }

    /// 本地各 group 引擎条目估值之和. 每个 group 有独立 DB, 直接读 SST 与 MemTable 计数, 不扫描.
    pub fn estimate_entry_count(&self) -> u64 {
        self.storages.read().values().fold(0u64, |sum, storage| {
            sum.saturating_add(storage.db().estimate_entry_count())
        })
    }

    /// 扫描本地 group 状态机的全部 (user_key, value) 对.
    ///
    /// 返回的 key 已剥离 `sm/{gid}/` 前缀, 即与写入时传入的 user key 一致.
    pub async fn scan_local_pairs(&self, group_id: u64) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.scan_local_pairs_in_range(group_id, Vec::new(), None)
            .await
    }

    /// 半开区间内字典序最大的一条. 空区间返回 None.
    /// 直接读本地状态机, 不进入 `spawn_blocking`.
    pub fn last_local_user_pair_sync(
        &self,
        group_id: u64,
        start_user: &[u8],
        end_user: &[u8],
    ) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        let db = {
            let storages = self.storages.read();
            storages.get(&group_id).map(|storage| storage.db().clone())
        };
        match db {
            Some(db) => last_sm_user_pair(&db, group_id, start_user, end_user),
            None => Err(ClusterError::Raft(format!("group {group_id} not found locally")).into()),
        }
    }

    /// 半开区间内字典序最大的一条. 空区间返回 None.
    pub async fn last_local_user_pair(
        &self,
        group_id: u64,
        start_user: Vec<u8>,
        end_user: Vec<u8>,
    ) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        let storages = Arc::clone(&self.storages);
        tokio::task::spawn_blocking(move || {
            let db = {
                let guard = storages.read();
                guard.get(&group_id).map(|storage| storage.db().clone())
            };
            match db {
                Some(db) => last_sm_user_pair(&db, group_id, &start_user, &end_user),
                None => {
                    Err(ClusterError::Raft(format!("group {group_id} not found locally")).into())
                }
            }
        })
        .await
        .map_err(|e| ClusterError::Internal(e.to_string()))?
    }

    /// 只读用户 key 半开区间 `[start_user, end_user)` 里的状态机记录.
    /// 直接读本地状态机, 不进入 `spawn_blocking`.
    pub fn scan_local_pairs_in_range_sync(
        &self,
        group_id: u64,
        start_user: &[u8],
        end_user: Option<&[u8]>,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let db = {
            let storages = self.storages.read();
            storages.get(&group_id).map(|storage| storage.db().clone())
        };
        match db {
            Some(db) => collect_sm_user_pairs(&db, group_id, start_user, end_user),
            None => Err(ClusterError::Raft(format!("group {group_id} not found locally")).into()),
        }
    }

    /// 只读用户 key 半开区间 `[start_user, end_user)` 里的状态机记录。
    /// `end_user == None` 表示扫到该 group 状态机末尾。
    pub async fn scan_local_pairs_in_range(
        &self,
        group_id: u64,
        start_user: Vec<u8>,
        end_user: Option<Vec<u8>>,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let storages = Arc::clone(&self.storages);
        tokio::task::spawn_blocking(move || {
            let db = {
                let guard = storages.read();
                guard.get(&group_id).map(|storage| storage.db().clone())
            };
            match db {
                Some(db) => collect_sm_user_pairs(&db, group_id, &start_user, end_user.as_deref()),
                None => {
                    Err(ClusterError::Raft(format!("group {group_id} not found locally")).into())
                }
            }
        })
        .await
        .map_err(|e| ClusterError::Internal(e.to_string()))?
    }

    /// 按用户 key 区间遍历。`visit` 返回 false 时停止读取。
    pub async fn visit_local_user_range<F>(
        &self,
        group_id: u64,
        start_user: Vec<u8>,
        end_user: Option<Vec<u8>>,
        visit: F,
    ) -> Result<()>
    where
        F: FnMut(Vec<u8>, Vec<u8>) -> bool + Send + 'static,
    {
        let db = {
            let storages = self.storages.read();
            storages.get(&group_id).map(|s| s.db().clone())
        };
        match db {
            Some(db) => tokio::task::spawn_blocking(move || {
                visit_sm_user_pairs(&db, group_id, &start_user, end_user.as_deref(), visit)
            })
            .await
            .map_err(|e| ClusterError::Internal(e.to_string()))?,
            None => Err(ClusterError::Raft(format!("group {group_id} not found locally")).into()),
        }
    }

    /// 读取指定 key 在 Group 中的 TTL.
    /// 注意: AiDb 引擎层不支持逐 key TTL; 始终返回 None.
    pub async fn get_ttl_from_group(&self, group_id: u64, key: &[u8]) -> Result<Option<u64>> {
        let _ = (group_id, key);
        Ok(None)
    }

    /// 扫描 Group 状态机中的全部 key (可选按*用户 key* 范围过滤).
    ///
    /// 返回的 key 已剥离内部 `sm/{gid}/` 前缀, 与调用方写入时传入的 user
    /// key 完全一致 (与 `scan_local_pairs` 语义对齐) —— 之前这里直接返回
    /// DB 原始编码 key (带 `\x01sm/{gid}/` 前缀), 导致所有基于返回值算
    /// `key_to_slot()` 的调用方 (slot 迁移 executor 的 slot 过滤、
    /// `CLUSTER COUNTKEYSINSLOT`/`GETKEYSINSLOT`) 全部算出错误的 slot,
    /// 实质上从未按预期工作过.
    pub async fn scan_keys(
        &self,
        group_id: u64,
        key_range: Option<(Vec<u8>, Vec<u8>)>,
    ) -> Result<Vec<Vec<u8>>> {
        use crate::cluster::storage::keys::{
            sm_key, sm_range_end, sm_range_start, user_key_from_sm_key,
        };

        let db = {
            let storages = self.storages.read();
            storages.get(&group_id).map(|s| s.db().clone())
        };
        match db {
            Some(db) => {
                tokio::task::spawn_blocking(move || {
                    let (start, end) = match &key_range {
                        Some((s, e)) => (sm_key(group_id, s), sm_key(group_id, e)),
                        None => (sm_range_start(group_id), sm_range_end(group_id)),
                    };
                    let iter = db.scan(Some(start.as_slice()), Some(end.as_slice()))?;
                    let mut keys = Vec::new();
                    for item in iter {
                        let (k, _) = item?;
                        let Some(user_key) = user_key_from_sm_key(group_id, &k) else {
                            continue; // 越界/非本 group 的 sm key, 理论上不会出现
                        };
                        keys.push(user_key);
                    }
                    Ok(keys)
                })
                .await
                .map_err(|e| ClusterError::Internal(e.to_string()))?
            }
            None => Err(ClusterError::Raft(format!("group {} not found locally", group_id)).into()),
        }
    }

    /// 变更数据 Group 的成员 (joint consensus, 全量替换).
    pub async fn change_group_membership(&self, group_id: u64, members: Vec<NodeId>) -> Result<()> {
        let group = self.groups.read().get(&group_id).cloned();
        match group {
            Some(node) => node.change_membership(members).await,
            None => Err(ClusterError::Raft(format!("group {} not found locally", group_id)).into()),
        }
    }

    /// 添加 Learner 到数据 Group (non-blocking, 不等待日志同步).
    pub async fn add_learner_to_group(
        &self,
        group_id: u64,
        node_id: NodeId,
        address: String,
    ) -> Result<()> {
        let group = self.groups.read().get(&group_id).cloned();
        match group {
            Some(node) => node.add_learner_nonblocking(node_id, address).await,
            None => Err(ClusterError::Raft(format!("group {} not found locally", group_id)).into()),
        }
    }

    /// 关闭所有资源.
    pub async fn shutdown(&self) {
        // 发送关闭信号给 lifecycle task
        if let Some(tx) = self.shutdown_tx.lock().take() {
            let _ = tx.send(true);
        }
        // 关闭所有 OpenRaftNode
        let group_list: Vec<(u64, Arc<OpenRaftNode>)> = self.groups.write().drain().collect();
        for (_, node) in group_list {
            let _ = node.shutdown().await;
        }
        // 清理 storage
        self.storages.write().clear();
        // 中止 gRPC server
        if let Some(handle) = self.server_handle.lock().take() {
            handle.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::visit_sm_user_pairs;
    use crate::cluster::storage::{OpenRaftStorage, DEFAULT_GROUP_ID};
    use crate::cluster::types::ThinWriteBatch;
    use crate::config::Options;
    use crate::DB;
    use tempfile::TempDir;

    #[test]
    fn user_prefix_range_skips_other_keys_and_can_stop() {
        let dir = TempDir::new().unwrap();
        let db = DB::open(dir.path(), Options::for_testing()).unwrap();
        let storage = OpenRaftStorage::new(db, DEFAULT_GROUP_ID, None).unwrap();
        let mut batch = ThinWriteBatch::new();
        batch.put(b"key:1".to_vec(), b"a".to_vec());
        batch.put(b"KvDoc:a".to_vec(), b"b".to_vec());
        batch.put(b"KvDoc:b".to_vec(), b"c".to_vec());
        batch.put(b"zzz".to_vec(), b"d".to_vec());
        storage.apply_batch_to_sm(&batch).unwrap();

        let mut keys = Vec::new();
        visit_sm_user_pairs(
            storage.db(),
            DEFAULT_GROUP_ID,
            b"KvDoc:",
            Some(b"KvDoc;"),
            |key, _| {
                keys.push(key);
                true
            },
        )
        .unwrap();
        assert_eq!(keys, vec![b"KvDoc:a".to_vec(), b"KvDoc:b".to_vec()]);

        let mut seen = 0usize;
        visit_sm_user_pairs(
            storage.db(),
            DEFAULT_GROUP_ID,
            b"KvDoc:",
            Some(b"KvDoc;"),
            |_, _| {
                seen += 1;
                false
            },
        )
        .unwrap();
        assert_eq!(seen, 1);
    }
}
