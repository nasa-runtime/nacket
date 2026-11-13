use super::*;
use crate::domain::{
    message::envelope::{identifier, SessionKey},
    route::policies::{QuotaScope, SameUserPolicy},
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;

/// 集群账本同时保留的连接名额硬上限，待排空名额仍占用容量。
pub const MAXIMUM_RESERVATIONS: usize = 16384;

#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
/// 集群名额的权威状态，排空屏障完成前不能重新授予接流资格。
pub enum ReservationPhase {
    /// 等待被替换会话的旧写出结束，尚不能 Ready。
    Waiting,
    /// 已满足接管条件，仍须持有有效路由许可。
    Active,
    /// 资格已撤销但责任尚未排空，不能当作名额已归还。
    Revoking,
}

/// 集群名额绑定精确会话和有限路由许可，连接断开与替换都须先结束实际写出。
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Reservation {
    /// 首次申请该名额的节点动作序号，归还水位阻止迟到申请重新开放会话。
    pub ordinal: u64,
    /// 名额唯一绑定的完整精确会话键，节点重启不自动转移旧责任。
    pub session_key: SessionKey,
    /// 配额所属的已验证租户，同用户标识在不同租户分别计算。
    pub tenant_id: String,
    /// 当前连接的真实用户身份，同用户策略仅在同租户同路由内生效。
    pub uid: String,
    /// 建立名额时对应的有限路由许可，用于失联后证明旧写出合法结束。
    pub permit_id: String,
    /// 等待旧会话、已可用或正在撤销的权威状态，只有 Active 才能继续 Ready。
    pub phase: ReservationPhase,
    /// 替换前必须结束的旧名额 ID 集合，不能因取消等待或节点缺席直接删除。
    pub barrier: BTreeSet<String>,
}

/// 业务作用：为完整会话建立固定长度的名额标识，不按用户或连接局部编号合并责任。
/// 参数说明：
/// - `key`：服务端生成的完整会话作用域。
///
/// 返回：规范键的 SHA256 摘要；非法会话拒绝建立名额。
pub fn reservation_id(key: &SessionKey) -> anyhow::Result<String> {
    key.validate()?;
    Ok(format!("{:x}", Sha256::digest(serde_json::to_vec(key)?)))
}

impl Ledger {
    /// 业务作用：由当前节点在同一持久边界申请或归还集群名额，旧写出责任不依赖 presence 判断。
    /// 参数说明：
    /// - `principal`：已验证节点身份。
    /// - `command`：精确会话申请或真实排空后的归还。
    ///
    /// 返回：持久候选回复；来源、定义、配额或序号不符时保持原状态。
    pub(super) fn quota(
        &mut self,
        principal: &Principal,
        command: &Command,
    ) -> anyhow::Result<Reply> {
        let (node_id, incarnation) = self.current_node(principal)?;
        match command {
            Command::ReserveConnection {
                ordinal,
                session_key,
                tenant_id,
                uid,
            } => {
                let id = reservation_id(session_key)?;
                identifier(tenant_id)?;
                identifier(uid)?;
                anyhow::ensure!(
                    *ordinal > 0
                        && session_key.cluster_id == self.cluster_id
                        && session_key.scope_epoch == self.scope_epoch
                        && session_key.node_id == node_id
                        && session_key.incarnation == incarnation.to_string(),
                    "quota_session_scope"
                );
                if let Some(existing) = self.reservations.get(&id) {
                    anyhow::ensure!(
                        existing.ordinal == *ordinal
                            && existing.tenant_id == *tenant_id
                            && existing.uid == *uid,
                        "quota_reservation_conflict"
                    );
                    return Ok(Reply::ConnectionReserved {
                        reservation: existing.clone(),
                    });
                }
                // 归还也推进序号下界，先到的取消必须永久阻止迟到申请重新占用或开放会话。
                anyhow::ensure!(
                    *ordinal > self.nodes[&node_id].quota_watermark,
                    "quota_admission_stale"
                );
                let route = self
                    .routes
                    .get(&session_key.route_id)
                    .ok_or_else(|| anyhow::anyhow!("route_missing"))?;
                let binding = Binding::of(&route.definition);
                let policy = &route.definition.route.connections;
                anyhow::ensure!(
                    policy.scope == QuotaScope::Cluster
                        && route.definition.route.enabled
                        && binding.revision.to_string() == session_key.route_revision
                        && binding.digest == session_key.definition_digest
                        && self.nodes[&node_id].prepared.get(&binding.route_id) == Some(&binding)
                        && matches!(
                            self.operations[&route.operation_id].phase,
                            Phase::Activating | Phase::Effective
                        ),
                    "quota_route_unavailable"
                );
                let permit = self
                    .permits
                    .values()
                    .find(|permit| {
                        permit.node_id == node_id
                            && permit.incarnation == incarnation
                            && permit.binding == binding
                    })
                    .ok_or_else(|| anyhow::anyhow!("quota_permit_missing"))?;
                let permit_id = permit.id.clone();
                anyhow::ensure!(
                    self.reservations.len() < MAXIMUM_RESERVATIONS,
                    "cluster_quota_capacity"
                );
                let current = self
                    .reservations
                    .values()
                    .filter(|entry| entry.session_key.route_id == binding.route_id)
                    .count();
                let same = self
                    .reservations
                    .iter()
                    .filter(|(_, entry)| {
                        entry.session_key.route_id == binding.route_id
                            && entry.tenant_id == *tenant_id
                            && entry.uid == *uid
                    })
                    .map(|(id, entry)| (id.clone(), entry.phase))
                    .collect::<Vec<_>>();
                let replacement = policy.same_user_policy == SameUserPolicy::ReplaceOld;
                match policy.same_user_policy {
                    SameUserPolicy::RejectNew => {
                        anyhow::ensure!(same.is_empty(), "user_already_connected")
                    }
                    SameUserPolicy::AllowMultiple => anyhow::ensure!(
                        !policy.limit_enabled || same.len() < policy.max_per_uid,
                        "user_connection_capacity"
                    ),
                    SameUserPolicy::ReplaceOld => anyhow::ensure!(
                        same.iter()
                            .all(|(_, phase)| *phase == ReservationPhase::Active),
                        "user_replacement_pending"
                    ),
                }
                let retained = current
                    - if replacement {
                        same.len()
                    } else {
                        0
                    };
                anyhow::ensure!(
                    !policy.limit_enabled || retained < policy.max_connections,
                    "route_connection_capacity"
                );
                let barrier: BTreeSet<_> = if replacement {
                    same.into_iter().map(|(id, _)| id).collect()
                } else {
                    BTreeSet::new()
                };
                let reservation = Reservation {
                    ordinal: *ordinal,
                    session_key: session_key.clone(),
                    tenant_id: tenant_id.clone(),
                    uid: uid.clone(),
                    permit_id,
                    phase: if barrier.is_empty() {
                        ReservationPhase::Active
                    } else {
                        ReservationPhase::Waiting
                    },
                    barrier,
                };
                // 先持久记录旧会话撤销与新会话等待，任何节点都不能把受理替换解释为可以 Ready。
                for old in &reservation.barrier {
                    self.reservations
                        .get_mut(old)
                        .expect("selected reservation")
                        .phase = ReservationPhase::Revoking;
                }
                self.nodes
                    .get_mut(&node_id)
                    .expect("current node")
                    .quota_watermark = *ordinal;
                self.reservations.insert(id, reservation.clone());
                Ok(Reply::ConnectionReserved {
                    reservation,
                })
            }
            Command::ReleaseConnection {
                ordinal,
                session_key,
            } => {
                let id = reservation_id(session_key)?;
                anyhow::ensure!(
                    *ordinal > 0
                        && session_key.cluster_id == self.cluster_id
                        && session_key.scope_epoch == self.scope_epoch
                        && session_key.node_id == node_id
                        && session_key.incarnation == incarnation.to_string(),
                    "quota_session_scope"
                );
                if let Some(existing) = self.reservations.get(&id) {
                    anyhow::ensure!(existing.ordinal == *ordinal, "quota_reservation_conflict");
                }
                let node = self.nodes.get_mut(&node_id).expect("current node");
                node.quota_watermark = node.quota_watermark.max(*ordinal);
                self.remove_reservations(&BTreeSet::from([id]));
                Ok(Reply::Recorded {})
            }
            _ => anyhow::bail!("invalid_quota_command"),
        }
    }

    /// 业务作用：只有精确路由许可已排空或取得完整失效证明后，才回收失联节点的会话名额。
    /// 参数说明：
    /// - `permit_id`：已经从权威许可集合解除的精确责任。
    ///
    /// 返回：关联名额及其替换屏障已收敛；节点重启、墙钟到期本身不会调用此入口。
    pub(super) fn quota_drained(&mut self, permit_id: &str) {
        let removed = self
            .reservations
            .iter()
            .filter(|(_, entry)| entry.permit_id == permit_id)
            .map(|(id, _)| id.clone())
            .collect();
        self.remove_reservations(&removed);
    }

    /// 业务作用：原子解除已证明结束的会话责任，并仅在全部旧责任结束后开放替换名额。
    /// 参数说明：
    /// - `removed`：精确会话名额集合。
    ///
    /// 返回：未受影响的会话保留；等待者不能越过任何剩余旧写出责任。
    fn remove_reservations(&mut self, removed: &BTreeSet<String>) {
        self.reservations.retain(|id, _| !removed.contains(id));
        for entry in self.reservations.values_mut() {
            entry.barrier.retain(|id| !removed.contains(id));
            if entry.phase == ReservationPhase::Waiting && entry.barrier.is_empty() {
                entry.phase = ReservationPhase::Active;
            }
        }
    }

    /// 业务作用：恢复名额时复验完整会话、节点序号、路由许可及替换屏障，缺失基线不能扩大准入。
    /// 参数说明: 无。
    ///
    /// 返回：全部责任可关联；不完整或跨作用域记录拒绝恢复。
    pub(super) fn validate_quotas(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.reservations.len() <= MAXIMUM_RESERVATIONS,
            "quota_baseline_capacity"
        );
        let mut ordinals = BTreeSet::new();
        for (id, entry) in &self.reservations {
            let key = &entry.session_key;
            identifier(&entry.tenant_id)?;
            identifier(&entry.uid)?;
            anyhow::ensure!(
                *id == reservation_id(key)?
                    && entry.ordinal > 0
                    && key.cluster_id == self.cluster_id
                    && key.scope_epoch == self.scope_epoch
                    && ordinals.insert((&key.node_id, &key.incarnation, entry.ordinal)),
                "quota_baseline_identity"
            );
            let permit = self
                .permits
                .get(&entry.permit_id)
                .ok_or_else(|| anyhow::anyhow!("quota_baseline_permit_missing"))?;
            anyhow::ensure!(
                permit.node_id == key.node_id
                    && permit.incarnation.to_string() == key.incarnation
                    && permit.binding.route_id == key.route_id
                    && permit.binding.revision.to_string() == key.route_revision
                    && permit.binding.digest == key.definition_digest,
                "quota_baseline_permit_mismatch"
            );
            let node = self
                .nodes
                .get(&key.node_id)
                .ok_or_else(|| anyhow::anyhow!("quota_baseline_node_missing"))?;
            anyhow::ensure!(
                node.incarnation.to_string() != key.incarnation
                    || node.quota_watermark >= entry.ordinal,
                "quota_baseline_watermark"
            );
            anyhow::ensure!(
                (entry.phase == ReservationPhase::Waiting) != entry.barrier.is_empty()
                    && entry
                        .barrier
                        .iter()
                        .all(
                            |old| self
                                .reservations
                                .get(old)
                                .is_some_and(|previous| previous.phase
                                    == ReservationPhase::Revoking
                                    && previous.session_key.route_id == key.route_id
                                    && previous.tenant_id == entry.tenant_id
                                    && previous.uid == entry.uid)
                        ),
                "quota_baseline_barrier"
            );
        }
        Ok(())
    }
}
