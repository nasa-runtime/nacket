use super::*;
use crate::domain::message::{
    control::nonce,
    envelope::{identifier, revision},
    UnixMillis,
};
use std::collections::{BTreeMap, BTreeSet};

impl Ledger {
    /// 业务作用：将连续时钟的精确许可失效证据转为待提交候选，任何并发续期都使旧证据无效。
    /// 参数说明：
    /// - `proof`：由已提交快照完整观察得到的不可反序列化证据。
    /// - `now`：操作完成的诊断时间。
    ///
    /// 返回：有实际变化时返回候选；必须再次通过后端 CAS 才能解除全局屏障。
    pub fn expire(
        &self,
        proof: &super::expiry::Expired,
        now: UnixMillis,
    ) -> anyhow::Result<Option<Transition>> {
        anyhow::ensure!(
            proof.cluster_id == self.cluster_id && proof.scope_epoch == self.scope_epoch,
            "expiry_scope_mismatch"
        );
        let mut next = self.clone();
        let mut changed = false;
        for (id, expected) in &proof.permits {
            let Some(permit) = next.permits.get(id) else {
                continue;
            };
            anyhow::ensure!(
                super::expiry::fingerprint(permit)? == *expected,
                "expiry_proof_stale"
            );
            // 观察覆盖完整许可寿命与部署余量后才解除责任，节点短暂失联或墙钟超前都不够。
            next.permits.remove(id);
            next.authorization_drained(id, now);
            next.quota_drained(id);
            for operation in next.operations.values_mut() {
                operation.barrier.remove(id);
            }
            changed = true;
        }
        if !changed {
            return Ok(None);
        }
        next.advance(now);
        next.sequence = next
            .sequence
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("control_sequence_exhausted"))?;
        Ok(Some(Transition {
            ledger: next,
            reply: Reply::Recorded {},
        }))
    }

    /// 业务作用：创建尚未授予任何节点许可的控制作用域。
    /// 参数说明：
    /// - `cluster_id`：部署指定的集群隔离标识。
    /// - `scope_epoch`：持久控制基线所属代际。
    ///
    /// 返回：空权威候选；必须由后端持久建立后才能参与控制处理。
    pub fn empty(cluster_id: String, scope_epoch: String) -> anyhow::Result<Self> {
        identifier(&cluster_id)?;
        revision(&scope_epoch)?;
        Ok(Self {
            cluster_id,
            scope_epoch,
            policy_digest: String::new(),
            sequence: 0,
            routes: BTreeMap::new(),
            nodes: BTreeMap::new(),
            permits: BTreeMap::new(),
            operations: BTreeMap::new(),
            ownership_actions: BTreeMap::new(),
            manifests: BTreeMap::new(),
            retirements: BTreeMap::new(),
            reservations: BTreeMap::new(),
            assertions: Default::default(),
            authorizations: BTreeMap::new(),
        })
    }

    /// 业务作用：从已提交状态计算完整候选，任一校验失败都不会部分更新当前权威。
    /// 参数说明：
    /// - `principal`：入口已验证的管理员或节点身份。
    /// - `command`：有界控制操作。
    /// - `now`：持久操作的整数毫秒诊断时间，不能单独证明许可失效。
    /// - `limits`：部署冻结的容量与许可 TTL。
    ///
    /// 返回：待持久提交的状态和回复；未完成后端提交前不得对外报告成功。
    pub fn transition(
        &self,
        principal: &Principal,
        command: &Command,
        now: UnixMillis,
        limits: &Limits,
    ) -> anyhow::Result<Transition> {
        anyhow::ensure!(
            limits.routes > 0
                && limits.routes <= 1024
                && limits.nodes > 0
                && limits.nodes <= 256
                && limits.operations > 0
                && limits.operations <= 65_536
                && limits.permits > 0
                && limits.permits <= 65_536
                && (1000..=60_000).contains(&limits.permit_ttl_ms),
            "invalid_control_limits"
        );
        let mut next = self.clone();
        let reply = next.apply(principal, command, now, limits)?;
        next.sequence = next
            .sequence
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("control_sequence_exhausted"))?;
        anyhow::ensure!(
            serde_json::to_vec(&next)?.len() <= 16 * 1024 * 1024,
            "control_snapshot_capacity"
        );
        Ok(Transition {
            ledger: next,
            reply,
        })
    }

    /// 业务作用：在唯一候选上执行来源检查、CAS、节点代际与许可屏障。
    /// 参数说明：
    /// - `principal`：已验证的入口来源。
    /// - `command`：待执行的单个控制动作。
    /// - `now`：操作记录的诊断时间。
    /// - `limits`：当前控制作用域的容量上限。
    ///
    /// 返回：候选回复；权限不符、同 ID 异内容或前序未结束均拒绝。
    pub(super) fn apply(
        &mut self,
        principal: &Principal,
        command: &Command,
        now: UnixMillis,
        limits: &Limits,
    ) -> anyhow::Result<Reply> {
        match command {
            Command::ReserveConnection {
                ..
            }
            | Command::ReleaseConnection {
                ..
            } => self.quota(principal, command),
            Command::ApplyAuthorization {
                ..
            }
            | Command::AuthorizationApplied {
                ..
            } => self.authorization(principal, command, now),
            Command::Observe {} => Ok(Reply::Recorded {}),
            Command::ConsumeAssertion {
                session_key,
                attempt_id,
                digest,
                expires_at,
            } => {
                let (node, incarnation) = self.current_node(principal)?;
                let owner = crate::domain::assertion::owner(session_key, attempt_id)?;
                anyhow::ensure!(
                    session_key.cluster_id == self.cluster_id
                        && session_key.scope_epoch == self.scope_epoch
                        && session_key.node_id == node
                        && session_key.incarnation == incarnation.to_string(),
                    "assertion_session_scope"
                );
                let route = self
                    .routes
                    .get(&session_key.route_id)
                    .ok_or_else(|| anyhow::anyhow!("route_missing"))?;
                let binding = Binding::of(&route.definition);
                // 旧定义、未准备节点和控制专用节点不能为新的接入尝试消耗断言或取得资格。
                anyhow::ensure!(
                    route.definition.route.enabled
                        && matches!(
                            route.definition.route.identity,
                            crate::domain::route::Identity::TrustedGateway { .. }
                        )
                        && binding.revision.to_string() == session_key.route_revision
                        && binding.digest == session_key.definition_digest
                        && self.nodes[&node].prepared.get(&binding.route_id) == Some(&binding)
                        && matches!(
                            self.operations[&route.operation_id].phase,
                            Phase::Activating | Phase::Effective
                        )
                        && self.permits.values().any(|permit| permit.node_id == node
                            && permit.incarnation == incarnation
                            && permit.binding == binding),
                    "assertion_route_unavailable"
                );
                self.assertions.consume(digest, &owner, *expires_at, now)?;
                Ok(Reply::Recorded {})
            }
            Command::Manifest {
                ..
            }
            | Command::ApplyManifest {
                ..
            } => self.manifest(principal, command, now, limits),
            Command::Batch {
                commands,
            } => {
                anyhow::ensure!(
                    matches!(principal, Principal::Node { .. }),
                    "node_identity_required"
                );
                anyhow::ensure!(
                    (1..=64).contains(&commands.len())
                        && commands.iter().all(|command| matches!(
                            command,
                            Command::Prepared { .. }
                                | Command::Renew { .. }
                                | Command::RenewAccess { .. }
                                | Command::Applied { .. }
                                | Command::Drained { .. }
                                | Command::Failed { .. }
                                | Command::AuthorizationApplied { .. }
                                | Command::ReserveConnection { .. }
                                | Command::ReleaseConnection { .. }
                        )),
                    "invalid_node_batch"
                );
                let mut replies = Vec::with_capacity(commands.len());
                for command in commands {
                    // 每个节点动作都先完成全部检查再改状态；一个路由拒绝不能延长它的许可或阻止其它路由续期。
                    replies.push(match self.apply(principal, command, now, limits) {
                        Ok(reply) => reply,
                        Err(error) => Reply::Rejected {
                            code: error.to_string(),
                        },
                    });
                }
                Ok(Reply::Batch {
                    replies,
                })
            }
            Command::Offline {
                ..
            }
            | Command::Resume {
                ..
            }
            | Command::Transfer {
                ..
            } => self.ownership(principal, command, now, limits),
            Command::Register {} | Command::RegisterController {} => {
                let Principal::Node {
                    node_id,
                    startup_request,
                } = principal
                else {
                    anyhow::bail!("node_identity_required")
                };
                identifier(node_id)?;
                identifier(startup_request)?;
                if let Some(node) = self.nodes.get(node_id) {
                    if let Some(incarnation) = node.startups.get(startup_request) {
                        return Ok(Reply::Registered {
                            incarnation: incarnation.to_string(),
                        });
                    }
                }
                anyhow::ensure!(
                    self.nodes.contains_key(node_id) || self.nodes.len() < limits.nodes,
                    "node_capacity"
                );
                let incarnation = self
                    .nodes
                    .get(node_id)
                    .map_or(Some(1), |node| node.incarnation.checked_add(1))
                    .ok_or_else(|| anyhow::anyhow!("incarnation_exhausted"))?;
                let mut startups = self
                    .nodes
                    .get(node_id)
                    .map_or_else(BTreeMap::new, |node| node.startups.clone());
                anyhow::ensure!(startups.len() < 1024, "startup_history_capacity");
                startups.insert(startup_request.clone(), incarnation);
                // 新代际立即阻止旧进程续期，但旧许可继续保留，不能把分配新代际当成已关闭旧进程。
                self.nodes.insert(
                    node_id.clone(),
                    Node {
                        incarnation,
                        startup_request: startup_request.clone(),
                        prepared: BTreeMap::new(),
                        startups,
                        active: true,
                        serving: matches!(command, Command::Register {}),
                        authorizations: BTreeMap::new(),
                        quota_watermark: 0,
                        access: BTreeMap::new(),
                    },
                );
                Ok(Reply::Registered {
                    incarnation: incarnation.to_string(),
                })
            }
            Command::Define {
                ..
            } => {
                anyhow::ensure!(
                    matches!(principal, Principal::Administrator { .. }),
                    "manifest_required"
                );
                self.define(principal, command, now, limits)
            }
            Command::Drained {
                permit_id,
            } => {
                let Principal::Node {
                    node_id,
                    startup_request,
                } = principal
                else {
                    anyhow::bail!("node_identity_required")
                };
                let Some(permit) = self.permits.get(permit_id) else {
                    return Ok(Reply::Recorded {});
                };
                anyhow::ensure!(
                    permit.node_id == *node_id && permit.startup_request == *startup_request,
                    "permit_owner_mismatch"
                );
                // 旧代际仍可报告自身已排空，来源必须精确匹配原许可，不能代替其它节点确认。
                self.permits.remove(permit_id);
                self.authorization_drained(permit_id, now);
                self.quota_drained(permit_id);
                for operation in self.operations.values_mut() {
                    operation.barrier.remove(permit_id);
                }
                self.advance(now);
                Ok(Reply::Recorded {})
            }
            Command::Prepared {
                binding,
            } => {
                let (id, incarnation) = self.current_node(principal)?;
                anyhow::ensure!(
                    !self
                        .permits
                        .values()
                        .any(|permit| permit.node_id == id && permit.incarnation != incarnation),
                    "previous_incarnation_not_drained"
                );
                let operation = self.operation_mut(binding)?;
                anyhow::ensure!(
                    matches!(
                        operation.phase,
                        Phase::Preparing | Phase::Activating | Phase::Effective
                    ),
                    "route_quiescing"
                );
                operation.prepared.insert(node_key(&id, incarnation));
                operation.failures.remove(&node_key(&id, incarnation));
                self.nodes
                    .get_mut(&id)
                    .expect("current node")
                    .prepared
                    .insert(binding.route_id.clone(), binding.clone());
                self.advance(now);
                Ok(Reply::Recorded {})
            }
            Command::Renew {
                binding,
                nonce: request_nonce,
            }
            | Command::RenewAccess {
                binding,
                nonce: request_nonce,
                ..
            } => {
                let listener = match command {
                    Command::RenewAccess {
                        listener,
                        ..
                    } => listener.as_ref(),
                    _ => None,
                };
                if let Some(listener) = listener {
                    listener.validate()?;
                }
                identifier(request_nonce)?;
                let (id, incarnation) = self.current_node(principal)?;
                let node = &self.nodes[&id];
                // 旧授权投影不能续期；节点必须先应用全部撤权并等待受影响写出结束。
                anyhow::ensure!(
                    node.authorizations
                        .get(&binding.route_id)
                        .copied()
                        .unwrap_or(0)
                        == self
                            .authorizations
                            .get(&binding.route_id)
                            .map_or(0, |state| state.revision),
                    "authorization_projection_stale"
                );
                anyhow::ensure!(
                    node.prepared.get(&binding.route_id) == Some(binding),
                    "route_not_prepared"
                );
                anyhow::ensure!(
                    !self
                        .permits
                        .values()
                        .any(|permit| permit.node_id == id && permit.incarnation != incarnation),
                    "previous_incarnation_not_drained"
                );
                let route = self
                    .routes
                    .get(&binding.route_id)
                    .ok_or_else(|| anyhow::anyhow!("route_missing"))?;
                if let Some(listener) = listener {
                    for endpoint in &listener.endpoints {
                        let expected = match endpoint.adapter {
                            crate::config::listeners::Protocol::NativeTcp
                            | crate::config::listeners::Protocol::NativeWs => {
                                route.definition.route.access.native_endpoint()
                            }
                            crate::config::listeners::Protocol::Socketio => {
                                route.definition.route.access.socketio_namespace()
                            }
                        };
                        anyhow::ensure!(
                            expected == Some(endpoint.endpoint.as_str()),
                            "access_route_mismatch"
                        );
                    }
                }
                let operation = &self.operations[&route.operation_id];
                anyhow::ensure!(
                    route.definition.route.enabled
                        && operation.binding == *binding
                        && matches!(operation.phase, Phase::Activating | Phase::Effective),
                    "route_permit_denied"
                );
                let current = self
                    .permits
                    .iter()
                    .find(|(_, permit)| {
                        permit.node_id == id
                            && permit.incarnation == incarnation
                            && permit.binding == *binding
                    })
                    .map(|(id, _)| id.clone());
                if let Some(current) = current
                    .as_ref()
                    .and_then(|id| self.permits.get(id))
                    .filter(|permit| permit.request_nonce == *request_nonce)
                {
                    anyhow::ensure!(
                        node.access
                            .get(&binding.route_id)
                            .map(|access| &access.listener)
                            == listener,
                        "access_nonce_conflict"
                    );
                    return Ok(Reply::Permit {
                        permit: current.clone(),
                    });
                }
                anyhow::ensure!(
                    current.is_some() || self.permits.len() < limits.permits,
                    "permit_capacity"
                );
                let permit = Permit {
                    id: current.unwrap_or_else(nonce),
                    node_id: id,
                    incarnation,
                    startup_request: node.startup_request.clone(),
                    binding: binding.clone(),
                    request_nonce: request_nonce.clone(),
                    ttl_ms: limits.permit_ttl_ms,
                    granted_at: now,
                };
                // 公开地址与许可同次提交，不能在权威确认之前把本地 bind 声明为集群 Serving。
                self.publish_access(&permit, listener)?;
                self.permits.insert(permit.id.clone(), permit.clone());
                Ok(Reply::Permit {
                    permit,
                })
            }
            Command::Applied {
                binding,
                permit_id,
            } => {
                let (id, incarnation) = self.current_node(principal)?;
                let permit = self
                    .permits
                    .get(permit_id)
                    .ok_or_else(|| anyhow::anyhow!("permit_missing"))?;
                anyhow::ensure!(
                    permit.node_id == id
                        && permit.incarnation == incarnation
                        && permit.binding == *binding,
                    "applied_scope_mismatch"
                );
                let operation = self.operation_mut(binding)?;
                operation.applied.insert(node_key(&id, incarnation));
                if let Some(access) = self
                    .nodes
                    .get_mut(&id)
                    .expect("current node")
                    .access
                    .get_mut(&binding.route_id)
                {
                    if access.permit_id == *permit_id {
                        access.serving = true;
                    }
                }
                self.advance(now);
                Ok(Reply::Recorded {})
            }
            Command::Failed {
                binding,
                reason,
            } => {
                identifier(reason)?;
                let (id, incarnation) = self.current_node(principal)?;
                self.operation_mut(binding)?
                    .failures
                    .insert(node_key(&id, incarnation), reason.clone());
                Ok(Reply::Recorded {})
            }
            Command::Retire {
                ..
            } => self.retire(principal, command, now, limits),
        }
    }

    /// 业务作用：从账本核对节点启动请求，旧进程不能借复用 nodeId 取得新代际权限。
    /// 参数说明：
    /// - `principal`：已认证的节点来源。
    ///
    /// 返回：当前稳定节点和持久 incarnation；未登记或旧启动申请拒绝。
    pub(super) fn current_node(&self, principal: &Principal) -> anyhow::Result<(String, u64)> {
        let Principal::Node {
            node_id,
            startup_request,
        } = principal
        else {
            anyhow::bail!("node_identity_required")
        };
        let node = self
            .nodes
            .get(node_id)
            .ok_or_else(|| anyhow::anyhow!("node_not_registered"))?;
        anyhow::ensure!(
            node.active && node.serving && node.startup_request == *startup_request,
            "node_fenced"
        );
        Ok((node_id.clone(), node.incarnation))
    }

    /// 业务作用：取得精确当前定义所属的操作，迟到节点回执不能推进另一个代次。
    /// 参数说明：
    /// - `binding`：节点报告携带的完整路由作用域。
    ///
    /// 返回：唯一匹配的操作；摘要或代次不一致时拒绝。
    fn operation_mut(&mut self, binding: &Binding) -> anyhow::Result<&mut Operation> {
        let route = self
            .routes
            .get(&binding.route_id)
            .ok_or_else(|| anyhow::anyhow!("route_missing"))?;
        let operation = self
            .operations
            .get_mut(&route.operation_id)
            .expect("route operation");
        anyhow::ensure!(operation.binding == *binding, "route_binding_changed");
        Ok(operation)
    }

    /// 业务作用：按完整回执推进全局阶段，墙钟或 presence 缺席不会解除任何旧写出屏障。
    /// 参数说明：
    /// - `now`：完成状态的诊断时间。
    ///
    /// 返回：只推进已有充分证据的操作；尚无服务节点的上线保持未完成。
    pub(super) fn advance(&mut self, now: UnixMillis) {
        for route in self.routes.values_mut() {
            let operation = self
                .operations
                .get_mut(&route.operation_id)
                .expect("route operation");
            if operation.phase == Phase::Quiescing && operation.barrier.is_empty() {
                operation.phase = if route.definition.route.enabled {
                    Phase::Preparing
                } else {
                    Phase::Effective
                };
            }
            if operation.phase == Phase::Preparing
                && operation.prepared.len() >= operation.minimum_ready
            {
                operation.phase = Phase::Activating;
            }
            if operation.phase == Phase::Activating
                && operation.applied.len() >= operation.minimum_ready
                && operation.targets.is_subset(&operation.applied)
            {
                operation.phase = Phase::Effective;
            }
            if operation.phase == Phase::Effective {
                operation.completed_at.get_or_insert(now);
                route.previous.clear();
            }
        }
    }

    /// 业务作用：在定义提交边界检查全部新旧接入映射，排空前不能把旧入口转交其它路由。
    /// 参数说明：
    /// - `route_id`：当前正在变更的路由。
    /// - `candidate`：候选完整定义。
    ///
    /// 返回：映射唯一；共享模板、独立模板和适配器分别组成作用域。
    pub(super) fn validate_paths(
        &self,
        route_id: &str,
        candidate: &Definition,
    ) -> anyhow::Result<()> {
        let mut paths = BTreeSet::new();
        for (id, route) in &self.routes {
            if id == route_id {
                continue;
            }
            for definition in std::iter::once(&route.definition).chain(route.previous.iter()) {
                if definition.route.enabled {
                    for path in paths_for(definition) {
                        paths.insert(path);
                    }
                }
            }
        }
        if candidate.route.enabled {
            for path in paths_for(candidate) {
                anyhow::ensure!(!paths.contains(&path), "route_mapping_conflict");
            }
        }
        Ok(())
    }
}

impl Binding {
    /// 业务作用：从已冻结定义提取所有控制请求共同使用的精确作用域。
    /// 参数说明：
    /// - `definition`：包含摘要和资源快照的完整定义。
    ///
    /// 返回：稳定 routeId、单调 revision 与定义摘要。
    pub fn of(definition: &Definition) -> Self {
        Self {
            route_id: definition.route.route_id.clone(),
            revision: definition.route.revision,
            digest: definition.digest.clone(),
        }
    }
}

/// 业务作用：为操作目标集合区分同一 nodeId 的不同启动代际。
/// 参数说明：
/// - `node`：稳定节点 ID。
/// - `incarnation`：持久分配的实例代际。
///
/// 返回：没有拼接歧义的规范目标键。
pub(super) fn node_key(node: &str, incarnation: u64) -> String {
    serde_json::to_string(&(node, incarnation.to_string())).expect("node identity")
}

/// 业务作用：枚举实际接入映射的唯一性键，独立路由可以在不同物理端口使用同一路径。
/// 参数说明：
/// - `definition`：需要检查的冻结定义。
///
/// 返回：适配器、监听模板、独立路由和路径组成的精确键集合。
fn paths_for(definition: &Definition) -> Vec<(String, String, Option<String>, String)> {
    [
        ("native", definition.route.access.native_endpoint()),
        ("socketio", definition.route.access.socketio_namespace()),
    ]
    .into_iter()
    .filter_map(|(adapter, path)| {
        path.map(|path| {
            (
                adapter.into(),
                definition.route.access.listener_ref.clone(),
                definition
                    .route
                    .access
                    .dedicated_port
                    .enabled
                    .then(|| definition.route.route_id.clone()),
                path.into(),
            )
        })
    })
    .collect()
}
