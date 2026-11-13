use super::transition::node_key;
use super::*;
use crate::domain::{
    message::{envelope::identifier, UnixMillis},
    route::compiled::CompiledRoute,
};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};

impl Ledger {
    /// 业务作用：在已验证管理动作或受理清单内部提交冻结定义，统一保持旧许可屏障与来源归属。
    /// 参数说明：
    /// - `principal`：管理入口或清单处理器确认的来源身份。
    /// - `command`：具有唯一操作 ID 与路由 CAS 的定义命令。
    /// - `now`：操作记录的整数毫秒时间。
    /// - `limits`：控制作用域容量合同。
    ///
    /// 返回：完整候选内的定义操作；拒绝不会改变已持久状态。
    pub(super) fn define(
        &mut self,
        principal: &Principal,
        command: &Command,
        now: UnixMillis,
        limits: &Limits,
    ) -> anyhow::Result<Reply> {
        let Command::Define {
            operation_id,
            expected_revision,
            definition,
            minimum_ready,
        } = command
        else {
            anyhow::bail!("definition_command_required");
        };
        let (subject, routes, owner_kind) = match principal {
            Principal::Administrator {
                subject,
                routes,
            } => (subject, routes, OwnerKind::Api),
            Principal::Provisioner {
                subject,
                routes,
            } => (subject, routes, OwnerKind::Config),
            _ => anyhow::bail!("administrator_required"),
        };
        identifier(subject)?;
        identifier(operation_id)?;
        let id = &definition.route.route_id;
        anyhow::ensure!(routes.contains(id), "route_control_denied");
        let fingerprint = format!("{:x}", Sha256::digest(serde_json::to_vec(command)?));
        anyhow::ensure!(
            !self.ownership_actions.contains_key(operation_id)
                && !self.retirements.contains_key(operation_id),
            "operation_conflict"
        );
        if let Some(previous) = self.operations.get(operation_id) {
            anyhow::ensure!(
                previous.actor == *subject && previous.fingerprint == fingerprint,
                "operation_conflict"
            );
            return Ok(Reply::Operation {
                operation: previous.clone(),
            });
        }
        anyhow::ensure!(
            self.operations.len() + self.ownership_actions.len() + self.retirements.len()
                < limits.operations
                && (self.routes.contains_key(id) || self.routes.len() < limits.routes),
            "control_capacity"
        );
        anyhow::ensure!(
            *minimum_ready > 0 && *minimum_ready <= limits.nodes,
            "invalid_minimum_ready"
        );
        // 路由显式门槛属于冻结定义，已签名的命令也不能把它降为更小的上线数量。
        anyhow::ensure!(
            !definition.route.enabled
                || definition
                    .route
                    .activation
                    .min_ready_nodes
                    .is_none_or(|value| value == *minimum_ready),
            "route_minimum_ready_mismatch"
        );
        let compiled = CompiledRoute::compile_with_verifiers(
            definition.route.clone(),
            &definition.endpoints,
            &definition.verifiers,
        )?;
        anyhow::ensure!(
            compiled.digest == definition.digest
                && compiled.endpoints == definition.endpoints
                && compiled.verifiers == definition.verifiers,
            "definition_not_frozen"
        );
        let old = self.routes.get(id);
        anyhow::ensure!(
            old.map_or(0, |route| route.definition.route.revision) == *expected_revision
                && expected_revision.checked_add(1) == Some(definition.route.revision),
            "route_revision_conflict"
        );
        if let Some(old) = old {
            anyhow::ensure!(
                old.owner == *subject && old.owner_kind == owner_kind,
                "route_source_conflict"
            );
            anyhow::ensure!(
                !old.suspended || !definition.route.enabled,
                "route_suspended"
            );
            anyhow::ensure!(
                self.operations[&old.operation_id].phase == Phase::Effective
                    || !definition.route.enabled,
                "route_operation_pending"
            );
        }
        self.validate_paths(id, definition)?;
        let mut previous = old.map_or_else(Vec::new, |route| route.previous.clone());
        let owner_revision = old.map_or(1, |route| route.owner_revision);
        let suspended = old.is_some_and(|route| route.suspended);
        if let Some(old) = old {
            if !previous
                .iter()
                .any(|definition| definition.digest == old.definition.digest)
            {
                previous.push(old.definition.clone());
            }
            if self.operations[&old.operation_id].phase != Phase::Effective {
                // 紧急下线接管全部尚未结束的许可责任，原操作保留被替代事实而不假报生效。
                let pending = self
                    .operations
                    .get_mut(&old.operation_id)
                    .expect("pending operation");
                pending.phase = Phase::Superseded;
                pending.superseded_by = Some(operation_id.clone());
            }
        }
        let barrier = self
            .permits
            .iter()
            .filter(|(_, permit)| permit.binding.route_id == *id)
            .map(|(id, _)| id.clone())
            .collect::<BTreeSet<_>>();
        let binding = Binding::of(definition);
        let phase = if barrier.is_empty() {
            if definition.route.enabled {
                Phase::Preparing
            } else {
                Phase::Effective
            }
        } else {
            Phase::Quiescing
        };
        let operation = Operation {
            id: operation_id.clone(),
            actor: subject.clone(),
            fingerprint,
            binding,
            phase,
            targets: self
                .nodes
                .iter()
                .filter(|(_, node)| node.active && node.serving)
                .map(|(id, node)| node_key(id, node.incarnation))
                .collect(),
            prepared: BTreeSet::new(),
            applied: BTreeSet::new(),
            failures: BTreeMap::new(),
            barrier,
            minimum_ready: *minimum_ready,
            created_at: now,
            completed_at: (phase == Phase::Effective).then_some(now),
            superseded_by: None,
        };
        // 期望定义与旧许可屏障进入同一候选，提交后再也不能为旧 revision 续期。
        self.routes.insert(
            id.clone(),
            Route {
                definition: definition.clone(),
                previous,
                operation_id: operation_id.clone(),
                owner: subject.clone(),
                owner_kind,
                owner_revision,
                suspended,
            },
        );
        self.operations
            .insert(operation_id.clone(), operation.clone());
        Ok(Reply::Operation {
            operation,
        })
    }
}
