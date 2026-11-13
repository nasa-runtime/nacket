use super::*;
use crate::domain::message::{envelope::identifier, UnixMillis};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;

impl Ledger {
    /// 业务作用：在精确实例全部旧许可排空后持久排除其操作责任，进程缺席不能单独构成排除证据。
    /// 参数说明：
    /// - `principal`：拥有全部受影响路由范围的管理员。
    /// - `command`：包含幂等操作 ID、稳定节点与精确 incarnation 的退出动作。
    /// - `now`：操作确认时间。
    /// - `limits`：控制历史容量。
    ///
    /// 返回：持久候选中的退出回执；仍有许可或权限不足时保持原目标集合。
    pub(super) fn retire(
        &mut self,
        principal: &Principal,
        command: &Command,
        now: UnixMillis,
        limits: &Limits,
    ) -> anyhow::Result<Reply> {
        let Principal::Administrator {
            subject,
            routes,
        } = principal
        else {
            anyhow::bail!("administrator_required");
        };
        let Command::Retire {
            operation_id,
            node_id,
            expected_incarnation,
        } = command
        else {
            anyhow::bail!("retirement_command_required");
        };
        identifier(subject)?;
        identifier(operation_id)?;
        identifier(node_id)?;
        let fingerprint = format!("{:x}", Sha256::digest(serde_json::to_vec(command)?));
        anyhow::ensure!(
            !self.operations.contains_key(operation_id)
                && !self.ownership_actions.contains_key(operation_id),
            "operation_conflict"
        );
        if let Some(action) = self.retirements.get(operation_id) {
            anyhow::ensure!(
                action.actor == *subject
                    && action.fingerprint == fingerprint
                    && action.routes.is_subset(routes),
                "operation_conflict"
            );
            return Ok(Reply::Retirement {
                action: action.clone(),
            });
        }
        anyhow::ensure!(
            self.operations.len() + self.ownership_actions.len() + self.retirements.len()
                < limits.operations,
            "control_capacity"
        );
        let node = self
            .nodes
            .get(node_id)
            .ok_or_else(|| anyhow::anyhow!("node_missing"))?;
        anyhow::ensure!(
            *expected_incarnation > 0
                && node
                    .startups
                    .values()
                    .any(|value| value == expected_incarnation),
            "node_incarnation_conflict"
        );
        let key = super::transition::node_key(node_id, *expected_incarnation);
        let affected = self
            .operations
            .values()
            .filter(|operation| {
                operation.targets.contains(&key)
                    || operation.prepared.contains(&key)
                    || operation.applied.contains(&key)
            })
            .map(|operation| operation.binding.route_id.clone())
            .collect::<BTreeSet<_>>();
        anyhow::ensure!(
            !affected.is_empty() && affected.is_subset(routes),
            "node_retirement_scope_denied"
        );
        anyhow::ensure!(
            !self
                .permits
                .values()
                .any(|permit| permit.node_id == *node_id
                    && permit.incarnation == *expected_incarnation),
            "node_permits_not_drained"
        );
        if node.incarnation == *expected_incarnation {
            // 当前实例先撤销申请资格；已被新 incarnation 围栏的旧实例不应关闭其继任者。
            self.nodes.get_mut(node_id).expect("known node").active = false;
        }
        for operation in self.operations.values_mut() {
            operation.targets.remove(&key);
            operation.prepared.remove(&key);
            operation.applied.remove(&key);
            operation.failures.remove(&key);
        }
        self.advance(now);
        let action = RetirementAction {
            id: operation_id.clone(),
            actor: subject.clone(),
            fingerprint,
            node_id: node_id.clone(),
            incarnation: expected_incarnation.to_string(),
            routes: affected,
            completed_at: now,
        };
        self.retirements
            .insert(operation_id.clone(), action.clone());
        Ok(Reply::Retirement {
            action,
        })
    }
}
