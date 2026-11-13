use super::*;
use crate::domain::{
    message::{envelope::identifier, UnixMillis},
    route::compiled::CompiledRoute,
};
use sha2::{Digest, Sha256};

impl Ledger {
    /// 业务作用：在同一持久候选内执行紧急下线、显式解除暂停和来源转移，配置方不能替自己解除管理暂停。
    /// 参数说明：
    /// - `principal`：已经由管理入口验证的管理员及路由范围。
    /// - `command`：包含操作 ID、路由 CAS 和归属 CAS 的明确管理动作。
    /// - `now`：操作完成记录使用的整数毫秒时间。
    /// - `limits`：统一控制容量合同。
    ///
    /// 返回：待提交的操作回复；权限不足、代次不符或旧写出未结束时不改变已提交状态。
    pub(super) fn ownership(
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
        let (id, route_id, expected) = match command {
            Command::Offline {
                operation_id,
                route_id,
                expected_revision,
            }
            | Command::Resume {
                operation_id,
                route_id,
                expected_revision,
                ..
            }
            | Command::Transfer {
                operation_id,
                route_id,
                expected_revision,
                ..
            } => (operation_id, route_id, expected_revision),
            _ => anyhow::bail!("ownership_command_required"),
        };
        identifier(subject)?;
        identifier(id)?;
        identifier(route_id)?;
        anyhow::ensure!(routes.contains(route_id), "route_control_denied");
        let fingerprint = format!("{:x}", Sha256::digest(serde_json::to_vec(command)?));
        anyhow::ensure!(!self.retirements.contains_key(id), "operation_conflict");
        if let Some(operation) = self.operations.get(id) {
            anyhow::ensure!(
                operation.actor == *subject
                    && operation.fingerprint == fingerprint
                    && matches!(command, Command::Offline { .. }),
                "operation_conflict"
            );
            return Ok(Reply::Operation {
                operation: operation.clone(),
            });
        }
        if let Some(action) = self.ownership_actions.get(id) {
            anyhow::ensure!(
                action.actor == *subject && action.fingerprint == fingerprint,
                "operation_conflict"
            );
            return Ok(Reply::Ownership {
                action: action.clone(),
            });
        }
        anyhow::ensure!(
            self.operations.len() + self.ownership_actions.len() + self.retirements.len()
                < limits.operations,
            "control_capacity"
        );
        let current = self
            .routes
            .get(route_id)
            .ok_or_else(|| anyhow::anyhow!("route_missing"))?;
        anyhow::ensure!(
            current.definition.route.revision == *expected,
            "route_revision_conflict"
        );
        let next_owner_revision = current
            .owner_revision
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("owner_revision_exhausted"))?;
        if matches!(command, Command::Offline { .. }) {
            let mut definition = current.definition.clone();
            definition.route.enabled = false;
            definition.route.revision = expected
                .checked_add(1)
                .ok_or_else(|| anyhow::anyhow!("route_revision_exhausted"))?;
            definition.digest = CompiledRoute::compile_with_verifiers(
                definition.route.clone(),
                &definition.endpoints,
                &definition.verifiers,
            )?
            .digest
            .clone();
            let source = match current.owner_kind {
                OwnerKind::Api => Principal::Administrator {
                    subject: current.owner.clone(),
                    routes: routes.clone(),
                },
                OwnerKind::Config => Principal::Provisioner {
                    subject: current.owner.clone(),
                    routes: routes.clone(),
                },
            };
            // 下线沿用完整旧许可屏障和映射责任，只在该候选内授权暂停，不把来源转移当作关闭证明。
            self.define(
                &source,
                &Command::Define {
                    operation_id: id.clone(),
                    expected_revision: *expected,
                    definition,
                    minimum_ready: 1,
                },
                now,
                limits,
            )?;
            let route = self
                .routes
                .get_mut(route_id)
                .expect("committed candidate route");
            route.suspended = true;
            route.owner_revision = next_owner_revision;
            let operation = self.operations.get_mut(id).expect("offline operation");
            operation.actor = subject.clone();
            operation.fingerprint = fingerprint;
            return Ok(Reply::Operation {
                operation: operation.clone(),
            });
        }
        anyhow::ensure!(
            !current.definition.route.enabled
                && self.operations[&current.operation_id].phase == Phase::Effective,
            "ownership_requires_effective_offline"
        );
        let current = self.routes.get_mut(route_id).expect("current route");
        match command {
            Command::Resume {
                expected_owner_revision,
                ..
            } => {
                anyhow::ensure!(
                    current.owner_revision == *expected_owner_revision,
                    "owner_revision_conflict"
                );
                // 解除暂停只允许后续新定义提案，当前 Offline 状态和旧 revision 不会因此重新开放。
                current.suspended = false;
            }
            Command::Transfer {
                expected_owner_revision,
                owner,
                owner_kind,
                ..
            } => {
                identifier(owner)?;
                anyhow::ensure!(
                    current.owner_revision == *expected_owner_revision,
                    "owner_revision_conflict"
                );
                current.owner = owner.clone();
                current.owner_kind = *owner_kind;
            }
            _ => anyhow::bail!("ownership_command_required"),
        }
        current.owner_revision = next_owner_revision;
        let action = OwnershipAction {
            id: id.clone(),
            actor: subject.clone(),
            fingerprint,
            route_id: route_id.clone(),
            owner_revision: next_owner_revision,
            completed_at: now,
        };
        self.ownership_actions.insert(id.clone(), action.clone());
        Ok(Reply::Ownership {
            action,
        })
    }
}
