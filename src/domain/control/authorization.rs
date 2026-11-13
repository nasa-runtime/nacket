use super::{Command, Ledger, Principal, Reply};
use crate::domain::{authorization::MAXIMUM_RECORDS, message::UnixMillis};

impl Ledger {
    /// 业务作用：经当前服务节点转交业务撤权，在权威定义中再次核对范围并持久保存写出屏障。
    /// 参数说明：
    /// - `principal`：受信节点请求身份，不能由业务元数据构造。
    /// - `request`：已完成 MQ 来源验证的动作或节点本地应用回执。
    /// - `now`：持久动作诊断时间。
    ///
    /// 返回：待持久提交的受理状态；节点应用成功不等于其它节点已经完成。
    pub(super) fn authorization(
        &mut self,
        principal: &Principal,
        request: &Command,
        now: UnixMillis,
    ) -> anyhow::Result<Reply> {
        let (node_id, incarnation) = self.current_node(principal)?;
        match request {
            Command::ApplyAuthorization {
                ingress,
                producer,
                command,
            } => {
                command.validate()?;
                anyhow::ensure!(
                    command.cluster_id == self.cluster_id
                        && command.scope_epoch == self.scope_epoch
                        && command.producer == *producer,
                    "authorization_scope_mismatch"
                );
                let route = self
                    .routes
                    .get(&command.route_id)
                    .ok_or_else(|| anyhow::anyhow!("route_missing"))?;
                anyhow::ensure!(
                    route.definition.route.authorization.control.allows(
                        ingress,
                        producer,
                        &command.action
                    ),
                    "authorization_source_denied"
                );
                if let Some(operation) = self
                    .authorizations
                    .get(&command.route_id)
                    .and_then(|state| state.operations.get(&command.command_id))
                {
                    anyhow::ensure!(
                        operation.fingerprint == command.fingerprint()?,
                        "authorization_command_conflict"
                    );
                    return Ok(Reply::Authorization {
                        operation: operation.clone(),
                    });
                }
                // 新动作必须匹配当前定义；已受理动作的精确重试只查询原结果，不再产生撤权副作用。
                anyhow::ensure!(
                    route.definition.route.revision.to_string() == command.route_revision
                        && route.definition.digest == command.definition_digest,
                    "authorization_definition_stale"
                );
                let count = self
                    .authorizations
                    .values()
                    .map(|state| state.operations.len())
                    .sum::<usize>();
                anyhow::ensure!(count < MAXIMUM_RECORDS, "authorization_operation_capacity");
                let barrier = self
                    .permits
                    .values()
                    .filter(|permit| permit.binding.route_id == command.route_id)
                    .map(|permit| permit.id.clone())
                    .collect();
                let operation = self
                    .authorizations
                    .entry(command.route_id.clone())
                    .or_default()
                    .accept(command, barrier, now)?;
                Ok(Reply::Authorization {
                    operation,
                })
            }
            Command::AuthorizationApplied {
                route_id,
                revision,
            } => {
                let state = self
                    .authorizations
                    .get_mut(route_id)
                    .ok_or_else(|| anyhow::anyhow!("authorization_state_missing"))?;
                anyhow::ensure!(
                    *revision == state.revision,
                    "authorization_projection_stale"
                );
                // 仅精确节点代际可以确认自己旧写出的结束，不能为失联节点清除屏障。
                for operation in state.operations.values_mut() {
                    operation.barrier.retain(|id| {
                        self.permits.get(id).is_some_and(|permit| {
                            permit.node_id != node_id || permit.incarnation != incarnation
                        })
                    });
                    if operation.barrier.is_empty() {
                        operation.completed_at.get_or_insert(now);
                    }
                }
                self.nodes
                    .get_mut(&node_id)
                    .expect("current node")
                    .authorizations
                    .insert(route_id.clone(), *revision);
                Ok(Reply::Recorded {})
            }
            _ => anyhow::bail!("authorization_command_invalid"),
        }
    }

    /// 业务作用：在精确排空或许可失效持久确认后清除同一许可的授权屏障责任。
    /// 参数说明：
    /// - `permit_id`：已经在同一候选中回收的实际许可。
    /// - `now`：完成记录的诊断时间。
    ///
    /// 返回：最后一个旧许可消失时记录完成，不凭墙钟提前清除其它许可。
    pub(super) fn authorization_drained(&mut self, permit_id: &str, now: UnixMillis) {
        for state in self.authorizations.values_mut() {
            for operation in state.operations.values_mut() {
                operation.barrier.remove(permit_id);
                if operation.barrier.is_empty() {
                    operation.completed_at.get_or_insert(now);
                }
            }
        }
    }
}
