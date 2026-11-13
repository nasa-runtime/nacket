use super::*;
use crate::domain::session::SubscriptionBarrier;

/// 尚未返回的旧写出由真实许可持有，节点不会等待它们而阻塞其它路由的续期。
pub(super) struct Projection {
    /// 本地已安装的授权状态 revision，与路由定义 revision 分开。
    revision: u64,
    /// 该授权投影失效的旧写出集合，全部真实归还后才能报告应用完成。
    barriers: Vec<SubscriptionBarrier>,
}

impl NodeRuntime {
    /// 业务作用：在申请任何路由许可前恢复业务撤权下界，并只为真实完成的本地写出屏障提交应用回执。
    /// 参数说明：
    /// - `snapshot`：本轮已确认的完整控制账本。
    ///
    /// 返回：每条可确认路由的有限节点命令；旧写出未结束时不会续发授权资格。
    pub(super) async fn reconcile_authorizations(
        &self,
        snapshot: &Ledger,
    ) -> anyhow::Result<Vec<Command>> {
        let node = &snapshot.nodes[&self.gateway.configuration.node_id];
        let mut commands = Vec::new();
        for (route_id, state) in &snapshot.authorizations {
            let installed = {
                let projections = self
                    .authorizations
                    .lock()
                    .unwrap_or_else(|error| error.into_inner());
                match projections.get(route_id) {
                    // 原投影仍承担未归还的旧写出责任；覆盖投影会丢失屏障，必须延后本路由的新投影和应用回执。
                    Some(projection)
                        if projection
                            .barriers
                            .iter()
                            .any(|barrier| !barrier.is_complete()) =>
                    {
                        continue
                    }
                    Some(projection) => projection.revision,
                    None => 0,
                }
            };
            if installed != state.revision {
                // 先发布已持久确认的共享下界，再撤销受影响会话并捕获旧写出；安装下界本身不等于旧写出已结束。
                let barriers = self.gateway.apply_authorizations(route_id, state).await?;
                tracing::debug!(
                    node_id = %self.gateway.configuration.node_id,
                    route_id,
                    authorization_revision = state.revision,
                    pending_writes = barriers.iter().filter(|barrier| !barrier.is_complete()).count(),
                    stage = "projection_installed",
                    "socket authorization node progress"
                );
                // 全部共享下界已经发布后才登记本轮投影；取消或失败不能伪造应用完成。
                self.authorizations
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .insert(
                        route_id.clone(),
                        Projection {
                            revision: state.revision,
                            barriers,
                        },
                    );
            }
            // 投影必须仍对应本轮授权 revision，且旧写出全部真实归还，才能向权威声明该节点已经应用。
            let complete = self
                .authorizations
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .get(route_id)
                .is_some_and(|projection| {
                    projection.revision == state.revision
                        && projection
                            .barriers
                            .iter()
                            .all(SubscriptionBarrier::is_complete)
                });
            // 此处只生成待提交回执；续期阶段把应用回执排在许可申请之前，不能先续权再补报撤权完成。
            if complete && node.authorizations.get(route_id) != Some(&state.revision) {
                commands.push(Command::AuthorizationApplied {
                    route_id: route_id.clone(),
                    revision: state.revision,
                });
            }
        }
        Ok(commands)
    }
}
