use super::Gateway;
use crate::domain::{
    authorization::{Action, State, MAXIMUM_RECORDS},
    session::{Revocation, SubscriptionBarrier},
};

impl Gateway {
    /// 业务作用：把已持久确认的完整撤权状态投影到当前路由共享权限域，再撤销受影响会话的实际资格。
    /// 参数说明：
    /// - `route_id`：已验证控制快照中的路由键，跨定义更新保持同一撤权域。
    /// - `state`：完整且通过恢复校验的持久授权下界。
    ///
    /// 返回：需要等待的旧写出责任；完成这些屏障后才能上报节点应用回执。
    pub async fn apply_authorizations(
        &self,
        route_id: &str,
        state: &State,
    ) -> anyhow::Result<Vec<SubscriptionBarrier>> {
        let revocations = self.revocations(route_id);
        for action in state.records.values() {
            let command = match action {
                Action::RevokeSubject {
                    tenant_id,
                    uid,
                    ..
                } => Revocation::Subject {
                    tenant: tenant_id.clone(),
                    uid: uid.clone(),
                    version: action.version()?,
                },
                Action::RevokeSubscription {
                    tenant_id,
                    uid,
                    group_id,
                    ..
                } => Revocation::Membership {
                    tenant: tenant_id.clone(),
                    uid: uid.clone(),
                    group: group_id.clone(),
                    version: action.version()?,
                },
                Action::RemoveGroup {
                    tenant_id,
                    group_id,
                    ..
                } => Revocation::Group {
                    tenant: tenant_id.clone(),
                    group: group_id.clone(),
                    version: action.version()?,
                },
                Action::Kick {
                    ..
                } => Revocation::Session {
                    key: action.key()?,
                },
            };
            // 共享下界先于目录清理发布；并发新连接、迟到授权回复和最终 writer 都会复验同一事实。
            revocations.apply(command, MAXIMUM_RECORDS)?;
        }
        let sessions = self
            .sessions
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .values()
            .filter(|connection| connection.session.key.route_id == route_id)
            .cloned()
            .collect::<Vec<_>>();
        let mut barriers = Vec::new();
        for chunk in sessions.chunks(256) {
            for connection in chunk {
                let owner = connection.clone();
                let pending = connection.uid_events.submit_control(
                    tokio::time::Instant::now() + std::time::Duration::from_secs(30),
                    65536,
                    move || {
                        let barrier = owner.session.apply_revocations();
                        if !owner.session.live() {
                            owner.cancellation.cancel();
                        }
                        Ok(barrier)
                    },
                );
                let result = match pending {
                    Ok(pending) => pending.wait().await,
                    Err(error) => Err(error),
                };
                let barrier = match result {
                    Ok(barrier) => barrier,
                    Err(_) => {
                        // 权限下界已生效；失去消费能力时只能关闭并捕获旧写出，不能跳过屏障上报完成。
                        connection.session.close();
                        connection.cancellation.cancel();
                        connection.session.apply_revocations()
                    }
                };
                if !barrier.is_complete() {
                    barriers.push(barrier);
                }
            }
            tokio::task::yield_now().await;
        }
        Ok(barriers)
    }
}
