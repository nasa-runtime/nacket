//! 主动刷新采用节点有界队列，远程等待不占用 UID 分区，授权结果在原分区复验。

use super::{
    authority::{AuthorizationResponse, CallContext},
    Connected, Gateway,
};
use crate::{adapters::outbound::identity::now, domain::message::control};
use sha2::{Digest, Sha256};
use std::{sync::Arc, time::Duration};
use tokio::{sync::mpsc, task::JoinSet, time::Instant};

/// 会话已有维护循环驱动的单个刷新意图，不创建逐连接计时任务。
pub(super) struct Schedule {
    /// 同一会话同时最多一个排队或执行中的自动刷新。
    in_flight: bool,
    /// 上次尝试确定的最早下次时刻，容量不足也不能紧密重试。
    next_attempt: Instant,
    /// 会话稳定的抖动种子，不从用户输入选择 HTTP 目标。
    jitter_seed: u64,
}

impl Schedule {
    /// 业务作用：为新会话分配固定抖动及空的刷新责任。
    /// 参数说明：
    /// - `session_id`：服务端生成的会话随机标识。
    ///
    /// 返回：没有在途请求、允许首次按到期窗口预约的状态。
    pub(super) fn new(session_id: &str) -> Self {
        let hash = Sha256::digest(session_id.as_bytes());
        Self {
            in_flight: false,
            next_attempt: Instant::now(),
            jitter_seed: u64::from_be_bytes(hash[..8].try_into().expect("digest prefix")),
        }
    }
}

/// 实际排队责任，关闭、取消和任务退出均释放会话合并标志。
struct Work {
    /// 本次预约的精确会话，不能在执行时改查同 UID 的新连接。
    connection: Arc<Connected>,
    /// 从预约时固定的总截止点，排队不增加网络预算。
    deadline: Instant,
    /// 当前等待或执行责任的指标，随真实 owner 释放。
    observed: crate::observability::Reservation,
}

impl Drop for Work {
    /// 业务作用：释放本次合并责任，保留预约时设定的退避下界。
    /// 参数说明: 无。
    ///
    /// 返回：后续维护可以按原截止点重新判断，不恢复已失效的授权。
    fn drop(&mut self) {
        self.connection
            .refresh_schedule
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .in_flight = false;
    }
}

/// 全节点自动刷新入口，队列与执行数量都受部署硬上限约束。
pub struct Refreshes {
    /// Ready 会话向受管执行器提交的有限责任队列。
    sender: mpsc::Sender<Work>,
}

impl Refreshes {
    /// 业务作用：在 napp 中登记固定刷新执行器，停机优先取消尚未获得新授权的责任。
    /// 参数说明：
    /// - `application`：唯一任务生命周期宿主。
    /// - `gateway`：提供固定路由、HTTP 预算和会话门禁的节点实例。
    ///
    /// 返回：有界入口；工作任务随应用退出完成取消和回收。
    pub async fn install(
        application: &nasa::Application,
        gateway: Arc<Gateway>,
    ) -> anyhow::Result<Arc<Self>> {
        let policy = &gateway.configuration.remote_calls;
        let parallel = policy.max_refresh_in_flight.min(policy.max_in_flight);
        let (sender, mut receiver) = mpsc::channel::<Work>(policy.max_refresh_pending);
        application
            .serve_when_ready("socket-authorization-refresh", move |shutdown| async move {
                let mut tasks = JoinSet::new();
                loop {
                    tokio::select! {
                        biased;
                        _ = shutdown.cancelled() => {
                            // 先关闭预约入口再取消执行，迟到 HTTP 允许不能在停机后重建会话权限。
                            receiver.close();
                            tasks.abort_all();
                            while tasks.join_next().await.is_some() {}
                            while receiver.try_recv().is_ok() {}
                            return Ok::<(), anyhow::Error>(());
                        }
                        _ = tasks.join_next(), if !tasks.is_empty() => {},
                        next = receiver.recv(), if tasks.len() < parallel => {
                            let Some(mut work) = next else {
                                return Ok(());
                            };
                            let gateway = gateway.clone();
                            let stop = shutdown.clone();
                            tasks.spawn(async move {
                                work.observed.transfer("authorization_refresh_inflight");
                                tokio::select! {
                                    biased;
                                    _ = stop.cancelled() => {},
                                    _ = work.connection.cancellation.cancelled() => {},
                                    _ = tokio::time::timeout_at(
                                        work.deadline,
                                        gateway.automatic_refresh(&work.connection, work.deadline),
                                    ) => {},
                                }
                            });
                        }
                    }
                }
            })
            .await?;
        Ok(Arc::new(Self {
            sender,
        }))
    }

    /// 业务作用：按现有维护节拍预约临期会话，同会话合并且不等待队列容量。
    /// 参数说明：
    /// - `connection`：已经完成 Ready 的精确会话。
    ///
    /// 返回：未到期窗口、已有责任或队列饱和时不发请求，授权原期限始终不变。
    pub fn schedule(&self, connection: &Arc<Connected>) {
        let policy = &connection.plan.definition.authorization.refresh;
        if !policy.enabled
            || !connection
                .wire_ready
                .load(std::sync::atomic::Ordering::Acquire)
            || connection.cancellation.is_cancelled()
            || connection.session.candidate(None).is_none()
        {
            return;
        }
        let identity = connection.session.identity();
        // 不能续期的原凭据已成为期限上界时，不向业务接口反复申请无效的延长。
        if connection
            .credential_deadline
            .is_some_and(|limit| identity.deadline >= limit)
        {
            return;
        }
        let current = Instant::now();
        let mut state = connection
            .refresh_schedule
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let jitter = state.jitter_seed % (policy.schedule.jitter_ms + 1);
        if state.in_flight
            || current < state.next_attempt
            || identity.deadline.saturating_duration_since(current)
                > Duration::from_millis(policy.schedule.advance_ms + jitter)
        {
            return;
        }
        state.next_attempt = current + Duration::from_millis(policy.schedule.retry_ms);
        state.in_flight = true;
        drop(state);
        let work = Work {
            connection: connection.clone(),
            deadline: identity
                .deadline
                .min(current + Duration::from_millis(policy.schedule.timeout_ms)),
            observed: connection
                .metrics
                .reserve("authorization_refresh_pending", 1),
        };
        if self.sender.try_send(work).is_err() {
            connection.metrics.add(
                crate::observability::Counter::Authorization,
                "refresh",
                "unavailable",
                1,
            );
        }
    }
}

impl Gateway {
    /// 业务作用：在旧授权仍有效时取得新的业务证据，自动刷新不伪造客户端通知或业务操作。
    /// 参数说明：
    /// - `connection`：原会话与冻结路由计划。
    /// - `deadline`：从预约起固定的完整预算。
    ///
    /// 返回：新证据在 UID 分区复验后生效；失败不延长旧期限。
    async fn automatic_refresh(
        &self,
        connection: &Arc<Connected>,
        deadline: Instant,
    ) -> anyhow::Result<()> {
        self.metrics
            .measure(
                crate::observability::Counter::Authorization,
                "refresh",
                "authorization",
                "allow",
                async {
                    let _serial = connection
                        .refresh
                        .try_lock()
                        .map_err(|_| anyhow::anyhow!("refresh_in_progress"))?;
                    anyhow::ensure!(
                        connection.session.candidate(None).is_some()
                            && !connection.cancellation.is_cancelled(),
                        "session_closed"
                    );
                    let policy = &connection.plan.definition.authorization.refresh;
                    let reference = policy
                        .endpoint_ref
                        .as_deref()
                        .ok_or_else(|| anyhow::anyhow!("authorization_unavailable"))?;
                    let endpoint = connection
                        .plan
                        .endpoints
                        .get(reference)
                        .ok_or_else(|| anyhow::anyhow!("authorization_unavailable"))?;
                    let identity = connection.session.identity();
                    let request_id = control::nonce();
                    let started = Instant::now();
                    let wall = now()?;
                    let request = serde_json::json!({
                        "requestId": request_id,
                        "operation": "refresh",
                        "operationName": "authorization.refresh",
                        "context": CallContext {
                            key: &connection.session.key,
                            attempt_id: &connection.attempt_id
                        },
                        "principal": {
                            "tenantId": identity.tenant,
                            "uid": identity.uid
                        },
                        "authorizationId": identity.authorization_id,
                        "authorizationVersion": identity.version.to_string()
                    });
                    let response: AuthorizationResponse = self
                        .calls
                        .call(
                            reference,
                            endpoint,
                            &connection.facts.input(&request),
                            deadline.min(identity.deadline),
                        )
                        .await?;
                    let evidence = response.evidence(&request_id, endpoint, started, wall)?;
                    self.apply_refresh(connection, response, evidence, deadline)
                        .await
                },
            )
            .await
    }

    /// 业务作用：将显式或自动刷新取得的证据交回原主体分区，统一执行凭据和撤权门禁。
    /// 参数说明：
    /// - `connection`：不能被刷新替换身份的精确会话。
    /// - `response`：已通过响应关联与期限检查的业务结果。
    /// - `evidence`：授权 ID、单调版本和保守截止点。
    /// - `operation_deadline`：本次刷新完整执行预算。
    ///
    /// 返回：新权限已消费；取消、过期、换主体或撤权均拒绝迟到允许。
    pub(super) async fn apply_refresh(
        &self,
        connection: &Arc<Connected>,
        mut response: AuthorizationResponse,
        evidence: (String, u64, Instant),
        operation_deadline: Instant,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(response.principal.is_none(), "authorization_scope_changed");
        let (id, version, deadline) = evidence;
        let deadline = connection
            .credential_deadline
            .map_or(deadline, |limit| limit.min(deadline));
        response.grants.bind(
            connection.plan.definition.subscriptions.max_per_session,
            deadline,
        )?;
        let owner = connection.clone();
        connection
            .uid_events
            .submit_control(operation_deadline, 65536, move || {
                // 控制权和撤权在消费点重新核对，旧请求不能因 HTTP 成功而跨越关闭或凭据期限。
                anyhow::ensure!(!owner.cancellation.is_cancelled(), "session_closed");
                owner.session.refresh(id, version, deadline)?;
                *owner
                    .grants
                    .lock()
                    .unwrap_or_else(|error| error.into_inner()) = response.grants;
                owner.sync_receiving();
                Ok(())
            })?
            .wait()
            .await
    }
}
