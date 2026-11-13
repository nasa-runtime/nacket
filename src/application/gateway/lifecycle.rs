//! 连接事实按精确会话有序发送；关闭权限不等待远端确认。

use super::{authority::CallContext, Connected, Gateway};
use crate::{
    adapters::outbound::{
        identity::now,
        publishing::{Guard, Outcome, Publication},
    },
    config::{lifecycle::LifecycleDelivery, publishing::Purpose},
    domain::{
        message::{control, UnixMillis},
        route::{compiled::CompiledRoute, policies::HookTarget},
    },
    observability::{Counter, Metrics, Operation, Reservation},
};
use bytes::Bytes;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::{
    sync::{Arc, Mutex, Weak},
    time::Duration,
};
use tokio::{
    sync::{mpsc, OwnedSemaphorePermit, Semaphore},
    task::JoinSet,
    time::Instant,
};
use tokio_util::sync::CancellationToken;

/// 状态只前进一次；初始化中关闭也产生可关联的下线事实。
#[derive(Default)]
pub(super) enum Phase {
    #[default]
    Initializing,
    Ready,
    Closed,
}

struct Fact {
    /// 连接事实首次创建的稳定 ID，发布重试不重新生成。
    event_id: String,
    /// 固定 online 或 offline 连接事实类别，不代表用户级全局在线状态。
    event: &'static str,
    /// 远程 HTTP 合同需要的有界事实请求，内容由服务端上下文生成。
    request: serde_json::Value,
    /// MQ 固定合同编码字节，重试保持相同事实内容。
    metadata: Bytes,
    /// 精确会话的 MQ 顺序键，重连产生不同会话序列。
    ordering_key: Vec<u8>,
    /// 生成事实时冻结的路由计划，发布不改用后续路由定义。
    plan: Arc<CompiledRoute>,
    /// 冻结的 HTTP 或 MQ 发布目标，事件内容不能选择任意出口。
    target: HookTarget,
    /// 本条事实发布的固定单调截止点，退避不刷新。
    deadline: Instant,
    /// 排队和发布过程持续保留的事实数量预约。
    _count: OwnedSemaphorePermit,
    /// 排队和发布过程持续保留的逻辑字节预约。
    _bytes: OwnedSemaphorePermit,
    /// 事实数量的实际生命周期观测责任。
    _observed_count: Reservation,
    /// 事实逻辑字节的实际生命周期观测责任。
    _observed_bytes: Reservation,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct Acknowledgement {
    /// 业务服务必须精确回显的原事实 ID。
    event_id: String,
    /// 固定合同要求的处理状态，HTTP 成功码不能替代事实确认。
    status: String,
}

/// 本节点的固定分片与额度，分片内顺序覆盖远程等待和重试。
pub struct Lifecycle {
    /// 固定数量工作分片的有界入口，同一精确会话落到同一分片。
    senders: Vec<mpsc::Sender<Fact>>,
    /// 是否仍允许登记新事实，与关闭队列的顺序同步。
    accepting: Mutex<bool>,
    /// 全节点尚未结束的连接事实数量预算。
    count: Arc<Semaphore>,
    /// 连接事实控制信息和编码的节点逻辑字节预算。
    bytes: Arc<Semaphore>,
    /// 启动时冻结的工作数、容量、重试及初始化关联策略。
    policy: LifecycleDelivery,
    /// 统一有限事实指标目录，不按会话生成动态标签。
    metrics: Arc<Metrics>,
}

impl Lifecycle {
    /// 业务作用：登记连接事实发送与关闭协调，让下线排空发生在生命周期 MQ worker 停止之前。
    /// 参数说明：
    /// - `application`：统一托管任务和全局退出期限的宿主。
    /// - `gateway`：只以弱引用交给 worker，避免资源目录形成所有权环。
    ///
    /// 返回：有界入口已建立；所有发送任务随应用启动和关闭，不产生独立后台运行时。
    pub async fn install(
        application: &nasa::Application,
        gateway: &Arc<Gateway>,
    ) -> anyhow::Result<Self> {
        let policy = gateway.configuration.lifecycle_delivery.clone();
        let mut senders = Vec::new();
        let mut receivers = Vec::new();
        for _ in 0..policy.workers {
            let (sender, receiver) = mpsc::channel(policy.max_pending);
            senders.push(sender);
            receivers.push(receiver);
        }
        let owner = Arc::downgrade(gateway);
        let worker_policy = policy.clone();
        let publisher_stop = gateway.lifecycle_publish_shutdown.clone();
        application
            .serve_when_ready("nacket-lifecycle", move |shutdown| async move {
                // 任意退出路径都解除 publisher 的等待，防止协调任务失败留下永久运行的发送者。
                let _publisher_stop = publisher_stop.drop_guard();
                let drain = CancellationToken::new();
                let mut workers = JoinSet::new();
                for receiver in receivers {
                    workers.spawn(Self::run_lane(
                        owner.clone(),
                        receiver,
                        drain.clone(),
                        worker_policy.clone(),
                    ));
                }
                tokio::select! {
                    biased;
                    _ = shutdown.cancelled() => {
                        if let Some(gateway) = owner.upgrade() {
                            // 先撤销全部会话并登记下线事实，再封闭事实入口；迟到 Ready 不得越过关闭状态。
                            gateway.disconnect_all().await;
                            if let Some(lifecycle) = gateway.lifecycle.get() {
                                *lifecycle
                                    .accepting
                                    .lock()
                                    .unwrap_or_else(|error| error.into_inner()) = false;
                            }
                        }
                        drain.cancel();
                        while let Some(result) = workers.join_next().await {
                            result??;
                        }
                        Ok::<(), anyhow::Error>(())
                    }
                    result = workers.join_next() => {
                        if let Some(result) = result {
                            result??;
                        }
                        anyhow::bail!("lifecycle_worker_stopped")
                    }
                }
            })
            .await?;
        Ok(Self {
            senders,
            accepting: Mutex::new(true),
            count: Arc::new(Semaphore::new(policy.max_pending)),
            bytes: Arc::new(Semaphore::new(policy.max_bytes)),
            policy,
            metrics: gateway.metrics.clone(),
        })
    }

    /// 业务作用：把服务器生成的连接事实转为固定目标事件；队列不足只损失通知，不保留权限。
    /// 参数说明：
    /// - `connection`：已冻结身份与定义的精确会话。
    /// - `event`：由状态机选择的 online 或 offline。
    /// - `was_ready`：此连接是否曾经发布 Ready，不能解释为用户级在线状态。
    ///
    /// 返回：事件已移交或明确拒绝；禁用回调不创建事件、不占用额度。
    pub(super) fn emit(&self, connection: &Connected, event: &'static str, was_ready: bool) {
        let hook = if event == "online" {
            &connection.plan.definition.lifecycle.online
        } else {
            &connection.plan.definition.lifecycle.offline
        };
        if !hook.enabled {
            return;
        }
        let result = (|| -> anyhow::Result<()> {
            let accepting = self
                .accepting
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            anyhow::ensure!(*accepting, "lifecycle_closed");
            let target = hook
                .target
                .clone()
                .ok_or_else(|| anyhow::anyhow!("lifecycle_target_missing"))?;
            let count = self.count.clone().try_acquire_owned()?;
            let event_id = control::nonce();
            let wall = now()?;
            let expires = UnixMillis::new(
                wall.as_millis()
                    .checked_add(hook.timeout_ms)
                    .ok_or_else(|| anyhow::anyhow!("lifecycle_time_overflow"))?,
            )
            .map_err(anyhow::Error::msg)?;
            let deadline = Instant::now() + Duration::from_millis(hook.timeout_ms);
            let identity = connection.session.identity();
            let request = serde_json::json!({
                "contract": "nacket.business/1",
                "kind": "lifecycle",
                "eventId": event_id,
                "requestId": event_id,
                "operation": event,
                "sequence": if event == "online" {
                    1
                } else {
                    2
                },
                "context": CallContext {
                    key: &connection.session.key,
                    attempt_id: &connection.attempt_id,
                },
                "principal": {
                    "uid": identity.uid,
                    "tenantId": identity.tenant,
                },
                "wasReady": was_ready,
                "createdAt": wall,
                "expiresAt": expires,
                "bodyPresent": false,
            });
            let metadata = Bytes::from(serde_json::to_vec(&request)?);
            anyhow::ensure!(
                metadata.len() <= self.policy.max_event_bytes,
                "lifecycle_event_capacity"
            );
            let size = metadata.len().saturating_mul(2).saturating_add(4096);
            let bytes = self.bytes.clone().try_acquire_many_owned(size as u32)?;
            let ordering_key = connection.session.key.route_session_id.as_bytes().to_vec();
            let hash = Sha256::digest(&ordering_key);
            let index = u32::from_be_bytes(hash[..4].try_into()?) as usize % self.senders.len();
            let fact = Fact {
                event_id,
                event,
                request,
                metadata,
                ordering_key,
                plan: connection.plan.clone(),
                target,
                deadline,
                _count: count,
                _bytes: bytes,
                _observed_count: self.metrics.reserve("lifecycle_items", 1),
                _observed_bytes: self.metrics.reserve("lifecycle_bytes", size as u64),
            };
            self.senders[index]
                .try_send(fact)
                .map_err(|_| anyhow::anyhow!("lifecycle_queue_unavailable"))?;
            Ok(())
        })();
        self.metrics.add(
            Counter::Lifecycle,
            event,
            if result.is_ok() {
                "accepted"
            } else {
                "rejected"
            },
            1,
        );
    }

    /// 业务作用：顺序发送同一分片的事实，关闭时处理已接纳项而不允许新的入队责任。
    /// 参数说明：
    /// - `owner`：产品运行域的弱引用。
    /// - `receiver`：本分片的有界事实入口。
    /// - `drain`：协调器已经登记全部下线后的关闭信号。
    /// - `policy`：固定重试预算。
    ///
    /// 返回：已接纳事实完成或期限耗尽；资源销毁不遗留脱管任务。
    async fn run_lane(
        owner: Weak<Gateway>,
        mut receiver: mpsc::Receiver<Fact>,
        drain: CancellationToken,
        policy: LifecycleDelivery,
    ) -> anyhow::Result<()> {
        let mut draining = false;
        loop {
            let next = tokio::select! {
                biased;
                _ = drain.cancelled(), if !draining => {
                    receiver.close();
                    draining = true;
                    continue;
                }
                next = receiver.recv() => next,
            };
            let Some(fact) = next else {
                return Ok(());
            };
            let gateway = owner
                .upgrade()
                .ok_or_else(|| anyhow::anyhow!("lifecycle_owner_missing"))?;
            let mut observed = Operation::new(
                gateway.metrics.clone(),
                Counter::Lifecycle,
                fact.event,
                "lifecycle_delivery",
            );
            let mut outcome = Outcome::NotDispatched;
            let mut uncertain = false;
            for attempt in 0..policy.max_attempts {
                if Instant::now() >= fact.deadline {
                    break;
                }
                outcome = Self::deliver(&gateway, &fact).await;
                uncertain |= outcome == Outcome::Unknown;
                if outcome == Outcome::Accepted || attempt + 1 == policy.max_attempts {
                    break;
                }
                // 重试沿用事件 ID 与原始截止点；未知副作用要求业务端按 eventId 幂等受理。
                let backoff = Instant::now() + Duration::from_millis(policy.retry_backoff_ms);
                if backoff >= fact.deadline {
                    break;
                }
                tokio::time::sleep_until(backoff).await;
            }
            observed.finish(match outcome {
                Outcome::Accepted => "committed",
                Outcome::NotDispatched if uncertain => "unknown",
                Outcome::NotDispatched if Instant::now() >= fact.deadline => "expired",
                Outcome::NotDispatched => "rejected",
                Outcome::Unknown => "unknown",
            });
        }
    }

    /// 业务作用：调用固定 HTTP 或生命周期发布器，响应只能确认事实，不能携带新权限或投递指令。
    /// 参数说明：
    /// - `gateway`：提供已受管远程入口和有限 MQ 权限的运行域。
    /// - `fact`：不可变事实和首次创建时确定的期限。
    ///
    /// 返回：受信目标明确确认、确定未发送或保守未知结局。
    async fn deliver(gateway: &Gateway, fact: &Fact) -> Outcome {
        match &fact.target {
            HookTarget::Http {
                endpoint_ref,
            } => {
                let Some(endpoint) = fact.plan.endpoints.get(endpoint_ref) else {
                    return Outcome::NotDispatched;
                };
                let result = gateway
                    .calls
                    .call_observed::<_, Acknowledgement>(
                        endpoint_ref,
                        endpoint,
                        &fact.request,
                        fact.deadline,
                    )
                    .await;
                match result {
                    Ok(response)
                        if response.event_id == fact.event_id
                            && matches!(response.status.as_str(), "accepted" | "duplicate") =>
                    {
                        Outcome::Accepted
                    }
                    Err(error) if !error.dispatched => Outcome::NotDispatched,
                    _ => Outcome::Unknown,
                }
            }
            HookTarget::Mq {
                publisher_ref,
            } => {
                let Some(publishers) = gateway.publishers.get() else {
                    return Outcome::NotDispatched;
                };
                publishers
                    .publish(
                        publisher_ref,
                        Purpose::Lifecycle,
                        &fact.plan.definition.route_id,
                        Publication {
                            metadata: fact.metadata.clone(),
                            body: Bytes::new(),
                            ordering_key: fact.ordering_key.clone(),
                            deadline: fact.deadline,
                            guard: Guard::Lifecycle,
                        },
                    )
                    .await
            }
        }
    }
}

impl Gateway {
    /// 业务作用：在产品配置应用完成后一次性切换两个日志开关，已有会话读取同一新策略。
    /// 参数说明：
    /// - `policy`：已经完成校验与应用的产品日志策略。
    ///
    /// 返回：后续事件采用新开关；调用方不得在候选被拒绝时发布。
    pub(crate) fn configure_connection_logging(
        &self,
        policy: crate::config::connection_logging::ConnectionLogging,
    ) {
        self.connection_logging.store(
            u8::from(policy.connected) | (u8::from(policy.disconnected) << 1),
            std::sync::atomic::Ordering::Release,
        );
    }

    /// 业务作用：在连接 Ready 顺序已建立后发布上线事实，和关闭状态共用同一互斥边界。
    /// 参数说明：
    /// - `connection`：已排定 Ready 回复的精确会话。
    ///
    /// 返回：有效连接首次进入可投递目录并按配置登记上线；已关闭连接不能重新 Ready。
    pub async fn publish_ready(
        self: &Arc<Self>,
        connection: &Arc<Connected>,
    ) -> anyhow::Result<()> {
        connection.handshake_attempt.stage("ready_publish");
        let gateway = self.clone();
        let owner = connection.clone();
        let result = async {
            connection
                .uid_events
                .submit_control(connection.session.identity().deadline, 65536, move || {
                    gateway.publish_ready_consumed(&owner)
                })?
                .wait()
                .await
        }
        .await;
        if let Err(error) = &result {
            connection.handshake_attempt.reject(error);
        } else {
            connection.handshake_attempt.ready();
        }
        result
    }

    /// 业务作用：在主体消费中发布 Ready 与上线事实，关闭或撤权后的迟到结果不能接流。
    /// 参数说明：
    /// - `connection`：当前严格顺序域中的精确会话。
    ///
    /// 返回：首次有效 Ready 已发布并按开关记录连接成功；失权或重复发布拒绝。
    fn publish_ready_consumed(&self, connection: &Connected) -> anyhow::Result<()> {
        let mut phase = connection
            .lifecycle_phase
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        anyhow::ensure!(
            !self.closing.load(std::sync::atomic::Ordering::Acquire)
                && matches!(*phase, Phase::Initializing)
                && connection.session.live()
                && !connection.cancellation.is_cancelled(),
            "session_closed"
        );
        *phase = Phase::Ready;
        connection.publish_ready();
        // 只有身份、定义确认和初始化均已通过才记录成功；日志开关不改变业务回调。
        if self
            .connection_logging
            .load(std::sync::atomic::Ordering::Acquire)
            & 1
            != 0
        {
            let identity = connection.session.identity();
            let key = &connection.session.key;
            tracing::info!(
                event = "client_connected",
                cluster_id = ?key.cluster_id,
                scope_epoch = %key.scope_epoch,
                node_id = ?key.node_id,
                incarnation = %key.incarnation,
                route_id = ?key.route_id,
                route_revision = %key.route_revision,
                session_id = %key.route_session_id,
                tenant_id = ?identity.tenant,
                uid = ?identity.uid,
                transport = connection.transport,
                peer_ip = ?connection.peer,
                serialization = ?connection.plan.definition.serialization,
                ready_after_ms = connection.session.duration_ms(),
                "客户端连接成功"
            );
        }
        if let Some(lifecycle) = self.lifecycle.get() {
            lifecycle.emit(connection, "online", true);
        }
        Ok(())
    }

    /// 业务作用：在停机时清空会话目录并复用唯一断开路径，随后事实协调器才关闭入口。
    /// 参数说明: 无。
    ///
    /// 返回：全部既有会话失去资格，下线通知已经有界入队或记录拒绝。
    pub async fn disconnect_all(self: &Arc<Self>) {
        self.closing
            .store(true, std::sync::atomic::Ordering::Release);
        let ids = self
            .sessions
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        for id in ids {
            self.disconnect(&id);
        }
        // 只等待节点固定事件预算；执行域失效不能无限拖延事实队列和宿主关闭。
        let deadline = Instant::now()
            + Duration::from_millis(self.configuration.transport.uid_events.timeout_ms);
        loop {
            let changed = self.closed_sessions.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self
                .sessions
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .is_empty()
            {
                break;
            }
            if tokio::time::timeout_at(deadline, changed).await.is_err() {
                break;
            }
        }
    }

    /// 业务作用：先永久撤销本连接的权限与待决责任，再登记一次下线事实。
    /// 参数说明：
    /// - `connection`：已经从可查询目录移除的连接。
    ///
    /// 返回：首次关闭按开关记录断连；重复关闭无副作用，远端通知失败不会保留本地资格。
    pub(super) fn close_connection(&self, connection: &Connected) {
        let mut phase = connection
            .lifecycle_phase
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if matches!(*phase, Phase::Closed) {
            return;
        }
        let was_ready = matches!(*phase, Phase::Ready);
        *phase = Phase::Closed;
        // 权限、写出准入与待决操作先撤销，通知只描述事实，不承担会话关闭权威。
        connection.session.close();
        connection.receipts.cancel(None, None);
        connection.cancellation.cancel();
        connection.requests.clear();
        connection
            .observed
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take();
        self.log_disconnected(connection, was_ready, "uid_partition");
        if let Some(lifecycle) = self.lifecycle.get() {
            lifecycle.emit(connection, "offline", was_ready);
        }
    }
    /// 业务作用：在权限与订阅撤销后记录单会话断连，日志不依赖远端下线通知是否启用。
    /// 参数说明：
    /// - `connection`：已关闭并完成本地订阅清理的精确会话。
    /// - `was_ready`：关闭前是否曾完成 Ready；初始化失败不能被解释为成功上线。
    /// - `cleanup_path`：固定的分区消费或本地清理路径；后者不会绕过分区发布业务下线通知。
    ///
    /// 返回：开关允许时向 nalog 输出 INFO 事件；调用方须持有生命周期锁且仅在首次关闭时调用。
    fn log_disconnected(
        &self,
        connection: &Connected,
        was_ready: bool,
        cleanup_path: &'static str,
    ) {
        if self
            .connection_logging
            .load(std::sync::atomic::Ordering::Acquire)
            & 2
            == 0
        {
            return;
        }
        let Some(closed) = connection.session.closure() else {
            return;
        };
        let identity = connection.session.identity();
        let key = &connection.session.key;
        tracing::info!(
            event = "client_disconnected",
            cluster_id = ?key.cluster_id,
            scope_epoch = %key.scope_epoch,
            node_id = ?key.node_id,
            incarnation = %key.incarnation,
            route_id = ?key.route_id,
            route_revision = %key.route_revision,
            session_id = %key.route_session_id,
            tenant_id = ?identity.tenant,
            uid = ?identity.uid,
            transport = connection.transport,
            peer_ip = ?connection.peer,
            serialization = ?connection.plan.definition.serialization,
            was_ready,
            cleanup_path,
            session_duration_ms = connection.session.duration_ms(),
            cancelled_subscriptions = closed.cancelled_subscriptions,
            pending_subscriptions = closed.pending_subscriptions,
            reason = closed.reason,
            "客户端断开连接，已取消全部订阅"
        );
    }
}

/// 终态事件同时拥有队列内与未消费路径的回收责任，不依赖网络任务继续存在。
pub(super) struct Closing {
    /// 持有目录和有限生命周期入口直到本次清理结束。
    gateway: Arc<Gateway>,
    /// 只回收精确会话，不触及相同 uid 的其它连接。
    connection: Arc<Connected>,
    /// 只有在主体分区消费后才允许产生下线事实。
    consumed: bool,
}

impl Closing {
    /// 业务作用：固定一次终态责任，队列拒绝也必须释放该会话的目录与待决资源。
    /// 参数说明：
    /// - `gateway`：拥有会话目录的产品运行域。
    /// - `connection`：已经收紧权限且只登记一次关闭的精确会话。
    ///
    /// 返回：尚未发布下线事实的回收 owner。
    pub(super) fn new(gateway: Arc<Gateway>, connection: Arc<Connected>) -> Self {
        Self {
            gateway,
            connection,
            consumed: false,
        }
    }

    /// 业务作用：在同一主体的严格消费位置登记下线事实，然后由唯一 owner 结束目录责任。
    /// 参数说明: 无。
    ///
    /// 返回：关闭状态已发布；重复事实仍由生命周期状态机拒绝。
    pub(super) fn consume(mut self) {
        self.gateway.close_connection(&self.connection);
        self.consumed = true;
    }
}

impl Drop for Closing {
    /// 业务作用：回收已消费或被执行域拒绝的终态责任，失权路径不能转移到网络线程发布业务事实。
    /// 参数说明: 无。
    ///
    /// 返回：目录、关联及指标责任释放并记录本地断连；未消费的远端事实只记录拒绝，不伪称已发送。
    fn drop(&mut self) {
        if !self.consumed {
            let mut phase = self
                .connection
                .lifecycle_phase
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            let newly_closed = !matches!(*phase, Phase::Closed);
            let was_ready = matches!(*phase, Phase::Ready);
            *phase = Phase::Closed;
            self.connection.session.close();
            self.connection.receipts.cancel(None, None);
            self.connection.cancellation.cancel();
            self.connection.requests.clear();
            self.connection
                .observed
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .take();
            // 执行域拒绝仍已撤销本地资格，应记录实际断开；已消费过的关闭不能重复记录。
            if newly_closed {
                self.gateway
                    .log_disconnected(&self.connection, was_ready, "local_fallback");
            }
            if self.connection.plan.definition.lifecycle.offline.enabled {
                self.connection
                    .metrics
                    .add(Counter::Lifecycle, "offline", "rejected", 1);
            }
        }
        self.gateway
            .sessions
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .remove(&self.connection.session.key.route_session_id);
        self.gateway.closed_sessions.notify_waiters();
    }
}
