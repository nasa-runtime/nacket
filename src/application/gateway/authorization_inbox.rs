use super::{
    dispatch::{Deferred, TrustedIngress},
    Gateway,
};
use crate::{
    application::{control::Authority, routes::RouteService},
    config::messaging::IngressPurpose,
    domain::{
        authorization::Command,
        control::clock::LeaseTime,
        control::{Command as ControlCommand, Reply},
        route::dependency::Dependency,
    },
    observability::{Metrics, Reservation},
};
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::{
    sync::{mpsc, Notify},
    task::JoinSet,
    time::Instant,
};

const MAXIMUM_ITEMS: usize = 128;
const MAXIMUM_BYTES: usize = 2 * 1024 * 1024;

enum Status {
    Pending,
    Retry(
        /// 允许下次提交的单调时刻，原进度保护期限继续生效。
        Instant,
    ),
    Completed(
        /// 持久受理结果或确定拒绝原因，未知提交不能进入成功终态。
        Result<(), &'static str>,
        /// 完成处理时记录的单调时刻；维护时据此回收超过三十秒的结果缓存。
        /// 回收本地缓存不删除持久操作身份或授权下界。
        Instant,
    ),
}
struct Entry {
    /// 唤醒等待持久结果的消费调用；通知本身不授予 broker 确认权。
    changed: Arc<Notify>,
    /// 业务命令的稳定关联 ID，仅用于日志，不进入指标标签。
    command_id: String,
    /// 命令限定的路由，关联入口与持久投影的处理阶段。
    route_id: String,
    /// 本节点首次交接的单调时刻，重试不重置等待观测。
    received: Instant,
    /// 当前命令的提交次数，用于限制重复失败日志。
    attempts: u32,
    /// 固定进度期限耗尽后只记录一次保护原因。
    stalled_reported: bool,
    /// 原命令的固定授权控制入口，用于维护对应执行健康证据。
    ingress: String,
    /// 首次接纳时固定的执行进度期限，重试和 broker 连通都不能刷新。
    progress: LeaseTime,
    /// 完整授权命令的稳定摘要，拒绝同作用域同 ID 异内容。
    fingerprint: String,
    /// 实际提交、等待重试或有限完成缓存的当前状态。
    status: Status,
    /// 该记录保守占用的逻辑字节数，参与节点交接容量核对。
    bytes: usize,
    /// 未释放授权交接记录的数量观测责任。
    _count: Reservation,
    /// 未释放授权交接记录的字节观测责任。
    _bytes: Reservation,
}
struct Protection {
    /// 授权实际执行进度的独立门禁证据，与 MQ 读取健康分开。
    dependency: Arc<Dependency>,
    /// 入口无法承担新命令的事实，立即保护依赖路由。
    saturated: AtomicBool,
}
struct Work {
    /// 绑定入口、集群、作用域、路由和命令 ID 的精确交接键。
    key: String,
    /// 权威需再次核对的固定授权控制入口。
    ingress: String,
    /// 来源认证后确认的业务生产者，不能采信命令自报身份。
    producer: String,
    /// 严格校验后的授权控制动作，仍需持久权威受理。
    command: Command,
}

/// 同步 broker 回调只预约有限工作；权威持久确认前一直保持未确认消费状态。
pub struct Inbox {
    /// 固定节点标识，区分同一命令在集群各节点的消费责任。
    node_id: String,
    /// 受管控制提交任务的有界入口，满容量不能丢弃或确认源记录。
    sender: mpsc::Sender<Work>,
    /// 仍承担未知结果责任或有限完成缓存的命令目录。
    entries: Mutex<BTreeMap<String, Entry>>,
    /// 节点统一授权控制指标源，不使用命令 ID 标签。
    metrics: Arc<Metrics>,
    /// 启动时固定的入口执行保护目录，不能由消息动态增加来源。
    protection: BTreeMap<String, Protection>,
}

impl Inbox {
    /// 业务作用：通过有限工作目录衔接 broker 消费与受管撤权提交，保持持久确认前的输入责任。
    /// 参数说明：
    /// - `application`：唯一任务和关闭责任宿主。
    /// - `authority`：已经建立的持久控制入口。
    /// - `routes`：冻结部署来源及其业务门禁。
    /// - `metrics`：节点固定事实源。
    ///
    /// 返回：有界交接入口；停机后不再接受新命令，未确认输入保留 broker 恢复责任。
    pub async fn install(
        application: &nasa::Application,
        authority: Arc<Authority>,
        routes: Arc<RouteService>,
        metrics: Arc<Metrics>,
    ) -> anyhow::Result<Arc<Self>> {
        let (sender, mut receiver) = mpsc::channel::<Work>(MAXIMUM_ITEMS);
        let protection = routes
            .configuration()
            .ingresses
            .iter()
            .filter(|(_, ingress)| {
                ingress.enabled && ingress.purpose == IngressPurpose::AuthorizationControl
            })
            .map(|(id, _)| {
                Ok((
                    id.clone(),
                    Protection {
                        dependency: routes.authorization_progress(id)?,
                        saturated: AtomicBool::new(false),
                    },
                ))
            })
            .collect::<anyhow::Result<BTreeMap<_, _>>>()?;
        let inbox = Arc::new(Self {
            node_id: routes.configuration().node_id.clone(),
            sender,
            entries: Mutex::new(BTreeMap::new()),
            metrics,
            protection,
        });
        let owner = inbox.clone();
        application
            .serve_when_ready("socket-authorization-inbox", move |shutdown| async move {
                let mut tasks = JoinSet::new();
                let mut timer = tokio::time::interval(Duration::from_secs(1));
                timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                loop {
                    tokio::select! {
                        biased;
                        _ = shutdown.cancelled() => {
                            // 先关闭交接入口，再收回等待任务；没有持久成功证据的 broker 输入不能确认。
                            receiver.close();
                            for entry in owner.entries.lock().unwrap_or_else(|error| error.into_inner()).values() {
                                entry.changed.notify_waiters();
                            }
                            for protection in owner.protection.values() {
                                protection.dependency.deny("authorization_stopped");
                            }
                            tasks.abort_all();
                            while tasks.join_next().await.is_some() {}
                            return Ok::<(), anyhow::Error>(());
                        }
                        _ = timer.tick() => owner.maintain(),
                        result = tasks.join_next(), if !tasks.is_empty() => {
                            if let Some(Err(error)) = result {
                                return Err(error.into());
                            }
                        }
                        work = receiver.recv(), if tasks.len() < 8 => {
                            let Some(work) = work else {
                                return Ok(());
                            };
                            let owner = owner.clone();
                            let authority = authority.clone();
                            tasks.spawn(async move {
                                let result = authority
                                    .node(ControlCommand::ApplyAuthorization {
                                        ingress: work.ingress,
                                        producer: work.producer,
                                        command: work.command,
                                    })
                                    .await;
                                owner.finish(&work.key, result);
                            });
                        }
                    }
                }
            })
            .await?;
        Ok(inbox)
    }

    /// 业务作用：把已验签命令交给持久权威，重复消费只查询同一内容的原工作。
    /// 参数说明：
    /// - `ingress`：当前记录的固定来源入口。
    /// - `producer`：来源认证后取得的业务服务身份。
    /// - `command`：严格解析的授权控制元数据，正文必须为空。
    ///
    /// 返回：权威已持久接受时通过；待决、容量不足或提交未知时要求 broker 保留输入。
    pub fn accept(
        &self,
        ingress: &TrustedIngress,
        producer: &str,
        command: Command,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(
            ingress.configuration.purpose == IngressPurpose::AuthorizationControl
                && command.producer == producer,
            "authorization_source_mismatch"
        );
        command.validate()?;
        let protection = self
            .protection
            .get(&ingress.id)
            .ok_or(Deferred("authorization_executor_unavailable"))?;
        let key = serde_json::to_string(&(
            &ingress.id,
            &command.cluster_id,
            &command.scope_epoch,
            &command.route_id,
            &command.command_id,
        ))?;
        let fingerprint = command.fingerprint()?;
        let mut entries = self
            .entries
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if let Some(entry) = entries.get(&key) {
            anyhow::ensure!(
                entry.fingerprint == fingerprint,
                "authorization_command_conflict"
            );
            match entry.status {
                Status::Completed(Ok(()), _) => return Ok(()),
                Status::Completed(Err(code), _) => anyhow::bail!(code),
                Status::Retry(next) if next <= Instant::now() => {}
                _ => return Err(Deferred("authorization_pending").into()),
            }
        } else {
            let bytes = serde_json::to_vec(&command)?.len() * 2 + key.len() + 1024;
            if entries.len() >= MAXIMUM_ITEMS
                || bytes
                    > MAXIMUM_BYTES
                        .saturating_sub(entries.values().map(|entry| entry.bytes).sum::<usize>())
            {
                // 入口无法承担新撤权责任时立即保护所有依赖路由，不能以 broker 连通掩盖执行容量不足。
                protection.saturated.store(true, Ordering::Release);
                protection.dependency.deny("authorization_capacity");
                return Err(Deferred("authorization_capacity").into());
            }
            entries.insert(
                key.clone(),
                Entry {
                    changed: Arc::new(Notify::new()),
                    command_id: command.command_id.clone(),
                    route_id: command.route_id.clone(),
                    received: Instant::now(),
                    attempts: 0,
                    stalled_reported: false,
                    ingress: ingress.id.clone(),
                    progress: LeaseTime::start(Duration::from_millis(
                        ingress.configuration.dependency_stale_ms,
                    ))?,
                    fingerprint,
                    status: Status::Pending,
                    bytes,
                    _count: self.metrics.reserve("authorization_commands", 1),
                    _bytes: self.metrics.reserve("authorization_bytes", bytes as u64),
                },
            );
        }
        let entry = entries.get_mut(&key).expect("reserved command");
        entry.status = Status::Pending;
        entry.attempts = entry.attempts.saturating_add(1);
        if entry.attempts == 1 {
            tracing::debug!(
                node_id = %self.node_id,
                ingress = %entry.ingress,
                route_id = %entry.route_id,
                command_id = %entry.command_id,
                stage = "received",
                "socket authorization command progress"
            );
        }
        protection.saturated.store(false, Ordering::Release);
        if self
            .sender
            .try_send(Work {
                key: key.clone(),
                ingress: ingress.id.clone(),
                producer: producer.into(),
                command,
            })
            .is_err()
        {
            entries.get_mut(&key).expect("reserved command").status =
                Status::Retry(Instant::now() + Duration::from_millis(250));
        }
        drop(entries);
        self.maintain();
        Err(Deferred("authorization_pending").into())
    }

    /// 业务作用：让异步消费调用等待同一撤权命令的持久结果，不把正常交接等待报告为消费失败。
    /// 参数说明：
    /// - `ingress`：经过来源校验的固定授权控制入口。
    /// - `producer`：来源认证取得的生产者身份。
    /// - `command`：沿用原始 ID、内容和有效期的撤权命令。
    ///
    /// 返回：只有权威持久受理才成功；确定拒绝、容量不足、原进度期限耗尽或停止时返回错误。
    /// 等待取消不撤销已提交工作，后续消费查询同一责任；等待和重试均不刷新原进度期限。
    pub async fn accept_wait(
        &self,
        ingress: &TrustedIngress,
        producer: &str,
        command: Command,
    ) -> anyhow::Result<()> {
        let key = serde_json::to_string(&(
            &ingress.id,
            &command.cluster_id,
            &command.scope_epoch,
            &command.route_id,
            &command.command_id,
        ))?;
        loop {
            // 停止交接后不能继续重提未知动作，也不能把仍在收尾的工作当成消费完成。
            if self.sender.is_closed() {
                return Err(Deferred("authorization_stopped").into());
            }
            let stalled = self
                .entries
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .get(&key)
                .is_some_and(|entry| {
                    !matches!(entry.status, Status::Completed(_, _))
                        && entry.progress.deadline().is_none()
                });
            if stalled {
                // 先保护路由再查询原动作；进度到期不丢弃未知结果，精确重投仍沿用原退避与命令有效期。
                self.maintain();
            }
            match self.accept(ingress, producer, command.clone()) {
                Err(error)
                    if error
                        .downcast_ref::<Deferred>()
                        .is_some_and(|error| error.0 == "authorization_pending") => {}
                result => return result,
            }
            let changed = self
                .entries
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .get(&key)
                .ok_or(Deferred("authorization_stopped"))?
                .changed
                .clone();
            let notified = changed.notified();
            tokio::pin!(notified);
            // 先登记通知再读结果；提交可能在两次读取之间完成，不能因此永久错过完成或停止。
            notified.as_mut().enable();
            let next = {
                let entries = self
                    .entries
                    .lock()
                    .unwrap_or_else(|error| error.into_inner());
                let entry = entries.get(&key).ok_or(Deferred("authorization_stopped"))?;
                if self.sender.is_closed() {
                    return Err(Deferred("authorization_stopped").into());
                }
                match entry.status {
                    Status::Completed(_, _) => continue,
                    Status::Pending => entry.progress.deadline(),
                    Status::Retry(next) => entry.progress.deadline().map(|end| next.min(end)),
                }
            };
            let Some(next) = next else {
                // 原进度期限耗尽时先保护依赖路由，源记录仍未确认，不能靠重复等待延长可用证据。
                self.maintain();
                return Err(Deferred("authorization_stalled").into());
            };
            tokio::select! {
                _ = notified => {}
                _ = tokio::time::sleep_until(next) => {}
            }
        }
    }

    /// 业务作用：只把确定的持久受理或明确拒绝交还给消费回调，未知提交不进入隔离队列。
    /// 参数说明：
    /// - `key`：原始有界工作键。
    /// - `result`：控制权威返回的持久回复或调用失败。
    ///
    /// 返回：更新同一工作状态；可重试失败沿用原始 commandId 与全部内容。
    fn finish(&self, key: &str, result: anyhow::Result<Reply>) {
        let (status, reason) = match result {
            Ok(Reply::Authorization {
                ..
            }) => (Status::Completed(Ok(()), Instant::now()), "persisted"),
            Err(error) => {
                let code = match error.to_string().as_str() {
                    "authorization_scope_mismatch" => Some("authorization_scope_mismatch"),
                    "authorization_source_denied" => Some("authorization_source_denied"),
                    "authorization_definition_stale" => Some("authorization_definition_stale"),
                    "authorization_command_conflict" => Some("authorization_command_conflict"),
                    "authorization_command_expired" => Some("authorization_command_expired"),
                    "route_missing" => Some("authorization_route_missing"),
                    _ => None,
                };
                match code {
                    Some(code) => (Status::Completed(Err(code), Instant::now()), code),
                    None => (
                        Status::Retry(Instant::now() + Duration::from_millis(500)),
                        if error.to_string() == "authorization_command_not_yet_valid" {
                            "authorization_command_not_yet_valid"
                        } else {
                            "control_operation_unknown"
                        },
                    ),
                }
            }
            _ => (
                Status::Retry(Instant::now() + Duration::from_millis(500)),
                "control_reply_unexpected",
            ),
        };
        if let Some(entry) = self
            .entries
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .get_mut(key)
        {
            if matches!(status, Status::Completed(_, _)) {
                entry._count.finish_pending();
                let stage = if matches!(status, Status::Completed(Ok(()), _)) {
                    "persisted"
                } else {
                    "rejected"
                };
                tracing::info!(
                    node_id = %self.node_id,
                    ingress = %entry.ingress,
                    route_id = %entry.route_id,
                    command_id = %entry.command_id,
                    stage,
                    reason,
                    attempt = entry.attempts,
                    elapsed_ms = entry.received.elapsed().as_millis() as u64,
                    "socket authorization command progress"
                );
            } else if entry.attempts <= 3 || entry.attempts.is_power_of_two() {
                tracing::warn!(
                    node_id = %self.node_id,
                    ingress = %entry.ingress,
                    route_id = %entry.route_id,
                    command_id = %entry.command_id,
                    stage = "deferred",
                    reason,
                    attempt = entry.attempts,
                    elapsed_ms = entry.received.elapsed().as_millis() as u64,
                    "socket authorization command progress"
                );
            }
            entry.status = status;
            entry.changed.notify_waiters();
        }
    }

    /// 业务作用：核对撤权提交的固定期限并维护执行门禁，完成缓存回收不删除持久操作身份。
    /// 参数说明: 无。
    ///
    /// 返回：停滞入口拒绝业务；新证据最多保留三秒，且不超过任何未完成工作的原始进度期限。
    fn maintain(&self) {
        let mut entries = self
            .entries
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        entries.retain(|_, entry| !matches!(entry.status, Status::Completed(_, at) if at.elapsed() > Duration::from_secs(30)));
        for (id, protection) in &self.protection {
            if protection.saturated.load(Ordering::Acquire) {
                protection.dependency.deny("authorization_capacity");
                continue;
            }
            // 维护窗口从扫描前开始；后续只收窄原证据，扫描或发布前的调度停顿同样消耗预算。
            let Ok(mut evidence) = LeaseTime::start(Duration::from_secs(3)) else {
                // 无法取得有限时钟证据时保持保护，不能以旧进度或 broker 连通代替。
                protection.dependency.deny("authorization_stalled");
                continue;
            };
            for entry in entries.values_mut().filter(|entry| {
                entry.ingress == *id && !matches!(entry.status, Status::Completed(_, _))
            }) {
                evidence = evidence.intersect(entry.progress);
                if entry.progress.deadline().is_none() && !entry.stalled_reported {
                    tracing::warn!(
                        node_id = %self.node_id,
                        ingress = %entry.ingress,
                        route_id = %entry.route_id,
                        command_id = %entry.command_id,
                        stage = "protected",
                        reason = "authorization_stalled",
                        attempt = entry.attempts,
                        elapsed_ms = entry.received.elapsed().as_millis() as u64,
                        "socket authorization command progress"
                    );
                    entry.stalled_reported = true;
                }
            }
            // 发布与业务准入都复验同一份固定证据，重试、MQ 心跳和迟到的维护不能延长撤权期限。
            if evidence.deadline().is_some() {
                protection.dependency.confirm(evidence);
            } else {
                protection.dependency.deny("authorization_stalled");
            }
        }
    }
}

impl Gateway {
    /// 业务作用：将受信 MQ 授权命令交给已装配的专用持久入口，不提供本地内存降级。
    /// 参数说明：
    /// - `ingress`：实际消息的固定来源。
    /// - `producer`：由 broker ACL 或签名获得的身份。
    /// - `command`：完整命令，包含原始期限与单值目标。
    ///
    /// 返回：已持久受理时完成消费；入口不可用或提交未知保持未确认。
    pub fn authorization_command(
        &self,
        ingress: &TrustedIngress,
        producer: &str,
        command: Command,
    ) -> anyhow::Result<()> {
        self.authorization_inbox
            .get()
            .ok_or(Deferred("authorization_unavailable"))?
            .accept(ingress, producer, command)
    }
}
