//! 接入会话、可信身份、操作执行与下行投递共用的产品运行时。

/// 校验客户端动作并执行订阅或受限业务调用，结果仅回复原请求者。
pub mod actions;
mod assertions;
pub(crate) mod authority;
pub mod authorization_cache;
/// 有界接纳受信撤权记录，持久提交后才推进消费完成证据。
pub mod authorization_inbox;
pub mod dispatch;
pub mod handshake;
pub mod initialization;
pub mod lifecycle;
pub mod outbox;
mod publishing;
/// 将同步 MQ 回调交给有限异步工作槽，保留原消息的完成责任。
pub mod push_inbox;
pub mod queue;
/// 维护连接名额及替换排空责任，未知申请结果不能当作名额已释放。
pub mod quotas;
pub mod refresh;
pub mod requests;
mod revocations;

use super::routes::RouteService;
use crate::{
    adapters::outbound::http::RemoteCalls,
    config::Deployment,
    domain::{
        route::compiled::CompiledRoute,
        session::{receipts::Receipts, Revocations, Session},
    },
};
pub use authority::{Handshake, LegacyPath, PendingSession};
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicBool, AtomicU8, Ordering},
        Arc, Mutex, OnceLock,
    },
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_util::sync::CancellationToken;

/// 已认证路由会话的唯一运行记录，物理传输关闭后立即撤销全部资格。
pub struct Connected {
    /// 同一握手跨传输与主体消费保留的失败观察，Ready 后不承担连接关闭权威。
    pub(crate) handshake_attempt: Arc<handshake::attempt::Attempt>,
    /// 仅用于本精确会话的只读订阅判定缓存，关闭或重连不能继承资格。
    pub authorization_cache: authorization_cache::Cache,
    /// 当前路由批准且受节点内存预算约束的只读握手上下文。
    pub facts: handshake::Facts,
    /// 建立连接时的真实网络对端 IP，仅用于日志，不从客户端转发头覆盖。
    peer: String,
    /// 接入层确认的传输类型，Socket.IO namespace 各自记录逻辑会话。
    transport: &'static str,
    /// 已验证主体的固定保序入口，跨会话与路由仍使用同一租户和用户键。
    pub uid_events: super::uid_events::Lane,
    /// 终态清理最多登记一次，重复网络出口不能放大主体队列。
    disconnect_queued: AtomicBool,
    /// 当前精确逻辑会话及其授权门禁，不随物理连接上的其它 namespace 共享身份。
    pub session: Arc<Session>,
    /// 握手时冻结的路由编译计划，配置更新不能悄然改变旧会话协议。
    pub plan: Arc<CompiledRoute>,
    /// 该会话的有界终端接收回执目录，操作 ACK 不能代替终端回执。
    pub receipts: Arc<Receipts>,
    /// 该会话的有限业务发送入口，排队不代表实际投递。
    pub outbox: outbox::Outbox,
    /// 会话关闭信号，接收、处理和写出任务共同观察。
    pub cancellation: CancellationToken,
    /// 会话内允许同时执行的客户端操作数量上限。
    pub operations: Arc<Semaphore>,
    /// 取消订阅的独立在途名额，慢远程操作不能借用它。
    pub control_operations: Arc<Semaphore>,
    /// 最近受信授权响应提供的有限能力快照，使用时仍核对截止点和撤权下界。
    pub grants: Mutex<authority::Grants>,
    /// 串行化同一会话的身份刷新，迟到响应不能覆盖后续状态。
    pub refresh: tokio::sync::Mutex<()>,
    /// 主动刷新唯一意图和退避，不为每个会话创建新的定时任务。
    refresh_schedule: Mutex<refresh::Schedule>,
    /// 节点受管自动刷新入口，弱引用不会延长服务关闭生命周期。
    automatic_refresh: std::sync::Weak<refresh::Refreshes>,
    /// 原凭据的单调截止点，远程刷新不能突破凭据自身有效期。
    pub credential_deadline: Option<tokio::time::Instant>,
    /// 建立本会话的精确握手尝试 ID，重连后的迟到消息不能混入。
    pub attempt_id: String,
    /// Ready 已经实际完整写出的事实，业务处理不能只凭初始化完成放行。
    pub wire_ready: AtomicBool,
    /// 节点共享的固定会话指标源。
    pub metrics: Arc<crate::observability::Metrics>,
    /// 会话内请求唯一执行和有限结果复用目录。
    pub requests: Arc<requests::RequestTable>,
    /// 初始化、Ready 和关闭事实的单向状态，避免重复发布连接事件。
    lifecycle_phase: Mutex<lifecycle::Phase>,
    /// 当前会话阶段的观测责任，随初始化到 Ready 的真实迁移转移。
    observed: Mutex<Option<crate::observability::Reservation>>,
    /// 节点或集群连接名额的实际责任，关闭仍需等待旧 writer 排空。
    quota: quotas::Admission,
    /// 节点逻辑会话容量许可，实际连接 owner 释放时归还。
    _capacity: OwnedSemaphorePermit,
}

/// 产品协调入口；HTTP、MQ 与监听器的生命周期仍由 napp 统一管理。
pub struct Gateway {
    /// 全节点只读授权缓存的条目和字节硬上限。
    pub authorization_cache_budget: Arc<authorization_cache::Budget>,
    /// 自动授权刷新有界工作入口，在接入开放前固定。
    pub refreshes: OnceLock<Arc<refresh::Refreshes>>,
    /// 当前已完整应用的日志策略；最低位控制成功日志，次低位控制断连日志。
    connection_logging: AtomicU8,
    /// 本节点可信主体事件的唯一预算目录，与全部传输和业务路由共享。
    pub uid_event_budget: Arc<super::uid_events::Budget>,
    /// 独立 broker 只读观察及固定采样容量，不参与业务消费和路由门禁。
    pub broker_metrics: Arc<crate::observability::broker::Broker>,
    /// 全部业务监听共享的物理连接和真实来源准入控制。
    pub admissions: Arc<super::admission::Admissions>,
    /// Kafka 同步回调向异步分发交接的有限目录，安装后不可替换。
    pub push_inbox: OnceLock<Arc<push_inbox::Inbox>>,
    /// MQ 原始正文跨异步阶段保留的共享字节预算。
    pub ingress_bytes: Arc<nasa::ws::transport::budget::Budget>,
    /// 节点并行推进业务分发的名额上限。
    pub fanouts: Arc<Semaphore>,
    /// 活跃分发保留候选会话引用的总容量，枚举之前预约。
    pub candidate_references: Arc<Semaphore>,
    /// 业务、初始化回复和授权控制三个独立 Redis 读前预算池。
    pub ingress_reads: [Arc<Semaphore>; 3],
    /// 无效记录持久隔离时保留原文的独立字节预算。
    pub isolation_bytes: Arc<Semaphore>,
    /// 组件、任务及退出责任的唯一宿主，不创建产品私有运行时。
    application: nasa::Application,
    /// 当前路由定义、依赖和控制门禁目录。
    pub routes: Arc<RouteService>,
    /// 进程启动时冻结的节点资源策略，路由热更通过 routes 提交。
    pub configuration: Deployment,
    /// 控制权威确认的本次节点代际，建立后不可覆盖。
    pub incarnation: OnceLock<String>,
    /// 当前进程启动唯一标识，也用于主监听绑定身份。
    pub startup_id: String,
    /// 启用集群时安装的持久控制入口，本地模式不伪造集群权威。
    pub control: OnceLock<Arc<crate::application::control::Authority>>,
    /// 受信授权撤销命令的有界持久交接入口。
    pub authorization_inbox: OnceLock<Arc<authorization_inbox::Inbox>>,
    /// 全部远程 HTTP 调用共用的客户端与并发、字节预算。
    pub calls: Arc<RemoteCalls>,
    /// Redis 所有业务入口共用的未完成记录和连续完成证据容量。
    pub redis_pending: Arc<Semaphore>,
    /// 启动时建立的固定 MQ 发布目标目录，消息不能动态选择任意 broker。
    pub publishers: OnceLock<crate::adapters::outbound::publishing::Publishers>,
    /// 连接事实的有限发布队列和固定工作分片。
    pub lifecycle: OnceLock<lifecycle::Lifecycle>,
    /// MQ 初始化请求到精确会话的有限关联目录。
    pub initializations: Arc<initialization::Initializations>,
    /// 在退出截止点终止连接事实发布的信号，不延长宿主总预算。
    pub lifecycle_publish_shutdown: CancellationToken,
    /// 节点已开始关闭的永久事实，禁止迟到初始化重新接流。
    closing: AtomicBool,
    /// 会话清理移出目录后的通知，停机排空不创建逐连接等待任务。
    closed_sessions: tokio::sync::Notify,
    /// 本节点受管 JWT 验签策略和有限 JWKS 缓存。
    pub verifiers: crate::adapters::outbound::jwt::Verifiers,
    /// 已登记逻辑会话目录，初始化中的会话仍不能作为可投递目标。
    pub sessions: Mutex<BTreeMap<String, Arc<Connected>>>,
    /// 按路由共享的持久授权下界投影，所有会话复验同一撤权事实。
    revocations: Mutex<BTreeMap<String, Arc<Revocations>>>,
    /// 所有适配器共享的逻辑会话硬上限。
    capacity: Arc<Semaphore>,
    /// 跨会话客户端操作的节点并发预算。
    pub operation_capacity: Arc<Semaphore>,
    /// 节点取消操作的独立并发预算，完成后才归还。
    pub control_operation_capacity: Arc<Semaphore>,
    /// 业务消息从排队到实际写出结束的节点逻辑字节预算。
    pub outbound_bytes: Arc<Semaphore>,
    /// 握手、Ready 和状态通知使用的独立控制输出预算。
    pub control_output: Arc<crate::domain::message::control_output::Budget>,
    /// 原生帧及 Socket.IO 附件接收的共享保留预算，最后 Bytes owner 释放后归还。
    pub inbound_bytes: Arc<nasa::ws::transport::budget::Budget>,
    /// 全部会话共用的请求关联记录数量预算。
    pub request_records: Arc<Semaphore>,
    /// 全部会话请求记录与结果缓存的节点字节预算。
    pub request_cache_bytes: Arc<Semaphore>,
    /// 有限窗口内的业务消息去重记录，不代表持久业务幂等账本。
    pub deduplication: Arc<crate::domain::message::deduplication::Window>,
    /// 本节点统一产品指标源，实例标签由宿主绑定。
    pub metrics: Arc<crate::observability::Metrics>,
    /// 最近持久控制确认的有限健康证据，不以连接存活延长许可。
    pub control_health: Arc<crate::domain::route::dependency::Dependency>,
    /// 集群连接名额申请、归还及未知结果的本地责任目录。
    pub quotas: quotas::Quotas,
    /// 节点作用域的连接名额和替换排空责任目录。
    pub(crate) local_quotas: quotas::local::Local,
}

impl Gateway {
    /// 业务作用：在传输升级前确认本节点可以接收新认证，避免把恢复中的依赖失效变成未知认证结果。
    /// 参数说明: 无。
    ///
    /// 返回：宿主就绪且尚未关闭时允许升级；认证阶段仍独立复验路由及身份权限。
    pub fn accepts_connections(&self) -> bool {
        self.application.is_ready() && !self.closing.load(Ordering::Acquire)
    }

    /// 业务作用：建立节点运行域和有限资源预算，不另建网络运行时或发现客户端。
    /// 参数说明：
    /// - `application`：提供受管 HTTP 与发现能力的宿主。
    /// - `routes`：已恢复的持久路由权威。
    /// - `configuration`：已经完整验证的部署快照。
    ///
    /// 返回：尚未接入连接的运行域；每次进程启动生成新的连接隔离材料。
    pub fn new(
        application: nasa::Application,
        routes: Arc<RouteService>,
        configuration: Deployment,
    ) -> Arc<Self> {
        let metrics = crate::observability::Metrics::configured(&configuration.metric_scopes);
        metrics.set(
            "request_record_capacity",
            configuration.transport.max_request_records as u64,
        );
        metrics.set(
            "request_cache_byte_capacity",
            configuration.transport.max_request_cache_bytes as u64,
        );
        metrics.set(
            "session_capacity",
            configuration.transport.max_sessions as u64,
        );
        metrics.set(
            "operation_capacity",
            configuration.transport.max_pending_operations as u64,
        );
        metrics.set(
            "outbound_byte_capacity",
            configuration.transport.max_outbound_bytes as u64,
        );
        metrics.set(
            "inbound_byte_capacity",
            configuration.transport.max_inbound_bytes as u64,
        );
        metrics.set(
            "physical_connection_capacity",
            configuration.transport.admission.max_physical_connections as u64,
        );
        metrics.set(
            "unauthenticated_connection_capacity",
            configuration.transport.admission.max_unauthenticated as u64,
        );
        metrics.set(
            "started_timestamp_seconds",
            application
                .info()
                .started_at()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis()
                .min(u64::MAX as u128) as u64,
        );
        Arc::new(Self {
            redis_pending: Arc::new(Semaphore::new(
                configuration.ingress_processing.max_pending_records,
            )),
            authorization_cache_budget: authorization_cache::Budget::new(
                &configuration.remote_calls,
                metrics.clone(),
            ),
            refreshes: OnceLock::new(),
            connection_logging: AtomicU8::new(
                u8::from(configuration.connection_logging.connected)
                    | (u8::from(configuration.connection_logging.disconnected) << 1),
            ),
            uid_event_budget: super::uid_events::Budget::new(
                configuration.transport.uid_events.clone(),
                metrics.clone(),
            )
            .expect("validated uid event budget"),
            control_output: crate::domain::message::control_output::Budget::new(
                configuration.transport.max_control_outbound_bytes,
                metrics.clone(),
            ),
            push_inbox: OnceLock::new(),
            ingress_reads: [
                Arc::new(Semaphore::new(
                    configuration.ingress_processing.max_read_bytes,
                )),
                Arc::new(Semaphore::new(
                    configuration.ingress_processing.max_control_read_bytes,
                )),
                Arc::new(Semaphore::new(
                    configuration.ingress_processing.max_control_read_bytes,
                )),
            ],
            isolation_bytes: Arc::new(Semaphore::new(
                configuration.ingress_processing.max_isolation_bytes,
            )),
            ingress_bytes: nasa::ws::transport::budget::Budget::new(
                configuration.ingress_processing.max_retained_bytes,
            )
            .expect("validated ingress byte capacity"),
            fanouts: Arc::new(Semaphore::new(
                configuration.ingress_processing.max_parallel_fanouts,
            )),
            candidate_references: Arc::new(Semaphore::new(
                configuration.ingress_processing.max_candidate_references,
            )),
            admissions: super::admission::Admissions::new(
                configuration.transport.admission.clone(),
                metrics.clone(),
            ),
            initializations: initialization::Initializations::new(
                &configuration.lifecycle_delivery,
                metrics.clone(),
            ),
            metrics: metrics.clone(),
            quotas: Default::default(),
            local_quotas: Default::default(),
            control_health: Arc::new(Default::default()),
            routes,
            incarnation: OnceLock::new(),
            startup_id: crate::domain::message::control::nonce(),
            control: OnceLock::new(),
            authorization_inbox: OnceLock::new(),
            calls: Arc::new(RemoteCalls::new(
                application.clone(),
                configuration.remote_calls.clone(),
                metrics.clone(),
            )),
            publishers: OnceLock::new(),
            lifecycle: OnceLock::new(),
            lifecycle_publish_shutdown: CancellationToken::new(),
            closing: AtomicBool::new(false),
            closed_sessions: tokio::sync::Notify::new(),
            application,
            verifiers: Default::default(),
            sessions: Mutex::new(BTreeMap::new()),
            revocations: Mutex::new(BTreeMap::new()),
            capacity: Arc::new(Semaphore::new(configuration.transport.max_sessions)),
            control_operation_capacity: Arc::new(Semaphore::new(
                configuration.transport.uid_events.control_reserve,
            )),
            operation_capacity: Arc::new(Semaphore::new(
                configuration.transport.max_pending_operations,
            )),
            outbound_bytes: Arc::new(Semaphore::new(configuration.transport.max_outbound_bytes)),
            inbound_bytes: nasa::ws::transport::budget::Budget::new(
                configuration.transport.max_inbound_bytes,
            )
            .expect("validated ingress capacity"),
            request_records: Arc::new(Semaphore::new(configuration.transport.max_request_records)),
            request_cache_bytes: Arc::new(Semaphore::new(
                configuration.transport.max_request_cache_bytes,
            )),
            deduplication: crate::domain::message::deduplication::Window::new(
                131072,
                32 * 1024 * 1024,
            ),
            broker_metrics: crate::observability::broker::Broker::new(&configuration),
            configuration,
        })
    }

    /// 业务作用：移除本次精确会话并先撤销其权限，防止迟到授权及队列写出继续生效。
    /// 参数说明：
    /// - `session_id`：由服务器生成的路由会话 ID。
    ///
    /// 返回：会话永久关闭；重复清理不会关闭其它连接。
    pub fn disconnect(self: &Arc<Self>, session_id: &str) {
        let connection = self
            .sessions
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(session_id)
            .cloned();
        let Some(connection) = connection else {
            return;
        };
        // 断开是单调撤权，不能排队等待后才禁止旧 writer 和迟到授权；正向状态只在消费域发布。
        connection.session.close();
        connection.cancellation.cancel();
        if connection.disconnect_queued.swap(true, Ordering::AcqRel) {
            return;
        }
        let closing = lifecycle::Closing::new(self.clone(), connection.clone());
        if let Ok(pending) = connection.uid_events.submit_control(
            tokio::time::Instant::now() + std::time::Duration::from_secs(30),
            65536,
            move || {
                closing.consume();
                Ok(())
            },
        ) {
            pending.detach();
        }
    }

    /// 业务作用：取得跨定义持续存在的撤权域，路由变更不能清空已知撤权事实。
    /// 参数说明：
    /// - `route_id`：本部署已经验证的稳定路由 ID。
    ///
    /// 返回：该路由共享的单调权限下界。
    fn revocations(&self, route_id: &str) -> Arc<Revocations> {
        self.revocations
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .entry(route_id.into())
            .or_insert_with(|| Arc::new(Revocations::default()))
            .clone()
    }
}

impl Connected {
    /// 业务作用：把操作结果的最终排队与同一主体的订阅、撤权和关闭放入统一顺序域。
    /// 参数说明：
    /// - `reply`：已经结算真实副作用的当前会话结果。
    /// - `ack`：可选 Socket.IO 编号及其独占 owner，原生协议为空。
    ///
    /// 返回：已接纳的本地排队责任，不表示网络写出或终端收到。
    pub fn enqueue_reply(
        self: &Arc<Self>,
        reply: actions::Reply,
        ack: Option<(u64, Arc<()>)>,
    ) -> anyhow::Result<super::uid_events::Pending<()>> {
        let owner = self.clone();
        let deadline = self.session.identity().deadline;
        let retained = reply
            .body
            .as_ref()
            .map_or(65536, |body| body.len().saturating_add(65536));
        self.uid_events.submit_control(deadline, retained, move || {
            anyhow::ensure!(
                owner.session.candidate(None).is_some() && !owner.cancellation.is_cancelled(),
                "session_closed"
            );
            let body = reply
                .body
                .map(|body| (owner.plan.definition.serialization.mode().ordinal(), body));
            let check =
                outbox::DeliveryCheck::direct(owner.session.clone(), owner.uid_events.clone());
            if let Some((ack, lease)) = ack {
                owner
                    .outbox
                    .enqueue_tracked(&reply.metadata, body, deadline, check, ack, lease)
            } else {
                owner.outbox.enqueue(
                    crate::domain::message::wire::FrameType::EventAck,
                    &reply.metadata,
                    body,
                    deadline,
                    Some(check),
                )
            }
        })
    }

    /// 业务作用：在主体顺序域维护授权通知和有限关联期限，不在物理读循环直接发布状态。
    /// 参数说明: 无。
    ///
    /// 返回：维护已实际消费；执行域失效时由传输立即关闭相应会话。
    pub async fn maintain(self: &Arc<Self>) -> anyhow::Result<()> {
        let owner = self.clone();
        self.uid_events
            .submit_control(
                tokio::time::Instant::now() + std::time::Duration::from_secs(30),
                65536,
                move || {
                    owner.session.maintain_notices();
                    owner.receipts.expire();
                    owner.requests.expire();
                    owner
                        .authorization_cache
                        .maintain(owner.session.authorization_generation());
                    if let Some(refreshes) = owner.automatic_refresh.upgrade() {
                        refreshes.schedule(&owner);
                    }
                    Ok(())
                },
            )?
            .wait()
            .await
    }

    /// 业务作用：把终端确认与同一主体的投递、取消和关闭放入同一消费域。
    /// 参数说明：
    /// - `receipt`：当前会话收到的固定票据结构，不授予其它会话确认权。
    ///
    /// 返回：票据已消费，失效票据沿用回执目录的拒绝语义。
    pub async fn confirm_receipt(
        self: &Arc<Self>,
        receipt: crate::domain::message::control::Receipt,
    ) -> anyhow::Result<()> {
        let owner = self.clone();
        self.uid_events
            .submit_control(self.session.identity().deadline, 65536, move || {
                anyhow::ensure!(!owner.cancellation.is_cancelled(), "session_closed");
                owner.receipts.confirm(&receipt);
                Ok(())
            })?
            .wait()
            .await
    }

    /// 业务作用：把连接能力快照同步到最终 writer 的权限状态，默认值仍由路由显式策略决定。
    /// 参数说明: 无。
    ///
    /// 返回：用户定向接收资格已更新，群权限继续独立校验。
    pub fn sync_receiving(&self) {
        let grants = self.grants.lock().unwrap_or_else(|e| e.into_inner());
        let allowed = self.plan.definition.receiving.direct.enabled
            && grants.valid()
            && grants
                .can_receive_direct
                .unwrap_or(!self.plan.definition.connect_authorization.enabled);
        self.session.direct_permission(if allowed {
            grants.deadline
        } else {
            None
        });
    }
    /// 业务作用：发布已经排定 Ready 的接流事实，防止初始化中间状态被 MQ 投递选中。
    /// 参数说明: 无。
    ///
    /// 返回：本连接可参与候选查询；最终写出仍复验领域门禁。
    fn publish_ready(&self) {
        self.wire_ready.store(true, Ordering::Release);
        if let Some(observed) = self
            .observed
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .as_mut()
        {
            observed.transfer("sessions_ready");
        }
    }
    /// 业务作用：读取连接是否已经建立对终端的 Ready 顺序。
    /// 参数说明: 无。
    ///
    /// 返回：只有已发布 Ready 且未取消时允许参与 MQ 候选。
    pub fn is_ready(&self) -> bool {
        self.wire_ready.load(Ordering::Acquire) && !self.cancellation.is_cancelled()
    }
}

impl Drop for Connected {
    /// 业务作用：在初始化中断或正常断连的最后 owner 退出时撤销权限并释放资源。
    /// 参数说明: 无。
    ///
    /// 返回：取消所有依赖任务，容量许可随记录释放。
    fn drop(&mut self) {
        self.session.close();
        self.cancellation.cancel();
    }
}
