use nasa::application::{MetricDescriptor, MetricKind};

/// 热路径只选择固定领域和结局，不接受客户端字符串生成标签。
#[derive(Clone, Copy)]
#[repr(usize)]
pub enum Counter {
    /// 逻辑连接建立与终止的事实计数。
    Connections,
    /// 客户端受限动作的执行结局。
    Operations,
    /// 身份验证尝试及其结果。
    Identity,
    /// 订阅、刷新等权限裁决结果。
    Authorization,
    /// 下行业务投递各阶段的结果，不直接等同于终端已处理。
    Deliveries,
    /// MQ 消费记录的处理结局。
    Consumer,
    /// MQ 发布尝试的提交结局。
    Producer,
    /// 持久控制处理及许可相关结果。
    Control,
    /// 配置接纳、应用和拒绝的事实。
    Configuration,
    /// 有界队列接纳、拒绝或替换等动作。
    Queue,
    /// 投递票据的最终观察结果。
    Receipts,
    /// 传输层实际处理的字节量。
    TransportBytes,
    /// 传输层实际处理的帧数量。
    TransportFrames,
    /// 业务正文复制操作的次数。
    PayloadCopies,
    /// 业务正文解码操作的次数。
    PayloadDecodes,
    /// 请求首次执行、重复观察等关联处理结果。
    Requests,
    /// 连接生命周期事实的发布结果。
    Lifecycle,
    /// 物理连接及认证前资源接纳结果。
    Admissions,
    /// 正文复制累计涉及的字节量，不表示唯一驻留内存。
    PayloadCopyBytes,
    /// 正文解码累计涉及的输入字节量。
    PayloadDecodeBytes,
    /// 各处理阶段观察到的业务正文数量。
    Payloads,
    /// 各处理阶段观察到的业务正文字节量。
    PayloadBytes,
    /// 心跳探测及匹配结果，不提供身份授权证据。
    Heartbeats,
    /// 终端回执输入的确认、拒绝、迟到和非法分类。
    ReceiptInputs,
    /// 依赖或状态恢复动作的结果。
    Recovery,
    /// 同 UID 保序队列的接纳与执行结局。
    UidEvents,
    /// 有限授权缓存的查询及复用结果。
    AuthorizationCache,
}

macro_rules! counters {
    ($(($id:ident, $name:literal, $help:literal, $label:literal, $subjects:expr)),* $(,)?) => {
        $(static $id: MetricDescriptor = MetricDescriptor {
            name: $name,
            help: $help,
            unit: "",
            kind: MetricKind::Counter,
            label_names: &[$label, "outcome"],
            histogram_bounds: &[],
        };)*
        pub(super) static COUNTERS: &[(&MetricDescriptor, &[&str])] = &[$((&$id, $subjects),)*];
    };
}

counters!(
    (
        CONNECTIONS,
        "nacket_connection_events_total",
        "受管物理连接的接入、结束及传输协商结局。",
        "adapter",
        &["unknown", "native_tcp", "native_ws", "socketio"]
    ),
    (
        OPERATIONS,
        "nacket_operations_total",
        "客户端操作的实际执行结局，取消不计成功。",
        "operation",
        &[
            "authenticate",
            "initialize",
            "sub",
            "unsub",
            "notify",
            "msg",
            "refresh",
            "confirm"
        ]
    ),
    (
        IDENTITY,
        "nacket_identity_checks_total",
        "固定身份策略的验证结局。",
        "strategy",
        &["remote", "local_jwt", "trusted_gateway", "anonymous"]
    ),
    (
        AUTHORIZATION,
        "nacket_authorization_checks_total",
        "业务权限检查结局，deny 与系统不可用独立。",
        "operation",
        &[
            "connect",
            "subscribe",
            "notify",
            "msg",
            "refresh",
            "initialize"
        ]
    ),
    (
        DELIVERIES,
        "nacket_deliveries_total",
        "本节点投递阶段结局；written 不代表终端收到。",
        "stage",
        &["admitted", "queued", "written", "received", "processed"]
    ),
    (
        CONSUMER,
        "nacket_mq_consume_total",
        "本节点实际 broker 记录处理结局。",
        "broker",
        &["redis", "kafka"]
    ),
    (
        PRODUCER,
        "nacket_mq_publish_total",
        "发布的真实 broker 结局，未知不计 committed。",
        "channel",
        &["control", "business", "lifecycle", "receipt", "dead_letter"]
    ),
    (
        CONTROL,
        "nacket_control_events_total",
        "控制请求的持久回复结局；提交回复不等于全局屏障已经完成。",
        "operation",
        &[
            "restore",
            "observe",
            "consume_assertion",
            "reserve_connection",
            "release_connection",
            "authorization",
            "authorization_applied",
            "register",
            "manifest",
            "apply_manifest",
            "define",
            "offline",
            "resume",
            "transfer",
            "drain",
            "prepare",
            "renew",
            "apply",
            "failed",
            "retire",
            "batch"
        ]
    ),
    (
        CONFIGURATION,
        "nacket_configuration_events_total",
        "候选配置解析、提交与有效状态变更结局。",
        "operation",
        &["reload", "apply", "restart_required"]
    ),
    (
        QUEUE,
        "nacket_queue_events_total",
        "有界队列的拒绝、替换与取消结局。",
        "queue",
        &[
            "ingress",
            "operation",
            "outbox",
            "receipt",
            "publish",
            "control"
        ]
    ),
    (
        RECEIPTS,
        "nacket_receipt_events_total",
        "当前终端投递票据的接纳与终止结局。",
        "level",
        &["received", "processed"]
    ),
    (
        TRANSPORT_BYTES,
        "nacket_transport_bytes_total",
        "产品适配器实际处理的完整协议消息字节，不含 TCP/TLS 开销。",
        "direction",
        &["receive", "transmit"]
    ),
    (
        TRANSPORT_FRAMES,
        "nacket_transport_frames_total",
        "产品适配器实际处理的完整消息帧。",
        "direction",
        &["receive", "transmit"]
    ),
    (
        PAYLOAD_COPIES,
        "nacket_payload_copies_total",
        "产品显式业务正文分配或复制次数；不覆盖系统、TLS 与 WebSocket 掩码。",
        "boundary",
        &["kafka", "redis", "socketio", "decode", "encode"]
    ),
    (
        PAYLOAD_DECODES,
        "nacket_payload_deserializations_total",
        "产品要求物化业务正文的次数，不包含固定控制 JSON。",
        "purpose",
        &["subscription", "operation", "validation"]
    ),
    (
        REQUESTS,
        "nacket_request_records_total",
        "会话请求的首次接纳、重复关联、身份冲突与结果正文容量结局。",
        "stage",
        &["admission", "result"]
    ),
    (
        LIFECYCLE,
        "nacket_lifecycle_events_total",
        "精确连接事实的接纳与远端提交结局，不代表用户级在线状态。",
        "event",
        &["online", "offline"]
    ),
    (
        ADMISSIONS,
        "nacket_admissions_total",
        "所有业务端口共享的物理接入容量和真实来源窗口结局。",
        "scope",
        &[
            "physical",
            "unauthenticated",
            "peer_directory",
            "peer_connections",
            "peer_rate"
        ]
    ),
    (
        PAYLOAD_COPY_BYTES,
        "nacket_payload_copy_bytes_total",
        "产品显式正文复制或编码分配的字节量，不表示全部分配或内存带宽。",
        "boundary",
        &["kafka", "redis", "socketio", "decode", "encode"]
    ),
    (
        PAYLOAD_DECODE_BYTES,
        "nacket_payload_deserialization_bytes_total",
        "实际尝试物化的原始业务编码字节，失败尝试也计入。",
        "purpose",
        &["subscription", "operation", "validation"]
    ),
    (
        PAYLOADS,
        "nacket_payloads_total",
        "进入业务处理入口的完整正文；重试重新计数，不表示唯一业务消息。",
        "mode",
        &[
            "JSON_BYTES",
            "VARINT_TLV",
            "BITPACK_TLV",
            "FAST_FIXED",
            "unknown"
        ]
    ),
    (
        PAYLOAD_BYTES,
        "nacket_payload_bytes_total",
        "进入业务处理入口的完整正文编码字节，不包含固定控制头。",
        "mode",
        &[
            "JSON_BYTES",
            "VARINT_TLV",
            "BITPACK_TLV",
            "FAST_FIXED",
            "unknown"
        ]
    ),
    (
        HEARTBEATS,
        "nacket_heartbeat_events_total",
        "业务连接心跳的有效回复与到期结局，不与 WebSocket 控制帧混合。",
        "adapter",
        &["native", "socketio"]
    ),
    (
        RECEIPT_INPUTS,
        "nacket_receipt_inputs_total",
        "终端回执输入的匹配结局；unknown 为无当前票据，不代表新的投递终态。",
        "level",
        &["received", "processed"]
    ),
    (
        RECOVERY,
        "nacket_recovery_events_total",
        "消费恢复中的实际读取、认领、确认、隔离与临时重试，不能视为终端投递结果。",
        "operation",
        &[
            "redis_read",
            "redis_claim",
            "redis_confirm",
            "redis_isolate",
            "redis_retry",
            "kafka_retry"
        ]
    ),
    (
        UID_EVENTS,
        "nacket_uid_events_total",
        "可信主体事件的实际消费结局，不将排队当作完成。",
        "class",
        &["data", "control"]
    ),
    (
        AUTHORIZATION_CACHE,
        "nacket_authorization_cache_total",
        "有界只读授权缓存的命中、查询、合并等待和容量旁路。",
        "operation",
        &["hit", "miss", "wait", "bypass", "fetched"]
    ),
);

pub(super) const OUTCOMES: &[&str] = &[
    "ok",
    "allow",
    "deny",
    "invalid",
    "rejected",
    "timeout",
    "unavailable",
    "unknown",
    "canceled",
    "expired",
    "duplicate",
    "conflict",
    "dropped",
    "committed",
    "accepted",
    "closed",
];

pub(super) static LIVE: MetricDescriptor = MetricDescriptor {
    name: "nacket_live_resources",
    help: "当前实际持有的逻辑资源，按责任释放，不由丢失事件推断。",
    unit: "",
    kind: MetricKind::Gauge,
    label_names: &["resource"],
    histogram_bounds: &[],
};

pub(super) static NODE: MetricDescriptor = MetricDescriptor {
    name: "nacket_node_info",
    help: "本进程持久登记的节点身份、代际、角色和控制后端；每个进程固定一组，不承载业务授权。",
    unit: "",
    kind: MetricKind::Gauge,
    label_names: &["node_id", "incarnation", "role", "control_backend"],
    histogram_bounds: &[],
};
pub(super) static AGE: MetricDescriptor = MetricDescriptor {
    name: "nacket_oldest_pending_age_seconds",
    help: "当前真实待决责任最旧年龄的保守上界，量化误差小于一秒；观察不完整时不输出。",
    unit: "seconds",
    kind: MetricKind::Gauge,
    label_names: &["resource"],
    histogram_bounds: &[],
};
pub(super) static AGE_COMPLETE: MetricDescriptor = MetricDescriptor {
    name: "nacket_pending_age_complete",
    help: "当前资源的待决年龄是否覆盖全部 owner；零表示未知而不是无等待。",
    unit: "",
    kind: MetricKind::Gauge,
    label_names: &["resource"],
    histogram_bounds: &[],
};
pub(super) const RESOURCES: &[&str] = &[
    "redis_pending_records",
    "physical_connections",
    "unauthenticated_connections",
    "connections_unknown",
    "connections_native_tcp",
    "connections_native_ws",
    "connections_socketio",
    "sessions_initializing",
    "sessions_ready",
    "operations",
    "remote_calls",
    "authorization_refresh_pending",
    "authorization_refresh_inflight",
    "authorization_cache_entries",
    "authorization_cache_bytes",
    "remote_bytes",
    "handshake_context_bytes",
    "subscriptions",
    "subscriptions_pending",
    "outbox_items",
    "outbox_bytes",
    "writing_items",
    "writing_bytes",
    "receipts",
    "control_pending",
    "control_output_items",
    "control_output_bytes",
    "authorization_commands",
    "fanouts",
    "fanout_candidates",
    "fanout_pending_records",
    "ingress_read_bytes",
    "initialization_read_bytes",
    "authorization_read_bytes",
    "isolation_bytes",
    "connection_reservations",
    "authorization_bytes",
    "request_records",
    "request_cache_bytes",
    "publish_items",
    "publish_bytes",
    "lifecycle_items",
    "lifecycle_bytes",
    "initialization_requests",
    "initialization_bytes",
    "uid_events",
    "uid_event_bytes",
];

pub(super) static DURATION: MetricDescriptor = MetricDescriptor {
    name: "nacket_duration_seconds",
    help: "固定领域的本地单调时间分布，跨节点时钟不参与。",
    unit: "seconds",
    kind: MetricKind::Histogram,
    label_names: &["stage"],
    histogram_bounds: &[
        0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0,
    ],
};
pub(super) const STAGES: &[&str] = &[
    "connection_lifetime",
    "handshake",
    "initialization",
    "operation",
    "authorization",
    "queue_wait",
    "write",
    "receipt",
    "broker_commit",
    "control_restore",
    "control_request",
    "route_transition",
    "configuration_apply",
    "shutdown",
    "heartbeat_rtt",
    "lifecycle_delivery",
    "uid_event_wait",
    "uid_event_execute",
    "uid_event_total",
];

pub(super) static PROJECTION: MetricDescriptor = MetricDescriptor {
    name: "nacket_runtime_state",
    help: "有界运行状态投影，不能用作权限或控制依据。",
    unit: "",
    kind: MetricKind::Gauge,
    label_names: &["state"],
    histogram_bounds: &[],
};
pub(super) const PROJECTIONS: &[&str] = &[
    "sessions_active",
    "sessions_idle",
    "subscription_targets",
    "session_sample_available",
    "subscription_target_sample_available",
    "session_sample_span_seconds",
    "session_sample_timestamp_seconds",
    "routes_online",
    "routes_protected",
    "routes_offline",
    "writers",
    "dependencies_ready",
    "dependencies_unavailable",
    "session_capacity",
    "operation_capacity",
    "outbound_byte_capacity",
    "inbound_byte_capacity",
    "inbound_bytes",
    "persistence_jobs",
    "ingress_retained_bytes",
    "ingress_byte_capacity",
    "physical_connection_capacity",
    "unauthenticated_connection_capacity",
    "request_record_capacity",
    "request_cache_byte_capacity",
    "ready",
    "local_eligible",
    "listeners_bound",
    "configuration_pending_restart",
    "started_timestamp_seconds",
    "uptime_seconds",
];

/// 业务作用：冻结产品指标名与有限标签维度，与框架统一目录核对冲突。
/// 参数说明: 无。
///
/// 返回：进程内固定描述符列表，不随路由、用户或消息 ID 增长。
pub(super) fn descriptors() -> &'static [&'static MetricDescriptor] {
    static VALUE: std::sync::OnceLock<Vec<&'static MetricDescriptor>> = std::sync::OnceLock::new();
    VALUE.get_or_init(|| {
        let mut values: Vec<_> = COUNTERS.iter().map(|(descriptor, _)| *descriptor).collect();
        values.extend([&LIVE, &DURATION, &PROJECTION, &AGE, &AGE_COMPLETE, &NODE]);
        values.extend([
            &super::control::COUNTS,
            &super::control::FRESH,
            &super::control::AGE,
            &super::control::PENDING,
            &super::control::LOCAL,
            &super::control::PERMIT,
            &super::control::OWNER_COUNTS,
            &super::control::OWNER_FRESH,
            &super::control::OWNER_AGE,
            &super::control::OWNER_REMAINING,
            &super::control::OWNER_PENDING,
        ]);
        values
    })
}
