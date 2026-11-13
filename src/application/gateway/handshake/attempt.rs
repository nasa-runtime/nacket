use crate::{
    application::gateway::Gateway,
    domain::{message::envelope::SessionKey, route::RouteGate},
};
use std::sync::{Arc, Mutex};
use tokio::time::Instant;

/// 同一次握手跨认证、确认和初始化保留的观测责任，不参与授权或恢复判定。
pub(crate) struct Attempt {
    /// 仅用于读取节点可用性，不能据此授予路由权限。
    application: nasa::Application,
    /// 真实接入协议，由适配器确定。
    transport: &'static str,
    /// 首次开始的单调时刻，不因阶段变化刷新。
    started: Instant,
    /// 物理接入时固定的认证期限。
    deadline: Instant,
    /// 不保存凭据、远程响应正文或任意错误文本的有界上下文。
    state: Mutex<State>,
}

struct State {
    /// 固定阶段名，用于区分身份允许之后的各个门禁。
    stage: &'static str,
    /// 已经过格式与长度校验的客户端尝试标识。
    attempt_id: Option<String>,
    /// 服务端生成的精确会话作用域。
    key: Option<SessionKey>,
    /// 原冻结定义的门禁，观察不取得写出许可。
    gate: Option<Arc<RouteGate>>,
    /// 仅在身份来源校验通过后记录租户和用户。
    principal: Option<(String, String)>,
    /// 确认失败后目录中的同主体会话数，包含尚在回收的连接。
    same_uid_sessions: Option<usize>,
    /// Ready 或首次失败后不再重复记录握手终态。
    finished: bool,
}

impl Attempt {
    /// 业务作用：为认证尝试建立独立观测责任，传输退出时仍能说明最后到达的阶段。
    /// 参数说明：
    /// - `gateway`：提供节点运行状态的宿主。
    /// - `transport`：服务端确定的固定协议名称。
    /// - `deadline`：原始握手截止时刻。
    ///
    /// 返回：尚未绑定客户端字段的记录；不占用会话或路由许可。
    pub(crate) fn new(gateway: &Gateway, transport: &'static str, deadline: Instant) -> Arc<Self> {
        Arc::new(Self {
            application: gateway.application.clone(),
            transport,
            started: Instant::now(),
            deadline,
            state: Mutex::new(State {
                stage: "request_validation",
                attempt_id: None,
                key: None,
                gate: None,
                principal: None,
                same_uid_sessions: None,
                finished: false,
            }),
        })
    }

    /// 业务作用：只保留已校验的关联值，非法认证正文不能进入日志。
    /// 参数说明：
    /// - `id`：通过 Authenticate 校验的尝试 ID。
    ///
    /// 返回：后续阶段与客户端尝试可关联；不改变身份状态。
    pub(crate) fn identify(&self, id: &str) {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .attempt_id = Some(id.into());
    }

    /// 业务作用：绑定服务端冻结的路由与会话，观察旧定义失效而不重新选路。
    /// 参数说明：
    /// - `key`：本次认证唯一会话键。
    /// - `gate`：同一定义的原门禁。
    ///
    /// 返回：后续拒绝记录携带原作用域和许可状态。
    pub(crate) fn bind(&self, key: &SessionKey, gate: &Arc<RouteGate>) {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        state.key = Some(key.clone());
        state.gate = Some(gate.clone());
    }

    /// 业务作用：在可信身份校验完成后建立跨新旧会话的主体关联。
    /// 参数说明：
    /// - `tenant`：已验证的租户。
    /// - `uid`：已验证的非空用户标识。
    ///
    /// 返回：仅复制有界标识，不记录凭据或授权正文。
    pub(crate) fn principal(&self, tenant: &str, uid: &str) {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .principal = Some((tenant.into(), uid.into()));
    }

    /// 业务作用：在确认失败后观察同主体会话数量，避免把本地 close 回执当作服务端回收。
    /// 参数说明：
    /// - `count`：失败后持有会话目录锁时统计的同主体连接数。
    ///
    /// 返回：保存观察快照；不删除旧连接或扩大配额。
    pub(crate) fn same_uid_sessions(&self, count: usize) {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .same_uid_sessions = Some(count);
    }

    /// 业务作用：在可能失败或等待的步骤之前记录固定阶段，取消也保留该位置。
    /// 参数说明：
    /// - `stage`：源码中固定的阶段名称，不能来自远程文本。
    ///
    /// 返回：只更新未完成握手的观察位置。
    pub(crate) fn stage(&self, stage: &'static str) {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if !state.finished {
            state.stage = stage;
        }
    }

    /// 业务作用：在首次握手失败时输出可关联的封闭原因，客户端错误与重连策略保持独立。
    /// 参数说明：
    /// - `error`：实际失败结果，只允许固定类别进入日志。
    ///
    /// 返回：至多一条 WARN 记录；不发送网络消息、不重试也不改变权限。
    pub(crate) fn reject(&self, error: &anyhow::Error) {
        self.report(reason(error));
    }

    /// 业务作用：在传输或协议步骤失败时保存真实阶段，并原样交还失败给现有关闭路径。
    /// 参数说明：
    /// - `result`：本步骤已经完成的结果。
    ///
    /// 返回：原结果不变；错误仅经过封闭分类后记录一次。
    pub(crate) fn check<T>(&self, result: anyhow::Result<T>) -> anyhow::Result<T> {
        result.inspect_err(|error| self.reject(error))
    }

    /// 业务作用：在服务端完成 Ready 发布后结束握手观测，正常断连由会话日志承担。
    /// 参数说明: 无。
    ///
    /// 返回：后续销毁不再记录未完成握手。
    pub(crate) fn ready(&self) {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .finished = true;
    }

    /// 业务作用：对失败和取消共用一次终态输出，快照不承担准入权威。
    /// 参数说明：
    /// - `reason`：已归类的固定原因。
    ///
    /// 返回：首次失败记录阶段、关联值及当下许可观察；不输出任意错误正文。
    fn report(&self, reason: &'static str) {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.finished {
            return;
        }
        state.finished = true;
        let key = state.key.as_ref();
        tracing::warn!(
            event = "handshake_rejected",
            stage = state.stage,
            reason,
            attempt_id = ?state.attempt_id,
            node_id = ?key.map(|key| &key.node_id),
            incarnation = ?key.map(|key| &key.incarnation),
            route_id = ?key.map(|key| &key.route_id),
            route_revision = ?key.map(|key| &key.route_revision),
            session_id = ?key.map(|key| &key.route_session_id),
            tenant_id = ?state.principal.as_ref().map(|principal| &principal.0),
            uid = ?state.principal.as_ref().map(|principal| &principal.1),
            same_uid_sessions_after_rejection = ?state.same_uid_sessions,
            transport = self.transport,
            runtime_ready = self.application.is_ready(),
            route_unavailable = ?state.gate.as_ref().and_then(|gate| gate.unavailable_reason()),
            route_permit_remaining_ms = ?state.gate.as_ref().map(|gate| gate.permit_remaining().as_millis()),
            elapsed_ms = self.started.elapsed().as_millis(),
            deadline_elapsed = Instant::now() >= self.deadline,
            "连接握手未完成"
        );
    }
}

impl Drop for Attempt {
    /// 业务作用：最后一个握手 owner 退出时记录尚未结束的阶段，任务取消不能抹去失败事实。
    /// 参数说明: 无。
    ///
    /// 返回：尚未 Ready 且未记录失败时输出取消或期限原因；不延长 owner 生命周期。
    fn drop(&mut self) {
        self.report(if Instant::now() >= self.deadline {
            "authentication_deadline"
        } else {
            "handshake_abandoned"
        });
    }
}

/// 业务作用：保留本地门禁的精确分类，外部错误只进入有限结局目录。
/// 参数说明：
/// - `error`：握手路径已经产生的失败。
///
/// 返回：固定字符串；凭据、远程响应及传输错误的任意文本不会写入日志。
fn reason(error: &anyhow::Error) -> &'static str {
    match error.to_string().as_str() {
        "authentication_deadline" => "authentication_deadline",
        "confirmation_required" => "confirmation_required",
        "authorization_response_capacity" => "authorization_response_capacity",
        "authorization_response_mismatch" => "authorization_response_mismatch",
        "authorization_decision_invalid" => "authorization_decision_invalid",
        "authorization_missing" => "authorization_missing",
        "authorization_expired" => "authorization_expired",
        "authorization_ttl" => "authorization_ttl",
        "permission_denied" => "permission_denied",
        "principal_missing" => "principal_missing",
        "unexpected_principal" => "unexpected_principal",
        "invalid_identifier" => "invalid_identifier",
        "invalid_uid" => "invalid_uid",
        "invalid_revision" => "invalid_revision",
        "route_changed" => "route_changed",
        "route_unavailable" => "route_unavailable",
        "directory_binding_stale" => "directory_binding_stale",
        "admission_unavailable" => "admission_unavailable",
        "connection_capacity" => "connection_capacity",
        "uid_event_capacity" => "uid_event_capacity",
        "uid_event_rejected" => "uid_event_rejected",
        "uid_event_incomplete" => "uid_event_incomplete",
        "uid_event_result_missing" => "uid_event_result_missing",
        "uid_event_expired" => "uid_event_expired",
        "uid_event_timeout" => "uid_event_timeout",
        "uid_events_unavailable" => "uid_events_unavailable",
        "confirmation_cancelled" => "confirmation_cancelled",
        "confirmation_mismatch" => "confirmation_mismatch",
        "node_connection_capacity" => "node_connection_capacity",
        "route_connection_capacity" => "route_connection_capacity",
        "user_connection_capacity" => "user_connection_capacity",
        "user_already_connected" => "user_already_connected",
        "quota_session_closed" => "quota_session_closed",
        "quota_admission_timeout" => "quota_admission_timeout",
        "initialization_closed" => "initialization_closed",
        "session_not_eligible" => "session_not_eligible",
        "session_not_ready" => "session_not_ready",
        "session_closed" => "session_closed",
        "invalid_identity_or_capacity" => "invalid_identity_or_capacity",
        "descriptor_capacity" => "descriptor_capacity",
        _ => crate::observability::outcome(error),
    }
}
