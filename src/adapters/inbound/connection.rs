use crate::{
    application::gateway::{Gateway, Handshake},
    domain::message::control::CONTRACT,
};
use nasa::ws::{
    server::ProtocolContext,
    transport::{Connection, ErrorResponse, Kind, Limits},
};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::net::TcpStream;

/// 业务作用：在受管监听器的原始认证期限内选择固定传输，并将业务会话交给产品策略。
/// 参数说明：
/// - `gateway`：共享产品运行域。
/// - `stream`：受管 listener 接收的唯一 TCP 连接。
/// - `context`：框架提供的容量、取消和子任务管理权。
/// - `binding`：实际端口对应的模板、协议与独立路由快照。
///
/// 返回：连接全部处理完成；拒绝和传输错误只记录固定分类，不记录凭据或正文。
pub async fn accept(
    gateway: Arc<Gateway>,
    stream: TcpStream,
    context: ProtocolContext,
    binding: crate::application::listeners::Binding,
) {
    let mut observation = crate::observability::Connection::new(gateway.metrics.clone());
    let result = run(gateway, stream, context, binding, &mut observation).await;
    if result.is_err() {
        tracing::trace!(reason = "connection_ended", "nacket connection closed");
    }
}

/// 业务作用：验证 WebSocket 路径、子协议和有限握手材料，禁止 URL 凭据或协议自动降级。
/// 参数说明：
/// - `gateway`：固定部署能力与路由服务。
/// - `stream`：真实连接。
/// - `context`：从 accept 开始计算的准入上下文。
/// - `binding`：端口实际绑定的模板及路由作用域。
/// - `observation`：当前物理连接的唯一指标责任。
///
/// 返回：协议正常结束；不匹配的握手在建立业务状态前关闭。
#[expect(
    clippy::result_large_err,
    reason = "升级回调必须返回宿主固定的 HTTP 拒绝响应类型。"
)]
async fn run(
    gateway: Arc<Gateway>,
    stream: TcpStream,
    context: ProtocolContext,
    binding: crate::application::listeners::Binding,
    observation: &mut crate::observability::Connection,
) -> anyhow::Result<()> {
    // 在协议识别前取得全节点预算，独立端口和转发头不能扩张真实来源的接入容量。
    let admission = gateway.admissions.acquire(stream.peer_addr()?.ip())?;
    let mut settings = gateway.configuration.transport.clone();
    use crate::config::listeners::Protocol;
    settings.native_tcp &= binding.protocols.contains(&Protocol::NativeTcp);
    settings.native_websocket &= binding.protocols.contains(&Protocol::NativeWs);
    settings.socketio &= binding.protocols.contains(&Protocol::Socketio);
    let mut assertion_headers = std::collections::BTreeSet::new();
    let mut allowed_headers = std::collections::BTreeMap::<String, usize>::new();
    for definition in gateway.routes.definitions() {
        for (name, limit) in &definition.handshake.headers {
            let maximum = allowed_headers.entry(name.clone()).or_default();
            *maximum = (*maximum).max(*limit);
        }
        if let crate::domain::route::Identity::TrustedGateway {
            assertion_header,
            ..
        } = definition.identity
        {
            assertion_headers.insert(assertion_header);
        }
    }
    let facts = Arc::new(Mutex::new((
        None,
        false,
        std::collections::BTreeMap::new(),
        std::collections::BTreeMap::new(),
        None,
    )));
    let captured = facts.clone();
    let policy = settings.clone();
    let routes = gateway.routes.clone();
    let actual_binding = binding.clone();
    let readiness = gateway.clone();
    let transport = Connection::accept_with_budget(
        stream,
        Limits {
            maximum_message: settings.max_frame_bytes,
            assembly_timeout: Duration::from_millis(settings.assembly_timeout_ms),
            write_timeout: Duration::from_millis(settings.write_timeout_ms),
        },
        context.auth_deadline(),
        settings.native_tcp && !context.websocket_only(),
        move |request, mut response| {
            // 升级前拒绝尚无本地接流资格的节点，客户端尚未发送 AUTH，恢复不必重放未知认证。
            // 此检查不授予路由权限；升级后的认证仍会复验依赖、精确定义和当前门禁。
            if !readiness.accepts_connections() {
                let mut unavailable = ErrorResponse::new(Some("service_unavailable".into()));
                *unavailable.status_mut() = 503u16.try_into().expect("HTTP status");
                return Err(unavailable);
            }
            let reject = || {
                let mut response = ErrorResponse::new(Some("unsupported_transport".into()));
                *response.status_mut() = 400u16.try_into().expect("HTTP status");
                response
            };
            if request.headers().len() > 64 || request.uri().to_string().len() > 1024 {
                return Err(reject());
            }
            // 安全字段必须只有一个解释，禁止重复头在代理和业务校验之间产生不同来源事实。
            for name in ["origin", "host", "sec-websocket-protocol"] {
                if request.headers().get_all(name).iter().count() > 1 {
                    return Err(reject());
                }
            }
            let mut assertions = std::collections::BTreeMap::new();
            let mut headers = std::collections::BTreeMap::new();
            let mut header_bytes = 0usize;
            for (name, limit) in &allowed_headers {
                // 签名断言只交给身份验证器，不能因其它路由配置普通参数而向业务端点复制。
                if assertion_headers.contains(name) {
                    continue;
                }
                let mut values = request.headers().get_all(name.as_str()).iter();
                if let Some(value) = values.next() {
                    let value = value.to_str().map_err(|_| reject())?;
                    header_bytes += name.len() + value.len();
                    if values.next().is_some() || value.len() > *limit || header_bytes > 8192 {
                        return Err(reject());
                    }
                    headers.insert(name.clone(), value.to_owned());
                }
            }
            for name in &assertion_headers {
                let values = request.headers().get_all(name);
                let mut values = values.iter();
                if let Some(value) = values.next() {
                    let value = value.to_str().map_err(|_| reject())?;
                    if values.next().is_some() || value.len() > 8192 {
                        return Err(reject());
                    }
                    assertions.insert(name.clone(), value.to_owned());
                }
            }
            let origin = match request.headers().get("origin") {
                Some(value) => {
                    let value = value.to_str().map_err(|_| reject())?;
                    if value.len() > 512 {
                        return Err(reject());
                    }
                    Some(value.to_owned())
                }
                None => None,
            };
            let mut legacy_path = None;
            let socketio = if request.uri().path() == policy.native_path && policy.native_websocket
            {
                if request.uri().query().is_some() {
                    return Err(reject());
                }
                let protocol = request
                    .headers()
                    .get("sec-websocket-protocol")
                    .and_then(|v| v.to_str().ok())
                    .ok_or_else(reject)?;
                if !protocol.split(',').any(|value| value.trim() == CONTRACT) {
                    return Err(reject());
                }
                response.headers_mut().insert(
                    "sec-websocket-protocol",
                    CONTRACT.parse().expect("fixed protocol"),
                );
                false
            } else if policy.socketio {
                if request.uri().path() != policy.socketio_path {
                    let path = request.uri().path();
                    let namespace = path.strip_suffix('/').unwrap_or(path);
                    let (plan, gate) = routes
                        .resolve(
                            namespace,
                            true,
                            actual_binding.route_id.as_deref(),
                            &actual_binding.listener_ref,
                        )
                        .ok_or_else(reject)?;
                    // 兼容入口必须由当前在线路由明确打开；只去除一个末尾斜杠，不解码或猜测其它路径。
                    if !plan.definition.compatibility.legacy_path
                        || gate.acquire(plan.definition.revision).is_none()
                        || actual_binding.access.as_ref().is_some_and(|access| {
                            access.dedicated_port != plan.definition.access.dedicated_port
                        })
                    {
                        return Err(reject());
                    }
                    legacy_path = Some(crate::application::gateway::LegacyPath {
                        route_id: plan.definition.route_id.clone(),
                        revision: plan.definition.revision,
                        digest: plan.digest.clone(),
                        namespace: namespace.to_owned(),
                    });
                }
                let query: Vec<_> =
                    url::form_urlencoded::parse(request.uri().query().unwrap_or("").as_bytes())
                        .collect();
                if query.len() != 2
                    || query.iter().filter(|(k, v)| k == "EIO" && v == "4").count() != 1
                    || query
                        .iter()
                        .filter(|(k, v)| k == "transport" && v == "websocket")
                        .count()
                        != 1
                {
                    return Err(reject());
                }
                true
            } else {
                return Err(reject());
            };
            *captured.lock().unwrap_or_else(|e| e.into_inner()) =
                (origin, socketio, assertions, headers, legacy_path);
            Ok(response)
        },
        Some(gateway.inbound_bytes.clone()),
    )
    .await?;
    let (origin, socketio, assertions, headers, legacy_path) =
        facts.lock().unwrap_or_else(|e| e.into_inner()).clone();
    observation.identify(if socketio {
        "socketio"
    } else if transport.kind() == Kind::WebSocket {
        "native_ws"
    } else {
        "native_tcp"
    });
    let handshake = Handshake {
        origin,
        peer: context.peer_addr().ip().to_string(),
        websocket: transport.kind() == Kind::WebSocket,
        socketio,
        binding,
        assertions,
        headers,
        legacy_path,
    };
    if socketio {
        return super::socketio::run(gateway, transport, context, handshake, admission).await;
    }
    super::native::run(gateway, transport, context, handshake, admission).await
}
