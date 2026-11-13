use super::policies::*;
use nasa::ws::proto::dynamic::SchemaDescriptor;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// 业务选择的唯一序列化模式，禁止客户端协商覆盖。
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Serialization {
    /// 使用 ProtocolBytes JSON 字节模式，字段合同仍由固定 schema 决定。
    JsonBytes,
    /// 使用 ProtocolBytes 变长整数 TLV 模式。
    VarintTlv,
    /// 使用 ProtocolBytes 位打包 TLV 模式。
    BitpackTlv,
    /// 使用 ProtocolBytes 固定布局模式，发送接收必须共享同一描述符。
    FastFixed,
}

impl Serialization {
    /// 业务作用：把路由选择转换为共享 ProtocolBytes 的稳定模式。
    /// 参数说明：无。
    ///
    /// 返回：与 Java 协议 ordinal 对应的唯一模式。
    pub fn mode(self) -> nasa::ws::proto::Mode {
        use nasa::ws::proto::Mode;
        match self {
            Self::JsonBytes => Mode::JsonBytes,
            Self::VarintTlv => Mode::VarintTlv,
            Self::BitpackTlv => Mode::BitpackTlv,
            Self::FastFixed => Mode::FastFixed,
        }
    }
}

/// 权限开关的缺省值为关闭，缺字段不能扩大能力。
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Toggle {
    /// 是否启用所属策略，具体能力仍须满足该策略的授权与资源前提。
    pub enabled: bool,
}

/// 独立端口只在显式开启后分配；None 表示按部署范围递增绑定。
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct DedicatedPort {
    /// 路由是否申请独立业务端口，关闭时使用受权模板的共享监听。
    pub enabled: bool,
    /// 可选固定独立端口；开启且未指定时仅在部署批准范围内实际 bind 搜索。
    pub port: Option<u16>,
}

/// 业务接入映射与部署监听模板绑定。
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Access {
    /// 当前路由使用的受权监听模板引用，决定地址、协议和独立端口边界。
    pub listener_ref: String,
    #[serde(default)]
    /// 通用逻辑接入路径，原生 endpoint 或 Socket.IO namespace 未覆盖时继承。
    pub endpoint: String,
    #[serde(default)]
    /// 路由独立端口开关及可选固定端口，不能占用管理或监控端口。
    pub dedicated_port: DedicatedPort,
    #[serde(default)]
    /// 原生适配器入口开关、逻辑 endpoint 和固定帧承载合同。
    pub native: NativeAccess,
    #[serde(default)]
    /// Socket.IO 兼容入口开关、namespace 和正文承载合同。
    pub socketio: SocketioAccess,
}

/// 原生业务 endpoint 与固定 wire profile，不因 Socket.IO 开关改变编码。
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase", deny_unknown_fields)]
pub struct NativeAccess {
    /// 是否允许本路由原生 TCP/WS 接入，物理监听协议开关仍同时生效。
    pub enabled: bool,
    /// 可选原生逻辑接入路径，缺省使用公共 access.endpoint。
    pub endpoint: Option<String>,
    /// 明确的原生外层帧合同，不改变路由 ProtocolBytes 正文模式。
    pub wire_profile: String,
}
impl Default for NativeAccess {
    /// 业务作用：默认不从原生接入开放业务路由，启用后固定使用分区信封。
    /// 参数说明: 无。
    ///
    /// 返回：关闭原生接入并选择 split_envelope profile。
    fn default() -> Self {
        Self {
            enabled: false,
            endpoint: None,
            wire_profile: "split_envelope".into(),
        }
    }
}
/// Socket.IO namespace 只是外部映射，业务体固定使用二进制附件。
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase", deny_unknown_fields)]
pub struct SocketioAccess {
    /// 是否允许本路由通过 Socket.IO namespace 接入，默认不因此启用其它能力。
    pub enabled: bool,
    /// 可选标准 namespace，缺省使用 access.endpoint，不能凭任意 path 猜测路由。
    pub namespace: Option<String>,
    /// 明确的 Socket.IO 控制头与二进制附件布局，不以错误结果临时降级。
    pub wire_layout: String,
}
impl Default for SocketioAccess {
    /// 业务作用：默认不开放兼容适配，启用时也不能改变路由唯一编码。
    /// 参数说明: 无。
    ///
    /// 返回：关闭 Socket.IO 接入并固定 binary_attachment 布局。
    fn default() -> Self {
        Self {
            enabled: false,
            namespace: None,
            wire_layout: "binary_attachment".into(),
        }
    }
}
impl Access {
    /// 业务作用：取得显式启用的原生业务 endpoint，共用 endpoint 仅是部署声明的路径简写。
    /// 参数说明: 无。
    ///
    /// 返回：已启用入口的固定路径，未启用或未配置时返回 None。
    pub fn native_endpoint(&self) -> Option<&str> {
        self.native
            .enabled
            .then(|| self.native.endpoint.as_deref().unwrap_or(&self.endpoint))
            .filter(|p| !p.is_empty())
    }
    /// 业务作用：取得显式启用的 Socket.IO namespace，不从客户端路径推断新路由。
    /// 参数说明: 无。
    ///
    /// 返回：已启用 namespace，未启用或未配置时返回 None。
    pub fn socketio_namespace(&self) -> Option<&str> {
        self.socketio
            .enabled
            .then(|| self.socketio.namespace.as_deref().unwrap_or(&self.endpoint))
            .filter(|p| !p.is_empty())
    }
}

/// 路由身份来源，由远程授权、本地验签、可信网关或显式匿名策略建立身份。
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
pub enum Identity {
    /// 不提供身份建立能力；不能以关闭认证为由接受客户端自报 UID。
    Disabled,
    /// 从固定业务接口取得身份，明确允许还须通过关联和有效期校验。
    #[serde(rename = "remote_http")]
    Remote {
        #[serde(rename = "endpointRef")]
        /// 远程身份验证使用的固定业务端点引用，客户端不能指定目标。
        endpoint_ref: String,
    },
    /// 按部署验签策略建立主体，不读取客户端任意公钥地址。
    #[serde(rename = "local_jwt")]
    LocalJwt {
        #[serde(rename = "verifierRef")]
        /// 本地 JWT 或网关断言使用的固定验签器引用。
        verifier_ref: String,
    },
    /// 仅接受可信网络对端提供的已验签一次性断言。
    #[serde(rename = "trusted_gateway")]
    TrustedGateway {
        #[serde(rename = "verifierRef")]
        /// 本地 JWT 或网关断言使用的固定验签器引用。
        verifier_ref: String,
        #[serde(rename = "assertionHeader")]
        /// 允许读取网关签名断言的单一 header 名，重复值直接拒绝。
        assertion_header: String,
        #[serde(rename = "trustedPeers")]
        /// 允许提供网关断言的真实 TCP 来源集合，单有 header 不代表可信代理。
        trusted_peers: std::collections::BTreeSet<std::net::IpAddr>,
    },
    /// 显式公共路由由服务端创建有限期匿名身份，仍保证 UID 非空。
    #[serde(rename = "anonymous")]
    Anonymous {
        #[serde(rename = "tenantId")]
        /// 显式公共匿名路由所属的固定租户，匿名客户端不能自选租户。
        tenant_id: String,
        #[serde(rename = "publicRoute")]
        /// 部署对匿名入口为公共能力的明确确认，未确认不能上线匿名路由。
        public_route: bool,
        #[serde(rename = "ttlMs")]
        /// 匿名会话身份的有限存活毫秒数，活动和心跳不能延长。
        ttl_ms: u64,
    },
}

/// 完整路由期望；运行状态与节点分配端口不进入此结构。
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RouteDefinition {
    /// 路由稳定标识，控制、消息、授权和监听绑定共同使用。
    pub route_id: String,
    /// 本定义单调递增的 revision，相同 revision 的不同内容必须拒绝。
    pub revision: u64,
    #[serde(default, skip_serializing_if = "Activation::is_inherited")]
    /// 本路由独立的上线门槛，缺省沿用部署设置，旧定义屏障仍必须完成。
    pub activation: Activation,
    #[serde(
        default,
        skip_serializing_if = "super::handshake::HandshakePolicy::is_empty"
    )]
    /// 转交固定 HTTP 端点的受限握手字段，不授予身份或业务权限。
    pub handshake: super::handshake::HandshakePolicy,
    #[serde(default)]
    /// 期望是否上线，实际开放还需持久提交、旧许可屏障和依赖准备。
    pub enabled: bool,
    /// 业务选择的唯一 ProtocolBytes 模式，各接入协议与 MQ 正文必须一致。
    pub serialization: Serialization,
    /// 共享或独立监听、逻辑入口与适配器能力声明。
    pub access: Access,
    /// 建立真实主体的唯一身份策略，关闭远程验证不能隐式匿名放行。
    pub identity: Identity,
    #[serde(default)]
    /// 默认关闭的客户端发送策略，显式开启仍必须经业务服务授权。
    pub client_messages: ClientMessages,
    #[serde(default)]
    /// 白名单服务器操作能力，不提供通用客户端广播器。
    pub notifications: Notifications,
    #[serde(default)]
    /// 当前连接的群读取申请与恢复策略，不改变业务成员关系。
    pub subscriptions: Subscriptions,
    #[serde(default)]
    /// 业务提供的有限 schema 描述，不能覆盖内置订阅对象。
    #[cfg_attr(
        feature = "schema",
        schemars(with = "BTreeMap<String, crate::config::schema::Descriptor>")
    )]
    pub schemas: BTreeMap<String, SchemaDescriptor>,
    #[serde(default)]
    /// 允许的事件、schema、正文容量及明确状态覆盖语义。
    pub events: Events,
    #[serde(default)]
    /// 身份建立后可选的额外连接业务授权，失败不保留部分准入。
    pub connect_authorization: RemotePolicy,
    #[serde(default)]
    /// 身份刷新与持久业务撤权策略，刷新不能绕过已有撤权下界。
    pub authorization: Authorization,
    #[serde(default)]
    /// 浏览器来源检查策略，与真实身份校验独立执行。
    pub origin_check: OriginCheck,
    #[serde(default)]
    /// 路由连接配额、同用户策略、操作速率和请求关联预算。
    pub connections: Connections,
    #[serde(default)]
    /// 受信业务下行入口及生产者、目标和正文检查策略。
    pub service_push: ServicePush,
    #[serde(default)]
    /// 定向与群读取通道开关，接收能力独立于客户端发送能力。
    pub receiving: Receiving,
    #[serde(default)]
    /// 消息期限、回执、去重、慢消费者与逻辑队列容量。
    pub delivery: Delivery,
    #[serde(default)]
    /// 初始化及连接上下线事实的固定业务交互策略。
    pub lifecycle: Lifecycle,
    #[serde(default)]
    /// 显式旧事件和路径兼容声明，不允许降低身份或订阅授权要求。
    pub compatibility: Compatibility,
}

impl RouteDefinition {
    /// 业务作用：为控制命令确定本定义的激活门槛，下线不依赖在线节点数量。
    /// 参数说明：
    /// - `deployment_default`：已经通过部署配置校验的默认节点数。
    ///
    /// 返回：上线采用路由覆盖或部署默认值；下线保留控制模型的最小合法值。
    pub fn minimum_ready(&self, deployment_default: usize) -> usize {
        if self.enabled {
            self.activation
                .min_ready_nodes
                .unwrap_or(deployment_default)
        } else {
            1
        }
    }

    /// 业务作用：在资源准备前检查路由身份、端口与权限的安全组合。
    /// 参数说明：无。
    ///
    /// 返回：结构合法时成功；拒绝歧义路径、无认证上线及隐含端口覆盖。
    pub fn validate(&self) -> anyhow::Result<()> {
        self.handshake.validate()?;
        if let Identity::TrustedGateway {
            assertion_header,
            ..
        } = &self.identity
        {
            anyhow::ensure!(
                !self.handshake.headers.contains_key(assertion_header),
                "gateway_assertion_forwarding_forbidden"
            );
        }
        anyhow::ensure!(
            self.activation
                .min_ready_nodes
                .is_none_or(|value| (1..=256).contains(&value)),
            "invalid_route_minimum_ready"
        );
        // 历史数据由业务接口提供，路由不能通过切换消费模式恢复节点离线期间的推送。
        anyhow::ensure!(
            self.delivery.mode == super::policies::DeliveryMode::Realtime,
            "route_requires_realtime_delivery"
        );
        anyhow::ensure!(
            !self.route_id.is_empty()
                && self.route_id.len() <= 128
                && self
                    .route_id
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b)),
            "invalid routeId"
        );
        anyhow::ensure!(self.revision > 0, "revision must be positive");
        anyhow::ensure!(
            !self.enabled
                || self.access.native_endpoint().is_some()
                || self.access.socketio_namespace().is_some(),
            "online route requires an enabled adapter"
        );
        anyhow::ensure!(
            self.access.native.wire_profile == "split_envelope"
                && self.access.socketio.wire_layout == "binary_attachment",
            "unsupported wire profile"
        );
        for path in [
            self.access.native_endpoint(),
            self.access.socketio_namespace(),
        ]
        .into_iter()
        .flatten()
        {
            anyhow::ensure!(
                path.starts_with('/')
                    && path.len() <= 256
                    && !path.contains("//")
                    && !path
                        .bytes()
                        .any(|b| b.is_ascii_control() || b"?#%,\\".contains(&b))
                    && !path.split('/').any(|part| part == "." || part == ".."),
                "invalid route endpoint"
            );
        }
        if self.compatibility.legacy_path {
            // 规范化旧传输路径不得使两个 namespace 共用一个别名，也不能把根路径变成默认路由。
            anyhow::ensure!(
                self.access
                    .socketio_namespace()
                    .is_some_and(|namespace| { namespace.len() > 1 && !namespace.ends_with('/') }),
                "legacy path requires an enabled non-root namespace without trailing slash"
            );
        }
        anyhow::ensure!(
            !self.access.listener_ref.is_empty(),
            "listenerRef is required"
        );
        let port = &self.access.dedicated_port;
        anyhow::ensure!(
            port.enabled || port.port.is_none(),
            "shared route cannot specify a port"
        );
        anyhow::ensure!(
            port.port != Some(0),
            "port zero is not a route allocation policy"
        );
        // 无身份来源的定义可保存为下线，不能仅因远程校验关闭而开放匿名接入。
        anyhow::ensure!(
            !self.enabled || self.identity != Identity::Disabled,
            "online route requires identity"
        );
        if let Identity::Remote {
            endpoint_ref,
        } = &self.identity
        {
            anyhow::ensure!(!endpoint_ref.is_empty(), "identity endpointRef is required");
        }
        match &self.identity {
            Identity::LocalJwt {
                verifier_ref,
            }
            | Identity::TrustedGateway {
                verifier_ref,
                ..
            } => crate::domain::message::envelope::identifier(verifier_ref)?,
            Identity::Anonymous {
                tenant_id,
                public_route,
                ttl_ms,
            } => {
                crate::domain::message::envelope::identifier(tenant_id)?;
                anyhow::ensure!(
                    *public_route
                        && (1000..=300_000).contains(ttl_ms)
                        && !self.client_messages.enabled
                        && !self.receiving.direct.enabled
                        && !self.connect_authorization.enabled
                        && !self.authorization.refresh.enabled
                        && !self.lifecycle.initialization.enabled,
                    "anonymous route must remain public and bounded"
                );
                anyhow::ensure!(
                    !self.subscriptions.enabled
                        || matches!(
                            self.subscriptions.authorization,
                            Some(SubscriptionAuthorization::PublicAllowlist { .. })
                        ),
                    "anonymous subscriptions require public allowlist"
                );
                anyhow::ensure!(
                    self.notifications
                        .operations
                        .iter()
                        .all(|operation| !operation.enabled
                            || (operation.action == Action::SelfStatus
                                && matches!(
                                    operation.authorization,
                                    OperationAuthorization::Authenticated {}
                                ))),
                    "anonymous operation denied"
                );
                anyhow::ensure!(
                    self.connections.limit_enabled
                        && self.connections.max_connections <= 10_000
                        && self.connections.max_pending_operations <= 8
                        && self.connections.operations_per_second <= 20,
                    "anonymous quota too broad"
                );
            }
            _ => {}
        }
        if let Identity::TrustedGateway {
            assertion_header,
            trusted_peers,
            ..
        } = &self.identity
        {
            anyhow::ensure!(
                assertion_header.starts_with("x-")
                    && assertion_header.len() <= 64
                    && assertion_header
                        .bytes()
                        .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
                    && !trusted_peers.is_empty()
                    && trusted_peers.len() <= 32,
                "invalid trusted gateway boundary"
            );
        }
        Ok(())
    }
}
