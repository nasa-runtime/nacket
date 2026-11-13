use super::{outbox::Outbox, Connected, Gateway};
use crate::{
    adapters::outbound::identity::{now, Principal},
    config::RemoteEndpoint,
    domain::{
        message::{
            control::{self, Authenticate, Confirm, Hello},
            envelope::{identifier, revision, user_identifier, SessionKey},
            UnixMillis,
        },
        route::{policies::QuotaScope, Identity},
        session::{receipts::Receipts, GroupGrant, IdentityGrant, Session},
    },
};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeSet,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{sync::OwnedSemaphorePermit, time::Instant};
use tokio_util::sync::CancellationToken;

/// 认证开始前发现目录绑定已失效，允许客户端重新读取可信目录。
#[derive(Debug)]
pub(crate) struct StaleBinding;

impl std::fmt::Display for StaleBinding {
    /// 业务作用：仅公开固定目录失效原因，不泄漏当前内部绑定或认证信息。
    /// 参数说明：
    /// - `formatter`：错误文本输出目标。
    ///
    /// 返回：固定原因码写出结果。
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("directory_binding_stale")
    }
}

impl std::error::Error for StaleBinding {}

/// 节点准入或有限控制许可暂不可用；不表示认证结果，也不授予旧会话权限。
#[derive(Debug)]
pub(crate) struct TemporarilyUnavailable;

impl std::fmt::Display for TemporarilyUnavailable {
    /// 业务作用：向终端提供有界重连的固定原因，不暴露控制权威细节。
    /// 参数说明：
    /// - `formatter`：错误文本目标。
    ///
    /// 返回：固定可用性原因码的写出结果。
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("route_unavailable")
    }
}

impl std::error::Error for TemporarilyUnavailable {}

/// 实际 HTTP 握手事实，只收集授权策略明确使用的字段。
#[derive(Clone)]
pub struct Handshake {
    /// 实际 WebSocket 握手 Origin，按路由允许列表核对而不当作身份凭据。
    pub origin: Option<String>,
    /// 真实网络对端描述，用于受信网关和远程认证上下文。
    pub peer: String,
    /// 是否经真实 WebSocket 升级，用于核对路由物理协议能力。
    pub websocket: bool,
    /// 是否已选择 Socket.IO 适配器，不能由认证正文自行切换。
    pub socketio: bool,
    /// 实际监听的冻结作用域，独立端口不能认证到其它路由。
    pub binding: crate::application::listeners::Binding,
    /// 握手收集的允许名称断言 header，使用前仍需验证签名和作用域。
    pub assertions: std::collections::BTreeMap<String, String>,
    /// 由路由允许目录选择的普通请求头候选，认证时再按精确定义筛选。
    pub headers: std::collections::BTreeMap<String, String>,
    /// 显式旧路径入口冻结的路由绑定；标准传输路径和原生接入为空。
    pub legacy_path: Option<LegacyPath>,
}

/// 旧 HTTP 路径只定位已登记定义，不提供身份或订阅权限。
#[derive(Clone)]
pub struct LegacyPath {
    /// 升级时匹配的业务路由 ID，物理连接不能再选择其它路由。
    pub route_id: String,
    /// 升级时冻结的定义 revision，定义变化后必须重新建立物理连接。
    pub revision: u64,
    /// 同一冻结定义的完整摘要，防止路径被重新分配后复用旧握手。
    pub digest: String,
    /// 核心使用的标准 namespace，线协议根 namespace 仅作为受控别名。
    pub namespace: String,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Default, Deserialize)]
#[serde(default, rename_all = "camelCase", deny_unknown_fields)]
pub struct Grants {
    /// 受信授权是否允许接收定向业务消息，缺省不能视为允许。
    pub can_receive_direct: Option<bool>,
    /// 受信授予的客户端操作名集合，仍与路由开关取交集。
    pub allowed_operations: BTreeSet<String>,
    /// 受信提供的订阅资格证据，客户端订阅意图本身不授予资格。
    pub groups: Vec<GroupEvidence>,
    #[serde(skip)]
    /// 本地绑定的单调有效期，不从 JSON 接受客户端指定的 Instant。
    pub deadline: Option<Instant>,
}

impl Grants {
    /// 业务作用：复验连接授权快照的固定期限，心跳和重复使用均不能续期。
    /// 参数说明: 无。
    ///
    /// 返回：来自受信响应且尚未到期时允许使用快照。
    pub fn valid(&self) -> bool {
        self.deadline
            .is_some_and(|deadline| deadline > Instant::now())
    }

    /// 业务作用：校验远程能力集合的边界并绑定实际有效期。
    /// 参数说明：
    /// - `maximum_groups`：路由允许的最大群证据数。
    /// - `deadline`：身份和本次授权共同限定的单调截止时间。
    ///
    /// 返回：有界快照；重复群或非法操作名不会进入会话。
    pub fn bind(&mut self, maximum_groups: usize, deadline: Instant) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.groups.len() <= maximum_groups && self.allowed_operations.len() <= 128,
            "grant_capacity"
        );
        let mut groups = BTreeSet::new();
        for group in &self.groups {
            identifier(&group.group_id)?;
            anyhow::ensure!(groups.insert(&group.group_id), "duplicate_group_grant");
        }
        for operation in &self.allowed_operations {
            identifier(operation)?;
        }
        self.deadline = Some(deadline);
        Ok(())
    }
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct GroupEvidence {
    /// 本条资格唯一允许订阅的目标标识。
    pub group_id: String,
    /// 业务授权证据 ID，用于关联并校验成员资格来源。
    pub authorization_id: String,
    /// 该主体在此订阅目标上的十进制成员授权版本。
    pub authorization_version: String,
    /// 订阅目标自身状态的十进制版本，与成员授权版本独立。
    pub group_version: String,
    /// 本条接收资格的 Unix 毫秒截止点，不能超过身份期限。
    pub expires_at: UnixMillis,
    /// 业务侧明确授予的接收权限，false 不能建立有效订阅。
    pub can_receive: bool,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AuthorizationResponse {
    /// 必须精确回显当前远程请求 ID，迟到或其它请求的响应拒绝。
    pub request_id: String,
    /// 明确 allow 或 deny 的授权判定，未知值拒绝。
    pub decision: String,
    /// 业务确认的真实主体，刷新时不能切换用户或租户。
    pub principal: Option<Principal>,
    /// 允许结果的授权证据 ID，缺失不能建立有效身份。
    pub authorization_id: Option<String>,
    /// 受信身份授权的十进制版本，必须高于持久撤权下界。
    pub authorization_version: Option<String>,
    /// 允许结果的 Unix 毫秒截止点，受端点最大 TTL 限制。
    pub expires_at: Option<UnixMillis>,
    #[serde(default)]
    /// 本次允许结果附带的有限操作及订阅能力。
    pub grants: Grants,
    /// 可选有限长度业务结果码，不能替代明确授权判定。
    pub code: Option<String>,
    /// 可选有限长度结果说明，不包含执行指令或授权材料。
    pub message: Option<String>,
}

/// 完整服务端上下文不接受客户端提供的租户、节点或身份覆盖。
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CallContext<'a> {
    #[serde(flatten)]
    /// 服务端生成的完整精确会话作用域，不能由客户端字段覆盖。
    pub key: &'a SessionKey,
    /// 对应本次握手尝试的关联值，防止初始化响应串入重连会话。
    pub attempt_id: &'a str,
}

/// 尚未确认的会话持有容量，但不会出现在可投递目录中。
pub struct PendingSession {
    /// 尚未 Ready 的握手观察责任，确认与初始化沿用同一关联和阶段。
    pub(crate) handshake_attempt: Arc<super::handshake::attempt::Attempt>,
    /// 确认期间仍保留的受限握手上下文，失败随待决会话释放。
    facts: super::handshake::Facts,
    /// 监听器确定的网络对端，确认后原样交给连接日志。
    peer: String,
    /// 握手确认的传输类型，不从认证正文取值。
    transport: &'static str,
    /// 身份来源核实后固定的本地消费键，后续 Confirm 不允许改用 session ID 分桶。
    pub uid_events: crate::application::uid_events::Lane,
    /// 已认证但尚未完成确认和初始化的会话门禁。
    pub session: Arc<Session>,
    /// 与本次 Hello 相同的冻结路由编译计划。
    pub plan: Arc<crate::domain::route::compiled::CompiledRoute>,
    /// 本次协商返回的公开能力与挑战信息，不含内部控制材料。
    pub hello: Hello,
    /// 初次身份校验得到的有限能力，确认时仍需复验有效期。
    pub grants: Grants,
    /// 双方协商的完整帧字节上限，后续入站和出站同时遵守。
    pub maximum_frame: usize,
    /// 原身份凭据的单调截止点，确认不得刷新它。
    credential_deadline: Option<Instant>,
    /// 尚未确认也必须持有的节点逻辑会话名额。
    capacity: OwnedSemaphorePermit,
}

impl AuthorizationResponse {
    /// 业务作用：校验授权响应关联和固定期限，拒绝任何含糊或过期的允许结果。
    /// 参数说明：
    /// - `request_id`：发起请求时保存的关联值。
    /// - `endpoint`：固定端点的最大授权 TTL。
    /// - `started`：调用前的单调时刻。
    /// - `wall`：调用前的整数毫秒时钟。
    ///
    /// 返回：授权 ID、单调版本和保守截止时间；不授予额外主体或群权限。
    pub fn evidence(
        &self,
        request_id: &str,
        endpoint: &RemoteEndpoint,
        started: Instant,
        wall: UnixMillis,
    ) -> anyhow::Result<(String, u64, Instant)> {
        self.evidence_with_ttl(request_id, endpoint.max_identity_ttl_ms, started, wall)
    }

    /// 业务作用：为 HTTP 与受信 MQ 授权复用关联、单调版本及期限校验，不借运输方式放宽证据。
    /// 参数说明：
    /// - `request_id`：首次发起时保存的请求标识。
    /// - `maximum_ttl_ms`：冻结策略允许的最大授权寿命。
    /// - `started`：首次发起时的单调时刻。
    /// - `wall`：首次发起时的整数毫秒时钟。
    ///
    /// 返回：有界授权证据；错误关联、拒绝、时钟回退和过期均不产生权限。
    pub fn evidence_with_ttl(
        &self,
        request_id: &str,
        maximum_ttl_ms: u64,
        started: Instant,
        wall: UnixMillis,
    ) -> anyhow::Result<(String, u64, Instant)> {
        anyhow::ensure!(
            self.code.as_ref().is_none_or(|v| v.len() <= 128)
                && self.message.as_ref().is_none_or(|v| v.len() <= 1024),
            "authorization_response_capacity"
        );
        anyhow::ensure!(
            self.request_id == request_id,
            "authorization_response_mismatch"
        );
        anyhow::ensure!(self.decision != "deny", "permission_denied");
        anyhow::ensure!(self.decision == "allow", "authorization_decision_invalid");
        let id = self
            .authorization_id
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("authorization_missing"))?;
        identifier(id)?;
        let version = revision(
            self.authorization_version
                .as_deref()
                .ok_or_else(|| anyhow::anyhow!("authorization_missing"))?,
        )?;
        let expiry = self
            .expires_at
            .ok_or_else(|| anyhow::anyhow!("authorization_missing"))?;
        let current = now()?;
        anyhow::ensure!(current >= wall && expiry > current, "authorization_expired");
        let remaining = expiry.as_millis() - current.as_millis();
        anyhow::ensure!(remaining <= maximum_ttl_ms, "authorization_ttl");
        let deadline = started + Duration::from_millis(remaining);
        anyhow::ensure!(deadline > Instant::now(), "authorization_expired");
        Ok((id.into(), version, deadline))
    }
}

impl GroupEvidence {
    /// 业务作用：把受信群证据转换为有限单调授权，客户端订阅意图本身不提供资格。
    /// 参数说明：
    /// - `maximum_ttl`：该路由群资格最长存活毫秒数。
    /// - `identity_deadline`：身份有效期，不允许群证据超越身份。
    /// - `started`：远程请求开始时的单调时刻。
    /// - `wall`：请求开始时的整数毫秒时钟。
    ///
    /// 返回：只含合法读取权限的授权记录，版本、关联和期限异常均拒绝。
    pub fn grant(
        &self,
        maximum_ttl: u64,
        identity_deadline: Instant,
        started: Instant,
        wall: UnixMillis,
    ) -> anyhow::Result<GroupGrant> {
        identifier(&self.group_id)?;
        identifier(&self.authorization_id)?;
        let current = now()?;
        anyhow::ensure!(
            self.can_receive && current >= wall && self.expires_at > current,
            "subscription_denied"
        );
        let remaining = self.expires_at.as_millis() - current.as_millis();
        anyhow::ensure!(remaining <= maximum_ttl, "subscription_ttl");
        let deadline = (started + Duration::from_millis(remaining)).min(identity_deadline);
        anyhow::ensure!(deadline > Instant::now(), "subscription_expired");
        Ok(GroupGrant {
            authorization_id: self.authorization_id.clone(),
            version: revision(&self.authorization_version)?,
            group_version: revision(&self.group_version)?,
            deadline,
        })
    }
}

impl Gateway {
    /// 业务作用：在接流前准备冻结路由需要的公钥来源，网络失败不产生可用身份策略。
    /// 参数说明：
    /// - `candidate`：配置变更时的候选部署，启动恢复时为 None。
    ///
    /// 返回：相关验签策略均已取得可用密钥；失败保持旧定义或启动保护态。
    pub async fn prepare_identities(
        &self,
        candidate: Option<&crate::config::Deployment>,
    ) -> anyhow::Result<()> {
        let definitions = match candidate {
            Some(candidate) => candidate.routes.clone(),
            None => self.routes.definitions(),
        };
        for definition in definitions
            .into_iter()
            .filter(|definition| definition.enabled)
        {
            let plan = match candidate {
                Some(candidate) => {
                    crate::domain::route::compiled::CompiledRoute::compile_with_verifiers(
                        definition,
                        &candidate.endpoints,
                        &candidate.verifiers,
                    )?
                }
                None => {
                    self.routes
                        .by_id(&definition.route_id)
                        .ok_or_else(|| anyhow::anyhow!("route missing"))?
                        .0
                }
            };
            for verifier in plan.verifiers.values() {
                self.verifiers
                    .prepare(
                        verifier,
                        &self.calls,
                        Instant::now() + Duration::from_secs(30),
                    )
                    .await?;
            }
        }
        Ok(())
    }

    /// 业务作用：依照路由唯一身份模式取得真实主体，代理声明必须验签且绑定当前入口。
    /// 参数说明：
    /// - `plan`：已冻结的业务定义与远程引用。
    /// - `request`：本连接的有限凭据和能力。
    /// - `handshake`：真实网络来源及受控代理头。
    /// - `facts`：当前路由批准的有限字段投影。
    /// - `key`：服务端生成的完整会话作用域。
    /// - `deadline`：从 accept 起固定的认证总截止时间。
    ///
    /// 返回：可信主体、授权标识和期限；公开匿名身份不继承用户定向或私有群权限。
    async fn identity(
        &self,
        plan: &crate::domain::route::compiled::CompiledRoute,
        request: &Authenticate,
        handshake: &Handshake,
        facts: &super::handshake::Facts,
        key: &SessionKey,
        deadline: Instant,
    ) -> anyhow::Result<(Principal, String, u64, Instant, UnixMillis, Grants)> {
        let strategy = match &plan.definition.identity {
            Identity::Disabled => return Err(anyhow::anyhow!("identity_unavailable")),
            Identity::Remote {
                ..
            } => "remote",
            Identity::LocalJwt {
                ..
            } => "local_jwt",
            Identity::TrustedGateway {
                ..
            } => "trusted_gateway",
            Identity::Anonymous {
                ..
            } => "anonymous",
        };
        self.metrics.measure(
            crate::observability::Counter::Identity,
            strategy,
            "authorization",
            "allow",
            async {
                let evidence: anyhow::Result<_> = match &plan.definition.identity {
                    Identity::Disabled => anyhow::bail!("identity_unavailable"),
                    Identity::Remote {
                        endpoint_ref,
                    } => {
                        let endpoint = plan
                            .endpoints
                            .get(endpoint_ref)
                            .ok_or_else(|| anyhow::anyhow!("identity_unavailable"))?;
                        let request_id = control::nonce();
                        let started = Instant::now();
                        let wall = now()?;
                        let input = serde_json::json!({
                            "requestId": request_id,
                            "operation": "authenticate",
                            "context": CallContext {
                                key,
                                attempt_id: &request.attempt_id
                            },
                            "credentials": {
                                "token": request.token.as_deref().ok_or_else(||anyhow::anyhow!("credentials_required"))?
                            }
                        });
                        let response: AuthorizationResponse =
                            self.calls.call(endpoint_ref, endpoint, &facts.input(&input), deadline).await?;
                        let (id, version, limit) = response.evidence(&request_id, endpoint, started, wall)?;
                        Ok((
                            response
                                .principal
                                .ok_or_else(|| anyhow::anyhow!("principal_missing"))?,
                            id,
                            version,
                            limit,
                            response.expires_at.expect("validated expiry"),
                            response.grants,
                        ))
                    }
                    Identity::LocalJwt {
                        verifier_ref,
                    }
                    | Identity::TrustedGateway {
                        verifier_ref,
                        ..
                    } => {
                        let verifier = plan
                            .verifiers
                            .get(verifier_ref)
                            .ok_or_else(|| anyhow::anyhow!("identity_unavailable"))?;
                        let token = if let Identity::TrustedGateway {
                            assertion_header,
                            trusted_peers,
                            ..
                        } = &plan.definition.identity
                        {
                            anyhow::ensure!(
                                handshake.websocket
                                    && request.token.is_none()
                                    && trusted_peers.contains(&handshake.peer.parse()?),
                                "gateway_boundary_denied"
                            );
                            handshake
                                .assertions
                                .get(assertion_header)
                                .map(String::as_str)
                                .ok_or_else(|| anyhow::anyhow!("gateway_assertion_required"))?
                        } else {
                            request
                                .token
                                .as_deref()
                                .ok_or_else(|| anyhow::anyhow!("credentials_required"))?
                        };
                        let evidence = self
                            .verifiers
                            .verify(verifier, &self.calls, token, deadline)
                            .await?;
                        if matches!(plan.definition.identity, Identity::TrustedGateway { .. }) {
                            let expected = serde_json::json!({
                                "clusterId": key.cluster_id,
                                "scopeEpoch": key.scope_epoch,
                                "routeId": key.route_id,
                                "listenerRef": handshake.binding.listener_ref,
                                "endpoint": request.endpoint
                            });
                            anyhow::ensure!(
                                evidence.claims.get("socketContext") == Some(&expected),
                                "gateway_assertion_scope"
                            );
                            let replay = serde_json::to_string(&(
                                &verifier.issuer,
                                &verifier.audience,
                                &key.cluster_id,
                                &key.scope_epoch,
                                &key.route_id,
                                &evidence.authorization_id,
                            ))?;
                            self.consume_assertion(
                                &replay,
                                key,
                                &request.attempt_id,
                                evidence.expires_at,
                                deadline.min(evidence.deadline),
                            )
                            .await?;
                        }
                        Ok((
                            evidence.principal,
                            evidence.authorization_id,
                            evidence.version,
                            evidence.deadline,
                            evidence.expires_at,
                            Grants::default(),
                        ))
                    }
                    Identity::Anonymous {
                        tenant_id,
                        ttl_ms,
                        ..
                    } => {
                        anyhow::ensure!(request.token.is_none(), "anonymous_credentials_denied");
                        let expires = now()?
                            .as_millis()
                            .checked_add(*ttl_ms)
                            .ok_or_else(|| anyhow::anyhow!("expiry_overflow"))?;
                        Ok((
                            Principal {
                                uid: format!("anonymous-{}", control::nonce()),
                                tenant_id: tenant_id.clone(),
                            },
                            control::nonce(),
                            1,
                            Instant::now() + Duration::from_millis(*ttl_ms),
                            UnixMillis::new(expires).map_err(anyhow::Error::msg)?,
                            Grants {
                                can_receive_direct: Some(false),
                                ..Default::default()
                            },
                        ))
                    }
                };
                let evidence = evidence?;
                // 可信来源的 allow 不能替代主体完整性校验；无效 uid 不得进入后续授权或会话创建。
                user_identifier(&evidence.0.uid)?;
                identifier(&evidence.0.tenant_id)?;
                Ok(evidence)
            },
        ).await
    }
    /// 业务作用：验证入口、来源和可信身份，生成绑定冻结定义的一次性确认材料。
    /// 参数说明：
    /// - `request`：未认证客户端的有限能力和凭据。
    /// - `handshake`：监听器确定的真实传输及来源。
    /// - `deadline`：accept 时固定的总认证截止点。
    ///
    /// 返回：待确认会话；任何远程失败、路由变化或配额不足均保持未就绪。
    pub async fn authenticate(
        self: &Arc<Self>,
        request: Authenticate,
        handshake: &Handshake,
        deadline: Instant,
    ) -> anyhow::Result<PendingSession> {
        let transport = if handshake.socketio {
            "socketio"
        } else if handshake.websocket {
            "websocket"
        } else {
            "tcp"
        };
        let attempt = super::handshake::attempt::Attempt::new(self, transport, deadline);
        let result =
            self.metrics
                .measure(
                    crate::observability::Counter::Operations,
                    "authenticate",
                    "handshake",
                    "ok",
                    async {
                        // 必要注册或运行依赖失效时先停止新认证；既有会话继续受各自路由和身份期限约束。
                        request.validate()?;
                        attempt.identify(&request.attempt_id);
                        attempt.stage("route_admission");
                        anyhow::ensure!(self.application.is_ready(), TemporarilyUnavailable);
                        let capacity = self
                            .capacity
                            .clone()
                            .try_acquire_owned()
                            .map_err(|_| anyhow::anyhow!("connection_capacity"))?;
                        let (plan, gate) = self
                            .routes
                            .resolve(
                                &request.endpoint,
                                handshake.socketio,
                                handshake.binding.route_id.as_deref(),
                                &handshake.binding.listener_ref,
                            )
                            .ok_or_else(|| {
                                if request.expected_binding.is_some() {
                                    anyhow::Error::new(StaleBinding)
                                } else {
                                    anyhow::anyhow!("route_unavailable")
                                }
                            })?;
                        if let Some(legacy) = &handshake.legacy_path {
                            // 旧路径只能收紧到升级时的精确定义；开关撤销或路径重分配后不能把凭据交给新目标。
                            anyhow::ensure!(
                                handshake.socketio
                                    && request.endpoint == legacy.namespace
                                    && plan.definition.route_id == legacy.route_id
                                    && plan.definition.revision == legacy.revision
                                    && plan.digest == legacy.digest
                                    && plan.definition.compatibility.legacy_path,
                                "legacy_path_binding_changed"
                            );
                        }
                        if let Some(expected) = &request.expected_binding {
                            // 目录材料只能收紧目标；旧端口、旧进程或旧定义必须在发送身份凭据前拒绝。
                            anyhow::ensure!(
                                expected.cluster_id == self.configuration.cluster_id
                                    && expected.scope_epoch == self.configuration.scope_epoch
                                    && expected.node_id == self.configuration.node_id
                                    && self.incarnation.get() == Some(&expected.incarnation)
                                    && expected.route_id == plan.definition.route_id
                                    && expected.route_revision
                                        == plan.definition.revision.to_string()
                                    && expected.definition_digest == plan.digest
                                    && expected.binding_id == handshake.binding.binding_id,
                                StaleBinding
                            );
                        }
                        anyhow::ensure!(
                            handshake.binding.access.as_ref().is_none_or(|access| access
                                .listener_ref
                                == plan.definition.access.listener_ref
                                && access.dedicated_port == plan.definition.access.dedicated_port),
                            "listener_binding_changed"
                        );
                        if gate.acquire(plan.definition.revision).is_none() {
                            // 只有有限许可过期属于可恢复保护态；定义撤销和路由关闭仍是终止条件。
                            if gate.unavailable_reason() == Some("route_permit_expired") {
                                return Err(TemporarilyUnavailable.into());
                            }
                            anyhow::bail!("route_unavailable");
                        }
                        anyhow::ensure!(
                            request
                                .capabilities
                                .serializations
                                .contains(&plan.definition.serialization),
                            "unsupported_serialization"
                        );
                        anyhow::ensure!(
                            !handshake.socketio || request.capabilities.binary_attachments,
                            "binary_attachments_required"
                        );
                        let origin = &plan.definition.origin_check;
                        if origin.enabled {
                            anyhow::ensure!(
                                match &handshake.origin {
                                    Some(value) => origin.allowed_origins.contains(value),
                                    None => origin.allow_missing,
                                },
                                "origin_denied"
                            );
                        }
                        let facts = super::handshake::Facts::new(
                            self,
                            &plan.definition.handshake,
                            &request,
                            handshake,
                        )?;
                        let key = SessionKey {
                            cluster_id: self.configuration.cluster_id.clone(),
                            scope_epoch: self.configuration.scope_epoch.clone(),
                            node_id: self.configuration.node_id.clone(),
                            incarnation: self
                                .incarnation
                                .get()
                                .ok_or_else(|| anyhow::anyhow!("node_not_registered"))?
                                .clone(),
                            route_id: plan.definition.route_id.clone(),
                            route_revision: plan.definition.revision.to_string(),
                            definition_digest: plan.digest.clone(),
                            route_session_id: control::nonce(),
                        };
                        attempt.bind(&key, &gate);
                        attempt.stage("identity");
                        let (
                            principal,
                            mut id,
                            mut version,
                            mut identity_deadline,
                            mut expires_at,
                            mut grants,
                        ) = self
                            .identity(&plan, &request, handshake, &facts, &key, deadline)
                            .await?;
                        let credential_deadline =
                            (!matches!(plan.definition.identity, Identity::Remote { .. }))
                                .then_some(identity_deadline);
                        user_identifier(&principal.uid)?;
                        identifier(&principal.tenant_id)?;
                        attempt.principal(&principal.tenant_id, &principal.uid);
                        attempt.stage("connect_authorization");
                        if plan.definition.connect_authorization.enabled {
                            self.metrics
                                .measure(
                                    crate::observability::Counter::Authorization,
                                    "connect",
                                    "authorization",
                                    "allow",
                                    async {
                                        let endpoint = plan
                                            .endpoints
                                            .get(
                                                plan.definition
                                                    .connect_authorization
                                                    .endpoint_ref
                                                    .as_deref()
                                                    .unwrap_or(""),
                                            )
                                            .ok_or_else(|| {
                                                anyhow::anyhow!("authorization_unavailable")
                                            })?;
                                        let request_id = control::nonce();
                                        let started = Instant::now();
                                        let wall = now()?;
                                        let input = serde_json::json!({
                                            "requestId": request_id,
                                            "operation": "connect",
                                            "context": CallContext {
                                                key: &key,
                                                attempt_id: &request.attempt_id
                                            },
                                            "principal": {
                                                "uid": principal.uid,
                                                "tenantId": principal.tenant_id
                                            }
                                        });
                                        let response: AuthorizationResponse = self
                                            .calls
                                            .call(
                                                plan.definition
                                                    .connect_authorization
                                                    .endpoint_ref
                                                    .as_deref()
                                                    .unwrap_or(""),
                                                endpoint,
                                                &facts.input(&input),
                                                deadline,
                                            )
                                            .await?;
                                        let (connect_id, connect_version, limit) = response
                                            .evidence(&request_id, endpoint, started, wall)?;
                                        anyhow::ensure!(
                                            response.principal.is_none(),
                                            "unexpected_principal"
                                        );
                                        identity_deadline = identity_deadline.min(limit);
                                        expires_at = expires_at
                                            .min(response.expires_at.expect("validated expiry"));
                                        id = connect_id;
                                        version = connect_version;
                                        grants = response.grants;

                                        Ok(())
                                    },
                                )
                                .await?;
                        }
                        grants.bind(
                            plan.definition.subscriptions.max_per_session,
                            identity_deadline,
                        )?;
                        attempt.stage("identity_handoff");
                        let lane = crate::application::uid_events::Lane::new(
                            self.application.clone(),
                            &principal.tenant_id,
                            &principal.uid,
                            &self.configuration.transport.uid_events,
                            self.uid_event_budget.clone(),
                        )?;
                        let owned_lane = lane.clone();
                        let gateway = self.clone();
                        let peer = handshake.peer.clone();
                        let handshake_attempt = attempt.clone();
                        lane.submit(deadline, move || {
                            // 身份网络等待期间可能下线；迟到 allow 必须重新检查原定义而不能查询并套用新定义。
                            anyhow::ensure!(
                                gate.acquire(plan.definition.revision).is_some()
                                    && Instant::now() < deadline
                                    && gateway.application.is_ready(),
                                "route_changed"
                            );
                            let session = Session::new(
                                key,
                                IdentityGrant {
                                    tenant: principal.tenant_id,
                                    uid: principal.uid,
                                    authorization_id: id,
                                    version,
                                    deadline: identity_deadline,
                                },
                                gate,
                                gateway.revocations(&plan.definition.route_id),
                                plan.definition.subscriptions.max_per_session,
                            )?;
                            let maximum_frame = request
                                .capabilities
                                .max_frame_bytes
                                .min(gateway.configuration.transport.max_frame_bytes);
                            let hello = Hello {
                                contract: control::CONTRACT,
                                attempt_id: request.attempt_id,
                                session_id: session.key.route_session_id.clone(),
                                confirm_nonce: control::nonce(),
                                expires_at,
                                route: plan.public(),
                            };
                            let size = serde_json::to_vec(&hello)?.len();
                            anyhow::ensure!(
                                size <= gateway.configuration.transport.max_metadata_bytes
                                    && size + 11 <= maximum_frame,
                                "descriptor_capacity"
                            );
                            Ok(PendingSession {
                                handshake_attempt,
                                facts,
                                peer,
                                transport,
                                uid_events: owned_lane,
                                session,
                                plan,
                                hello,
                                grants,
                                maximum_frame,
                                credential_deadline,
                                capacity,
                            })
                        })?
                        .wait()
                        .await
                    },
                )
                .await;
        if let Err(error) = &result {
            attempt.reject(error);
        } else {
            attempt.stage("hello_write");
        }
        result
    }

    /// 业务作用：确认本次定义并登记会话容量，重复或跨连接确认不能建立新的权限。
    /// 参数说明：
    /// - `pending`：同一物理连接保存的唯一待决认证。
    /// - `confirmation`：客户端复述的随机材料与摘要。
    /// - `cancellation`：当前物理传输取消域。
    /// - `deadline`：物理接入时已经冻结的认证截止点。
    ///
    /// 返回：尚待初始化完成的连接和唯一 writer 队列；拒绝时释放待决容量。
    pub async fn confirm(
        self: &Arc<Self>,
        pending: PendingSession,
        confirmation: Confirm,
        cancellation: CancellationToken,
        deadline: Instant,
    ) -> anyhow::Result<(Arc<Connected>, super::queue::Receiver)> {
        let attempt = pending.handshake_attempt.clone();
        attempt.stage("confirm_admission");
        let identity = pending.session.identity();
        let lane = pending.uid_events.clone();
        let gateway = self.clone();
        let result = async {
            lane.submit(deadline, move || {
                anyhow::ensure!(!cancellation.is_cancelled(), "confirmation_cancelled");
                gateway.confirm_consumed(pending, confirmation, cancellation)
            })?
            .wait()
            .await
        }
        .await;
        if let Err(error) = &result {
            attempt.same_uid_sessions(
                self.sessions
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .values()
                    .filter(|connection| {
                        let other = connection.session.identity();
                        other.tenant == identity.tenant && other.uid == identity.uid
                    })
                    .count(),
            );
            attempt.reject(error);
        }
        result
    }

    /// 业务作用：在已认证主体分区内复验 Confirm 并登记唯一逻辑会话，接纳失败不开放能力。
    /// 参数说明：
    /// - `pending`：原尝试保留的身份与路由证据。
    /// - `confirmation`：当前尝试返回的定义与随机材料。
    /// - `cancellation`：真实连接关闭信号。
    ///
    /// 返回：尚未初始化的连接及独占接收队列，重复或迟到确认拒绝。
    fn confirm_consumed(
        &self,
        pending: PendingSession,
        confirmation: Confirm,
        cancellation: CancellationToken,
    ) -> anyhow::Result<(Arc<Connected>, super::queue::Receiver)> {
        anyhow::ensure!(self.application.is_ready(), "admission_unavailable");
        anyhow::ensure!(
            confirmation.attempt_id == pending.hello.attempt_id
                && confirmation.confirm_nonce == pending.hello.confirm_nonce
                && confirmation.definition_digest == pending.plan.digest,
            "confirmation_mismatch"
        );
        let mut sessions = self.sessions.lock().unwrap_or_else(|e| e.into_inner());
        // 与停机清空目录共用锁内复验，防止早先通过的确认在关闭之后重新插入会话。
        anyhow::ensure!(
            !self.closing.load(std::sync::atomic::Ordering::Acquire),
            "admission_unavailable"
        );
        let policy = &pending.plan.definition.connections;
        let quota = if policy.scope == QuotaScope::Cluster {
            anyhow::ensure!(self.control.get().is_some(), "cluster_quota_unavailable");
            super::quotas::Admission::Cluster(self.quotas.register(
                pending.session.clone(),
                cancellation.clone(),
                &self.metrics,
            )?)
        } else {
            super::quotas::Admission::Node(self.local_quotas.register(
                pending.session.clone(),
                cancellation.clone(),
                policy,
                self.configuration.transport.max_sessions,
            )?)
        };
        let delivery = &pending.plan.definition.delivery;
        let (mut outbox, receiver) = Outbox::new(
            delivery.max_queue_messages,
            delivery.max_queue_bytes,
            self.outbound_bytes.clone(),
            pending.maximum_frame,
            self.configuration.transport.max_metadata_bytes,
        );
        outbox.observe(self.metrics.clone());
        let _ = pending
            .session
            .notices
            .set(crate::domain::session::notices::Notices::new(
                crate::domain::message::control::PublicContext {
                    route_id: pending.session.key.route_id.clone(),
                    route_revision: pending.session.key.route_revision.clone(),
                    definition_digest: pending.plan.digest.clone(),
                },
                pending.hello.attempt_id.clone(),
                pending.session.key.route_session_id.clone(),
                self.control_output.clone(),
            ));
        let connection = Arc::new(Connected {
            handshake_attempt: pending.handshake_attempt,
            authorization_cache: super::authorization_cache::Cache::new(
                pending.plan.definition.authorization.cache.clone(),
                self.authorization_cache_budget.clone(),
            ),
            facts: pending.facts,
            refresh_schedule: Mutex::new(super::refresh::Schedule::new(
                &pending.session.key.route_session_id,
            )),
            automatic_refresh: self.refreshes.get().map(Arc::downgrade).unwrap_or_default(),
            peer: pending.peer,
            transport: pending.transport,
            disconnect_queued: std::sync::atomic::AtomicBool::new(false),
            uid_events: pending.uid_events,
            metrics: self.metrics.clone(),
            session: pending.session,
            plan: pending.plan.clone(),
            receipts: Arc::new(Receipts::new(delivery.client_ack.max_pending)),
            outbox,
            cancellation,
            control_operations: Arc::new(tokio::sync::Semaphore::new(2)),
            operations: Arc::new(tokio::sync::Semaphore::new(policy.max_pending_operations)),
            requests: super::requests::RequestTable::new(
                policy.requests.clone(),
                self.request_records.clone(),
                self.request_cache_bytes.clone(),
            ),
            grants: Mutex::new(pending.grants),
            refresh: tokio::sync::Mutex::new(()),
            credential_deadline: pending.credential_deadline,
            attempt_id: pending.hello.attempt_id,
            wire_ready: std::sync::atomic::AtomicBool::new(false),
            lifecycle_phase: Mutex::new(super::lifecycle::Phase::Initializing),
            observed: Mutex::new(Some(self.metrics.reserve_route(
                &pending.plan.definition.route_id,
                "sessions_initializing",
                1,
            ))),
            quota,
            _capacity: pending.capacity,
        });
        connection.receipts.observe(self.metrics.clone());
        connection.requests.observe(self.metrics.clone());
        connection.session.observe(self.metrics.clone());
        connection.sync_receiving();
        sessions.insert(
            connection.session.key.route_session_id.clone(),
            connection.clone(),
        );
        Ok((connection, receiver))
    }
}
