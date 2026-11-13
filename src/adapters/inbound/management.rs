use crate::{
    application::{control::Authority, gateway::Gateway, listeners::Listeners},
    config::control::Principal,
    domain::{
        control::{Command, Definition},
        message::{
            envelope::{identifier, revision},
            json,
        },
        route::compiled::CompiledRoute,
    },
};
use nasa::web::{IntoResponse, Json, Request, Response, StatusCode};
use serde::Deserialize;
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, BTreeSet},
    net::SocketAddr,
    sync::Arc,
};

struct Credential {
    /// 管理 Bearer 凭据的比较摘要，不保存到公开响应。
    digest: String,
    /// 此管理身份允许使用的控制签名密钥 ID。
    control_key: String,
    /// 只读身份标志，开启时在解析变更正文之前拒绝非 GET 请求。
    read_only: bool,
    /// 该身份允许管理的路由集合，签名有效也不能扩大范围。
    routes: BTreeSet<String>,
}
struct Management {
    /// 持久控制命令入口，HTTP 受理不能绕过权威提交。
    authority: Arc<Authority>,
    /// 当前节点路由和会话事实的只读查询来源。
    gateway: Arc<Gateway>,
    /// 实际绑定及公共接入声明目录，不把配置端口冒充已绑定资源。
    listeners: Arc<Listeners>,
    /// 部署冻结的有限管理身份目录，不接受请求动态注册身份。
    credentials: BTreeMap<String, Credential>,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct Proposal {
    /// 业务管理员指定的稳定操作 ID，未知结果应以同 ID 查询或重试。
    operation_id: String,
    /// 要求当前权威定义仍匹配的十进制 revision，防止覆盖并发变更。
    expected_revision: String,
    /// 候选完整定义，在提交前严格解析并核对依赖与权限。
    #[cfg_attr(
        feature = "schema",
        schemars(schema_with = "proposal_definition_schema")
    )]
    definition: Value,
    #[serde(default)]
    /// 可选的候选摘要，提供时必须与规范化定义一致。
    definition_digest: Option<String>,
}
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct Change {
    /// 本次启停或状态变更的稳定幂等操作 ID。
    operation_id: String,
    /// 请求所依据的十进制路由 revision，过时请求拒绝。
    expected_revision: String,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct OwnershipChange {
    /// 归属变更的稳定操作 ID，重试不能产生第二次转移。
    operation_id: String,
    /// 转移时要求匹配的路由定义 revision。
    expected_revision: String,
    /// 转移时要求匹配的归属 revision，与定义 revision 独立。
    expected_owner_revision: String,
    #[serde(default)]
    /// 目标归属主体，按转移动作核对是否必须提供。
    owner: Option<String>,
    #[serde(default)]
    /// 目标归属类别，必须与目标主体和动作合同一致。
    owner_kind: Option<crate::domain::control::OwnerKind>,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct Retirement {
    /// 节点退役的稳定操作 ID，保留未知提交结果的查询关联。
    operation_id: String,
    /// 必须精确匹配的节点代际，旧请求不能退役重启后的实例。
    expected_incarnation: String,
}

/// 业务作用：公开管理提案实际接受的定义结构，路由 revision 始终由当前 CAS 派生。
/// 参数说明：
/// - `generator`：共享本提案所需嵌套 schema 的生成宿主。
///
/// 返回：完整路由字段结构，其中调用方 revision 可省略且不会改变服务端分配结果。
#[cfg(feature = "schema")]
fn proposal_definition_schema(generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
    use schemars::JsonSchema;
    let mut schema = crate::domain::route::RouteDefinition::json_schema(generator);
    let object = schema.ensure_object();
    if let Some(required) = object.get_mut("required").and_then(Value::as_array_mut) {
        required.retain(|name| name != "revision");
    }
    if let Some(properties) = object.get_mut("properties").and_then(Value::as_object_mut) {
        properties.insert(
            "revision".into(),
            json!({
                "description": "可省略；服务端始终用 expectedRevision + 1 替换调用方值。"
            }),
        );
    }
    schema
}

/// 业务作用：在 napp 的独立受管 Web listener 上登记有界管理入口，默认不开启网络服务。
/// 参数说明：
/// - `application`：HTTP 监听与请求治理的唯一宿主。
/// - `authority`：经过签名来源校验和持久提交的控制入口。
/// - `gateway`：当前产品配置与本地路由投影。
/// - `listeners`：受管业务监听目录。
/// - `tree`：同一 naml 快照的完整部署配置。
///
/// 返回：管理身份和边界全部验证后才登记路由；未开启时不加载凭据。
pub fn install(
    application: &nasa::Application,
    authority: Arc<Authority>,
    gateway: Arc<Gateway>,
    listeners: Arc<Listeners>,
    tree: &Value,
) -> anyhow::Result<()> {
    let config = &gateway.configuration.management;
    if !config.enabled {
        return Ok(());
    }
    let host: std::net::IpAddr = tree
        .pointer("/server/host")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("management_bind_required"))?
        .parse()?;
    let port = tree
        .pointer("/server/port")
        .and_then(Value::as_u64)
        .ok_or_else(|| anyhow::anyhow!("management_port_required"))?;
    anyhow::ensure!(
        port > 0
            && port <= 65535
            && port != listeners.primary().0.port() as u64
            && !host.is_multicast(),
        "management_port_invalid"
    );
    if let Some(bind) = tree
        .pointer("/grafana/observability/prometheus/scrape/bind")
        .and_then(Value::as_str)
    {
        anyhow::ensure!(
            bind.parse::<SocketAddr>()?.port() as u64 != port,
            "management_monitoring_port_conflict"
        );
    }
    anyhow::ensure!(
        tree.pointer("/server/request_body_limit_bytes")
            .and_then(Value::as_u64)
            .is_some_and(|value| (1024..=262144).contains(&value))
            && tree
                .pointer("/server/max_inflight_requests")
                .and_then(Value::as_u64)
                .is_some_and(|value| (1..=128).contains(&value))
            && tree
                .pointer("/server/request_deadline_ms")
                .and_then(Value::as_u64)
                .is_some_and(|value| (100..=10000).contains(&value)),
        "management_request_budgets_required"
    );
    let mut credentials = BTreeMap::new();
    for (id, identity) in &config.identities {
        anyhow::ensure!(
            authority.settings.provisioner_key_id.as_ref() != Some(&identity.control_key_id),
            "management_key_cannot_be_provisioner"
        );
        let key = authority
            .settings
            .keys
            .get(&identity.control_key_id)
            .ok_or_else(|| anyhow::anyhow!("management_control_key_missing"))?;
        let Principal::Administrator {
            routes,
            ..
        } = &key.principal
        else {
            anyhow::bail!("management_administrator_required");
        };
        let token = std::env::var(&identity.token_secret_ref)
            .map_err(|_| anyhow::anyhow!("management_secret_missing"))?;
        anyhow::ensure!(
            (32..=512).contains(&token.len()),
            "management_secret_length"
        );
        credentials.insert(
            id.clone(),
            Credential {
                digest: nasa::crypto::hmac_sha256("socket-management-authentication", &token),
                control_key: identity.control_key_id.clone(),
                read_only: identity.read_only,
                routes: routes.clone(),
            },
        );
    }
    let management = Arc::new(Management {
        authority,
        gateway,
        listeners,
        credentials,
    });
    application.configure_router(move |router| {
        router.route(
            "/management/{*path}",
            axum::routing::any(move |request: Request| serve(management.clone(), request)),
        )
    })?;
    Ok(())
}

/// 业务作用：在读取管理正文之前验证真实网络对端和独立服务凭据，异常不回显配置或秘密。
/// 参数说明：
/// - `management`：已冻结身份与控制来源。
/// - `request`：框架治理后的真实 HTTP 请求。
///
/// 返回：有界 JSON 响应；权限失败不提交任何控制命令。
async fn serve(management: Arc<Management>, request: Request) -> Response {
    let result = management.dispatch(request).await;
    match result {
        Ok(value) => (StatusCode::OK, Json(value)).into_response(),
        Err(error) => {
            let code = error.to_string();
            let code = if code.len() <= 128
                && code
                    .bytes()
                    .all(|byte| byte.is_ascii_lowercase() || byte == b'_')
            {
                code
            } else {
                "management_request_rejected".into()
            };
            let status = match code.as_str() {
                "management_source_denied" | "management_identity_denied" => {
                    StatusCode::UNAUTHORIZED
                }
                "management_scope_denied"
                | "node_retirement_scope_denied"
                | "management_read_only" => StatusCode::FORBIDDEN,
                "management_not_found" => StatusCode::NOT_FOUND,
                "control_operation_unknown" => StatusCode::SERVICE_UNAVAILABLE,
                "route_revision_conflict"
                | "operation_conflict"
                | "route_source_conflict"
                | "route_operation_pending"
                | "route_suspended"
                | "owner_revision_conflict"
                | "node_permits_not_drained"
                | "node_incarnation_conflict"
                | "ownership_requires_effective_offline" => StatusCode::CONFLICT,
                _ => StatusCode::BAD_REQUEST,
            };
            (
                status,
                Json(json!({
                    "code": code
                })),
            )
                .into_response()
        }
    }
}

impl Management {
    /// 业务作用：将受权管理操作投递给统一控制权威，不提供绕过业务服务的用户或群消息推送。
    /// 参数说明：
    /// - `request`：包含方法、固定路径、身份头和有限正文的请求。
    ///
    /// 返回：定义验证、已提交操作或范围内查询结果；错误不声明全局生效。
    async fn dispatch(&self, request: Request) -> anyhow::Result<Value> {
        let peer = request
            .extensions()
            .get::<axum::extract::ConnectInfo<SocketAddr>>()
            .ok_or_else(|| anyhow::anyhow!("management_source_denied"))?
            .0
            .ip();
        anyhow::ensure!(
            self.gateway
                .configuration
                .management
                .allowed_peers
                .contains(&peer),
            "management_source_denied"
        );
        anyhow::ensure!(
            request.uri().query().is_none()
                && request.uri().path().len() <= 1024
                && request
                    .headers()
                    .iter()
                    .map(|(name, value)| name.as_str().len() + value.as_bytes().len())
                    .sum::<usize>()
                    <= 8192,
            "management_request_capacity"
        );
        let tokens = request
            .headers()
            .get_all("authorization")
            .iter()
            .collect::<Vec<_>>();
        anyhow::ensure!(tokens.len() == 1, "management_identity_denied");
        let token = tokens[0]
            .to_str()?
            .strip_prefix("Bearer ")
            .ok_or_else(|| anyhow::anyhow!("management_identity_denied"))?;
        let (id, secret) = token
            .split_once('.')
            .ok_or_else(|| anyhow::anyhow!("management_identity_denied"))?;
        let credential = self
            .credentials
            .get(id)
            .ok_or_else(|| anyhow::anyhow!("management_identity_denied"))?;
        anyhow::ensure!(
            secret.len() <= 512
                && nasa::crypto::hmac_sha256_verify(
                    "socket-management-authentication",
                    secret,
                    &credential.digest
                ),
            "management_identity_denied"
        );
        let method = request.method().clone();
        // 发现调用方的只读令牌不能借同一内部控制身份发起管理副作用。
        anyhow::ensure!(
            !credential.read_only || method == axum::http::Method::GET,
            "management_read_only"
        );
        let path = request.uri().path().to_owned();
        let pieces = path.trim_start_matches('/').split('/').collect::<Vec<_>>();
        let bytes = axum::body::to_bytes(request.into_body(), 262144).await?;
        let ledger = self.authority.snapshot().await?;
        if method == axum::http::Method::GET && pieces.len() == 4 && pieces[1] == "manifests" {
            anyhow::ensure!(bytes.is_empty(), "management_body_not_allowed");
            identifier(pieces[2])?;
            revision(pieces[3])?;
            let manifest = ledger
                .manifests
                .get(pieces[2])
                .and_then(|history| history.iter().find(|record| record.generation == pieces[3]))
                .ok_or_else(|| anyhow::anyhow!("management_not_found"))?;
            // 清单中任何路由超出管理范围都拒绝，不能利用状态查询枚举其它业务的发布内容。
            anyhow::ensure!(
                !manifest.entries.is_empty()
                    && manifest
                        .entries
                        .keys()
                        .all(|id| credential.routes.contains(id)),
                "management_scope_denied"
            );
            return Ok(json!({
                "manifest": manifest
            }));
        }
        if method == axum::http::Method::GET && pieces.len() == 3 && pieces[1] == "operations" {
            anyhow::ensure!(bytes.is_empty(), "management_body_not_allowed");
            if let Some(action) = ledger.retirements.get(pieces[2]) {
                anyhow::ensure!(
                    action.routes.is_subset(&credential.routes),
                    "management_scope_denied"
                );
                return Ok(json!({
                    "action": action
                }));
            }
            if let Some(action) = ledger.ownership_actions.get(pieces[2]) {
                anyhow::ensure!(
                    credential.routes.contains(&action.route_id),
                    "management_scope_denied"
                );
                return Ok(json!({
                    "action": action
                }));
            }
            let operation = ledger
                .operations
                .get(pieces[2])
                .ok_or_else(|| anyhow::anyhow!("management_not_found"))?;
            anyhow::ensure!(
                credential.routes.contains(&operation.binding.route_id),
                "management_scope_denied"
            );
            return Ok(json!({
                "operation": operation
            }));
        }
        if method == axum::http::Method::POST
            && pieces.len() == 4
            && pieces[1] == "nodes"
            && pieces[3] == "retire"
        {
            let retirement: Retirement = json::decode(&bytes, 4096)?;
            identifier(pieces[2])?;
            let command = Command::Retire {
                operation_id: retirement.operation_id,
                node_id: pieces[2].into(),
                expected_incarnation: revision(&retirement.expected_incarnation)?,
            };
            return Ok(serde_json::to_value(
                self.authority
                    .execute(&credential.control_key, command)
                    .await?,
            )?);
        }
        if method == axum::http::Method::POST
            && pieces.as_slice() == ["management", "routes", "validate"]
        {
            let proposal: Proposal = json::decode(&bytes, 262144)?;
            let (_, definition) = self.compile(proposal, credential)?;
            return Ok(json!({
                "definition": definition.route,
                "definitionDigest": definition.digest,
                "revision": definition.route.revision.to_string()
            }));
        }
        anyhow::ensure!(
            pieces.len() >= 3 && pieces[1] == "routes",
            "management_not_found"
        );
        let route_id = pieces[2];
        identifier(route_id)?;
        anyhow::ensure!(
            credential.routes.contains(route_id),
            "management_scope_denied"
        );
        if method == axum::http::Method::PUT && pieces.len() == 3 {
            let proposal: Proposal = json::decode(&bytes, 262144)?;
            let (command, definition) = self.compile(proposal, credential)?;
            anyhow::ensure!(
                definition.route.route_id == route_id,
                "management_route_mismatch"
            );
            return Ok(serde_json::to_value(
                self.authority
                    .execute(&credential.control_key, command)
                    .await?,
            )?);
        }
        let route = ledger
            .routes
            .get(route_id)
            .ok_or_else(|| anyhow::anyhow!("management_not_found"))?;
        if method == axum::http::Method::GET && pieces.len() == 5 && pieces[3] == "authorizations" {
            anyhow::ensure!(bytes.is_empty(), "management_body_not_allowed");
            identifier(pieces[4])?;
            let operation = ledger
                .authorizations
                .get(route_id)
                .and_then(|state| state.operations.get(pieces[4]))
                .ok_or_else(|| anyhow::anyhow!("management_not_found"))?;
            let effect = if operation.completed_at.is_some() {
                "completed"
            } else {
                "accepted"
            };
            return Ok(json!({
                "routeId": route_id,
                "operation": operation,
                "effect": effect
            }));
        }
        if method == axum::http::Method::GET && pieces.len() == 4 {
            anyhow::ensure!(bytes.is_empty(), "management_body_not_allowed");
            match pieces[3] {
                "status" => {
                    let active = self
                        .gateway
                        .routes
                        .by_id(route_id)
                        .is_some_and(|(plan, gate)| {
                            plan.digest == route.definition.digest
                                && gate.acquire(plan.definition.revision).is_some()
                        });
                    return Ok(json!({
                        "routeId": route_id,
                        "revision": route.definition.route.revision.to_string(),
                        "definitionDigest": route.definition.digest,
                        "desiredOnline": route.definition.route.enabled,
                        "localOnline": active,
                        "localReason": self.gateway.routes.by_id(route_id).and_then(|(_, gate)| gate.unavailable_reason()),
                        "clusterConnections": if route.definition.route.connections.scope == crate::domain::route::policies::QuotaScope::Cluster {
                            let mut counts = [0_usize; 3];
                            for reservation in ledger.reservations.values().filter(|entry| entry.session_key.route_id == route_id) {
                                let index = match reservation.phase {
                                    crate::domain::control::quota::ReservationPhase::Active => 0,
                                    crate::domain::control::quota::ReservationPhase::Waiting => 1,
                                    crate::domain::control::quota::ReservationPhase::Revoking => 2,
                                };
                                counts[index] += 1;
                            }
                            Some(json!({
                                "active": counts[0],
                                "waiting": counts[1],
                                "revoking": counts[2]
                            }))
                        } else {
                            None
                        },
                        "owner": route.owner,
                        "ownerKind": route.owner_kind,
                        "ownerRevision": route.owner_revision.to_string(),
                        "suspended": route.suspended,
                        "operation": ledger.operations[&route.operation_id]
                    }));
                }
                "describe" => {
                    let plan = CompiledRoute::compile_with_verifiers(
                        route.definition.route.clone(),
                        &route.definition.endpoints,
                        &route.definition.verifiers,
                    )?;
                    return Ok(serde_json::to_value(plan.public())?);
                }
                "directory" => {
                    let now = crate::adapters::outbound::identity::now()?;
                    return Ok(json!({
                        "clusterId": ledger.cluster_id,
                        "scopeEpoch": ledger.scope_epoch,
                        "routeId": route_id,
                        "routeRevision": route.definition.route.revision.to_string(),
                        "definitionDigest": route.definition.digest,
                        "observedAt": now,
                        "entries": ledger.public_access(route_id, now)
                    }));
                }
                "bindings" => {
                    let bound = self.listeners.bound(&route.definition.route);
                    let public = ledger
                        .public_access(route_id, crate::adapters::outbound::identity::now()?)
                        .into_iter()
                        .find(|entry| entry.node_id == self.gateway.configuration.node_id);
                    let active = self
                        .gateway
                        .routes
                        .by_id(route_id)
                        .is_some_and(|(plan, gate)| {
                            plan.digest == route.definition.digest
                                && gate.acquire(plan.definition.revision).is_some()
                        });
                    return Ok(json!({
                        "routeId": route_id,
                        "nodeId": self.gateway.configuration.node_id,
                        "incarnation": self.gateway.incarnation.get(),
                        "bindingId": bound.as_ref().map(|(id, _)| id),
                        "boundAddress": bound.as_ref().map(|(_, address)| address),
                        "state": if active { "serving" } else if bound.is_some() { "preparing" } else { "closed" },
                        "advertised": active && public.is_some(),
                        "reachability": if active && public.is_some() { "deployment_declared" } else { "unpublished" },
                        "publicAccess": if active { public } else { None }
                    }));
                }
                _ => {}
            }
        }
        if method == axum::http::Method::POST
            && pieces.len() == 4
            && ["online", "offline"].contains(&pieces[3])
        {
            let change: Change = json::decode(&bytes, 4096)?;
            identifier(&change.operation_id)?;
            let expected = expected_revision(&change.expected_revision)?;
            if pieces[3] == "offline" {
                return Ok(serde_json::to_value(
                    self.authority
                        .execute(
                            &credential.control_key,
                            Command::Offline {
                                operation_id: change.operation_id,
                                route_id: route_id.into(),
                                expected_revision: expected,
                            },
                        )
                        .await?,
                )?);
            }
            let mut definition = route.definition.clone();
            definition.route.revision = expected
                .checked_add(1)
                .ok_or_else(|| anyhow::anyhow!("route_revision_exhausted"))?;
            definition.route.enabled = pieces[3] == "online";
            let plan = CompiledRoute::compile_with_verifiers(
                definition.route.clone(),
                &definition.endpoints,
                &definition.verifiers,
            )?;
            definition.digest = plan.digest.clone();
            return Ok(serde_json::to_value(
                self.authority
                    .execute(
                        &credential.control_key,
                        Command::Define {
                            operation_id: change.operation_id,
                            expected_revision: expected,
                            minimum_ready: definition
                                .route
                                .minimum_ready(self.authority.settings.minimum_ready_nodes),
                            definition,
                        },
                    )
                    .await?,
            )?);
        }
        if method == axum::http::Method::POST
            && pieces.len() == 4
            && ["resume", "transfer"].contains(&pieces[3])
        {
            let change: OwnershipChange = json::decode(&bytes, 4096)?;
            let expected_revision = expected_revision(&change.expected_revision)?;
            let expected_owner_revision = revision(&change.expected_owner_revision)?;
            let command = if pieces[3] == "resume" {
                anyhow::ensure!(
                    change.owner.is_none() && change.owner_kind.is_none(),
                    "management_owner_not_allowed"
                );
                Command::Resume {
                    operation_id: change.operation_id,
                    route_id: route_id.into(),
                    expected_revision,
                    expected_owner_revision,
                }
            } else {
                let owner = change
                    .owner
                    .ok_or_else(|| anyhow::anyhow!("management_owner_required"))?;
                let owner_kind = change
                    .owner_kind
                    .ok_or_else(|| anyhow::anyhow!("management_owner_required"))?;
                anyhow::ensure!(self.authority.settings.keys.iter().any(|(key_id, key)| matches!(&key.principal, Principal::Administrator { subject, routes } if subject == &owner && routes.contains(route_id)) && (self.authority.settings.provisioner_key_id.as_ref() == Some(key_id)) == (owner_kind == crate::domain::control::OwnerKind::Config)), "management_owner_not_registered");
                Command::Transfer {
                    operation_id: change.operation_id,
                    route_id: route_id.into(),
                    expected_revision,
                    expected_owner_revision,
                    owner,
                    owner_kind,
                }
            };
            return Ok(serde_json::to_value(
                self.authority
                    .execute(&credential.control_key, command)
                    .await?,
            )?);
        }
        anyhow::bail!("management_not_found")
    }

    /// 业务作用：按当前配置目录冻结资源并分配下一代次，客户端不能自行选择超大的 revision 覆盖权限。
    /// 参数说明：
    /// - `proposal`：受权用户提供的完整候选和当前 CAS。
    /// - `credential`：当前管理主体允许修改的固定路由集合。
    ///
    /// 返回：规范化定义及待提交命令；schema、引用和摘要不符均拒绝。
    fn compile(
        &self,
        proposal: Proposal,
        credential: &Credential,
    ) -> anyhow::Result<(Command, Definition)> {
        identifier(&proposal.operation_id)?;
        let expected = expected_revision(&proposal.expected_revision)?;
        let mut value = proposal.definition;
        value
            .as_object_mut()
            .ok_or_else(|| anyhow::anyhow!("management_definition_required"))?
            .insert(
                "revision".into(),
                json!(expected
                    .checked_add(1)
                    .ok_or_else(|| anyhow::anyhow!("route_revision_exhausted"))?),
            );
        let definition = serde_json::from_value::<crate::domain::route::RouteDefinition>(value)?;
        anyhow::ensure!(
            credential.routes.contains(&definition.route_id),
            "management_scope_denied"
        );
        let mut resources = self.gateway.routes.configuration();
        resources.route_provisioning.enabled = false;
        resources.routes = vec![definition.clone()];
        resources.validate()?;
        let plan = CompiledRoute::compile_with_verifiers(
            definition.clone(),
            &resources.endpoints,
            &resources.verifiers,
        )?;
        anyhow::ensure!(
            proposal
                .definition_digest
                .is_none_or(|digest| digest == plan.digest),
            "management_definition_changed"
        );
        let definition = Definition {
            route: definition,
            endpoints: plan.endpoints.clone(),
            verifiers: plan.verifiers.clone(),
            digest: plan.digest.clone(),
        };
        let command = Command::Define {
            operation_id: proposal.operation_id,
            expected_revision: expected,
            definition: definition.clone(),
            minimum_ready: definition
                .route
                .minimum_ready(self.authority.settings.minimum_ready_nodes),
        };
        Ok((command, definition))
    }
}

/// 业务作用：读取管理 CAS 的十进制 revision，首次创建只允许规范字符串零。
/// 参数说明：
/// - `value`：管理请求的明确期望代次。
///
/// 返回：完整 u64，不经过 JavaScript 浮点中转。
fn expected_revision(value: &str) -> anyhow::Result<u64> {
    if value == "0" {
        Ok(0)
    } else {
        revision(value)
    }
}
