//! 无副作用路由编译，将外部引用冻结在定义摘要内。

use super::{policies::*, Identity, RouteDefinition, Serialization};
use crate::{
    config::RemoteEndpoint,
    domain::message::{control::PublicRoute, envelope::identifier},
};
use bytes::Bytes;
use nasa::ws::proto::dynamic::{
    FieldDescriptor, Limits, SchemaDescriptor, SchemaPlan, Type, Value,
};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

/// 内置订阅对象不能被业务同名 schema 覆盖。
pub const SUBSCRIPTION_SCHEMA: &str = "nacket.subscription";

/// 权限、schema 和远程资源均固定在同一计划中，运行期不读取可漂移的全局引用。
pub struct CompiledRoute {
    /// 已经验证且冻结的完整路由声明，现有会话不随新候选漂移。
    pub definition: RouteDefinition,
    /// 定义及实际引用资源的确定摘要，参与会话、消息与控制许可绑定。
    pub digest: String,
    /// 按路由唯一模式编译的有限 schema 计划，共享使用而不逐消息重建。
    pub schemas: BTreeMap<String, Arc<SchemaPlan>>,
    /// 该路由实际允许引用的远程端点快照，热更不能换掉旧会话的调用目标。
    pub endpoints: BTreeMap<String, RemoteEndpoint>,
    /// 该路由身份验证器的冻结策略快照，运行期密钥刷新仍受该策略约束。
    pub verifiers: BTreeMap<String, crate::config::identity::Verifier>,
}

impl CompiledRoute {
    /// 业务作用：在任何监听、账本或会话副作用前编译完整路由，并冻结所有允许的远程资源。
    /// 参数说明：
    /// - `definition`：完整期望定义，缺省值已由配置模型展开。
    /// - `endpoints`：部署批准的远程入口目录。
    ///
    /// 返回：不可变执行计划；引用、schema、安全组合或资源限额不完整时拒绝。
    pub fn compile(
        definition: RouteDefinition,
        endpoints: &BTreeMap<String, RemoteEndpoint>,
    ) -> anyhow::Result<Arc<Self>> {
        Self::compile_with_verifiers(definition, endpoints, &BTreeMap::new())
    }

    /// 业务作用：将身份验签引用与路由定义一同冻结，运行期间不套用其它配置代次的密钥来源策略。
    /// 参数说明：
    /// - `definition`：完整候选路由。
    /// - `endpoints`：部署批准的业务接口目录。
    /// - `verifiers`：部署批准的身份验签策略目录。
    ///
    /// 返回：确定摘要的计划；任一引用缺失或公开描述超限时拒绝。
    pub fn compile_with_verifiers(
        definition: RouteDefinition,
        endpoints: &BTreeMap<String, RemoteEndpoint>,
        verifiers: &BTreeMap<String, crate::config::identity::Verifier>,
    ) -> anyhow::Result<Arc<Self>> {
        definition.validate()?;
        definition.authorization.control.validate()?;
        definition.authorization.refresh.schedule.validate()?;
        definition.authorization.cache.validate()?;
        let mut resolved_verifiers = BTreeMap::new();
        if let Identity::LocalJwt {
            verifier_ref,
        }
        | Identity::TrustedGateway {
            verifier_ref,
            ..
        } = &definition.identity
        {
            let verifier = verifiers
                .get(verifier_ref)
                .ok_or_else(|| anyhow::anyhow!("identity verifierRef missing"))?;
            verifier.validate()?;
            resolved_verifiers.insert(verifier_ref.clone(), verifier.clone());
        }
        let mut refs = BTreeSet::new();
        if let Identity::Remote {
            endpoint_ref,
        } = &definition.identity
        {
            refs.insert(endpoint_ref.clone());
        }
        for (enabled, endpoint_ref) in [
            (
                definition.connect_authorization.enabled,
                &definition.connect_authorization.endpoint_ref,
            ),
            (
                definition.authorization.refresh.enabled,
                &definition.authorization.refresh.endpoint_ref,
            ),
        ] {
            if enabled {
                refs.insert(endpoint_ref.clone().ok_or_else(|| {
                    anyhow::anyhow!("enabled remote policy requires endpointRef")
                })?);
            }
        }
        let limits = &definition.connections;
        limits.requests.validate()?;
        anyhow::ensure!(
            (1..=100_000).contains(&limits.max_connections)
                && (1..=1024).contains(&limits.max_per_uid)
                && (1..=128).contains(&limits.max_pending_operations)
                && (1..=10000).contains(&limits.operations_per_second),
            "invalid connection limits"
        );
        let delivery = &definition.delivery;
        anyhow::ensure!(
            (1..=86_400_000).contains(&delivery.max_ttl_ms)
                && delivery.future_skew_ms <= 30_000
                && (1..=4096).contains(&delivery.max_queue_messages)
                && (1024..=64 * 1024 * 1024).contains(&delivery.max_queue_bytes),
            "invalid delivery limits"
        );
        if delivery.client_ack.enabled {
            anyhow::ensure!(
                delivery.client_ack.level.is_some()
                    && (1..=60_000).contains(&delivery.client_ack.timeout_ms)
                    && (1..=4096).contains(&delivery.client_ack.max_pending),
                "invalid client ACK policy"
            );
        }
        for origin in &definition.origin_check.allowed_origins {
            anyhow::ensure!(
                origin.len() <= 2048
                    && (origin.starts_with("https://") || origin.starts_with("http://"))
                    && !origin.contains('*')
                    && !origin.chars().any(char::is_control),
                "invalid origin allowlist"
            );
        }
        let mut descriptors = definition.schemas.clone();
        anyhow::ensure!(
            descriptors.len() <= 256 && !descriptors.contains_key(SUBSCRIPTION_SCHEMA),
            "invalid schema catalog"
        );
        let subscriptions = &definition.subscriptions;
        anyhow::ensure!(
            (1..=4096).contains(&subscriptions.max_per_session)
                && (1..=128).contains(&subscriptions.max_per_request)
                && subscriptions.max_per_request <= subscriptions.max_per_session
                && (1..=3_600_000).contains(&subscriptions.max_grant_ttl_ms),
            "invalid subscription limits"
        );
        if subscriptions.enabled {
            let authorization = subscriptions
                .authorization
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("subscription authorization required"))?;
            match authorization {
                SubscriptionAuthorization::RemoteHttp {
                    endpoint_ref,
                } => {
                    refs.insert(endpoint_ref.clone());
                }
                SubscriptionAuthorization::PublicAllowlist {
                    groups,
                } => {
                    anyhow::ensure!(
                        !groups.is_empty() && groups.len() <= 4096,
                        "invalid public group list"
                    );
                    for group in groups {
                        validate_group(group, subscriptions.group_id_format)?;
                    }
                }
                SubscriptionAuthorization::ConnectionGrants {} => anyhow::ensure!(
                    definition.lifecycle.initialization.enabled
                        && definition.lifecycle.initialization.required,
                    "connection grants require initialization"
                ),
            }
            if definition.serialization == Serialization::FastFixed {
                anyhow::ensure!(
                    subscriptions.group_id_format == GroupIdFormat::I64Decimal
                        && subscriptions.max_per_request == 1,
                    "fixed subscriptions require one i64 group"
                );
            }
            if let InitialSubscriptions::Configured {
                group_ids,
            } = &subscriptions.initial
            {
                anyhow::ensure!(
                    !group_ids.is_empty() && group_ids.len() <= subscriptions.max_per_request,
                    "initial subscription capacity"
                );
                for group in group_ids {
                    validate_group(group, subscriptions.group_id_format)?;
                }
            }
            if matches!(
                subscriptions.initial,
                InitialSubscriptions::AuthorizedSnapshot {}
            ) {
                anyhow::ensure!(
                    definition.lifecycle.initialization.enabled
                        && definition.lifecycle.initialization.required,
                    "initial snapshot requires initialization"
                );
            }
        }
        // unsub 始终允许清理当前连接；即使 sub 关闭也公布同一参数布局，不能切换 codec。
        if definition.serialization != Serialization::FastFixed
            || subscriptions.group_id_format == GroupIdFormat::I64Decimal
        {
            let (name, typ) = if definition.serialization == Serialization::FastFixed {
                ("groupId", Type::I64 {})
            } else {
                (
                    "groupIds",
                    Type::List {
                        element: Box::new(Type::String {}),
                        nullable_elements: false,
                    },
                )
            };
            descriptors.insert(
                SUBSCRIPTION_SCHEMA.into(),
                SchemaDescriptor {
                    schema_id: SUBSCRIPTION_SCHEMA.into(),
                    fields: vec![FieldDescriptor {
                        tag: 1,
                        name: name.into(),
                        typ,
                        nullable: false,
                        max_bytes: 65536,
                        max_elements: 128,
                    }],
                },
            );
        }
        let mut schemas = BTreeMap::new();
        for id in descriptors.keys() {
            schemas.insert(
                id.clone(),
                SchemaPlan::compile(
                    id,
                    &descriptors,
                    definition.serialization.mode(),
                    Limits::default(),
                )?,
            );
        }
        anyhow::ensure!(definition.events.len() <= 1024, "event catalog capacity");
        for (event, policy) in &definition.events {
            identifier(event)?;
            anyhow::ensure!(
                schemas.contains_key(&policy.schema)
                    && (1..=1024 * 1024).contains(&policy.max_bytes),
                "event schema or limit missing"
            );
        }
        let messages = &definition.client_messages;
        if messages.enabled {
            anyhow::ensure!(
                !matches!(definition.identity, Identity::Anonymous { .. })
                    && definition.service_push.enabled,
                "client_messages_require_identity_and_push"
            );
            let execution = messages
                .execution
                .ok_or_else(|| anyhow::anyhow!("message execution required"))?;
            refs.insert(
                messages
                    .endpoint_ref
                    .clone()
                    .ok_or_else(|| anyhow::anyhow!("message endpointRef required"))?,
            );
            anyhow::ensure!(
                !messages.allowed_events.is_empty()
                    && messages
                        .allowed_events
                        .iter()
                        .all(|e| definition.events.contains_key(e))
                    && !messages.allowed_targets.is_empty()
                    && !messages
                        .allowed_targets
                        .contains(&TargetKind::TenantRouteBroadcast)
                    && (1..=1024 * 1024).contains(&messages.max_request_bytes),
                "invalid client message bounds"
            );
            if execution == MessageExecution::AuthorizeThenPublish {
                anyhow::ensure!(
                    messages.publisher_ref.is_some(),
                    "message publisher required"
                );
            } else {
                anyhow::ensure!(
                    messages.payload_access != PayloadAccess::MetadataOnly,
                    "business_command_requires_body"
                );
                anyhow::ensure!(
                    messages.publisher_ref.is_none(),
                    "business command does not publish locally"
                );
            }
        }
        let mut operations = BTreeSet::new();
        anyhow::ensure!(
            definition.notifications.operations.len() <= 128,
            "notification capacity"
        );
        for operation in &definition.notifications.operations {
            identifier(&operation.name)?;
            anyhow::ensure!(
                operations.insert(&operation.name),
                "duplicate notification operation"
            );
            for schema in [&operation.request_schema, &operation.response_schema]
                .into_iter()
                .flatten()
            {
                anyhow::ensure!(schemas.contains_key(schema), "operation schema missing");
            }
            if !definition.notifications.enabled || !operation.enabled {
                continue;
            }
            match &operation.authorization {
                OperationAuthorization::RemoteHttp {
                    endpoint_ref,
                } => {
                    refs.insert(endpoint_ref.clone());
                }
                OperationAuthorization::ConnectionGrant {} => anyhow::ensure!(
                    definition.connect_authorization.enabled,
                    "connection grant required"
                ),
                _ => {}
            }
            match operation.action {
                Action::RemoteCall => {
                    refs.insert(
                        operation
                            .endpoint_ref
                            .clone()
                            .ok_or_else(|| anyhow::anyhow!("remote operation endpoint required"))?,
                    );
                }
                Action::RefreshAuthorization => anyhow::ensure!(
                    definition.authorization.refresh.enabled,
                    "refresh policy required"
                ),
                Action::BusinessSignal => anyhow::ensure!(
                    operation.publisher_ref.is_some() && operation.response_schema.is_none(),
                    "signal publisher required"
                ),
                Action::SelfStatus => {}
            }
        }
        for (enabled, target, timeout_ms) in [
            (
                definition.lifecycle.initialization.enabled,
                &definition.lifecycle.initialization.target,
                definition.lifecycle.initialization.timeout_ms,
            ),
            (
                definition.lifecycle.online.enabled,
                &definition.lifecycle.online.target,
                definition.lifecycle.online.timeout_ms,
            ),
            (
                definition.lifecycle.offline.enabled,
                &definition.lifecycle.offline.target,
                definition.lifecycle.offline.timeout_ms,
            ),
        ] {
            if enabled {
                anyhow::ensure!((1..=30_000).contains(&timeout_ms), "hook deadline required");
                match target
                    .as_ref()
                    .ok_or_else(|| anyhow::anyhow!("hook target required"))?
                {
                    HookTarget::Http {
                        endpoint_ref,
                    } => {
                        refs.insert(endpoint_ref.clone());
                    }
                    HookTarget::Mq {
                        publisher_ref,
                    } => identifier(publisher_ref)?,
                }
            }
        }
        let push = &definition.service_push;
        if definition.enabled && push.enabled {
            anyhow::ensure!(
                push.ingress_ref.is_some()
                    && !push.allowed_producers.is_empty()
                    && !push.allowed_targets.is_empty(),
                "service push requires trusted ingress and producer scope"
            );
        }
        if delivery.slow_consumer == SlowConsumerPolicy::KeepLatest {
            anyhow::ensure!(
                definition.events.values().all(|event| event.coalescible),
                "keep_latest requires state events"
            );
        }
        let mut resolved = BTreeMap::new();
        for reference in refs {
            identifier(&reference)?;
            resolved.insert(
                reference.clone(),
                endpoints
                    .get(&reference)
                    .cloned()
                    .ok_or_else(|| anyhow::anyhow!("remote endpoint reference missing"))?,
            );
        }
        if definition.client_messages.enabled {
            let endpoint = &resolved[definition
                .client_messages
                .endpoint_ref
                .as_ref()
                .expect("validated endpoint")];
            anyhow::ensure!(
                endpoint.call.method == crate::config::RemoteMethod::Post,
                "client_messages_require_post_endpoint"
            );
        }
        for (enabled, target, _timeout_ms) in [
            (
                definition.lifecycle.initialization.enabled,
                &definition.lifecycle.initialization.target,
                definition.lifecycle.initialization.timeout_ms,
            ),
            (
                definition.lifecycle.online.enabled,
                &definition.lifecycle.online.target,
                definition.lifecycle.online.timeout_ms,
            ),
            (
                definition.lifecycle.offline.enabled,
                &definition.lifecycle.offline.target,
                definition.lifecycle.offline.timeout_ms,
            ),
        ] {
            if enabled {
                if let Some(HookTarget::Http {
                    endpoint_ref,
                }) = target
                {
                    anyhow::ensure!(
                        resolved[endpoint_ref].call.method == crate::config::RemoteMethod::Post,
                        "lifecycle_requires_post_endpoint"
                    );
                }
            }
        }
        let normalized = serde_json::json!({
            "definition": definition,
            "endpoints": resolved,
            "schemas": descriptors,
            "verifiers": resolved_verifiers
        });
        let bytes = serde_json::to_vec(&normalized)?;
        anyhow::ensure!(
            bytes.len() <= 1024 * 1024,
            "compiled route definition capacity"
        );
        let digest = format!("sha256:{:x}", Sha256::digest(&bytes));
        let compiled = Arc::new(Self {
            definition,
            digest,
            schemas,
            endpoints: resolved,
            verifiers: resolved_verifiers,
        });
        anyhow::ensure!(
            serde_json::to_vec(&compiled.public())?.len() <= 30_000,
            "public route descriptor capacity"
        );
        Ok(compiled)
    }

    /// 业务作用：只公布当前定义的 codec、schema 与可申请能力，不暴露业务接口和凭据。
    /// 参数说明: 无。
    ///
    /// 返回：供 hello 和受权目录使用的定义投影。
    pub fn public(&self) -> PublicRoute {
        PublicRoute {
            route_id: self.definition.route_id.clone(),
            route_revision: self.definition.revision.to_string(),
            definition_digest: self.digest.clone(),
            serialization: self.definition.serialization,
            schemas: self
                .schemas
                .iter()
                .map(|(id, plan)| (id.clone(), plan.descriptor().clone()))
                .collect(),
            subscriptions: self.definition.subscriptions.enabled,
            restore_subscriptions: self.definition.subscriptions.enabled
                && self.definition.subscriptions.restore.enabled,
            client_messages: self.definition.client_messages.enabled,
            notifications: self
                .definition
                .notifications
                .operations
                .iter()
                .filter(|o| self.definition.notifications.enabled && o.enabled)
                .map(|o| o.name.clone())
                .collect(),
        }
    }

    /// 业务作用：按路由唯一模式读取 sub/unsub 参数，并检查群 ID 格式和批次容量。
    /// 参数说明：
    /// - `body`：内置订阅 schema 的原始字节。
    ///
    /// 返回：规范化去重后的群 ID 列表，固定布局只产生一个 i64 群标识。
    pub fn subscription_groups(&self, body: Bytes) -> anyhow::Result<Vec<String>> {
        let schema = self
            .schemas
            .get(SUBSCRIPTION_SCHEMA)
            .ok_or_else(|| anyhow::anyhow!("subscription schema unavailable"))?;
        let Value::Object(mut object) = schema.decode(body)? else {
            anyhow::bail!("subscription object required");
        };
        let mut groups = if self.definition.serialization == Serialization::FastFixed {
            let Some(Value::Signed(group)) = object.remove("groupId") else {
                anyhow::bail!("groupId required");
            };
            vec![group.to_string()]
        } else {
            let Some(Value::List(groups)) = object.remove("groupIds") else {
                anyhow::bail!("groupIds required");
            };
            groups
                .into_iter()
                .map(|g| match g {
                    Value::String(group) => Ok(group),
                    _ => Err(anyhow::anyhow!("group ID must be a string")),
                })
                .collect::<anyhow::Result<Vec<_>>>()?
        };
        anyhow::ensure!(
            !groups.is_empty() && groups.len() <= self.definition.subscriptions.max_per_request,
            "subscription batch capacity"
        );
        for group in &groups {
            validate_group(group, self.definition.subscriptions.group_id_format)?;
        }
        groups.sort();
        groups.dedup();
        Ok(groups)
    }
}

/// 业务作用：验证群标识的公开字符串形式，固定布局禁止前导零或溢出产生别名。
/// 参数说明：
/// - `group`：业务提交的群 ID。
/// - `format`：路由选择的唯一 ID 格式。
///
/// 返回：有界且符合路由格式的标识，不更改业务群成员关系。
pub fn validate_group(group: &str, format: GroupIdFormat) -> anyhow::Result<()> {
    identifier(group)?;
    if format == GroupIdFormat::I64Decimal {
        let number = group.parse::<i64>()?;
        anyhow::ensure!(number.to_string() == group, "noncanonical group ID");
    }
    Ok(())
}
