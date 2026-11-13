//! 只有经过来源适配器认证的 Push 能进入本地收件选择，群订阅和身份在最终 writer 再次复验。

use super::{outbox::DeliveryCheck, Gateway};
use crate::{
    adapters::outbound::identity::now,
    config::messaging::{Ingress, SourceIdentity},
    domain::{
        message::{
            control::{self, DeliveryAck, Downstream, PublicContext, PublicDelivery, PublicSender},
            deduplication::Admission,
            envelope::{Push, Target},
            wire::FrameType,
            UnixMillis,
        },
        route::policies::{SlowConsumerPolicy, TargetKind},
    },
};
use bytes::Bytes;
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::time::Instant;

/// 固定入口持有的来源材料不进入日志、客户端描述或 serde 配置输出。
pub struct TrustedIngress {
    /// 部署固定的入口引用，参与路由允许列表和有限指标归类。
    pub id: String,
    /// 该入口冻结的来源、认证、用途和投递合同。
    pub configuration: Ingress,
    /// 允许的 key ID 到生产者及密钥材料映射，不接受消息自带身份材料。
    keys: BTreeMap<String, (String, String)>,
    /// 本入口最近观察的 Unix 毫秒时间，用于拒绝时钟回退下的期限判断。
    wall: Mutex<Option<UnixMillis>>,
}

/// 每条记录的本地完成决定；不表示 TCP 写出或终端已处理。
#[derive(Clone, Default)]
pub struct DispatchReport {
    /// 本次分发成功进入会话队列的候选数量，不代表终端接收。
    pub queued: usize,
    /// 本次因权限、容量或期限不能入队的候选数量。
    pub dropped: usize,
    /// 是否命中原消息去重记录，重复输入不再次产生下行。
    pub duplicate: bool,
}

/// 记录本身没有被判定无效，但当前依赖或资源不足以完成消费决定。
#[derive(Debug)]
pub struct Deferred(
    /// 固定暂时依赖原因，要求源记录保留恢复责任，不能作为无效输入隔离。
    pub &'static str,
);
impl std::fmt::Display for Deferred {
    /// 业务作用：给 broker 重试状态提供稳定原因，不包含原始信封和凭据。
    /// 参数说明：
    /// - `formatter`：调用者提供的文本目标。
    ///
    /// 返回：只写固定原因码。
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.0)
    }
}
impl std::error::Error for Deferred {}

impl TrustedIngress {
    /// 业务作用：启动时解析部署批准的验签密钥，避免逐条消息读取环境或改变来源身份。
    /// 参数说明：
    /// - `id`：固定部署中的入口标识。
    /// - `configuration`：已经验证的 broker 与权限配置。
    ///
    /// 返回：可用于验证记录的冻结来源；缺密钥、弱长度或配置不合法阻止启动。
    pub fn new(id: String, configuration: Ingress) -> anyhow::Result<Arc<Self>> {
        configuration.validate()?;
        let mut keys = BTreeMap::new();
        if let SourceIdentity::HmacSha256 {
            keys: configured,
        } = &configuration.identity
        {
            for (id, key) in configured {
                let secret = std::env::var(&key.secret_ref)
                    .map_err(|_| anyhow::anyhow!("ingress signing secret unavailable"))?;
                anyhow::ensure!(
                    (32..=4096).contains(&secret.len()),
                    "ingress signing secret size"
                );
                keys.insert(id.clone(), (key.producer.clone(), secret));
            }
        }
        Ok(Arc::new(Self {
            id,
            configuration,
            keys,
            wall: Mutex::new(None),
        }))
    }

    /// 业务作用：从 ACL 固定身份或完整验签结果取得生产者身份，拒绝单独自报 producer。
    /// 参数说明：
    /// - `metadata`：实际接收到的控制 JSON 原文。
    /// - `body_digest`：对本记录原始正文计算一次的 SHA-256。
    /// - `key_id`：可选受信签名材料索引。
    /// - `signature`：可选小写十六进制认证码。
    ///
    /// 返回：部署确定的真实生产者标识；缺失、重复或未知材料不授予来源权限。
    pub fn producer<'a>(
        &'a self,
        metadata: &[u8],
        body_digest: &[u8; 32],
        key_id: Option<&str>,
        signature: Option<&str>,
    ) -> anyhow::Result<&'a str> {
        match &self.configuration.identity {
            SourceIdentity::Acl {
                producer,
            } => {
                anyhow::ensure!(
                    key_id.is_none() && signature.is_none(),
                    "unexpected_signature"
                );
                Ok(producer)
            }
            SourceIdentity::HmacSha256 {
                ..
            } => {
                let (id, signature) = (
                    key_id.ok_or_else(|| anyhow::anyhow!("signature_required"))?,
                    signature.ok_or_else(|| anyhow::anyhow!("signature_required"))?,
                );
                let (producer, secret) = self
                    .keys
                    .get(id)
                    .ok_or_else(|| anyhow::anyhow!("unknown_signing_key"))?;
                anyhow::ensure!(
                    signature.len() == 64
                        && signature
                            .bytes()
                            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
                    "invalid_signature"
                );
                let content = signing_content(&self.id, producer, id, metadata, body_digest);
                anyhow::ensure!(
                    nasa::crypto::hmac_sha256_verify(&content, secret, signature),
                    "invalid_signature"
                );
                Ok(producer)
            }
        }
    }

    /// 业务作用：拒绝已经观察到的时钟回退，防止消费重投以更早墙钟延长消息期限。
    /// 参数说明: 无。
    ///
    /// 返回：不低于上一条记录的整数毫秒时间；回退期间拒绝消费决定。
    fn time(&self) -> anyhow::Result<UnixMillis> {
        let current = now()?;
        let mut wall = self.wall.lock().unwrap_or_else(|e| e.into_inner());
        if wall.is_some_and(|old| current < old) {
            return Err(Deferred("clock_regression").into());
        }
        *wall = Some(current);
        Ok(current)
    }
}

/// 业务作用：定义跨 SDK 一致的签名消息，绑定合同、来源、密钥索引和实际 metadata/body。
/// 参数说明：
/// - `ingress`：部署固定的来源入口。
/// - `producer`：该密钥对应的业务服务身份。
/// - `key_id`：本次签名使用的轮换材料索引。
/// - `metadata`：实际控制 JSON 字节，不重新序列化。
/// - `body_digest`：实际业务体的 SHA-256。
///
/// 返回：固定域分隔和长度前缀字段的 SHA-256 小写文本，供框架 HMAC 接口签名。
pub fn signing_content(
    ingress: &str,
    producer: &str,
    key_id: &str,
    metadata: &[u8],
    body_digest: &[u8; 32],
) -> String {
    let mut hash = Sha256::new();
    hash.update(b"nacket.ingress.signature\0");
    for field in [
        control::CONTRACT.as_bytes(),
        ingress.as_bytes(),
        producer.as_bytes(),
        key_id.as_bytes(),
        metadata,
        body_digest,
    ] {
        hash.update((field.len() as u64).to_be_bytes());
        hash.update(field);
    }
    format!("{:x}", hash.finalize())
}

/// 业务作用：为多个语言生产者定义完全相同的业务排序键，不将路由更新拆到不同分区。
/// 参数说明：
/// - `push`：已验证完整作用域和唯一目标的受信业务 metadata。
///
/// 返回：包含目标类型及规范集合的长度前缀字节，不能由生产者自由指定。
pub fn ordering_key(push: &Push) -> Vec<u8> {
    let mut fields = vec![
        push.context.cluster_id.clone(),
        push.context.scope_epoch.clone(),
        push.context.tenant_id.clone(),
        push.context.route_id.clone(),
    ];
    match &push.target {
        Target::Group {
            group_id,
        } => {
            fields.push("group".into());
            fields.push(group_id.clone());
        }
        Target::Uids {
            uids,
        } => {
            fields.push("uids".into());
            let mut uids = uids.clone();
            uids.sort();
            fields.extend(uids);
        }
        Target::TenantRouteBroadcast {} => fields.push("tenant_route_broadcast".into()),
        _ => fields.push("invalid".into()),
    }
    let mut bytes = b"nacket.ordering\0".to_vec();
    for field in fields {
        bytes.extend_from_slice(&(field.len() as u32).to_be_bytes());
        bytes.extend_from_slice(field.as_bytes());
    }
    bytes
}

impl Gateway {
    /// 业务作用：验证受信下行完整作用域并选择本地当前合法会话，消息正文只保留共享字节。
    /// 参数说明：
    /// - `ingress`：已经绑定真实 broker 来源的冻结权限。
    /// - `producer`：入口从 ACL 或验签得出的主体。
    /// - `push`：严格解析且没有客户端控制字段的元数据。
    /// - `body`：业务微服务已经按唯一路由 codec 编码的原始正文。
    /// - `body_digest`：同一份原始正文的 SHA-256，避免扇出重复计算。
    ///
    /// 返回：所有目标已入队或按慢连接策略终止；失败时 broker 不能提前确认。
    pub async fn dispatch(
        &self,
        ingress: &TrustedIngress,
        producer: &str,
        push: Push,
        body: Bytes,
        body_digest: [u8; 32],
    ) -> anyhow::Result<DispatchReport> {
        anyhow::ensure!(
            ingress.configuration.purpose == crate::config::messaging::IngressPurpose::Push,
            "push_source_purpose"
        );
        anyhow::ensure!(
            push.context.cluster_id == self.configuration.cluster_id
                && push.context.scope_epoch == self.configuration.scope_epoch,
            "scope_mismatch"
        );
        let current = ingress.time()?;
        // 原始消息年龄也是重试上界；容量或路由不可用不能把过期责任无限留在顺序键前端。
        if push.delivery.expires_at <= current
            || current
                .as_millis()
                .saturating_sub(push.delivery.created_at.as_millis())
                >= self.configuration.ingress_processing.max_retry_age_ms
        {
            return Ok(DispatchReport::default());
        }
        let _fanout = self
            .fanouts
            .clone()
            .try_acquire_owned()
            .map_err(|_| Deferred("fanout_capacity"))?;
        let _fanout_observation = self.metrics.reserve("fanouts", 1);
        let (plan, gate) = self
            .routes
            .by_id(&push.context.route_id)
            .ok_or(Deferred("route_unavailable"))?;
        let definition = &plan.definition;
        let policy = &definition.service_push;
        anyhow::ensure!(
            policy.enabled
                && policy.ingress_ref.as_deref() == Some(&ingress.id)
                && policy.allowed_producers.contains(producer),
            "producer_denied"
        );
        let target = match push.target {
            Target::Group {
                ..
            } => TargetKind::Group,
            Target::Uids {
                ..
            } => TargetKind::Uids,
            Target::TenantRouteBroadcast {} => TargetKind::TenantRouteBroadcast,
            _ => anyhow::bail!("target_denied"),
        };
        anyhow::ensure!(policy.allowed_targets.contains(&target), "target_denied");
        let remaining = push.validate(
            current,
            definition.delivery.max_ttl_ms,
            definition.delivery.future_skew_ms,
        )?;
        let retry_remaining = self
            .configuration
            .ingress_processing
            .max_retry_age_ms
            .saturating_sub(
                current
                    .as_millis()
                    .saturating_sub(push.delivery.created_at.as_millis()),
            );
        let deadline = Instant::now() + remaining.min(Duration::from_millis(retry_remaining));
        let revision = push.context.route_revision.parse::<u64>()?;
        if revision < definition.revision || !definition.enabled {
            return Ok(DispatchReport::default());
        }
        if revision > definition.revision {
            return Err(Deferred("definition_pending").into());
        }
        anyhow::ensure!(
            push.context.definition_digest == plan.digest,
            "definition_mismatch"
        );
        if gate.acquire(definition.revision).is_none() {
            return Err(Deferred("route_protected").into());
        }
        let event = definition
            .events
            .get(&push.event)
            .ok_or_else(|| anyhow::anyhow!("event_not_found"))?;
        anyhow::ensure!(
            push.schema == event.schema
                && body.len() <= event.max_bytes
                && body.len() <= ingress.configuration.max_body_bytes,
            "schema_or_body_limit"
        );
        self.metrics
            .payload(definition.serialization.mode().ordinal(), body.len());
        if policy.validate_body {
            let result = plan
                .schemas
                .get(&push.schema)
                .ok_or_else(|| anyhow::anyhow!("schema_not_found"))?
                .decode(body.clone());
            self.metrics
                .payload_decode("validation", body.len(), result.is_ok());
            result?;
        }

        let message_key = push.message_key(producer);
        let attempt = if definition.delivery.deduplication.enabled {
            match self.deduplication.admit(
                message_key.clone(),
                push.fingerprint(&body_digest)?,
                deadline,
            ) {
                Admission::Accepted(attempt) => Some(attempt),
                Admission::Completed => {
                    return Ok(DispatchReport {
                        duplicate: true,
                        ..Default::default()
                    })
                }
                Admission::Expired => return Ok(DispatchReport::default()),
                Admission::Conflict => anyhow::bail!("message_conflict"),
                _ => return Err(Deferred("deduplication_capacity").into()),
            }
        } else {
            None
        };
        self.metrics.add(
            crate::observability::Counter::Deliveries,
            "admitted",
            "accepted",
            1,
        );
        let (candidates, _references, _reference_observation) = {
            let sessions = self
                .sessions
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            // 引用快照在分配前按全目录上界预约，多个入口不能各自保留无限候选。
            let count =
                u32::try_from(sessions.len()).map_err(|_| Deferred("candidate_capacity"))?;
            let references = self
                .candidate_references
                .clone()
                .try_acquire_many_owned(count)
                .map_err(|_| Deferred("candidate_capacity"))?;
            let observed = self.metrics.reserve("fanout_candidates", count as u64);
            let candidates: Vec<_> = sessions
                .values()
                .filter(|connection| {
                    connection.is_ready()
                        && connection.session.key.route_id == push.context.route_id
                        && connection.session.key.definition_digest == plan.digest
                })
                .cloned()
                .collect();
            (candidates, references, observed)
        };
        let push = Arc::new(push);
        let coalescible = event.coalescible;
        let mut report = DispatchReport::default();
        for (index, connection) in candidates.into_iter().enumerate() {
            if index % self.configuration.ingress_processing.fanout_batch_size == 0 {
                if index > 0 {
                    tokio::task::yield_now().await;
                }
                // 每批让出后复验原始期限和路由门禁；到期或下线只终止剩余目标，不刷新消息寿命。
                if Instant::now() >= deadline || gate.acquire(definition.revision).is_none() {
                    break;
                }
            }
            let owner = connection.clone();
            let plan = plan.clone();
            let push = push.clone();
            let body = body.clone();
            let message_key = message_key.clone();
            let retained = body
                .len()
                .saturating_add(self.configuration.transport.max_metadata_bytes);
            let queued = connection
                .uid_events
                .submit_data(deadline, retained, move || {
                    Self::dispatch_target(
                        &owner,
                        &plan,
                        &push,
                        &body,
                        deadline,
                        current,
                        &message_key,
                        coalescible,
                    )
                });
            let result = match queued {
                Ok(queued) => queued.wait().await,
                Err(error) => Err(error),
            };
            match result {
                Ok(Some(true)) => report.queued += 1,
                Ok(None) => {}
                Ok(Some(false)) | Err(_) => {
                    report.dropped += 1;
                    // 不能在分区拒绝后绕过主体顺序补投；关闭策略只收紧连接资格。
                    if definition.delivery.slow_consumer == SlowConsumerPolicy::Disconnect {
                        connection.session.close();
                        connection.cancellation.cancel();
                    }
                }
            }
        }
        // 只有每个候选都有明确入队或终止决定后才完成去重，终端回执不参与 broker 水位。
        if let Some(attempt) = attempt {
            attempt.complete();
        }
        Ok(report)
    }

    /// 业务作用：在目标主体的严格消费域完成收件筛选、回执预约与出站排队，正文保持共享字节。
    /// 参数说明：
    /// - `connection`：本次候选的精确会话。
    /// - `plan`：已通过来源验证的冻结路由计划。
    /// - `push`：业务服务授权后提交的固定控制信封。
    /// - `body`：不随目标重新解码的业务正文。
    /// - `deadline`：原消息不可延长的投递截止点。
    /// - `current`：原入口取得的 Unix 毫秒时间。
    /// - `message_key`：覆盖来源与消息作用域的去重键。
    /// - `coalescible`：路由声明的完整状态覆盖资格。
    ///
    /// 返回：无资格目标返回 None，入队成功返回 true，容量拒绝返回 false；不代表网络写出。
    #[expect(
        clippy::too_many_arguments,
        reason = "逐目标投递必须同时复验冻结定义、原期限、去重键及覆盖资格。"
    )]
    fn dispatch_target(
        connection: &super::Connected,
        plan: &crate::domain::route::compiled::CompiledRoute,
        push: &Push,
        body: &Bytes,
        deadline: Instant,
        current: UnixMillis,
        message_key: &str,
        coalescible: bool,
    ) -> anyhow::Result<Option<bool>> {
        if !connection.is_ready() || Instant::now() >= deadline {
            return Ok(None);
        }
        let definition = &plan.definition;
        let group = match &push.target {
            Target::Group {
                group_id,
            } => Some(group_id.as_str()),
            _ => None,
        };
        let identity = connection.session.identity();
        if identity.tenant != push.context.tenant_id
            || push.delivery.exclude_uids.contains(&identity.uid)
            || push.delivery.exclude_session_key.as_ref() == Some(&connection.session.key)
        {
            return Ok(None);
        }
        let receives = if group.is_some() {
            definition.receiving.group.enabled
        } else {
            definition.receiving.direct.enabled && {
                let grants = connection.grants.lock().unwrap_or_else(|e| e.into_inner());
                grants.valid()
                    && grants
                        .can_receive_direct
                        .unwrap_or(!definition.connect_authorization.enabled)
            }
        };
        if !receives {
            return Ok(None);
        }
        if let Target::Uids {
            uids,
        } = &push.target
        {
            if !uids.contains(&identity.uid) {
                return Ok(None);
            }
        }
        let Some(generation) = connection.session.candidate(group) else {
            return Ok(None);
        };
        let target = match &push.target {
            Target::Group {
                group_id,
            } => Target::Group {
                group_id: group_id.clone(),
            },
            _ => Target::Current {},
        };
        let mut metadata = Downstream {
            kind: push.kind,
            id: push.id.clone(),
            correlation_id: push.correlation_id.clone(),
            operation_id: push.operation_id.clone(),
            context: PublicContext {
                route_id: push.context.route_id.clone(),
                route_revision: push.context.route_revision.clone(),
                definition_digest: push.context.definition_digest.clone(),
            },
            event: push.event.clone(),
            schema: Some(push.schema.clone()),
            target,
            sender: push.sender.as_ref().map(|s| PublicSender {
                uid: s.uid.clone(),
            }),
            delivery: Some(PublicDelivery {
                created_at: push.delivery.created_at,
                expires_at: push.delivery.expires_at,
                message_key: message_key.to_owned(),
                ack: None,
            }),
            result: None,
        };
        let mut receipt = None;
        if definition.delivery.client_ack.enabled {
            let policy = &definition.delivery.client_ack;
            let level = policy
                .level
                .ok_or_else(|| anyhow::anyhow!("receipt_policy_missing"))?;
            let receipt_deadline = deadline
                .min(identity.deadline)
                .min(Instant::now() + Duration::from_millis(policy.timeout_ms));
            match connection.receipts.reserve(
                message_key.to_owned(),
                level,
                receipt_deadline,
                group.map(str::to_owned),
            ) {
                Ok(token) => {
                    metadata.delivery.as_mut().expect("delivery").ack = Some(DeliveryAck {
                        token: token.clone(),
                        level,
                        deadline: UnixMillis::new(
                            current.as_millis()
                                + receipt_deadline
                                    .saturating_duration_since(Instant::now())
                                    .as_millis() as u64,
                        )
                        .map_err(anyhow::Error::msg)?,
                    });
                    receipt = Some((connection.receipts.clone(), token));
                }
                Err(_) => {
                    if definition.delivery.slow_consumer == SlowConsumerPolicy::Disconnect {
                        connection.session.close();
                        connection.cancellation.cancel();
                    }
                    return Ok(Some(false));
                }
            }
        }
        let check = DeliveryCheck {
            uid_events: connection.uid_events.clone(),
            session: connection.session.clone(),
            group: group.map(str::to_owned),
            generation,
            direct_receiving: group.is_none(),
            receipt: receipt.clone(),
        };
        let result =
            if definition.delivery.slow_consumer == SlowConsumerPolicy::KeepLatest && coalescible {
                let key = serde_json::to_string(&(&push.event, &push.schema, &metadata.target))?;
                connection.outbox.enqueue_latest(
                    &metadata,
                    (definition.serialization.mode().ordinal(), body.clone()),
                    deadline,
                    check,
                    key,
                )
            } else {
                connection.outbox.enqueue(
                    FrameType::Broadcast,
                    &metadata,
                    Some((definition.serialization.mode().ordinal(), body.clone())),
                    deadline,
                    Some(check),
                )
            };
        if result.is_err() {
            if let Some((owner, token)) = receipt {
                owner.cancel(Some(&token), None);
            }
            if definition.delivery.slow_consumer == SlowConsumerPolicy::Disconnect {
                connection.session.close();
                connection.cancellation.cancel();
            }
        }
        Ok(Some(result.is_ok()))
    }
}
