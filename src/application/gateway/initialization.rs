//! 初始化回复绑定唯一待决请求、精确会话和冻结来源，不按 uid 查找并更新任意连接。

use super::{
    authority::{AuthorizationResponse, CallContext},
    dispatch::TrustedIngress,
    Connected, Gateway,
};
use crate::{
    adapters::outbound::{
        identity::{now, Principal},
        publishing::{Guard, Outcome, Publication},
    },
    config::{lifecycle::LifecycleDelivery, messaging::IngressPurpose, publishing::Purpose},
    domain::message::{
        control,
        envelope::{identifier, user_identifier, SessionKey},
        UnixMillis,
    },
    observability::{Metrics, Reservation},
};
use bytes::Bytes;
use serde::Deserialize;
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, Weak},
    time::Duration,
};
use tokio::{
    sync::{oneshot, OwnedSemaphorePermit, Semaphore},
    time::Instant,
};

/// 初始化回复入口接受的唯一消息类别，不与业务下行混用。
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReplyKind {
    /// 仅用于关联待决初始化，还须核对请求、会话与主体。
    InitializationReply,
}

/// 业务授权服务的回复只进入初始化等待者，不具有 push 或路由控制权限。
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct InitializationReply {
    /// 固定初始化回复类别，业务推送不能冒充初始化结果。
    pub kind: ReplyKind,
    /// 必须精确匹配当前待决初始化的请求 ID。
    pub request_id: String,
    /// 回复绑定的完整会话作用域，旧节点或旧会话回复拒绝。
    pub context: SessionKey,
    /// 业务确认的主体，必须与当前已认证用户及租户一致。
    pub principal: Principal,
    /// 受信业务授权结果，仍核对请求关联、版本和固定期限。
    pub response: AuthorizationResponse,
}

impl InitializationReply {
    /// 业务作用：约束回复自身的关联范围，未找到本节点等待者也不接纳畸形身份。
    /// 参数说明: 无。
    ///
    /// 返回：完整且合法的关联头；不在此阶段授予任何权限。
    pub fn validate(&self) -> anyhow::Result<()> {
        identifier(&self.request_id)?;
        self.context.validate()?;
        user_identifier(&self.principal.uid)?;
        identifier(&self.principal.tenant_id)?;
        anyhow::ensure!(
            self.response.request_id == self.request_id,
            "initialization_request_mismatch"
        );
        Ok(())
    }

    /// 业务作用：固定回复在 Kafka 中的排序键，业务服务不能自由指定另一分区。
    /// 参数说明: 无。
    ///
    /// 返回：合同域与精确会话、请求字段的长度前缀字节，不依赖 JSON 字段顺序。
    pub fn ordering_key(&self) -> Vec<u8> {
        ordering_key(&self.context, &self.request_id)
    }
}

/// 业务作用：为初始化请求与回复提供跨语言一致的分区材料。
/// 参数说明：
/// - `key`：服务器签发的精确会话键。
/// - `request_id`：本次初始化的唯一请求标识。
///
/// 返回：固定域分隔与四字节大端长度前缀字段的拼接。
pub fn ordering_key(key: &SessionKey, request_id: &str) -> Vec<u8> {
    let mut bytes = b"nacket.initialization\0".to_vec();
    for field in [
        &key.cluster_id,
        &key.scope_epoch,
        &key.node_id,
        &key.incarnation,
        &key.route_id,
        &key.route_revision,
        &key.definition_digest,
        &key.route_session_id,
        request_id,
    ] {
        bytes.extend_from_slice(&(field.len() as u32).to_be_bytes());
        bytes.extend_from_slice(field.as_bytes());
    }
    bytes
}

struct Waiter {
    /// 精确待初始化连接的弱引用，等待记录不能使已关闭连接复活。
    connection: Weak<Connected>,
    /// 首次登记的单调截止点，重复消息不延长等待。
    deadline: Instant,
    /// 只交付给原初始化任务的一次性响应入口。
    sender: oneshot::Sender<AuthorizationResponse>,
}

/// 数量和完整回复预算在发送请求之前预约，取消或超时即释放待决关联。
pub struct Initializations {
    /// 请求 ID 到精确连接的有限关联目录。
    waiters: Mutex<BTreeMap<String, Waiter>>,
    /// 节点待决初始化数量预算，与业务分发容量分开。
    count: Arc<Semaphore>,
    /// 初始化关联控制信息的独立字节预算。
    bytes: Arc<Semaphore>,
    /// 节点固定初始化指标源，不以请求或用户作为标签。
    metrics: Arc<Metrics>,
}

struct Pending {
    /// 当前任务拥有的唯一待决初始化 ID，结束时精确移除。
    request_id: String,
    /// 持有待决目录及关闭责任的共享 owner。
    owner: Arc<Initializations>,
    /// 仅接收与当前请求和会话完整匹配的授权响应。
    receiver: oneshot::Receiver<AuthorizationResponse>,
    /// 实际等待期间保留的节点初始化名额。
    _count: OwnedSemaphorePermit,
    /// 实际等待期间保留的初始化关联字节预约。
    _bytes: OwnedSemaphorePermit,
    /// 待决初始化数量的生命周期观测责任。
    _observed_count: Reservation,
    /// 待决初始化关联字节的生命周期观测责任。
    _observed_bytes: Reservation,
}

impl Initializations {
    /// 业务作用：建立节点初始化关联预算，进程重启不复用旧会话的等待者。
    /// 参数说明：
    /// - `policy`：冻结的数量与回复内存预算。
    /// - `metrics`：有限资源指标源。
    ///
    /// 返回：空的有界等待目录，不创建网络任务。
    pub fn new(policy: &LifecycleDelivery, metrics: Arc<Metrics>) -> Arc<Self> {
        Arc::new(Self {
            waiters: Mutex::new(BTreeMap::new()),
            count: Arc::new(Semaphore::new(policy.max_pending_initializations)),
            bytes: Arc::new(Semaphore::new(policy.max_initialization_bytes)),
            metrics,
        })
    }

    /// 业务作用：在 MQ 请求提交前安装唯一等待者，防止快速业务回复早于关联登记。
    /// 参数说明：
    /// - `connection`：尚在初始化中的当前连接。
    /// - `deadline`：建连、身份与初始化共同限定的截止点。
    /// - `maximum_reply_bytes`：固定入口的完整控制头上限，提前预约解析及保存责任。
    ///
    /// 返回：持有全部预算的唯一等待者；容量不足时不发送 MQ 请求。
    fn register(
        self: &Arc<Self>,
        connection: &Arc<Connected>,
        deadline: Instant,
        maximum_reply_bytes: usize,
    ) -> anyhow::Result<Pending> {
        anyhow::ensure!(
            deadline > Instant::now()
                && connection.session.live()
                && !connection.cancellation.is_cancelled(),
            "initialization_closed"
        );
        let count = self.count.clone().try_acquire_owned()?;
        let size = maximum_reply_bytes
            .checked_mul(2)
            .and_then(|size| size.checked_add(4096))
            .ok_or_else(|| anyhow::anyhow!("initialization_capacity"))?;
        let bytes = self
            .bytes
            .clone()
            .try_acquire_many_owned(u32::try_from(size)?)?;
        let request_id = control::nonce();
        let (sender, receiver) = oneshot::channel();
        self.waiters
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .insert(
                request_id.clone(),
                Waiter {
                    connection: Arc::downgrade(connection),
                    deadline,
                    sender,
                },
            );
        Ok(Pending {
            request_id,
            owner: self.clone(),
            receiver,
            _count: count,
            _bytes: bytes,
            _observed_count: self.metrics.reserve("initialization_requests", 1),
            _observed_bytes: self.metrics.reserve("initialization_bytes", size as u64),
        })
    }

    /// 业务作用：只把受信、同主体且同会话的回复交给原等待者；旧连接回复不能影响重连。
    /// 参数说明：
    /// - `ingress`：已完成 ACL 或签名认证的固定回复入口。
    /// - `producer`：真实来源身份，不取自 metadata 自报值。
    /// - `reply`：严格解析的初始化回复。
    ///
    /// 返回：本节点存在且仍有效的等待者已获得回复；其它节点、迟到或重复回复不会产生权限。
    pub fn resolve(
        &self,
        ingress: &TrustedIngress,
        producer: &str,
        reply: InitializationReply,
    ) -> anyhow::Result<bool> {
        anyhow::ensure!(
            ingress.configuration.purpose == IngressPurpose::InitializationReply,
            "initialization_source_purpose"
        );
        reply.validate()?;
        let mut waiters = self
            .waiters
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let Some(waiter) = waiters.get(&reply.request_id) else {
            return Ok(false);
        };
        let Some(connection) = waiter.connection.upgrade() else {
            waiters.remove(&reply.request_id);
            return Ok(false);
        };
        let identity = connection.session.identity();
        let policy = connection
            .plan
            .definition
            .lifecycle
            .initialization
            .reply
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("initialization_reply_disabled"))?;
        // 完整关联核验先于移除等待者，错误回复不能抢占合法业务回复的唯一完成权。
        anyhow::ensure!(
            connection.session.key == reply.context
                && identity.uid == reply.principal.uid
                && identity.tenant == reply.principal.tenant_id
                && policy.ingress_ref == ingress.id
                && policy.allowed_producers.contains(producer),
            "initialization_reply_scope_mismatch"
        );
        if waiter.deadline <= Instant::now()
            || !connection.session.live()
            || connection.cancellation.is_cancelled()
            || connection.is_ready()
        {
            waiters.remove(&reply.request_id);
            return Ok(false);
        }
        let waiter = waiters.remove(&reply.request_id).expect("validated waiter");
        Ok(waiter.sender.send(reply.response).is_ok())
    }
}

impl Drop for Pending {
    /// 业务作用：撤销本次初始化关联，断连、取消和超时后迟到回复不得留下可复用授权。
    /// 参数说明: 无。
    ///
    /// 返回：等待项及预约容量释放；已经移交 MQ 的原始请求不伪装成撤销成功。
    fn drop(&mut self) {
        self.owner
            .waiters
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .remove(&self.request_id);
    }
}

impl Gateway {
    /// 业务作用：发布固定初始化请求并等待独立受信回复，broker 接受本身不代表获得授权。
    /// 参数说明：
    /// - `connection`：尚未 Ready 的精确连接。
    /// - `publisher_ref`：配置批准的生命周期发布器。
    /// - `deadline`：原始建连截止时间。
    ///
    /// 返回：经关联与期限验证的授权响应；超时、关闭或无效回复不更新连接权限。
    pub(super) async fn initialize_mq(
        &self,
        connection: &Arc<Connected>,
        publisher_ref: &str,
        deadline: Instant,
    ) -> anyhow::Result<(AuthorizationResponse, Instant)> {
        let hook = &connection.plan.definition.lifecycle.initialization;
        let policy = hook
            .reply
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("initialization_reply_required"))?;
        let ingress = self
            .configuration
            .ingresses
            .get(&policy.ingress_ref)
            .ok_or_else(|| anyhow::anyhow!("initialization_ingress_missing"))?;
        let identity = connection.session.identity();
        let started = Instant::now();
        let deadline = deadline
            .min(identity.deadline)
            .min(started + Duration::from_millis(hook.timeout_ms));
        let wall = now()?;
        let expires = UnixMillis::new(
            wall.as_millis() + deadline.saturating_duration_since(started).as_millis() as u64,
        )
        .map_err(anyhow::Error::msg)?;
        let mut pending =
            self.initializations
                .register(connection, deadline, ingress.max_metadata_bytes)?;
        let request = serde_json::json!({
            "contract": "nacket.business/1",
            "kind": "initialization_request",
            "requestId": pending.request_id,
            "operation": "initialize",
            "context": CallContext {
                key: &connection.session.key,
                attempt_id: &connection.attempt_id
            },
            "principal": {
                "uid": identity.uid,
                "tenantId": identity.tenant
            },
            "createdAt": wall,
            "expiresAt": expires,
            "bodyPresent": false,
        });
        let publication = Publication {
            metadata: Bytes::from(serde_json::to_vec(&request)?),
            body: Bytes::new(),
            ordering_key: ordering_key(&connection.session.key, &pending.request_id),
            deadline,
            guard: Guard::Initialization(Arc::downgrade(connection)),
        };
        let publisher = self
            .publishers
            .get()
            .ok_or_else(|| anyhow::anyhow!("publisher_unavailable"))?;
        tokio::time::timeout_at(deadline, async {
            tokio::select! {
                biased;
                _ = connection.cancellation.cancelled() => anyhow::bail!("initialization_closed"),
                result = async {
                    let published = publisher
                        .publish(
                            publisher_ref,
                            Purpose::Lifecycle,
                            &connection.plan.definition.route_id,
                            publication,
                        )
                        .await;
                    anyhow::ensure!(
                        published != Outcome::NotDispatched,
                        "initialization_not_dispatched"
                    );
                    // 未知发布仍可能已有合法回复；只等待原请求，不能生成新请求扩大授权寿命。
                    let response = (&mut pending.receiver).await?;
                    let (_, _, expires) = response.evidence_with_ttl(
                        &pending.request_id,
                        policy.max_grant_ttl_ms,
                        started,
                        wall,
                    )?;
                    Ok::<_, anyhow::Error>((response, expires))
                } => result,
            }
        })
        .await?
    }
}
