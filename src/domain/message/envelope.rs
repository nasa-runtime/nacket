//! 固定控制信封与通信方向；业务体不进入 JSON 模型。

use super::{DeliveryWindow, UnixMillis};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;

/// 同一个稳定连接在节点、路由和控制代际中的精确身份。
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SessionKey {
    /// 精确会话所属集群身份，参与所有内部关联检查。
    pub cluster_id: String,
    /// 会话所属控制基线的十进制代际，旧基线不能影响新作用域。
    pub scope_epoch: String,
    /// 创建会话的稳定节点 ID，单独此值不足以定位一次实例。
    pub node_id: String,
    /// 创建会话的节点实例十进制代际，节点重启后旧会话键失效。
    pub incarnation: String,
    /// 会话所属路由 ID，与租户身份一起约束业务权限。
    pub route_id: String,
    /// 会话创建时冻结的十进制路由 revision。
    pub route_revision: String,
    /// 会话绑定的完整定义摘要，阻止同 revision 内容冲突。
    pub definition_digest: String,
    /// 服务端生成的本次路由会话唯一 ID，断线重连必须重新生成。
    pub route_session_id: String,
}

/// 权威路由定义与租户作用域；生产者不能用缺省值表示通配。
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Context {
    /// 业务消息所属集群，必须匹配部署受权范围。
    pub cluster_id: String,
    /// 消息所属控制作用域十进制代际，不能跨恢复基线复用。
    pub scope_epoch: String,
    /// 经业务生产者确认的租户隔离键，客户端不能自行声明覆盖。
    pub tenant_id: String,
    /// 业务消息明确选择的路由 ID，仍需来源及目标策略允许。
    pub route_id: String,
    /// 业务消息生成时绑定的十进制路由 revision。
    pub route_revision: String,
    /// 消息必须匹配的完整冻结定义摘要，不能把旧正文套用新 schema。
    pub definition_digest: String,
}

/// 单选目标，不接受其它目标类别的附加字段。
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Target {
    /// 交给服务端动作处理，不构成客户端下行目标。
    Server {},
    /// 仅回复当前请求会话，不能跨连接选择接收者。
    #[serde(rename = "self")]
    Current {},
    /// 选择同租户路由内一个订阅目标，接收会话必须持有有效资格。
    Group {
        #[serde(rename = "groupId")]
        /// 单个订阅目标标识，读取仍要求当前会话持有有效成员与群版本证据。
        group_id: String,
    },
    /// 选择同租户路由内有限用户，仍需来源权限允许。
    Uids {
        /// 同租户、同路由内有限用户目标列表，不赋予生产者额外发送权限。
        uids: Vec<String>,
    },
    /// 选择内部完整会话键，旧节点代际或旧会话不能命中新连接。
    Session {
        #[serde(rename = "sessionKey")]
        /// 用于内部精确会话目标的完整绑定，不能靠单个连接 ID 跨节点定位。
        session_key: SessionKey,
    },
    /// 广播范围限制在单一租户和路由，必须显式开放该目标能力。
    TenantRouteBroadcast {},
}

/// 信封类别与接入操作必须一致，不能以自报 control 获取管理权限。
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Kind {
    /// 受信业务来源的下行，客户端不能以此类别直接转发。
    Push,
    /// 请求执行路由允许的服务动作。
    Notify,
    /// 请求向其它接收者发送，默认关闭且必须经过业务权限校验。
    Msg,
    /// 申请单个目标的订阅资格。
    Sub,
    /// 撤销本会话的订阅并等待旧写出屏障。
    Unsub,
    /// 向原请求者返回操作结果，不表示其它终端已接收。
    Result,
    /// 受信控制用途，声明此类别本身不授予控制权限。
    Control,
}

/// 来源展示信息，由可信生产者确认，不构成来源认证。
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Sender {
    /// 受信来源确认的发送用户，用于业务归属和受限公开投影。
    pub uid: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// 可选的原发送会话完整键，排除回显或重试时必须保持原关联。
    pub session_key: Option<SessionKey>,
}

/// 生产者提供的有界投递时间与排除条件，不包含服务端派生回执。
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Delivery {
    /// 生产者首次确定的 Unix 毫秒创建时刻，outbox 重试不得刷新。
    pub created_at: UnixMillis,
    /// 消息的 Unix 毫秒最晚投递时刻，不能超路由 TTL 或扩大接收资格。
    pub expires_at: UnixMillis,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    /// 从已允许目标中排除的用户，排除其全部匹配会话，不扩大接收范围。
    pub exclude_uids: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// 从已允许目标中排除的单个精确会话，不影响用户其它合法连接。
    pub exclude_session_key: Option<SessionKey>,
}

/// MQ Push 的唯一 metadata 布局，显式排除客户端及节点控制字段。
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Push {
    /// 受信下行固定类别，业务来源不能冒用任意内部控制方向。
    pub kind: Kind,
    /// 生产者稳定消息 ID，同 ID 的冲突内容拒绝，重投保留原身份。
    pub id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// 可选业务请求关联标识，不参与取得发送或接收权限。
    pub correlation_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// 可选的稳定业务操作 ID，与业务幂等结果关联。
    pub operation_id: Option<String>,
    /// 完整部署、租户与冻结路由绑定，消费时逐项复验。
    pub context: Context,
    /// 当前路由允许的业务事件名，决定 schema 和单条正文容量。
    pub event: String,
    /// 事件对应的固定 schema 引用，不能自行改变路由序列化方案。
    pub schema: String,
    /// 业务授权后的目标声明，消费仍检查生产者、租户和接收会话资格。
    pub target: Target,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// 可选可信发送主体及原会话关联，不使用客户端自报身份。
    pub sender: Option<Sender>,
    /// 固定原始时间和只收窄目标的排除条件，重投不延长期限。
    pub delivery: Delivery,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// 可选受限链路关联标识，不作为监控标签或任何授权证据。
    pub trace_id: Option<String>,
}

/// 客户端只能复述当前路由代次，其余身份上下文从服务端会话取得。
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ClientContext {
    /// 客户端理解的十进制路由 revision，真实租户及完整绑定由服务端补齐。
    pub route_revision: String,
}

/// 上行没有 sender、delivery 或任意租户字段；业务体由独立帧区承载。
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ClientRequest {
    /// 当前方向允许的 sub/unsub/notify/msg 动作类别，不能调用内部控制动作。
    pub kind: Kind,
    /// 当前会话内的请求关联 ID，同 ID 必须保持相同请求指纹。
    pub id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// 有副作用操作使用的稳定业务幂等 ID，与会话请求 ID 分工不同。
    pub operation_id: Option<String>,
    /// 客户端有限上下文，只允许声明当前路由 revision。
    pub context: ClientContext,
    /// 待执行的固定动作或路由白名单事件，不是可任意分发的事件名。
    pub event: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// 可选独立正文的 schema 引用，与动作和当前唯一编码核对。
    pub schema: Option<String>,
    /// 候选操作目标，默认互发关闭；即使能力打开也需业务授权收窄。
    pub target: Target,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// 可选受限链路标识，不能用于选择远程地址或绕过请求关联。
    pub trace_id: Option<String>,
}

/// 明确区分未执行、受理、完成和结果未知，防止一次 HTTP 成功被解释为终端收到。
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Effect {
    /// 明确未产生本次业务副作用。
    None,
    /// 执行方已受理，尚未证明业务完成。
    Accepted,
    /// 操作执行已明确完成，不自动代表终端投递完成。
    Completed,
    /// 操作可能已经发生，缺少确定结果，不能据此盲目重放。
    Unknown,
}

/// 固定控制结果；业务返回对象需要独立 schema 与业务体。
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ActionResult {
    /// 有限公开结果码，不透出内部错误文本或远程凭据。
    pub code: String,
    /// 当前操作报告的处理阶段，不能将排队或受理混为业务完成。
    pub phase: String,
    /// 明确区分无副作用、已接纳、已完成和结果未知，超时不能伪装未执行。
    pub effect: Effect,
    /// 是否允许按当前合同重试；副作用操作仍必须沿用稳定幂等身份。
    pub retryable: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// 可选重试退避建议，单位毫秒，不延长原授权或消息期限。
    pub retry_after_ms: Option<u64>,
}

/// 转发期间保留原始业务体，克隆只增加共享引用计数。
#[derive(Clone)]
pub struct Message<M> {
    /// 按当前通信方向严格验证的控制信息，与业务正文分开持有。
    pub metadata: M,
    /// 可选共享的原始正文 Bytes，克隆只延续 owner，不重建业务对象。
    pub body: Option<Bytes>,
}

/// 业务作用：验证跨语言标识符，限制日志、索引和路由键的内存占用。
/// 参数说明：
/// - `value`：标识符原文。
///
/// 返回：非空、有界且无控制字符时成功，不归一化业务 ID。
pub fn identifier(value: &str) -> anyhow::Result<()> {
    anyhow::ensure!(
        !value.is_empty() && value.len() <= 128 && !value.chars().any(char::is_control),
        "invalid_identifier"
    );
    Ok(())
}

/// 业务作用：拒绝无法建立可信用户主体的标识，避免空白身份进入会话和 uid 顺序域。
/// 参数说明：
/// - `value`：身份来源验证后取得的 uid 原文。
///
/// 返回：非空白且满足标识符边界时成功；拒绝结果不包含身份原文，不归一化合法 ID。
pub fn user_identifier(value: &str) -> anyhow::Result<()> {
    identifier(value)?;
    anyhow::ensure!(
        value.chars().any(|character| !character.is_whitespace()),
        "invalid_uid"
    );
    Ok(())
}

/// 业务作用：验证以字符串承载的单调代次，禁止前导零和跨语言整数截断。
/// 参数说明：
/// - `value`：十进制正整数字符串。
///
/// 返回：可比较的 u64；非规范形式或溢出拒绝。
pub fn revision(value: &str) -> anyhow::Result<u64> {
    anyhow::ensure!(
        !value.starts_with('0') && !value.is_empty() && value.bytes().all(|b| b.is_ascii_digit()),
        "invalid_revision"
    );
    Ok(value.parse()?)
}

impl Context {
    /// 业务作用：检查完整受信作用域的语法，不把结构有效当作来源授权。
    /// 参数说明: 无。
    ///
    /// 返回：各字段有界且摘要和代次规范；缺失作用域拒绝。
    pub fn validate(&self) -> anyhow::Result<()> {
        for id in [&self.cluster_id, &self.tenant_id, &self.route_id] {
            identifier(id)?;
        }
        revision(&self.scope_epoch)?;
        revision(&self.route_revision)?;
        validate_digest(&self.definition_digest)
    }
}

impl SessionKey {
    /// 业务作用：验证精确连接引用，防止不完整引用跨节点误排除或误回执。
    /// 参数说明: 无。
    ///
    /// 返回：完整且规范的会话键有效；任一缺失字段拒绝。
    pub fn validate(&self) -> anyhow::Result<()> {
        for id in [
            &self.cluster_id,
            &self.node_id,
            &self.incarnation,
            &self.route_id,
            &self.route_session_id,
        ] {
            identifier(id)?;
        }
        revision(&self.scope_epoch)?;
        revision(&self.route_revision)?;
        validate_digest(&self.definition_digest)
    }
}

/// 业务作用：约束公开 SHA-256 表示，避免摘要比较接受多种编码形式。
/// 参数说明：
/// - `value`：带 sha256 前缀的小写十六进制摘要。
///
/// 返回：固定长度和字符集正确时成功。
pub fn validate_digest(value: &str) -> anyhow::Result<()> {
    anyhow::ensure!(
        value.len() == 71
            && value.starts_with("sha256:")
            && value.as_bytes()[7..]
                .iter()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(b)),
        "invalid_digest"
    );
    Ok(())
}

impl Target {
    /// 业务作用：检查目标大小和重复项，防止单记录产生无界扇出计划。
    /// 参数说明: 无。
    ///
    /// 返回：结构合法且集合有限；权限仍由当前 ingress 与路由检查。
    pub fn validate(&self) -> anyhow::Result<()> {
        match self {
            Self::Group {
                group_id,
            } => identifier(group_id)?,
            Self::Uids {
                uids,
            } => validate_ids(uids, 1024, false)?,
            Self::Session {
                session_key,
            } => session_key.validate()?,
            _ => {}
        }
        Ok(())
    }
}

/// 业务作用：限制身份集合数量并拒绝重复项，维持指纹与实际目标一致。
/// 参数说明：
/// - `ids`：待验证身份集合。
/// - `maximum`：允许的最大条数。
/// - `empty`：当前集合是否允许为空。
///
/// 返回：唯一、有界的身份列表；不静默截断或去重。
fn validate_ids(ids: &[String], maximum: usize, empty: bool) -> anyhow::Result<()> {
    anyhow::ensure!(
        ids.len() <= maximum && (empty || !ids.is_empty()),
        "target_capacity"
    );
    let mut seen = BTreeSet::new();
    for id in ids {
        identifier(id)?;
        anyhow::ensure!(seen.insert(id), "duplicate_identity");
    }
    Ok(())
}

impl Push {
    /// 业务作用：在建立投递计划前验证业务入口允许的 Push profile 与固定存活窗口。
    /// 参数说明：
    /// - `now`：消费时的整数毫秒时间。
    /// - `maximum_ttl_ms`：部署与路由共同允许的存活上限。
    /// - `future_skew_ms`：允许的生产者时钟偏差。
    ///
    /// 返回：剩余时长；非法目标、元数据或过期消息不进入分发。
    pub fn validate(
        &self,
        now: UnixMillis,
        maximum_ttl_ms: u64,
        future_skew_ms: u64,
    ) -> anyhow::Result<std::time::Duration> {
        anyhow::ensure!(
            self.kind == Kind::Push
                && matches!(
                    self.target,
                    Target::Group { .. } | Target::Uids { .. } | Target::TenantRouteBroadcast {}
                ),
            "invalid_push_profile"
        );
        for id in [&self.id, &self.event, &self.schema] {
            identifier(id)?;
        }
        for id in [&self.correlation_id, &self.operation_id, &self.trace_id]
            .into_iter()
            .flatten()
        {
            identifier(id)?;
        }
        self.context.validate()?;
        self.target.validate()?;
        if let Some(sender) = &self.sender {
            identifier(&sender.uid)?;
            if let Some(key) = &sender.session_key {
                key.validate()?;
            }
        }
        validate_ids(&self.delivery.exclude_uids, 1024, true)?;
        if let Some(key) = &self.delivery.exclude_session_key {
            key.validate()?;
        }
        DeliveryWindow {
            created_at: self.delivery.created_at,
            expires_at: self.delivery.expires_at,
        }
        .remaining(now, maximum_ttl_ms, future_skew_ms)
    }

    /// 业务作用：派生不暴露生产者权限域的稳定终端去重键，跨节点与重投保持一致。
    /// 参数说明：
    /// - `producer`：从 ACL 隔离入口或验签确定的真实生产者身份。
    ///
    /// 返回：域分隔、长度前缀编码后的 SHA-256；自报来源不参与信任建立。
    pub fn message_key(&self, producer: &str) -> String {
        let mut hash = Sha256::new();
        hash.update(b"nacket.message-key\0");
        for item in [
            &self.context.cluster_id,
            &self.context.scope_epoch,
            producer,
            &self.context.route_id,
            &self.context.route_revision,
            &self.context.definition_digest,
            &self.context.tenant_id,
            &self.id,
        ] {
            hash.update((item.len() as u32).to_be_bytes());
            hash.update(item.as_bytes());
        }
        format!("sha256:{:x}", hash.finalize())
    }

    /// 业务作用：识别相同去重键下的内容冲突，不把不同目标或正文当作合法重投。
    /// 参数说明：
    /// - `body_digest`：对实际原始业务体计算一次得到的摘要。
    ///
    /// 返回：规范化权限语义与正文摘要的联合指纹；观测 trace 不影响业务身份。
    pub fn fingerprint(&self, body_digest: &[u8; 32]) -> anyhow::Result<[u8; 32]> {
        let mut normalized = self.clone();
        normalized.trace_id = None;
        normalized.delivery.exclude_uids.sort();
        if let Target::Uids {
            uids,
        } = &mut normalized.target
        {
            uids.sort();
        }
        let mut hash = Sha256::new();
        hash.update(b"nacket.message-fingerprint\0");
        hash.update(serde_json::to_vec(&normalized)?);
        hash.update(body_digest);
        Ok(hash.finalize().into())
    }
}

impl ClientRequest {
    /// 业务作用：阻止客户端填写受信下行或管理类型，按操作约束候选目标。
    /// 参数说明：
    /// - `body_present`：帧头明确给出的业务体存在标志，零长度仍视为存在。
    ///
    /// 返回：合法上行结构；动作权限需由路由策略另行授予。
    pub fn validate(&self, body_present: bool) -> anyhow::Result<()> {
        identifier(&self.id)?;
        identifier(&self.event)?;
        revision(&self.context.route_revision)?;
        for id in [&self.operation_id, &self.trace_id, &self.schema]
            .into_iter()
            .flatten()
        {
            identifier(id)?;
        }
        anyhow::ensure!(
            self.schema.is_some() == body_present,
            "schema_body_mismatch"
        );
        self.target.validate()?;
        let allowed = match self.kind {
            Kind::Sub | Kind::Unsub => {
                self.target == (Target::Server {})
                    && self.event
                        == if self.kind == Kind::Sub {
                            "sub"
                        } else {
                            "unsub"
                        }
                    && body_present
            }
            Kind::Notify => self.target == (Target::Server {}),
            Kind::Msg => {
                matches!(self.target, Target::Group { .. } | Target::Uids { .. })
                    && body_present
                    && self.operation_id.is_some()
            }
            _ => false,
        };
        anyhow::ensure!(allowed, "invalid_client_profile");
        Ok(())
    }
}
