//! 产品握手与下行固定控制模型，不随业务 schema 或 Socket.IO 参数格式改变。

use super::{
    envelope::{identifier, ActionResult, Kind, Target},
    UnixMillis,
};
use crate::domain::route::Serialization;
use nasa::ws::proto::dynamic::SchemaDescriptor;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// 公开 wire contract 标识，变更字段解释必须采用新的显式入口合同。
pub const CONTRACT: &str = "nacket.split-envelope";

/// 客户端能力只用于确认能否使用路由配置，不能协商另一种业务编码。
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Capabilities {
    /// 客户端能够解码的模式集合，只用于检查是否包含当前路由唯一模式，不参与逐消息协商。
    pub serializations: Vec<Serialization>,
    /// 客户端可接收的完整帧字节上限，与服务端上限取较小值。
    pub max_frame_bytes: usize,
    /// 客户端是否支持二进制附件；需要附件的适配器不能降级为字符串正文。
    pub binary_attachments: bool,
}

/// 每次连接只允许一个认证尝试，身份不接受客户端自行声明 uid 或 tenant。
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Authenticate {
    /// 明确声明的外部握手合同，服务端必须精确核对后继续认证。
    pub contract: String,
    /// 客户端申请的逻辑接入路径，由受权监听和路由目录解析，不能指定远程服务地址。
    pub endpoint: String,
    /// 本次认证尝试的唯一关联 ID，确认、Ready 和状态通知沿用该值。
    pub attempt_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// 当前身份策略所需的原始认证材料，不作为 uid 或租户的自报值。
    pub token: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// 路由允许的有限附加标量；null 等同未提供，不能覆盖服务端主体或上下文。
    pub auth_data: Option<std::collections::BTreeMap<String, serde_json::Value>>,
    /// 客户端对编码、帧容量与附件的支持声明，不能改变路由权限。
    pub capabilities: Capabilities,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// 可选目录绑定期望；不匹配时在远程身份调用前拒绝，避免凭据交给其它路由。
    pub expected_binding: Option<ExpectedBinding>,
}

/// 可选目录绑定只用于收紧接入目标，不能替代凭据、路由门禁或业务授权。
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ExpectedBinding {
    /// 期望的部署集群身份，目录查询结果不能跨集群复用。
    pub cluster_id: String,
    /// 期望的控制作用域十进制代际，旧基线目录不能授权新连接。
    pub scope_epoch: String,
    /// 期望接入的稳定节点 ID，实际转发目标必须匹配。
    pub node_id: String,
    /// 期望节点实例的十进制代际，重启后的实例不能继承旧绑定。
    pub incarnation: String,
    /// 期望路由 ID，必须与实际监听解析出的路由相同。
    pub route_id: String,
    /// 期望路由的十进制 revision，旧定义不能通过同地址继续接入。
    pub route_revision: String,
    /// 期望完整冻结定义的摘要，防止同 revision 内容分歧。
    pub definition_digest: String,
    /// 期望实际监听 owner 的绑定 ID，端口复用不能复活旧目录声明。
    pub binding_id: String,
}

impl ExpectedBinding {
    /// 业务作用：限制目录定位材料为完整且规范的精确绑定，部分字段不能冒充稳定目标。
    /// 参数说明: 无。
    ///
    /// 返回：完整字段合法；匹配本节点仍由接入过程在身份调用前执行。
    pub fn validate(&self) -> anyhow::Result<()> {
        for value in [
            &self.cluster_id,
            &self.node_id,
            &self.route_id,
            &self.binding_id,
        ] {
            identifier(value)?;
        }
        for value in [&self.scope_epoch, &self.incarnation, &self.route_revision] {
            crate::domain::message::envelope::revision(value)?;
        }
        anyhow::ensure!(
            self.definition_digest.len() == 71
                && self.definition_digest.starts_with("sha256:")
                && self.definition_digest[7..]
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)),
            "expected_binding_invalid"
        );
        Ok(())
    }
}

impl Authenticate {
    /// 业务作用：限制未认证输入及能力范围，保证同一连接不能以能力声明扩大服务能力。
    /// 参数说明: 无。
    ///
    /// 返回：固定合同和有界请求；未知合同、重复模式或异常凭据长度拒绝。
    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(self.contract == CONTRACT, "unsupported_contract");
        if let Some(values) = &self.auth_data {
            anyhow::ensure!(
                values.len() <= 16 && serde_json::to_vec(values)?.len() <= 8192,
                "auth_data_capacity"
            );
            for (key, value) in values {
                identifier(key)?;
                anyhow::ensure!(
                    !value.is_array() && !value.is_object(),
                    "auth_data_scalar_required"
                );
            }
        }
        identifier(&self.attempt_id)?;
        if let Some(expected) = &self.expected_binding {
            expected.validate()?;
        }
        anyhow::ensure!(
            self.endpoint.starts_with('/')
                && self.endpoint.len() <= 256
                && !self.endpoint.chars().any(char::is_control),
            "invalid_endpoint"
        );
        anyhow::ensure!(
            self.token
                .as_ref()
                .is_none_or(|t| !t.is_empty() && t.len() <= 8192),
            "invalid_credentials"
        );
        anyhow::ensure!(
            !self.capabilities.serializations.is_empty()
                && self.capabilities.serializations.len() <= 4,
            "invalid_capabilities"
        );
        let mut modes = std::collections::BTreeSet::new();
        for mode in &self.capabilities.serializations {
            anyhow::ensure!(modes.insert(mode.mode().ordinal()), "duplicate_capability");
        }
        anyhow::ensure!(
            (1024..=64 * 1024 * 1024).contains(&self.capabilities.max_frame_bytes),
            "invalid_frame_capacity"
        );
        Ok(())
    }
}

/// 对客户端公开的当前定义，只包含理解业务线路所需的数据。
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PublicRoute {
    /// 当前认证得到的路由 ID，后续消息绑定该逻辑入口。
    pub route_id: String,
    /// 当前路由定义的十进制 revision，终端确认时不能忽略定义变化。
    pub route_revision: String,
    /// 当前冻结定义的摘要，用于确认和消息上下文关联。
    pub definition_digest: String,
    /// 由业务配置选择的唯一 ProtocolBytes 模式，所有适配器共同使用。
    pub serialization: Serialization,
    /// 该路由公开的有限 schema 描述，终端不得以任意字段结构替换。
    #[cfg_attr(
        feature = "schema",
        schemars(with = "BTreeMap<String, crate::config::schema::Descriptor>")
    )]
    pub schemas: BTreeMap<String, SchemaDescriptor>,
    /// 路由是否提供 sub/unsub 能力；true 不表示已取得任意订阅的权限。
    pub subscriptions: bool,
    /// 是否允许 SDK 在重连 Ready 后自动重新申请期望群；仍须逐次授权，不继承旧资格。
    pub restore_subscriptions: bool,
    /// 是否配置客户端 msg 能力；开启后仍需业务授权，不代表直接广播。
    pub client_messages: bool,
    /// 路由声明的可用通知操作名，实际执行仍检查当前身份及授权。
    pub notifications: Vec<String>,
}

/// 成功认证仍需确认及初始化，hello 不代表可以执行业务。
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Hello {
    /// 服务端确定的外部握手合同标识，与客户端声明必须一致。
    pub contract: &'static str,
    /// 当前认证尝试关联 ID，终端不能使用旧尝试的响应进行确认。
    pub attempt_id: String,
    /// 服务端为本次连接创建的精确会话 ID，尚不代表初始化已完成。
    pub session_id: String,
    /// 本次确认的一次性随机材料，绑定身份与冻结路由定义。
    pub confirm_nonce: String,
    /// 当前身份及附加连接授权收窄后的 Unix 毫秒截止时间，心跳不能延长。
    pub expires_at: UnixMillis,
    /// 仅向当前终端公开的路由能力与 schema，不包含内部控制身份。
    pub route: PublicRoute,
}

/// 客户端复述本次随机确认材料与定义摘要，不能修改服务器选择的编码。
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Confirm {
    /// 必须与本次 Hello 相同的认证尝试 ID。
    pub attempt_id: String,
    /// 终端已接受的路由定义摘要，不匹配时不能进入 Ready。
    pub definition_digest: String,
    /// 原样回传本次 Hello 的一次性确认材料，重复或旧值不能建立新权限。
    pub confirm_nonce: String,
}

/// 所有门禁完成后才产生的接流事实，绝对时间使用整数毫秒。
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Ready {
    /// 完成确认和初始化的认证尝试 ID。
    pub attempt_id: String,
    /// 当前已经获得业务准入的精确会话 ID。
    pub session_id: String,
    /// 实际接纳的十进制路由 revision，不能用旧 Ready 激活新连接。
    pub route_revision: String,
    /// 服务端生成就绪事实的 Unix 毫秒整数，不作为新的授权有效期。
    pub issued_at: UnixMillis,
}

/// 心跳必须匹配未完成的服务器随机探测，普通业务包不延长活性。
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Heartbeat {
    /// 本轮心跳的精确关联随机值，旧回复不能解除新一轮超时。
    pub nonce: String,
}

/// 关闭只描述本连接事实，不允许客户端提交管理动作。
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Close {
    /// 有限公开关闭原因码，不包含远程地址、凭据或原始错误文本。
    pub code: String,
}

/// 下行只公开本连接路由信息，隐藏集群和其它租户的内部作用域。
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PublicContext {
    /// 当前下行所属路由 ID，不公开集群和租户控制上下文。
    pub route_id: String,
    /// 当前下行绑定的十进制路由 revision。
    pub route_revision: String,
    /// 当前下行绑定的完整定义摘要，终端据此拒绝旧定义数据。
    pub definition_digest: String,
}

/// 对终端只展示可信发送用户，不公开节点代际和内部会话作用域。
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Serialize)]
pub struct PublicSender {
    /// 经业务来源或授权服务确认且允许公开的发送主体，不接受上行伪造。
    pub uid: String,
}

/// 回执层级只能由当前路由选择，不能把接纳消息等同于业务处理成功。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum AckLevel {
    /// SDK 确认接收到对应消息，不表示应用完成业务处理。
    SdkReceived,
    /// 应用明确确认处理结果，SDK 接收不能代替此层级。
    ApplicationProcessed,
}

/// 服务端为本次会话写出派生的不可猜测回执信息。
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DeliveryAck {
    /// 本次精确会话投递的不可猜测票据，只能确认对应写出尝试。
    pub token: String,
    /// 路由要求的 SDK 接收或应用处理确认层级，不能互相替代。
    pub level: AckLevel,
    /// 本票据的 Unix 毫秒截止时刻，不超过消息与当前资格期限。
    pub deadline: UnixMillis,
}

/// 投递下行隐藏 producer 排除项和用户收件列表，messageKey 仅用于同域去重。
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PublicDelivery {
    /// 原业务消息创建时间，Unix 毫秒整数，重投不能刷新。
    pub created_at: UnixMillis,
    /// 原业务消息最晚投递时间，Unix 毫秒整数，不延长订阅或身份权限。
    pub expires_at: UnixMillis,
    /// 同一受信消息域的稳定去重键，不包含其它接收者名单。
    pub message_key: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// 该次投递可选的终端确认票据；未配置回执时省略。
    pub ack: Option<DeliveryAck>,
}

/// 对客户端的信封投影；message 仍在外层帧的独立原始字节区。
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Downstream {
    /// 服务端确定的下行类别，操作结果和业务推送使用不同语义。
    pub kind: Kind,
    /// 当前下行消息标识，业务重投沿用稳定消息身份。
    pub id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// 可选的原请求关联 ID，仅关联结果，不授予任何投递权。
    pub correlation_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// 可选业务幂等操作 ID，跨连接重试仍由业务服务决定处理结果。
    pub operation_id: Option<String>,
    /// 面向当前终端收窄后的路由绑定，不透出内部节点权威材料。
    pub context: PublicContext,
    /// 当前路由目录中的事件名或固定操作结果事件。
    pub event: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// 独立正文使用的 schema 引用；没有正文时按方向合同省略。
    pub schema: Option<String>,
    /// 已经按接收方向收窄的目标，不暴露完整用户收件列表或其它会话键。
    pub target: Target,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// 可选受信发送主体的公开投影，不从原始上行字段直接复制。
    pub sender: Option<PublicSender>,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// 业务推送的原始期限、稳定键和可选回执；操作结果按合同省略。
    pub delivery: Option<PublicDelivery>,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// 操作结果的固定状态与副作用语义，不能用成功传输代替业务完成。
    pub result: Option<ActionResult>,
}

/// 终端回执仅确认当前连接的已写出尝试，不触发广播或 MQ 重投。
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Receipt {
    /// 必须命中当前会话已经取得写出资格的票据，不能跨会话确认。
    pub token: String,
    /// 必须与票据原消息一致的稳定键，防止串联其它投递尝试。
    pub message_key: String,
    /// 必须与服务器票据协商层级一致，SDK 接收不能冒充应用处理。
    pub level: AckLevel,
    /// 终端声明接纳或拒绝该层级的结果，不表示用户阅读，也不触发重投。
    pub accepted: bool,
}

/// 业务作用：生成一次性连接、请求或回执材料，避免跨连接猜测关联键。
/// 参数说明: 无。
///
/// 返回：由操作系统随机源生成的 UUID，不含用户或凭据内容。
pub fn nonce() -> String {
    uuid::Uuid::new_v4().to_string()
}
