//! 路由能力上限与固定镜像内置动作，运行事实不作为可写配置。

use super::Toggle;
use crate::domain::message::control::AckLevel;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

/// 路由激活门槛随定义冻结，省略时继承部署默认值。
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase", deny_unknown_fields)]
pub struct Activation {
    /// 上线至少需要的 Ready 节点数；空值继承部署配置，不代表零节点即可上线。
    pub min_ready_nodes: Option<usize>,
}

impl Activation {
    /// 业务作用：省略完全继承的配置，保持未设置路由覆盖时的定义摘要稳定。
    /// 参数说明: 无。
    ///
    /// 返回：未配置独立门槛时为真。
    pub fn is_inherited(&self) -> bool {
        self.min_ready_nodes.is_none()
    }
}

/// 可选远程授权阶段，关闭不移除其它身份或订阅授权要求。
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase", deny_unknown_fields)]
pub struct RemotePolicy {
    /// 是否执行所属业务阶段的额外远程授权或刷新，默认不开启。
    pub enabled: bool,
    /// 开启时必须提供的固定端点引用，关闭不把拒绝转换成允许。
    pub endpoint_ref: Option<String>,
}

/// 跨接入共用的群 ID 约束；固定布局采用单个 i64 的规范十进制表示。
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GroupIdFormat {
    /// 以受限 UTF-8 字符串表示订阅目标。
    #[default]
    Utf8,
    /// 使用单个 i64 的规范十进制文本，供固定布局协议跨语言精确传递。
    I64Decimal,
}

/// 群读取权威来源；没有失败后自动允许的策略。
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
pub enum SubscriptionAuthorization {
    /// 每次申请向固定业务接口取得有限成员授权。
    RemoteHttp {
        #[serde(rename = "endpointRef")]
        /// 逐次订阅授权使用的固定端点，返回结果仍受当前意图和撤权下界约束。
        endpoint_ref: String,
    },
    /// 仅允许显式声明为公共的目标，不推定其它目标也可读取。
    PublicAllowlist {
        /// 明确无需私有成员资格的公共群允许列表，不接受任意群 ID。
        groups: BTreeSet<String>,
    },
    /// 使用当前连接已取得的有限授权快照，仍复验期限与撤权下界。
    ConnectionGrants {},
}

/// 初始化订阅是显式策略，不能覆盖同一连接较新的取消意图。
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
pub enum InitialSubscriptions {
    /// 不自动申请，等待客户端显式 sub。
    None {},
    /// 对部署列出的目标逐一走正常订阅授权。
    Configured {
        #[serde(rename = "groupIds")]
        /// 连接初始化后希望自动申请的有限群列表，仍走相同授权和容量流程。
        group_ids: Vec<String>,
    },
    /// 从连接授权快照申请初始订阅，不能覆盖后续取消意图。
    AuthorizedSnapshot {},
}
impl Default for InitialSubscriptions {
    /// 业务作用：默认不建立隐式群订阅，等待连接明确申请读取范围。
    /// 参数说明: 无。
    ///
    /// 返回：无初始订阅策略。
    fn default() -> Self {
        Self::None {}
    }
}

/// 订阅开关独立于客户端发送；关闭仍允许清理本连接既有订阅。
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase", deny_unknown_fields)]
pub struct Subscriptions {
    /// 是否接受新的 sub；关闭后仍允许 unsub 清理当前会话。
    pub enabled: bool,
    /// 群 ID 的规范格式，FAST_FIXED 订阅要求单个 i64 十进制值。
    pub group_id_format: GroupIdFormat,
    /// 启用订阅时唯一读取授权来源，不能以无校验策略代替失败的远程验证。
    pub authorization: Option<SubscriptionAuthorization>,
    /// 首次 Ready 前的初始订阅来源，不继承其它连接的成员权限。
    pub initial: InitialSubscriptions,
    /// 向 SDK 声明重连后是否自动重新申请期望群，每次恢复仍需新的授权。
    pub restore: Toggle,
    /// 本会话允许保留的订阅意图总数，待授权预约也占用容量。
    pub max_per_session: usize,
    /// 一次 sub/unsub 的群数量上限，不能超过会话总容量。
    pub max_per_request: usize,
    /// 单个成员读取授权的最长期限，单位毫秒，最终还受身份截止点限制。
    pub max_grant_ttl_ms: u64,
}
impl Default for Subscriptions {
    /// 业务作用：建立默认禁止新群订阅的有限配置，显式开放时仍需选择读取授权来源。
    /// 参数说明: 无。
    ///
    /// 返回：每连接 128 群、每请求 32 群、授权最多一分钟，连接恢复只表示重新申请。
    fn default() -> Self {
        Self {
            enabled: false,
            group_id_format: GroupIdFormat::Utf8,
            authorization: None,
            initial: InitialSubscriptions::None {},
            restore: Toggle {
                enabled: true,
            },
            max_per_session: 128,
            max_per_request: 32,
            max_grant_ttl_ms: 60_000,
        }
    }
}

/// 业务发送路径二选一，均须经过业务 REST 权限校验。
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MessageExecution {
    /// 业务 REST 接口校验并执行业务命令，后续下行由受信 MQ 独立进入。
    BusinessCommand,
    /// 业务 REST 接口签发固定发布许可，节点复验后提交指定发布器。
    AuthorizeThenPublish,
}

/// 业务接口可取得原始正文、schema 转换结果或明确批准的摘要；不根据请求临时切换布局。
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PayloadAccess {
    /// 保留路由原始编码正文，通过独立正文布局交给业务接口。
    #[default]
    RawBody,
    /// 按冻结 schema 转换成业务接口可读的 JSON 参数。
    Json,
    /// 仅提供批准的控制元数据与摘要，不向接口暴露正文。
    MetadataOnly,
}

/// 可授权目标类别；广播只对受信服务开放。
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TargetKind {
    /// 单个订阅目标，接收者必须具备有效订阅资格。
    Group,
    /// 同租户路由内的有限用户列表。
    Uids,
    /// 同租户路由广播，只允许显式批准的受信来源。
    TenantRouteBroadcast,
}

/// 同一会话内请求结果的有限保留策略，不能替代业务 operationId 的持久幂等。
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase", deny_unknown_fields)]
pub struct RequestCache {
    /// 单会话请求关联记录数量上限，重复请求与首次执行共用同一记录。
    pub max_records: usize,
    /// 单会话已完成结果正文的逻辑字节预算，同时受节点总预算约束。
    pub max_bytes: usize,
    /// 请求结果关联保留毫秒数，不构成跨进程持久业务幂等保证。
    pub retention_ms: u64,
}

impl Default for RequestCache {
    /// 业务作用：为重复请求保留短期任务和结果，同时约束每会话占用。
    /// 参数说明: 无。
    ///
    /// 返回：最多 256 条记录、一 MiB 逻辑字节，完成结果保留三十秒。
    fn default() -> Self {
        Self {
            max_records: 256,
            max_bytes: 1024 * 1024,
            retention_ms: 30_000,
        }
    }
}

impl RequestCache {
    /// 业务作用：在路由编译前验证结果保留窗口，禁止无限请求历史或缓存。
    /// 参数说明: 无。
    ///
    /// 返回：数量、字节和保留期限均在产品上限内时通过。
    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            (1..=4096).contains(&self.max_records)
                && (4096..=8 * 1024 * 1024).contains(&self.max_bytes)
                && (100..=120_000).contains(&self.retention_ms),
            "invalid session request cache capacity"
        );
        Ok(())
    }
}

/// 客户端发送只是业务请求入口，不授予任何本地 Dispatcher 访问能力。
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase", deny_unknown_fields)]
pub struct ClientMessages {
    /// 是否接受客户端 msg，默认关闭，不能直接在本节点广播。
    pub enabled: bool,
    /// 开启后选择业务命令或授权后发布，两条路径都以业务 REST 为权限入口。
    pub execution: Option<MessageExecution>,
    /// 处理命令或签发发布许可的固定业务端点。
    pub endpoint_ref: Option<String>,
    /// 授权后发布使用的固定受信发布器引用，业务命令路径不隐式发布。
    pub publisher_ref: Option<String>,
    /// 允许客户端申请发送的有限事件集合，成员读取资格不能扩张此集合。
    pub allowed_events: BTreeSet<String>,
    /// 允许申请的目标类型集合，业务服务仍需校验并收窄实际对象。
    pub allowed_targets: BTreeSet<TargetKind>,
    /// 客户端发送正文的最大字节数，最终还受事件和传输容量限制。
    pub max_request_bytes: usize,
    /// 业务接口接收原始正文、JSON 转换或仅 metadata 的固定方式。
    pub payload_access: PayloadAccess,
}
impl Default for ClientMessages {
    /// 业务作用：默认关闭客户端发送并限制潜在请求大小，不隐式装配业务发布通道。
    /// 参数说明: 无。
    ///
    /// 返回：无发送策略、无允许目标、正文上限 64 KiB 的配置。
    fn default() -> Self {
        Self {
            enabled: false,
            execution: None,
            endpoint_ref: None,
            publisher_ref: None,
            allowed_events: BTreeSet::new(),
            allowed_targets: BTreeSet::new(),
            max_request_bytes: 65536,
            payload_access: PayloadAccess::RawBody,
        }
    }
}

/// 通知权限必须单独声明；远程调用的业务接口可以在同一次调用中完成校验。
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
pub enum OperationAuthorization {
    /// 有效身份即可申请该白名单操作，其余路由及执行门禁仍生效。
    Authenticated {},
    /// 要求连接授权快照明确包含该操作且尚未到期。
    ConnectionGrant {},
    /// 每次操作从指定业务接口取得授权。
    RemoteHttp {
        #[serde(rename = "endpointRef")]
        /// 该通知操作的远程业务授权端点，不由客户端选择。
        endpoint_ref: String,
    },
}

/// 固定镜像允许的封闭操作集合，没有任意 handler、脚本或广播动作。
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Action {
    /// 只查询当前会话自身状态。
    SelfStatus,
    /// 按路由策略刷新本会话授权，不能替换主体。
    RefreshAuthorization,
    /// 执行固定业务 HTTP 调用，仅向原请求者回复结果。
    RemoteCall,
    /// 向固定业务信号入口发布，不取得终端下行权限。
    BusinessSignal,
}

/// 业务通知操作的完整声明，输入和输出均绑定当前路由 codec。
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Operation {
    /// 客户端可请求的白名单操作名，不是任意函数或脚本入口。
    pub name: String,
    #[serde(default)]
    /// 是否开放本操作，关闭不影响其它已启用的通知操作。
    pub enabled: bool,
    /// 固定内置动作类别，不能取得通用发送器或路由管理能力。
    pub action: Action,
    /// 本操作所需的当前身份、连接授权或远程业务授权策略。
    pub authorization: OperationAuthorization,
    #[serde(default)]
    /// 需要 HTTP 执行或刷新时的固定目标端点，授权与执行职责按动作区分。
    pub endpoint_ref: Option<String>,
    #[serde(default)]
    /// 业务信号动作使用的独立发布入口，不能与客户端下行用途混用。
    pub publisher_ref: Option<String>,
    #[serde(default)]
    /// 可选输入正文 schema，缺省时该操作不能接收任意业务体。
    pub request_schema: Option<String>,
    #[serde(default)]
    /// 可选操作结果正文 schema，未声明则不透传远端任意对象。
    pub response_schema: Option<String>,
}

/// 开放 notify 仍要求每个操作显式启用与授权。
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Notifications {
    /// 是否开放 notify 白名单能力，关闭不影响合法 sub、心跳和服务端下行。
    pub enabled: bool,
    /// 有限通知操作目录，名称唯一并且逐项验证动作所需引用。
    pub operations: Vec<Operation>,
}

/// 业务事件的唯一 schema 与投递特征，不能由逐条消息选择另一种编码。
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct EventDefinition {
    /// 本事件固定的业务 schema 引用，必须在路由目录中可编译。
    pub schema: String,
    /// 本事件正文最大字节数，同时服从传输和来源更小的上限。
    pub max_bytes: usize,
    #[serde(default)]
    /// 是否明确允许把尚未写出的同目标同事件状态替换为最新值，普通逐条消息默认禁止。
    pub coalescible: bool,
}

/// 来源隔离和目标权限都满足后才允许服务推送。
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase", deny_unknown_fields)]
pub struct ServicePush {
    /// 是否接纳受信业务下行，开启不允许客户端直接向其它连接发送。
    pub enabled: bool,
    /// 本路由唯一受信业务消费入口引用，生产者不能自行选另一条来源。
    pub ingress_ref: Option<String>,
    /// 允许向本路由推送的生产者集合，入口认证通过后仍需核对。
    pub allowed_producers: BTreeSet<String>,
    /// 允许的下行目标类型，不得由消息转换为其它更宽的范围。
    pub allowed_targets: BTreeSet<TargetKind>,
    /// 是否按冻结 schema 物化检查正文；关闭仍检查长度、schema 关联及所有投递权限。
    pub validate_body: bool,
}
impl Default for ServicePush {
    /// 业务作用：默认允许建立受信服务下行，但上线前必须明确绑定来源及生产者范围。
    /// 参数说明: 无。
    ///
    /// 返回：只允许用户和群目标，不开启广播或正文完整解析。
    fn default() -> Self {
        Self {
            enabled: true,
            ingress_ref: None,
            allowed_producers: BTreeSet::new(),
            allowed_targets: [TargetKind::Group, TargetKind::Uids].into_iter().collect(),
            validate_body: false,
        }
    }
}

/// 用户定向和群接收能力分别控制，互不隐式开启发送权限。
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Receiving {
    /// 是否允许用户定向接收，仍需当前有效的 direct 接收证据。
    pub direct: Toggle,
    /// 是否允许群目标接收，仍需有效订阅意图、成员资格和群版本。
    pub group: Toggle,
}
impl Default for Receiving {
    /// 业务作用：允许经过其余权限门禁的服务下行，群接收仍要求有效订阅。
    /// 参数说明: 无。
    ///
    /// 返回：两类接收开关开启，其余授权检查保持独立。
    fn default() -> Self {
        Self {
            direct: Toggle {
                enabled: true,
            },
            group: Toggle {
                enabled: true,
            },
        }
    }
}

/// Origin 是浏览器来源限制，不能替代用户身份。
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase", deny_unknown_fields)]
pub struct OriginCheck {
    /// 是否核对 WS Origin，身份校验始终独立存在。
    pub enabled: bool,
    /// 允许的精确来源集合，不支持通配符扩张。
    pub allowed_origins: BTreeSet<String>,
    /// 是否允许没有 Origin 的接入，不能据此跳过主体和路由授权。
    pub allow_missing: bool,
}
impl Default for OriginCheck {
    /// 业务作用：默认拒绝未经登记的浏览器来源，非浏览器无 Origin 接入也须显式允许。
    /// 参数说明: 无。
    ///
    /// 返回：开启过滤且无允许来源的安全基线。
    fn default() -> Self {
        Self {
            enabled: true,
            allowed_origins: BTreeSet::new(),
            allow_missing: false,
        }
    }
}

/// 额外连接配额的计数范围，集群范围需要权威许可而不是 presence 估算。
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QuotaScope {
    /// 在当前节点计算路由与同用户配额。
    #[default]
    Node,
    /// 由持久集群账本裁决名额，节点 presence 不能替代权威。
    Cluster,
}
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
/// 同租户同路由下新旧会话的接纳关系，替换也受真实写出排空约束。
pub enum SameUserPolicy {
    /// 在数量上限内允许同一用户持有多个会话。
    #[default]
    AllowMultiple,
    /// 已有同用户会话时拒绝新连接。
    RejectNew,
    /// 撤销旧会话，等待全部旧写出排空后再接纳新连接。
    ReplaceOld,
}

/// 路由附加配额关闭时仍有部署级总资源限制。
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase", deny_unknown_fields)]
pub struct Connections {
    /// 是否启用本路由连接数量策略，节点物理硬上限始终生效。
    pub limit_enabled: bool,
    /// 路由及同用户配额按节点还是按持久集群权威计数。
    pub scope: QuotaScope,
    /// 同租户同用户的新连接策略，替换旧会话必须等待对应真实 writer。
    pub same_user_policy: SameUserPolicy,
    /// 当前配额作用域内本路由最大连接数，不等于唯一在线用户数。
    pub max_connections: usize,
    /// 同租户同用户在本路由允许的最大会话数。
    pub max_per_uid: usize,
    /// 单会话允许的操作在途数量，同时受节点全局操作预算约束。
    pub max_pending_operations: usize,
    /// 每会话一秒窗口内接受的业务操作次数上限。
    pub operations_per_second: u32,
    /// 本会话请求身份、重复关联和结果保留的有限缓存策略。
    pub requests: RequestCache,
}
impl Default for Connections {
    /// 业务作用：为路由、用户和异步操作建立有限配额，避免固定镜像被无限请求占满。
    /// 参数说明: 无。
    ///
    /// 返回：节点范围一万连接、每用户八连接、每连接八个在途和每秒三十二操作。
    fn default() -> Self {
        Self {
            limit_enabled: true,
            scope: QuotaScope::Node,
            same_user_policy: SameUserPolicy::AllowMultiple,
            max_connections: 10_000,
            max_per_uid: 8,
            max_pending_operations: 8,
            operations_per_second: 32,
            requests: RequestCache::default(),
        }
    }
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
/// MQ 消费基线策略，实时业务输入与可恢复的安全控制输入分别处理。
pub enum DeliveryMode {
    #[default]
    /// 本次进程从来源末尾开始，只处理建立消费基线后的在线输入。
    Realtime,
    /// 仅授权控制入口允许恢复未提交的安全状态变更，不适用于业务下行。
    Recoverable,
}
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
/// 单连接发送压力达到上限时的行为，不改变原消息期限或投递权限。
pub enum SlowConsumerPolicy {
    /// 关闭无法及时接收的连接，停止继续积压。
    #[default]
    Disconnect,
    /// 保留已入队内容，拒绝新的投递。
    RejectNew,
    /// 仅对声明为可替换的状态消息保留最新完整包，不替换已交给 writer 的内容。
    KeepLatest,
}

/// 终端回执是有界观察能力，不阻塞 MQ 处理水位。
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase", deny_unknown_fields)]
pub struct ClientAck {
    /// 是否为业务下行建立终端票据，关闭不把完整写出伪装为终端确认。
    pub enabled: bool,
    /// 开启时要求的 SDK 接收或应用处理层级，不能把低层级确认升级。
    pub level: Option<AckLevel>,
    /// 从实际投递责任起约束的确认毫秒预算，消息和资格截止点仍优先。
    pub timeout_ms: u64,
    /// 单会话同时待决的回执票据数量上限。
    pub max_pending: usize,
}
impl Default for ClientAck {
    /// 业务作用：默认不建立终端回执等待，开启后仍有固定时间和数量上限。
    /// 参数说明: 无。
    ///
    /// 返回：关闭状态、十秒超时和每连接 128 条上限。
    fn default() -> Self {
        Self {
            enabled: false,
            level: None,
            timeout_ms: 10_000,
            max_pending: 128,
        }
    }
}

/// 投递策略保留消息原始期限，不以慢消费者重试刷新有效期。
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase", deny_unknown_fields)]
pub struct Delivery {
    /// 路由只允许 realtime，进程重启不恢复停机期间的业务积压。
    #[cfg_attr(
        feature = "schema",
        schemars(schema_with = "crate::config::schema::realtime_delivery")
    )]
    pub mode: DeliveryMode,
    /// 业务消息从创建到到期允许的最长毫秒跨度，重试不重算寿命。
    pub max_ttl_ms: u64,
    /// 业务创建时刻允许领先节点墙钟的毫秒误差上限。
    pub future_skew_ms: u64,
    /// 终端确认层级、期限和容量，不表示服务端保存离线历史。
    pub client_ack: ClientAck,
    /// 是否在原消息期限内抑制本节点已完成的相同消息，冲突内容不能覆盖旧键。
    pub deduplication: Toggle,
    /// 队列压力时关闭、拒绝新消息或按声明保留最新状态的明确策略。
    pub slow_consumer: SlowConsumerPolicy,
    /// 单会话尚未移交 writer 的完整包数量上限。
    pub max_queue_messages: usize,
    /// 单会话排队与实际写出共同持有的逻辑字节上限，共享正文仍按接收者计费。
    pub max_queue_bytes: usize,
}
impl Default for Delivery {
    /// 业务作用：建立有界实时投递基线，队列满时关闭慢连接且保留消费结果的明确层级。
    /// 参数说明: 无。
    ///
    /// 返回：一分钟消息期限、两秒时钟偏差、开启去重和有限出站队列。
    fn default() -> Self {
        Self {
            mode: DeliveryMode::Realtime,
            max_ttl_ms: 60_000,
            future_skew_ms: 2000,
            client_ack: ClientAck::default(),
            deduplication: Toggle {
                enabled: true,
            },
            slow_consumer: SlowConsumerPolicy::Disconnect,
            max_queue_messages: 128,
            max_queue_bytes: 1024 * 1024,
        }
    }
}

/// 连接生命周期只允许部署批准的 HTTP 或业务命令入口。
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
pub enum HookTarget {
    /// 通过固定受管 HTTP 端点交互。
    Http {
        #[serde(rename = "endpointRef")]
        /// 生命周期 HTTP 交互的固定受权端点引用。
        endpoint_ref: String,
    },
    /// 通过用途匹配的固定 MQ 发布器交互。
    Mq {
        #[serde(rename = "publisherRef")]
        /// 生命周期 MQ 请求或事实的固定发布器引用，来源用途必须相符。
        publisher_ref: String,
    },
}
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase", deny_unknown_fields)]
/// Ready 前初始化策略，依赖其授权时必须等待明确成功。
pub struct Hook {
    /// 是否执行连接初始化，关闭不发初始化请求或等待业务回复。
    pub enabled: bool,
    /// 初始化失败是否阻止 Ready；依赖初始化授权的策略必须要求成功。
    pub required: bool,
    /// 开启初始化时使用的固定 HTTP 或 MQ 目标，客户端不能指定。
    pub target: Option<HookTarget>,
    /// 初始化总等待毫秒预算，与原始接入及身份截止点共同收窄。
    pub timeout_ms: u64,
    /// MQ 初始化的独立受信回复策略，broker 接受请求不能代替授权完成。
    pub reply: Option<InitializationReplyPolicy>,
}
impl Default for Hook {
    /// 业务作用：默认不调用业务生命周期接口，开启必要初始化时不允许先接流再补授权。
    /// 参数说明: 无。
    ///
    /// 返回：关闭状态、必要语义及五秒等待上限。
    fn default() -> Self {
        Self {
            enabled: false,
            required: true,
            target: None,
            timeout_ms: 5000,
            reply: None,
        }
    }
}

/// MQ 初始化回复只能来自独立入口和明确生产者，不接受客户端填写来源。
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct InitializationReplyPolicy {
    /// 唯一允许完成该初始化等待者的独立回复入口。
    pub ingress_ref: String,
    /// 允许签发本初始化结果的业务生产者集合，必须匹配完整会话与请求。
    pub allowed_producers: BTreeSet<String>,
    /// 初始化回复中读取证据的最长有效毫秒数，还受身份期限限制。
    pub max_grant_ttl_ms: u64,
}
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
/// 分离初始化请求与上下线事实，事实通知失败不能保留失效权限。
pub struct Lifecycle {
    /// Ready 之前的业务资料和授权请求，不与 online 事实混用。
    pub initialization: Hook,
    /// 首次实际 Ready 后发送的精确连接上线事实策略。
    pub online: FactHook,
    /// 会话退出后的精确下线事实策略，未就绪即失败也能报告 wasReady=false。
    pub offline: FactHook,
}

/// 上下线事实不产生授权，不支持用 required 阻止或恢复本地状态迁移。
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase", deny_unknown_fields)]
pub struct FactHook {
    /// 是否发布该类连接事实，关闭不会阻止会话权限先行失效。
    pub enabled: bool,
    /// 固定 HTTP 或 MQ 事实目标，不接受任意业务脚本。
    pub target: Option<HookTarget>,
    /// 该条事实的交付毫秒预算，重试保持原事件身份且服从宿主关闭期限。
    pub timeout_ms: u64,
}

impl Default for FactHook {
    /// 业务作用：默认关闭远端事实通知，连接自身的权限清理始终执行。
    /// 参数说明: 无。
    ///
    /// 返回：没有远端目的地，开启时事件排队、尝试与重试共用五秒上限。
    fn default() -> Self {
        Self {
            enabled: false,
            target: None,
            timeout_ms: 5000,
        }
    }
}

/// 刷新和兼容选项都需要当前定义明确开放，不能恢复旧权限。
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Authorization {
    #[serde(default, skip_serializing_if = "super::cache::CachePolicy::is_default")]
    /// 默认关闭的只读订阅授权缓存，不作用于身份刷新或单次消息发送。
    pub cache: super::cache::CachePolicy,
    /// 自动与显式授权刷新共用的策略，新的证据仍需关联当前主体并复验撤权下界。
    pub refresh: super::refresh::RefreshPolicy,
    /// 持久业务撤权的独立来源、生产者及动作授权。
    pub control: super::authorization::ControlPolicy,
}
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase", deny_unknown_fields)]
/// 显式启用的旧接入别名，始终复用相同身份、订阅与路由门禁。
pub struct Compatibility {
    /// 是否把旧 join/leave 事件映射到相同 sub/unsub 授权和结果路径。
    pub join_leave_aliases: bool,
    /// 是否允许以标准 namespace 作为旧 HTTP 路径，将线协议根 namespace 绑定到该冻结定义。
    pub legacy_path: bool,
}

/// 业务事件目录和通知操作共同引用路由 schema 表。
pub type Events = BTreeMap<String, EventDefinition>;
