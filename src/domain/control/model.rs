use crate::{
    config::{identity::Verifier, RemoteEndpoint},
    domain::{message::UnixMillis, route::RouteDefinition},
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

/// 路由引用随定义完整提交，恢复时不得套用其它节点当前配置中的同名资源。
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Definition {
    /// 完整路由声明，控制提交前已验证且与引用资源一起冻结。
    pub route: RouteDefinition,
    /// 该定义实际引用的业务 HTTP 端点快照，不在节点应用时换成其它配置代次。
    pub endpoints: BTreeMap<String, RemoteEndpoint>,
    /// 该定义实际引用的验签策略快照，控制摘要覆盖其权限边界。
    pub verifiers: BTreeMap<String, Verifier>,
    /// 完整定义及资源引用的确定摘要，节点准备和续期必须精确匹配。
    pub digest: String,
}

/// 控制账本里的精确路由代次，不允许仅按 endpoint 关联。
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Binding {
    /// 控制动作限定的单一路由 ID，不能仅凭端口定位权限域。
    pub route_id: String,
    /// 本动作绑定的路由定义 revision，旧动作不能作用于新定义。
    pub revision: u64,
    /// 完整冻结定义摘要，阻止同 revision 的不同内容被当作一致。
    pub digest: String,
}

/// 来源由受管入口认证后建立，不能从命令 JSON 反序列化取得管理权。
pub enum Principal {
    /// 对固定路由范围发起管理动作，不能冒充节点上报。
    Administrator {
        /// 已验签管理员或配置提案者的主体身份，不采用命令自报主体。
        subject: String,
        /// 当前主体允许操作的路由集合，批次和清单也必须逐项满足。
        routes: BTreeSet<String>,
    },
    /// 代表指定节点的精确启动实例，登记与续期必须关联该实例。
    Node {
        /// 节点密钥绑定的稳定节点 ID，不能代表其它节点上报状态。
        node_id: String,
        /// 本次节点启动的稳定请求 ID，用于关联实例代际与防止旧进程续期。
        startup_request: String,
    },
    /// 按配置来源提交清单，不能越过路由归属或紧急暂停事实。
    Provisioner {
        /// 已验签管理员或配置提案者的主体身份，不采用命令自报主体。
        subject: String,
        /// 当前主体允许操作的路由集合，批次和清单也必须逐项满足。
        routes: BTreeSet<String>,
    },
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
/// 路由定义的管理来源，决定后续提案应匹配的归属身份。
pub enum OwnerKind {
    /// 由显式管理 API 持有定义所有权。
    Api,
    /// 由固定配置来源持有定义所有权。
    Config,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
/// 已提交的路由归属动作，用稳定 ID 与指纹拒绝冲突重试，不代表数据面切换完成。
pub struct OwnershipAction {
    /// 调用方稳定的归属操作 ID，重试不得换成另一条已发生的动作。
    pub id: String,
    /// 提交本动作的已验证控制主体。
    pub actor: String,
    /// 原归属请求的确定指纹，同 ID 内容变化时拒绝。
    pub fingerprint: String,
    /// 归属动作限定的单一路由，不能改变其它路由所有权。
    pub route_id: String,
    /// 动作完成后的归属代次，与业务路由 revision 分开。
    pub owner_revision: u64,
    /// 权威提交归属事实的 Unix 毫秒时刻，不代表数据面已切换定义。
    pub completed_at: UnixMillis,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
/// 完成排空或合法失效证明后的精确节点退役记录，不能作用于后继实例。
pub struct RetirementAction {
    /// 节点退役操作的稳定幂等 ID。
    pub id: String,
    /// 已验证且具备受影响路由范围的退役请求主体。
    pub actor: String,
    /// 退役请求的确定指纹，防止同 ID 修改目标节点或代际。
    pub fingerprint: String,
    /// 明确退役的稳定节点 ID。
    pub node_id: String,
    /// 实际被退役的十进制实例代际，不能顺带退役重启后的新实例。
    pub incarnation: String,
    /// 本动作实际影响的路由集合，用于权限范围与结果查询。
    pub routes: BTreeSet<String>,
    /// 完成合法退役事实的 Unix 毫秒时刻，缺乏排空或失效证据不能提前写入。
    pub completed_at: UnixMillis,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
/// 路由变更的持久阶段，旧许可屏障与新接流确认分别成立后才能完成。
pub enum Phase {
    /// 停止旧准入并等待已有许可排空或合法失效。
    Quiescing,
    /// 准备新定义及其依赖，尚未授予接流资格。
    Preparing,
    /// 已可签发新许可，等待目标节点应用并满足完成门槛。
    Activating,
    /// 本操作的全局完成条件已成立，后续接流仍须有效许可。
    Effective,
    /// 被后续操作明确取代，旧许可责任仍由屏障约束。
    Superseded,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
/// 一次路由变更的持久进度，目标集合、屏障和幂等身份在受理时固定。
pub struct Operation {
    /// 路由变更的稳定幂等操作 ID，后续查询与逐节点回执引用它。
    pub id: String,
    /// 发起该变更的已验证控制主体，不能由节点回执覆盖。
    pub actor: String,
    /// 首次受理请求指纹，相同 ID 的不同定义或期望条件拒绝。
    pub fingerprint: String,
    /// 本操作准备开放或关闭的完整路由定义绑定。
    pub binding: Binding,
    /// 停止旧准入、准备、激活、生效或被取代的权威阶段。
    pub phase: Phase,
    /// 受理时固定的目标节点集合，不能因节点暂时缺席静默缩小屏障。
    pub targets: BTreeSet<String>,
    /// 已经证明准备好本绑定的目标节点集合，尚不表示实际接流。
    pub prepared: BTreeSet<String>,
    /// 已经确认应用本绑定及对应许可的节点集合。
    pub applied: BTreeSet<String>,
    /// 节点返回的固定准备失败原因，失败节点保持保护而不回退旧定义。
    pub failures: BTreeMap<String, String>,
    /// 切换前必须排空或证明合法失效的旧许可 ID 集合。
    pub barrier: BTreeSet<String>,
    /// 上线完成所需的最少有效接流节点数，不能由局部准备数量替代。
    pub minimum_ready: usize,
    /// 权威首次接纳操作的 Unix 毫秒时刻，重试保持不变。
    pub created_at: UnixMillis,
    /// 全局完成条件成立后的 Unix 毫秒时刻，待决时为空。
    pub completed_at: Option<UnixMillis>,
    /// 明确取代此操作的后续操作 ID，取代不能跳过旧许可屏障。
    pub superseded_by: Option<String>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
/// 路由的权威期望及归属事实，存在定义不等于任一节点已获准接流。
pub struct Route {
    /// 权威当前期望的完整定义，节点仍需完成屏障后才能据此接流。
    pub definition: Definition,
    /// 仍需要核对旧许可及屏障的历史定义，不能把旧摘要套到当前模式。
    pub previous: Vec<Definition>,
    /// 当前驱动本路由收敛的操作 ID。
    pub operation_id: String,
    /// 当前路由归属的管理主体或配置来源标识。
    pub owner: String,
    /// 归属来源类别，区分显式管理与配置清单，避免两者无条件覆盖。
    pub owner_kind: OwnerKind,
    /// 单调归属代次，转移和解除暂停操作须核对预期值。
    pub owner_revision: u64,
    /// 紧急下线保留的暂停事实，后续配置提案不能隐式清除。
    pub suspended: bool,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
/// 稳定节点的当前实例及控制回执，旧启动请求不能继承新实例权威。
pub struct Node {
    /// 该稳定节点当前单调实例代际，旧进程请求不能取得新许可。
    pub incarnation: u64,
    /// 当前实例已登记的启动请求 ID，精确重试保持同一代际。
    pub startup_request: String,
    /// 该实例已经准备的逐路由完整定义绑定。
    pub prepared: BTreeMap<String, Binding>,
    /// 已处理启动请求到代际的持久关联，防止迟到注册创建额外实例。
    pub startups: BTreeMap<String, u64>,
    /// 节点是否仍参与当前控制作用域，退役不能被普通迟到请求重新激活。
    pub active: bool,
    /// 该节点是否承担业务接入职责，纯控制器不作为接流完成目标。
    pub serving: bool,
    /// 本节点已应用的逐路由持久授权投影 revision，续期必须赶上权威下界。
    pub authorizations: BTreeMap<String, u64>,
    /// 已受理连接配额动作的单调序号水位，拒绝迟到申请越过较新的归还。
    pub quota_watermark: u64,
    /// 本实例当前有限接入目录声明，随对应许可和定义共同核对。
    pub access: BTreeMap<String, super::access::PublishedAccess>,
}

/// 账本时间用于诊断，许可失效的完成证明必须另外来自精确节点回执或受验证的 elapsed 证据。
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Permit {
    /// 权威签发的唯一许可 ID，实际写出与排空证明精确关联。
    pub id: String,
    /// 许可所属稳定节点，不能跨节点转移写权。
    pub node_id: String,
    /// 许可所属节点实例代际，重启不得继承旧实例资格。
    pub incarnation: u64,
    /// 签发时冻结的启动请求关联，旧进程不能借相同 nodeId 续期。
    pub startup_request: String,
    /// 许可唯一绑定的路由定义，不能跨 revision 或摘要使用。
    pub binding: Binding,
    /// 原续期请求的随机关联值，迟到回复不能作为另一请求的新许可。
    pub request_nonce: String,
    /// 许可寿命毫秒数，本地从原请求起点保守计算，不从回复到达重新计时。
    pub ttl_ms: u64,
    /// 权威签发记录的 Unix 毫秒时刻，参与持久审查但不替代本地单调期限。
    pub granted_at: UnixMillis,
}

/// 完整状态可由 Redis 原子替换或 Kafka 事务投影保存；候选状态在提交前不具备控制权。
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Ledger {
    /// 整个控制账本所属集群身份，恢复时必须与部署一致。
    pub cluster_id: String,
    /// 当前控制基线的十进制作用域代际，隔离重新建立的权威和旧输入。
    pub scope_epoch: String,
    /// 控制安全策略的冻结摘要，节点策略不一致时不能继续获得许可。
    pub policy_digest: String,
    /// 每次实际权威变更递增的提交序号，本地高水位防止恢复回退。
    pub sequence: u64,
    /// 当前路由定义、归属和暂停事实的持久目录。
    pub routes: BTreeMap<String, Route>,
    /// 稳定节点到当前实例及历史启动关联的持久目录。
    pub nodes: BTreeMap<String, Node>,
    /// 仍需有效性或排空判断的精确路由许可目录。
    pub permits: BTreeMap<String, Permit>,
    /// 路由变更的幂等记录和固定全局屏障，不因重试重新创建目标集合。
    pub operations: BTreeMap<String, Operation>,
    /// 归属变更及恢复动作的持久幂等证据。
    pub ownership_actions: BTreeMap<String, OwnershipAction>,
    /// 按配置来源保留的单调清单历史和逐路由结果，旧代不能继续执行。
    pub manifests: BTreeMap<String, Vec<ManifestRecord>>,
    /// 节点退役的持久幂等记录，包含精确实例与受影响路由。
    pub retirements: BTreeMap<String, RetirementAction>,
    /// 全局连接配额的持久会话责任，崩溃或换实例不能自动释放旧份额。
    pub reservations: BTreeMap<String, super::quota::Reservation>,
    /// 一次性网关断言的消费证据与时间下界，控制器重启后仍拒绝重放。
    pub assertions: crate::domain::assertion::Replay,
    /// 逐路由的持久业务撤权下界与屏障操作，不能由新节点空投影覆盖。
    pub authorizations: BTreeMap<String, crate::domain::authorization::State>,
}

/// 发布清单先持久登记定义摘要与 CAS，完整定义逐项提交，避免一次命令携带全部业务 schema。
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ManifestProposal {
    /// 清单声明的目标路由定义绑定，实际应用必须提供同摘要完整定义。
    pub binding: Binding,
    /// 提交清单时要求权威仍持有的旧路由 revision，阻止并发覆盖。
    pub expected_revision: u64,
    /// 本条目期望的上线状态，暂停事实及控制屏障仍优先。
    pub enabled: bool,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
/// 单个配置清单条目的持久处理结果，与整个清单受理及数据面完成分开判断。
pub enum ManifestResult {
    /// 等待提交完整定义，执行时还须匹配受理时的归属代次。
    Pending {
        #[serde(rename = "expectedOwnerRevision")]
        /// 受理条目时冻结的归属代次，执行前再次核对避免越过转移。
        expected_owner_revision: u64,
    },
    /// 已创建路由操作，仍需查询该操作的准备与接流屏障。
    Applied {
        #[serde(rename = "operationId")]
        /// 实际创建的路由操作 ID，查询其屏障才能判断数据面完成。
        operation_id: String,
    },
    /// 当前定义已满足提案，不另建变更操作。
    Unchanged {},
    /// 条目未获准应用，保留固定拒绝原因。
    Rejected {
        /// 该条目被拒绝的固定原因码，不记录秘密或原始定义正文。
        code: String,
    },
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
/// 清单中的固定提案及其处理结果，逐条推进时不改变原并发条件。
pub struct ManifestEntry {
    /// 本条清单首次提交的固定候选绑定与并发条件。
    pub proposal: ManifestProposal,
    /// 该条目的待决、已应用、不变或拒绝结果，不以整体清单受理代替。
    pub result: ManifestResult,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
/// 一个配置来源的持久清单身份与逐项进度，同代不同内容不能覆盖。
pub struct ManifestRecord {
    /// 配置清单的归属来源 ID，业务侧不能借更高代次冒用其它来源。
    pub source_id: String,
    /// 该来源的单调十进制清单代次，精确重试保持同代及同内容。
    pub generation: String,
    /// 完整清单的确定摘要，同代不同内容不能替换原提案。
    pub digest: String,
    /// 提交本清单的已验证提案者主体。
    pub actor: String,
    /// 权威首次接受该清单的 Unix 毫秒时刻。
    pub accepted_at: UnixMillis,
    /// 按路由保存的固定条目和执行结果，允许独立推进但不省略权限检查。
    pub entries: BTreeMap<String, ManifestEntry>,
}

/// 控制账本容量及许可寿命边界，资源不足时拒绝扩展而不淘汰有效安全证据。
pub struct Limits {
    /// 控制账本允许保存的路由数上限，容量满不删除持久下线事实腾位。
    pub routes: usize,
    /// 控制账本允许保存的稳定节点数量上限。
    pub nodes: usize,
    /// 持久幂等操作目录的数量上限，不能淘汰仍用于拒绝冲突的证据。
    pub operations: usize,
    /// 尚未合法释放的控制许可数量上限。
    pub permits: usize,
    /// 权威允许签发的最大许可寿命，单位毫秒。
    pub permit_ttl_ms: u64,
}

/// 节点报告不能代替管理员修改定义；各命令在同一个状态提交边界内验证。
#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Command {
    /// 观察并推进当前控制状态，不自行修改路由定义。
    Observe {},
    /// 为当前节点的精确会话申请集群连接名额。
    ReserveConnection {
        /// 当前节点连接配额动作的单调序号，归还推进水位后拒绝迟到申请。
        ordinal: u64,
        #[serde(rename = "sessionKey")]
        /// 此次配额或断言动作精确绑定的完整会话键，不能操作其它实例的会话。
        session_key: crate::domain::message::envelope::SessionKey,
        #[serde(rename = "tenantId")]
        /// 连接配额所属的已验证租户，用户数量在该隔离域内计算。
        tenant_id: String,
        /// 连接配额所属的真实用户，不能用候选消息目标替换。
        uid: String,
    },
    /// 在实际写出排空后归还名额，并推进防止迟到申请复活的水位。
    ReleaseConnection {
        /// 当前节点连接配额动作的单调序号，归还推进水位后拒绝迟到申请。
        ordinal: u64,
        #[serde(rename = "sessionKey")]
        /// 此次配额或断言动作精确绑定的完整会话键，不能操作其它实例的会话。
        session_key: crate::domain::message::envelope::SessionKey,
    },
    /// 将受信业务撤权命令纳入持久控制提交。
    ApplyAuthorization {
        /// 原业务撤权命令到达的固定受信入口，用于权威再次检查来源范围。
        ingress: String,
        /// 该入口已验证的业务生产者，权威按当前路由策略再次授权。
        producer: String,
        /// 严格的业务撤权命令，仍需校验定义绑定、版本和稳定指纹。
        command: crate::domain::authorization::Command,
    },
    /// 节点确认已应用指定路由的撤权投影。
    AuthorizationApplied {
        #[serde(rename = "routeId")]
        /// 当前控制动作明确作用的路由 ID，必须属于签名主体允许范围。
        route_id: String,
        /// 节点已应用的持久授权投影 revision，不是客户端声明的业务授权版本。
        revision: u64,
    },
    /// 持久占用一次性断言，只有同一精确认证尝试可以等价重试。
    ConsumeAssertion {
        #[serde(rename = "sessionKey")]
        /// 此次配额或断言动作精确绑定的完整会话键，不能操作其它实例的会话。
        session_key: crate::domain::message::envelope::SessionKey,
        #[serde(rename = "attemptId")]
        /// 一次性断言绑定的精确认证尝试，不能跨握手复用。
        attempt_id: String,
        /// 一次性断言的稳定消费键摘要，原始凭据不进入控制命令。
        digest: String,
        #[serde(rename = "expiresAt")]
        /// 原断言的 Unix 毫秒失效时刻，重复消费不刷新期限。
        expires_at: UnixMillis,
    },
    /// 登记配置来源的有限提案清单，尚不提交完整路由定义。
    Manifest {
        #[serde(rename = "sourceId")]
        /// 清单提案或应用对应的配置归属来源。
        source_id: String,
        /// 当前清单的十进制代次，应用时必须仍是该来源允许执行的代次。
        generation: String,
        /// 清单中的有限路由候选集合，整体受理前核对全部范围。
        entries: Vec<ManifestProposal>,
    },
    /// 对仍有效的清单条目提交完整定义，继续核对归属与并发条件。
    ApplyManifest {
        #[serde(rename = "sourceId")]
        /// 清单提案或应用对应的配置归属来源。
        source_id: String,
        /// 当前清单的十进制代次，应用时必须仍是该来源允许执行的代次。
        generation: String,
        /// 当前动作提交的完整冻结路由定义及引用资源。
        definition: Definition,
        #[serde(rename = "minimumReady")]
        /// 本次上线所需最少有效接流节点数，不能绕过旧定义屏障。
        minimum_ready: usize,
    },
    /// 在同一主体范围内按顺序处理有限控制动作。
    Batch {
        /// 受同一节点主体约束的有限有序控制批次，逐条结果保持对应次序。
        commands: Vec<Command>,
    },
    /// 登记承担业务接入职责的节点启动实例。
    Register {},
    /// 登记只承担控制职责的实例，不纳入业务接流完成目标。
    RegisterController {},
    /// 创建路由下线操作并保留暂停事实，配置提案不能隐式恢复。
    Offline {
        #[serde(rename = "operationId")]
        /// 管理动作的稳定幂等 ID，重复 ID 内容必须一致。
        operation_id: String,
        #[serde(rename = "routeId")]
        /// 当前控制动作明确作用的路由 ID，必须属于签名主体允许范围。
        route_id: String,
        #[serde(rename = "expectedRevision")]
        /// 命令要求权威仍持有的旧定义 revision，冲突时拒绝覆盖。
        expected_revision: u64,
    },
    /// 在定义与归属代次匹配时明确解除暂停。
    Resume {
        #[serde(rename = "operationId")]
        /// 管理动作的稳定幂等 ID，重复 ID 内容必须一致。
        operation_id: String,
        #[serde(rename = "routeId")]
        /// 当前控制动作明确作用的路由 ID，必须属于签名主体允许范围。
        route_id: String,
        #[serde(rename = "expectedRevision")]
        /// 命令要求权威仍持有的旧定义 revision，冲突时拒绝覆盖。
        expected_revision: u64,
        #[serde(rename = "expectedOwnerRevision")]
        /// 命令要求仍成立的归属代次，防止越过并发归属变化。
        expected_owner_revision: u64,
    },
    /// 在并发条件成立时转移路由归属，不直接授予数据面接流资格。
    Transfer {
        #[serde(rename = "operationId")]
        /// 管理动作的稳定幂等 ID，重复 ID 内容必须一致。
        operation_id: String,
        #[serde(rename = "routeId")]
        /// 当前控制动作明确作用的路由 ID，必须属于签名主体允许范围。
        route_id: String,
        #[serde(rename = "expectedRevision")]
        /// 命令要求权威仍持有的旧定义 revision，冲突时拒绝覆盖。
        expected_revision: u64,
        #[serde(rename = "expectedOwnerRevision")]
        /// 命令要求仍成立的归属代次，防止越过并发归属变化。
        expected_owner_revision: u64,
        /// 归属转移的明确目标标识，不通过配置省略隐式改写。
        owner: String,
        #[serde(rename = "ownerKind")]
        /// 新归属的管理或配置来源类别，必须与目标标识符合合同。
        owner_kind: OwnerKind,
    },
    /// 提交完整路由期望，旧许可屏障完成后才能开放新定义。
    Define {
        #[serde(rename = "operationId")]
        /// 管理动作的稳定幂等 ID，重复 ID 内容必须一致。
        operation_id: String,
        #[serde(rename = "expectedRevision")]
        /// 命令要求权威仍持有的旧定义 revision，冲突时拒绝覆盖。
        expected_revision: u64,
        /// 当前动作提交的完整冻结路由定义及引用资源。
        definition: Definition,
        #[serde(rename = "minimumReady")]
        /// 本次上线所需最少有效接流节点数，不能绕过旧定义屏障。
        minimum_ready: usize,
    },
    /// 精确许可所属节点确认对应旧写出责任已全部结束。
    Drained {
        #[serde(rename = "permitId")]
        /// 节点操作精确关联的许可 ID，不能用任意旧许可证明当前写出已结束。
        permit_id: String,
    },
    /// 节点报告已准备指定定义及依赖，尚不等于接流完成。
    Prepared {
        /// 当前准备、续期、应用或失败回执的完整路由定义绑定。
        binding: Binding,
    },
    /// 为完整绑定申请有限新许可，原请求起点约束有效期。
    Renew {
        /// 当前准备、续期、应用或失败回执的完整路由定义绑定。
        binding: Binding,
        /// 本次续期的随机关联值，回复与原始请求起点共同验证新鲜度。
        nonce: String,
    },
    /// 在续期的同次提交中登记监听公告，公告不能脱离许可单独生效。
    RenewAccess {
        /// 当前准备、续期、应用或失败回执的完整路由定义绑定。
        binding: Binding,
        /// 本次续期的随机关联值，回复与原始请求起点共同验证新鲜度。
        nonce: String,
        /// 与续期一同提交的可选外部监听声明，不绕过真实网络准备与端口映射。
        listener: Option<super::access::ListenerAccess>,
    },
    /// 节点确认已应用精确绑定与许可，计入操作的接流完成条件。
    Applied {
        /// 当前准备、续期、应用或失败回执的完整路由定义绑定。
        binding: Binding,
        #[serde(rename = "permitId")]
        /// 节点操作精确关联的许可 ID，不能用任意旧许可证明当前写出已结束。
        permit_id: String,
    },
    /// 节点报告准备失败并保持保护，不自动回退旧定义。
    Failed {
        /// 当前准备、续期、应用或失败回执的完整路由定义绑定。
        binding: Binding,
        /// 节点报告的有限失败原因，不能借失败回执自动回退并开放旧定义。
        reason: String,
    },
    /// 明确退役指定节点代际，仍要求合法结束其旧许可责任。
    Retire {
        #[serde(rename = "operationId")]
        /// 管理动作的稳定幂等 ID，重复 ID 内容必须一致。
        operation_id: String,
        #[serde(rename = "nodeId")]
        /// 退役动作明确选择的稳定节点 ID。
        node_id: String,
        #[serde(rename = "expectedIncarnation")]
        /// 退役时要求仍成立的实例代际，重启后的新实例不能被旧请求误退役。
        expected_incarnation: u64,
    },
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
/// 与持久控制提交对应的结果，调用者不能把受理、登记或名额等待当作业务可用。
pub enum Reply {
    /// 返回精确名额的权威状态，只有激活后才可能继续接入。
    ConnectionReserved {
        /// 权威持久保存的精确连接配额责任，等待或撤销阶段不代表已经可用。
        reservation: super::quota::Reservation,
    },
    /// 返回业务撤权操作及其应用屏障进度。
    Authorization {
        /// 本动作的持久操作记录和屏障状态，受理不等于全局完成。
        operation: crate::domain::authorization::Operation,
    },
    /// 返回已经确认的精确节点退役事实。
    Retirement {
        /// 已经持久确认的管理动作记录，精确重试返回同一事实。
        action: RetirementAction,
    },
    /// 返回固定清单及逐项结果，不表示全部路由已接流。
    Manifest {
        /// 已接受清单的固定内容及逐条进度，不重新生成原始接纳时间。
        manifest: ManifestRecord,
    },
    /// 按原命令次序返回结果，调用者必须保持逐项关联。
    Batch {
        /// 与原有限命令批次逐项同序对应的结果，不能错配续期或回执。
        replies: Vec<Reply>,
    },
    /// 返回本次启动获得的节点代际，不能转交其它启动实例使用。
    Registered {
        /// 注册得到的当前节点实例十进制代际，后续请求必须携带匹配启动身份。
        incarnation: String,
    },
    /// 返回路由变更的持久阶段与屏障，不以操作存在推断完成。
    Operation {
        /// 本动作的持久操作记录和屏障状态，受理不等于全局完成。
        operation: Operation,
    },
    /// 返回当前续期的有限许可，接收延迟不能刷新期限。
    Permit {
        /// 本次精确续期请求获得的有限许可，不能从迟到接收时重新计算寿命。
        permit: Permit,
    },
    /// 当前动作已记录，不额外授予路由、身份或投递权限。
    Recorded {},
    /// 权威明确拒绝本次请求，不产生新的资格。
    Rejected {
        /// 权威拒绝的固定公开原因码，拒绝不产生新的权限。
        code: String,
    },
    /// 返回已提交的归属动作，与路由上线状态分开判断。
    Ownership {
        /// 已经持久确认的管理动作记录，精确重试返回同一事实。
        action: OwnershipAction,
    },
}

/// 未提交候选由后端独占，只有持久事务确认后才能把 reply 返回给调用方。
pub struct Transition {
    /// 执行当前命令得到的完整候选权威状态，适配器必须先持久提交再发布结果。
    pub ledger: Ledger,
    /// 与同一次候选状态对应的控制回复，不能先于权威提交被用于授权。
    pub reply: Reply,
}
