use crate::domain::route::RouteDefinition;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// 产品部署正文；来源、连接池和监控出口由 napp 的同一配置树管理。
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Deployment {
    /// 已接受配置中的会话日志开关，热更影响既有连接的后续事件。
    #[serde(default)]
    pub connection_logging: super::connection_logging::ConnectionLogging,
    /// 本地或集群控制权威配置，决定路由定义及许可的持久来源。
    #[serde(default)]
    pub control: super::control::Control,
    /// 独立管理入口的开关、身份和网络范围，不向业务客户端授予管理权。
    #[serde(default)]
    pub management: super::management::Management,
    /// 配置清单提案策略，关闭不会删除权威账本中已有的路由。
    #[serde(default)]
    pub route_provisioning: super::provisioning::Provisioning,
    /// 节点就绪判定策略，可额外要求指定路由全部可接流。
    #[serde(default)]
    pub readiness: super::readiness::Readiness,
    /// 启动固定的详细监控名称目录，变更后必须重启才能重新预留系列容量。
    #[serde(default)]
    pub metric_scopes: super::metric_scopes::MetricScopes,
    /// 固定 MQ 来源的独立只读积压采样，关闭时不创建观测请求。
    #[serde(default)]
    pub broker_metrics: super::broker_metrics::BrokerMetrics,
    /// 会话业务活跃状态与订阅目标的分批观察，不参与权限决定。
    #[serde(default)]
    pub session_metrics: super::session_metrics::SessionMetrics,
    /// 部署选择的稳定节点标识；同一控制作用域内不可由多个活跃实例共享。
    pub node_id: String,
    /// 本地持久基线文件路径，其数据卷和锁随实际提交任务保留。
    pub route_ledger: String,
    /// 集群隔离身份，参与路由、授权与消息上下文绑定。
    #[serde(default = "default_cluster")]
    pub cluster_id: String,
    /// 控制作用域的十进制代际，用于隔离不同持久基线下的旧消息与许可。
    #[serde(default = "default_epoch")]
    pub scope_epoch: String,
    /// 接入适配器、握手心跳和节点传输容量策略，全部业务端口共用硬上限。
    #[serde(default)]
    pub transport: super::Transport,
    /// 全节点远程请求数量和构造字节预算，不被单端点并发配置绕过。
    #[serde(default)]
    pub remote_calls: super::remote_calls::RemoteCalls,
    /// 当前配置提供的路由候选；集群模式仍需权威提交与旧许可屏障。
    #[serde(default)]
    pub routes: Vec<RouteDefinition>,
    /// 允许业务流程引用的固定远程端点，客户端不能在此目录外指定目标。
    #[serde(default)]
    pub endpoints: BTreeMap<String, RemoteEndpoint>,
    /// 本地 JWT 和受信网关断言使用的部署验证器目录。
    #[serde(default)]
    pub verifiers: BTreeMap<String, super::identity::Verifier>,
    /// 受信 MQ 消费入口及用途、来源身份和恢复语义，不能由消息自行创建。
    #[serde(default)]
    pub ingresses: BTreeMap<String, super::messaging::Ingress>,
    /// MQ 读前保留、异步处理和扇出的节点共享容量。
    #[serde(default)]
    pub ingress_processing: super::ingress_processing::IngressProcessing,
    /// 固定 MQ 发布入口，区分业务、初始化、连接事实与回执等用途。
    #[serde(default)]
    pub publishers: BTreeMap<String, super::publishing::Publisher>,
    /// 连接事实与待决初始化的独立并发、数量、字节和退避预算。
    #[serde(default)]
    pub lifecycle_delivery: super::lifecycle::LifecycleDelivery,
    /// 主业务监听的模板引用，与 napp 已绑定地址核对后使用。
    #[serde(default = "super::listeners::primary")]
    pub primary_listener: String,
    /// 受权监听模板及独立端口范围；自动端口通过实际 bind 取得。
    #[serde(default)]
    pub listeners: BTreeMap<String, super::listeners::ListenerTemplate>,
}

/// 业务作用：为单部署空路由启动提供稳定作用域，跨环境部署应明确选择不同 clusterId。
/// 参数说明: 无。
///
/// 返回：默认集群标识，不从客户端读取。
fn default_cluster() -> String {
    "nacket".into()
}
/// 业务作用：为首次建立的控制账本选择初始代际，恢复后不得由旧客户端覆盖。
/// 参数说明: 无。
///
/// 返回：规范十进制初始代际。
fn default_epoch() -> String {
    "1".into()
}

/// 固定业务远程入口，不接受客户端提供的地址或服务名。
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RemoteEndpoint {
    /// 部署固定的直连或服务发现目标，远端响应不能更换此目标。
    pub target: RemoteTarget,
    /// 单次调用总预算，单位毫秒；与操作原始截止点取较早值。
    pub timeout_ms: u64,
    /// 响应正文最大字节数，超过上限拒绝解析及授权。
    pub max_response_bytes: usize,
    /// 该端点可签发身份或授权证据的最长期限，单位毫秒。
    pub max_identity_ttl_ms: u64,
    /// 请求方法、并发、服务凭据与显式字段映射策略。
    #[serde(default)]
    pub call: RemoteCallPolicy,
}

/// 远程调用方法由部署固定，GET 不转交用户凭据或执行业务副作用。
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum RemoteMethod {
    /// 只读调用，不用于携带用户凭据或执行有业务副作用的操作。
    Get,
    /// 标准 JSON 或原始正文请求；副作用仍由固定端点合同约束。
    #[default]
    Post,
}

/// 固定输入映射只写 parameters，不能覆盖 context、principal 或关联标识。
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "source", rename_all = "snake_case", deny_unknown_fields)]
pub enum Parameter {
    /// 使用部署常量，不从客户端请求读取值。
    Constant {
        /// 部署固定写入 parameters 的值，不能覆盖可信上下文。
        value: serde_json::Value,
    },
    /// 从允许的请求路径读取有界值，必需值缺失时不发起调用。
    Field {
        /// 从固定请求模型读取值的 JSON Pointer，配置校验限制可读取范围。
        pointer: String,
        /// 字段缺失时是否拒绝调用，拒绝发生在请求移交 HTTP 之前。
        required: bool,
        /// 映射值的最大序列化字节数，同时参与请求构造预算。
        #[serde(rename = "maxBytes")]
        max_bytes: usize,
    },
}

/// 远端接口的并发、服务认证及兼容映射，不允许客户端决定目标或 header 名。
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase", deny_unknown_fields)]
pub struct RemoteCallPolicy {
    /// 部署固定的 HTTP 方法，客户端上行不能覆盖。
    pub method: RemoteMethod,
    /// 当前端点允许的并发调用数；等待仍受节点容量和原始截止点约束。
    pub max_in_flight: usize,
    /// 可选服务凭据的 secret 引用，仅交给受权固定端点，不转发客户端任意 header。
    pub service_token_secret_ref: Option<String>,
    /// 目标参数名到固定来源映射，只补充请求 parameters。
    pub parameters: BTreeMap<String, Parameter>,
    /// 标准响应字段到远端 JSON Pointer 的显式映射，映射后仍按标准模型校验。
    pub response_fields: BTreeMap<String, String>,
    /// 远端允许值的固定文本，映射为标准 allow 后仍需关联和期限校验。
    pub allow_value: String,
    /// 远端拒绝值的固定文本；其它未知值不能隐式视为允许。
    pub deny_value: String,
}
impl Default for RemoteCallPolicy {
    /// 业务作用：默认使用一次 POST 请求和标准响应合同，限制并发且不复制任意凭据。
    /// 参数说明: 无。
    ///
    /// 返回：最多 128 个在途调用、无服务 token 和无额外参数映射。
    fn default() -> Self {
        Self {
            method: RemoteMethod::Post,
            max_in_flight: 128,
            service_token_secret_ref: None,
            parameters: BTreeMap::new(),
            response_fields: BTreeMap::new(),
            allow_value: "allow".into(),
            deny_value: "deny".into(),
        }
    }
}

/// URL 与服务发现互斥，二者都借用 napp 管理的 HTTP 生命周期。
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
pub enum RemoteTarget {
    /// 通过受管 HTTP 客户端访问固定绝对地址。
    Direct {
        /// napp 中已经声明的受管 HTTP 客户端引用，共用其连接池和关闭责任。
        #[serde(rename = "clientRef")]
        client_ref: String,
        /// 允许直连的固定绝对地址，不从客户端输入或远端响应重定向获得。
        url: String,
    },
    /// 由受管服务发现选择实例，服务名称及请求路径仍由部署固定。
    Discovery {
        /// 受管服务发现连接引用，限定注册中心及命名空间范围。
        connection: String,
        /// 部署指定的目标服务名，由共享发现目录选择可用实例。
        service: String,
        /// 固定服务内路径，不接受客户端拼接任意路由或查询目标。
        path: String,
    },
}
