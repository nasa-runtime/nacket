use crate::domain::message::envelope::identifier;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

/// 本地运行与集群权威互斥；控制来源和身份在进程运行期间固定。
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
pub enum Control {
    /// 仅管理当前节点的定义与会话，不提供集群持久权威。
    #[default]
    Local,
    /// 以同槽原子提交维护 Redis 控制账本与输入水位。
    Redis {
        #[serde(rename = "sourceRef")]
        /// 控制后端的固定受管来源引用，业务入口不能动态替换权威连接。
        source_ref: String,
        /// 该后端内的控制资源命名空间，用于隔离不同部署的持久账本。
        namespace: String,
        #[serde(default)]
        /// 是否允许具备资格的节点首次建立空基线；已观察权威的数据卷不得重新初始化丢失状态。
        initialize: bool,
        /// 控制身份、许可期限、续期和屏障完成策略。
        settings: Settings,
    },
    /// 以 Kafka 事务提交控制账本，恢复只采纳已提交快照。
    Kafka {
        #[serde(rename = "sourceRef")]
        /// 控制后端的固定受管来源引用，业务入口不能动态替换权威连接。
        source_ref: String,
        /// 该后端内的控制资源命名空间，用于隔离不同部署的持久账本。
        namespace: String,
        #[serde(rename = "brokerClusterId")]
        /// 部署固定的 Kafka 集群身份，恢复时核对实际 broker，避免接入其它集群。
        broker_cluster_id: String,
        #[serde(rename = "commandTopic")]
        /// Kafka 持久控制命令主题，与业务数据主题分开。
        command_topic: String,
        #[serde(rename = "stateTopic")]
        /// Kafka 控制快照及回复的事务状态主题，恢复使用已提交记录。
        state_topic: String,
        #[serde(default)]
        /// 是否允许具备资格的节点首次建立空基线；已观察权威的数据卷不得重新初始化丢失状态。
        initialize: bool,
        /// 控制身份、许可期限、续期和屏障完成策略。
        settings: Settings,
    },
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
/// 进程启动时固定的职责，不能由业务路由临时提升权限。
pub enum Role {
    /// 同时运行控制处理和客户端接入。
    #[default]
    Combined,
    /// 只处理持久控制，不接入业务客户端。
    Controller,
    /// 只承担业务接入，控制权威由集群控制器提供。
    Socket,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
/// 集群控制的身份、有限许可及完成条件，签名与有效期必须同时满足。
pub struct Settings {
    #[serde(default)]
    /// 本进程承担控制器、接入或组合职责，不通过运行期路由配置改变角色。
    pub role: Role,
    /// 本节点签署控制请求的密钥 ID，必须对应部署允许的节点主体。
    pub node_key_id: String,
    /// 允许验证控制签名的有限密钥目录及各密钥的主体权限。
    pub keys: BTreeMap<String, Key>,
    #[serde(default)]
    /// 配置清单提案使用的管理员密钥 ID，未声明时不能隐式取得提案权限。
    pub provisioner_key_id: Option<String>,
    #[serde(default = "permit_ttl")]
    /// 路由控制许可的最长有效毫秒数，续期必须取得新的权威证据。
    pub permit_ttl_ms: u64,
    #[serde(default = "renew_interval")]
    /// 节点主动续期的间隔毫秒数，不能通过心跳直接延长现有许可。
    pub renew_interval_ms: u64,
    #[serde(default = "request_timeout")]
    /// 一次控制请求及关联回复等待的毫秒预算，超时不表示提交未发生。
    pub request_timeout_ms: u64,
    #[serde(default = "clock_margin")]
    /// 跨节点期限核对预留的时钟误差毫秒数，参与合法失效证据计算。
    pub clock_margin_ms: u64,
    #[serde(default)]
    /// 仅在部署能保证时钟误差与检查至 I/O 的最长不可中断窗口时启用，缺省只接受精确排空证明。
    pub expiry_completion: bool,
    #[serde(default = "write_budget")]
    /// 同时覆盖写出终止和检查至实际 I/O 的调度、暂停上界；不能用普通网络超时替代该保证。
    pub write_budget_ms: u64,
    #[serde(default = "minimum_ready")]
    /// 新定义完成上线操作所需的最少 Ready 节点数，准备成功不等于已接流。
    pub minimum_ready_nodes: usize,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
/// 控制密钥与允许主体的绑定，密钥本身不授予目录外权限。
pub struct Key {
    /// 控制签名材料的 secret 引用，密钥 ID 本身不包含秘密。
    pub secret_ref: String,
    /// 该密钥允许代表的节点或管理员主体，签名有效不能越过此范围。
    pub principal: Principal,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "role", rename_all = "snake_case", deny_unknown_fields)]
/// 控制签名可代表的部署身份，节点责任与管理权限分离。
pub enum Principal {
    /// 仅代表指定节点，启动代际由持久登记进一步限定。
    Node {
        #[serde(rename = "nodeId")]
        /// 节点密钥唯一绑定的稳定节点标识，不能冒用其它节点请求。
        node_id: String,
    },
    /// 在固定路由集合内提出管理动作，不取得节点接流身份。
    Administrator {
        /// 管理员主体标识，用于持久操作归属与授权关联。
        subject: String,
        /// 该管理员可操作的路由范围，签名不能扩大到目录外路由。
        routes: BTreeSet<String>,
    },
}

impl Control {
    /// 业务作用：决定当前进程是否承担业务连接与推送职责，独立控制器只运行控制服务。
    /// 参数说明: 无。
    ///
    /// 返回：本地、socket 和 combined 模式为 true，controller 模式为 false。
    pub fn serves_clients(&self) -> bool {
        self.settings()
            .is_none_or(|settings| settings.role != Role::Controller)
    }

    /// 业务作用：让不同控制后端使用同一来源权限与有限许可合同。
    /// 参数说明: 无。
    ///
    /// 返回：集群模式的固定策略；本地模式不提供集群控制权威。
    pub fn settings(&self) -> Option<&Settings> {
        match self {
            Self::Local => None,
            Self::Redis {
                settings,
                ..
            }
            | Self::Kafka {
                settings,
                ..
            } => Some(settings),
        }
    }
    /// 业务作用：确定路由是否必须依赖持久集群许可，本地模式不能冒充跨节点权威。
    /// 参数说明: 无。
    ///
    /// 返回：配置选择了集群后端时为 true。
    pub fn clustered(&self) -> bool {
        !matches!(self, Self::Local)
    }

    /// 业务作用：检查控制身份、有限许可和停止预算的组合，避免不可能及时续期的配置接流。
    /// 参数说明：
    /// - `node_id`：部署固定节点标识。
    ///
    /// 返回：所有身份引用和期限均有效；未声明时钟模型时不启用到期完成。
    pub fn validate(&self, node_id: &str) -> anyhow::Result<()> {
        let (source_ref, namespace, settings) = match self {
            Self::Local => return Ok(()),
            Self::Redis {
                source_ref,
                namespace,
                settings,
                ..
            } => (source_ref, namespace, settings),
            Self::Kafka {
                source_ref,
                namespace,
                settings,
                broker_cluster_id,
                command_topic,
                state_topic,
                ..
            } => {
                anyhow::ensure!(
                    !broker_cluster_id.is_empty() && broker_cluster_id.len() <= 256,
                    "control_broker_identity_invalid"
                );
                for topic in [command_topic, state_topic] {
                    anyhow::ensure!(
                        !topic.is_empty()
                            && topic.len() <= 249
                            && !matches!(topic.as_str(), "." | "..")
                            && topic.bytes().all(|byte| byte.is_ascii_alphanumeric()
                                || matches!(byte, b'.' | b'_' | b'-')),
                        "control_topic_invalid"
                    );
                }
                anyhow::ensure!(command_topic != state_topic, "control_topic_conflict");
                (source_ref, namespace, settings)
            }
        };
        identifier(source_ref)?;
        identifier(namespace)?;
        anyhow::ensure!(
            (1..=128).contains(&settings.keys.len()),
            "control_key_capacity"
        );
        for (id, key) in &settings.keys {
            identifier(id)?;
            anyhow::ensure!(
                !key.secret_ref.is_empty()
                    && key.secret_ref.len() <= 128
                    && key
                        .secret_ref
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_'),
                "control_secret_ref_invalid"
            );
            match &key.principal {
                Principal::Node {
                    node_id,
                } => identifier(node_id)?,
                Principal::Administrator {
                    subject,
                    routes,
                } => {
                    identifier(subject)?;
                    anyhow::ensure!(
                        (1..=1024).contains(&routes.len()),
                        "control_route_scope_invalid"
                    );
                    for route in routes {
                        identifier(route)?;
                    }
                }
            }
        }
        anyhow::ensure!(
            matches!(settings.keys.get(&settings.node_key_id).map(|key| &key.principal), Some(Principal::Node { node_id: id }) if id == node_id),
            "control_node_key_mismatch"
        );
        if let Some(id) = &settings.provisioner_key_id {
            anyhow::ensure!(
                matches!(
                    settings.keys.get(id).map(|key| &key.principal),
                    Some(Principal::Administrator { .. })
                ),
                "control_provisioner_key_mismatch"
            );
        }
        anyhow::ensure!(
            (3000..=60000).contains(&settings.permit_ttl_ms)
                && (100..=10000).contains(&settings.renew_interval_ms)
                && (100..=10000).contains(&settings.request_timeout_ms)
                && (1..=5000).contains(&settings.clock_margin_ms)
                && (100..=30000).contains(&settings.write_budget_ms)
                && settings.renew_interval_ms
                    + settings.request_timeout_ms
                    + 2 * settings.clock_margin_ms
                    < settings.permit_ttl_ms
                && (1..=256).contains(&settings.minimum_ready_nodes),
            "control_deadline_policy_invalid"
        );
        Ok(())
    }
}

/// 业务作用：提供节点控制许可的有限寿命。
/// 参数说明: 无。
///
/// 返回：15000 毫秒。
fn permit_ttl() -> u64 {
    15000
}
/// 业务作用：给常规续期留下网络重试和保护余量。
/// 参数说明: 无。
///
/// 返回：5000 毫秒。
fn renew_interval() -> u64 {
    5000
}
/// 业务作用：限制控制请求在网络和队列中的总等待。
/// 参数说明: 无。
///
/// 返回：1500 毫秒。
fn request_timeout() -> u64 {
    1500
}
/// 业务作用：从可用许可寿命中扣除部署时钟误差余量。
/// 参数说明: 无。
///
/// 返回：250 毫秒。
fn clock_margin() -> u64 {
    250
}
/// 业务作用：给旧写出停止和调度恢复保留有界完成窗口。
/// 参数说明: 无。
///
/// 返回：5000 毫秒。
fn write_budget() -> u64 {
    5000
}
/// 业务作用：防止无服务节点时报告路由上线已生效。
/// 参数说明: 无。
///
/// 返回：至少一个完成应用的节点。
fn minimum_ready() -> usize {
    1
}
