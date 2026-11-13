/// 定义认证前的节点及来源容量，限制未获权连接占用。
pub mod admission;
/// 定义客户端可见地址，监听地址不自动成为可连接的公告地址。
pub mod advertisement;
/// 配置独立 broker 观察及采样预算，不影响业务消费权限。
pub mod broker_metrics;
/// 分别控制连接成功和断开日志，不关闭必要的失败诊断。
pub mod connection_logging;
/// 限定 MQ 读入、分发、隔离及完成证据的节点容量。
pub mod ingress_processing;
pub mod kafka_topic;
pub mod lifecycle;
pub mod listeners;
pub mod messaging;
mod model;
/// 限制共享远程调用的并发与响应字节，避免单个端点耗尽节点。
pub mod remote_calls;
mod transport;
/// 配置主体事件的固定分区与有限队列，决定同 UID 操作的接纳边界。
pub mod uid_events;
mod validate;
pub use transport::Transport;

pub use model::{
    Deployment, Parameter, RemoteCallPolicy, RemoteEndpoint, RemoteMethod, RemoteTarget,
};
/// 选择持久控制后端及有限租约，区分节点、管理和业务发布权限。
pub mod control;
pub mod identity;
/// 配置独立管理入口及其认证，不复用客户端路由权限。
pub mod management;
/// 约束配置来源对动态路由的所有权与撤销行为。
pub mod provisioning;
pub mod publishing;
/// 定义节点就绪所需的路由条件，不授予单个会话接入权限。
pub mod readiness;
/// 配置逻辑会话采样和容量，使监控不保留无限主体目录。
pub mod session_metrics;

/// 限定详细指标标签的来源和数量，禁止外部输入扩展序列。
pub mod metric_scopes;
#[cfg(feature = "schema")]
pub mod schema;
