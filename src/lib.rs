//! 配置驱动的长连接动态路由、授权与集群消息分发服务。
//!
//! 每条路由由业务选择唯一的 ProtocolBytes 模式，原生 TCP/WebSocket 与可选 Socket.IO
//! 共用身份、订阅和最终写出门禁。受信下行通过业务 REST 授权后进入 Redis Stream 或 Kafka，
//! 默认禁止客户端互发；纯转发保留共享正文，不反序列化业务对象。
//!
//! napp 托管生命周期，naml 管理严格配置与热更，nalog 提供日志，nafana 使用独立观测端口。
//! 路由切换先撤销旧权限并排空写出，再开放新定义；业务历史与端到端恰好一次不在服务合同内。

/// 将网络与消息系统的输入输出接入统一授权、提交及写出边界。
pub mod adapters;
/// 协调会话、路由和持久控制状态，约束异步任务的接纳与回收顺序。
pub mod application;
/// 在 napp 生命周期内装配资源、注册组件并管理服务接流与关闭。
pub mod bootstrap;
/// 定义部署选项及引用校验，配置通过验证后才可成为运行策略。
pub mod config;
#[cfg(feature = "schema")]
pub mod contracts;
pub mod domain;
pub mod observability;
