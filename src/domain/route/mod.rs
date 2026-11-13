//! 路由配置、权限上限与写出门禁。

/// 定义远程授权响应及关联校验，明确允许也须满足主体和有效期约束。
pub mod authorization;
pub mod cache;
pub mod compiled;
/// 用有限健康证据保护路由，依赖恢复必须取得新的可用证据。
pub mod dependency;
mod gate;
pub mod handshake;
mod model;
pub mod policies;
pub mod refresh;

pub use gate::{RouteGate, WritePermit};
pub use model::{Access, DedicatedPort, Identity, RouteDefinition, Serialization, Toggle};
