//! 集群控制状态只由持久提交推进，控制消息与业务下行使用不同权限域。

/// 保存节点接流意图与确认事实，持久许可仍需独立复验。
pub mod access;
mod authorization;
/// 将有限控制租约映射为本地单调截止点，网络等待不能延长有效期。
pub mod clock;
mod definition;
/// 处理控制状态的到期与保留边界，过期权威不能继续授权。
pub mod expiry;
mod manifest;
mod model;
mod ownership;
/// 裁决集群连接名额及接管流程，旧连接排空前不释放替换屏障。
pub mod quota;
mod retirement;
mod transition;
mod validate;
pub use model::*;
