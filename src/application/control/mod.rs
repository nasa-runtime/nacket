//! 受管控制入口只处理固定命令合同，业务消息发布权限不参与控制状态转换。

mod node;
mod request;
pub use node::NodeRuntime;
pub use request::Authority;
