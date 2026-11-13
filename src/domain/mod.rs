//! 产品领域模型，不持有网络客户端或应用容器。

pub mod assertion;
pub mod authorization;
pub mod control;
/// 定义通信信封、帧校验及有限去重规则，业务正文保持独立字节。
pub mod message;
pub mod route;
pub mod session;
