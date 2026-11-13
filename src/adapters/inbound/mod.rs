/// 为物理接入选择协议与监听绑定，并使连接任务受宿主关闭边界约束。
pub mod connection;
pub mod listener;
/// 将经过控制身份验证的管理请求交给持久命令入口，禁止客户端自授管理权。
pub mod management;
pub mod messaging;
mod native;
mod socketio;
