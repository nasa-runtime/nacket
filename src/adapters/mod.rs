pub mod control;
/// 校验监听绑定、外部协议和管理请求，未认证输入不能直接进入业务投递。
pub mod inbound;
/// 对外执行身份调用与受信发布，保留远程失败和交付未知的语义。
pub mod outbound;
