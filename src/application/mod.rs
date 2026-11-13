/// 在身份建立前限制物理连接与来源占用，避免认证请求耗尽接入资源。
pub mod admission;
pub mod control;
pub mod gateway;
pub mod listeners;
pub mod persistence;
/// 将持久定义编译为本地路由，并在依赖或许可失效时关闭接流。
pub mod routes;
/// 以可信租户和 UID 固定事件分区，使同一主体跨连接保序执行。
pub mod uid_events;
