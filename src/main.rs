use nacket::bootstrap;

/// 业务作用：登记产品能力，由 napp 统一准备组件、开放服务并完成停机。
/// 参数说明：
///
/// - `app`：提供配置快照、受管资源与业务任务登记入口。
///
/// 返回：计划登记成功后交回统一生命周期；失败时不开放业务接入。
#[nasa::application(
    "log", "nacos-config", "redis", "kafka", "ws", "discovery", "web", "partition",
    config = bootstrap::configuration
)]
async fn main(app: nasa::Application) -> anyhow::Result<()> {
    bootstrap::install(&app).await
}
