use crate::application::{gateway::Gateway, listeners::Listeners};
use nasa::discovery::runtime::{
    assembly::{DiscoveryConfig, ListenerEndpoint},
    Eligibility,
};
use std::sync::Arc;

/// 业务作用：把共享 Socket 入口及独立本地资格接入受管注册中心，不创建第二套发现客户端。
/// 参数说明：
/// - `application`：处于计划登记阶段的 napp 应用。
/// - `gateway`：本地控制基线、依赖和路由策略。
/// - `listeners`：冻结的监听模板与真实绑定目录。
/// - `tree`：同一次严格加载的完整配置树。
///
/// 返回：启用时登记 socket 引用；缺省关闭，非法后端或角色组合在注册写入前拒绝。
pub(super) fn install(
    application: &nasa::Application,
    gateway: Arc<Gateway>,
    listeners: Arc<Listeners>,
    tree: &serde_json::Value,
) -> anyhow::Result<()> {
    let config: DiscoveryConfig = serde_json::from_value(
        tree.get("discovery")
            .cloned()
            .unwrap_or_else(|| serde_json::json!({})),
    )?;
    config.validate()?;
    if !config.enabled {
        return Ok(());
    }
    anyhow::ensure!(
        config
            .connections
            .values()
            .all(|connection| connection.provider == "nacos"),
        "discovery_provider_unsupported"
    );
    anyhow::ensure!(
        gateway.configuration.control.serves_clients() || config.registrations.is_empty(),
        "controller_socket_registration_denied"
    );
    for registration in config.registrations.values() {
        anyhow::ensure!(
            registration.listener_ref.as_deref() == Some("socket")
                && registration.eligibility_ref.as_deref() == Some("socket"),
            "socket_registration_scope_required"
        );
    }
    if config.registrations.is_empty() {
        return Ok(());
    }
    let (expected, _) = listeners.primary();
    let observer = application.clone();
    // 主端口在启动期固定；真正发布前同步核对实际绑定及本地资格，不依赖注册成功自身。
    let eligibility = Eligibility::from_check(move || {
        observer.ws_addr() == Some(expected)
            && super::readiness::status(&observer, &gateway, &listeners) == "ready"
    });
    application.configure_discovery_endpoint(
        "socket",
        ListenerEndpoint {
            ip: expected.ip().to_string(),
            port: expected.port(),
        },
        eligibility,
    )?;
    Ok(())
}
