use crate::application::{gateway::Gateway, listeners::Listeners};
use nasa::application::{
    ApplicationFuture, DependencyState, Initialization, InitializationContext, InitializerKind,
    InitializerSpec, ReadinessPolicy,
};
use std::{sync::Arc, time::Duration};

mod observation;

/// 业务作用：复用 hosted initializer 登记本地资格，不把产品路由状态写进框架。
/// 参数说明：
/// - `application`：统一生命周期宿主。
/// - `gateway`：当前节点的控制与 MQ 证据。
/// - `listeners`：真实派生监听资源目录。
///
/// 返回：长期就绪任务已纳入 Ready 与停止屏障，初始状态保持不可用。
pub(super) fn install(
    application: &nasa::Application,
    gateway: Arc<Gateway>,
    listeners: Arc<Listeners>,
) -> anyhow::Result<()> {
    application.register_initializer(
        InitializerSpec::new("nacket-readiness").kind(InitializerKind::Hosted),
        Readiness {
            application: application.clone(),
            gateway,
            listeners,
        },
    )?;
    Ok(())
}

struct Readiness {
    /// 统一组件宿主，用于核对应用生命周期是否允许接流。
    application: nasa::Application,
    /// 当前节点的路由、依赖与会话运行域，提供业务可用证据。
    gateway: Arc<Gateway>,
    /// 实际业务监听目录，准备完成仍需满足激活条件。
    listeners: Arc<Listeners>,
}

impl Initialization for Readiness {
    /// 业务作用：在注册表封口前冻结产品就绪贡献项，运行任务只更新这些固定名称。
    /// 参数说明：
    /// - `context`：hosted initializer 的任务与就绪登记边界。
    ///
    /// 返回：Ready 后持续发布本地资格；任务停止或证据过期都使关键贡献项失效。
    fn initialize<'a>(
        &'a mut self,
        context: &'a mut InitializationContext<'_>,
    ) -> ApplicationFuture<'a> {
        Box::pin(async move {
            let policy = ReadinessPolicy {
                affects_ready: true,
                failure_threshold: 1,
                recovery_threshold: 1,
                stale_after: Some(Duration::from_secs(1)),
            };
            let local = context.register_readiness("runtime", policy)?;
            let application = self.application.clone();
            let gateway = self.gateway.clone();
            let listeners = self.listeners.clone();
            context.stage_critical("local-eligibility", move |shutdown| async move {
                let mut timer = tokio::time::interval(Duration::from_millis(100));
                timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                let mut next_reap = tokio::time::Instant::now();
                let mut observation = observation::Observation::default();
                loop {
                    tokio::select! {
                        biased;
                        _ = shutdown.cancelled() => {
                            // 摘流先于资源排空，迟到指标或注册响应不能重新获得本地资格。
                            local.observe(
                                DependencyState::NotReady,
                                "stopping",
                                std::time::Instant::now(),
                            );
                            gateway.metrics.set("local_eligible", 0);
                            gateway.metrics.set("ready", 0);
                            observation.observe(application.state(), &application.readiness_snapshot());
                            return Ok(());
                        }
                        _ = timer.tick() => {
                            if tokio::time::Instant::now() >= next_reap {
                                gateway.local_quotas.reap();
                                next_reap = tokio::time::Instant::now() + Duration::from_secs(1);
                            }
                            let reason = status(&application, &gateway, &listeners);
                            let eligible = reason == "ready";
                            local.observe(
                                if eligible {
                                    DependencyState::Ready
                                } else {
                                    DependencyState::NotReady
                                },
                                reason,
                                std::time::Instant::now(),
                            );
                            gateway.metrics.set("local_eligible", u64::from(eligible));
                            let ready = observation.observe(application.state(), &application.readiness_snapshot());
                            gateway.metrics.set("ready", u64::from(ready));
                            let (count, _) = listeners.observation();
                            gateway.metrics.set(
                                "listeners_bound",
                                count + u64::from(application.ws_addr().is_some()),
                            );
                        }
                    }
                }
            })
        })
    }
}

/// 业务作用：只依赖本地证据计算接入资格，避免自注册与整体 readiness 互相等待。
/// 参数说明：
/// - `application`：提供受管主监听和应用生命周期事实。
/// - `gateway`：控制基线、必要 MQ 与路由策略。
/// - `listeners`：派生监听的本地责任。
///
/// 返回：固定原因码；空路由目录按 runtime 策略允许，必需清单按当前门禁逐项检查。
pub(super) fn status(
    application: &nasa::Application,
    gateway: &Gateway,
    listeners: &Listeners,
) -> &'static str {
    if application.state() != nasa::application::ApplicationState::Ready {
        return "application_not_running";
    }
    if gateway.configuration.control.clustered() && gateway.control_health.status() != "ready" {
        return "control_unavailable";
    }
    let routes = gateway.routes.readiness_status();
    if routes != "ready" {
        return routes;
    }
    if gateway.configuration.control.serves_clients() {
        if application.ws_addr().is_none() || !listeners.observation().1 {
            return "listener_unavailable";
        }
        if gateway
            .routes
            .dependency_status()
            .values()
            .any(|status| *status != "ready")
        {
            return "ingress_unavailable";
        }
    }
    "ready"
}
