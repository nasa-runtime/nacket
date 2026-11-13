use crate::{application::routes::RouteService, config::Deployment};
use std::{sync::Arc, time::Duration};

/// 业务作用：登记受管监听、配置协调和身份期限维护，不另建应用运行时。
/// 参数说明：
///
/// - `application`：处于 UserHook 的 napp 应用句柄。
///
/// 返回：全部产品计划登记成功；无效配置阻止进入 Ready。
pub async fn install(application: &nasa::Application) -> anyhow::Result<()> {
    let mut updates = application.subscribe_config();
    let initial = updates.borrow_and_update().clone();
    let tree = initial.snapshot().value();
    let configuration: Deployment = initial.snapshot().section("nacket")?;
    let serves_clients = configuration.control.serves_clients();
    anyhow::ensure!(
        tree.pointer("/ws/enabled")
            .and_then(|v| v.as_bool())
            .unwrap_or(true)
            == serves_clients,
        "ws.enabled must match the process role"
    );
    let http_enabled = tree
        .pointer("/server/enabled")
        .and_then(|value| value.as_bool())
        .ok_or_else(|| anyhow::anyhow!("server.enabled must be explicit"))?;
    // 健康探针可独立于管理权限开放；管理动作仍须显式开启且由独立身份验证。
    anyhow::ensure!(
        !configuration.management.enabled || http_enabled,
        "management requires server.enabled=true"
    );
    if http_enabled {
        let http_port = tree
            .pointer("/server/port")
            .and_then(|value| value.as_u64())
            .filter(|port| (1..=65535).contains(port))
            .ok_or_else(|| anyhow::anyhow!("HTTP port must be explicit"))?;
        if let Some(address) = tree.pointer("/ws/addr").and_then(|value| value.as_str()) {
            let address: std::net::SocketAddr = address.parse()?;
            anyhow::ensure!(
                http_port != u64::from(address.port()),
                "HTTP port must differ from the business port"
            );
        }
        if let Some(address) = tree
            .pointer("/grafana/observability/prometheus/scrape/bind")
            .and_then(|value| value.as_str())
        {
            let address: std::net::SocketAddr = address.parse()?;
            anyhow::ensure!(
                http_port != u64::from(address.port()),
                "HTTP port must differ from the monitoring port"
            );
        }
    }
    // 统一业务端口必须由唯一受管 listener 分发协议，禁止重复监听或退回隐式默认地址。
    anyhow::ensure!(
        tree.pointer("/ws/multiplex").and_then(|v| v.as_bool()) == Some(true),
        "nacket requires ws.multiplex=true"
    );
    if tree
        .pointer("/grafana/observability/enabled")
        .and_then(|v| v.as_bool())
        == Some(true)
    {
        anyhow::ensure!(
            tree.pointer("/grafana/observability/prometheus/scrape/listener")
                .and_then(|v| v.as_str())
                == Some("dedicated"),
            "monitoring requires a dedicated listener"
        );
        if let (Some(metrics), Some(ws)) = (
            tree.pointer("/grafana/observability/prometheus/scrape/bind")
                .and_then(|v| v.as_str()),
            tree.pointer("/ws/addr").and_then(|v| v.as_str()),
        ) {
            let metrics: std::net::SocketAddr = metrics.parse()?;
            let ws: std::net::SocketAddr = ws.parse()?;
            anyhow::ensure!(
                metrics.port() != ws.port(),
                "monitoring port must differ from the business port"
            );
        }
    }
    crate::application::uid_events::install(application, &configuration.transport.uid_events)?;
    let routes = RouteService::open(configuration.clone())?;
    let gateway = crate::application::gateway::Gateway::new(
        application.clone(),
        routes.clone(),
        configuration,
    );
    application.register_managed(RouteOwner {
        routes: routes.clone(),
        metrics: gateway.metrics.clone(),
    })?;
    application.register_metrics_source_reserved(
        gateway.metrics.clone(),
        gateway.metrics.worst_case_series(),
    )?;

    if gateway.configuration.metric_scopes.enabled {
        application.register_metrics_source_reserved(
            gateway.metrics.scopes.clone(),
            gateway.metrics.scopes.worst_case_series(),
        )?;
    }
    if gateway.broker_metrics.worst_case_series() > 0 {
        application.register_metrics_source_reserved(
            gateway.broker_metrics.clone(),
            gateway.broker_metrics.worst_case_series(),
        )?;
    }

    if !gateway.configuration.control.clustered() {
        let incarnation = gateway.routes.local_incarnation()?;
        gateway
            .incarnation
            .set(incarnation)
            .map_err(|_| anyhow::anyhow!("node_already_registered"))?;
    }
    if serves_clients {
        gateway
            .authorization_cache_budget
            .install(application)
            .await?;
        let refreshes =
            crate::application::gateway::refresh::Refreshes::install(application, gateway.clone())
                .await?;
        gateway
            .refreshes
            .set(refreshes)
            .map_err(|_| anyhow::anyhow!("refresh_already_installed"))?;
        let publishers = crate::adapters::outbound::publishing::Publishers::install(
            application,
            &gateway.configuration.publishers,
            gateway.metrics.clone(),
            gateway.lifecycle_publish_shutdown.clone(),
        )
        .await?;
        gateway
            .publishers
            .set(publishers)
            .map_err(|_| anyhow::anyhow!("publisher_already_installed"))?;
        let lifecycle =
            crate::application::gateway::lifecycle::Lifecycle::install(application, &gateway)
                .await?;
        gateway
            .lifecycle
            .set(lifecycle)
            .map_err(|_| anyhow::anyhow!("lifecycle_already_installed"))?;
        application.register_initializer(
            nasa::application::InitializerSpec::new("nacket-identity"),
            IdentityInitializer(gateway.clone()),
        )?;
        crate::adapters::inbound::messaging::install(application, gateway.clone()).await?;
    }
    let listeners = crate::application::listeners::Listeners::new(gateway.clone(), tree)?;
    application.register_managed(crate::application::listeners::ListenerOwner(
        listeners.clone(),
    ))?;
    if serves_clients {
        listeners.prepare().await?;
    }
    let control = match crate::application::control::Authority::install(
        application,
        &gateway.configuration,
        routes.clone(),
        gateway.metrics.clone(),
    )
    .await?
    {
        Some(authority) => {
            gateway
                .control
                .set(authority.clone())
                .map_err(|_| anyhow::anyhow!("control_already_installed"))?;
            if serves_clients
                && gateway.configuration.ingresses.values().any(|ingress| {
                    ingress.enabled
                        && ingress.purpose
                            == crate::config::messaging::IngressPurpose::AuthorizationControl
                })
            {
                let inbox = crate::application::gateway::authorization_inbox::Inbox::install(
                    application,
                    authority.clone(),
                    gateway.routes.clone(),
                    gateway.metrics.clone(),
                )
                .await?;
                gateway
                    .authorization_inbox
                    .set(inbox)
                    .map_err(|_| anyhow::anyhow!("authorization_inbox_already_installed"))?;
            }
            let node = crate::application::control::NodeRuntime::install(
                application,
                authority,
                gateway.clone(),
                listeners.clone(),
            )
            .await?;
            crate::adapters::inbound::management::install(
                application,
                node.authority.clone(),
                gateway.clone(),
                listeners.clone(),
                tree,
            )?;
            application.register_initializer(
                nasa::application::InitializerSpec::new("nacket-control"),
                ControlInitializer(node.clone()),
            )?;
            Some(node)
        }
        None => None,
    };
    crate::observability::install(application, gateway.clone()).await?;
    super::readiness::install(application, gateway.clone(), listeners.clone())?;
    super::discovery::install(application, gateway.clone(), listeners.clone(), tree)?;
    let metrics = gateway.metrics.clone();
    let logging_gateway = gateway.clone();
    if serves_clients {
        let (primary_address, primary_binding) = listeners.primary();
        let primary_template = listeners.primary_template();
        application.configure_ws(move |builder| {
            builder
                .addr(primary_address.to_string())
                .auth_timeout(Duration::from_millis(
                    primary_template.authentication_timeout_ms,
                ))
                .max_connections(primary_template.max_connections)
                .max_unauthenticated(primary_template.max_unauthenticated)
                .policy(nasa::ws::deny_all())
                .authorize(|_, _| async { nasa::ws::AuthResult::fail("unsupported_profile") })
                .protocol_handler(move |stream, context| {
                    let gateway = gateway.clone();
                    let binding = primary_binding.clone();
                    async move {
                        crate::adapters::inbound::connection::accept(
                            gateway, stream, context, binding,
                        )
                        .await
                    }
                })
        })?;
    }
    application
        .serve_when_ready("nacket-routes", move |shutdown| async move {
            if serves_clients {
                listeners.activate();
            }
            let mut timer = tokio::time::interval(Duration::from_millis(100));
            timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    biased;
                    _ = shutdown.cancelled() => {
                        // 停机优先于配置通知，下线后的门禁不能被迟到的 online 候选重新开放。
                        routes.close();
                        return Ok::<(), anyhow::Error>(());
                    }
                    change = updates.changed() => {
                        change?;
                        metrics.set("configuration_pending_restart", 0);
                        let view = updates.borrow_and_update().clone();
                        let candidate = view.snapshot().section::<Deployment>("nacket");
                        match candidate {
                            Ok(candidate) => {
                                let connection_logging = candidate.connection_logging;
                                metrics.add(
                                    crate::observability::Counter::Configuration,
                                    "reload",
                                    "accepted",
                                    1,
                                );
                                let work = metrics.measure(
                                    crate::observability::Counter::Configuration,
                                    "apply",
                                    "configuration_apply",
                                    "ok",
                                    async {
                                        if let Some(control) = &control {
                                            let committing = routes.clone();
                                            let snapshot = candidate.clone();
                                            routes.persistence.run(move || committing.apply(snapshot)).await?;
                                            control.provision(&candidate).await
                                        } else {
                                            listeners.apply(candidate).await
                                        }
                                    },
                                );
                                // 持久提交开始后仍持有完成责任，不能因取消等待而遗失阻塞任务。
                                let applied = work.await;
                                if let Err(error) = applied {
                                    if error.to_string().ends_with("require restart")
                                        || error.to_string().ends_with("requires restart")
                                    {
                                        metrics.set("configuration_pending_restart", 1);
                                        metrics.add(
                                            crate::observability::Counter::Configuration,
                                            "restart_required",
                                            "rejected",
                                            1,
                                        );
                                    }
                                    tracing::warn!(
                                        reason = "candidate_rejected",
                                        "nacket configuration was not applied"
                                    );
                                } else {
                                    // 日志策略只跟随完整成功的产品应用，拒绝或部分提交不能提前关闭记录。
                                    logging_gateway.configure_connection_logging(connection_logging);
                                }
                            }
                            Err(_) => {
                                metrics.add(
                                    crate::observability::Counter::Configuration,
                                    "reload",
                                    "invalid",
                                    1,
                                );
                                tracing::warn!(
                                    reason = "invalid_candidate",
                                    "nacket configuration was not applied"
                                );
                            }
                        }
                    }
                    _ = timer.tick() => routes.advance(),
                }
            }
        })
        .await?;
    Ok(())
}

struct ControlInitializer(
    /// 在宿主进入 Ready 前建立持久节点身份的控制运行域。
    Arc<crate::application::control::NodeRuntime>,
);
impl nasa::application::Initialization for ControlInitializer {
    /// 业务作用：在 Ready 前取得持久节点代际，集群节点不能在权威不可用时用随机材料启动。
    /// 参数说明：
    /// - `context`：应用共享启动截止点。
    ///
    /// 返回：控制身份已确认；清单由独立任务推进，具体路由仍等待全局准备屏障与许可。
    fn initialize<'a>(
        &'a mut self,
        context: &'a mut nasa::application::InitializationContext<'_>,
    ) -> nasa::application::ApplicationFuture<'a> {
        Box::pin(async move {
            match tokio::time::timeout_at(context.deadline(), self.0.initialize()).await {
                Ok(Ok(())) => Ok(()),
                _ => Err(nasa::application::ApplicationError::new(
                    nasa::application::ComponentId::Application,
                    nasa::application::ApplicationPhase::Prepare,
                    "socket control baseline unavailable",
                )),
            }
        })
    }
}

struct IdentityInitializer(
    /// 持有冻结身份策略和初始路由门禁的共享运行域。
    Arc<crate::application::gateway::Gateway>,
);
impl nasa::application::Initialization for IdentityInitializer {
    /// 业务作用：在受管出站客户端完成 Prepare 后预热身份来源，成功后才开放路由初始门禁。
    /// 参数说明：
    /// - `context`：宿主共享启动截止点和取消边界。
    ///
    /// 返回：全部身份依赖就绪；失败阻止 napp 进入 Ready。
    fn initialize<'a>(
        &'a mut self,
        context: &'a mut nasa::application::InitializationContext<'_>,
    ) -> nasa::application::ApplicationFuture<'a> {
        Box::pin(async move {
            let prepared =
                tokio::time::timeout_at(context.deadline(), self.0.prepare_identities(None)).await;
            if !matches!(prepared, Ok(Ok(()))) {
                return Err(nasa::application::ApplicationError::new(
                    nasa::application::ComponentId::Application,
                    nasa::application::ApplicationPhase::Prepare,
                    "socket identity preparation failed",
                ));
            }
            // 初始门禁发布晚于密钥和远程客户端准备，真正的 accept 继续等待全应用 Ready。
            self.0.routes.prepare_protocol().map_err(|_| {
                nasa::application::ApplicationError::new(
                    nasa::application::ComponentId::Application,
                    nasa::application::ApplicationPhase::Prepare,
                    "socket route preparation failed",
                )
            })
        })
    }
}

struct RouteOwner {
    /// 本地路由及数据卷责任，关闭时先撤销准入，再等待已经接纳的持久任务。
    routes: Arc<RouteService>,
    /// 与产品运行共用的指标来源，记录持久排空阶段的单调耗时。
    metrics: Arc<crate::observability::Metrics>,
}

impl nasa::application::ManagedResource for RouteOwner {
    /// 业务作用：在业务资源回收时撤销路由权限，复用 napp 的关闭顺序。
    /// 参数说明：
    ///
    /// - `context`：宿主共享关闭截止点。
    ///
    /// 返回：本地入口已撤销且真实持久工作全部结束；超时明确报告未完成，数据卷 owner 仍由实际任务保留。
    fn shutdown<'a>(
        &'a mut self,
        context: &'a nasa::application::ShutdownContext,
    ) -> nasa::application::ApplicationFuture<'a> {
        self.routes.close();
        self.routes.persistence.close();
        let observed = self.metrics.timer("shutdown");
        Box::pin(async move {
            let _observed = observed;
            self.routes
                .persistence
                .stop_and_wait(context.deadline().into())
                .await
                .map_err(|_| {
                    nasa::application::ApplicationError::new(
                        nasa::application::ComponentId::Application,
                        nasa::application::ApplicationPhase::Stopping,
                        "socket persistence drain incomplete",
                    )
                })
        })
    }
}

impl Drop for RouteOwner {
    /// 业务作用：在启动中断或资源登记失败时关闭已经恢复的业务入口。
    /// 参数说明: 无。
    ///
    /// 返回：关闭门禁，不删除已提交的路由记录。
    fn drop(&mut self) {
        self.routes.close();
        self.routes.persistence.close();
    }
}
