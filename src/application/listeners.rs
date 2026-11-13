//! 派生 listener 的准备、激活与回收，实际 socket 与配额始终由同一受管 owner 持有。

mod access;

use super::gateway::Gateway;
use crate::{
    adapters::inbound::listener::{ListenerPool, PortReservation},
    config::{
        listeners::{ListenerTemplate, Protocol},
        Deployment,
    },
    domain::route::Access,
};
use nasa::ws::{RunningServer, Server};
use std::{
    collections::{BTreeMap, BTreeSet},
    net::SocketAddr,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

/// 实际端口绑定的接入作用域，不能由 AUTH 或 namespace 覆盖。
#[derive(Clone, PartialEq, Eq)]
pub struct Binding {
    /// 本次实际 socket owner 的唯一标识，旧公共目录不能跨重绑复用。
    pub binding_id: String,
    /// 冻结的监听模板引用，决定地址、准入与协议能力。
    pub listener_ref: String,
    /// 独立端口唯一允许的路由；共享端口为空，仍逐路由授权。
    pub route_id: Option<String>,
    /// 当前绑定冻结的路由接入声明，不接受客户端覆盖。
    pub access: Option<Access>,
    /// 该实际监听允许的物理适配器集合，路由能力不能扩大此集合。
    pub protocols: BTreeSet<Protocol>,
    /// 自物理接入开始计算的认证总预算，单位毫秒。
    pub authentication_timeout_ms: u64,
}

impl Binding {
    /// 业务作用：判断路由变更是否可继续使用同一个真实 socket，路径和业务 schema 不要求重新绑定端口。
    /// 参数说明：
    /// - `other`：候选定义需要的监听作用域。
    ///
    /// 返回：端口策略、模板和物理协议都一致时为 true。
    fn same_socket(&self, other: &Self) -> bool {
        self.listener_ref == other.listener_ref
            && self.route_id == other.route_id
            && self.protocols == other.protocols
            && self.authentication_timeout_ms == other.authentication_timeout_ms
            && self.access.as_ref().map(|a| &a.dedicated_port)
                == other.access.as_ref().map(|a| &a.dedicated_port)
    }
}

struct Owned {
    /// 与实际运行 socket 同生命周期的不可变接入作用域。
    binding: Binding,
    /// 实际监听及任务的关闭责任，准备失败也必须等待其退出。
    server: RunningServer,
    /// 独立端口及模板容量预约，实际 owner 释放后才能重新使用。
    _reservation: Option<PortReservation>,
}
#[derive(Default)]
struct State {
    /// 所有仍持有资源的派生监听，包含准备中和排空中的 owner。
    all: BTreeMap<String, Arc<Owned>>,
    /// 当前业务绑定键到实际 owner ID 的映射，切换不提前释放旧监听。
    current: BTreeMap<String, String>,
}

/// 聚合全部派生服务，失败准备也保持关闭责任直至实际任务退出。
pub struct Listeners {
    /// 共享路由及会话运行域，派生端口不能创建独立权限体系。
    gateway: Arc<Gateway>,
    /// 启动时冻结的有限监听模板目录。
    templates: BTreeMap<String, ListenerTemplate>,
    /// 各模板的端口和容量分配器，自动端口必须真实 bind 成功。
    pools: BTreeMap<String, Arc<ListenerPool>>,
    /// 实际 owner 与当前绑定索引的短临界区，不能跨异步关闭持锁。
    state: Mutex<State>,
    /// 串行化准备、提交和绑定切换，避免并发候选覆盖实际资源责任。
    serial: tokio::sync::Mutex<()>,
    /// 应用已允许监听接流的事实，关闭标志仍具有优先权。
    active: AtomicBool,
    /// 永久停止新监听准备和激活的标志，迟到任务不能重新开放端口。
    closed: AtomicBool,
}

impl Listeners {
    /// 业务作用：读取派生监听的有限资源目录，不把准备中的 socket 当作已开放入口。
    /// 参数说明: 无。
    ///
    /// 返回：实际持有的监听数量及管理器是否已开放准入；关闭标志优先于旧激活状态。
    pub fn observation(&self) -> (u64, bool) {
        let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        (
            state.all.len() as u64,
            self.active.load(Ordering::Acquire) && !self.closed.load(Ordering::Acquire),
        )
    }

    /// 业务作用：冻结监听模板和所有保留端口，空模板声明沿用 napp 主业务地址。
    /// 参数说明：
    /// - `gateway`：路由与共享资源运行域。
    /// - `tree`：同一 naml 快照的完整部署树。
    ///
    /// 返回：尚未绑定派生 socket 的受管目录；端口冲突或模板非法时拒绝。
    pub fn new(gateway: Arc<Gateway>, tree: &serde_json::Value) -> anyhow::Result<Arc<Self>> {
        let mut templates = gateway.configuration.listeners.clone();
        if templates.is_empty() {
            let address: SocketAddr = tree
                .pointer("/ws/addr")
                .and_then(|v| v.as_str())
                .unwrap_or("0.0.0.0:3030")
                .parse()?;
            let template = ListenerTemplate {
                bind_address: address.ip(),
                default_port: address.port(),
                authentication_timeout_ms: tree
                    .pointer("/ws/auth_timeout_ms")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(5000),
                ..Default::default()
            };
            templates.insert(gateway.configuration.primary_listener.clone(), template);
        }
        let mut reserved = BTreeSet::new();
        if gateway.configuration.management.enabled {
            let port = tree
                .pointer("/server/port")
                .and_then(|value| value.as_u64())
                .ok_or_else(|| anyhow::anyhow!("management port required"))?;
            anyhow::ensure!((1..=65535).contains(&port), "management port invalid");
            reserved.insert(port as u16);
        }
        if let Some(address) = tree
            .pointer("/grafana/observability/prometheus/scrape/bind")
            .and_then(|v| v.as_str())
        {
            anyhow::ensure!(
                reserved.insert(address.parse::<SocketAddr>()?.port()),
                "monitoring port is reserved"
            );
        }
        for template in templates.values() {
            template.validate()?;
            anyhow::ensure!(
                reserved.insert(template.default_port),
                "listener port is reserved or duplicated"
            );
        }
        let mut pools = BTreeMap::new();
        for (name, template) in &templates {
            let ports = &template.dedicated_ports;
            pools.insert(
                name.clone(),
                ListenerPool::new(
                    template.bind_address,
                    (ports.auto_start, ports.auto_end),
                    ports.max_listeners,
                    reserved.clone(),
                )?,
            );
        }
        Ok(Arc::new(Self {
            gateway,
            templates,
            pools,
            state: Mutex::new(State::default()),
            serial: tokio::sync::Mutex::new(()),
            active: AtomicBool::new(false),
            closed: AtomicBool::new(false),
        }))
    }

    /// 业务作用：取得 napp 主 listener 的精确地址及协议作用域，使共享端口也服从模板。
    /// 参数说明: 无。
    ///
    /// 返回：主业务监听地址和不可由客户端更改的绑定。
    pub fn primary(&self) -> (SocketAddr, Binding) {
        let name = &self.gateway.configuration.primary_listener;
        let template = &self.templates[name];
        (
            SocketAddr::new(template.bind_address, template.default_port),
            self.binding(name, None, None),
        )
    }

    /// 业务作用：提供主监听器的准入预算，保证主端口和派生端口使用同一模板语义。
    /// 参数说明: 无。
    ///
    /// 返回：启动时冻结的主模板快照。
    pub fn primary_template(&self) -> ListenerTemplate {
        self.templates[&self.gateway.configuration.primary_listener].clone()
    }

    /// 业务作用：准备所有初始派生 listener，保持 accept 暂停直到整个应用进入 Ready。
    /// 参数说明: 无。
    ///
    /// 返回：真实端口均已取得；失败会关闭本次准备的资源。
    pub async fn prepare(&self) -> anyhow::Result<()> {
        let _serial = self.serial.lock().await;
        let definitions = self.gateway.routes.definitions();
        let desired = self.desired(&definitions)?;
        let staged = self.stage(&desired, &BTreeSet::new()).await?;
        self.install(desired, staged).await
    }

    /// 业务作用：先准备新监听再提交路由，旧 writer 退出后才发布替换绑定。
    /// 参数说明：
    /// - `candidate`：来自同一配置快照的完整路由候选。
    ///
    /// 返回：路由和监听应用完成；失败保持受影响入口关闭，不自动退回共享端口。
    pub async fn apply(&self, candidate: Deployment) -> anyhow::Result<()> {
        let _observed = self.gateway.metrics.timer("route_transition");
        let _serial = self.serial.lock().await;
        anyhow::ensure!(
            !self.closed.load(Ordering::Acquire),
            "listener manager closed"
        );
        candidate.validate()?;
        self.gateway.prepare_identities(Some(&candidate)).await?;
        let mut definitions: BTreeMap<_, _> = self
            .gateway
            .routes
            .definitions()
            .into_iter()
            .map(|route| (route.route_id.clone(), route))
            .collect();
        for route in &candidate.routes {
            definitions.insert(route.route_id.clone(), route.clone());
        }
        let desired = self.desired(&definitions.into_values().collect::<Vec<_>>())?;
        let staged = self.stage(&desired, &BTreeSet::new()).await?;
        let routes = self.gateway.routes.clone();
        let applied = self
            .gateway
            .routes
            .persistence
            .run(move || routes.apply(candidate))
            .await;
        if let Err(error) = applied {
            self.close_ids(
                &staged.values().cloned().collect::<Vec<_>>(),
                Duration::from_secs(5),
            )
            .await?;
            return Err(error);
        }
        self.install(desired, staged).await
    }

    /// 业务作用：为已提交集群定义准备冻结身份引用和真实监听，路由门禁继续等待权威许可。
    /// 参数说明：
    /// - `definition`：持久控制日志中的完整定义。
    /// - `fixed_ports`：同一权威快照中全部启用路由的指定端口，包含尚未开始本地准备的路由。
    ///
    /// 返回：已准备的关闭门禁；任何资源准备失败都不开放部分入口。
    pub async fn prepare_control(
        &self,
        definition: &crate::domain::control::Definition,
        fixed_ports: &BTreeSet<u16>,
    ) -> anyhow::Result<Arc<crate::domain::route::RouteGate>> {
        let _observed = self.gateway.metrics.timer("route_transition");
        let _serial = self.serial.lock().await;
        anyhow::ensure!(
            !self.closed.load(Ordering::Acquire),
            "listener manager closed"
        );
        let mut candidate = self.gateway.configuration.clone();
        candidate.route_provisioning.enabled = false;
        candidate.routes = vec![definition.route.clone()];
        candidate.endpoints = definition.endpoints.clone();
        candidate.verifiers = definition.verifiers.clone();
        candidate.validate()?;
        self.gateway.prepare_identities(Some(&candidate)).await?;
        let mut definitions = self
            .gateway
            .routes
            .definitions()
            .into_iter()
            .map(|route| (route.route_id.clone(), route))
            .collect::<BTreeMap<_, _>>();
        definitions.insert(definition.route.route_id.clone(), definition.route.clone());
        let desired = self.desired(&definitions.into_values().collect::<Vec<_>>())?;
        let staged = self.stage(&desired, fixed_ports).await?;
        let routes = self.gateway.routes.clone();
        let frozen = definition.clone();
        let prepared = self
            .gateway
            .routes
            .persistence
            .run(move || routes.prepare_control(&frozen))
            .await;
        let gate = match prepared {
            Ok(gate) => gate,
            Err(error) => {
                self.close_ids(
                    &staged.into_values().collect::<Vec<_>>(),
                    Duration::from_secs(5),
                )
                .await?;
                return Err(error);
            }
        };
        // 监听成功只建立接入资源，路由仍处于关闭快照，不把 bind 结果当作上线许可。
        self.install(desired, staged).await?;
        Ok(gate)
    }

    /// 业务作用：在 napp Ready 屏障之后开放已准备的派生 listener，关闭后不能重新激活。
    /// 参数说明: 无。
    ///
    /// 返回：当前全部真实绑定开放 accept，业务仍需路由与身份门禁。
    pub fn activate(&self) {
        if self.closed.load(Ordering::Acquire) {
            return;
        }
        self.active.store(true, Ordering::Release);
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        for id in state.current.values() {
            if let Some(owner) = state.all.get(id) {
                owner.server.activate();
            }
        }
    }

    /// 业务作用：提供本节点真实绑定端口的只读投影，不把内核分配值写回路由定义。
    /// 参数说明: 无。
    ///
    /// 返回：内部绑定键到实际地址；外部目录仍需配置公网映射。
    pub fn addresses(&self) -> BTreeMap<String, SocketAddr> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        state
            .current
            .iter()
            .filter_map(|(key, id)| {
                state
                    .all
                    .get(id)
                    .map(|owner| (key.clone(), owner.server.local_addr))
            })
            .collect()
    }

    /// 业务作用：停止全部派生 accept 并在同一个剩余预算内等待真实连接任务退出。
    /// 参数说明：
    /// - `budget`：宿主全局停机还允许使用的时长。
    ///
    /// 返回：全部责任已退出；超时保留 owner 并返回失败，不报告端口已完成回收。
    pub async fn close(&self, budget: Duration) -> anyhow::Result<()> {
        let deadline = tokio::time::Instant::now() + budget;
        self.closed.store(true, Ordering::Release);
        self.gateway.routes.close();
        // 先取消现有 accept，再等配置准备退出；关闭标志阻止迟到 bind 继续发布新资源。
        for owner in self
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .all
            .values()
        {
            owner.server.shutdown();
        }
        let _serial = tokio::time::timeout_at(deadline, self.serial.lock()).await?;
        let ids = self
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .all
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        self.close_ids(
            &ids,
            deadline.saturating_duration_since(tokio::time::Instant::now()),
        )
        .await
    }

    /// 业务作用：按模板构造不可变绑定事实，使旧端口不能接入已迁移到新端口的路由。
    /// 参数说明：
    /// - `listener`：已验证的模板名。
    /// - `route`：独立端口所属路由，共享绑定为 None。
    /// - `access`：独立路由当时的接入定义，共享绑定为 None。
    ///
    /// 返回：绑定字段和模板协议集合的快照。
    fn binding(&self, listener: &str, route: Option<String>, access: Option<Access>) -> Binding {
        let template = &self.templates[listener];
        Binding {
            binding_id: if route.is_none()
                && listener == self.gateway.configuration.primary_listener
            {
                self.gateway.startup_id.clone()
            } else {
                crate::domain::message::control::nonce()
            },
            listener_ref: listener.into(),
            route_id: route,
            access,
            protocols: template.protocols.clone(),
            authentication_timeout_ms: template.authentication_timeout_ms,
        }
    }

    /// 业务作用：编译本节点实际需要的派生监听集合，未启用独立端口不创建额外 socket。
    /// 参数说明：
    /// - `definitions`：包含持久保留项的完整路由快照。
    ///
    /// 返回：所有派生共享或独立绑定；未批准的模板或端口开关拒绝。
    fn desired(
        &self,
        definitions: &[crate::domain::route::RouteDefinition],
    ) -> anyhow::Result<BTreeMap<String, Binding>> {
        let mut desired = BTreeMap::new();
        for name in self.templates.keys() {
            if name != &self.gateway.configuration.primary_listener {
                desired.insert(format!("shared:{name}"), self.binding(name, None, None));
            }
        }
        for definition in definitions {
            if !definition.enabled || !definition.access.dedicated_port.enabled {
                continue;
            }
            let name = &definition.access.listener_ref;
            let template = self
                .templates
                .get(name)
                .ok_or_else(|| anyhow::anyhow!("listener template missing"))?;
            anyhow::ensure!(template.dedicated_ports.allowed, "dedicated ports disabled");
            if template.advertise.enabled {
                if let Some(port) = definition.access.dedicated_port.port {
                    template.advertise.public_port(port)?;
                } else {
                    // 自动候选范围必须全部由外部网络声明覆盖，不能先绑定一个未映射端口再宣告 Serving。
                    template.advertise.covers(
                        template.dedicated_ports.auto_start,
                        template.dedicated_ports.auto_end,
                    )?;
                }
            }
            desired.insert(
                format!("route:{}", definition.route_id),
                self.binding(
                    name,
                    Some(definition.route_id.clone()),
                    Some(definition.access.clone()),
                ),
            );
        }
        Ok(desired)
    }

    /// 业务作用：为变化的绑定直接取得真实 socket 并登记关闭责任，显式端口优先于自动分配。
    /// 参数说明：
    /// - `desired`：已经编译的完整目标集合。
    /// - `fixed_ports`：权威快照中指定的端口，只约束自动分配，不授予端口所有权。
    ///
    /// 返回：新准备的绑定键与 owner ID；没有发布 accept 或业务权限。
    async fn stage(
        &self,
        desired: &BTreeMap<String, Binding>,
        fixed_ports: &BTreeSet<u16>,
    ) -> anyhow::Result<BTreeMap<String, String>> {
        let mut fixed_ports = fixed_ports.clone();
        fixed_ports.extend(
            desired
                .values()
                .filter_map(|binding| binding.access.as_ref()?.dedicated_port.port),
        );
        let mut pending = Vec::new();
        {
            let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
            for (key, binding) in desired {
                if state
                    .current
                    .get(key)
                    .and_then(|id| state.all.get(id))
                    .is_none_or(|owner| !owner.binding.same_socket(binding))
                {
                    pending.push((key.clone(), binding.clone()));
                }
            }
        }
        pending.sort_by_key(|(_, binding)| {
            binding
                .access
                .as_ref()
                .is_some_and(|access| access.dedicated_port.port.is_none())
        });
        let mut staged = BTreeMap::new();
        for (key, binding) in pending {
            anyhow::ensure!(
                !self.closed.load(Ordering::Acquire),
                "listener manager closed"
            );
            let result = self.bind(binding, &fixed_ports).await;
            let owner = match result {
                Ok(owner) => owner,
                Err(error) => {
                    self.close_ids(
                        &staged.values().cloned().collect::<Vec<_>>(),
                        Duration::from_secs(5),
                    )
                    .await?;
                    return Err(error);
                }
            };
            let id = owner.binding.binding_id.clone();
            // 真实服务创建后立即纳入聚合 owner，后续提交失败或异步取消都不会丢失关闭责任。
            self.state
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .all
                .insert(id.clone(), Arc::new(owner));
            staged.insert(key, id);
            if self.closed.load(Ordering::Acquire) {
                self.close_ids(
                    &staged.values().cloned().collect::<Vec<_>>(),
                    Duration::from_secs(5),
                )
                .await?;
                anyhow::bail!("listener manager closed");
            }
        }
        Ok(staged)
    }

    /// 业务作用：从唯一端口池移交真实 listener 给通用 naws 协议宿主，不进行探测后重绑。
    /// 参数说明：
    /// - `binding`：已冻结的实际端口作用域。
    /// - `fixed_ports`：本地目录与当前权威快照共同保留的指定端口。
    ///
    /// 返回：保持暂停的运行服务与端口预约；绑定失败不改变其它路由。
    async fn bind(&self, binding: Binding, fixed_ports: &BTreeSet<u16>) -> anyhow::Result<Owned> {
        let template = &self.templates[&binding.listener_ref];
        let (listener, reservation) = if let Some(access) = &binding.access {
            let bound = self.pools[&binding.listener_ref]
                .bind(
                    access.dedicated_port.port,
                    fixed_ports,
                    Duration::from_millis(template.dedicated_ports.bind_timeout_ms),
                )
                .await?;
            let (listener, reservation) = bound.into_parts();
            (listener, Some(reservation))
        } else {
            (
                tokio::net::TcpListener::bind(SocketAddr::new(
                    template.bind_address,
                    template.default_port,
                ))
                .await?,
                None,
            )
        };
        let gateway = self.gateway.clone();
        let scope = binding.clone();
        let server = Server::builder()
            .multiplex(true)
            .auth_timeout(Duration::from_millis(template.authentication_timeout_ms))
            .max_connections(template.max_connections)
            .max_unauthenticated(template.max_unauthenticated)
            .max_frame_bytes(self.gateway.configuration.transport.max_frame_bytes)
            .authorize(|_, _| async { nasa::ws::AuthResult::fail("unsupported_profile") })
            .policy(nasa::ws::deny_all())
            .protocol_handler(move |stream, context| {
                let gateway = gateway.clone();
                let scope = scope.clone();
                async move {
                    crate::adapters::inbound::connection::accept(gateway, stream, context, scope)
                        .await
                }
            })
            .build()?
            .bind_suspended_on(listener)
            .await?;
        Ok(Owned {
            binding,
            server,
            _reservation: reservation,
        })
    }

    /// 业务作用：在路由提交后先回收被替换的真实 owner，再发布新绑定并允许 accept。
    /// 参数说明：
    /// - `desired`：完整目标绑定。
    /// - `staged`：已经成功 bind 的新 owner。
    ///
    /// 返回：目录与 listener 一致；旧任务未退出则不激活新 listener。
    async fn install(
        &self,
        desired: BTreeMap<String, Binding>,
        staged: BTreeMap<String, String>,
    ) -> anyhow::Result<()> {
        let retiring = {
            let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
            state
                .current
                .iter()
                .filter(|(key, _)| !desired.contains_key(*key) || staged.contains_key(*key))
                .map(|(_, id)| id.clone())
                .collect::<Vec<_>>()
        };
        self.close_ids(&retiring, Duration::from_secs(5)).await?;
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        anyhow::ensure!(
            !self.closed.load(Ordering::Acquire),
            "listener manager closed"
        );
        state.current.retain(|key, _| desired.contains_key(key));
        for (key, id) in staged {
            if self.active.load(Ordering::Acquire) {
                state.all[&id].server.activate();
            }
            state.current.insert(key, id);
        }
        Ok(())
    }

    /// 业务作用：用单个截止时间关闭一组监听器，真实任务退出后才释放端口配额。
    /// 参数说明：
    /// - `ids`：由本管理器创建的精确 owner ID。
    /// - `budget`：本轮关闭共享的最大时长。
    ///
    /// 返回：目标责任均已退出；超时保留资源事实以供最终停机继续回收。
    async fn close_ids(&self, ids: &[String], budget: Duration) -> anyhow::Result<()> {
        let owners = {
            let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
            ids.iter()
                .filter_map(|id| state.all.get(id).map(|owner| (id.clone(), owner.clone())))
                .collect::<Vec<_>>()
        };
        for (_, owner) in &owners {
            owner.server.shutdown();
        }
        let deadline = tokio::time::Instant::now() + budget;
        for (id, owner) in owners {
            anyhow::ensure!(
                owner
                    .server
                    .shutdown_graceful(
                        deadline.saturating_duration_since(tokio::time::Instant::now())
                    )
                    .await,
                "listener drain deadline"
            );
            self.state
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .all
                .remove(&id);
        }
        Ok(())
    }
}

impl Drop for Listeners {
    /// 业务作用：聚合资源异常释放时仍先取消所有真实监听任务，正常路径由受管 shutdown 等待退出。
    /// 参数说明: 无。
    ///
    /// 返回：停止新接入，保留框架任务自己的最终 socket owner。
    fn drop(&mut self) {
        for owner in self
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .all
            .values()
        {
            owner.server.shutdown();
        }
    }
}

/// 将派生监听聚合关闭责任纳入 napp 的统一剩余预算。
pub struct ListenerOwner(
    /// 纳入宿主关闭流程的全部派生监听责任，实际任务退出前保留 owner。
    pub Arc<Listeners>,
);
impl nasa::application::ManagedResource for ListenerOwner {
    /// 业务作用：使用宿主唯一停机期限收回所有派生端口和连接任务。
    /// 参数说明：
    /// - `context`：宿主关闭原因及剩余预算。
    ///
    /// 返回：真实责任已经结束；超时向宿主报告失败而非伪装完成。
    fn shutdown<'a>(
        &'a mut self,
        context: &'a nasa::application::ShutdownContext,
    ) -> nasa::application::ApplicationFuture<'a> {
        Box::pin(async move {
            self.0.close(context.remaining()).await.map_err(|_| {
                nasa::application::ApplicationError::new(
                    nasa::application::ComponentId::Application,
                    nasa::application::ApplicationPhase::Stopping,
                    "socket listener drain incomplete",
                )
            })
        })
    }
}
