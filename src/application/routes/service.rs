use super::ledger::{LocalLedger, RouteRecord};
use crate::{config::Deployment, domain::route::RouteGate};
use std::{
    collections::BTreeMap,
    path::Path,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex, RwLock,
    },
};

struct RouteState {
    /// 当前接受的持久定义及已解析依赖，作为恢复与比较依据。
    record: RouteRecord,
    /// 该定义实际数据面门禁，旧会话保留旧 gate 直到排空。
    gate: Arc<RouteGate>,
    /// 按同一冻结定义编译的事件和端点查找计划。
    plan: Arc<crate::domain::route::compiled::CompiledRoute>,
}

/// 持久路由定义与本地写出门禁的编排者，身份授权由会话运行域独立管理。
pub struct RouteService {
    /// 当前已应用部署快照；短锁仅用于克隆或发布，不跨磁盘等待。
    deployment: Mutex<Deployment>,
    /// 持久路由定义、控制高水位和断言消费证据的节点账本。
    ledger: LocalLedger,
    /// 当前路由到定义、门禁和编译计划的目录，发布时保持三者一致。
    routes: RwLock<BTreeMap<String, Arc<RouteState>>>,
    /// 产品接入协议是否已经完成准备；未准备时本地路由不得开放。
    custom_protocol: AtomicBool,
    /// 共享给全部 gate 的永久关闭事实，停止时不等待目录或磁盘锁。
    closed: Arc<AtomicBool>,
    /// 串行化路由持久变更，防止后完成的旧候选覆盖新定义。
    edits: Mutex<()>,
    /// 实际阻塞持久任务的容量和关闭责任，取消等待不能提前释放 owner。
    pub persistence: Arc<crate::application::persistence::Persistence>,
    /// 新定义开放前必须排空的旧 gate 集合，旧写出未结束不得激活。
    activation_blockers: Mutex<BTreeMap<String, Vec<Arc<RouteGate>>>>,
    /// 各固定入口的有限新鲜度证据，连接存活不能替代消费可用。
    dependencies: BTreeMap<String, Arc<crate::domain::route::dependency::Dependency>>,
    /// 授权控制实际执行进度的独立证据，不能用 broker 健康刷新。
    authorization_progress: BTreeMap<String, Arc<crate::domain::route::dependency::Dependency>>,
    /// 本地断言持久消费的并发名额，实际任务结束后归还。
    assertion_work: Arc<tokio::sync::Semaphore>,
}

impl RouteService {
    /// 业务作用：在有限阻塞工作预算内持久消费单节点断言，取消等待不提前释放实际落盘责任。
    /// 参数说明：
    /// - `key`：已验证的断言作用域摘要。
    /// - `owner`：精确认证尝试摘要。
    /// - `expires_at`：原始断言的整数毫秒到期时间。
    ///
    /// 返回：本地持久提交成功；排队超限、集群模式或未知落盘结果均拒绝接入。
    pub async fn consume_local_assertion(
        self: &Arc<Self>,
        key: String,
        owner: String,
        expires_at: crate::domain::message::UnixMillis,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(
            !self.configuration().control.clustered(),
            "cluster_assertion_authority_required"
        );
        let capacity = self
            .assertion_work
            .clone()
            .try_acquire_owned()
            .map_err(|_| anyhow::anyhow!("assertion_commit_capacity"))?;
        let service = self.clone();
        self.persistence
            .run(move || {
                let _capacity = capacity;
                // 持久任务继续持有账本和文件锁；连接取消不能使尚未结束的写入失去 owner。
                service.ledger.consume_assertion(
                    &key,
                    &owner,
                    expires_at,
                    crate::adapters::outbound::identity::now()?,
                )
            })
            .await
    }
    /// 业务作用：按部署策略提供有限的路由资格观察，动态空目录不阻塞默认运行资格。
    /// 参数说明: 无。
    ///
    /// 返回：ready 或固定不可用原因；本方法不取得写出许可。
    pub fn readiness_status(&self) -> &'static str {
        if self.closed.load(Ordering::Acquire) {
            return "routes_closed";
        }
        let deployment = self
            .deployment
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if let crate::config::readiness::Readiness::RequiredRoutes {
            route_ids,
        } = &deployment.readiness
        {
            let routes = self
                .routes
                .read()
                .unwrap_or_else(|error| error.into_inner());
            for id in route_ids {
                let Some(route) = routes.get(id) else {
                    return "required_route_missing";
                };
                if !route.record.definition.enabled || route.gate.observation().0 != "online" {
                    return "required_route_unavailable";
                }
            }
        }
        "ready"
    }

    /// 业务作用：从有界路由目录取得运行数量，不在监控抓取中复制完整定义或扫描连接。
    /// 参数说明: 无。
    ///
    /// 返回：在线、保护、下线路由与实际 writer 数；不作为控制权威。
    pub fn observation(&self) -> [u64; 4] {
        let mut counts = [0u64; 4];
        for route in self
            .routes
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .values()
        {
            let (state, writers) = route.gate.observation();
            let index = if !route.record.definition.enabled {
                2
            } else if state == "online" {
                0
            } else {
                1
            };
            counts[index] += 1;
            counts[3] += writers as u64;
        }
        counts
    }

    /// 业务作用：在集群节点后台投影期望在线路由的最小本地许可寿命，未授权路由不能省略。
    /// 参数说明：
    /// - `observation`：固定控制指标来源，不参与路由权限判断。
    ///
    /// 返回：发布当前路由数量与最小剩余时长，不改变门禁。
    pub fn project_control_permits(&self, observation: &crate::observability::Control) {
        let routes = self
            .routes
            .read()
            .unwrap_or_else(|error| error.into_inner());
        let mut count = 0;
        let mut remaining = None;
        for route in routes
            .values()
            .filter(|route| route.record.definition.enabled)
        {
            count += 1;
            let value = route.gate.permit_remaining();
            remaining =
                Some(remaining.map_or(value, |previous: std::time::Duration| previous.min(value)));
        }
        observation.permits(count, remaining.unwrap_or_default());
    }

    /// 业务作用：在后台更新固定名称指标，入口仅在消费和授权执行证据都有效时记为可用。
    /// 参数说明：
    /// - `scopes`：启动冻结的详细指标目录，关闭时不扫描路由。
    ///
    /// 返回：替换路由状态、实际 writer 和入口可用性观察，不赋予任何业务权限。
    pub fn project_scopes(&self, scopes: &crate::observability::scopes::Scopes) {
        use crate::observability::scopes::Kind;
        if !scopes.enabled() {
            return;
        }
        let mut values = Vec::new();
        {
            let routes = self
                .routes
                .read()
                .unwrap_or_else(|error| error.into_inner());
            for (id, route) in routes.iter() {
                let (state, writers) = route.gate.observation();
                let state = if !route.record.definition.enabled {
                    "offline"
                } else if state == "online" {
                    "online"
                } else {
                    "protected"
                };
                values.push((id.clone(), state, 1));
                values.push((id.clone(), "writers", writers as u64));
            }
        }
        scopes.project(
            Kind::Route,
            &["online", "protected", "offline", "writers"],
            values,
        );
        let sources = self.dependencies.iter().map(|(id, dependency)| {
            let ready = dependency.status() == "ready"
                && self
                    .authorization_progress
                    .get(id)
                    .is_none_or(|progress| progress.status() == "ready");
            (
                id.clone(),
                if ready {
                    "ready"
                } else {
                    "unavailable"
                },
                1,
            )
        });
        scopes.project(Kind::Ingress, &["ready", "unavailable"], sources);
    }
    /// 业务作用：取得部署内固定 MQ 入口的可用性域，所有引用该入口的路由共享失败证据。
    /// 参数说明：
    /// - `ingress`：已配置且启用的固定入口名称。
    ///
    /// 返回：唯一依赖门禁；未部署的来源不能动态获得可信入口资格。
    pub fn dependency(
        &self,
        ingress: &str,
    ) -> anyhow::Result<Arc<crate::domain::route::dependency::Dependency>> {
        self.dependencies
            .get(ingress)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("route_ingress_unavailable"))
    }

    /// 业务作用：报告全部必要业务来源的当前证据状态，供独立 readiness 与指标读取。
    /// 参数说明: 无。
    ///
    /// 返回：固定配置名称和状态码，不包含 broker 地址和凭据。
    pub fn dependency_status(&self) -> BTreeMap<String, &'static str> {
        self.dependencies
            .iter()
            .map(|(id, dependency)| (id.clone(), dependency.status()))
            .chain(
                self.authorization_progress
                    .iter()
                    .map(|(id, dependency)| (format!("authorization:{id}"), dependency.status())),
            )
            .collect()
    }

    /// 业务作用：取得撤权执行进度的独立门禁，MQ 心跳不能替代权威提交已经推进的证据。
    /// 参数说明：
    /// - `ingress`：部署中启用的授权控制入口。
    ///
    /// 返回：该入口唯一的执行门禁；其它用途或未配置的入口拒绝。
    pub fn authorization_progress(
        &self,
        ingress: &str,
    ) -> anyhow::Result<Arc<crate::domain::route::dependency::Dependency>> {
        self.authorization_progress
            .get(ingress)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("authorization_executor_unavailable"))
    }
    /// 业务作用：限制首次建立空控制作用域的资格，已观察权威的数据卷不能自动重建丢失的账本。
    /// 参数说明: 无。
    ///
    /// 返回：尚未保存过控制高水位时为 true。
    pub fn control_is_fresh(&self) -> bool {
        !self.ledger.has_control_mark()
    }

    /// 业务作用：在控制快照被用于签发或安装许可前记录持久观察，异常立即关闭业务准入。
    /// 参数说明：
    /// - `sequence`：已提交的控制序号。
    /// - `digest`：后端验证过的完整快照摘要。
    ///
    /// 返回：高水位已落盘；作用域冲突、回退或持久失败使本进程保持关闭。
    pub fn observe_control(&self, sequence: u64, digest: &str) -> anyhow::Result<()> {
        let deployment = self.configuration();
        let result = self.ledger.observe_control(
            &deployment.cluster_id,
            &deployment.scope_epoch,
            &deployment.node_id,
            sequence,
            digest,
        );
        if result.is_err() {
            // 本节点已经见过的控制历史不能丢失，恢复前不允许用旧基线延长任何路由许可。
            self.close();
        }
        result
    }
    /// 业务作用：取得同一次已验证产品配置中的资源目录，管理定义编译不能拼接不同配置快照。
    /// 参数说明: 无。
    ///
    /// 返回：当前完整候选目录副本，既有路由仍使用各自已冻结的引用。
    pub fn configuration(&self) -> Deployment {
        self.deployment
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clone()
    }

    /// 业务作用：为显式本地模式取得持久启动代际，集群模式必须使用控制账本分配。
    /// 参数说明: 无。
    ///
    /// 返回：本地计数已落盘；集群模式或持久失败均拒绝。
    pub fn local_incarnation(&self) -> anyhow::Result<String> {
        let deployment = self
            .deployment
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        anyhow::ensure!(
            !deployment.control.clustered(),
            "cluster_incarnation_required"
        );
        self.ledger.incarnation(
            &deployment.cluster_id,
            &deployment.scope_epoch,
            &deployment.node_id,
        )
    }

    /// 业务作用：恢复持久路由并检查部署候选，启动期间保持全部接入门禁关闭。
    /// 参数说明：
    ///
    /// - `deployment`：同一配置快照中提取的产品正文。
    ///
    /// 返回：可交给 napp 装配的路由服务；账本或配置不一致时不启动监听。
    pub fn open(deployment: Deployment) -> anyhow::Result<Arc<Self>> {
        deployment.validate()?;
        let dependencies = deployment
            .ingresses
            .iter()
            .filter(|(_, ingress)| ingress.enabled)
            .map(|(id, _)| {
                (
                    id.clone(),
                    Arc::new(crate::domain::route::dependency::Dependency::default()),
                )
            })
            .collect::<BTreeMap<_, _>>();
        let (ledger, mut records) = LocalLedger::open(Path::new(&deployment.route_ledger))?;
        let authorization_progress = deployment
            .ingresses
            .iter()
            .filter(|(_, ingress)| {
                ingress.enabled
                    && ingress.purpose
                        == crate::config::messaging::IngressPurpose::AuthorizationControl
            })
            .map(|(id, _)| {
                (
                    id.clone(),
                    Arc::new(crate::domain::route::dependency::Dependency::default()),
                )
            })
            .collect::<BTreeMap<_, _>>();
        if deployment.control.clustered() {
            // 集群节点只能从当前持久权威恢复，本地缓存和旧 YAML 都没有重开路由的资格。
            records.clear();
        }
        let closed = Arc::new(AtomicBool::new(false));
        let routes = records
            .into_iter()
            .map(|(id, record)| -> anyhow::Result<_> {
                let plan = crate::domain::route::compiled::CompiledRoute::compile_with_verifiers(
                    record.definition.clone(),
                    &record.resolved_endpoints,
                    &record.resolved_verifiers,
                )?;
                let gate = make_gate(
                    &record.definition,
                    &dependencies,
                    &authorization_progress,
                    &closed,
                )?;
                Ok((
                    id,
                    Arc::new(RouteState {
                        record,
                        gate,
                        plan,
                    }),
                ))
            })
            .collect::<anyhow::Result<_>>()?;
        let service = Arc::new(Self {
            deployment: Mutex::new(deployment.clone()),
            ledger,
            routes: RwLock::new(routes),
            custom_protocol: AtomicBool::new(false),
            closed,
            edits: Mutex::new(()),
            persistence: crate::application::persistence::Persistence::new(),
            activation_blockers: Mutex::new(BTreeMap::new()),
            dependencies,
            authorization_progress,
            assertion_work: Arc::new(tokio::sync::Semaphore::new(16)),
        });
        service.apply(deployment)?;
        Ok(service)
    }

    /// 业务作用：受理完整 YAML 候选，保留未列出的记录并拒绝旧代次覆盖已提交状态。
    /// 参数说明：
    ///
    /// - `candidate`：通过同一个 naml 快照得到的完整产品配置。
    ///
    /// 返回：定义提交并发布成功；旧 writer 未排空时继续保护，由 advance 开放数据面。
    /// 无效候选不改变状态，持久写入失败保持关闭并返回错误；账本提交不等于路由已可接入。
    pub fn apply(self: &Arc<Self>, candidate: Deployment) -> anyhow::Result<()> {
        candidate.validate()?;
        let _edit = self.edits.lock().unwrap_or_else(|error| error.into_inner());
        let deployment = self.configuration();
        anyhow::ensure!(
            !self.closed.load(Ordering::Acquire),
            "route service is closed"
        );
        anyhow::ensure!(
            candidate.node_id == deployment.node_id
                && candidate.route_ledger == deployment.route_ledger
                && candidate.cluster_id == deployment.cluster_id
                && candidate.scope_epoch == deployment.scope_epoch
                && candidate.transport == deployment.transport
                && candidate.remote_calls == deployment.remote_calls
                && candidate.metric_scopes == deployment.metric_scopes
                && candidate.broker_metrics == deployment.broker_metrics
                && candidate.session_metrics == deployment.session_metrics,
            "deployment identity change requires restart"
        );
        anyhow::ensure!(
            candidate.readiness == deployment.readiness,
            "readiness changes require restart"
        );
        anyhow::ensure!(
            candidate.control == deployment.control,
            "control changes require restart"
        );
        anyhow::ensure!(
            candidate.management == deployment.management,
            "management changes require restart"
        );
        anyhow::ensure!(
            candidate.ingresses == deployment.ingresses
                && candidate.ingress_processing == deployment.ingress_processing,
            "ingress changes require restart"
        );
        anyhow::ensure!(
            candidate.publishers == deployment.publishers,
            "publisher changes require restart"
        );
        anyhow::ensure!(
            candidate.lifecycle_delivery == deployment.lifecycle_delivery,
            "lifecycle delivery changes require restart"
        );
        anyhow::ensure!(
            candidate.listeners == deployment.listeners
                && candidate.primary_listener == deployment.primary_listener,
            "listener templates require restart"
        );
        if candidate.control.clustered() {
            *self
                .deployment
                .lock()
                .unwrap_or_else(|error| error.into_inner()) = candidate;
            return Ok(());
        }
        let current = self.routes.read().unwrap_or_else(|e| e.into_inner());
        let mut records: BTreeMap<_, _> = current
            .iter()
            .map(|(id, route)| (id.clone(), route.record.clone()))
            .collect();
        let mut changed = Vec::new();
        for definition in &candidate.routes {
            let plan = crate::domain::route::compiled::CompiledRoute::compile_with_verifiers(
                definition.clone(),
                &candidate.endpoints,
                &candidate.verifiers,
            )?;
            let record = RouteRecord {
                definition: definition.clone(),
                resolved_endpoints: plan.endpoints.clone(),
                resolved_verifiers: plan.verifiers.clone(),
            };
            if let Some(old) = records.get(&definition.route_id) {
                // 相同代次只允许幂等重放；旧 YAML 不能恢复较新的下线决定。
                anyhow::ensure!(
                    definition.revision >= old.definition.revision,
                    "stale route revision"
                );
                if definition.revision == old.definition.revision {
                    anyhow::ensure!(&record == old, "same route revision has different content");
                    continue;
                }
            }
            changed.push(definition.route_id.clone());
            records.insert(definition.route_id.clone(), record);
        }
        let mut paths = std::collections::BTreeSet::new();
        for record in records.values() {
            record.definition.validate()?;
            for (adapter, path) in [
                ("native", record.definition.access.native_endpoint()),
                ("socketio", record.definition.access.socketio_namespace()),
            ] {
                if let Some(path) = path {
                    anyhow::ensure!(
                        paths.insert((
                            &record.definition.access.listener_ref,
                            record
                                .definition
                                .access
                                .dedicated_port
                                .enabled
                                .then_some(record.definition.route_id.as_str()),
                            adapter,
                            path
                        )),
                        "route endpoint is already owned"
                    );
                }
            }
        }
        if changed.is_empty() {
            *self
                .deployment
                .lock()
                .unwrap_or_else(|error| error.into_inner()) = candidate;
            return Ok(());
        }
        for id in &changed {
            if let Some(old) = current.get(id) {
                // 持久操作前先保护受影响入口；磁盘失败或取消不能留下旧权限继续服务。
                old.gate.close();
                tracing::info!(event = "route_configuration", stage = "protected", route_id = %id,
                    previous_revision = old.record.definition.revision, revision = records[id].definition.revision,
                    "route configuration transition");
                let mut blockers = self
                    .activation_blockers
                    .lock()
                    .unwrap_or_else(|e| e.into_inner());
                let gates = blockers.entry(id.clone()).or_default();
                gates.retain(|gate| !gate.drained());
                if !old.gate.drained() {
                    gates.push(old.gate.clone());
                }
            }
        }
        drop(current);
        self.ledger.commit(&records)?;
        for id in &changed {
            tracing::info!(event = "route_configuration", stage = "committed", route_id = %id,
                revision = records[id].definition.revision, desired_online = records[id].definition.enabled,
                "route configuration transition");
        }
        let mut prepared = Vec::with_capacity(changed.len());
        for id in changed {
            let record = records.remove(&id).expect("changed record is present");
            let plan = crate::domain::route::compiled::CompiledRoute::compile_with_verifiers(
                record.definition.clone(),
                &record.resolved_endpoints,
                &record.resolved_verifiers,
            )?;
            let route = Arc::new(RouteState {
                gate: make_gate(
                    &record.definition,
                    &self.dependencies,
                    &self.authorization_progress,
                    &self.closed,
                )?,
                record,
                plan,
            });
            if route.record.definition.enabled
                && !self.closed.load(Ordering::Acquire)
                && self.custom_protocol.load(Ordering::Acquire)
                && self
                    .activation_blockers
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .get(&id)
                    .is_none_or(Vec::is_empty)
            {
                // 已提交定义只有在全部旧 writer 结束后才能获得本地接流资格。
                route.gate.activate()?;
            }
            prepared.push((id, route));
        }
        // 新快照完整准备后才短暂持有目录写锁，磁盘等待不阻塞读取与停机。
        {
            let mut routes = self
                .routes
                .write()
                .unwrap_or_else(|error| error.into_inner());
            for (id, route) in prepared {
                let revision = route.record.definition.revision;
                let desired_online = route.record.definition.enabled;
                let reason = route.gate.unavailable_reason();
                routes.insert(id.clone(), route);
                // 日志晚于目录发布，精确区分已提交定义与当前数据面资格。
                tracing::info!(event = "route_configuration", stage = "published", route_id = %id,
                    revision, desired_online, local_online = reason.is_none(), reason = reason.unwrap_or("ready"),
                    "route configuration transition");
            }
        }
        *self
            .deployment
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = candidate;
        Ok(())
    }

    /// 业务作用：在全部旧 writer 退出后解除新路由的本地准备屏障。
    /// 参数说明: 无。
    ///
    /// 返回：完成屏障的有效定义取得准入资格，关闭状态不能重新开放。
    pub fn advance(&self) {
        // 维护与定义提交共享变更顺序，不能在提交中途开放旧目录或记录错误的激活代次。
        let _edit = self.edits.lock().unwrap_or_else(|error| error.into_inner());
        {
            let _deployment = self.deployment.lock().unwrap_or_else(|e| e.into_inner());
            if _deployment.control.clustered() {
                return;
            }
            if self.closed.load(Ordering::Acquire) || !self.custom_protocol.load(Ordering::Acquire)
            {
                return;
            }
            let mut blockers = self
                .activation_blockers
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            blockers.retain(|id, gates| {
                gates.retain(|gate| !gate.drained());
                if !gates.is_empty() {
                    return true;
                }
                if let Some(route) = self
                    .routes
                    .read()
                    .unwrap_or_else(|e| e.into_inner())
                    .get(id)
                {
                    if route.record.definition.enabled {
                        // 全部旧许可已结束才开放新定义，编码变更不能与旧写出同时有效。
                        if route.gate.activate().is_ok() {
                            tracing::info!(event = "route_configuration", stage = "activated", route_id = %id,
                                revision = route.record.definition.revision, "route writers drained");
                        } else if route.gate.unavailable_reason() == Some("route_closed") {
                            // 已关闭门禁不能再次开放；保留明确阶段供运维区分排空与接流。
                            tracing::info!(event = "route_configuration", stage = "activation_rejected", route_id = %id,
                                revision = route.record.definition.revision,
                                reason = route.gate.unavailable_reason().unwrap_or("already_active"),
                                "route activation unavailable");
                        }
                    }
                }
                false
            });
        }
    }

    /// 业务作用：停机时先撤销全部路由准入，阻止迟到远程结果重新接流。
    /// 参数说明: 无。
    ///
    /// 返回：所有路由门禁永久关闭，协议任务的最终排空由 napp 完成。
    pub fn close(&self) {
        // 节点终止标志与全部门禁共享，不等待仍可能执行 fsync 的数据卷或配置锁。
        self.closed.store(true, Ordering::Release);
    }

    /// 业务作用：为自有产品协议开放已完成准备的初始定义，监听接流仍等待 napp Ready。
    /// 参数说明: 无。
    ///
    /// 返回：初始门禁已安装；此方法只能在启动装配阶段调用一次。
    pub fn prepare_protocol(&self) -> anyhow::Result<()> {
        let _deployment = self.deployment.lock().unwrap_or_else(|e| e.into_inner());
        anyhow::ensure!(
            !self.closed.load(Ordering::Acquire),
            "route service is closed"
        );
        anyhow::ensure!(
            !self.custom_protocol.swap(true, Ordering::AcqRel),
            "protocol already prepared"
        );
        if _deployment.control.clustered() {
            return Ok(());
        }
        for route in self
            .routes
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .values()
        {
            if route.record.definition.enabled {
                route.gate.activate()?;
            }
        }
        Ok(())
    }

    /// 业务作用：按实际接入类别查找业务定义，不能从未开启的适配器借用另一入口的资格。
    /// 参数说明：
    /// - `endpoint`：原生 AUTH endpoint 或 Socket.IO CONNECT namespace。
    /// - `socketio`：是否来自已确认的 Socket.IO 适配器。
    /// - `dedicated_route`：独立监听器绑定的 routeId，共享监听器为 None。
    /// - `listener_ref`：实际接入的监听模板，不能由客户端选择。
    ///
    /// 返回：同一冻结定义与准入门禁；入口关闭、路由不匹配或独立端口串线时返回 None。
    pub fn resolve(
        &self,
        endpoint: &str,
        socketio: bool,
        dedicated_route: Option<&str>,
        listener_ref: &str,
    ) -> Option<(
        Arc<crate::domain::route::compiled::CompiledRoute>,
        Arc<RouteGate>,
    )> {
        self.routes
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .values()
            .find(|route| {
                let definition = &route.record.definition;
                let port_matches = match dedicated_route {
                    Some(id) => {
                        definition.route_id == id && definition.access.dedicated_port.enabled
                    }
                    None => !definition.access.dedicated_port.enabled,
                };
                definition.access.listener_ref == listener_ref
                    && port_matches
                    && if socketio {
                        definition.access.socketio_namespace() == Some(endpoint)
                    } else {
                        definition.access.native_endpoint() == Some(endpoint)
                    }
            })
            .map(|route| (route.plan.clone(), route.gate.clone()))
    }

    /// 业务作用：为受信 MQ 和控制请求取得精确路由定义，不以接入路径猜测权限域。
    /// 参数说明：
    /// - `route_id`：来源合同中明确指定的稳定路由 ID。
    ///
    /// 返回：冻结计划和同代门禁；不存在时不选择默认路由。
    pub fn by_id(
        &self,
        route_id: &str,
    ) -> Option<(
        Arc<crate::domain::route::compiled::CompiledRoute>,
        Arc<RouteGate>,
    )> {
        self.routes
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(route_id)
            .map(|route| (route.plan.clone(), route.gate.clone()))
    }

    /// 业务作用：为监听准备和只读目录复制当前完整路由定义，运行事实不写回配置。
    /// 参数说明: 无。
    ///
    /// 返回：持久账本中所有路由的冻结定义，包含已下线记录。
    pub fn definitions(&self) -> Vec<crate::domain::route::RouteDefinition> {
        self.routes
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .values()
            .map(|route| route.record.definition.clone())
            .collect()
    }

    /// 业务作用：将已提交的集群定义安装为关闭快照，准备本地资源不能自行授予业务权限。
    /// 参数说明：
    /// - `definition`：持久权威冻结的完整路由、引用和摘要。
    ///
    /// 返回：新的关闭门禁；旧写出未结束、摘要不符或持久失败均拒绝准备。
    pub fn prepare_control(
        &self,
        definition: &crate::domain::control::Definition,
    ) -> anyhow::Result<Arc<RouteGate>> {
        let _edit = self.edits.lock().unwrap_or_else(|error| error.into_inner());
        let deployment = self.configuration();
        anyhow::ensure!(
            deployment.control.clustered() && !self.closed.load(Ordering::Acquire),
            "control_route_unavailable"
        );
        deployment.validate_publishers(&definition.route)?;
        let plan = crate::domain::route::compiled::CompiledRoute::compile_with_verifiers(
            definition.route.clone(),
            &definition.endpoints,
            &definition.verifiers,
        )?;
        anyhow::ensure!(
            plan.digest == definition.digest,
            "control_definition_digest"
        );
        let routes = self
            .routes
            .read()
            .unwrap_or_else(|error| error.into_inner());
        if let Some(old) = routes.get(&definition.route.route_id) {
            old.gate.close();
            // 新门禁的发布必须晚于原门禁的真实 writer 排空，不能通过替换 Arc 忽略责任。
            anyhow::ensure!(old.gate.drained(), "control_route_not_drained");
            anyhow::ensure!(
                definition.route.revision >= old.record.definition.revision,
                "control_revision_regression"
            );
            if definition.route.revision == old.record.definition.revision {
                anyhow::ensure!(
                    old.plan.digest == plan.digest,
                    "control_definition_conflict"
                );
            }
        }
        let record = RouteRecord {
            definition: definition.route.clone(),
            resolved_endpoints: definition.endpoints.clone(),
            resolved_verifiers: definition.verifiers.clone(),
        };
        let mut records = routes
            .iter()
            .map(|(id, route)| (id.clone(), route.record.clone()))
            .collect::<BTreeMap<_, _>>();
        records.insert(definition.route.route_id.clone(), record.clone());
        drop(routes);
        self.ledger.commit(&records)?;
        let gate = make_gate(
            &definition.route,
            &self.dependencies,
            &self.authorization_progress,
            &self.closed,
        )?;
        self.routes
            .write()
            .unwrap_or_else(|error| error.into_inner())
            .insert(
                definition.route.route_id.clone(),
                Arc::new(RouteState {
                    record,
                    gate: gate.clone(),
                    plan,
                }),
            );
        Ok(gate)
    }
}

/// 业务作用：在门禁发布前冻结必要入口引用，使新旧路由都受同一个依赖状态约束。
/// 参数说明：
/// - `definition`：已经编译的完整路由定义。
/// - `dependencies`：部署固定入口目录。
/// - `authorization_progress`：授权命令持久提交的独立进度门禁。
/// - `stopping`：所有路由共享的永久终止事实。
///
/// 返回：仍关闭的新门禁；启用推送却缺少入口时拒绝准备。
fn make_gate(
    definition: &crate::domain::route::RouteDefinition,
    dependencies: &BTreeMap<String, Arc<crate::domain::route::dependency::Dependency>>,
    authorization_progress: &BTreeMap<String, Arc<crate::domain::route::dependency::Dependency>>,
    stopping: &Arc<AtomicBool>,
) -> anyhow::Result<Arc<RouteGate>> {
    let gate = RouteGate::new_scoped(definition.revision, stopping.clone());
    if definition.authorization.control.enabled {
        let dependency = definition
            .authorization
            .control
            .ingress_ref
            .as_ref()
            .and_then(|id| dependencies.get(id))
            .ok_or_else(|| anyhow::anyhow!("authorization_ingress_unavailable"))?;
        gate.require(dependency.clone())?;
        let progress = definition
            .authorization
            .control
            .ingress_ref
            .as_ref()
            .and_then(|id| authorization_progress.get(id))
            .ok_or_else(|| anyhow::anyhow!("authorization_executor_unavailable"))?;
        gate.require(progress.clone())?;
    }
    if definition.service_push.enabled {
        let dependency = definition
            .service_push
            .ingress_ref
            .as_ref()
            .and_then(|id| dependencies.get(id))
            .ok_or_else(|| anyhow::anyhow!("route_ingress_unavailable"))?;
        gate.require(dependency.clone())?;
    }
    if definition.lifecycle.initialization.enabled {
        if let Some(reply) = &definition.lifecycle.initialization.reply {
            let dependency = dependencies
                .get(&reply.ingress_ref)
                .ok_or_else(|| anyhow::anyhow!("initialization_ingress_unavailable"))?;
            // 回复消费者有新鲜基线后才允许建连，不能先开放连接再从无游标入口等待授权。
            gate.require(dependency.clone())?;
        }
    }
    Ok(gate)
}
