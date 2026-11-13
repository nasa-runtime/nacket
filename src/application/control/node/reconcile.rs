use super::*;
use std::collections::BTreeSet;
use tokio::task::{Id, JoinSet};

type Prepared = (Binding, anyhow::Result<Arc<RouteGate>>);

/// 准备任务独立于续期循环，已创建的 listener 仍由受管 Listeners 持有关闭责任。
#[derive(Default)]
pub(super) struct Preparations {
    /// 实际仍在执行的准备任务集合，取消必须保留资源关闭责任。
    tasks: JoinSet<Prepared>,
    /// 任务 ID 到精确定义绑定的映射，完成结果不能应用到新定义。
    jobs: BTreeMap<Id, Binding>,
    /// 每条路由失败绑定及允许重试的单调时刻，配置变更需重新匹配。
    backoff: BTreeMap<String, (Binding, Instant)>,
}

impl NodeRuntime {
    /// 业务作用：优先维护已有路由的有限许可，再异步准备新资源，慢远程来源不能拖住其它路由续期。
    /// 参数说明: 无。
    ///
    /// 返回：本轮回执和续期完成；准备任务保持有界并由后续轮次收集真实结果。
    pub(super) async fn reconcile(&self) -> anyhow::Result<()> {
        self.expire();
        let evidence =
            LeaseTime::start(Duration::from_millis(self.authority.settings.permit_ttl_ms))?;
        let snapshot = match self.authority.snapshot().await {
            Ok(snapshot) => snapshot,
            Err(error) => {
                // 本轮未取得可复验快照，立即撤销控制健康证据；旧快照不能替代新的权威读取支撑业务准入。
                self.gateway.control_health.deny("control_unavailable");
                self.gateway.metrics.control.unavailable();
                return Err(error);
            }
        };
        {
            let mut sequence = self
                .last_sequence
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if snapshot.sequence < *sequence {
                // 权威倒退不能靠旧许可恢复服务，所有本地入口立即进入永久保护。
                self.protect();
                anyhow::bail!("control_baseline_regression");
            }
            *sequence = snapshot.sequence;
        }
        // 节点目录缺席不能按旧快照推断仍获授权；关闭本地路由，防止失去登记的实例继续接流。
        let Some(node) = snapshot.nodes.get(&self.gateway.configuration.node_id) else {
            self.protect();
            anyhow::bail!("node_missing");
        };
        // 失活或启动标识变化表示本实例已被撤回或替代，必须先关闭旧权限，避免旧进程继续冒用节点身份。
        if !node.active || node.startup_request != self.authority.startup {
            self.protect();
            anyhow::bail!("node_fenced");
        }
        // 快照序号和本启动身份都通过复验后才恢复控制健康，证据仍受原请求起点的有限期限约束。
        self.gateway.control_health.confirm(evidence);
        self.gateway.metrics.control.observe(
            &snapshot,
            Duration::from_millis(self.authority.settings.permit_ttl_ms),
        );
        let acknowledgments = if self.authority.settings.role != Role::Controller {
            self.reconcile_authorizations(&snapshot).await?
        } else {
            Vec::new()
        };
        let mut local = self.local.lock().await;
        let failed = self.collect_preparations(&snapshot, &mut local);
        let own = snapshot
            .permits
            .values()
            .filter(|permit| {
                permit.node_id == self.gateway.configuration.node_id
                    && permit.startup_request == self.authority.startup
            })
            .collect::<Vec<_>>();
        let mut remove = Vec::new();
        for (id, entry) in local.iter() {
            let desired = snapshot
                .routes
                .get(id)
                .is_some_and(|route| Binding::of(&route.definition) == entry.binding);
            if !desired || (entry.authorized && !entry.gate.authority_valid()) {
                // 定义已替换或本地权威已过期时先阻止旧路由继续写出，再等待既有许可排空。
                self.close_route(id);
                // 只有本地写出结束且权威目录不再保留本节点许可，才能丢弃用于归还许可的状态。
                if entry.gate.drained() && !own.iter().any(|permit| permit.binding.route_id == *id)
                {
                    remove.push(id.clone());
                }
            }
        }
        for id in remove {
            local.remove(&id);
        }
        let mut drain = Vec::new();
        for permit in &own {
            let usable = snapshot
                .routes
                .get(&permit.binding.route_id)
                .is_some_and(|route| {
                    route.definition.route.enabled
                        && Binding::of(&route.definition) == permit.binding
                })
                && local.get(&permit.binding.route_id).is_some_and(|entry| {
                    entry.binding == permit.binding
                        && entry.authorized
                        && entry.gate.authority_valid()
                });
            if !usable {
                // 先关闭不可用许可对应的数据面，避免上报排空后仍产生旧定义写出。
                self.close_route(&permit.binding.route_id);
                // Drained 允许权威推进定义切换，必须等实际写出排空，不能仅凭关闭门禁上报。
                if local
                    .get(&permit.binding.route_id)
                    .is_none_or(|entry| entry.gate.drained())
                {
                    drain.push((
                        permit.binding.route_id.clone(),
                        Command::Drained {
                            permit_id: permit.id.clone(),
                        },
                    ));
                }
            }
        }
        for chunk in drain.chunks(64) {
            let replies = self
                .batch(chunk.iter().map(|(_, command)| command.clone()).collect())
                .await?;
            for ((id, _), reply) in chunk.iter().zip(replies) {
                if matches!(reply, Reply::Recorded {}) {
                    // 只有持久回执确认许可已归还才删除本地责任；未知结果保留状态供后续重试。
                    local.remove(id);
                }
            }
        }
        if self.authority.settings.role == Role::Controller {
            return Ok(());
        }
        self.renew_all(&snapshot, &mut local, acknowledgments)
            .await?;
        let quota_commands = self
            .gateway
            .quotas
            .reconcile(&snapshot, &self.gateway.configuration.node_id);
        if !quota_commands.is_empty() {
            let replies = self.batch(quota_commands.clone()).await?;
            self.gateway.quotas.applied(&quota_commands, &replies);
        }
        let preparing = local
            .iter()
            .filter(|(id, entry)| {
                !entry.prepared
                    && snapshot.routes.get(*id).is_some_and(|route| {
                        route.definition.route.enabled
                            && Binding::of(&route.definition) == entry.binding
                            && snapshot.operations[&route.operation_id].phase != Phase::Quiescing
                    })
            })
            .map(|(id, entry)| {
                (
                    id.clone(),
                    Command::Prepared {
                        binding: entry.binding.clone(),
                    },
                )
            })
            .collect::<Vec<_>>();
        for chunk in preparing.chunks(64) {
            let replies = self
                .batch(chunk.iter().map(|(_, command)| command.clone()).collect())
                .await?;
            for ((id, _), reply) in chunk.iter().zip(replies) {
                if matches!(reply, Reply::Recorded {}) {
                    local.get_mut(id).expect("local preparation").prepared = true;
                }
            }
        }
        for chunk in failed.chunks(64) {
            self.batch(chunk.to_vec()).await?;
        }
        self.schedule_preparations(&snapshot, &local);
        Ok(())
    }

    /// 业务作用：把多个独立节点动作放入同一个持久提交边界，逐路由保留明确允许或拒绝结果。
    /// 参数说明：
    /// - `commands`：最多 64 个非嵌套节点动作，不包含管理员命令。
    ///
    /// 返回：与输入等长且保序的已提交回复，未知提交不能推测单项成功。
    async fn batch(&self, commands: Vec<Command>) -> anyhow::Result<Vec<Reply>> {
        let count = commands.len();
        let evidence =
            LeaseTime::start(Duration::from_millis(self.authority.settings.permit_ttl_ms))?;
        let Reply::Batch {
            replies,
        } = self
            .authority
            .node(Command::Batch {
                commands,
            })
            .await?
        else {
            anyhow::bail!("control_batch_invalid");
        };
        anyhow::ensure!(replies.len() == count, "control_batch_invalid");
        // 已提交回复连同当前快照构成新的控制证据，仍从发送时扣除整个往返耗时。
        self.gateway.control_health.confirm(evidence);
        Ok(replies)
    }

    /// 业务作用：在任何新资源准备之前分批续约现有路由，回复只影响其精确绑定并沿用请求起点。
    /// 参数说明：
    /// - `snapshot`：本轮已确认的完整控制状态。
    /// - `local`：已准备且尚未被替换的本地门禁。
    /// - `acknowledgments`：本轮已经完成本地撤权屏障、尚未持久确认的路由回执。
    ///
    /// 返回：合法许可已安装，Applied 只在本地门禁开放后提交；单项拒绝不扩大其它路由权限。
    async fn renew_all(
        &self,
        snapshot: &Ledger,
        local: &mut BTreeMap<String, Local>,
        acknowledgments: Vec<Command>,
    ) -> anyhow::Result<()> {
        let settings = &self.authority.settings;
        let mut acknowledgments = acknowledgments
            .into_iter()
            .map(|command| {
                let Command::AuthorizationApplied {
                    route_id,
                    ..
                } = &command
                else {
                    unreachable!();
                };
                (route_id.clone(), command)
            })
            .collect::<BTreeMap<_, _>>();
        let due = local
            .iter()
            .filter(|(id, entry)| {
                entry.prepared
                    && (entry.next_renew <= Instant::now() || acknowledgments.contains_key(*id))
                    && snapshot.routes.get(*id).is_some_and(|route| {
                        route.definition.route.enabled
                            && Binding::of(&route.definition) == entry.binding
                            && matches!(
                                snapshot.operations[&route.operation_id].phase,
                                Phase::Activating | Phase::Effective
                            )
                    })
            })
            .map(|(id, entry)| (id.clone(), entry.binding.clone()))
            .collect::<Vec<_>>();
        for chunk in due.chunks(32) {
            let started = LeaseTime::start(Duration::from_millis(
                settings.permit_ttl_ms - settings.clock_margin_ms,
            ))?;
            let renewals = chunk
                .iter()
                .map(|(_, binding)| {
                    let route = &snapshot.routes[&binding.route_id].definition.route;
                    Ok(Command::RenewAccess {
                        binding: binding.clone(),
                        nonce: nonce(),
                        listener: self.listeners.access(route)?,
                    })
                })
                .collect::<anyhow::Result<Vec<_>>>()?;
            let mut commands = Vec::new();
            let mut positions = Vec::new();
            for ((id, _), renewal) in chunk.iter().zip(&renewals) {
                if let Some(acknowledgment) = acknowledgments.remove(id) {
                    // 同一持久批次先确认撤权应用再申请许可，既保持权限顺序，也避免额外往返耗尽旧许可。
                    commands.push(acknowledgment);
                }
                positions.push(commands.len());
                commands.push(renewal.clone());
            }
            let replies = self.batch(commands).await?;
            for (((id, binding), command), position) in chunk.iter().zip(renewals).zip(positions) {
                let reply = replies[position].clone();
                let entry = local.get_mut(id).expect("renewable local entry");
                entry.next_renew = Instant::now() + Duration::from_millis(250);
                let Reply::Permit {
                    permit,
                } = reply
                else {
                    continue;
                };
                let Command::RenewAccess {
                    nonce,
                    ..
                } = command
                else {
                    unreachable!();
                };
                // 续权回复必须属于当前定义、本节点启动与本次请求，拒绝旧启动或其它请求的授权投影。
                anyhow::ensure!(
                    permit.binding == *binding
                        && permit.node_id == self.gateway.configuration.node_id
                        && permit.startup_request == self.authority.startup
                        && permit.incarnation
                            == snapshot.nodes[&self.gateway.configuration.node_id].incarnation
                        && permit.request_nonce == nonce
                        && permit.ttl_ms == settings.permit_ttl_ms,
                    "control_permit_mismatch"
                );
                // 沿用原请求起点扣除往返耗时，收到回复不能重新起算 TTL 而延长数据面的可写期限。
                if entry.gate.authorize(started).is_err() {
                    // 迟到回复不能复活过期门禁；后续轮次先排空并归还许可，再准备新的会话门禁。
                    self.close_route(id);
                    continue;
                }
                entry.authorized = true;
                entry.permit_id = Some(permit.id);
                entry.next_renew =
                    Instant::now() + Duration::from_millis(settings.renew_interval_ms);
            }
        }
        let remaining = acknowledgments.into_values().collect::<Vec<_>>();
        for chunk in remaining.chunks(64) {
            self.batch(chunk.to_vec()).await?;
        }
        // Applied 声明本地已经可服务，必须先成功开放并复验写出许可，再向权威持久上报。
        let applied = local
            .iter()
            .filter(|(_, entry)| {
                entry.authorized
                    && !entry.applied
                    && entry.permit_id.is_some()
                    && entry.gate.acquire(entry.binding.revision).is_some()
            })
            .map(|(id, entry)| {
                (
                    id.clone(),
                    Command::Applied {
                        binding: entry.binding.clone(),
                        permit_id: entry.permit_id.clone().expect("active permit"),
                    },
                )
            })
            .collect::<Vec<_>>();
        for chunk in applied.chunks(64) {
            let replies = self
                .batch(chunk.iter().map(|(_, command)| command.clone()).collect())
                .await?;
            for ((id, _), reply) in chunk.iter().zip(replies) {
                if matches!(reply, Reply::Recorded {}) {
                    // 持久回执才结束本地的应用上报责任，提交结果未知时仍需继续报告同一绑定。
                    local.get_mut(id).expect("applied local entry").applied = true;
                }
            }
        }
        Ok(())
    }

    /// 业务作用：收集异步准备的真实结束结果，迟到旧定义只保持关闭，不能安装为当前可申请资格。
    /// 参数说明：
    /// - `snapshot`：用于复验定义与全局阶段的已提交状态。
    /// - `local`：待更新的本地准备事实。
    ///
    /// 返回：需要上报权威的有界失败动作；已准备成功的门禁仍然关闭。
    fn collect_preparations(
        &self,
        snapshot: &Ledger,
        local: &mut BTreeMap<String, Local>,
    ) -> Vec<Command> {
        let mut pending = self
            .preparations
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let mut failures = Vec::new();
        while let Some(result) = pending.tasks.try_join_next_with_id() {
            let (binding, prepared) = match result {
                Ok((id, result)) => {
                    pending.jobs.remove(&id);
                    result
                }
                Err(error) => {
                    let Some(binding) = pending.jobs.remove(&error.id()) else {
                        continue;
                    };
                    (binding, Err(anyhow::anyhow!("preparation_interrupted")))
                }
            };
            let current = snapshot.routes.get(&binding.route_id).is_some_and(|route| {
                Binding::of(&route.definition) == binding
                    && snapshot.operations[&route.operation_id].phase != Phase::Quiescing
            });
            match prepared {
                Ok(gate) if current => {
                    pending.backoff.remove(&binding.route_id);
                    local.insert(
                        binding.route_id.clone(),
                        Local {
                            binding,
                            gate,
                            prepared: false,
                            applied: false,
                            authorized: false,
                            next_renew: Instant::now(),
                            permit_id: None,
                        },
                    );
                }
                Ok(gate) => gate.close(),
                Err(_) if current => {
                    pending.backoff.insert(
                        binding.route_id.clone(),
                        (binding.clone(), Instant::now() + Duration::from_secs(1)),
                    );
                    failures.push(Command::Failed {
                        binding,
                        reason: "local_preparation_failed".into(),
                    });
                }
                Err(_) => {}
            }
        }
        failures
    }

    /// 业务作用：启动有限的新资源准备任务，续期循环不等待远程公钥、端口绑定或磁盘提交。
    /// 参数说明：
    /// - `snapshot`：当前完整期望目录。
    /// - `local`：已经完成准备的本地路由事实。
    ///
    /// 返回：最多四个任务在途；失败路由采用有限退避，不占据所有准备机会。
    fn schedule_preparations(&self, snapshot: &Ledger, local: &BTreeMap<String, Local>) {
        let mut pending = self
            .preparations
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        pending.backoff.retain(|id, (binding, _)| {
            snapshot
                .routes
                .get(id)
                .is_some_and(|route| Binding::of(&route.definition) == *binding)
        });
        let running = pending
            .jobs
            .values()
            .map(|binding| binding.route_id.clone())
            .collect::<BTreeSet<_>>();
        // 并发准备的先后顺序不能决定端口归属；先从完整权威快照为指定端口保留自动分配边界。
        let fixed_ports = Arc::new(
            snapshot
                .routes
                .values()
                .filter(|route| {
                    route.definition.route.enabled
                        && route.definition.route.access.dedicated_port.enabled
                })
                .filter_map(|route| route.definition.route.access.dedicated_port.port)
                .collect::<BTreeSet<_>>(),
        );
        for (id, route) in &snapshot.routes {
            if pending.tasks.len() >= 4 {
                break;
            }
            let binding = Binding::of(&route.definition);
            if local.contains_key(id)
                || running.contains(id)
                || snapshot.operations[&route.operation_id].phase == Phase::Quiescing
                || pending
                    .backoff
                    .get(id)
                    .is_some_and(|(_, until)| *until > Instant::now())
            {
                continue;
            }
            let listeners = self.listeners.clone();
            let definition = route.definition.clone();
            let job_binding = binding.clone();
            let fixed_ports = fixed_ports.clone();
            let handle = pending.tasks.spawn(async move {
                let result = listeners.prepare_control(&definition, &fixed_ports).await;
                (job_binding, result)
            });
            pending.jobs.insert(handle.id(), binding);
        }
    }

    /// 业务作用：停机或失权时停止所有未完成的准备，阻止迟到任务继续创建路由资源。
    /// 参数说明: 无。
    ///
    /// 返回：任务取消已触发；真实 listener 关闭仍由受管聚合 owner 完成。
    pub(super) fn cancel_preparations(&self) {
        self.preparations
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .tasks
            .abort_all();
    }
}
