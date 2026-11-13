mod authorization;
mod reconcile;
use super::Authority;
use crate::{
    application::{gateway::Gateway, listeners::Listeners},
    config::{control::Role, Deployment},
    domain::{
        control::{clock::LeaseTime, Binding, Command, Definition, Ledger, Phase, Reply},
        message::control::nonce,
        route::{compiled::CompiledRoute, RouteGate},
    },
};
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, sync::Arc, time::Duration};
use tokio::time::Instant;

struct Local {
    /// 当前本地准备对应的精确控制定义绑定，不能跨 revision 复用许可。
    binding: Binding,
    /// 该绑定的数据面门禁，只有权威许可与依赖同时有效才开放。
    gate: Arc<RouteGate>,
    /// 本地资源已经准备的事实，尚不代表权威已接受应用回执。
    prepared: bool,
    /// 权威已确认本节点应用该定义的事实，仍需取得接流许可。
    applied: bool,
    /// 本轮是否已获得并安装有效控制许可，失权即撤销本地准入。
    authorized: bool,
    /// 下一次主动续期的单调时刻，不延长现有许可截止点。
    next_renew: Instant,
    /// 已安装的精确路由许可标识，用于区分迟到回执和当前资格。
    permit_id: Option<String>,
}

/// 节点投影只保存本地资源事实；route revision、incarnation 和全局完成结果以持久权威为准。
pub struct NodeRuntime {
    /// 持久控制请求及快照入口，节点本地候选不能替代它。
    pub authority: Arc<Authority>,
    /// 当前节点会话和授权运行域，负责实际关闭与写出排空。
    gateway: Arc<Gateway>,
    /// 实际监听准备及绑定目录，准备成功才提交应用事实。
    listeners: Arc<Listeners>,
    /// 每条路由当前精确绑定的本地推进状态。
    local: tokio::sync::Mutex<BTreeMap<String, Local>>,
    /// 本节点最近处理的控制账本序号，旧观察不能覆盖已处理状态。
    last_sequence: std::sync::Mutex<u64>,
    /// 有界异步路由准备任务及原绑定，失败退避不阻塞其它路由续期。
    preparations: std::sync::Mutex<reconcile::Preparations>,
    /// 最近完成处理的清单指纹，同一配置不反复生成提案。
    provisioning: tokio::sync::Mutex<Option<String>>,
    /// 已安装授权下界及尚未排空的旧写出屏障，全部完成才提交回执。
    authorizations: std::sync::Mutex<BTreeMap<String, authorization::Projection>>,
}

impl NodeRuntime {
    /// 业务作用：连接持久控制来源与本节点监听资源，登记 Ready 后的有限许可维护任务。
    /// 参数说明：
    /// - `application`：统一运行和停机责任宿主。
    /// - `authority`：已装配且来源验证固定的控制入口。
    /// - `gateway`：本地会话与最终写出运行域。
    /// - `listeners`：真实监听资源的唯一 owner。
    ///
    /// 返回：可交给启动 initializer 的节点协调器。
    pub async fn install(
        application: &nasa::Application,
        authority: Arc<Authority>,
        gateway: Arc<Gateway>,
        listeners: Arc<Listeners>,
    ) -> anyhow::Result<Arc<Self>> {
        let runtime = Arc::new(Self {
            authority,
            gateway,
            listeners,
            local: tokio::sync::Mutex::new(BTreeMap::new()),
            last_sequence: std::sync::Mutex::new(0),
            preparations: std::sync::Mutex::new(reconcile::Preparations::default()),
            provisioning: tokio::sync::Mutex::new(None),
            authorizations: std::sync::Mutex::new(BTreeMap::new()),
        });
        let actor = runtime.clone();
        application
            .serve_when_ready("socket-control-node", move |shutdown| async move {
                loop {
                    tokio::select! {
                        biased;
                        _ = shutdown.cancelled() => {
                            actor.protect();
                            return Ok(());
                        }
                        result = actor.reconcile() => {
                            if let Err(error) = result {
                                actor.expire();
                                if matches!(
                                    error.to_string().as_str(),
                                    "node_fenced" | "node_missing" | "control_baseline_regression"
                                ) {
                                    // 失去持久节点身份后不能继续占有业务 consumer assignment；退出交给宿主排空和释放。
                                    return Err(error);
                                }
                            }
                        }
                    }
                    tokio::select! {
                        _ = shutdown.cancelled() => {},
                        _ = tokio::time::sleep(Duration::from_millis(50)) => {},
                    }
                }
            })
            .await?;
        let provisioner = runtime.clone();
        application
            .serve_when_ready("socket-route-provisioning", move |shutdown| async move {
                let mut timer = tokio::time::interval(Duration::from_secs(2));
                timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                loop {
                    tokio::select! {
                        biased;
                        _ = shutdown.cancelled() => return Ok(()),
                        _ = timer.tick() => {},
                    }
                    let candidate = provisioner.gateway.routes.configuration();
                    tokio::select! {
                        biased;
                        _ = shutdown.cancelled() => return Ok(()),
                        result = provisioner.provision(&candidate) => {
                            if result.is_err() {
                                tracing::warn!(
                                    reason = "manifest_submission_unconfirmed",
                                    "route manifest remains pending"
                                );
                            }
                        }
                    }
                }
            })
            .await?;
        Ok(runtime)
    }

    /// 业务作用：启动时取得持久节点代际；清单提交交给独立任务，不能用配置绕开现存权威。
    /// 参数说明: 无。
    ///
    /// 返回：当前启动申请取得唯一 incarnation；控制不可用时阻止接流。
    pub async fn initialize(&self) -> anyhow::Result<()> {
        let registration = if self.authority.settings.role == Role::Controller {
            Command::RegisterController {}
        } else {
            Command::Register {}
        };
        let incarnation = loop {
            match self.authority.node(registration.clone()).await {
                Ok(Reply::Registered {
                    incarnation,
                }) => break incarnation,
                Ok(_) => anyhow::bail!("control_registration_invalid"),
                Err(error) if error.to_string() == "control_operation_unknown" => {
                    // 启动申请身份不变，未知结果重试由权威查回同一代际；外层仍受全应用启动期限约束。
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                Err(error) => return Err(error),
            }
        };
        let snapshot = self.authority.snapshot().await?;
        let node = snapshot
            .nodes
            .get(&self.gateway.configuration.node_id)
            .ok_or_else(|| anyhow::anyhow!("node_not_registered"))?;
        anyhow::ensure!(
            node.startup_request == self.authority.startup
                && node.incarnation.to_string() == incarnation
                && node.active,
            "node_fenced"
        );
        self.gateway
            .incarnation
            .set(incarnation)
            .map_err(|_| anyhow::anyhow!("node_already_registered"))?;
        Ok(())
    }

    /// 业务作用：把完整 YAML 中的显式新定义交给同一受权控制路径，不在本节点直接应用。
    /// 参数说明：
    /// - `candidate`：同一 naml 快照中的完整产品候选。
    ///
    /// 返回：清单进度已持久记录；被拒条目保留明确结果，未确认提交留给相同清单重试。
    pub async fn provision(&self, candidate: &Deployment) -> anyhow::Result<()> {
        candidate.validate()?;
        anyhow::ensure!(
            candidate.control == self.gateway.configuration.control,
            "control changes require restart"
        );
        let mut completed = self.provisioning.lock().await;
        let configuration = &candidate.route_provisioning;
        if !configuration.enabled {
            *completed = None;
            return Ok(());
        }
        let fingerprint = format!(
            "{:x}",
            Sha256::digest(serde_json::to_vec(&(
                configuration,
                &candidate.routes,
                &candidate.endpoints,
                &candidate.verifiers
            ))?)
        );
        if completed.as_ref() == Some(&fingerprint) {
            return Ok(());
        }
        let key = self
            .authority
            .settings
            .provisioner_key_id
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("route_provisioner_required"))?;
        let source = configuration
            .source_id
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("manifest_source_required"))?;
        let generation = configuration
            .generation
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("manifest_generation_required"))?;
        let snapshot = self.authority.snapshot().await?;
        // 旧配置仅影响本地观察，不能把已提交的新代次回退；重启仍按权威投影恢复路由。
        if snapshot
            .manifests
            .get(source)
            .and_then(|history| history.last())
            .is_some_and(|last| {
                last.generation.parse::<u64>().ok() > generation.parse::<u64>().ok()
            })
        {
            *completed = Some(fingerprint);
            return Ok(());
        }
        let mut definitions = BTreeMap::new();
        let mut entries = Vec::new();
        for entry in &configuration.entries {
            let route = candidate
                .routes
                .iter()
                .find(|route| route.route_id == entry.route_id)
                .ok_or_else(|| anyhow::anyhow!("manifest_definition_missing"))?;
            let plan = CompiledRoute::compile_with_verifiers(
                route.clone(),
                &candidate.endpoints,
                &candidate.verifiers,
            )?;
            let definition = Definition {
                route: route.clone(),
                endpoints: plan.endpoints.clone(),
                verifiers: plan.verifiers.clone(),
                digest: plan.digest.clone(),
            };
            entries.push(crate::domain::control::ManifestProposal {
                binding: Binding::of(&definition),
                expected_revision: entry.expected_revision.parse()?,
                enabled: route.enabled,
            });
            definitions.insert(route.route_id.clone(), definition);
        }
        let Reply::Manifest {
            mut manifest,
        } = self
            .authority
            .execute(
                key,
                Command::Manifest {
                    source_id: source.clone(),
                    generation: generation.clone(),
                    entries,
                },
            )
            .await?
        else {
            anyhow::bail!("manifest_reply_invalid");
        };
        // 完整清单先受理，再按持久 Pending 进度提交定义；中途退出后的同代重试不分配第二个操作 ID。
        let pending = manifest
            .entries
            .iter()
            .filter(|(_, entry)| {
                matches!(
                    entry.result,
                    crate::domain::control::ManifestResult::Pending { .. }
                )
            })
            .map(|(id, _)| id.clone())
            .collect::<Vec<_>>();
        for id in pending {
            let definition = definitions
                .remove(&id)
                .ok_or_else(|| anyhow::anyhow!("manifest_definition_missing"))?;
            let reply = self
                .authority
                .execute(
                    key,
                    Command::ApplyManifest {
                        source_id: source.clone(),
                        generation: generation.clone(),
                        minimum_ready: definition
                            .route
                            .minimum_ready(self.authority.settings.minimum_ready_nodes),
                        definition,
                    },
                )
                .await?;
            let Reply::Manifest {
                manifest: next,
            } = reply
            else {
                anyhow::bail!("manifest_reply_invalid");
            };
            manifest = next;
        }
        if manifest.entries.values().any(|entry| {
            matches!(
                entry.result,
                crate::domain::control::ManifestResult::Rejected { .. }
            )
        }) {
            tracing::warn!(
                reason = "manifest_entries_rejected",
                "route manifest contains rejected entries"
            );
        }
        *completed = Some(fingerprint);
        Ok(())
    }

    /// 业务作用：先撤销指定路由准入再取消其会话，迟到远程结果不能恢复旧权限。
    /// 参数说明：
    /// - `route_id`：已经从权威或本地投影取得的稳定路由标识。
    ///
    /// 返回：门禁关闭且连接取消已触发，真实 writer 排空仍需单独确认。
    fn close_route(&self, route_id: &str) {
        if let Some((_, gate)) = self.gateway.routes.by_id(route_id) {
            gate.close();
        }
        let sessions = self
            .gateway
            .sessions
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .values()
            .filter(|connection| connection.session.key.route_id == route_id)
            .map(|connection| connection.session.key.route_session_id.clone())
            .collect::<Vec<_>>();
        for id in sessions {
            self.gateway.disconnect(&id);
        }
    }

    /// 业务作用：主动移除已经失去最终准入资格的会话，安全判断仍由每个业务门禁同步执行。
    /// 参数说明: 无。
    ///
    /// 返回：到期会话的关闭责任已触发，不以维护任务运行频率延长控制许可。
    fn expire(&self) {
        let sessions = self
            .gateway
            .sessions
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .values()
            .filter(|connection| {
                if connection.session.live() {
                    return false;
                }
                tracing::debug!(
                    reason = connection
                        .session
                        .unavailable_reason()
                        .unwrap_or("session_unavailable"),
                    "socket session eligibility ended"
                );
                true
            })
            .map(|connection| connection.session.key.route_session_id.clone())
            .collect::<Vec<_>>();
        for id in sessions {
            self.gateway.disconnect(&id);
        }
    }

    /// 业务作用：失去节点身份或停机时立即撤销全部本地路由权限。
    /// 参数说明: 无。
    ///
    /// 返回：当前所有路由关闭，之后只有新的受信节点启动流程能够接流。
    fn protect(&self) {
        self.gateway.control_health.deny("control_protected");
        self.gateway.metrics.control.unavailable();
        self.cancel_preparations();
        self.gateway.routes.close();
        for route in self.gateway.routes.definitions() {
            self.close_route(&route.route_id);
        }
    }
}
