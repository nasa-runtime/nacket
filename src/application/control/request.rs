use crate::{
    adapters::control::redis::{Snapshot, Store as RedisStore},
    config::{
        control::{Control, Principal as KeyPrincipal, Role, Settings},
        Deployment,
    },
    domain::{
        control::{expiry::Observations, Command, Ledger, Limits, Principal, Reply, Transition},
        message::{control::nonce, json, UnixMillis},
    },
};
use serde::{Deserialize, Serialize};

mod backend;
mod kafka;
mod progress;
use backend::Backend;
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Duration,
};

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Request {
    /// 固定控制请求合同标识，解析后必须精确匹配支持的合同。
    contract: String,
    /// 签名请求绑定的控制集群，防止跨集群重放。
    cluster_id: String,
    /// 请求绑定的控制作用域代际，旧代请求不能进入新账本。
    scope_epoch: String,
    /// 部署密钥目录中的签名身份引用，不是密钥内容。
    key_id: String,
    /// 本次节点启动请求标识，将命令绑定到准确启动实例。
    startup_request: String,
    /// 单次控制请求的关联随机标识，重试等待沿用其提交身份。
    nonce: String,
    /// 首次创建请求的 Unix 毫秒整数，签名后固定。
    created_at: UnixMillis,
    /// 该请求可被受理的 Unix 毫秒截止点，重试不延长有效期。
    expires_at: UnixMillis,
    /// 已完成入口权限收窄的控制动作，权威仍独立验证主体权限。
    command: Command,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Signed {
    /// 完整签名覆盖的控制请求，不能仅签正文而放过作用域字段。
    request: Request,
    /// 固定算法产生的请求签名，验证成功仍不等于动作已提交。
    signature: String,
}

/// 客户端只签署固定来源，controller 串行处理持久输入；节点永远不直接提交本地候选。
pub struct Authority {
    /// 启动时冻结的控制身份、许可和请求期限策略。
    pub settings: Settings,
    /// 当前进程启动请求的唯一标识，重启不能冒用旧 incarnation。
    pub startup: String,
    /// 统一控制后端，只接受已确认持久状态作为权威。
    store: Backend,
    /// 本控制运行域的固定集群身份。
    cluster_id: String,
    /// 本控制运行域的固定作用域代际。
    scope_epoch: String,
    /// 控制策略摘要，节点与权威必须确认同一安全规则。
    policy_digest: String,
    /// 本部署是否获准首次创建空控制基线，不允许覆盖已有高水位。
    initialize: bool,
    /// 本进程角色允许使用的签名材料，不能输出到日志或公开响应。
    keys: BTreeMap<String, String>,
    /// 当前控制器实例唯一身份，用于取得及核对处理权。
    controller_id: String,
    /// 当前 Redis 控制处理租约令牌，失效后不得提交旧候选。
    owner: tokio::sync::Mutex<Option<String>>,
    /// 本地连续时间观察，用于证明旧许可真实失效。
    observations: Mutex<Observations>,
    /// 等待持久控制回复的节点并发上限，避免未知结果无限积压。
    requests: tokio::sync::Semaphore,
    /// 持有数据卷高水位和停止门禁的路由运行域。
    routes: Arc<crate::application::routes::RouteService>,
    /// 快照与回复读取的顺序责任，延续到高水位实际落盘完成。
    reads: Arc<tokio::sync::Mutex<()>>,
    /// 固定控制请求和容量指标源，不用请求 ID 作为标签。
    metrics: Arc<crate::observability::Metrics>,
}

impl Authority {
    /// 业务作用：借用 napp 的控制后端并登记受监督的控制处理任务，不新建 Redis 生命周期。
    /// 参数说明：
    /// - `application`：统一组件宿主。
    /// - `deployment`：启动时冻结的来源、作用域和身份声明。
    /// - `routes`：持有本节点数据卷高水位与业务关闭责任的运行域。
    /// - `metrics`：节点的固定产品指标源。
    ///
    /// 返回：启用集群时返回控制入口，本地模式不创建控制资源。
    pub async fn install(
        application: &nasa::Application,
        deployment: &Deployment,
        routes: Arc<crate::application::routes::RouteService>,
        metrics: Arc<crate::observability::Metrics>,
    ) -> anyhow::Result<Option<Arc<Self>>> {
        let Some(settings) = deployment.control.settings() else {
            return Ok(None);
        };
        metrics.control.enable();
        let (store, initialize) = match &deployment.control {
            Control::Redis {
                source_ref,
                namespace,
                initialize,
                ..
            } => (
                Backend::Redis(RedisStore::new(
                    application.redis(source_ref).await?,
                    namespace,
                    deployment.cluster_id.clone(),
                    deployment.scope_epoch.clone(),
                )?),
                *initialize,
            ),
            Control::Kafka {
                initialize,
                ..
            } => (
                Backend::Kafka(
                    crate::adapters::control::kafka::Store::open(application, deployment).await?,
                ),
                *initialize,
            ),
            Control::Local => unreachable!(),
        };
        let mut keys = BTreeMap::new();
        for (id, key) in &settings.keys {
            if settings.role == Role::Socket
                && id != &settings.node_key_id
                && settings.provisioner_key_id.as_ref() != Some(id)
            {
                continue;
            }
            let secret = std::env::var(&key.secret_ref)
                .map_err(|_| anyhow::anyhow!("control_secret_missing"))?;
            anyhow::ensure!((32..=4096).contains(&secret.len()), "control_secret_length");
            keys.insert(id.clone(), secret);
        }
        let authority = Arc::new(Self {
            settings: settings.clone(),
            startup: nonce(),
            store,
            cluster_id: deployment.cluster_id.clone(),
            scope_epoch: deployment.scope_epoch.clone(),
            policy_digest: policy_digest(settings)?,
            initialize,
            keys,
            controller_id: nonce(),
            owner: tokio::sync::Mutex::new(None),
            observations: Mutex::new(Observations::default()),
            requests: tokio::sync::Semaphore::new(128),
            routes,
            reads: Arc::new(tokio::sync::Mutex::new(())),
            metrics,
        });
        if settings.role != Role::Socket && matches!(&authority.store, Backend::Redis(_)) {
            let controller = authority.clone();
            application.spawn_critical("socket-control-authority", move |shutdown| async move {
                let mut failures = 0u32;
                let mut last_warning = None::<tokio::time::Instant>;
                loop {
                    tokio::select! {
                        biased;
                        _ = shutdown.cancelled() => {
                            controller.metrics.control.owner_unavailable();
                            if let Some(owner) = controller.owner.lock().await.take() {
                                let _ = tokio::time::timeout(Duration::from_millis(500), controller.store.release(&owner))
                                    .await;
                            }
                            return Ok(());
                        }
                        (result, progress) = async {
                            let mut progress = progress::Progress::new();
                            let result = tokio::time::timeout(
                                Duration::from_millis(controller.settings.request_timeout_ms),
                                controller.pump(&mut progress),
                            ).await;
                            (result, progress)
                        } => {
                            if !matches!(result, Ok(Ok(()))) {
                                // 控制提交未确认时不发布候选；节点会在各自有限许可到期后同步拒绝写出。
                                controller.metrics.control.owner_unavailable();
                                failures = failures.saturating_add(1).min(7);
                                if last_warning.is_none_or(|last| last.elapsed() >= Duration::from_secs(30)) {
                                    tracing::warn!(
                                        reason = "control_unavailable",
                                        node_id = %controller.routes.configuration().node_id,
                                        stage = progress.stage,
                                        read_step = ?progress.read_step,
                                        owner_term = ?progress.term,
                                        cursor = %progress.cursor,
                                        request_id = %progress.request_id,
                                        command_id = ?progress.command_id,
                                        route_id = ?progress.route_id,
                                        timed_out = result.is_err(),
                                        timeout_ms = controller.settings.request_timeout_ms,
                                        "socket control authority remains protected"
                                    );
                                    last_warning = Some(tokio::time::Instant::now());
                                }
                            } else {
                                failures = 0;
                            }
                        }
                    }
                    // 故障时限制依赖重试和日志压力，退避不延长已签发许可的任何期限。
                    let delay = Duration::from_millis((25u64 << failures).min(2000));
                    tokio::select! {
                        _ = shutdown.cancelled() => {},
                        _ = tokio::time::sleep(delay) => {},
                    }
                }
            }).await?;
        }
        if matches!(&authority.store, Backend::Kafka(_)) {
            kafka::install(application, &authority).await?;
        }
        Ok(Some(authority))
    }

    /// 业务作用：签署本节点或已受权管理员命令并等待持久回复，超时不撤销已经提交的动作。
    /// 参数说明：
    /// - `key_id`：部署固定且已经通过入口授权的签名身份。
    /// - `command`：完成业务入口校验的控制命令。
    ///
    /// 返回：已确认控制回复；失败返回 unknown，调用方保留原 operationId 查询。
    pub async fn execute(&self, key_id: &str, command: Command) -> anyhow::Result<Reply> {
        let subjects: Vec<_> = match &command {
            Command::Batch {
                commands,
            } => commands.iter().map(control_subject).collect(),
            other => vec![control_subject(other)],
        };
        let mut observations: Vec<_> = subjects
            .into_iter()
            .map(|subject| {
                crate::observability::Operation::new(
                    self.metrics.clone(),
                    crate::observability::Counter::Control,
                    subject,
                    "control_request",
                )
            })
            .collect();
        let result: anyhow::Result<Reply> = async {
            let _capacity = self
                .requests
                .try_acquire()
                .map_err(|_| anyhow::anyhow!("control_request_capacity"))?;
            let _observed_capacity = self.metrics.reserve("control_pending", 1);
            let secret = self
                .keys
                .get(key_id)
                .ok_or_else(|| anyhow::anyhow!("control_key_missing"))?;
            let created = now()?;
            let expires = UnixMillis::new(created.as_millis() + self.settings.request_timeout_ms)
                .map_err(anyhow::Error::msg)?;
            let request = Request {
                contract: "nacket-control/1".into(),
                cluster_id: self.cluster_id.clone(),
                scope_epoch: self.scope_epoch.clone(),
                key_id: key_id.into(),
                startup_request: self.startup.clone(),
                nonce: nonce(),
                created_at: created,
                expires_at: expires,
                command,
            };
            let bytes = serde_json::to_vec(&request)?;
            let id = format!("{:x}", Sha256::digest(&bytes));
            let signature = nasa::crypto::hmac_sha256(std::str::from_utf8(&bytes)?, secret);
            let signed = serde_json::to_vec(&Signed {
                request,
                signature,
            })?;
            tokio::time::timeout(
                Duration::from_millis(self.settings.request_timeout_ms),
                async {
                    let mut publication = crate::observability::Operation::new(
                        self.metrics.clone(),
                        crate::observability::Counter::Producer,
                        "control",
                        "broker_commit",
                    );
                    // 发送开始后取消不能证明 broker 未接受；只有明确回执才记 accepted。
                    publication.finish("unknown");
                    self.store.submit(&signed).await?;
                    publication.finish("accepted");
                    drop(publication);
                    loop {
                        if let Some(reply) = self.reply_checked(&id).await? {
                            if let Reply::Rejected {
                                code,
                            } = reply
                            {
                                anyhow::bail!("{code}");
                            }
                            return Ok(reply);
                        }
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                },
            )
            .await
            .map_err(|_| anyhow::anyhow!("control_operation_unknown"))?
        }
        .await;
        match &result {
            Ok(Reply::Batch {
                replies,
            }) if replies.len() == observations.len() => {
                for (observation, reply) in observations.iter_mut().zip(replies) {
                    observation.finish(if matches!(reply, Reply::Rejected { .. }) {
                        "rejected"
                    } else {
                        "committed"
                    });
                }
            }
            _ => {
                let outcome = result
                    .as_ref()
                    .map(|_| "committed")
                    .unwrap_or_else(|error| crate::observability::outcome(error));
                for observation in &mut observations {
                    observation.finish(outcome);
                }
            }
        }
        result
    }

    /// 业务作用：以固定节点身份提交回执和续期，调用者不能在命令体中声明管理员权限。
    /// 参数说明：
    /// - `command`：节点类别的固定控制命令。
    ///
    /// 返回：控制提交结果，未登记或旧 incarnation 由权威拒绝。
    pub async fn node(&self, command: Command) -> anyhow::Result<Reply> {
        self.execute(&self.settings.node_key_id, command).await
    }

    /// 业务作用：读取持久权威的完整已提交状态，恢复时不使用本地 YAML 推断当前路由。
    /// 参数说明: 无。
    ///
    /// 返回：与日志检查点一致的账本；来源不可用或部分持久状态均拒绝。
    pub async fn snapshot(&self) -> anyhow::Result<Ledger> {
        self.metrics
            .measure(
                crate::observability::Counter::Control,
                "restore",
                "control_restore",
                "ok",
                async {
                    let ledger = self.read_checked(None).await?.ledger;
                    anyhow::ensure!(
                        ledger.policy_digest == self.policy_digest,
                        "control_policy_mismatch"
                    );
                    Ok(ledger)
                },
            )
            .await
    }

    /// 业务作用：串行读取已提交快照并推进本节点数据卷高水位，避免并发读取的完成顺序造成假回退。
    /// 参数说明：
    /// - `progress`：可选的本轮诊断上下文，只记录固定读取子步骤，不参与控制决策。
    ///
    /// 返回：后端检查点和本地持久观察均确认的快照，并清空读取子步骤；失败或取消保留最后进入的子步骤。
    /// 磁盘等待不占据异步执行线程，诊断上下文不改变落盘任务持有串行权的责任。
    async fn read_checked(
        &self,
        mut progress: Option<&mut progress::Progress>,
    ) -> anyhow::Result<Snapshot> {
        if let Some(progress) = progress.as_mut() {
            progress.read_step = Some("serial_lock");
        }
        let serial = self.reads.clone().lock_owned().await;
        if let Some(progress) = progress.as_mut() {
            progress.read_step = Some("backend_snapshot");
        }
        let snapshot = self.store.read().await?;
        if let Some(progress) = progress.as_mut() {
            progress.read_step = Some("persist_checkpoint");
        }
        self.observe_checkpoint(snapshot.ledger.sequence, snapshot.hash.clone(), serial)
            .await?;
        if let Some(progress) = progress {
            progress.read_step = None;
        }
        Ok(snapshot)
    }

    /// 业务作用：串行确认控制回复和同一已提交基线，回复用于业务前先持久推进本节点高水位。
    /// 参数说明：
    /// - `id`：原始签名请求的唯一关联摘要。
    ///
    /// 返回：高水位确认后的回复；未知输入或失败不能授予业务资格。
    async fn reply_checked(&self, id: &str) -> anyhow::Result<Option<Reply>> {
        let serial = self.reads.clone().lock_owned().await;
        let Some((reply, snapshot)) = self.store.reply_snapshot(id).await? else {
            return Ok(None);
        };
        self.observe_checkpoint(snapshot.ledger.sequence, snapshot.hash, serial)
            .await?;
        Ok(Some(reply))
    }

    /// 业务作用：把受读取串行锁保护的控制观察落盘，实际阻塞任务持有数据卷责任直到结束。
    /// 参数说明：
    /// - `sequence`：已提交快照序号。
    /// - `digest`：该序号完整账本的固定摘要。
    /// - `serial`：当前读取顺序责任，移交给实际落盘任务持有。
    ///
    /// 返回：数据卷确认成功；取消等待不撤回已经发生的持久副作用。
    async fn observe_checkpoint(
        &self,
        sequence: u64,
        digest: String,
        serial: tokio::sync::OwnedMutexGuard<()>,
    ) -> anyhow::Result<()> {
        let routes = self.routes.clone();
        self.routes
            .persistence
            .run(move || {
                // 等待者取消后仍持有串行权，防止未完成的旧落盘与下一次观察交叉发布。
                let _serial = serial;
                routes.observe_control(sequence, &digest)
            })
            .await?;
        Ok(())
    }

    /// 业务作用：持有真实 Redis fencing 后按输入日志顺序提交命令和回复。
    /// 参数说明：
    /// - `progress`：本轮阶段及安全关联，超时取消后由调用方记录。
    ///
    /// 返回：本轮已处理有界输入；owner、CAS 或日志不一致时停止本轮而不猜测提交结果。
    async fn pump(&self, progress: &mut progress::Progress) -> anyhow::Result<()> {
        let Backend::Redis(store) = &self.store else {
            return Ok(());
        };
        let mut owner = self.owner.lock().await;
        let acquired_at = std::time::Instant::now();
        progress.stage = "owner_acquire";
        let acquired = store
            .acquire(
                owner.as_deref(),
                &self.controller_id,
                5000,
                self.initialize && self.routes.control_is_fresh(),
            )
            .await?;
        let Some(token) = acquired else {
            // 备用控制器的完整快照只能作为节点观察，未取得 owner 不能发布权威目录。
            self.metrics.control.owner_unavailable();
            *owner = None;
            return Ok(());
        };
        if owner.as_ref() != Some(&token) {
            // 新控制实例恢复完整许可账本后重新观察，不继承内存中的过期判断。
            *self
                .observations
                .lock()
                .unwrap_or_else(|error| error.into_inner()) = Observations::default();
        }
        *owner = Some(token.clone());
        progress.term = token.split(':').next().and_then(|value| value.parse().ok());
        progress.stage = "snapshot_read_and_checkpoint";
        let mut base = self.read_checked(Some(progress)).await?;
        if base.ledger.policy_digest.is_empty() {
            anyhow::ensure!(
                base.ledger.sequence == 0
                    && base.ledger.nodes.is_empty()
                    && base.ledger.routes.is_empty(),
                "control_policy_baseline_missing"
            );
            let mut transition = self.unchanged(&base, Reply::Recorded {})?;
            transition.ledger.policy_digest = self.policy_digest.clone();
            progress.stage = "policy_commit";
            // 权限来源与许可时钟合同先于任何节点登记持久固定，切换 controller 不能扩大授权范围。
            store
                .commit(&token, &base, transition, &base.cursor, &nonce(), 1000)
                .await?;
            progress.stage = "policy_read_and_checkpoint";
            base = self.read_checked(Some(progress)).await?;
        }
        anyhow::ensure!(
            base.ledger.policy_digest == self.policy_digest,
            "control_policy_mismatch"
        );
        progress.stage = "commands_read";
        progress.cursor.clone_from(&base.cursor);
        for entry in store.commands(&base.cursor).await? {
            progress.stage = "command_validate";
            progress.cursor.clone_from(&entry.id);
            progress.command_id = None;
            progress.route_id = None;
            progress.request_id.clear();
            let raw = if entry.fields.len() == 1 && entry.fields[0].event == "request" {
                entry.fields[0].raw.as_slice()
            } else {
                &[]
            };
            let parsed = json::decode_bounded::<Signed>(raw, 262144, 32, 4096).ok();
            let authorized = parsed.as_ref().and_then(|signed| self.verify(signed).ok());
            let (request_id, transition) =
                if let (Some(signed), Some(principal)) = (parsed, authorized) {
                    let request_id =
                        format!("{:x}", Sha256::digest(serde_json::to_vec(&signed.request)?));
                    progress.request_id.clone_from(&request_id);
                    progress.correlate(&signed.request.command);
                    progress.stage = "reply_read";
                    let transition = if let Some(reply) = store.reply(&request_id).await? {
                        self.unchanged(&base, reply)?
                    } else {
                        progress.stage = "transition";
                        match base.ledger.transition(
                            &principal,
                            &signed.request.command,
                            now()?,
                            &self.limits(),
                        ) {
                            Ok(transition) => transition,
                            Err(error) => self.unchanged(
                                &base,
                                Reply::Rejected {
                                    code: error.to_string(),
                                },
                            )?,
                        }
                    };
                    (request_id, transition)
                } else {
                    // 未验证请求只能记入自己的输入位置，不能污染已认证请求的结果键。
                    (
                        format!("invalid-{}", entry.id),
                        self.unchanged(
                            &base,
                            Reply::Rejected {
                                code: "control_source_denied".into(),
                            },
                        )?,
                    )
                };
            progress.stage = "command_commit";
            let outcome = match &transition.reply {
                Reply::Rejected {
                    code,
                } if code == "authorization_command_not_yet_valid" => "not_yet_valid",
                Reply::Rejected {
                    ..
                } => "rejected",
                _ => "committed",
            };
            store
                .commit(&token, &base, transition, &entry.id, &request_id, 120000)
                .await?;
            progress.stage = "commit_read_and_checkpoint";
            base = self.read_checked(Some(progress)).await?;
            if let Some(command_id) = &progress.command_id {
                tracing::debug!(
                    node_id = %self.routes.configuration().node_id,
                    command_id,
                    route_id = ?progress.route_id,
                    request_id = %progress.request_id,
                    cursor = %progress.cursor,
                    owner_term = ?progress.term,
                    sequence = base.ledger.sequence,
                    stage = "commit_confirmed",
                    outcome,
                    "socket authorization control progress"
                );
            }
        }
        progress.command_id = None;
        progress.route_id = None;
        progress.request_id.clear();
        progress.stage = "expiry_observe";
        if self.settings.expiry_completion {
            let margin = Duration::from_millis(
                self.settings.write_budget_ms + 2 * self.settings.clock_margin_ms,
            );
            let proof = self
                .observations
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .observe(&base.ledger, margin)?;
            if let Some(transition) = base.ledger.expire(&proof, now()?)? {
                progress.stage = "expiry_commit";
                // 许可失效候选仍需 owner 与完整快照 CAS；并发续期不能被旧观察覆盖。
                store
                    .commit(&token, &base, transition, &base.cursor, &nonce(), 1000)
                    .await?;
                progress.stage = "expiry_read_and_checkpoint";
                base = self.read_checked(Some(progress)).await?;
            }
        }
        // 只有已提交且已复验的目录可用于权威观测；从请求起点扣除等待和时钟余量。
        self.metrics.control.redis_owner(
            &base.ledger,
            acquired_at
                + Duration::from_millis(5000u64.saturating_sub(self.settings.clock_margin_ms)),
        );
        Ok(())
    }

    /// 业务作用：把已验证 key 的部署身份转为领域权威，并核对完整作用域、时效和签名。
    /// 参数说明：
    /// - `signed`：经过严格结构解析的完整控制请求。
    ///
    /// 返回：可信来源；命令体中的字段不能改变节点或管理 scope。
    fn verify(&self, signed: &Signed) -> anyhow::Result<Principal> {
        let request = &signed.request;
        let key = self
            .settings
            .keys
            .get(&request.key_id)
            .ok_or_else(|| anyhow::anyhow!("control_source_denied"))?;
        let timestamp = now()?.as_millis();
        anyhow::ensure!(
            request.contract == "nacket-control/1"
                && request.cluster_id == self.cluster_id
                && request.scope_epoch == self.scope_epoch
                && request.expires_at > request.created_at
                && request.expires_at.as_millis() - request.created_at.as_millis()
                    <= self.settings.request_timeout_ms
                && request.created_at.as_millis().saturating_sub(timestamp)
                    <= self.settings.clock_margin_ms
                && request.expires_at.as_millis() > timestamp,
            "control_request_expired_or_mismatched"
        );
        crate::domain::message::envelope::identifier(&request.startup_request)?;
        crate::domain::message::envelope::identifier(&request.nonce)?;
        if let Command::Define {
            minimum_ready,
            definition,
            ..
        }
        | Command::ApplyManifest {
            minimum_ready,
            definition,
            ..
        } = &request.command
        {
            anyhow::ensure!(
                *minimum_ready
                    == definition
                        .route
                        .minimum_ready(self.settings.minimum_ready_nodes),
                "control_minimum_ready_mismatch"
            );
        }
        anyhow::ensure!(
            nasa::crypto::hmac_sha256_verify(
                &serde_json::to_string(request)?,
                &self.keys[&request.key_id],
                &signed.signature
            ),
            "control_signature_denied"
        );
        Ok(match &key.principal {
            KeyPrincipal::Node {
                node_id,
            } => Principal::Node {
                node_id: node_id.clone(),
                startup_request: request.startup_request.clone(),
            },
            KeyPrincipal::Administrator {
                subject,
                routes,
            } => {
                if self.settings.provisioner_key_id.as_ref() == Some(&request.key_id) {
                    Principal::Provisioner {
                        subject: subject.clone(),
                        routes: routes.clone(),
                    }
                } else {
                    Principal::Administrator {
                        subject: subject.clone(),
                        routes: routes.clone(),
                    }
                }
            }
        })
    }

    /// 业务作用：对被拒绝或已经执行的请求只推进输入水位，不重复执行业务状态转换。
    /// 参数说明：
    /// - `base`：当前已提交完整状态。
    /// - `reply`：此次输入应保存的确定回复。
    ///
    /// 返回：仅推进提交序号的候选，仍需真实后端确认。
    fn unchanged(&self, base: &Snapshot, reply: Reply) -> anyhow::Result<Transition> {
        let mut ledger = base.ledger.clone();
        ledger.sequence = ledger
            .sequence
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("control_sequence_exhausted"))?;
        Ok(Transition {
            ledger,
            reply,
        })
    }

    /// 业务作用：为所有请求使用同一控制容量合同，不接受请求方扩大限额。
    /// 参数说明: 无。
    ///
    /// 返回：有限节点、路由、操作和许可容量。
    fn limits(&self) -> Limits {
        Limits {
            routes: 1024,
            nodes: 256,
            operations: 65536,
            permits: 65536,
            permit_ttl_ms: self.settings.permit_ttl_ms,
        }
    }
}

/// 业务作用：为控制请求和持久操作记录提供统一整数毫秒诊断时间。
/// 参数说明: 无。
///
/// 返回：跨语言安全范围内的 Unix 时间，不作为许可失效的独立证据。
fn now() -> anyhow::Result<UnixMillis> {
    let millis = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_millis();
    UnixMillis::new(u64::try_from(millis)?).map_err(anyhow::Error::msg)
}

/// 业务作用：冻结所有 controller 必须一致的来源权限和许可合同，节点本地角色或凭据挂载路径不改变权威策略。
/// 参数说明：
/// - `settings`：同一作用域的控制策略。
///
/// 返回：不含凭据的确定性摘要；策略变更必须通过新的受信控制基线。
fn policy_digest(settings: &Settings) -> anyhow::Result<String> {
    let principals = settings
        .keys
        .iter()
        .map(|(id, key)| (id, &key.principal))
        .collect::<BTreeMap<_, _>>();
    let bytes = serde_json::to_vec(&serde_json::json!({
        "principals": principals,
        "provisionerKeyId": settings.provisioner_key_id,
        "permitTtlMs": settings.permit_ttl_ms,
        "renewIntervalMs": settings.renew_interval_ms,
        "requestTimeoutMs": settings.request_timeout_ms,
        "clockMarginMs": settings.clock_margin_ms,
        "writeBudgetMs": settings.write_budget_ms,
        "expiryCompletion": settings.expiry_completion,
        "minimumReadyNodes": settings.minimum_ready_nodes
    }))?;
    Ok(format!("{:x}", Sha256::digest(bytes)))
}

/// 业务作用：把受信控制命令投影为固定操作类别，路由名和 operationId 不进入标签。
/// 参数说明：
/// - `command`：本节点准备提交的完整命令。
///
/// 返回：静态类别；批次的各个命令由调用方分别记录。
fn control_subject(command: &Command) -> &'static str {
    match command {
        Command::ApplyAuthorization {
            ..
        } => "authorization",
        Command::AuthorizationApplied {
            ..
        } => "authorization_applied",
        Command::Observe {} => "observe",
        Command::ReserveConnection {
            ..
        } => "reserve_connection",
        Command::ReleaseConnection {
            ..
        } => "release_connection",
        Command::ConsumeAssertion {
            ..
        } => "consume_assertion",
        Command::Register {} | Command::RegisterController {} => "register",
        Command::Manifest {
            ..
        } => "manifest",
        Command::ApplyManifest {
            ..
        } => "apply_manifest",
        Command::Define {
            ..
        } => "define",
        Command::Offline {
            ..
        } => "offline",
        Command::Resume {
            ..
        } => "resume",
        Command::Transfer {
            ..
        } => "transfer",
        Command::Drained {
            ..
        } => "drain",
        Command::Prepared {
            ..
        } => "prepare",
        Command::Renew {
            ..
        }
        | Command::RenewAccess {
            ..
        } => "renew",
        Command::Applied {
            ..
        } => "apply",
        Command::Failed {
            ..
        } => "failed",
        Command::Retire {
            ..
        } => "retire",
        Command::Batch {
            ..
        } => "batch",
    }
}
