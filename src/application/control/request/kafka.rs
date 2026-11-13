use super::*;
use crate::adapters::control::kafka::{parse_offset, CachedReply, Frame, Store, Writer};
use nasa::kafka::{
    AckMode, KafkaHeaders, Tp, TransactionContext, TransactionIdentity, TransactionLimits,
    TransactionOutput, TransactionPlan,
};
use std::collections::BTreeSet;

/// 业务作用：登记单控制分区的事务 owner，备用实例等待分配，候选提交前不发布成功。
/// 参数说明：
/// - `application`：持有 Kafka、关闭期限和后台任务的宿主。
/// - `authority`：产品来源验证、账本规则与本节点高水位。
///
/// 返回：socket 角色只保留读写请求入口；controller 角色取得受监督的事务责任。
pub(super) async fn install(
    application: &nasa::Application,
    authority: &Arc<Authority>,
) -> anyhow::Result<()> {
    if authority.settings.role == Role::Socket {
        return Ok(());
    }
    let Backend::Kafka(store) = &authority.store else {
        anyhow::bail!("control_backend_mismatch")
    };
    let store = store.clone();
    let runtime = tokio::runtime::Handle::current();
    let weak = Arc::downgrade(authority);
    let tick_authority = weak.clone();
    let observed_epoch = Mutex::new(None);
    let processor_store = store.clone();
    let processor = Arc::new(move |context: &mut TransactionContext| {
        let authority = weak
            .upgrade()
            .ok_or(nasa::kafka::TransactionError::Closed)?;
        let mut observed = observed_epoch
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if *observed != Some(context.assignment_epoch()) {
            // 观察期限必须在当前分配取得 fencing 后重新累积，不能继承上一任 owner 的停顿时间。
            *authority
                .observations
                .lock()
                .unwrap_or_else(|error| error.into_inner()) = Observations::default();
            *observed = Some(context.assignment_epoch());
        }
        process(&authority, &processor_store, &runtime, context).map_err(|error| {
            let reason = error
                .downcast_ref::<nasa::kafka::TransactionError>()
                .copied()
                .unwrap_or(nasa::kafka::TransactionError::HandlerFailed);
            tracing::warn!(reason=%error, "socket Kafka control candidate was not committed");
            reason
        })
    });
    let limits = TransactionLimits {
        max_batch_records: 32,
        max_batch_bytes: 8 * 1024 * 1024,
        max_output_records: 1,
        max_output_bytes: 32 * 1024 * 1024,
        handler_timeout_ms: 15000,
        transaction_timeout_ms: 30000,
        operation_timeout_ms: 5000,
        max_aborted_retries: 3,
        retry_backoff_ms: 250,
    };
    let plan = TransactionPlan {
        source_id: store.source_ref.clone(),
        cluster_id: store.broker_cluster_id.clone(),
        identity: TransactionIdentity {
            application: "nacket-control".into(),
            slot: store.identity.clone(),
            owner: "authority".into(),
        },
        group: format!("socket-control.{}", store.identity),
        input_topics: BTreeSet::from([store.command_topic.clone()]),
        outputs: BTreeSet::from([Tp::new(&store.state_topic, 0)]),
        ack_mode: AckMode::Auto,
        limits,
    };
    let handle = store.handle.transact_on_assignment(plan, processor).await?;
    application
        .spawn_critical("socket-kafka-control", move |shutdown| async move {
            let mut topology = tokio::time::interval(Duration::from_secs(5));
            let mut observe = tokio::time::interval(Duration::from_secs(1));
            topology.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    biased;
                    _ = shutdown.cancelled() => {
                        if let Some(authority) = tick_authority.upgrade() {
                            authority.metrics.control.owner_unavailable();
                        }
                        handle.cancel();
                        return Ok(());
                    }
                    _ = observe.tick() => {
                        if let Some(authority) = tick_authority.upgrade() {
                            let status = handle.status();
                            if status.ready {
                                // 无业务命令时仍以已提交输入推进保守期限观测，不能在本地静默删除许可。
                                let started = std::time::Instant::now();
                                let result = async {
                                    authority.node(Command::Observe {}).await?;
                                    let frame = store
                                        .restore(false)
                                        .await?
                                        .ok_or_else(|| anyhow::anyhow!("control_baseline_missing"))?;
                                    anyhow::ensure!(
                                        frame
                                            .writer
                                            .as_ref()
                                            .is_some_and(|writer| writer.startup_request == authority.startup),
                                        "control_observation_owner_changed"
                                    );
                                    // 回复来自已提交事务且提交者仍是本进程；导出时继续复验同一真实分配代次。
                                    authority.metrics.control.kafka_owner(
                                        &frame.ledger,
                                        started
                                            + Duration::from_millis(authority.settings.request_timeout_ms.min(5000)),
                                        handle.clone(),
                                        status.assignment_epoch,
                                    );
                                    Ok::<(), anyhow::Error>(())
                                }
                                .await;
                                if result.is_err() {
                                    authority.metrics.control.owner_unavailable();
                                }
                            } else {
                                authority.metrics.control.owner_unavailable();
                            }
                        }
                    }
                    _ = topology.tick() => {
                        if let Some(error) = handle.status().failure {
                            anyhow::bail!("control_transaction_failed:{error}");
                        }
                        if store.verify_topics().await.is_err() {
                            // 拓扑或保留合同失去证据时先撤销事务准入，不能继续使用上一次检查结果。
                            if let Some(authority) = tick_authority.upgrade() {
                                authority.metrics.control.owner_unavailable();
                            }
                            handle.cancel();
                            anyhow::bail!("control_topic_contract_unavailable");
                        }
                    }
                }
            }
        })
        .await?;
    Ok(())
}

/// 业务作用：在 producer fencing 之后恢复已提交投影，串行计算整批命令并暂存一个原子输出。
/// 参数说明：
/// - `authority`：固定来源、策略和容量合同。
/// - `store`：所选 Kafka 控制资源及只读恢复入口。
/// - `runtime`：受管运行时，用于等待有限只读恢复。
/// - `context`：不能跨线程移动的事务输入和输出权限。
///
/// 返回：只暂存候选；真正的成功由框架 commit 和后续 read_committed 读取确认。
fn process(
    authority: &Authority,
    store: &Store,
    runtime: &tokio::runtime::Handle,
    context: &mut TransactionContext,
) -> anyhow::Result<()> {
    let restored = runtime.block_on(async {
        // 事务恢复与节点读取共用观察顺序，旧读取完成较晚不能覆盖已经落盘的更新高水位。
        let serial = authority.reads.clone().lock_owned().await;
        let restored = store.restore(true).await?;
        if let Some(frame) = &restored {
            let digest = format!("{:x}", Sha256::digest(serde_json::to_vec(&frame.ledger)?));
            authority
                .observe_checkpoint(frame.ledger.sequence, digest, serial)
                .await?;
        }
        Ok::<_, anyhow::Error>(restored)
    })?;
    let timestamp = now()?;
    let first = context
        .records()
        .first()
        .ok_or_else(|| anyhow::anyhow!("control_input_empty"))?;
    let mut frame = match restored {
        Some(frame) => frame,
        None => {
            // 首次建账必须同时具有显式配置、全新本地数据卷和完整输入起点；已有卷不能重建丢失权威。
            anyhow::ensure!(
                store.initialize && authority.routes.control_is_fresh() && first.offset == 0,
                "control_baseline_missing"
            );
            let mut ledger =
                Ledger::empty(authority.cluster_id.clone(), authority.scope_epoch.clone())?;
            ledger.policy_digest = authority.policy_digest.clone();
            Frame {
                contract: "nacket-control-state/1".into(),
                ledger,
                consumed_offset: "0".into(),
                replies: BTreeMap::new(),
                updated_at: timestamp,
                writer: None,
            }
        }
    };
    anyhow::ensure!(
        frame.ledger.policy_digest == authority.policy_digest
            && first.offset >= parse_offset(&frame.consumed_offset)?,
        "control_restore_mismatch"
    );
    frame
        .replies
        .retain(|_, entry| entry.expires_at > timestamp);
    if authority.settings.expiry_completion {
        let margin = Duration::from_millis(
            authority.settings.write_budget_ms + 2 * authority.settings.clock_margin_ms,
        );
        let proof = authority
            .observations
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .observe(&frame.ledger, margin)?;
        if let Some(transition) = frame.ledger.expire(&proof, timestamp)? {
            frame.ledger = transition.ledger;
        }
    }
    for record in context.records() {
        anyhow::ensure!(
            record.topic == store.command_topic && record.partition == 0,
            "control_input_mismatch"
        );
        let parsed = (record.key.as_deref() == Some(store.identity.as_bytes()))
            .then_some(record.payload.as_deref())
            .flatten()
            .and_then(|body| json::decode_bounded::<Signed>(body, 262144, 32, 4096).ok());
        let principal = parsed
            .as_ref()
            .and_then(|signed| authority.verify(signed).ok());
        let base = Snapshot {
            ledger: frame.ledger.clone(),
            cursor: frame.consumed_offset.clone(),
            position: frame.consumed_offset.clone(),
            hash: String::new(),
        };
        let (id, transition) = if let (Some(signed), Some(principal)) = (parsed, principal) {
            let id = format!("{:x}", Sha256::digest(serde_json::to_vec(&signed.request)?));
            let transition = if let Some(reply) = frame.replies.get(&id) {
                authority.unchanged(&base, reply.reply.clone())?
            } else {
                match frame.ledger.transition(
                    &principal,
                    &signed.request.command,
                    timestamp,
                    &authority.limits(),
                ) {
                    Ok(transition) => transition,
                    Err(error) => authority.unchanged(
                        &base,
                        Reply::Rejected {
                            code: error.to_string(),
                        },
                    )?,
                }
            };
            (id, transition)
        } else {
            (
                format!("invalid-{}", record.offset),
                authority.unchanged(
                    &base,
                    Reply::Rejected {
                        code: "control_source_denied".into(),
                    },
                )?,
            )
        };
        anyhow::ensure!(
            frame.replies.len() < 4096 || frame.replies.contains_key(&id),
            "control_reply_capacity"
        );
        frame.replies.insert(
            id,
            CachedReply {
                reply: transition.reply,
                expires_at: UnixMillis::new(
                    timestamp.as_millis()
                        + 2 * authority.settings.request_timeout_ms
                        + authority.settings.clock_margin_ms,
                )
                .map_err(anyhow::Error::msg)?,
            },
        );
        frame.ledger = transition.ledger;
        frame.consumed_offset = record
            .offset
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("control_offset_exhausted"))?
            .to_string();
    }
    frame.updated_at = timestamp;
    let KeyPrincipal::Node {
        node_id,
    } = &authority.settings.keys[&authority.settings.node_key_id].principal
    else {
        anyhow::bail!("control_node_identity_invalid")
    };
    frame.writer = Some(Writer {
        node_id: node_id.clone(),
        startup_request: authority.startup.clone(),
    });
    store.validate_frame(&frame)?;
    let payload = serde_json::to_vec(&frame)?;
    anyhow::ensure!(
        payload.len() <= 32 * 1024 * 1024 - 1024,
        "control_snapshot_capacity"
    );
    context.send(TransactionOutput {
        destination: Tp::new(&store.state_topic, 0),
        timestamp: Some(timestamp.as_millis() as i64),
        key: Some(store.identity.as_bytes().to_vec()),
        payload: Some(payload),
        headers: KafkaHeaders::default(),
    })?;
    Ok(())
}
