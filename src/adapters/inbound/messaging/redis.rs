use super::*;
use crate::application::gateway::dispatch::Deferred;
use crate::domain::control::clock::LeaseTime;
use crate::domain::{
    message::{control::CONTRACT, envelope::Push, json},
    route::policies::DeliveryMode,
};
use nasa::redis::{RedisClient, RedisCommand};
use sha2::{Digest, Sha256};
use std::time::Duration;
mod keyed;
mod realtime;
mod record;
mod recovery;
use record::Record;

/// 业务作用：在 Ready 前读取具体初始游标，随后由受管任务逐条完成本地投递决定。
/// 参数说明：
/// - `application`：提供 Redis 来源与后台任务管理。
/// - `gateway`：本地会话分发器。
/// - `ingress`：固定来源权限和预算。
/// - `source`：受管 Redis 名称。
/// - `stream`：部署批准的业务 stream。
///
/// 返回：消费计划登记成功；来源或初始基线不明时拒绝启动。
pub async fn install(
    application: &nasa::Application,
    gateway: Arc<Gateway>,
    ingress: Arc<TrustedIngress>,
    source: &str,
    stream: &str,
) -> anyhow::Result<()> {
    let client = application.redis(source).await?;
    if ingress.configuration.delivery == DeliveryMode::Recoverable {
        return install_recovery(application, gateway, ingress, client).await;
    }
    let dependency = gateway.routes.dependency(&ingress.id)?;
    let maximum =
        ingress.configuration.max_body_bytes + ingress.configuration.max_metadata_bytes + 4096;
    let read_plan = super::buffers::ReadPlan::new(&gateway, &ingress.configuration)?;
    let mut reader =
        realtime::Realtime::open(client.clone(), stream, read_plan.batch, maximum).await?;
    if gateway.broker_metrics.source(&ingress.id).is_some() {
        let observer = reader.observer();
        crate::observability::broker::Broker::install(
            application,
            gateway.broker_metrics.clone(),
            &ingress.id,
            move |_| {
                let observer = observer.clone();
                async move { observer.read().await }
            },
        )
        .await?;
    }
    application
        .serve_when_ready(
            format!("socket-ingress-{}", ingress.id),
            move |shutdown| async move {
                // 依赖确认前重新固定实际接流末尾，不读取停机或启动准备期间积累的业务记录。
                tokio::select! {
                    biased;
                    _ = shutdown.cancelled() => return Ok::<(), anyhow::Error>(()),
                    result = reader.start_at_tail() => result?,
                }
                if ingress.configuration.purpose == crate::config::messaging::IngressPurpose::Push {
                    return keyed::run(gateway, ingress, client, reader, read_plan, shutdown).await;
                }
                let mut failures = 0u32;
                'read: loop {
                    let _read_reservation = match read_plan.acquire(&gateway) {
                        Ok(reservation) => reservation,
                        Err(_) => {
                            tokio::select! {
                                _ = shutdown.cancelled() => return Ok(()),
                                _ = tokio::time::sleep(Duration::from_millis(50)) => {},
                            }
                            continue;
                        }
                    };
                    let evidence = LeaseTime::start(Duration::from_millis(
                        ingress.configuration.dependency_stale_ms,
                    ))?;
                    let records = tokio::select! {
                        biased;
                        _ = shutdown.cancelled() => return Ok::<(), anyhow::Error>(()),
                        result = reader.read() => result,
                    };
                    match records {
                        Ok(records) => {
                            dependency.confirm(evidence);
                            failures = 0;
                            if records.is_empty() {
                                tokio::select! {
                                    _ = shutdown.cancelled() => return Ok(()),
                                    _ = tokio::time::sleep(Duration::from_millis(25)) => {},
                                }
                                continue;
                            }
                            for mut entry in records {
                                // 隔离记录得到 Redis 回执前不移动游标；取消等待不能把尚未完成的记录视为已处理。
                                if let Err(error) = process(&gateway, &ingress, &mut entry).await {
                                    if error.downcast_ref::<Deferred>().is_some() {
                                        // 依赖或容量暂时不足不是无效业务记录，保留原游标并按原始消息期限重新判断。
                                        tokio::select! {
                                            _ = shutdown.cancelled() => return Ok(()),
                                            _ = tokio::time::sleep(Duration::from_millis(100)) => {},
                                        }
                                        continue 'read;
                                    }
                                    if let Err(error) = dead_letter(&gateway, &client, &ingress, &entry).await
                                    {
                                        if error.downcast_ref::<Deferred>().is_some() {
                                            tokio::select! {
                                                _ = shutdown.cancelled() => return Ok(()),
                                                _ = tokio::time::sleep(Duration::from_millis(100)) => {},
                                            }
                                            continue 'read;
                                        }
                                        dependency.deny("isolation_unavailable");
                                        return Err(error);
                                    }
                                }
                                reader.complete(entry.id);
                            }
                        }
                        Err(error) => {
                            dependency.deny("broker_unavailable");
                            if error.to_string().contains("SC_REALTIME_") {
                                dependency.deny("stream_baseline_lost");
                                gateway.routes.close();
                                return Err(error);
                            }
                            failures = failures.saturating_add(1);
                            let delay = Duration::from_millis(
                                (100u64.saturating_mul(1u64 << failures.min(6))).min(5000),
                            );
                            tokio::select! {
                                _ = shutdown.cancelled() => return Ok(()),
                                _ = tokio::time::sleep(delay) => {},
                            }
                        }
                    }
                }
            },
        )
        .await?;
    Ok(())
}

/// 业务作用：节点代际确认后启动持久组恢复，取消和失败都不提前确认正在处理的记录。
/// 参数说明：
/// - `application`：提供 Ready 顺序和关键任务退出责任。
/// - `gateway`：维护当前连接及控制门禁的产品运行域。
/// - `ingress`：固定来源权限和恢复策略。
/// - `client`：已经建立的受管 Redis 数据来源。
///
/// 返回：任务已登记；运行中缺口或失权关闭服务，连接故障保留 PEL 并退避。
async fn install_recovery(
    application: &nasa::Application,
    gateway: Arc<Gateway>,
    ingress: Arc<TrustedIngress>,
    client: Arc<RedisClient>,
) -> anyhow::Result<()> {
    let dependency = gateway.routes.dependency(&ingress.id)?;
    let read_plan = super::buffers::ReadPlan::new(&gateway, &ingress.configuration)?;
    let mut read_configuration = ingress.configuration.clone();
    read_configuration.batch_size = read_plan.batch;
    let observation = Arc::new(std::sync::OnceLock::<recovery::Recovery>::new());
    let source_observation = observation.clone();
    crate::observability::broker::Broker::install(
        application,
        gateway.broker_metrics.clone(),
        &ingress.id,
        move |_| {
            let observed = source_observation.clone();
            async move {
                observed
                    .get()
                    .ok_or_else(|| anyhow::anyhow!("recovery_observation_not_ready"))?
                    .observe()
                    .await
            }
        },
    )
    .await?;
    application
        .serve_when_ready(
            format!("socket-recovery-{}", ingress.id),
            move |shutdown| async move {
                let incarnation = gateway
                    .incarnation
                    .get()
                    .ok_or_else(|| anyhow::anyhow!("recovery_incarnation_missing"))?;
                let recovery = recovery::Recovery::open(
                    client,
                    &gateway.configuration,
                    &ingress.id,
                    &read_configuration,
                    incarnation,
                    &gateway.startup_id,
                )
                .await?;
                if gateway.broker_metrics.source(&ingress.id).is_some() {
                    let _ = observation.set(recovery.clone());
                }
                let mut failures = 0u32;
                'consume: loop {
                    let _read_reservation = match read_plan.acquire(&gateway) {
                        Ok(reservation) => reservation,
                        Err(_) => {
                            tokio::select! {
                                _ = shutdown.cancelled() => return Ok(()),
                                _ = tokio::time::sleep(Duration::from_millis(50)) => {},
                            }
                            continue;
                        }
                    };
                    let evidence = LeaseTime::start(Duration::from_millis(
                        ingress.configuration.dependency_stale_ms,
                    ))?;
                    let records = tokio::select! {
                        biased;
                        _ = shutdown.cancelled() => return Ok(()),
                        result = recovery.read() => result,
                    };
                    let records = match records {
                        Ok(records) => {
                            gateway.metrics.add(
                                crate::observability::Counter::Recovery,
                                "redis_read",
                                "ok",
                                1,
                            );
                            gateway.metrics.add(
                                crate::observability::Counter::Recovery,
                                "redis_claim",
                                "ok",
                                records.iter().filter(|record| record.reclaimed).count() as u64,
                            );
                            dependency.confirm(evidence);
                            failures = 0;
                            records
                        }
                        Err(error) => {
                            gateway.metrics.add(
                                crate::observability::Counter::Recovery,
                                "redis_read",
                                if error.to_string().contains("SC_RECOVERY_FENCED") {
                                    "conflict"
                                } else {
                                    "unavailable"
                                },
                                1,
                            );
                            dependency.deny("broker_unavailable");
                            // 服务端脚本拒绝代表权威或基线不成立；网络错误不能通过重建组绕开恢复责任。
                            if error.to_string().contains("SC_RECOVERY_") {
                                gateway.routes.close();
                                return Err(error);
                            }
                            failures = failures.saturating_add(1);
                            let delay =
                                Duration::from_millis((100u64 << failures.min(5)).min(5000));
                            tokio::select! {
                                _ = shutdown.cancelled() => return Ok(()),
                                _ = tokio::time::sleep(delay) => {},
                            }
                            continue;
                        }
                    };
                    let empty = records.is_empty();
                    for mut record in records {
                        let reject = match process(&gateway, &ingress, &mut record).await {
                            Ok(()) => false,
                            Err(error) if error.downcast_ref::<Deferred>().is_some() => {
                                gateway.metrics.add(
                                    crate::observability::Counter::Recovery,
                                    "redis_retry",
                                    "unavailable",
                                    1,
                                );
                                tokio::select! {
                                    _ = shutdown.cancelled() => return Ok(()),
                                    _ = tokio::time::sleep(Duration::from_millis(100)) => {},
                                }
                                continue 'consume;
                            }
                            Err(_) => true,
                        };
                        // 本地处理与 broker 确认之间可能中断，恢复允许相同 messageId 再处理，不能承诺终端恰好一次。
                        let completed = tokio::select! {
                            biased;
                            _ = shutdown.cancelled() => return Ok(()),
                            result = recovery.complete(&record.id, reject) => result,
                        };
                        gateway.metrics.add(
                            crate::observability::Counter::Recovery,
                            if reject {
                                "redis_isolate"
                            } else {
                                "redis_confirm"
                            },
                            if completed.is_ok() {
                                "committed"
                            } else {
                                "unavailable"
                            },
                            1,
                        );
                        if let Err(error) = completed {
                            dependency.deny("confirmation_unavailable");
                            if error.to_string().contains("SC_RECOVERY_FENCED")
                                || error.to_string().contains("SC_RECOVERY_GAP")
                                || error.to_string().contains("SC_RECOVERY_BASELINE")
                            {
                                gateway.routes.close();
                                return Err(error);
                            }
                            tokio::select! {
                                _ = shutdown.cancelled() => return Ok(()),
                                _ = tokio::time::sleep(Duration::from_millis(250)) => {},
                            }
                            continue 'consume;
                        }
                    }
                    if empty {
                        if recovery.cleanup().await.is_err() {
                            dependency.deny("recovery_cleanup_unavailable");
                        }
                        tokio::select! {
                            _ = shutdown.cancelled() => return Ok(()),
                            _ = tokio::time::sleep(Duration::from_millis(50)) => {},
                        }
                    }
                }
            },
        )
        .await?;
    Ok(())
}

/// 业务作用：严格识别单布局 Redis 字段并验证真实来源，拒绝重复字段和隐藏的旧 relay 头。
/// 参数说明：
/// - `gateway`：只接受受信服务 Push 的分发器。
/// - `ingress`：当前 stream 固定权限。
/// - `entry`：底层保序、保重复、保二进制的完整记录。
///
/// 返回：所有本地收件决定完成；任何无效结构不进入分发。
async fn process(
    gateway: &Gateway,
    ingress: &TrustedIngress,
    entry: &mut Record,
) -> anyhow::Result<()> {
    let mut scoped = gateway.metrics.scopes.event(
        crate::observability::scopes::Kind::Ingress,
        &ingress.id,
        "consume",
    );
    let result = process_record(gateway, ingress, entry).await;
    scoped.finish(
        result
            .as_ref()
            .map(|_| "accepted")
            .unwrap_or_else(|error| crate::observability::outcome(error)),
    );
    gateway.metrics.add(
        crate::observability::Counter::Consumer,
        "redis",
        result
            .as_ref()
            .map(|_| "accepted")
            .unwrap_or_else(|error| crate::observability::outcome(error)),
        1,
    );
    result
}

/// 业务作用：保留输入原文完成来源认证与本地投递，失败仍能交给原文隔离入口。
/// 参数说明：
/// - `gateway`：共享消息分发与预算。
/// - `ingress`：部署固定的受信来源。
/// - `entry`：可能移交正文分配的完整记录。
///
/// 返回：本地决定完成；任何失败都保留隔离所需字段。
async fn process_record(
    gateway: &Gateway,
    ingress: &TrustedIngress,
    entry: &mut Record,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        (3..=5).contains(&entry.fields.len()),
        "invalid_stream_fields"
    );
    let mut fields = BTreeMap::new();
    for field in &entry.fields {
        anyhow::ensure!(
            [
                b"contract".as_slice(),
                b"meta",
                b"message",
                b"keyId",
                b"signature"
            ]
            .contains(&field.0.as_ref())
                && fields.insert(field.0.as_ref(), field.1.as_ref()).is_none(),
            "invalid_stream_fields"
        );
    }
    anyhow::ensure!(
        fields.get(b"contract".as_slice()).copied() == Some(CONTRACT.as_bytes()),
        "unsupported_contract"
    );
    let metadata = fields
        .get(b"meta".as_slice())
        .ok_or_else(|| anyhow::anyhow!("metadata_missing"))?;
    let body = fields
        .get(b"message".as_slice())
        .ok_or_else(|| anyhow::anyhow!("body_missing"))?;
    anyhow::ensure!(
        body.len() <= ingress.configuration.max_body_bytes,
        "body_capacity"
    );
    let digest: [u8; 32] = Sha256::digest(body).into();
    let key = fields
        .get(b"keyId".as_slice())
        .map(|v| std::str::from_utf8(v))
        .transpose()?;
    let signature = fields
        .get(b"signature".as_slice())
        .map(|v| std::str::from_utf8(v))
        .transpose()?;
    let producer = ingress.producer(metadata, &digest, key, signature)?;
    if ingress.configuration.purpose
        == crate::config::messaging::IngressPurpose::AuthorizationControl
    {
        anyhow::ensure!(body.is_empty(), "authorization_body_forbidden");
        let command = json::decode(metadata, ingress.configuration.max_metadata_bytes)?;
        return gateway.authorization_command(ingress, producer, command);
    }
    if ingress.configuration.purpose
        == crate::config::messaging::IngressPurpose::InitializationReply
    {
        anyhow::ensure!(body.is_empty(), "initialization_reply_body_forbidden");
        let reply = json::decode(metadata, ingress.configuration.max_metadata_bytes)?;
        gateway.initializations.resolve(ingress, producer, reply)?;
        return Ok(());
    }
    let push: Push = json::decode(metadata, ingress.configuration.max_metadata_bytes)?;
    drop(fields);
    let index = entry
        .fields
        .iter()
        .position(|field| field.0.as_ref() == b"message")
        .expect("validated message field");
    // 原字段继续保有原始证据，正文只共享同一分配；超限或异步取消不会丢失隔离材料。
    let body = gateway
        .ingress_bytes
        .retain(entry.fields[index].1.clone(), entry.capacities[index])
        .map_err(|_| Deferred("ingress_byte_capacity"))?;
    gateway
        .dispatch(ingress, producer, push, body, digest)
        .await?;
    Ok(())
}

/// 业务作用：把无法处理的原始记录写入隔离 stream，写入确认前不得越过数据游标。
/// 参数说明：
/// - `gateway`：独立隔离字节预算。
/// - `client`：受管 Redis 来源。
/// - `ingress`：固定隔离位置及大小预算。
/// - `entry`：保留原字段顺序与重复项的输入。
///
/// 返回：Redis 已确认隔离记录；过大或隔离不可用时停止该消费任务。
async fn dead_letter(
    gateway: &Gateway,
    client: &RedisClient,
    ingress: &TrustedIngress,
    entry: &Record,
) -> anyhow::Result<()> {
    let size = entry
        .fields
        .iter()
        .try_fold(0usize, |size, field| {
            size.checked_add(field.0.len() + field.1.len())
        })
        .ok_or_else(|| anyhow::anyhow!("dead_letter_capacity"))?;
    anyhow::ensure!(
        size <= ingress.configuration.max_body_bytes
            + ingress.configuration.max_metadata_bytes
            + 4096,
        "dead_letter_capacity"
    );
    let cost = size * 2 + 4096;
    let _permit = gateway
        .isolation_bytes
        .clone()
        .try_acquire_many_owned(cost as u32)
        .map_err(|_| Deferred("isolation_capacity"))?;
    let _observation = gateway.metrics.reserve("isolation_bytes", cost as u64);
    let mut command = RedisCommand::new();
    command
        .arg("XADD")
        .arg(&ingress.configuration.dead_letter)
        .arg("*")
        .arg("sourceId")
        .arg(&entry.id)
        .arg("sourceStream")
        .arg(&entry.stream)
        .arg("reason")
        .arg("record_rejected");
    for field in &entry.fields {
        command.arg(field.0.as_ref()).arg(field.1.as_ref());
    }
    let _: String = client.execute_raw(&command).await?;
    Ok(())
}
