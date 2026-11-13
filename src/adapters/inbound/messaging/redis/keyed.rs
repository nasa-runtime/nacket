//! 实时业务 Stream 的有界按键处理；预取位置与连续完成水位分别维护。

use super::{dead_letter, realtime::Realtime, Record};
use crate::{
    adapters::inbound::messaging::buffers::{ReadPlan, ReadReservation},
    application::gateway::{
        dispatch::{ordering_key, Deferred, TrustedIngress},
        Gateway,
    },
    domain::{
        control::clock::LeaseTime,
        message::{control::CONTRACT, envelope::Push, json},
    },
    observability::{Counter, Reservation},
};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, VecDeque},
    sync::Arc,
    time::Duration,
};
use tokio::{sync::OwnedSemaphorePermit, task::JoinSet, time::Instant};

/// 一次解析后保留的可信元数据，正文仍引用原始记录分配。
pub(super) struct Prepared {
    /// 验签或来源 ACL 确定的生产者，不采用信封自报身份。
    producer: String,
    /// 完整严格解析的业务头，重试不重新反序列化。
    push: Push,
    /// 原始正文的摘要，扇出不重复计算。
    digest: [u8; 32],
    /// 正文字段在原始记录中的唯一位置。
    body_index: usize,
}

impl Prepared {
    /// 业务作用：在进入业务顺序键之前核对原始布局、来源身份和目标结构。
    /// 参数说明：
    /// - `ingress`：部署固定的来源认证与大小限制。
    /// - `record`：保留重复字段与原始字节的输入。
    ///
    /// 返回：可复用的可信元数据；拒绝项仍保留完整原文用于隔离。
    pub fn new(ingress: &TrustedIngress, record: &Record) -> anyhow::Result<Self> {
        anyhow::ensure!(
            (3..=5).contains(&record.fields.len()),
            "invalid_stream_fields"
        );
        let mut fields = BTreeMap::new();
        for (index, field) in record.fields.iter().enumerate() {
            anyhow::ensure!(
                [
                    b"contract".as_slice(),
                    b"meta",
                    b"message",
                    b"keyId",
                    b"signature"
                ]
                .contains(&field.0.as_ref())
                    && fields
                        .insert(field.0.as_ref(), (index, field.1.as_ref()))
                        .is_none(),
                "invalid_stream_fields"
            );
        }
        anyhow::ensure!(
            fields.get(b"contract".as_slice()).map(|value| value.1) == Some(CONTRACT.as_bytes()),
            "unsupported_contract"
        );
        let metadata = fields
            .get(b"meta".as_slice())
            .ok_or_else(|| anyhow::anyhow!("metadata_missing"))?
            .1;
        let (body_index, body) = *fields
            .get(b"message".as_slice())
            .ok_or_else(|| anyhow::anyhow!("body_missing"))?;
        anyhow::ensure!(
            body.len() <= ingress.configuration.max_body_bytes,
            "body_capacity"
        );
        let digest = Sha256::digest(body).into();
        let key = fields
            .get(b"keyId".as_slice())
            .map(|value| std::str::from_utf8(value.1))
            .transpose()?;
        let signature = fields
            .get(b"signature".as_slice())
            .map(|value| std::str::from_utf8(value.1))
            .transpose()?;
        let producer = ingress
            .producer(metadata, &digest, key, signature)?
            .to_owned();
        let push: Push = json::decode(metadata, ingress.configuration.max_metadata_bytes)?;
        push.context.validate()?;
        push.target.validate()?;
        Ok(Self {
            producer,
            push,
            digest,
            body_index,
        })
    }

    /// 业务作用：共享同一原文分配完成当前本地投递，重试仍使用原始消息期限。
    /// 参数说明：
    /// - `gateway`：持有全节点分发与字节预算的运行域。
    /// - `ingress`：原可信来源。
    /// - `record`：准备阶段绑定的同一原始记录。
    ///
    /// 返回：本地决定完成；容量与依赖暂不可用时保留重试责任。
    pub async fn process(
        &self,
        gateway: &Gateway,
        ingress: &TrustedIngress,
        record: &Record,
    ) -> anyhow::Result<()> {
        let now = crate::adapters::outbound::identity::now()?;
        // 字节容量不足也不能继续持有已经超过业务时间窗的顺序责任。
        if self.push.delivery.expires_at <= now
            || now
                .as_millis()
                .saturating_sub(self.push.delivery.created_at.as_millis())
                >= gateway.configuration.ingress_processing.max_retry_age_ms
        {
            return Ok(());
        }
        let body = gateway
            .ingress_bytes
            .retain(
                record.fields[self.body_index].1.clone(),
                record.capacities[self.body_index],
            )
            .map_err(|_| Deferred("ingress_byte_capacity"))?;
        gateway
            .dispatch(
                ingress,
                &self.producer,
                self.push.clone(),
                body,
                self.digest,
            )
            .await?;
        Ok(())
    }
}

struct Work {
    /// 单调本地位置，完成只能更新同一槽位。
    sequence: u64,
    /// 已验证顺序键；非法记录采用独立位置键，不能阻塞合法业务键。
    key: Vec<u8>,
    /// 原文保留到本地处理或必要隔离确认之后。
    record: Record,
    /// 唯一解析所得的可信元数据，None 表示必须隔离。
    prepared: Option<Prepared>,
    /// 后续尝试的最早时刻，退避不占据处理并发名额。
    retry_at: Instant,
    /// 首次接管确定的本地责任期限，超时转为隔离。
    deadline: Instant,
    /// 本批响应保守字节预算，最后一条原文结束后才释放。
    _read: Arc<ReadReservation>,
}

struct Slot {
    /// 原始 Stream ID，只有连续完成项可发布给观察水位。
    id: String,
    /// 本地序号，与实际处理责任精确关联。
    sequence: u64,
    /// 本地处理或隔离已经明确完成。
    completed: bool,
    /// 完成证据仍占用记录名额，禁止旧头部被无界后续完成项淹没。
    _capacity: OwnedSemaphorePermit,
    /// 节点实际未释放记录责任的观察。
    _observed: Reservation,
}

struct Lane {
    /// 当前键按输入次序等待的记录。
    pending: VecDeque<Work>,
    /// 每个键同时只允许一个真实处理责任。
    running: bool,
}

/// 业务作用：按有限顺序键推进实时业务，未知或未确认记录不能越过完成水位。
/// 参数说明：
/// - `gateway`：业务权限、分发与共享预算。
/// - `ingress`：固定实时业务来源。
/// - `client`：受管 Redis 连接入口。
/// - `reader`：已经固定当前接流末尾的读取基线。
/// - `plan`：读前响应保留预算。
/// - `shutdown`：统一应用的停止信号。
///
/// 返回：正常关闭释放全部责任；来源缺口或隔离失败使依赖关闭且任务失败。
pub async fn run(
    gateway: Arc<Gateway>,
    ingress: Arc<TrustedIngress>,
    client: Arc<nasa::redis::RedisClient>,
    mut reader: Realtime,
    plan: ReadPlan,
    shutdown: tokio_util::sync::CancellationToken,
) -> anyhow::Result<()> {
    let dependency = gateway.routes.dependency(&ingress.id)?;
    let policy = &gateway.configuration.ingress_processing;
    let mut lanes: BTreeMap<Vec<u8>, Lane> = BTreeMap::new();
    let mut progress: VecDeque<Slot> = VecDeque::new();
    let mut tasks = JoinSet::new();
    let mut sequence = 0u64;
    let mut timer = tokio::time::interval(Duration::from_millis(25));
    timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut read_at = Instant::now();
    let mut failures = 0u32;
    loop {
        // 同键前项保有执行或隔离责任时不发布后项，无关键共享有限并发名额。
        for lane in lanes.values_mut() {
            if tasks.len() >= policy.max_parallel_fanouts {
                break;
            }
            if lane.running
                || lane
                    .pending
                    .front()
                    .is_none_or(|work| work.retry_at > Instant::now())
            {
                continue;
            }
            let mut work = lane.pending.pop_front().expect("observed head");
            lane.running = true;
            let gateway = gateway.clone();
            let ingress = ingress.clone();
            let client = client.clone();
            tasks.spawn(async move {
                let mut scoped = gateway.metrics.scopes.event(
                    crate::observability::scopes::Kind::Ingress,
                    &ingress.id,
                    "consume",
                );
                let result = if let Some(prepared) = &work.prepared {
                    if Instant::now() >= work.deadline {
                        Err(anyhow::anyhow!("ingress_retry_age_exceeded"))
                    } else {
                        match tokio::time::timeout_at(
                            work.deadline,
                            prepared.process(&gateway, &ingress, &work.record),
                        )
                        .await
                        {
                            Ok(result) => result,
                            Err(_) => Err(anyhow::anyhow!("ingress_retry_age_exceeded")),
                        }
                    }
                } else {
                    Err(anyhow::anyhow!("record_rejected"))
                };
                scoped.finish(
                    result
                        .as_ref()
                        .map(|_| "accepted")
                        .unwrap_or_else(|error| crate::observability::outcome(error)),
                );
                gateway.metrics.add(
                    Counter::Consumer,
                    "redis",
                    result
                        .as_ref()
                        .map(|_| "accepted")
                        .unwrap_or_else(|error| crate::observability::outcome(error)),
                    1,
                );
                let completed = match result {
                    Ok(()) => Ok(true),
                    Err(error) if error.is::<Deferred>() && Instant::now() < work.deadline => {
                        Ok(false)
                    }
                    Err(_) => {
                        work.prepared = None;
                        match dead_letter(&gateway, &client, &ingress, &work.record).await {
                            Ok(()) => Ok(true),
                            Err(error)
                                if error.is::<Deferred>() && Instant::now() < work.deadline =>
                            {
                                Ok(false)
                            }
                            Err(error) => Err(error),
                        }
                    }
                };
                (work, completed)
            });
        }
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => {
                // 先撤销来源就绪，再取消真实执行；未完成记录不伪装为已确认水位。
                dependency.deny("shutdown");
                tasks.abort_all();
                while tasks.join_next().await.is_some() {}
                return Ok(());
            }
            result = tasks.join_next(), if !tasks.is_empty() => {
                let (mut work, completed) = result.expect("running task")?;
                let lane = lanes.get_mut(&work.key).expect("registered key");
                lane.running = false;
                match completed {
                    Ok(true) => {
                        progress
                            .iter_mut()
                            .find(|slot| slot.sequence == work.sequence)
                            .expect("retained progress")
                            .completed = true;
                        if lane.pending.is_empty() {
                            lanes.remove(&work.key);
                        }
                    }
                    Ok(false) => {
                        work.retry_at = Instant::now() + Duration::from_millis(100);
                        lane.pending.push_front(work);
                    }
                    Err(error) => {
                        dependency.deny("isolation_unavailable");
                        return Err(error);
                    }
                }
                while progress.front().is_some_and(|slot| slot.completed) {
                    reader.complete(progress.pop_front().expect("completed prefix").id);
                }
            }
            _ = timer.tick() => {
                if Instant::now() < read_at {
                    continue;
                }
                let evidence = LeaseTime::start(Duration::from_millis(
                    ingress.configuration.dependency_stale_ms,
                ))?;
                let read_budget = plan.acquire(&gateway).ok();
                let record_budget = gateway
                    .redis_pending
                    .clone()
                    .try_acquire_many_owned(plan.batch as u32)
                    .ok();
                let keys_have_space = lanes.values().all(|lane| {
                    lane.pending.len() + usize::from(lane.running)
                        <= policy.max_key_pending_records.saturating_sub(plan.batch)
                });
                let result = if let (Some(read_budget), Some(mut record_budget), true) =
                    (read_budget, record_budget, keys_have_space)
                {
                    match reader.read().await {
                        Ok(records) => {
                            let read_budget = Arc::new(read_budget);
                            for record in records {
                                sequence = sequence
                                    .checked_add(1)
                                    .ok_or_else(|| anyhow::anyhow!("ingress_sequence_exhausted"))?;
                                let prepared = Prepared::new(&ingress, &record).ok();
                                let key = prepared
                                    .as_ref()
                                    .map(|value| ordering_key(&value.push))
                                    .unwrap_or_else(|| {
                                        let mut key = b"rejected\0".to_vec();
                                        key.extend_from_slice(record.id.as_bytes());
                                        key
                                    });
                                reader.admit(&record.id);
                                progress.push_back(Slot {
                                    id: record.id.clone(),
                                    sequence,
                                    completed: false,
                                    _capacity: record_budget.split(1).expect("reserved batch"),
                                    _observed: gateway.metrics.reserve("redis_pending_records", 1),
                                });
                                lanes
                                    .entry(key.clone())
                                    .or_insert_with(|| Lane {
                                        pending: VecDeque::new(),
                                        running: false,
                                    })
                                    .pending
                                    .push_back(Work {
                                        sequence,
                                        key,
                                        record,
                                        prepared,
                                        retry_at: Instant::now(),
                                        deadline: Instant::now()
                                            + Duration::from_millis(policy.max_retry_age_ms),
                                        _read: read_budget.clone(),
                                    });
                            }
                            Ok(())
                        }
                        Err(error) => Err(error),
                    }
                } else {
                    reader.check().await
                };
                match result {
                    Ok(()) => {
                        dependency.confirm(evidence);
                        failures = 0;
                    }
                    Err(error) => {
                        dependency.deny("broker_unavailable");
                        if error.to_string().contains("SC_REALTIME_") {
                            // 输入基线缺失时关闭本地权限，不能跳到新末尾掩盖缺口。
                            gateway.routes.close();
                            return Err(error);
                        }
                        failures = failures.saturating_add(1);
                        read_at = Instant::now() + Duration::from_millis((100u64 << failures.min(5)).min(5000));
                    }
                }
            }
        }
    }
}
