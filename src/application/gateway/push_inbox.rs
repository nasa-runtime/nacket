use super::{
    dispatch::{Deferred, DispatchReport, TrustedIngress},
    Gateway,
};
use crate::{
    domain::message::envelope::Push,
    observability::{Metrics, Reservation},
};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    sync::{mpsc, Notify},
    task::JoinSet,
    time::Instant,
};

struct Work {
    /// 固定来源和 broker 记录位置的交接键，同一记录不启动第二次分发。
    key: String,
    /// 尚未轮询的唯一分发 future，只能由受管异步任务取得执行权。
    future:
        std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<DispatchReport>> + Send>>,
}

enum Status {
    Pending,
    Completed(
        /// 原分发的准确结果，broker 重取时不得重新执行成功部分。
        Result<DispatchReport, Failure>,
        /// 此完成缓存的单调保留截止点，不延长原消息期限。
        Instant,
    ),
}
enum Failure {
    Deferred,
    Rejected,
}
struct Entry {
    /// 唤醒原消费调用；通知不授予 broker 提交权，调用方仍须查询准确结果。
    changed: Arc<Notify>,
    /// 原始记录内容指纹，拒绝相同位置被替换为其它内容。
    fingerprint: [u8; 32],
    /// 实际分发责任或有限完成缓存，broker 重取时查询原结果。
    status: Status,
    /// 尚未释放的交接记录数量观测责任。
    _observed: Reservation,
}

/// Kafka 异步调用保留有限交接责任；候选扇出由受管任务执行，原消费调用等待准确结果。
pub struct Inbox {
    /// 交给受管异步工作任务的有界入口，满容量保留源记录恢复责任。
    sender: mpsc::Sender<Work>,
    /// 交接键到待决任务或完成缓存的有限目录。
    entries: Mutex<BTreeMap<String, Entry>>,
    /// 目录记录数量上限，完成待重取条目仍计入容量。
    maximum: usize,
    /// 节点统一入口指标源，不把消息 ID 放入标签。
    metrics: Arc<Metrics>,
}
impl Inbox {
    /// 业务作用：登记有界分发执行器，让消费等待取消后仍由原任务完成已开始的扇出。
    /// 参数说明：
    /// - `application`：唯一任务与关闭屏障宿主。
    /// - `gateway`：共享身份门禁、字节预算及本地会话目录。
    ///
    /// 返回：可查询同一记录完成状态的交接入口；停止时未完成记录保持 broker 未确认。
    pub async fn install(
        application: &nasa::Application,
        gateway: Arc<Gateway>,
    ) -> anyhow::Result<Arc<Self>> {
        let policy = &gateway.configuration.ingress_processing;
        let (sender, mut receiver) = mpsc::channel::<Work>(policy.max_pending_records);
        let owner = Arc::new(Self {
            sender,
            entries: Mutex::new(BTreeMap::new()),
            maximum: policy.max_pending_records,
            metrics: gateway.metrics.clone(),
        });
        let inbox = owner.clone();
        let parallel = policy.max_parallel_fanouts;
        application.serve_when_ready("socket-push-inbox", move |shutdown| async move {
            let mut tasks = JoinSet::new();
            let mut timer = tokio::time::interval(Duration::from_secs(1));
            timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    biased;
                    _ = shutdown.cancelled() => {
                        // 先停止接受交接，再取消并回收实际扇出 owner；没有完成决定的 broker 记录不确认。
                        receiver.close();
                        tasks.abort_all();
                        while tasks.join_next().await.is_some() {
                        }
                        while receiver.try_recv().is_ok() {
                        }
                        let mut entries = inbox
                            .entries
                            .lock()
                            .unwrap_or_else(|error| error.into_inner());
                        for entry in entries.values() {
                            entry.changed.notify_waiters();
                        }
                        entries.clear();
                        return Ok::<(), anyhow::Error>(());
                    }
                    _ = timer.tick() => {
                        inbox.entries
                            .lock()
                            .unwrap_or_else(|error| error.into_inner())
                            .retain(|_, entry| {
                                !matches!(entry.status, Status::Completed(_, at) if at.elapsed() >= Duration::from_secs(30))
                            });
                    }
                    result = tasks.join_next(), if !tasks.is_empty() => {
                        if let Some(Err(error)) = result {
                            return Err(error.into());
                        }
                    }
                    work = receiver.recv(), if tasks.len() < parallel => {
                        let Some(work) = work else {
                            return Ok(());
                        };
                        let inbox = inbox.clone();
                        tasks.spawn(async move {
                            let result = work.future.await;
                            let result = result.map_err(|error| {
                                if error.downcast_ref::<Deferred>().is_some() {
                                    Failure::Deferred
                                } else {
                                    Failure::Rejected
                                }
                            });
                            if let Some(entry) = inbox
                                .entries
                                .lock()
                                .unwrap_or_else(|error| error.into_inner())
                                .get_mut(&work.key)
                            {
                                entry._observed.finish_pending();
                                entry.status = Status::Completed(result, Instant::now());
                                entry.changed.notify_waiters();
                            }
                        });
                    }
                }
            }
        }).await?;
        Ok(owner)
    }

    /// 业务作用：按实际 broker 位置关联同一次交接，消费调用等待原任务完成而不主动触发 broker 重取。
    /// 参数说明：
    /// - `gateway`：提供独立 MQ 正文预算。
    /// - `key`：入口、topic、partition 和 offset 的规范组合。
    /// - `fingerprint`：真实生产者、原始 metadata 与正文摘要的指纹。
    /// - `ingress`：已验证的来源与权限快照。
    /// - `producer`：来源认证取得的真实主体。
    /// - `push`：严格解析且分区键已核对的业务元数据。
    /// - `body`：框架已经物化的共享原文，包含仍被引用的来源预算。
    /// - `digest`：同一正文已经计算的摘要。
    ///
    /// 返回：全部本地决定完成后返回结果；容量或停机拒绝仍保留 broker 未确认责任。
    #[expect(
        clippy::too_many_arguments,
        reason = "入站责任须绑定原消息、当前期限及有界容量。"
    )]
    pub async fn accept(
        &self,
        gateway: &Arc<Gateway>,
        key: String,
        fingerprint: [u8; 32],
        ingress: Arc<TrustedIngress>,
        producer: String,
        push: Push,
        body: bytes::Bytes,
        digest: [u8; 32],
    ) -> anyhow::Result<DispatchReport> {
        let changed = self.submit(
            gateway,
            key.clone(),
            fingerprint,
            ingress,
            producer,
            push,
            body,
            digest,
        )?;
        let notified = changed.notified();
        tokio::pin!(notified);
        loop {
            // 先登记通知再查结果，避免分发在查询与等待之间结束而丢失唤醒。
            notified.as_mut().enable();
            {
                let mut entries = self
                    .entries
                    .lock()
                    .unwrap_or_else(|error| error.into_inner());
                let entry = entries.get(&key).ok_or(Deferred("fanout_stopped"))?;
                if matches!(entry.status, Status::Completed(_, _)) {
                    let entry = entries.remove(&key).expect("observed record");
                    let Status::Completed(result, _) = entry.status else {
                        unreachable!();
                    };
                    return result.map_err(|error| match error {
                        Failure::Deferred => Deferred("fanout_dependency").into(),
                        Failure::Rejected => anyhow::anyhow!("fanout_record_rejected"),
                    });
                }
            }
            notified.as_mut().await;
            notified.set(changed.notified());
        }
    }

    /// 业务作用：为唯一分发预留条数与原文字节，重复调用仅取得原责任的完成通知。
    /// 参数说明：
    /// - `gateway`：节点预算及分发入口。
    /// - `key`：固定 broker 记录位置。
    /// - `fingerprint`：来源与实际内容的摘要。
    /// - `ingress`：已验证来源。
    /// - `producer`：受信生产主体。
    /// - `push`：已校验元数据。
    /// - `body`：共享且已拥有的原文。
    /// - `digest`：实际正文摘要。
    ///
    /// 返回：唯一分发的通知句柄；重复冲突、容量不足或停止时不创建新责任。
    #[expect(
        clippy::too_many_arguments,
        reason = "已预留容量与原消息权限一同移交，不延后补取。"
    )]
    fn submit(
        &self,
        gateway: &Arc<Gateway>,
        key: String,
        fingerprint: [u8; 32],
        ingress: Arc<TrustedIngress>,
        producer: String,
        push: Push,
        body: bytes::Bytes,
        digest: [u8; 32],
    ) -> anyhow::Result<Arc<Notify>> {
        let mut entries = self
            .entries
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if let Some(entry) = entries.get(&key) {
            anyhow::ensure!(entry.fingerprint == fingerprint, "broker_record_conflict");
            return Ok(entry.changed.clone());
        }
        if self.sender.is_closed() || entries.len() >= self.maximum {
            return Err(Deferred("fanout_inbox_capacity").into());
        }
        let slot = self
            .sender
            .try_reserve()
            .map_err(|_| Deferred("fanout_inbox_capacity"))?;
        // 原文随框架输入预算存活；产品再预约共享正文容量，不因跨任务交接增加正文副本。
        let length = body.len();
        let body = gateway
            .ingress_bytes
            .retain(body, length)
            .map_err(|_| Deferred("ingress_byte_capacity"))?;
        let gateway = gateway.clone();
        let future: std::pin::Pin<
            Box<dyn std::future::Future<Output = anyhow::Result<DispatchReport>> + Send>,
        > = Box::pin(async move {
            gateway
                .dispatch(&ingress, &producer, push, body, digest)
                .await
        });
        // 唯一受管任务负责 UID 等待；消费调用取消不复制执行权，也不重放已接纳部分。
        let work = Work {
            key: key.clone(),
            future,
        };
        slot.send(work);
        let changed = Arc::new(Notify::new());
        entries.insert(
            key,
            Entry {
                changed: changed.clone(),
                fingerprint,
                status: Status::Pending,
                _observed: self.metrics.reserve("fanout_pending_records", 1),
            },
        );
        Ok(changed)
    }
}
