//! 会话内请求的任务归属、内容指纹及有限结果保留。

use super::actions::Reply;
use crate::domain::{message::envelope::ClientRequest, route::policies::RequestCache};
use bytes::Bytes;
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex, OnceLock,
    },
    time::Duration,
};
use tokio::{
    sync::{Notify, OwnedSemaphorePermit, Semaphore},
    time::Instant,
};

const RECORD_BYTES: u32 = 4096;

struct Completed {
    /// 唯一执行结果，重复请求只能观察该结果而不能重做动作。
    reply: Reply,
    /// 完成结果的单调保留截止点，在途请求不因保留期淘汰。
    expires: Instant,
    /// 结果正文同时占用的会话与节点字节预约。
    _body_capacity: Option<(OwnedSemaphorePermit, OwnedSemaphorePermit)>,
    /// 结果正文缓存的实际保留字节观测责任。
    _body_observed: Option<crate::observability::Reservation>,
}

/// 等待者只借用同一结果记录，不取得执行业务动作的权限。
pub struct Record {
    /// 完整控制信息和原始正文的 SHA-256 指纹，拒绝同 ID 异内容。
    fingerprint: [u8; 32],
    /// 首次完成结果的单次发布位置，空值表示仍有执行责任。
    result: Mutex<Option<Completed>>,
    /// 唤醒原结果等待者的信号，唤醒不授予再次执行资格。
    changed: Notify,
    /// 当前会话结果缓存字节池，用于完成时预约正文。
    local_bytes: Arc<Semaphore>,
    /// 节点共享结果缓存字节池，单会话不能绕过总预算。
    node_bytes: Arc<Semaphore>,
    /// 完成后的有限保留时长，不延长业务消息或授权有效期。
    retention: Duration,
    /// 可选节点指标源，不用请求 ID 作为标签。
    metrics: Option<Arc<crate::observability::Metrics>>,
    /// 请求记录数量和固定记录空间的观测责任。
    _observed: Option<(
        crate::observability::Reservation,
        crate::observability::Reservation,
    )>,
    /// 节点记录数量、会话和节点固定记录字节的三份实际预约。
    _capacity: (
        OwnedSemaphorePermit,
        OwnedSemaphorePermit,
        OwnedSemaphorePermit,
    ),
}

/// 每个会话拥有独立目录；节点配额由全部会话共享。
pub struct RequestTable {
    /// 握手冻结的请求记录数量、字节和保留期限策略。
    policy: RequestCache,
    /// 当前会话请求 ID 到唯一执行记录的有限目录。
    records: Mutex<BTreeMap<String, Arc<Record>>>,
    /// 全部会话共享的记录数量容量。
    node_records: Arc<Semaphore>,
    /// 当前会话记录及结果正文的字节预算。
    local_bytes: Arc<Semaphore>,
    /// 全部会话记录及结果正文的节点字节预算。
    node_bytes: Arc<Semaphore>,
    /// 在首次请求前绑定的固定指标实例，后续不能替换。
    metrics: OnceLock<Arc<crate::observability::Metrics>>,
    /// 目录创建的单调起点，用于计算无需墙钟的清理节奏。
    started: Instant,
    /// 相对于 started 的下次允许清理毫秒数，原子限制并发扫描频率。
    next_sweep: AtomicU64,
}

/// 首次请求独占执行责任；释放责任必须唤醒等待者且保留取消结果。
pub struct Execution {
    /// 本次首次执行独占负责完成的关联记录。
    record: Arc<Record>,
    /// 尚未正常完成时由 Drop 发布的取消回复，避免等待者永久悬挂。
    cancellation: Option<Reply>,
}

/// 会话请求表的接纳结果，同一请求指纹最多授予一次执行责任。
#[expect(
    clippy::large_enum_variant,
    reason = "首次动作直接移交取消责任，重复动作仅借用已有记录。"
)]
pub enum Admission {
    /// 首次请求负责完成动作，并在取消时保存可观察终态。
    Execute(
        /// 只授予首次请求的唯一执行责任。
        Execution,
    ),
    /// 重复请求仅等待原结果，不重放可能产生副作用的操作。
    Observe(
        /// 重复请求共享的原结果记录，不得再次执行动作。
        Arc<Record>,
    ),
}

impl RequestTable {
    /// 业务作用：建立空会话目录，所有记录及缓存正文同时受本地与节点预算约束。
    /// 参数说明：
    /// - `policy`：路由已经验证的数量、字节和保留期限。
    /// - `node_records`：整个节点共享的记录容量。
    /// - `node_bytes`：整个节点共享的缓存字节容量。
    ///
    /// 返回：没有分配任何请求槽位的会话目录。
    pub fn new(
        policy: RequestCache,
        node_records: Arc<Semaphore>,
        node_bytes: Arc<Semaphore>,
    ) -> Arc<Self> {
        Arc::new(Self {
            local_bytes: Arc::new(Semaphore::new(policy.max_bytes)),
            policy,
            records: Mutex::new(BTreeMap::new()),
            node_records,
            node_bytes,
            metrics: OnceLock::new(),
            started: Instant::now(),
            next_sweep: AtomicU64::new(0),
        })
    }

    /// 业务作用：在订阅意图或远程动作建立前确定请求归属，同 ID 异内容不能再次执行。
    /// 参数说明：
    /// - `request`：已通过结构校验的客户端请求。
    /// - `body`：不可变业务字节，仅计算摘要，不进行反序列化。
    /// - `cancelled`：执行责任提前结束时保留的固定响应。
    ///
    /// 返回：新请求取得唯一执行责任；重复请求只观察原结果，冲突或容量不足拒绝。
    pub fn begin(
        &self,
        request: &ClientRequest,
        body: Option<&Bytes>,
        cancelled: Reply,
    ) -> anyhow::Result<Admission> {
        let metadata = serde_json::to_vec(request)?;
        let mut hasher = Sha256::new();
        hasher.update((metadata.len() as u64).to_be_bytes());
        hasher.update(&metadata);
        hasher.update([u8::from(body.is_some())]);
        if let Some(body) = body {
            hasher.update(body);
        }
        let fingerprint: [u8; 32] = hasher.finalize().into();
        let mut records = self
            .records
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        records.retain(|_, record| !record.expired());
        if let Some(record) = records.get(&request.id) {
            if let Some(metrics) = self.metrics.get() {
                metrics.add(
                    crate::observability::Counter::Requests,
                    "admission",
                    if record.fingerprint == fingerprint {
                        "duplicate"
                    } else {
                        "conflict"
                    },
                    1,
                );
            }
            anyhow::ensure!(
                record.fingerprint == fingerprint,
                "request_identity_conflict"
            );
            return Ok(Admission::Observe(record.clone()));
        }
        anyhow::ensure!(records.len() < self.policy.max_records, "request_capacity");
        // 先取得两级容量再公布记录；任何失败都不能留下半个请求或消耗业务动作。
        let count = self.node_records.clone().try_acquire_owned()?;
        let local = self
            .local_bytes
            .clone()
            .try_acquire_many_owned(RECORD_BYTES)?;
        let node = self
            .node_bytes
            .clone()
            .try_acquire_many_owned(RECORD_BYTES)?;
        let record = Arc::new(Record {
            fingerprint,
            result: Mutex::new(None),
            changed: Notify::new(),
            local_bytes: self.local_bytes.clone(),
            node_bytes: self.node_bytes.clone(),
            retention: Duration::from_millis(self.policy.retention_ms),
            _capacity: (count, local, node),
            metrics: self.metrics.get().cloned(),
            _observed: self.metrics.get().map(|metrics| {
                (
                    metrics.reserve("request_records", 1),
                    metrics.reserve("request_cache_bytes", u64::from(RECORD_BYTES)),
                )
            }),
        });
        records.insert(request.id.clone(), record.clone());
        if let Some(metrics) = self.metrics.get() {
            metrics.add(
                crate::observability::Counter::Requests,
                "admission",
                "accepted",
                1,
            );
        }
        Ok(Admission::Execute(Execution {
            record,
            cancellation: Some(cancelled),
        }))
    }

    /// 业务作用：在连接关闭时释放目录所有权，不取消其它仍持有真实执行责任的任务。
    /// 参数说明: 无。
    ///
    /// 返回：缓存目录清空；在途责任及等待者保留的记录仍占用实际预算直至释放。
    pub fn clear(&self) {
        self.records
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clear();
    }
}

impl RequestTable {
    /// 业务作用：把实际缓存责任登记到统一指标源，不能由请求 ID 创建动态标签。
    /// 参数说明：
    /// - `metrics`：进程内唯一产品指标实例。
    ///
    /// 返回：首次绑定生效；应在接入第一个请求前调用。
    pub fn observe(&self, metrics: Arc<crate::observability::Metrics>) {
        let _ = self.metrics.set(metrics);
    }

    /// 业务作用：在会话存活期间收回闲置的过期结果，避免只能靠新请求触发释放。
    /// 参数说明: 无。
    ///
    /// 返回：每秒最多扫描一次有限目录；在途记录不淘汰，旧结果不再改变任何授权状态。
    pub fn expire(&self) {
        let elapsed = self.started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64;
        let next = self.next_sweep.load(Ordering::Relaxed);
        if elapsed < next
            || self
                .next_sweep
                .compare_exchange(
                    next,
                    elapsed.saturating_add(1000),
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                )
                .is_err()
        {
            return;
        }
        self.records
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .retain(|_, record| !record.expired());
    }
}

impl Record {
    /// 业务作用：只淘汰已经结束且保留期已过的记录，慢业务动作不能因时间流逝被重复启动。
    /// 参数说明: 无。
    ///
    /// 返回：已完成记录到期为 true，Pending 始终为 false。
    fn expired(&self) -> bool {
        self.result
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .as_ref()
            .is_some_and(|result| result.expires <= Instant::now())
    }

    /// 业务作用：等待原请求完成，不重新执行订阅或远程副作用。
    /// 参数说明: 无。
    ///
    /// 返回：原控制结果和共享正文；取消等待不会影响原执行责任。
    pub async fn wait(&self) -> Reply {
        loop {
            let notified = self.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if let Some(result) = self
                .result
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .as_ref()
            {
                return result.reply.clone();
            }
            notified.await;
        }
    }

    /// 业务作用：一次发布业务结果并唤醒等待者，缓存不足不改变已经发生的副作用层级。
    /// 参数说明：
    /// - `reply`：执行完成时的唯一控制结果和可选共享正文。
    ///
    /// 返回：全部调用者使用同一保留结果；正文预算不足时保留 effect 并明确省略正文。
    fn complete(&self, mut reply: Reply) -> Reply {
        let mut state = self
            .result
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if let Some(result) = state.as_ref() {
            return result.reply.clone();
        }
        let capacity = reply.body.as_ref().and_then(|body| {
            let size = u32::try_from(body.len()).ok()?;
            let local = self.local_bytes.clone().try_acquire_many_owned(size).ok()?;
            let node = self.node_bytes.clone().try_acquire_many_owned(size).ok()?;
            Some((local, node))
        });
        if reply.body.is_some() && capacity.is_none() {
            // 已完成的业务不因缓存不足自动重做；保留原 effect，客户端按 operationId 查询业务结果。
            reply.body = None;
            reply.metadata.schema = None;
            if let Some(result) = reply.metadata.result.as_mut() {
                result.code = "operation_result_unavailable".into();
                result.retryable = false;
            }
            if let Some(metrics) = &self.metrics {
                metrics.add(
                    crate::observability::Counter::Requests,
                    "result",
                    "dropped",
                    1,
                );
            }
        }
        let observed = self
            .metrics
            .as_ref()
            .zip(reply.body.as_ref())
            .map(|(metrics, body)| metrics.reserve("request_cache_bytes", body.len() as u64));
        *state = Some(Completed {
            reply: reply.clone(),
            expires: Instant::now() + self.retention,
            _body_capacity: capacity,
            _body_observed: observed,
        });
        drop(state);
        self.changed.notify_waiters();
        reply
    }
}

impl Execution {
    /// 业务作用：结束唯一执行责任并发布结果，不复制底层业务正文。
    /// 参数说明：
    /// - `reply`：当前执行的最终控制结果。
    ///
    /// 返回：与所有重复请求相同的可发送结果；不会再次应用其中描述的本地状态。
    pub fn complete(mut self, reply: Reply) -> Reply {
        self.cancellation = None;
        self.record.complete(reply)
    }
}

impl Drop for Execution {
    /// 业务作用：任务取消或提前返回时结束 Pending，保留结果未知事实以阻止重复执行。
    /// 参数说明: 无。
    ///
    /// 返回：等待者被唤醒；目录保留期限内相同请求不能获得新的执行责任。
    fn drop(&mut self) {
        if let Some(reply) = self.cancellation.take() {
            self.record.complete(reply);
        }
    }
}
