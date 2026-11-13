//! 只读订阅判定的有界复用与查询合并；资格期限和撤权代际不会因命中缓存刷新。

use super::Connected;
use crate::{
    domain::{route::cache::CachePolicy, session::GroupGrant},
    observability::{Counter, Metrics, Reservation},
};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    sync::{mpsc, Notify, OwnedSemaphorePermit, Semaphore},
    task::JoinSet,
    time::Instant,
};

/// 只有已经核对响应关联的明确拒绝和完整目标资格可以复用。
#[derive(Clone)]
pub enum Decision {
    /// 业务明确拒绝，网络错误和未知响应不属于此状态。
    Denied,
    /// 原目标资格，所有单调截止点保持首次验证时的值。
    Allowed(BTreeMap<String, GroupGrant>),
}

/// 节点共享的长期缓存容量，旧等待者持有的条目仍占用预算。
pub struct Budget {
    /// 全节点条目数额度，待决查询与已完成缓存共用。
    entries: Arc<Semaphore>,
    /// 全节点保守缓存字节额度。
    bytes: Arc<Semaphore>,
    /// 固定节点指标，不以 UID 或订阅目标扩张标签。
    metrics: Arc<Metrics>,
    /// 共享查询由独立有界队列承接，单个等待者取消不撤销其它等待者的查询。
    sender: mpsc::Sender<Work>,
    /// 生命周期安装前唯一的接收责任。
    receiver: Mutex<Option<mpsc::Receiver<Work>>>,
    /// 同时执行的远程查询上限，仍受 HTTP 全局预算约束。
    parallel: usize,
}

impl Budget {
    /// 业务作用：按部署硬上限创建唯一授权缓存预算。
    /// 参数说明：
    /// - `policy`：已通过校验的全节点 HTTP 与缓存资源策略。
    /// - `metrics`：统一产品指标源。
    ///
    /// 返回：尚未占用任何条目或字节的共享预算。
    pub fn new(
        policy: &crate::config::remote_calls::RemoteCalls,
        metrics: Arc<Metrics>,
    ) -> Arc<Self> {
        let (sender, receiver) = mpsc::channel(policy.max_authorization_cache_entries);
        Arc::new(Self {
            entries: Arc::new(Semaphore::new(policy.max_authorization_cache_entries)),
            bytes: Arc::new(Semaphore::new(policy.max_authorization_cache_bytes)),
            metrics,
            sender,
            receiver: Mutex::new(Some(receiver)),
            parallel: policy.max_in_flight,
        })
    }

    /// 业务作用：在应用生命周期内接管共享只读查询，隔离各等待者的取消责任。
    /// 参数说明：
    /// - `application`：负责启动和关闭工作队列的统一应用。
    ///
    /// 返回：队列成功安装；重复安装拒绝，关闭时取消剩余查询并唤醒等待者。
    pub async fn install(&self, application: &nasa::Application) -> anyhow::Result<()> {
        let mut receiver = self
            .receiver
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take()
            .ok_or_else(|| anyhow::anyhow!("authorization_cache_already_installed"))?;
        let parallel = self.parallel;
        application
            .serve_when_ready("socket-authorization-cache", move |shutdown| async move {
                let mut tasks = JoinSet::new();
                loop {
                    tokio::select! {
                        biased;
                        _ = shutdown.cancelled() => break,
                        Some(_) = tasks.join_next(), if !tasks.is_empty() => {},
                        work = receiver.recv(), if tasks.len() < parallel => {
                            match work {
                                Some(work) => {
                                    tasks.spawn(work.run());
                                }
                                None => break,
                            }
                        }
                    }
                }
                // 停止接收后撤销剩余查询责任；所有等待者收到失败，不留下跨停机的判定。
                receiver.close();
                tasks.abort_all();
                while tasks.join_next().await.is_some() {}
                while receiver.try_recv().is_ok() {}
                Ok(())
            })
            .await?;
        Ok(())
    }
}

enum Status {
    Pending,
    Ready(Decision, Instant),
    Failed,
}

struct Entry {
    /// 当前唯一查询责任或已经完成的有限判定。
    status: Mutex<Status>,
    /// 发布完成或取消事实的通知，等待者查询状态后再决定是否复用。
    changed: Notify,
    /// 创建查询时的撤权代际，变化后立即停止逻辑复用。
    generation: u64,
    /// 在途查询的原始截止点，不因等待者加入而延长。
    deadline: Instant,
    /// 会话及节点的条目、字节实际责任。
    _capacity: [OwnedSemaphorePermit; 4],
    /// 条目保留数观察责任。
    _observed_entries: Reservation,
    /// 保守字节保留观察责任。
    _observed_bytes: Reservation,
}

struct Fetch {
    /// 唯一发起 HTTP 的责任，取消时必须唤醒合并等待者。
    entry: Arc<Entry>,
}

struct Work {
    /// 查询所属精确会话，断连后不得继续形成授权结论。
    connection: Arc<Connected>,
    /// 查询创建时冻结的缓存期限。
    policy: CachePolicy,
    /// 原身份的截止点，不能因共享等待延长。
    identity_deadline: Instant,
    /// 已捕获有限参数的只读查询。
    future: Pin<Box<dyn Future<Output = anyhow::Result<Decision>> + Send>>,
    /// 无论完成、取消还是异常都负责通知等待者。
    responsibility: Fetch,
}

impl Work {
    /// 业务作用：在原查询期限内获取判定，复验会话后发布给所有独立等待者。
    /// 参数说明: 无。
    ///
    /// 返回：合法判定成为有限缓存，失败由责任释放通知等待者且不形成负缓存。
    async fn run(self) -> anyhow::Result<()> {
        let entry = &self.responsibility.entry;
        let decision = tokio::select! {
            biased;
            _ = self.connection.cancellation.cancelled() => anyhow::bail!("subscription_closed"),
            result = tokio::time::timeout_at(entry.deadline, self.future) => result??,
        };
        // 撤权或连接关闭先于判定发布时，旧授权结论必须丢弃。
        Cache::validate(&self.connection, entry.generation, &decision)?;
        let ttl = match &decision {
            Decision::Denied => self.policy.negative_ttl_ms,
            Decision::Allowed(_) => self.policy.positive_ttl_ms,
        };
        let mut expires = self
            .identity_deadline
            .min(Instant::now() + Duration::from_millis(ttl));
        if let Decision::Allowed(grants) = &decision {
            for grant in grants.values() {
                expires = expires.min(grant.deadline);
            }
        }
        *entry
            .status
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = Status::Ready(decision, expires);
        Ok(())
    }
}

impl Drop for Fetch {
    /// 业务作用：把唯一查询的取消发布为不可复用状态，避免等待者永久等待。
    /// 参数说明: 无。
    ///
    /// 返回：仍在 Pending 的责任转为 Failed，既有已完成判定保持原期限。
    fn drop(&mut self) {
        let mut state = self
            .entry
            .status
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if matches!(*state, Status::Pending) {
            *state = Status::Failed;
        }
        drop(state);
        self.entry.changed.notify_waiters();
    }
}

/// 缓存归属于精确会话，重连不继承原会话资格或待决查询。
pub struct Cache {
    /// 当前冻结定义中的缓存策略。
    policy: CachePolicy,
    /// 每会话条目上限，条目移出目录后仍被等待者持有时不提前归还。
    entries: Arc<Semaphore>,
    /// 每会话字节上限。
    bytes: Arc<Semaphore>,
    /// 节点共享容量与观测责任。
    budget: Arc<Budget>,
    /// 有界查询键目录，键包含完整会话、端点、权限代际与目标集合。
    records: Mutex<BTreeMap<[u8; 32], Arc<Entry>>>,
    /// 限制常规扫描频率，撤权代际变化仍立即触发清理。
    maintenance: Mutex<(Instant, u64)>,
}

impl Cache {
    /// 业务作用：为会话建立空缓存目录，关闭缓存时不会分配条目容量。
    /// 参数说明：
    /// - `policy`：路由冻结策略。
    /// - `budget`：全节点共享硬上限。
    ///
    /// 返回：尚无查询或判定的会话缓存。
    pub fn new(policy: CachePolicy, budget: Arc<Budget>) -> Self {
        Self {
            entries: Arc::new(Semaphore::new(policy.max_entries)),
            bytes: Arc::new(Semaphore::new(policy.max_bytes)),
            policy,
            budget,
            records: Mutex::new(BTreeMap::new()),
            maintenance: Mutex::new((Instant::now(), 0)),
        }
    }

    /// 业务作用：限制定期清理开销，同时在撤权变化时立即移除旧目录引用。
    /// 参数说明：
    /// - `generation`：当前撤权投影代际。
    ///
    /// 返回：至多每秒执行一次常规清理；不改变现有资格的截止点。
    pub fn maintain(&self, generation: u64) {
        let mut last = self
            .maintenance
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let now = Instant::now();
        if generation == last.1 && now.duration_since(last.0) < Duration::from_secs(1) {
            return;
        }
        *last = (now, generation);
        drop(last);
        self.expire(generation);
    }

    /// 业务作用：移除已失效、失败或撤权代际不同的条目，仍在等待的 owner 继续持有额度。
    /// 参数说明：
    /// - `generation`：当前路由撤权投影的本地代际。
    ///
    /// 返回：释放目录中的过期引用，不更改业务资格或请求结果。
    pub fn expire(&self, generation: u64) {
        if !self.policy.enabled {
            return;
        }
        let now = Instant::now();
        self.records
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .retain(|_, entry| {
                entry.generation == generation
                    && match &*entry
                        .status
                        .lock()
                        .unwrap_or_else(|error| error.into_inner())
                    {
                        Status::Pending => entry.deadline > now,
                        Status::Ready(_, deadline) => *deadline > now,
                        Status::Failed => false,
                    }
            });
    }

    /// 业务作用：取得双层条目和字节预算，缓存饱和时允许原只读请求直接走远端。
    /// 参数说明：
    /// - `bytes`：本目标集合的保守最大缓存开销。
    /// - `generation`：当前撤权代际。
    /// - `deadline`：原始查询截止点。
    ///
    /// 返回：全部预约成功的待决条目，任一预约失败自动归还已取得额度。
    fn reserve(
        &self,
        bytes: usize,
        generation: u64,
        deadline: Instant,
    ) -> anyhow::Result<Arc<Entry>> {
        let amount = u32::try_from(bytes)?;
        Ok(Arc::new(Entry {
            status: Mutex::new(Status::Pending),
            changed: Notify::new(),
            generation,
            deadline,
            _capacity: [
                self.entries.clone().try_acquire_owned()?,
                self.bytes.clone().try_acquire_many_owned(amount)?,
                self.budget.entries.clone().try_acquire_owned()?,
                self.budget.bytes.clone().try_acquire_many_owned(amount)?,
            ],
            _observed_entries: self
                .budget
                .metrics
                .reserve("authorization_cache_entries", 1),
            _observed_bytes: self
                .budget
                .metrics
                .reserve("authorization_cache_bytes", bytes as u64),
        }))
    }

    /// 业务作用：仅合并同一精确会话的等价只读订阅查询，命中后仍复验当前门禁。
    /// 参数说明：
    /// - `connection`：原会话及固定路由、身份、握手投影。
    /// - `groups`：本次显式申请的有限目标集合。
    /// - `endpoint`：冻结的业务授权端点。
    /// - `fetch`：只在缓存未命中且取得查询责任时执行的远程判定。
    ///
    /// 返回：原始期限不变的明确判定；取消、超时、撤权或网络错误不进入负缓存。
    pub async fn get_or_fetch<F>(
        &self,
        connection: &Arc<Connected>,
        groups: &[String],
        endpoint: &crate::config::RemoteEndpoint,
        fetch: F,
    ) -> anyhow::Result<Decision>
    where
        F: Future<Output = anyhow::Result<Decision>> + Send + 'static,
    {
        if !self.policy.enabled {
            return fetch.await;
        }
        let identity = connection.session.identity();
        let generation = connection.session.authorization_generation();
        self.expire(generation);
        let mut ordered = groups.iter().collect::<Vec<_>>();
        ordered.sort();
        let key: [u8; 32] = Sha256::digest(serde_json::to_vec(&(
            "subscribe",
            &connection.session.key,
            &identity.tenant,
            &identity.uid,
            &identity.authorization_id,
            identity.version,
            generation,
            endpoint,
            &ordered,
            &connection.facts,
        ))?)
        .into();
        let deadline = identity
            .deadline
            .min(Instant::now() + Duration::from_millis(endpoint.timeout_ms));
        let bytes = ordered.iter().map(|group| group.len() + 256).sum::<usize>() * 4 + 1024;
        let (entry, leader) = {
            let mut records = self
                .records
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if let Some(entry) = records.get(&key) {
                (Some(entry.clone()), false)
            } else {
                match self.reserve(bytes, generation, deadline) {
                    Ok(entry) => {
                        records.insert(key, entry.clone());
                        (Some(entry), true)
                    }
                    Err(_) => (None, false),
                }
            }
        };
        let Some(entry) = entry else {
            self.budget
                .metrics
                .add(Counter::AuthorizationCache, "bypass", "ok", 1);
            return fetch.await;
        };
        let mut fetch = Some(fetch);
        if leader {
            self.budget
                .metrics
                .add(Counter::AuthorizationCache, "miss", "ok", 1);
            let work = Work {
                connection: connection.clone(),
                policy: self.policy.clone(),
                identity_deadline: identity.deadline,
                future: Box::pin(fetch.take().expect("unique query")),
                responsibility: Fetch {
                    entry: entry.clone(),
                },
            };
            self.budget
                .sender
                .try_send(work)
                .map_err(|_| anyhow::anyhow!("authorization_cache_unavailable"))?;
        }
        let notified = entry.changed.notified();
        tokio::pin!(notified);
        loop {
            notified.as_mut().enable();
            let value = {
                let state = entry
                    .status
                    .lock()
                    .unwrap_or_else(|error| error.into_inner());
                match &*state {
                    Status::Ready(value, expires) if *expires > Instant::now() => {
                        Some(value.clone())
                    }
                    Status::Pending => None,
                    _ => anyhow::bail!("authorization_cache_unavailable"),
                }
            };
            if let Some(value) = value {
                Self::validate(connection, generation, &value)?;
                self.budget.metrics.add(
                    Counter::AuthorizationCache,
                    if leader {
                        "fetched"
                    } else {
                        "hit"
                    },
                    "ok",
                    1,
                );
                return Ok(value);
            }
            if !leader && !self.policy.coalesce {
                self.budget
                    .metrics
                    .add(Counter::AuthorizationCache, "bypass", "ok", 1);
                return fetch.take().expect("independent query").await;
            }
            self.budget
                .metrics
                .add(Counter::AuthorizationCache, "wait", "ok", 1);
            tokio::select! {
                biased;
                _ = connection.cancellation.cancelled() => anyhow::bail!("subscription_closed"),
                result = tokio::time::timeout_at(deadline, notified.as_mut()) => {
                    result?;
                }
            }
            notified.set(entry.changed.notified());
        }
    }

    /// 业务作用：在返回缓存判定前复验本会话与共享撤权下界，后续 UID 应用仍检查意图。
    /// 参数说明：
    /// - `connection`：当前等待者所属会话。
    /// - `generation`：查询或缓存键建立时的撤权代际。
    /// - `decision`：不延长原期限的判定。
    ///
    /// 返回：当前仍可复用；任何保护状态、取消或旧资格均拒绝。
    fn validate(
        connection: &Connected,
        generation: u64,
        decision: &Decision,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(
            !connection.cancellation.is_cancelled()
                && connection.session.candidate(None).is_some()
                && connection.session.authorization_generation() == generation,
            "authorization_cache_stale"
        );
        if let Decision::Allowed(grants) = decision {
            anyhow::ensure!(
                connection.session.grants_allowed(grants),
                "authorization_cache_stale"
            );
        }
        Ok(())
    }
}
