/// 节点内连接名额与替换屏障，不冒充集群范围的配额权威。
pub mod local;

use crate::domain::{
    control::{
        quota::{reservation_id, ReservationPhase},
        Command, Ledger, Reply,
    },
    session::Session,
};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};
use tokio::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;

const MAXIMUM_LOCAL: usize = 16384;

#[derive(Default)]
struct Outcome {
    /// 权威已确认此精确名额可接流的事实，申请受理不等于 active。
    active: bool,
    /// 该本地名额责任已经关闭的事实，迟到允许不能重新激活会话。
    closed: bool,
    /// 权威明确拒绝的有限错误信息，未知提交结果保持待决责任。
    error: Option<String>,
}

struct Entry {
    /// 本次精确集群名额申请 ID，重试不创建第二份申请。
    id: String,
    /// 名额绑定的真实会话，用于复验存活与旧 writer 排空。
    session: Arc<Session>,
    /// 名额被撤回或拒绝后取消对应会话的信号。
    cancellation: CancellationToken,
    /// 名额激活、拒绝和关闭事实的同一短临界区。
    outcome: Mutex<Outcome>,
    /// 尚未归还的本地集群名额责任数量观测。
    _observed: crate::observability::Reservation,
}

#[derive(Default)]
struct State {
    /// 当前节点代际内的下一名额序号，归还高水位阻止迟到申请复活。
    next: u64,
    /// 序号到精确名额责任的有限目录，未知结果不能提前移除。
    entries: BTreeMap<u64, Arc<Entry>>,
}

/// 已申请的连接名额责任，满足对应范围的排空屏障后才允许初始化。
pub enum Admission {
    /// 集群权威确认的名额，取消后仍保留未知提交与归还责任。
    Cluster(
        /// 由持久权威确认激活和归还的集群名额责任。
        Lease,
    ),
    /// 本节点裁决的名额，不约束其它节点上的同主体连接。
    Node(
        /// 由本节点持有并核对旧 writer 排空的名额责任。
        local::LocalLease,
    ),
}

impl Admission {
    /// 业务作用：统一等待当前配额范围的精确写出屏障，传输适配器不能绕过准入策略。
    /// 参数说明：
    /// - `deadline`：接入时固定的总认证期限。
    ///
    /// 返回：名额开放且旧责任已结束；任一失败阻止初始化及 Ready。
    pub async fn wait(&self, deadline: Instant) -> anyhow::Result<()> {
        match self {
            Self::Cluster(lease) => lease.wait(deadline).await,
            Self::Node(lease) => lease.wait(deadline).await,
        }
    }
}

/// 本地目录保留未知提交的真实责任，名额只由持久控制回复开放。
#[derive(Default)]
pub struct Quotas {
    /// 集群名额本地序号分配及责任目录的同一临界区。
    state: Mutex<State>,
}

/// 连接持有的准入责任；取消不直接删除权威名额。
pub struct Lease {
    /// 连接持有的集群名额 owner，释放时触发归还而非直接假定权威已回收。
    entry: Arc<Entry>,
}

impl Entry {
    /// 业务作用：先关闭会话最终写出资格，再请求网络任务结束，防止取消被解释为已经排空。
    /// 参数说明: 无。
    ///
    /// 返回：名额仍留在协调目录，等待实际 writer 结束及持久归还。
    fn close(&self) {
        self.outcome
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .closed = true;
        self.session.close();
        self.cancellation.cancel();
    }
}

impl Quotas {
    /// 业务作用：在确认阶段登记有界准入意图，尚未提交或正在替换的连接不能进入 Ready。
    /// 参数说明：
    /// - `session`：已认证但尚未初始化的精确会话。
    /// - `cancellation`：当前路由会话的取消域。
    /// - `metrics`：节点统一事实源。
    ///
    /// 返回：连接必须持有的名额责任；容量或序号耗尽拒绝新申请。
    pub fn register(
        &self,
        session: Arc<Session>,
        cancellation: CancellationToken,
        metrics: &Arc<crate::observability::Metrics>,
    ) -> anyhow::Result<Lease> {
        let id = reservation_id(&session.key)?;
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        anyhow::ensure!(state.entries.len() < MAXIMUM_LOCAL, "local_quota_capacity");
        let ordinal = state
            .next
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("quota_sequence_exhausted"))?;
        state.next = ordinal;
        let entry = Arc::new(Entry {
            id,
            session,
            cancellation,
            outcome: Mutex::new(Outcome::default()),
            _observed: metrics.reserve("connection_reservations", 1),
        });
        state.entries.insert(ordinal, entry.clone());
        Ok(Lease {
            entry,
        })
    }

    /// 业务作用：按已确认快照撤销旧会话并构造有界准入/归还批次，不在续期循环等待 writer。
    /// 参数说明：
    /// - `snapshot`：同一控制作用域的完整已提交账本。
    /// - `node_id`：当前节点标识，代际已经由控制协调器验证。
    ///
    /// 返回：至多 32 条按本节点准入序号排序的动作；未排空会话不会提前归还。
    pub fn reconcile(&self, snapshot: &Ledger, node_id: &str) -> Vec<Command> {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let mut commands = Vec::new();
        let mut completed = Vec::new();
        for (ordinal, entry) in &state.entries {
            if entry.cancellation.is_cancelled() || !entry.session.live() {
                entry.close();
            }
            if let Some(record) = snapshot.reservations.get(&entry.id) {
                if record.phase == ReservationPhase::Revoking {
                    // 已提交替换先关闭旧会话；只有真实写出结束后的 Release 才能解除新会话屏障。
                    entry.close();
                } else if record.phase == ReservationPhase::Active {
                    let mut outcome = entry
                        .outcome
                        .lock()
                        .unwrap_or_else(|error| error.into_inner());
                    if !outcome.closed {
                        outcome.active = true;
                    }
                }
            } else if snapshot.nodes[node_id].quota_watermark >= *ordinal {
                let mut outcome = entry
                    .outcome
                    .lock()
                    .unwrap_or_else(|error| error.into_inner());
                outcome
                    .error
                    .get_or_insert_with(|| "quota_admission_stale".into());
                drop(outcome);
                entry.close();
                if entry.session.drained() {
                    completed.push(*ordinal);
                }
                continue;
            }
            let outcome = entry
                .outcome
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if commands.len() >= 32 {
                continue;
            }
            if outcome.closed {
                if entry.session.drained() {
                    commands.push(Command::ReleaseConnection {
                        ordinal: *ordinal,
                        session_key: entry.session.key.clone(),
                    });
                }
            } else if !snapshot.reservations.contains_key(&entry.id) {
                let identity = entry.session.identity();
                commands.push(Command::ReserveConnection {
                    ordinal: *ordinal,
                    session_key: entry.session.key.clone(),
                    tenant_id: identity.tenant,
                    uid: identity.uid,
                });
            }
        }
        for ordinal in completed {
            state.entries.remove(&ordinal);
        }
        commands
    }

    /// 业务作用：接收持久批次的明确结果，未知提交保留原意图直到后续快照证明其状态。
    /// 参数说明：
    /// - `commands`：本次发送的有界节点动作。
    /// - `replies`：已验证数量和顺序的持久回复。
    ///
    /// 返回：成功准入仅开放 Active；明确拒绝结束握手，但仍提交序号围栏防止迟到重放。
    pub fn applied(&self, commands: &[Command], replies: &[Reply]) {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        for (command, reply) in commands.iter().zip(replies) {
            let (ordinal, releasing) = match command {
                Command::ReserveConnection {
                    ordinal,
                    ..
                } => (*ordinal, false),
                Command::ReleaseConnection {
                    ordinal,
                    ..
                } => (*ordinal, true),
                _ => continue,
            };
            let Some(entry) = state.entries.get(&ordinal).cloned() else {
                continue;
            };
            match reply {
                Reply::Recorded {} if releasing => {
                    state.entries.remove(&ordinal);
                }
                Reply::ConnectionReserved {
                    reservation,
                } if !releasing => {
                    if reservation.phase == ReservationPhase::Revoking {
                        entry.close();
                    } else if reservation.phase == ReservationPhase::Active {
                        let mut outcome = entry
                            .outcome
                            .lock()
                            .unwrap_or_else(|error| error.into_inner());
                        if !outcome.closed {
                            outcome.active = true;
                        }
                    }
                }
                Reply::Rejected {
                    code,
                } if !releasing => {
                    entry
                        .outcome
                        .lock()
                        .unwrap_or_else(|error| error.into_inner())
                        .error = Some(code.clone());
                    entry.close();
                }
                _ => {}
            }
        }
    }
}

impl Lease {
    /// 业务作用：必要初始化之前等待集群名额真正开放，替换受理或本地 presence 不能充当授权。
    /// 参数说明：
    /// - `deadline`：accept 时确定的总握手期限，配额等待不会延长。
    ///
    /// 返回：Active 且会话仍有效；取消、旧责任未结束、明确拒绝或超时均不能进入初始化。
    pub async fn wait(&self, deadline: Instant) -> anyhow::Result<()> {
        loop {
            {
                let outcome = self
                    .entry
                    .outcome
                    .lock()
                    .unwrap_or_else(|error| error.into_inner());
                if let Some(error) = &outcome.error {
                    anyhow::bail!(error.clone());
                }
                anyhow::ensure!(
                    !outcome.closed && self.entry.session.live(),
                    "quota_session_closed"
                );
                if outcome.active {
                    return Ok(());
                }
            }
            anyhow::ensure!(Instant::now() < deadline, "quota_admission_timeout");
            tokio::select! {
                biased;
                _ = self.entry.cancellation.cancelled() => anyhow::bail!("quota_session_closed"),
                _ = tokio::time::sleep_until(deadline.min(Instant::now() + Duration::from_millis(25))) => {},
            }
        }
    }
}

impl Drop for Lease {
    /// 业务作用：取消的握手和已经断开的连接保留归还责任，不因本地句柄消失泄漏或提前复用名额。
    /// 参数说明: 无。
    ///
    /// 返回：会话关闭并由控制协调器等待实际排空，未知提交仍可被原序号围栏收敛。
    fn drop(&mut self) {
        self.entry.close();
    }
}
