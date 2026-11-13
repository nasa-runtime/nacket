use crate::{
    config::admission::AdmissionPolicy,
    observability::{Counter, Metrics, Reservation},
};
use std::{
    collections::BTreeMap,
    net::IpAddr,
    sync::{Arc, Mutex},
};
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore},
    time::{Duration, Instant},
};

struct Peer {
    /// 当前真实来源一秒准入窗口的单调起点，断连不刷新窗口。
    started: Instant,
    /// 该窗口内已接纳的物理连接次数，用于限制反复重连。
    attempts: u32,
    /// 当前来源仍由实际 I/O owner 持有的物理连接数。
    live: usize,
}

struct Peers {
    /// 以真实 TCP 对端 IP 为键的有限准入目录，不读取代理声明。
    entries: BTreeMap<IpAddr, Peer>,
    /// 最近一次空闲来源回收的单调时刻，避免每次连接遍历全部来源。
    reaped: Instant,
}

/// 所有主监听和派生监听共享的接入预算，真实 TCP 来源不能由转发头覆盖。
pub struct Admissions {
    /// 进程启动时冻结的节点容量和来源速率约束。
    policy: AdmissionPolicy,
    /// 所有业务端口共用的物理连接名额池。
    physical: Arc<Semaphore>,
    /// 尚未完成首个 Ready 的物理连接名额池。
    unauthenticated: Arc<Semaphore>,
    /// 来源窗口和并存数量的同一短临界区，保证检查与占用原子完成。
    peers: Mutex<Peers>,
    /// 节点固定准入指标源，不把来源 IP 放入标签。
    metrics: Arc<Metrics>,
}

/// 物理连接及其 I/O 子任务共同持有容量，只有全部 owner 结束才归还实际连接名额。
pub struct Admission {
    /// 负责归还来源计数的共享准入目录，随连接任务存活。
    owner: Arc<Admissions>,
    /// 实际接入的来源 IP，用于精确归还该来源的并存计数。
    peer: IpAddr,
    /// 由全部 I/O owner 共同持有的节点物理容量，最后释放时归还。
    _physical: OwnedSemaphorePermit,
    /// 首个 Ready 前保留的未认证许可和观测责任，只能取出一次。
    pending: Mutex<Option<(OwnedSemaphorePermit, Reservation)>>,
    /// 物理连接观测责任，存活范围与实际准入许可一致。
    _observed: Reservation,
}

impl Admissions {
    /// 业务作用：在任何端口开始接流之前建立全节点物理连接、未认证和来源容量。
    /// 参数说明：
    /// - `policy`：启动时已经校验的节点硬上限。
    /// - `metrics`：统一有限指标目录。
    ///
    /// 返回：全部产品监听器共享的唯一准入控制器。
    pub fn new(policy: AdmissionPolicy, metrics: Arc<Metrics>) -> Arc<Self> {
        Arc::new(Self {
            physical: Arc::new(Semaphore::new(policy.max_physical_connections)),
            unauthenticated: Arc::new(Semaphore::new(policy.max_unauthenticated)),
            peers: Mutex::new(Peers {
                entries: BTreeMap::new(),
                reaped: Instant::now(),
            }),
            policy,
            metrics,
        })
    }

    /// 业务作用：在读取协议和身份材料之前取得节点准入责任，独立端口不能绕开全节点上限。
    /// 参数说明：
    /// - `peer`：实际 TCP 对端 IP，不读取 Forwarded 或客户端声明。
    ///
    /// 返回：有限连接责任；任何容量或固定一秒来源窗口超限均立即关闭新接入。
    pub fn acquire(self: &Arc<Self>, peer: IpAddr) -> anyhow::Result<Arc<Admission>> {
        let physical = self
            .physical
            .clone()
            .try_acquire_owned()
            .map_err(|_| self.reject("physical", "physical_connection_capacity"))?;
        let pending = self
            .unauthenticated
            .clone()
            .try_acquire_owned()
            .map_err(|_| self.reject("unauthenticated", "unauthenticated_capacity"))?;
        let now = Instant::now();
        let mut peers = self.peers.lock().unwrap_or_else(|error| error.into_inner());
        // 空闲且窗口已经结束的来源才可回收，不能通过快速断连重建条目来绕过速率上限。
        if now.duration_since(peers.reaped) >= Duration::from_secs(1) {
            peers.entries.retain(|_, entry| {
                entry.live > 0 || now.duration_since(entry.started) < Duration::from_secs(1)
            });
            peers.reaped = now;
        }
        let peers = &mut peers.entries;
        if !peers.contains_key(&peer) && peers.len() >= self.policy.max_tracked_peers {
            return Err(self.reject("peer_directory", "peer_directory_capacity"));
        }
        let entry = peers.entry(peer).or_insert(Peer {
            started: now,
            attempts: 0,
            live: 0,
        });
        if now.duration_since(entry.started) >= Duration::from_secs(1) {
            entry.started = now;
            entry.attempts = 0;
        }
        if entry.live >= self.policy.max_connections_per_peer {
            return Err(self.reject("peer_connections", "peer_connection_capacity"));
        }
        if entry.attempts >= self.policy.connections_per_second_per_peer {
            return Err(self.reject("peer_rate", "peer_admission_rate"));
        }
        entry.attempts += 1;
        entry.live += 1;
        self.metrics
            .add(Counter::Admissions, "physical", "accepted", 1);
        Ok(Arc::new(Admission {
            owner: self.clone(),
            peer,
            _physical: physical,
            pending: Mutex::new(Some((
                pending,
                self.metrics.reserve("unauthenticated_connections", 1),
            ))),
            _observed: self.metrics.reserve("physical_connections", 1),
        }))
    }

    /// 业务作用：记录封闭准入拒绝类别，不把来源 IP、凭据或端口加入指标标签。
    /// 参数说明：
    /// - `scope`：静态目录中的预算类别。
    /// - `code`：固定拒绝原因。
    ///
    /// 返回：可供接入层结束连接的错误，不改变既有连接权限。
    fn reject(&self, scope: &'static str, code: &'static str) -> anyhow::Error {
        self.metrics.add(Counter::Admissions, scope, "rejected", 1);
        anyhow::anyhow!(code)
    }
}

impl Admission {
    /// 业务作用：首个路由会话完成 Ready 后归还未认证容量，后续 namespace 不重复归还。
    /// 参数说明: 无。
    ///
    /// 返回：只解除未认证责任；物理连接和来源计数继续由全部 I/O owner 持有。
    pub fn authenticated(&self) {
        self.pending
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take();
    }
}

impl Drop for Admission {
    /// 业务作用：最后一个物理 I/O owner 退出后归还来源计数，快速断连仍保留当前窗口的速率记录。
    /// 参数说明: 无。
    ///
    /// 返回：物理与未认证许可随真实责任释放；其它连接不受影响。
    fn drop(&mut self) {
        let mut peers = self
            .owner
            .peers
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if let Some(entry) = peers.entries.get_mut(&self.peer) {
            entry.live = entry.live.saturating_sub(1);
        }
    }
}
