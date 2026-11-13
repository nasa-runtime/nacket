use crate::domain::control::clock::LeaseTime;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use tokio::sync::watch;

/// 写出许可和关闭决定共享同一临界区，旧快照不能在下线后重新准入。
pub struct RouteGate {
    /// 本门禁唯一绑定的定义 revision，不能迁移到后续路由定义。
    revision: u64,
    /// 节点永久停止事实，停机无需等待磁盘或路由目录锁即可撤销旧、新门禁资格。
    stopping: Arc<AtomicBool>,
    /// 准入、实际 writer 与权威证据的统一短临界区。
    state: Mutex<GateState>,
    /// 向下线排空等待者发布变化，通知只唤醒复验，不代表屏障已经完成。
    changes: watch::Sender<u64>,
}

struct GateState {
    /// 本定义是否已经允许新的写出准入，仍需同时满足租约和依赖证据。
    open: bool,
    /// 实际持有 WritePermit 的 writer 数量，取消等待者不能提前减去真实责任。
    writing: usize,
    /// 集群模式的有限控制许可时间；本地模式不借此声明集群权威。
    lease: Option<LeaseTime>,
    /// 是否已经完成首次激活，防止激活后替换依赖来源或重新准备同一门禁。
    activated: bool,
    /// 本门禁是否已永久关闭，迟到续期不能将它重新开放。
    closed: bool,
    /// 最多四项启动前绑定的必要证据，涵盖下行、初始化回复、撤权消费与撤权进度。
    dependencies: [Option<Arc<super::dependency::Dependency>>; 4],
}

/// 仅在持有期间允许本次业务写出；释放会推进下线屏障。
pub struct WritePermit {
    /// 本次真实写出占用的原门禁 owner，释放后才减少旧定义屏障责任。
    gate: Arc<RouteGate>,
    /// 取得许可时确定的可选本地写出截止点，后续续期不能延长该旧 writer。
    pub deadline: Option<tokio::time::Instant>,
    /// 取得写出资格时冻结的权威时间证据，用于持续核对有限有效期。
    lease: Option<LeaseTime>,
    /// 取得写出时冻结的依赖连续有效区间，恢复不复活已失效的旧批次。
    dependencies: [Option<super::dependency::DependencyPermit>; 4],
}

impl RouteGate {
    /// 业务作用：观察当前门禁的有限控制许可剩余时间，关闭、未授权或过期均不报有效寿命。
    /// 参数说明: 无。
    ///
    /// 返回：当前单调剩余时长；本地模式不产生集群许可，返回零。
    pub fn permit_remaining(&self) -> std::time::Duration {
        let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if self.stopping.load(Ordering::Acquire) || !state.open {
            return std::time::Duration::ZERO;
        }
        state
            .lease
            .and_then(|lease| lease.deadline())
            .map(|deadline| deadline.saturating_duration_since(tokio::time::Instant::now()))
            .unwrap_or_default()
    }

    /// 业务作用：提供路由停止准入的固定原因，诊断不能取得许可或改变门禁状态。
    /// 参数说明: 无。
    ///
    /// 返回：关闭、许可过期或依赖失效的原因；当前可用时返回 None。
    pub fn unavailable_reason(&self) -> Option<&'static str> {
        if self.stopping.load(Ordering::Acquire) {
            return Some("route_closed");
        }
        let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if !state.open {
            return Some(if state.closed {
                "route_closed"
            } else {
                "route_preparing"
            });
        }
        if state.lease.is_some_and(|lease| lease.deadline().is_none()) {
            return Some("route_permit_expired");
        }
        state
            .dependencies
            .iter()
            .flatten()
            .map(|dependency| dependency.status())
            .find(|status| *status != "ready")
    }
    /// 业务作用：读取准入与写出责任的观察快照，不通过观察取得写权限。
    /// 参数说明: 无。
    ///
    /// 返回：当前门禁分类和真实 writer 数量；依赖或许可到期按 protected 报告。
    pub fn observation(&self) -> (&'static str, usize) {
        let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let available = !self.stopping.load(Ordering::Acquire)
            && state.open
            && state.lease.is_none_or(|lease| lease.deadline().is_some())
            && state
                .dependencies
                .iter()
                .flatten()
                .all(|dependency| dependency.status() == "ready");
        (
            if available {
                "online"
            } else {
                "protected"
            },
            state.writing,
        )
    }
    /// 业务作用：为一个已验证定义建立尚未开放的路由门禁。
    /// 参数说明：
    ///
    /// - `revision`：此门禁唯一所属的权威路由代次。
    ///
    /// 返回：关闭状态的共享门禁，未激活前不接受任何写出。
    pub fn new(revision: u64) -> Arc<Self> {
        Self::new_scoped(revision, Arc::new(AtomicBool::new(false)))
    }

    /// 业务作用：将新路由绑定节点终止事实，停机不需要等待数据卷或目录锁才能撤权。
    /// 参数说明：
    /// - `revision`：本门禁唯一的定义代次。
    /// - `stopping`：节点永久终止标志，只能从 false 变为 true。
    ///
    /// 返回：共享终止域中的关闭门禁；终止后任何旧或迟到门禁都不能准入。
    pub fn new_scoped(revision: u64, stopping: Arc<AtomicBool>) -> Arc<Self> {
        let (changes, _) = watch::channel(0);
        Arc::new(Self {
            revision,
            stopping,
            state: Mutex::new(GateState {
                open: false,
                writing: 0,
                lease: None,
                activated: false,
                closed: false,
                dependencies: [None, None, None, None],
            }),
            changes,
        })
    }

    /// 业务作用：在路由发布前绑定必要 MQ 证据，控制许可不能绕过业务依赖不可用状态。
    /// 参数说明：
    /// - `dependency`：同一固定入口共享的可用性门禁。
    ///
    /// 返回：最多四个必要依赖已绑定；同一来源重复绑定幂等，门禁开放后禁止更换权限依据。
    pub fn require(&self, dependency: Arc<super::dependency::Dependency>) -> anyhow::Result<()> {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        anyhow::ensure!(
            !self.stopping.load(Ordering::Acquire) && !state.activated && !state.closed,
            "route_dependency_already_bound"
        );
        if state
            .dependencies
            .iter()
            .flatten()
            .any(|bound| Arc::ptr_eq(bound, &dependency))
        {
            return Ok(());
        }
        let slot = state
            .dependencies
            .iter_mut()
            .find(|slot| slot.is_none())
            .ok_or_else(|| anyhow::anyhow!("route_dependency_capacity"))?;
        *slot = Some(dependency);
        Ok(())
    }

    /// 业务作用：控制协调单独检查路由许可，不因 MQ 暂时不可用而伪造已排空回执。
    /// 参数说明: 无。
    ///
    /// 返回：本地路由权威仍有效时为 true，业务准入还必须通过 acquire。
    pub fn authority_valid(&self) -> bool {
        let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        !self.stopping.load(Ordering::Acquire)
            && state.open
            && state.lease.is_none_or(|lease| lease.deadline().is_some())
    }

    /// 业务作用：在外部控制屏障完成后发布当前定义的接入资格。
    /// 参数说明：无。
    ///
    /// 返回：门禁首次开放成功；关闭后的同一门禁不得重新使用。
    pub fn activate(&self) -> anyhow::Result<()> {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        anyhow::ensure!(
            !self.stopping.load(Ordering::Acquire) && !state.activated && !state.closed,
            "route gate cannot be reactivated"
        );
        // 只有新门禁可开放，撤权后的旧 Arc 永远失去控制资格。
        state.open = true;
        state.activated = true;
        self.changes.send_replace(1);
        Ok(())
    }

    /// 业务作用：仅凭本次精确权威响应安装或续期有限写出许可，失效门禁不能复活旧会话。
    /// 参数说明：
    /// - `lease`：从请求发送时刻计算且已经扣除误差余量的许可。
    ///
    /// 返回：当前门禁仍可安全启用；已关闭或曾过期时拒绝，必须建立新会话门禁。
    pub fn authorize(&self, lease: LeaseTime) -> anyhow::Result<()> {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if state
            .lease
            .is_some_and(|previous| previous.deadline().is_none())
        {
            // 到期后的旧 Arc 永远失权，新的续期响应不能重新授权尚未退出的旧会话。
            state.open = false;
            state.closed = true;
        }
        anyhow::ensure!(
            !self.stopping.load(Ordering::Acquire) && !state.closed && lease.deadline().is_some(),
            "route_permit_expired"
        );
        anyhow::ensure!(
            !state.activated || state.lease.is_some(),
            "route_authority_mismatch"
        );
        state.lease = Some(lease);
        state.activated = true;
        state.open = true;
        self.changes
            .send_modify(|epoch| *epoch = epoch.saturating_add(1));
        Ok(())
    }

    /// 业务作用：在最终写出前同时复验代次和路由关闭状态。
    /// 参数说明：
    ///
    /// - `revision`：待发送任务携带的原路由代次。
    ///
    /// 返回：成功取得在途许可；过期或关闭时返回 None 且不计入写出。
    pub fn acquire(self: &Arc<Self>, revision: u64) -> Option<WritePermit> {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        // 代次与开放状态必须在同一临界区复验，旧任务不得借持有的 Arc 越过撤权。
        if self.stopping.load(Ordering::Acquire) || revision != self.revision || !state.open {
            return None;
        }
        let mut dependencies = [None, None, None, None];
        for (permit, required) in dependencies.iter_mut().zip(&state.dependencies) {
            if let Some(required) = required {
                *permit = Some(required.acquire()?);
            }
        }
        let deadline = match state.lease {
            Some(lease) => match lease.deadline() {
                Some(deadline) => Some(deadline),
                None => {
                    // 定时维护可能尚未执行，最终准入点仍须同步撤权并通知关闭责任。
                    state.open = false;
                    state.closed = true;
                    self.changes
                        .send_modify(|epoch| *epoch = epoch.saturating_add(1));
                    return None;
                }
            },
            None => None,
        };
        state.writing = state.writing.checked_add(1)?;
        Some(WritePermit {
            gate: self.clone(),
            deadline,
            lease: state.lease,
            dependencies,
        })
    }

    /// 业务作用：立即禁止新写出，并通知等待旧任务退出的控制器。
    /// 参数说明：无。
    ///
    /// 返回：旧门禁永久关闭；已经取得的许可仍须完成或由连接关闭收口。
    pub fn close(&self) {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        // 先关准入再发观察通知，监控或任务调度延迟不能延长写权限。
        state.open = false;
        state.closed = true;
        self.changes
            .send_modify(|epoch| *epoch = epoch.saturating_add(1).max(2));
    }

    /// 业务作用：核对旧定义已经停止准入且真实写出责任全部结束，供新定义激活前建立屏障。
    /// 参数说明: 无。
    ///
    /// 返回：永久关闭且在途数量为零时为 true；开放门禁不能视为已排空。
    pub fn drained(&self) -> bool {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        (!state.open || self.stopping.load(Ordering::Acquire)) && state.writing == 0
    }

    /// 业务作用：关闭门禁并等待真实在途写出退出。
    /// 参数说明：无。
    ///
    /// 返回：所有旧许可已释放；取消等待不会重新开放门禁。
    pub async fn drain(&self) {
        let mut changes = self.changes.subscribe();
        self.close();
        loop {
            if self.state.lock().unwrap_or_else(|e| e.into_inner()).writing == 0 {
                return;
            }
            if changes.changed().await.is_err() {
                return;
            }
        }
    }
}

impl WritePermit {
    /// 业务作用：在一次已登记写出的每次推进前复验原控制期限，不借后来的续期延长旧批次。
    /// 参数说明: 无。
    ///
    /// 返回：原许可仍有效；机器休眠跨过期限后立即拒绝继续推进 I/O。
    pub fn valid(&self) -> bool {
        !self.gate.stopping.load(Ordering::Acquire)
            && self.lease.is_none_or(|lease| lease.deadline().is_some())
            && self
                .dependencies
                .iter()
                .flatten()
                .all(|dependency| dependency.valid())
    }
}

impl Drop for WritePermit {
    /// 业务作用：释放一次实际写出责任，使控制器能够证明旧写出已经结束。
    /// 参数说明：无。
    ///
    /// 返回：在途数量减少并唤醒等待者，不恢复路由权限。
    fn drop(&mut self) {
        let mut state = self.gate.state.lock().unwrap_or_else(|e| e.into_inner());
        state.writing -= 1;
        self.gate
            .changes
            .send_modify(|epoch| *epoch = epoch.saturating_add(1));
    }
}
