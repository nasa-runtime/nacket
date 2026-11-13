mod budget;
pub use budget::Budget;

use crate::config::uid_events::UidEvents;
use nasa::partition::{Submission, TaskSpec, TaskStatus, TaskType};
use std::{sync::Arc, time::Duration};
use tokio::{sync::oneshot, time::Instant};

/// 本节点全部可信主体共用的命名执行域，不按用户、路由或连接尝试创建 Runner。
pub const RUNNER: &str = nasa::partition::DEFAULT_RUNNER;

/// 同一原始分区的所有业务事件采用同一严格类型，不能用事件名绕开顺序。
const UID_EVENT: TaskType = TaskType(1);

/// 已验证主体的固定提交入口；只借用 napp 受管 Runner，没有启停权。
#[derive(Clone)]
pub struct Lane {
    /// 组件与停机权威的唯一宿主。
    application: nasa::Application,
    /// 租户与用户共同决定本地顺序域，路由或 session ID 不参与分桶。
    key: (Arc<str>, Arc<str>),
    /// 排队最长等待不能突破上层会话或动作的原始期限。
    timeout: Duration,
    /// 全节点共享的主体和字节预算，跨连接不能重复获得额度。
    budget: Arc<Budget>,
}

/// 已接纳本地事件的实际执行证据，停止观察不会转移或复制执行权。
pub struct Pending<T> {
    /// 普通观察取消撤销排队动作；终态清理可显式把完整责任交给受管 Runner。
    cancel_on_drop: bool,
    /// napart 的稳定终态和取消权威。
    submission: Submission,
    /// 只存储该次消费生成的业务结果。
    result: oneshot::Receiver<anyhow::Result<T>>,
    /// 在入队前固定的最长观察期限。
    deadline: Instant,
}

/// 业务作用：在 UserHook 冻结统一 UID 事件域，启动、健康与退出全部归 napp 管理。
/// 参数说明：
/// - `application`：已声明 partition 组件的应用。
/// - `config`：启动时冻结的节点事件预算。
///
/// 返回：计划已登记但尚未启动；同名重复配置或无效容量拒绝启动。
pub fn install(application: &nasa::Application, config: &UidEvents) -> anyhow::Result<()> {
    config.validate()?;
    application.configure_partition_runner(
        RUNNER,
        nasa::application::PartitionApplicationPlan::new(
            config.partitions,
            config.queue_capacity,
            config.max_inflight,
        )?
        .with_max_lanes(config.partitions)?
        .with_critical(true)
        .with_force_on_timeout(false),
    )?;
    Ok(())
}

impl Lane {
    /// 业务作用：在身份来源验证完成后固定主体顺序域，客户端自报身份不得直接调用此入口。
    /// 参数说明：
    /// - `application`：拥有统一受管执行域的宿主。
    /// - `tenant`：已验证的租户标识。
    /// - `uid`：已验证的非空用户标识。
    /// - `config`：已验证的节点事件预算。
    /// - `budget`：该节点唯一的主体预算与指标来源。
    ///
    /// 返回：本节点可复用的主体提交入口；非法身份拒绝，尚未提交任何事件。
    pub fn new(
        application: nasa::Application,
        tenant: &str,
        uid: &str,
        config: &UidEvents,
        budget: Arc<Budget>,
    ) -> anyhow::Result<Self> {
        crate::domain::message::envelope::identifier(tenant)?;
        crate::domain::message::envelope::user_identifier(uid)?;
        Ok(Self {
            application,
            budget,
            key: (Arc::from(tenant), Arc::from(uid)),
            timeout: Duration::from_millis(config.timeout_ms),
        })
    }

    /// 业务作用：同步登记本地短事件，再由调用者在分区外等待，不能把远程 I/O 放入消费闭包。
    /// 参数说明：
    /// - `deadline`：原会话或业务动作的最晚有效时刻。
    /// - `action`：有界本地状态动作，取得消费权后仍须复验原会话与意图。
    ///
    /// 返回：已接纳责任；容量、运行域或期限不足立即拒绝，不等待分区名额。
    pub fn submit<T, F>(&self, deadline: Instant, action: F) -> anyhow::Result<Pending<T>>
    where
        T: Send + 'static,
        F: FnOnce() -> anyhow::Result<T> + Send + 'static,
    {
        self.submit_counted(deadline, 65536, false, action)
    }

    /// 业务作用：将共享正文及控制信息的保留量纳入主体预算后提交普通事件。
    /// 参数说明：
    /// - `deadline`：原动作不可延长的截止点。
    /// - `bytes`：正文与控制信息的逻辑保留字节，不要求复制原文。
    /// - `action`：无远程等待的本地动作。
    ///
    /// 返回：保留全部预算的提交责任；容量不足立即拒绝。
    pub fn submit_data<T, F>(
        &self,
        deadline: Instant,
        bytes: usize,
        action: F,
    ) -> anyhow::Result<Pending<T>>
    where
        T: Send + 'static,
        F: FnOnce() -> anyhow::Result<T> + Send + 'static,
    {
        self.submit_counted(deadline, bytes, false, action)
    }

    /// 业务作用：让完成、取消及关闭使用预留容量，仍提交同一严格类型且不得越过先前事件。
    /// 参数说明：
    /// - `deadline`：原责任的截止点。
    /// - `bytes`：完成结果持有的逻辑字节。
    /// - `action`：只推进既有责任的有限本地动作。
    ///
    /// 返回：保留控制责任的提交；控制容量耗尽仍明确拒绝，不能切换执行域。
    pub fn submit_control<T, F>(
        &self,
        deadline: Instant,
        bytes: usize,
        action: F,
    ) -> anyhow::Result<Pending<T>>
    where
        T: Send + 'static,
        F: FnOnce() -> anyhow::Result<T> + Send + 'static,
    {
        self.submit_counted(deadline, bytes, true, action)
    }

    /// 业务作用：在节点与主体预算冻结后登记唯一执行权，并观察实际排队及消费结局。
    /// 参数说明：
    /// - `deadline`：上层责任的原始截止点。
    /// - `bytes`：闭包持有的逻辑字节。
    /// - `control`：内部完成路径是否可以使用预留。
    /// - `action`：取得消费权后仍须复验会话与意图的同步动作。
    ///
    /// 返回：可取消、可等待的原 Submission，未接纳时释放全部预约。
    pub(crate) fn submit_counted<T, F>(
        &self,
        deadline: Instant,
        bytes: usize,
        control: bool,
        action: F,
    ) -> anyhow::Result<Pending<T>>
    where
        T: Send + 'static,
        F: FnOnce() -> anyhow::Result<T> + Send + 'static,
    {
        let lease = self.budget.reserve(&self.key, bytes, control)?;
        let mut observed = crate::observability::Operation::new(
            self.budget.metrics.clone(),
            crate::observability::Counter::UidEvents,
            if control {
                "control"
            } else {
                "data"
            },
            "uid_event_total",
        );
        let wait = self.budget.metrics.timer("uid_event_wait");
        let metrics = self.budget.metrics.clone();
        let deadline = deadline.min(Instant::now() + self.timeout);
        anyhow::ensure!(Instant::now() < deadline, "uid_event_expired");
        let runner = self
            .application
            .partition_runner(RUNNER)
            .map_err(|_| anyhow::anyhow!("uid_events_unavailable"))?;
        // 失去执行权威时不能绕过分区在网络线程继续授权；立即返回由上层收紧连接门禁。
        anyhow::ensure!(runner.is_healthy(), "uid_events_unavailable");
        let (result, receiver) = oneshot::channel();
        let submission = runner
            .submit_typed(
                self.key.clone(),
                TaskSpec::strict(UID_EVENT),
                move || async move {
                    let _lease = lease;
                    drop(wait);
                    let _executing = metrics.timer("uid_event_execute");
                    let value = if Instant::now() < deadline {
                        action()
                    } else {
                        Err(anyhow::anyhow!("uid_event_expired"))
                    };
                    observed.finish(if value.is_ok() {
                        "ok"
                    } else if Instant::now() >= deadline {
                        "expired"
                    } else {
                        "rejected"
                    });
                    let _ = result.send(value);
                },
            )
            .map_err(|_| anyhow::anyhow!("uid_event_rejected"))?;
        Ok(Pending {
            cancel_on_drop: true,
            submission,
            result: receiver,
            deadline,
        })
    }
}

impl<T> Pending<T> {
    /// 业务作用：把已经接纳的终态清理完整交给 napp，调用方退出不撤销必要的会话回收。
    /// 参数说明: 无。
    ///
    /// 返回：不承诺执行成功；闭包必须在消费及被丢弃两条路径均持有实际清理责任。
    pub fn detach(mut self) {
        self.cancel_on_drop = false;
    }

    /// 业务作用：先核实 napart 实际终态再交付结果，不能把登记或通知当成执行完成。
    /// 参数说明: 无。
    ///
    /// 返回：完整消费后的本地结果；超时或失权拒绝并请求撤销尚未执行的事件。
    pub async fn wait(mut self) -> anyhow::Result<T> {
        let status = tokio::time::timeout_at(self.deadline, self.submission.await_outcome())
            .await
            .map_err(|_| anyhow::anyhow!("uid_event_timeout"))?;
        anyhow::ensure!(status == TaskStatus::Completed, "uid_event_incomplete");
        self.result
            .try_recv()
            .map_err(|_| anyhow::anyhow!("uid_event_result_missing"))?
    }
}

impl<T> Drop for Pending<T> {
    /// 业务作用：观察方取消后撤销尚未执行的事件，已执行动作不重放也不伪装回滚。
    /// 参数说明: 无。
    ///
    /// 返回：取消只作用于原 Submission；真实终态与许可释放仍由 napart 维护。
    fn drop(&mut self) {
        if self.cancel_on_drop {
            self.submission.cancel();
        }
    }
}
