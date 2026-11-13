//! 产品数据卷阻塞工作的有限责任目录，等待取消不撤回实际落盘。

use std::sync::{Arc, Mutex};
use tokio::{sync::Semaphore, time::Instant};
use tokio_util::task::TaskTracker;

/// 节点数据卷的有限阻塞任务域，提交者退出后仍追踪落盘直到完成或停机截止。
pub struct Persistence {
    /// 提交准入的永久关闭事实，与任务登记共用短临界区，避免排空后出现新任务。
    closed: Mutex<bool>,
    /// 三十二个真实阻塞任务的共享容量；异步等待者取消不归还执行者的份额。
    capacity: Arc<Semaphore>,
    /// 由真实阻塞闭包持有 token 的任务目录，停机在原始截止点内等待其归零。
    tasks: TaskTracker,
}

impl Persistence {
    /// 业务作用：为同一产品数据卷建立有限阻塞提交目录，避免取消积累无主任务。
    /// 参数说明: 无。
    ///
    /// 返回：最多三十二个真实任务的责任域，不创建额外线程池。
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            closed: Mutex::new(false),
            capacity: Arc::new(Semaphore::new(32)),
            tasks: TaskTracker::new(),
        })
    }

    /// 业务作用：把持久任务及容量移交给真实阻塞执行者，调用方取消后仍纳入关闭屏障。
    /// 参数说明：
    /// - `work`：拥有所需数据卷引用、串行锁和输入的持久操作。
    ///
    /// 返回：实际操作结果；容量耗尽、关闭或执行异常拒绝，不宣称副作用已经撤销。
    pub async fn run<F, T>(&self, work: F) -> anyhow::Result<T>
    where
        F: FnOnce() -> anyhow::Result<T> + Send + 'static,
        T: Send + 'static,
    {
        let (token, capacity) = {
            let closed = self
                .closed
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            anyhow::ensure!(!*closed, "persistence_closed");
            let capacity = self
                .capacity
                .clone()
                .try_acquire_owned()
                .map_err(|_| anyhow::anyhow!("persistence_capacity"))?;
            (self.tasks.token(), capacity)
        };
        tokio::task::spawn_blocking(move || {
            let (_token, _capacity) = (token, capacity);
            work()
        })
        .await?
    }

    /// 业务作用：观察尚未结束的真实持久任务，包含已经失去异步等待者的操作。
    /// 参数说明: 无。
    ///
    /// 返回：任务 owner 数量，不把超时或取消计为执行结束。
    pub fn active(&self) -> usize {
        self.tasks.len()
    }

    /// 业务作用：立即停止新持久责任，不依赖异步关闭 future 是否仍有时间被调度。
    /// 参数说明: 无。
    ///
    /// 返回：后续提交永久拒绝，现有真实任务继续保留 owner。
    pub fn close(&self) {
        let mut closed = self
            .closed
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        // 停止接纳与登记任务共用临界区，空目录确认之后不能再出现迟到任务。
        *closed = true;
        self.tasks.close();
    }

    /// 业务作用：关闭新提交并在宿主原始截止点内等待全部真实持久责任结束。
    /// 参数说明：
    /// - `deadline`：napp 共享关闭截止点，不为该组件重建独立预算。
    ///
    /// 返回：全部阻塞任务结束后成功；逾期保留 owner 并明确报告未完成。
    pub async fn stop_and_wait(&self, deadline: Instant) -> anyhow::Result<()> {
        self.close();
        tokio::time::timeout_at(deadline, self.tasks.wait())
            .await
            .map_err(|_| anyhow::anyhow!("persistence_drain_incomplete"))?;
        Ok(())
    }
}
