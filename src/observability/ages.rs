use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

/// 固定资源名称，待决年龄只跟随真实 owner，不保留消息或会话标识。
pub(super) const RESOURCES: &[&str] = &[
    "operations",
    "uid_events",
    "subscriptions_pending",
    "outbox_items",
    "writing_items",
    "receipts",
    "remote_calls",
    "control_pending",
    "authorization_commands",
    "fanout_pending_records",
    "publish_items",
    "lifecycle_items",
    "initialization_requests",
];
const BUCKETS: usize = 256;
const COUNT_MASK: u64 = u32::MAX as u64;

struct Queue {
    /// 高三十二位为起始秒加一，低三十二位为 owner 数；同一原子单元避免复用时串代。
    buckets: [AtomicU64; BUCKETS],
    /// 未能进入有限时间槽但仍存活的 owner 数；非零时年龄必须报告未知。
    untracked: AtomicU64,
}

/// 每类资源使用固定时间槽；常见路径只更新一个原子单元，不创建逐消息观察对象目录。
pub struct Ages {
    /// 本进程观察起点，时间槽和年龄均使用同一单调时钟。
    started: Instant,
    /// 与静态资源目录逐项对应的预分配槽位，不随连接数扩展。
    queues: Vec<Queue>,
}

/// 随待决责任移动的时间槽份额，完成或取消都会归还。
pub struct Lease {
    /// 原观察目录，实际责任结束之前持续存活。
    owner: Arc<Ages>,
    /// 固定资源目录索引，不含任意业务标识。
    queue: usize,
    /// 已占用时间槽；容量或争用使其缺失时归还 untracked 责任。
    bucket: Option<usize>,
}

impl Ages {
    /// 业务作用：预分配全部待决年龄槽位，避免在消息热路径建立时间索引。
    /// 参数说明: 无。
    ///
    /// 返回：每类资源最多二百五十六个同时存活起始秒的观察目录。
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            started: Instant::now(),
            queues: RESOURCES
                .iter()
                .map(|_| Queue {
                    buckets: std::array::from_fn(|_| AtomicU64::new(0)),
                    untracked: AtomicU64::new(0),
                })
                .collect(),
        })
    }

    /// 业务作用：为真实待决 owner 登记有界年龄份额，观测容量不足不得阻止业务动作。
    /// 参数说明：
    /// - `resource`：已登记的固定资源类别。
    ///
    /// 返回：目录内类别取得观察责任；其它资源没有年龄统计。
    pub fn acquire(self: &Arc<Self>, resource: &str) -> Option<Lease> {
        let queue = RESOURCES.iter().position(|name| *name == resource)?;
        Some(self.acquire_at(queue, self.started.elapsed()))
    }

    /// 业务作用：把本地持续时间量化为一秒槽，槽中仍有旧 owner 时绝不覆盖其年龄证据。
    /// 参数说明：
    /// - `queue`：已经核对的静态资源索引。
    /// - `elapsed`：从目录创建起经过的单调时长。
    ///
    /// 返回：有界原子登记责任；槽位、计数或时间范围耗尽时明确保留未知责任。
    fn acquire_at(self: &Arc<Self>, queue: usize, elapsed: Duration) -> Lease {
        let state = &self.queues[queue];
        if let Some(epoch) = elapsed
            .as_secs()
            .checked_add(1)
            .filter(|value| *value <= COUNT_MASK)
        {
            let first = epoch as usize % BUCKETS;
            for offset in 0..BUCKETS {
                let bucket = (first + offset) % BUCKETS;
                let cell = &state.buckets[bucket];
                let mut old = cell.load(Ordering::Relaxed);
                for _ in 0..8 {
                    let count = old & COUNT_MASK;
                    if count == COUNT_MASK || (count != 0 && old >> 32 != epoch) {
                        break;
                    }
                    let next = (epoch << 32) | (count + 1);
                    match cell.compare_exchange_weak(
                        old,
                        next,
                        Ordering::Relaxed,
                        Ordering::Relaxed,
                    ) {
                        Ok(_) => {
                            return Lease {
                                owner: self.clone(),
                                queue,
                                bucket: Some(bucket),
                            };
                        }
                        Err(current) => old = current,
                    }
                }
            }
        }
        // 有限观察槽无法保存完整事实时不能把已知子集冒充全量最旧年龄，也不反向拒绝业务。
        state.untracked.fetch_add(1, Ordering::Relaxed);
        Lease {
            owner: self.clone(),
            queue,
            bucket: None,
        }
    }

    /// 业务作用：只扫描固定原子槽生成年龄观察，不查询业务目录或持有会话锁。
    /// 参数说明: 无。
    ///
    /// 返回：每个资源的当前年龄上界，量化误差小于一秒；存在未知 owner 时为 None，空闲为零。
    pub fn snapshot(&self) -> Vec<Option<Duration>> {
        let elapsed = self.started.elapsed();
        self.queues.iter().map(|queue| queue.age(elapsed)).collect()
    }
}

impl Queue {
    /// 业务作用：从仍有 owner 的最早时间槽计算保守年龄，未跟踪责任使整个类别保持未知。
    /// 参数说明：
    /// - `elapsed`：观察时从共同单调起点经过的时长。
    ///
    /// 返回：完整观察时的年龄上界；并发更新按各原子单元采样，不声称全目录事务快照。
    fn age(&self, elapsed: Duration) -> Option<Duration> {
        if self.untracked.load(Ordering::Relaxed) != 0 {
            return None;
        }
        let first = self
            .buckets
            .iter()
            .filter_map(|cell| {
                let value = cell.load(Ordering::Relaxed);
                (value & COUNT_MASK != 0).then_some(value >> 32)
            })
            .min();
        if self.untracked.load(Ordering::Relaxed) != 0 {
            return None;
        }
        Some(
            first
                .map(|epoch| elapsed.saturating_sub(Duration::from_secs(epoch - 1)))
                .unwrap_or_default(),
        )
    }
}

impl Drop for Lease {
    /// 业务作用：实际待决责任结束时归还原槽份额，最后一个 owner 归还前该槽不能换代。
    /// 参数说明: 无。
    ///
    /// 返回：只减少本责任的一份计数；未知责任全部结束后自动恢复完整年龄观察。
    fn drop(&mut self) {
        let queue = &self.owner.queues[self.queue];
        if let Some(bucket) = self.bucket {
            queue.buckets[bucket].fetch_sub(1, Ordering::Relaxed);
        } else {
            queue.untracked.fetch_sub(1, Ordering::Relaxed);
        }
    }
}
