use serde::{Deserialize, Serialize};

/// 同一可信主体的本地保序事件预算，路由和连接尝试不改变顺序域。
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase", deny_unknown_fields)]
pub struct UidEvents {
    /// 节点固定分区数，必须为二的幂，不能随用户数量创建执行域。
    pub partitions: usize,
    /// 同一原始分区中统一事件类型的排队上限。
    pub queue_capacity: usize,
    /// 节点排队与执行中的事件总数上限。
    pub max_inflight: usize,
    /// 单个本地事件从接纳起算的最长等待毫秒数，不能延长业务期限。
    pub timeout_ms: u64,
    /// 同一可信主体跨全部路由与连接的本地待决事件上限。
    pub max_per_uid: usize,
    /// 单个主体排队事件持有的逻辑字节预算，正文共享引用仍计算保留量。
    pub max_bytes_per_uid: usize,
    /// 节点排队事件持有的逻辑字节预算，与传输实际字节预算同时生效。
    pub max_bytes: usize,
    /// 为完成、取消和关闭保留的事件数量，仍保持相同严格类型与提交顺序。
    pub control_reserve: usize,
}

impl Default for UidEvents {
    /// 业务作用：提供固定节点执行域和有限排队责任，避免为每个 uid 创建线程或队列。
    /// 参数说明: 无。
    ///
    /// 返回：三十二个分区、每分区二百五十六个排队事件和一秒等待上限。
    fn default() -> Self {
        Self {
            partitions: 32,
            queue_capacity: 256,
            max_inflight: 8192,
            timeout_ms: 1000,
            max_per_uid: 128,
            max_bytes_per_uid: 8 * 1024 * 1024,
            max_bytes: 256 * 1024 * 1024,
            control_reserve: 16,
        }
    }
}

impl UidEvents {
    /// 业务作用：在启动前限制本地事件并发及保留时间，过载不能变为无界网络积压。
    /// 参数说明: 无。
    ///
    /// 返回：全部预算合法；超界或不一致配置拒绝启动。
    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            (1..=1024).contains(&self.partitions)
                && self.partitions.is_power_of_two()
                && (1..=8192).contains(&self.queue_capacity)
                && (self.partitions..=262_144).contains(&self.max_inflight)
                && (10..=30_000).contains(&self.timeout_ms),
            "invalid uid event capacity"
        );
        anyhow::ensure!(
            (2..=8192).contains(&self.max_per_uid)
                && (1..self.queue_capacity.min(self.max_inflight)).contains(&self.control_reserve)
                && (131_072..=256 * 1024 * 1024).contains(&self.max_bytes_per_uid)
                && (self.max_bytes_per_uid..=1024 * 1024 * 1024).contains(&self.max_bytes)
                && self.control_reserve.saturating_mul(65536) < self.max_bytes,
            "invalid uid event reservation"
        );
        Ok(())
    }
}
