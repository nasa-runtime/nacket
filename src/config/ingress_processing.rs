use serde::{Deserialize, Serialize};

/// MQ 分发的节点总预算独立于连接入站与控制命令，不允许数据流占用控制保留容量。
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase", deny_unknown_fields)]
pub struct IngressProcessing {
    /// Kafka 异步交接目录的记录数量上限，完成等待重取的记录仍占用责任。
    pub max_pending_records: usize,
    /// Redis 同一业务顺序键允许持有的最大未完成记录数，达到边界暂停预取。
    pub max_key_pending_records: usize,
    /// 从消息首次创建时刻起允许占用重试责任的最长毫秒数，不因重新消费刷新。
    pub max_retry_age_ms: u64,
    /// 节点跨异步入口保留的拥有型正文总字节数，不按接收者重复分配。
    pub max_retained_bytes: usize,
    /// 业务 Redis 读前保守响应空间预算，单位字节。
    pub max_read_bytes: usize,
    /// 初始化回复与授权控制各自独立的读前字节预算，业务积压不能耗尽它们。
    pub max_control_read_bytes: usize,
    /// 错误原文进入隔离存储时独立预约的临时字节预算。
    pub max_isolation_bytes: usize,
    /// 节点同时推进的业务分发数上限，不能用无限任务隐藏反压。
    pub max_parallel_fanouts: usize,
    /// 全部活跃分发可同时保存的候选会话引用总数，枚举候选前预约。
    pub max_candidate_references: usize,
    /// 每次让出执行权前处理的最大接收者数，持续沿用同一原始消息截止点。
    pub fanout_batch_size: usize,
}
impl Default for IngressProcessing {
    /// 业务作用：限制异步交接、共享正文和本地候选快照，批量投递定期归还执行权。
    /// 参数说明: 无。
    ///
    /// 返回：256 个交接记录、64 MiB 正文、四个并行扇出和有限候选引用。
    fn default() -> Self {
        Self {
            max_pending_records: 256,
            max_key_pending_records: 64,
            max_retry_age_ms: 60000,
            max_retained_bytes: 64 * 1024 * 1024,
            max_read_bytes: 128 * 1024 * 1024,
            max_control_read_bytes: 8 * 1024 * 1024,
            max_isolation_bytes: 64 * 1024 * 1024,
            max_parallel_fanouts: 4,
            max_candidate_references: 1000000,
            fanout_batch_size: 256,
        }
    }
}
impl IngressProcessing {
    /// 业务作用：启动前拒绝无界交接或不可推进的批次，热更不能扩张节点固定资源。
    /// 参数说明: 无。
    ///
    /// 返回：所有硬上限有效；零值不表示关闭限制。
    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            (1..=4096).contains(&self.max_pending_records)
                && (1..=self.max_pending_records).contains(&self.max_key_pending_records)
                && (1000..=3600000).contains(&self.max_retry_age_ms)
                && (1024 * 1024..=1024 * 1024 * 1024).contains(&self.max_retained_bytes)
                && (1..=32).contains(&self.max_parallel_fanouts)
                && (1..=4000000).contains(&self.max_candidate_references)
                && (1..=1024).contains(&self.fanout_batch_size),
            "ingress_processing_capacity"
        );
        for capacity in [
            self.max_read_bytes,
            self.max_control_read_bytes,
            self.max_isolation_bytes,
        ] {
            anyhow::ensure!(
                (1024 * 1024..=1024 * 1024 * 1024).contains(&capacity),
                "ingress_buffer_capacity"
            );
        }
        Ok(())
    }
}
