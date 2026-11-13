//! 连接事实的节点级预算；回调失败不会保留已经失效的会话权限。

use serde::{Deserialize, Serialize};

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase", deny_unknown_fields)]
/// 连接事实发布与初始化关联的节点预算，两者均不能无限占用业务接入资源。
pub struct LifecycleDelivery {
    /// 连接事实发布的固定工作分片数，同一精确会话按分片保持发起顺序。
    pub workers: usize,
    /// 已接纳且尚未结束的连接事实总数上限。
    pub max_pending: usize,
    /// 连接事实排队及处理的共享逻辑字节预算。
    pub max_bytes: usize,
    /// 单个连接事实编码的最大字节数，防止大字段占尽通知容量。
    pub max_event_bytes: usize,
    /// 每条事实最多发布次数，重试保持原事件 ID、时间和会话序列。
    pub max_attempts: usize,
    /// 事实发布失败后的固定退避毫秒数，不能延长宿主关闭截止点。
    pub retry_backoff_ms: u64,
    /// 同时等待 MQ 初始化回复的精确请求数量上限。
    pub max_pending_initializations: usize,
    /// 初始化关联及控制信息的独立字节预算，直到回复、取消或到期才释放。
    pub max_initialization_bytes: usize,
}

impl Default for LifecycleDelivery {
    /// 业务作用：为连接事实提供独立有限预算，避免远端不可用耗尽连接或数据投递资源。
    /// 参数说明: 无。
    ///
    /// 返回：16 个有序分片、最多 1024 条事实和 8 MiB 预算，默认只尝试一次。
    fn default() -> Self {
        Self {
            workers: 16,
            max_pending: 1024,
            max_bytes: 8 * 1024 * 1024,
            max_event_bytes: 16 * 1024,
            max_attempts: 1,
            retry_backoff_ms: 100,
            max_pending_initializations: 128,
            max_initialization_bytes: 8 * 1024 * 1024,
        }
    }
}

impl LifecycleDelivery {
    /// 业务作用：在启动前限定事实大小、在途数量及重试次数，重试仍受事件原始期限约束。
    /// 参数说明: 无。
    ///
    /// 返回：全部容量有界且允许单个事件入队；无效组合拒绝启动。
    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            (1..=64).contains(&self.workers)
                && (self.workers..=65_536).contains(&self.max_pending)
                && (1024..=65_536).contains(&self.max_event_bytes)
                && (self.max_event_bytes * 2 + 4096..=64 * 1024 * 1024).contains(&self.max_bytes)
                && (1..=5).contains(&self.max_attempts)
                && (10..=5000).contains(&self.retry_backoff_ms),
            "lifecycle_delivery_capacity_invalid"
        );
        anyhow::ensure!(
            (1..=65_536).contains(&self.max_pending_initializations)
                && (4096..=64 * 1024 * 1024).contains(&self.max_initialization_bytes),
            "initialization_capacity_invalid"
        );
        Ok(())
    }
}
