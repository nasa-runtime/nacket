use serde::Deserialize;

/// 会话观察采用有界分批扫描，采样不能改变任何授权与投递结论。
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, PartialEq, Eq, Deserialize)]
#[serde(default, rename_all = "camelCase", deny_unknown_fields)]
pub struct SessionMetrics {
    /// 是否采样有效会话的业务活跃状态与本节点订阅目标。
    pub enabled: bool,
    /// 距离最近成功业务动作或完整业务写出的活跃窗口，单位毫秒。
    pub activity_window_ms: u64,
    /// 两批采样之间的最小间隔，单位毫秒。
    pub interval_ms: u64,
    /// 每批最多访问的会话数；每个会话仍受订阅容量上限约束。
    pub batch_size: usize,
    /// 一轮采样允许保留的不同路由、租户、订阅目标组合数。
    pub max_targets: usize,
    /// 目标目录的保守字节预算，不保存正文或 UID。
    pub max_target_bytes: usize,
    /// 一轮扫描的最大跨度，超过后报告未知而不发布部分总量。
    pub max_cycle_ms: u64,
}

impl Default for SessionMetrics {
    /// 业务作用：默认提供低开销的连接活动与目标数量观察。
    /// 参数说明: 无。
    ///
    /// 返回：一分钟活跃窗口、每百毫秒一批和两分钟完整扫描期限。
    fn default() -> Self {
        Self {
            enabled: true,
            activity_window_ms: 60000,
            interval_ms: 100,
            batch_size: 128,
            max_targets: 65536,
            max_target_bytes: 16 * 1024 * 1024,
            max_cycle_ms: 120000,
        }
    }
}

impl SessionMetrics {
    /// 业务作用：把会话目录扫描成本限制在部署硬上限内。
    /// 参数说明: 无。
    ///
    /// 返回：周期、批量与保留预算合法；变更这些采样资源需要重启。
    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            (1000..=3600000).contains(&self.activity_window_ms)
                && (50..=10000).contains(&self.interval_ms)
                && (1..=256).contains(&self.batch_size)
                && (1..=1000000).contains(&self.max_targets)
                && (1024..=256 * 1024 * 1024).contains(&self.max_target_bytes)
                && self.max_cycle_ms >= self.interval_ms * 2
                && self.max_cycle_ms <= 300000,
            "invalid_session_metrics_budget"
        );
        Ok(())
    }
}
