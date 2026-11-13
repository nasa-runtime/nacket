use serde::Deserialize;

/// broker 位置采样与业务消费独立，关闭时不增加观测网络请求。
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, PartialEq, Eq, Deserialize)]
#[serde(default, rename_all = "camelCase", deny_unknown_fields)]
pub struct BrokerMetrics {
    /// 是否启用固定来源的只读积压采样，不改变消费或业务准入资格。
    pub enabled: bool,
    /// 同一来源两次采样之间的最短间隔，单位毫秒。
    pub interval_ms: u64,
    /// 单轮采样全部请求共用的最大时长，单位毫秒。
    pub timeout_ms: u64,
    /// 成功样本允许展示的最大年龄，单位毫秒；失败立即停止展示旧数量。
    pub stale_after_ms: u64,
    /// 节点允许同时执行的采样任务数，容量不足直接报告未知，不堆积等待。
    pub max_in_flight: usize,
}

impl Default for BrokerMetrics {
    /// 业务作用：默认不增加 broker 观测请求，开启时采用低频且有界的读取。
    /// 参数说明: 无。
    ///
    /// 返回：关闭开关、五秒周期、一秒请求、十五秒新鲜度和四个并发名额。
    fn default() -> Self {
        Self {
            enabled: false,
            interval_ms: 5000,
            timeout_ms: 1000,
            stale_after_ms: 15000,
            max_in_flight: 4,
        }
    }
}

impl BrokerMetrics {
    /// 业务作用：在启动前限制主动观测成本，禁止采样频率和等待吞没业务连接池。
    /// 参数说明: 无。
    ///
    /// 返回：周期、期限和并发关系合法；运行期变更需重启以重新冻结目录和资源。
    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            (1000..=60000).contains(&self.interval_ms)
                && (100..=5000).contains(&self.timeout_ms)
                && self.timeout_ms <= self.interval_ms
                && self.stale_after_ms >= self.interval_ms * 2
                && self.stale_after_ms <= 300000
                && (1..=16).contains(&self.max_in_flight),
            "invalid_broker_metrics_budget"
        );
        Ok(())
    }
}
