//! 主动授权刷新只延长业务授权，原始身份凭据及撤权下界仍独立限制会话。

use serde::{Deserialize, Serialize};

/// 刷新开关与端点随路由定义冻结；开启后自动维护已有 Ready 会话。
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase", deny_unknown_fields)]
pub struct RefreshPolicy {
    /// 是否允许主动或显式刷新；关闭不延长任何现有授权。
    pub enabled: bool,
    /// 只读刷新授权使用的固定业务端点，不接受客户端选择目标。
    pub endpoint_ref: Option<String>,
    #[serde(skip_serializing_if = "SchedulePolicy::is_default")]
    /// 提前量、抖动和失败退避，网络调用仍受节点与端点预算约束。
    pub schedule: SchedulePolicy,
}

/// 每会话只保留一个在途刷新意图，队列与执行并发另有节点上限。
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase", deny_unknown_fields)]
pub struct SchedulePolicy {
    /// 在现有授权到期前开始刷新的毫秒数，不能用作延长授权的宽限期。
    pub advance_ms: u64,
    /// 按会话稳定分散额外提前量的最大毫秒数，避免集中到期同时请求。
    pub jitter_ms: u64,
    /// 连续尝试的最短间隔，拒绝、超时和容量不足均不紧密重试。
    pub retry_ms: u64,
    /// 单次刷新包含排队和网络等待的最长毫秒数，仍受现有授权截止点收紧。
    pub timeout_ms: u64,
}

impl Default for SchedulePolicy {
    /// 业务作用：为启用主动刷新的路由提供有限默认提前量与退避。
    /// 参数说明: 无。
    ///
    /// 返回：提前五秒并分散最多一秒，每次尝试间隔至少一秒、最多等待一千五百毫秒。
    fn default() -> Self {
        Self {
            advance_ms: 5000,
            jitter_ms: 1000,
            retry_ms: 1000,
            timeout_ms: 1500,
        }
    }
}

impl SchedulePolicy {
    /// 业务作用：保留省略调度配置时的规范定义表示。
    /// 参数说明: 无。
    ///
    /// 返回：全部字段等于有限默认值时省略该对象。
    pub fn is_default(&self) -> bool {
        self == &Self::default()
    }

    /// 业务作用：拒绝过密重试、无界提前或不能在原授权窗口内结束的调度参数。
    /// 参数说明: 无。
    ///
    /// 返回：全部毫秒参数在可执行范围内，非法值不进入路由摘要与运行期。
    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            (100..=300_000).contains(&self.advance_ms)
                && self.jitter_ms <= self.advance_ms
                && (100..=60_000).contains(&self.retry_ms)
                && (1..=30_000).contains(&self.timeout_ms),
            "authorization_refresh_schedule_invalid"
        );
        Ok(())
    }
}
