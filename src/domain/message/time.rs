use super::UnixMillis;
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// 生产者首次确定的投递时间窗，重试和跨节点传输保持不变。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DeliveryWindow {
    /// 原始创建时刻的 Unix 毫秒整数，时钟偏差需在部署允许范围内。
    pub created_at: UnixMillis,
    /// 原始最晚投递时刻的 Unix 毫秒整数，重试不能延长有效区间。
    pub expires_at: UnixMillis,
}

impl DeliveryWindow {
    /// 业务作用：限制消息有效期并把剩余时间交给本地单调时钟管理。
    /// 参数说明：
    ///
    /// - `now`：本次受理时取得的 Unix 毫秒时间。
    /// - `maximum_ttl_ms`：路由和部署共同允许的最大消息存活毫秒数。
    /// - `maximum_future_skew_ms`：允许创建时间领先本地时钟的最大毫秒数。
    ///
    /// 返回：合法且未过期时返回剩余时长；倒序、超长、超前或过期均拒绝。
    pub fn remaining(
        &self,
        now: UnixMillis,
        maximum_ttl_ms: u64,
        maximum_future_skew_ms: u64,
    ) -> anyhow::Result<Duration> {
        let created = self.created_at.as_millis();
        let expires = self.expires_at.as_millis();
        let now = now.as_millis();
        anyhow::ensure!(expires > created, "expiresAt must be after createdAt");
        anyhow::ensure!(
            expires - created <= maximum_ttl_ms,
            "message TTL exceeds policy"
        );
        // 未来创建时刻不能借时钟偏差扩大存活时间；不使用可能溢出的时间加法。
        anyhow::ensure!(
            created.saturating_sub(now) <= maximum_future_skew_ms,
            "message creation time is too far in the future"
        );
        anyhow::ensure!(expires > now, "message has expired");
        Ok(Duration::from_millis(expires - now))
    }
}
