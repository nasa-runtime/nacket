//! 读取授权缓存只复用有界资格，不能复用单次发送授权或替身份凭据续期。

use serde::{Deserialize, Serialize};

/// 只读订阅授权的可选缓存，默认关闭，所有命中仍在应用点复验撤权与当前意图。
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase", deny_unknown_fields)]
pub struct CachePolicy {
    /// 是否复用同一精确会话的只读订阅判定，关闭则逐次调用业务端点。
    pub enabled: bool,
    /// 正缓存的最大毫秒数，最终取远端资格和当前身份截止点的更早者。
    pub positive_ttl_ms: u64,
    /// 明确拒绝的独立短缓存毫秒数；网络失败不进入负缓存。
    pub negative_ttl_ms: u64,
    /// 每会话最多保留的判定或正在合并的查询数，仍受节点总量约束。
    pub max_entries: usize,
    /// 每会话缓存保留字节上限，包含仍被等待者引用的旧条目。
    pub max_bytes: usize,
    /// 是否合并同一精确授权键的在途只读查询，各等待者仍独立取消和超时。
    pub coalesce: bool,
}

impl Default for CachePolicy {
    /// 业务作用：默认逐次授权，显式启用时提供短 TTL 与有限容量。
    /// 参数说明: 无。
    ///
    /// 返回：关闭缓存，正缓存一秒、负缓存两百毫秒，每会话最多一百二十八项。
    fn default() -> Self {
        Self {
            enabled: false,
            positive_ttl_ms: 1000,
            negative_ttl_ms: 200,
            max_entries: 128,
            max_bytes: 1048576,
            coalesce: true,
        }
    }
}

impl CachePolicy {
    /// 业务作用：省略完全默认的关闭策略，保持既有定义的摘要表示。
    /// 参数说明: 无。
    ///
    /// 返回：与默认配置相同时为真。
    pub fn is_default(&self) -> bool {
        self == &Self::default()
    }

    /// 业务作用：限制可复用判定的时间和空间，防止缓存变成无期限的授权来源。
    /// 参数说明: 无。
    ///
    /// 返回：策略有界且合法；零 TTL 不表示无限缓存。
    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            (1..=60_000).contains(&self.positive_ttl_ms)
                && (1..=5000).contains(&self.negative_ttl_ms)
                && (1..=1024).contains(&self.max_entries)
                && (1024..=16 * 1024 * 1024).contains(&self.max_bytes),
            "authorization_cache_policy_invalid"
        );
        Ok(())
    }
}
