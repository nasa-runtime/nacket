use serde::{Deserialize, Serialize};

/// 所有业务 HTTP 与公钥读取共用的节点硬上限，单端点并发限制仍独立生效。
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase", deny_unknown_fields)]
pub struct RemoteCalls {
    /// 节点同时持有的远程调用数上限，包含等待单端点容量的任务。
    pub max_in_flight: usize,
    /// 所有远程调用共享的请求构造和响应临时空间保守预算，单位字节。
    pub max_bytes: usize,
    /// 单次 JSON 请求序列化的最大字节数，分配正文前先做长度检查。
    pub max_request_bytes: usize,
    /// 主动刷新最多同时执行的请求数，仍占用统一远程调用总预算。
    pub max_refresh_in_flight: usize,
    /// 主动刷新等待队列的最大会话数，满容量按原授权期限退避。
    pub max_refresh_pending: usize,
    /// 已建立或待确认会话保留的握手上下文总字节预算，独立于临时 HTTP 请求缓冲。
    pub max_context_bytes: usize,
    /// 所有会话共享的只读授权缓存条目上限，待决合并责任也占用额度。
    pub max_authorization_cache_entries: usize,
    /// 全节点授权缓存的保守保留字节上限，不与临时 HTTP 缓冲混用。
    pub max_authorization_cache_bytes: usize,
}
impl Default for RemoteCalls {
    /// 业务作用：限制跨路由远程调用总量及控制 JSON 的保守构造容量。
    /// 参数说明: 无。
    ///
    /// 返回：256 个调用、256 MiB 保守缓冲预算与 1 MiB 单请求上限。
    fn default() -> Self {
        Self {
            max_in_flight: 256,
            max_bytes: 256 * 1024 * 1024,
            max_request_bytes: 1024 * 1024,
            max_refresh_in_flight: 32,
            max_refresh_pending: 256,
            max_context_bytes: 64 * 1024 * 1024,
            max_authorization_cache_entries: 4096,
            max_authorization_cache_bytes: 16 * 1024 * 1024,
        }
    }
}
impl RemoteCalls {
    /// 业务作用：在启动前拒绝无限远程并发、缓冲或请求体配置。
    /// 参数说明: 无。
    ///
    /// 返回：有限合法配置；关闭某个业务功能不会取消其它调用的节点总限制。
    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            (1..=65536).contains(&self.max_authorization_cache_entries)
                && (1024..=256 * 1024 * 1024).contains(&self.max_authorization_cache_bytes),
            "authorization_cache_capacity_invalid"
        );
        anyhow::ensure!(
            (1..=4096).contains(&self.max_refresh_in_flight)
                && (1..=4096).contains(&self.max_refresh_pending)
                && (1024 * 1024..=1024 * 1024 * 1024).contains(&self.max_context_bytes),
            "authorization_refresh_capacity_invalid"
        );
        anyhow::ensure!(
            (1..=4096).contains(&self.max_in_flight)
                && (1024 * 1024..=1024 * 1024 * 1024).contains(&self.max_bytes)
                && (4096..=1024 * 1024).contains(&self.max_request_bytes),
            "remote_capacity_invalid"
        );
        Ok(())
    }
}
