use serde::{Deserialize, Serialize};

/// 节点硬上限在所有业务 listener 间共享，路由附加配额开关不能取消这些边界。
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase", deny_unknown_fields)]
pub struct AdmissionPolicy {
    /// 跨共享及独立业务端口的物理连接硬上限，实际 I/O owner 全部退出后归还。
    pub max_physical_connections: usize,
    /// 节点尚未完成首个会话 Ready 的物理连接数量上限。
    pub max_unauthenticated: usize,
    /// 同一真实 TCP 对端的并存连接上限，不使用可伪造代理头作为来源。
    pub max_connections_per_peer: usize,
    /// 真实来源在一秒窗口内可接纳的连接次数上限。
    pub connections_per_second_per_peer: u32,
    /// 准入目录最多保留的真实来源数量，满容量时拒绝新来源登记。
    pub max_tracked_peers: usize,
}

impl Default for AdmissionPolicy {
    /// 业务作用：提供跨端口共享的接入容量及来源窗口，防止独立 listener 成倍放大节点准入。
    /// 参数说明: 无。
    ///
    /// 返回：十万物理连接、256 个未认证连接及有限来源目录，来源速率按固定一秒窗口计数。
    fn default() -> Self {
        Self {
            max_physical_connections: 100000,
            max_unauthenticated: 256,
            max_connections_per_peer: 10000,
            connections_per_second_per_peer: 1024,
            max_tracked_peers: 16384,
        }
    }
}

impl AdmissionPolicy {
    /// 业务作用：在建立任何监听前核对全节点与来源容量，零值不解释为无限或关闭保护。
    /// 参数说明: 无。
    ///
    /// 返回：全部容量有限且子限额不超过物理总数；无效配置拒绝启动。
    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            (1..=1000000).contains(&self.max_physical_connections)
                && (1..=4096).contains(&self.max_unauthenticated)
                && self.max_unauthenticated <= self.max_physical_connections
                && (1..=self.max_physical_connections).contains(&self.max_connections_per_peer)
                && (1..=100000).contains(&self.connections_per_second_per_peer)
                && (1..=65536).contains(&self.max_tracked_peers),
            "admission_capacity_invalid"
        );
        Ok(())
    }
}
