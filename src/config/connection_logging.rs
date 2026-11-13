use serde::{Deserialize, Serialize};

/// 节点会话日志策略，与业务生命周期回调及 nalog 输出目标分别控制。
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase", deny_unknown_fields)]
pub struct ConnectionLogging {
    /// 是否记录首次 Ready 的连接成功日志，关闭不影响认证或上线通知。
    pub connected: bool,
    /// 是否记录首次关闭的断连日志，关闭仍须取消全部订阅并释放会话责任。
    pub disconnected: bool,
}

impl Default for ConnectionLogging {
    /// 业务作用：缺省保留建连与断连记录，便于关联同一次会话的接入和退出。
    /// 参数说明: 无。
    ///
    /// 返回：两个独立开关均开启，实际输出仍遵循 nalog 级别过滤。
    fn default() -> Self {
        Self {
            connected: true,
            disconnected: true,
        }
    }
}
