//! 产品接入能力与资源预算，由受管 WS listener 承载。

use serde::{Deserialize, Serialize};

/// 全局接入只允许固定 profile，路由仍须分别显式开放 native 或 Socket.IO。
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase", deny_unknown_fields)]
pub struct Transport {
    /// 可信主体的固定分区事件预算，所有业务端口和外部协议共用。
    pub uid_events: super::uid_events::UidEvents,
    /// 全部业务端口共享的物理接入准入与真实来源限制。
    pub admission: super::admission::AdmissionPolicy,
    /// 是否接纳原生 TCP 接入；关闭不自动打开其它适配器。
    pub native_tcp: bool,
    /// 是否接纳原生 WebSocket 接入，仍受路由能力限制。
    pub native_websocket: bool,
    /// 是否接纳 Socket.IO 兼容入口，路由还需单独启用该能力。
    pub socketio: bool,
    /// 原生 WebSocket 升级路径，用于在物理入口选择适配器。
    pub native_path: String,
    /// Engine.IO WebSocket 升级路径，与业务 namespace 分开。
    pub socketio_path: String,
    /// 单个完整原生帧及协议组装的字节上限，超过时拒绝继续处理。
    pub max_frame_bytes: usize,
    /// 固定控制区最大字节数，业务正文不能借 metadata 绕过容量。
    pub max_metadata_bytes: usize,
    /// 完整消息或附件的总组装预算，单位毫秒；碎片到达不刷新起点。
    pub assembly_timeout_ms: u64,
    /// 单次写出最长等待毫秒数，同时不能超过消息和权限的原始截止点。
    pub write_timeout_ms: u64,
    /// 业务心跳间隔，单位毫秒；不与底层 WS 控制帧重复计算。
    pub heartbeat_interval_ms: u64,
    /// 已发送业务心跳等待有效响应的毫秒预算，逾期关闭连接。
    pub heartbeat_timeout_ms: u64,
    /// 一条 Socket.IO 物理连接允许同时登记的 namespace 数量上限。
    pub max_namespaces: usize,
    /// 节点逻辑会话容量，Socket.IO 每个 namespace 独立占用一份。
    pub max_sessions: usize,
    /// 节点客户端操作在途数上限，跨连接共用，队列不能无限增长。
    pub max_pending_operations: usize,
    /// 节点业务出站逻辑字节预算，排队和 writer 持有阶段都计费。
    pub max_outbound_bytes: usize,
    /// 节点独立控制输出字节预算，握手和状态通知不与业务正文争用。
    pub max_control_outbound_bytes: usize,
    /// 节点共享的原生及附件接收保留预算，Bytes 最后 owner 释放后归还。
    pub max_inbound_bytes: usize,
    /// 节点请求关联记录容量，防止幂等标识耗尽内存。
    pub max_request_records: usize,
    /// 节点请求结果正文缓存字节预算，不包含业务历史补发能力。
    pub max_request_cache_bytes: usize,
}
impl Default for Transport {
    /// 业务作用：提供有限的原生接入默认值，Socket.IO 兼容入口由部署明确开启。
    /// 参数说明: 无。
    ///
    /// 返回：1 MiB 帧、64 KiB metadata、单连接最多八个 namespace 的接入配置。
    fn default() -> Self {
        Self {
            uid_events: Default::default(),
            admission: Default::default(),
            native_tcp: true,
            native_websocket: true,
            socketio: false,
            native_path: "/nacket".into(),
            socketio_path: "/socket.io/".into(),
            max_frame_bytes: 1024 * 1024,
            max_metadata_bytes: 65536,
            assembly_timeout_ms: 30_000,
            write_timeout_ms: 5000,
            heartbeat_interval_ms: 15_000,
            heartbeat_timeout_ms: 10_000,
            max_namespaces: 8,
            max_sessions: 100_000,
            max_pending_operations: 4096,
            max_outbound_bytes: 256 * 1024 * 1024,
            max_control_outbound_bytes: 8 * 1024 * 1024,
            max_inbound_bytes: 256 * 1024 * 1024,
            max_request_records: 65_536,
            max_request_cache_bytes: 64 * 1024 * 1024,
        }
    }
}
impl Transport {
    /// 业务作用：在 listener 启动前检查路径、帧与心跳预算，避免合法心跳晚于接收等待期限。
    /// 参数说明: 无。
    ///
    /// 返回：配置有界且互相一致；路径冲突或无限容量拒绝。
    pub fn validate(&self) -> anyhow::Result<()> {
        self.uid_events.validate()?;
        self.admission.validate()?;
        anyhow::ensure!(
            (65536..=64 * 1024 * 1024).contains(&self.max_control_outbound_bytes),
            "invalid control output capacity"
        );
        anyhow::ensure!(
            (1024 * 1024..=1024 * 1024 * 1024).contains(&self.max_inbound_bytes),
            "invalid node ingress capacity"
        );
        anyhow::ensure!(
            (1..=262_144).contains(&self.max_request_records)
                && (4096..=1024 * 1024 * 1024).contains(&self.max_request_cache_bytes),
            "invalid node request cache capacity"
        );
        anyhow::ensure!(
            (1024 * 1024..=1024 * 1024 * 1024).contains(&self.max_outbound_bytes),
            "invalid node outbox capacity"
        );
        anyhow::ensure!(
            self.native_tcp || self.native_websocket || self.socketio,
            "at least one transport is required"
        );
        for path in [&self.native_path, &self.socketio_path] {
            anyhow::ensure!(
                path.starts_with('/')
                    && path.len() <= 256
                    && !path
                        .bytes()
                        .any(|b| b.is_ascii_control() || b"?#%\\".contains(&b)),
                "invalid transport path"
            );
        }
        anyhow::ensure!(
            self.native_path != self.socketio_path,
            "transport paths conflict"
        );
        anyhow::ensure!(
            (65536..=16 * 1024 * 1024).contains(&self.max_frame_bytes)
                && (32768..=65536).contains(&self.max_metadata_bytes)
                && self.max_metadata_bytes + 11 < self.max_frame_bytes,
            "invalid frame limits"
        );
        anyhow::ensure!(
            (100..=60_000).contains(&self.heartbeat_interval_ms)
                && (100..=60_000).contains(&self.heartbeat_timeout_ms)
                && self.assembly_timeout_ms
                    >= self.heartbeat_interval_ms + self.heartbeat_timeout_ms
                && self.assembly_timeout_ms <= 120_000
                && (1..=30_000).contains(&self.write_timeout_ms),
            "inconsistent transport deadlines"
        );
        anyhow::ensure!(
            (1..=32).contains(&self.max_namespaces)
                && (1..=1_000_000).contains(&self.max_sessions)
                && (1..=65_536).contains(&self.max_pending_operations),
            "invalid transport capacities"
        );
        Ok(())
    }
}
