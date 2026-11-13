//! 部署监听模板控制网络边界；路由只能引用模板及选择是否派生独立端口。

use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeSet,
    net::{IpAddr, Ipv4Addr},
};

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
/// 监听模板允许的外部传输，业务正文编码由路由独立选择。
pub enum Protocol {
    /// TCP 上承载原生帧，使用原生握手与认证合同。
    NativeTcp,
    /// WebSocket 上承载原生帧，升级不等于业务认证成功。
    NativeWs,
    /// 经 Socket.IO 兼容入口建立逻辑路由会话。
    Socketio,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase", deny_unknown_fields)]
/// 路由派生独立监听的许可与容量，端口分配以实际 bind 成功为准。
pub struct DedicatedPorts {
    /// 是否允许引用本模板的路由申请独立监听端口。
    pub allowed: bool,
    /// 未指定端口时尝试 bind 的起始端口，包含该值。
    pub auto_start: u16,
    /// 自动 bind 的结束端口，包含该值；范围耗尽明确拒绝。
    pub auto_end: u16,
    /// 本模板允许保留的独立监听数量，准备与排空阶段仍占用份额。
    pub max_listeners: usize,
    /// 取得端口与监听准备的最长等待毫秒数，不回退到共享端口。
    pub bind_timeout_ms: u64,
}
impl Default for DedicatedPorts {
    /// 业务作用：为自动独立端口提供有限分配范围，始终以实际 bind 结果确定占用。
    /// 参数说明: 无。
    ///
    /// 返回：允许 128 个独立 listener，从 3030 向上尝试且整轮最多五秒。
    fn default() -> Self {
        Self {
            allowed: true,
            auto_start: 3030,
            auto_end: 65535,
            max_listeners: 128,
            bind_timeout_ms: 5000,
        }
    }
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase", deny_unknown_fields)]
/// 路由可引用的部署监听边界，路由选择不能扩大模板的协议及资源权限。
pub struct ListenerTemplate {
    /// 本地业务监听绑定地址，不代表客户端可访问的外部地址。
    pub bind_address: IpAddr,
    /// 使用共享监听的路由默认端口，与主受管监听配置核对。
    pub default_port: u16,
    /// 本监听允许的物理接入协议集合，路由不能打开目录外协议。
    pub protocols: BTreeSet<Protocol>,
    /// 独立端口能力、自动搜索范围及资源限制。
    pub dedicated_ports: DedicatedPorts,
    /// 该监听附加的物理连接数上限，节点硬上限仍同时生效。
    pub max_connections: usize,
    /// 该监听附加的未认证连接上限，不能覆盖节点全局限制。
    pub max_unauthenticated: usize,
    /// 从真实接入开始固定的认证毫秒预算，协议识别不重新计时。
    pub authentication_timeout_ms: u64,
    /// 外部地址声明与本地端口映射，声明本身不授予业务权限。
    pub advertise: super::advertisement::Advertisement,
}
impl Default for ListenerTemplate {
    /// 业务作用：建立共享 3030 的产品监听基线，接入能力仍受全局协议与路由开关共同限制。
    /// 参数说明: 无。
    ///
    /// 返回：有限连接、未认证容量及认证截止时间的模板。
    fn default() -> Self {
        Self {
            bind_address: IpAddr::V4(Ipv4Addr::UNSPECIFIED),
            default_port: 3030,
            protocols: [Protocol::NativeTcp, Protocol::NativeWs, Protocol::Socketio]
                .into_iter()
                .collect(),
            dedicated_ports: DedicatedPorts::default(),
            max_connections: 100000,
            max_unauthenticated: 256,
            authentication_timeout_ms: 5000,
            advertise: Default::default(),
        }
    }
}
impl ListenerTemplate {
    /// 业务作用：在监听产生副作用前核验部署范围、协议集合和有限容量。
    /// 参数说明: 无。
    ///
    /// 返回：模板合法；零端口、空协议、无界容量和非法自动范围拒绝。
    pub fn validate(&self) -> anyhow::Result<()> {
        self.advertise.validate()?;
        if self.advertise.enabled {
            self.advertise.public_port(self.default_port)?;
        }
        let ports = &self.dedicated_ports;
        anyhow::ensure!(
            self.default_port > 0
                && !self.protocols.is_empty()
                && (1..=1_000_000).contains(&self.max_connections)
                && (1..=4096).contains(&self.max_unauthenticated)
                && self.max_unauthenticated <= self.max_connections
                && (100..=60000).contains(&self.authentication_timeout_ms),
            "invalid listener template"
        );
        anyhow::ensure!(
            ports.auto_start > 0
                && ports.auto_start <= ports.auto_end
                && (1..=1024).contains(&ports.max_listeners)
                && (1..=30000).contains(&ports.bind_timeout_ms),
            "invalid dedicated port budget"
        );
        Ok(())
    }
}

/// 业务作用：选择主受管业务 listener 的稳定模板名。
/// 参数说明: 无。
///
/// 返回：默认 shared 模板，不从客户端读取。
pub fn primary() -> String {
    "shared".into()
}
