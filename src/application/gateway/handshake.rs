//! 固定业务端点只接收本路由批准的握手投影，投影与会话共同持有节点内存额度。

pub(crate) mod attempt;

use super::{Gateway, Handshake};
use crate::domain::{message::control::Authenticate, route::handshake::HandshakePolicy};
use serde::Serialize;
use std::collections::BTreeMap;

/// 网络事实与未认证附加值分开，不把前端自报值提升为可信声明。
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Serialize)]
pub struct Metadata {
    /// 真实 TCP 对端，不从允许转交的 HTTP 头取得。
    peer: String,
    /// 浏览器提供并经过路由来源规则检查的 Origin，不作为身份凭据。
    origin: Option<String>,
    /// 经路由字段目录及字节限制筛选的客户端自报标量。
    auth: BTreeMap<String, serde_json::Value>,
}

/// 会话唯一保留的有限投影，不能用于改变远程目标或身份。
#[derive(Serialize)]
pub struct Facts {
    /// HTTP 标准请求的 handshake 部分，额外属性位于 auth 子对象。
    handshake: Metadata,
    /// 明确允许转交的 HTTP header，TCP 连接为空。
    headers: BTreeMap<String, String>,
    #[serde(skip)]
    /// 与上下文 owner 同寿命的节点内存责任。
    _capacity: tokio::sync::OwnedSemaphorePermit,
    #[serde(skip)]
    /// 握手上下文实际保留空间的有限资源观察。
    _observed: crate::observability::Reservation,
}

/// 将标准操作输入与只读投影合并序列化，不复制业务正文。
#[derive(Serialize)]
pub struct Input<'a, Q: ?Sized> {
    #[serde(flatten)]
    /// 服务端构造的请求身份、操作与业务参数。
    request: &'a Q,
    #[serde(flatten)]
    /// 只包含允许字段的固定会话投影。
    facts: &'a Facts,
}

impl Facts {
    /// 业务作用：在调用任何远端身份端点前校验附加字段并保留有界投影。
    /// 参数说明：
    /// - `gateway`：提供节点握手保留预算与指标。
    /// - `policy`：当前冻结路由允许的字段与字节限制。
    /// - `request`：客户端 AUTH 输入，不能覆盖真实网络事实。
    /// - `transport`：接入层收集的有限网络事实与允许头候选。
    ///
    /// 返回：持有节点容量的固定投影；未知属性、过长字段或容量不足拒绝接入。
    pub fn new(
        gateway: &Gateway,
        policy: &HandshakePolicy,
        request: &Authenticate,
        transport: &Handshake,
    ) -> anyhow::Result<Self> {
        let mut auth = BTreeMap::new();
        for (name, value) in request.auth_data.iter().flatten() {
            let limit = policy
                .auth_fields
                .get(name)
                .ok_or_else(|| anyhow::anyhow!("auth_data_field_denied"))?;
            if value.is_null() {
                continue;
            }
            anyhow::ensure!(
                serde_json::to_vec(value)?.len() <= *limit,
                "auth_data_value_capacity"
            );
            auth.insert(name.clone(), value.clone());
        }
        let mut headers = BTreeMap::new();
        for (name, limit) in &policy.headers {
            if let Some(value) = transport.headers.get(name) {
                anyhow::ensure!(value.len() <= *limit, "handshake_header_capacity");
                headers.insert(name.clone(), value.clone());
            }
        }
        let handshake = Metadata {
            peer: transport.peer.clone(),
            origin: transport.origin.clone(),
            auth,
        };
        let bytes = serde_json::to_vec(&(&handshake, &headers))?.len();
        anyhow::ensure!(bytes <= 16384, "handshake_context_capacity");
        let retained = bytes * 8 + 512;
        let capacity = gateway.calls.retain_context(retained)?;
        Ok(Self {
            handshake,
            headers,
            _capacity: capacity,
            _observed: gateway
                .metrics
                .reserve("handshake_context_bytes", retained as u64),
        })
    }

    /// 业务作用：为固定业务请求附加允许的握手投影，原始字段无需重新物化。
    /// 参数说明：
    /// - `request`：不包含 handshake 或 headers 的服务端标准请求。
    ///
    /// 返回：借用式序列化视图，生命周期不超过原会话与请求。
    pub fn input<'a, Q: Serialize + ?Sized>(&'a self, request: &'a Q) -> Input<'a, Q> {
        Input {
            request,
            facts: self,
        }
    }
}
