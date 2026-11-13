use super::{Binding, Ledger, Phase};
use crate::{
    config::listeners::Protocol,
    domain::message::{envelope::identifier, UnixMillis},
};
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;

/// 节点已准备的外部接入声明，只有与当前路由许可共同提交后才进入受权目录。
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ListenerAccess {
    /// 实际监听 owner 的唯一绑定 ID，端口再利用不继承旧绑定。
    pub binding_id: String,
    /// 节点实际持有的本地监听地址，仅供受权目录诊断，不推断公共可达性。
    pub bound_address: SocketAddr,
    /// 部署明确声明的外部主机名或地址。
    pub host: String,
    /// 按已验证端口映射得到的外部端口，不假设与本地相同。
    pub port: u16,
    /// 外部入口是否提供安全传输的部署声明，不代表本地完成 TLS 终止。
    pub secure: bool,
    /// 该真实监听及路由允许公布的有限适配器接入描述。
    pub endpoints: Vec<Endpoint>,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
/// 一种适配器的公开连接方式，逻辑 endpoint 与物理升级路径分别声明。
pub struct Endpoint {
    /// 原生 TCP、原生 WS 或 Socket.IO 接入类别，各自遵循固定帧合同。
    pub adapter: Protocol,
    /// 公开地址使用的传输 scheme，由适配器及安全声明确定。
    pub scheme: String,
    /// 可选 WebSocket 升级路径，原生 TCP 不需要 HTTP path。
    pub path: Option<String>,
    /// 路由逻辑 endpoint 或 Socket.IO namespace，不与物理升级路径混用。
    pub endpoint: String,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
/// 随有限路由许可提交的节点接入声明，仅当前定义已应用且许可有效时可用。
pub struct PublishedAccess {
    /// 该接入声明绑定的完整路由定义，定义变化后旧目录不能续用。
    pub binding: Binding,
    /// 声明所属的稳定节点 ID，与实际监听和控制主体核对。
    pub node_id: String,
    /// 声明所属节点实例的十进制代际，重启后旧地址身份失效。
    pub incarnation: String,
    /// 与本声明同次提交的有限路由许可 ID。
    pub permit_id: String,
    /// 原续期的精确关联值，重试不能改变地址或刷新目录期限。
    pub request_nonce: String,
    /// 该有限声明的 Unix 毫秒签发时刻，来自原权威记录。
    pub issued_at: UnixMillis,
    /// 声明最晚有效的 Unix 毫秒时刻，同时还需当前许可及应用状态成立。
    pub expires_at: UnixMillis,
    /// 部署确认网络已就绪并完成映射的真实监听描述。
    pub listener: ListenerAccess,
    /// 节点是否已应用对应绑定并取得接流资格，准备阶段不公开为可用地址。
    pub serving: bool,
}

impl ListenerAccess {
    /// 业务作用：在持久提交前校验节点接入声明的字段及适配器唯一性，不接受任意重定向 URL。
    /// 参数说明: 无。
    ///
    /// 返回：有界且非歧义的地址材料；仍不构成用户身份或业务授权。
    pub fn validate(&self) -> anyhow::Result<()> {
        identifier(&self.binding_id)?;
        crate::config::advertisement::host(&self.host)?;
        anyhow::ensure!(
            self.port > 0
                && self.bound_address.port() > 0
                && (1..=3).contains(&self.endpoints.len()),
            "access_address_invalid"
        );
        let mut adapters = std::collections::BTreeSet::new();
        for endpoint in &self.endpoints {
            anyhow::ensure!(
                adapters.insert(endpoint.adapter),
                "access_adapter_duplicate"
            );
            path(&endpoint.endpoint)?;
            match endpoint.adapter {
                Protocol::NativeTcp => anyhow::ensure!(
                    endpoint.path.is_none()
                        && endpoint.scheme
                            == if self.secure {
                                "tls"
                            } else {
                                "tcp"
                            },
                    "access_tcp_invalid"
                ),
                Protocol::NativeWs | Protocol::Socketio => {
                    path(
                        endpoint
                            .path
                            .as_deref()
                            .ok_or_else(|| anyhow::anyhow!("access_path_missing"))?,
                    )?;
                    let expected = match (endpoint.adapter, self.secure) {
                        (Protocol::NativeWs, true) => "wss",
                        (Protocol::NativeWs, false) => "ws",
                        (Protocol::Socketio, true) => "https",
                        _ => "http",
                    };
                    anyhow::ensure!(endpoint.scheme == expected, "access_scheme_invalid");
                }
            }
        }
        Ok(())
    }
}

/// 业务作用：限制接入路径为绝对协议路径，不在目录中提供凭据、查询参数或跨主机重定向。
/// 参数说明：
/// - `value`：原生 endpoint、WebSocket path 或 Socket.IO namespace。
///
/// 返回：合法路径；客户端仍须分别使用 transport path 和业务路由 endpoint。
fn path(value: &str) -> anyhow::Result<()> {
    anyhow::ensure!(
        value.starts_with('/')
            && value.len() <= 256
            && !value.contains("//")
            && !value
                .bytes()
                .any(|byte| byte.is_ascii_control() || b"?#%,\\".contains(&byte))
            && !value.split('/').any(|part| part == "." || part == ".."),
        "access_path_invalid"
    );
    Ok(())
}

impl Ledger {
    /// 业务作用：为已确认的精确路由许可安装有限地址声明，旧请求重试不能刷新目录寿命。
    /// 参数说明：
    /// - `permit`：同一候选中刚验证或续发的路由许可。
    /// - `listener`：节点已准备的公开接入材料；None 撤下该节点路由的旧声明。
    ///
    /// 返回：目录和许可保持同一提交边界；不更改任何业务权限。
    pub(super) fn publish_access(
        &mut self,
        permit: &super::Permit,
        listener: Option<&ListenerAccess>,
    ) -> anyhow::Result<()> {
        let node = self
            .nodes
            .get_mut(&permit.node_id)
            .ok_or_else(|| anyhow::anyhow!("node_missing"))?;
        if let Some(listener) = listener {
            let serving = node
                .access
                .get(&permit.binding.route_id)
                .is_some_and(|previous| {
                    previous.serving
                        && previous.permit_id == permit.id
                        && previous.listener == *listener
                });
            let published = PublishedAccess {
                serving,
                binding: permit.binding.clone(),
                node_id: permit.node_id.clone(),
                incarnation: permit.incarnation.to_string(),
                permit_id: permit.id.clone(),
                request_nonce: permit.request_nonce.clone(),
                issued_at: permit.granted_at,
                expires_at: UnixMillis::new(
                    permit
                        .granted_at
                        .as_millis()
                        .checked_add(permit.ttl_ms)
                        .ok_or_else(|| anyhow::anyhow!("access_time_overflow"))?,
                )
                .map_err(anyhow::Error::msg)?,
                listener: listener.clone(),
            };
            node.access
                .insert(permit.binding.route_id.clone(), published);
        } else {
            node.access.remove(&permit.binding.route_id);
        }
        Ok(())
    }

    /// 业务作用：只返回与当前定义、节点代际及许可一致的有限地址，失联和过期声明不作为可用入口。
    /// 参数说明：
    /// - `route_id`：已经通过管理身份路由范围验证的稳定路由。
    /// - `now`：查询时的整数毫秒时间；早于声明时刻也拒绝该声明。
    ///
    /// 返回：各节点独立地址；该结果仅用于发现，连接仍须完整认证和路由门禁。
    pub fn public_access(&self, route_id: &str, now: UnixMillis) -> Vec<PublishedAccess> {
        let Some(route) = self.routes.get(route_id) else {
            return Vec::new();
        };
        let binding = Binding::of(&route.definition);
        if !route.definition.route.enabled
            || !matches!(
                self.operations[&route.operation_id].phase,
                Phase::Activating | Phase::Effective
            )
        {
            return Vec::new();
        }
        self.nodes
            .values()
            .filter_map(|node| {
                let access = node.access.get(route_id)?;
                let permit = self.permits.get(&access.permit_id)?;
                (access.serving
                    && node.active
                    && node.serving
                    && node.incarnation.to_string() == access.incarnation
                    && node.prepared.get(route_id) == Some(&binding)
                    && access.binding == binding
                    && permit.node_id == access.node_id
                    && permit.incarnation == node.incarnation
                    && permit.binding == binding
                    && permit.request_nonce == access.request_nonce
                    && now >= access.issued_at
                    && now < access.expires_at)
                    .then(|| access.clone())
            })
            .collect()
    }

    /// 业务作用：恢复完整目录时校验所有地址声明与持久节点历史，无法关联的地址不能对外发布。
    /// 参数说明: 无。
    ///
    /// 返回：有限、精确关联的目录；旧许可已经排空的记录允许保留但不会被查询返回。
    pub(super) fn validate_access(&self) -> anyhow::Result<()> {
        for (node_id, node) in &self.nodes {
            anyhow::ensure!(node.access.len() <= 1024, "access_baseline_capacity");
            for (route_id, access) in &node.access {
                access.listener.validate()?;
                access.binding.validate()?;
                identifier(&access.permit_id)?;
                identifier(&access.request_nonce)?;
                anyhow::ensure!(
                    access.node_id == *node_id
                        && access.incarnation == node.incarnation.to_string()
                        && access.binding.route_id == *route_id
                        && self.routes.contains_key(route_id)
                        && access.expires_at > access.issued_at
                        && access.expires_at.as_millis() - access.issued_at.as_millis() <= 60000,
                    "access_baseline_invalid"
                );
            }
        }
        Ok(())
    }
}
