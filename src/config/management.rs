use serde::Deserialize;
use std::{
    collections::{BTreeMap, BTreeSet},
    net::IpAddr,
};

/// 管理身份与节点控制身份分开；网络允许列表只核对真实 TCP 对端。
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(default, rename_all = "camelCase", deny_unknown_fields)]
pub struct Management {
    /// 是否安装受权管理 HTTP 入口；关闭时不提供路由写操作。
    pub enabled: bool,
    /// 允许访问管理入口的真实网络来源，不能用客户端 header 绕过。
    pub allowed_peers: BTreeSet<IpAddr>,
    /// 固定管理身份目录，先验证身份与网络再解析和执行管理正文。
    pub identities: BTreeMap<String, Identity>,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
/// 管理 HTTP 凭据与控制主体的绑定，只读限制在提交任何变更前生效。
pub struct Identity {
    /// 当前管理身份的 Bearer 凭据引用，不与业务认证或监控读取凭据混用。
    pub token_secret_ref: String,
    /// 该管理身份关联的控制签名主体，路由范围由控制密钥权限决定。
    pub control_key_id: String,
    #[serde(default)]
    /// 是否只允许 GET；开启时在读取写操作正文和提交控制请求前拒绝变更。
    pub read_only: bool,
}

impl Management {
    /// 业务作用：在监听前检查管理网络与凭据边界，关闭状态不加载凭据或创建管理入口。
    /// 参数说明: 无。
    ///
    /// 返回：启用时具有显式来源和有限身份集合；空范围不解释为放行所有来源。
    pub fn validate(&self) -> anyhow::Result<()> {
        if !self.enabled {
            return Ok(());
        }
        anyhow::ensure!(
            (1..=64).contains(&self.identities.len())
                && (1..=256).contains(&self.allowed_peers.len()),
            "management_boundary_required"
        );
        for (id, identity) in &self.identities {
            crate::domain::message::envelope::identifier(id)?;
            crate::domain::message::envelope::identifier(&identity.control_key_id)?;
            anyhow::ensure!(
                !identity.token_secret_ref.is_empty()
                    && identity.token_secret_ref.len() <= 128
                    && identity
                        .token_secret_ref
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_'),
                "management_secret_ref_invalid"
            );
        }
        Ok(())
    }
}
