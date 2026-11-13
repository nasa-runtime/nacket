//! 本地验签只选择部署批准的密钥来源，令牌内的 URL 与自带密钥不参与发现。

use crate::domain::message::envelope::identifier;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
/// JWT 公钥来源，只接受部署选择的材料，不使用令牌中的远程地址。
pub enum Keys {
    /// 使用配置中的固定公钥集合。
    Static {
        /// 部署固定的公钥集合，不接受客户端提供签名验证材料。
        jwks: serde_json::Value,
    },
    /// 从固定 JWKS 地址受限刷新，超出陈旧期限停止放行。
    Remote {
        #[serde(rename = "clientRef")]
        /// 获取远程 JWKS 的受管 HTTP 客户端引用，共用节点调用预算。
        client_ref: String,
        /// 允许读取公钥的固定地址，token 内任意 URL 不会替换它。
        url: String,
        #[serde(rename = "refreshIntervalMs")]
        /// 公钥正常刷新间隔，单位毫秒；未知 kid 仍受合并请求与退避约束。
        refresh_interval_ms: u64,
        #[serde(rename = "staleAfterMs")]
        /// 公钥快照可接受的最大陈旧毫秒数，超过后不能继续用旧材料放行。
        stale_after_ms: u64,
        #[serde(rename = "timeoutMs")]
        /// 单次 JWKS 获取的最长等待毫秒数，同时受原认证截止点约束。
        timeout_ms: u64,
    },
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
/// 一条本地身份验证策略，签名、签发者、接收者和期限共同决定凭据有效性。
pub struct Verifier {
    /// 必须精确匹配的签发者，签名正确但签发者不同仍拒绝。
    pub issuer: String,
    /// 当前服务要求的接收者声明，不能只凭签名建立身份。
    pub audience: String,
    /// 部署允许的签名算法集合，token 不能自行选择目录外算法。
    pub algorithms: BTreeSet<String>,
    /// 公钥的固定配置或受管远程来源，不包含产品业务路由逻辑。
    pub keys: Keys,
    /// 可接受 token 有效跨度的上限，配置单位毫秒，JWT NumericDate 保持标准秒语义。
    pub max_token_ttl_ms: u64,
    #[serde(default)]
    /// 验证时间声明时允许的有限时钟误差毫秒数，不改变会话原始授权期限。
    pub clock_tolerance_ms: u64,
    /// 从已验证声明到内部身份字段的固定名称映射。
    pub claims: Claims,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
/// 已验证声明到内部身份的固定映射，字段名不表示已经取得相应权限。
pub struct Claims {
    /// 已验证 token 中用户身份声明的字段名，不是用户 ID 本身。
    pub uid: String,
    /// 已验证 token 中租户声明的字段名，参与跨租户隔离。
    pub tenant: String,
    /// 已验证 token 中授权证据 ID 的字段名，用于关联其业务来源。
    pub authorization_id: String,
    /// 已验证 token 中单调授权版本的字段名，用于复验持久撤权下界。
    pub authorization_version: String,
}

impl Verifier {
    /// 业务作用：在路由编译时冻结验签算法、来源与身份字段，不允许令牌自行改变安全策略。
    /// 参数说明: 无。
    ///
    /// 返回：有限且受支持的配置；未知算法、远程不安全地址或不完整公钥拒绝。
    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            !self.issuer.is_empty()
                && self.issuer.len() <= 1024
                && !self.audience.is_empty()
                && self.audience.len() <= 256,
            "invalid JWT issuer or audience"
        );
        anyhow::ensure!(
            self.algorithms.len() == 1 && self.algorithms.contains("RS256"),
            "unsupported JWT algorithms"
        );
        anyhow::ensure!(
            (1000..=3_600_000).contains(&self.max_token_ttl_ms)
                && self.clock_tolerance_ms <= 30_000
                && self.clock_tolerance_ms.is_multiple_of(1000),
            "invalid JWT time policy"
        );
        for pointer in [
            &self.claims.uid,
            &self.claims.tenant,
            &self.claims.authorization_id,
            &self.claims.authorization_version,
        ] {
            anyhow::ensure!(
                pointer.starts_with('/') && pointer.len() <= 256,
                "invalid JWT claim pointer"
            );
        }
        match &self.keys {
            Keys::Static {
                jwks,
            } => {
                anyhow::ensure!(serde_json::to_vec(jwks)?.len() <= 65_536, "JWKS capacity");
                serde_json::from_value::<nasa::oauth::JwkSet>(jwks.clone())?
                    .validate_with_max_keys(32)?;
            }
            Keys::Remote {
                client_ref,
                url,
                refresh_interval_ms,
                stale_after_ms,
                timeout_ms,
            } => {
                identifier(client_ref)?;
                let parsed = url::Url::parse(url)?;
                let loopback = parsed.host_str().is_some_and(|host| {
                    host == "localhost"
                        || host
                            .parse::<std::net::IpAddr>()
                            .is_ok_and(|ip| ip.is_loopback())
                });
                anyhow::ensure!(
                    url.len() <= 2048
                        && parsed.has_host()
                        && parsed.username().is_empty()
                        && parsed.password().is_none()
                        && parsed.fragment().is_none()
                        && parsed.query().is_none()
                        && (parsed.scheme() == "https" || (parsed.scheme() == "http" && loopback)),
                    "invalid JWKS URL"
                );
                anyhow::ensure!(
                    (1000..=300_000).contains(refresh_interval_ms)
                        && stale_after_ms > refresh_interval_ms
                        && *stale_after_ms <= 3_600_000
                        && (1..=30_000).contains(timeout_ms),
                    "invalid JWKS refresh policy"
                );
            }
        }
        Ok(())
    }
}
