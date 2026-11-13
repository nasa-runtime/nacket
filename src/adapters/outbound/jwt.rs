//! 验签与 JWKS 轮换复用 nasa::oauth，产品只映射身份并限制代理断言的业务作用域。

use super::{
    http::RemoteCalls,
    identity::{now, Principal},
};
use crate::{
    config::identity::{Keys, Verifier},
    domain::message::{
        envelope::{identifier, revision, user_identifier},
        json, UnixMillis,
    },
};
use nasa::oauth::{JwkSet, TokenPolicy};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime},
};
use tokio::time::Instant;

struct Snapshot {
    /// 已经验证结构并接受的公钥集合，不包含 token 指定的任意远程来源。
    keys: Arc<JwkSet>,
    /// 本地取得该快照的单调时刻，用于核对最大陈旧期限。
    observed: Instant,
}
#[derive(Default)]
struct KeyState {
    /// 当前可用公钥快照，缺失或超期时不能凭旧材料继续放行。
    snapshot: Option<Snapshot>,
    /// 允许下次获取公钥的单调时刻，未知 kid 不能绕过失败退避。
    next_attempt: Option<Instant>,
}

/// 按完整验签配置隔离公钥缓存与刷新退避，并拒绝墙钟回退延长身份。
#[derive(Default)]
pub struct Verifiers {
    /// 以完整验证配置摘要为键的公钥状态目录，同配置请求合并刷新。
    entries: Mutex<BTreeMap<[u8; 32], Arc<tokio::sync::Mutex<KeyState>>>>,
    /// 已观察的 Unix 毫秒时间下界，墙钟回退不能恢复过期身份。
    clock: Mutex<Option<UnixMillis>>,
}

/// 完成签名、声明及映射验证后的身份材料，使用时仍受截止点与撤权约束。
pub struct Evidence {
    /// 签名及声明校验后得到的真实用户和租户。
    pub principal: Principal,
    /// 已验证声明中的授权证据 ID，不能由未验证字段覆盖。
    pub authorization_id: String,
    /// 已验证身份授权的单调版本，用于复验持久撤权下界。
    pub version: u64,
    /// 凭据到期的 Unix 毫秒整数，由标准 JWT 秒时间转换。
    pub expires_at: UnixMillis,
    /// 按验证时刻固定的单调凭据截止点，刷新不能延长原凭据。
    pub deadline: Instant,
    /// 已经验签及验证范围的声明，仅用于固定受信映射。
    pub claims: Value,
}

impl Verifiers {
    /// 业务作用：预取将要开放的远程公钥，使新路由不会在无验证能力时接流。
    /// 参数说明：
    /// - `configuration`：冻结的验签策略。
    /// - `calls`：应用受管 HTTP 能力。
    /// - `deadline`：本轮准备的总截止点。
    ///
    /// 返回：已取得合法密钥；仍旧或读取失败的来源不满足首次准备。
    pub async fn prepare(
        &self,
        configuration: &Verifier,
        calls: &RemoteCalls,
        deadline: Instant,
    ) -> anyhow::Result<()> {
        self.keys(configuration, calls, None, deadline)
            .await
            .map(|_| ())
    }

    /// 业务作用：使用固定来源和算法验证令牌，再映射本路由需要的真实主体与授权期限。
    /// 参数说明：
    /// - `configuration`：与路由摘要绑定的验证策略。
    /// - `calls`：共享远程公钥读取入口。
    /// - `token`：尚未可信的紧凑 JWT，不允许自带密钥来源。
    /// - `deadline`：认证原始截止点。
    ///
    /// 返回：已验签身份和声明；过期、未知密钥、歧义 JSON 或时钟回退均拒绝。
    pub async fn verify(
        &self,
        configuration: &Verifier,
        calls: &RemoteCalls,
        token: &str,
        deadline: Instant,
    ) -> anyhow::Result<Evidence> {
        anyhow::ensure!(
            !token.is_empty() && token.len() <= 8192,
            "invalid_token_capacity"
        );
        let started = Instant::now();
        let wall = now()?;
        {
            let mut clock = self.clock.lock().unwrap_or_else(|e| e.into_inner());
            anyhow::ensure!(
                clock.is_none_or(|last| wall >= last),
                "identity_clock_regressed"
            );
            *clock = Some(wall);
        }
        let parts: Vec<_> = token.split('.').collect();
        anyhow::ensure!(parts.len() == 3, "invalid_token");
        let header_bytes = nasa::crypto::base64_url_decode(parts[0])?;
        let header: Value = json::decode(&header_bytes, 2048)?;
        let object = header
            .as_object()
            .ok_or_else(|| anyhow::anyhow!("invalid_token_header"))?;
        anyhow::ensure!(
            object
                .keys()
                .all(|key| matches!(key.as_str(), "alg" | "typ" | "kid")),
            "unsupported_token_header"
        );
        let kid = object
            .get("kid")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow::anyhow!("token_kid_missing"))?;
        identifier(kid)?;
        let claims: Value = json::decode(&nasa::crypto::base64_url_decode(parts[1])?, 8192)?;
        let keys = self.keys(configuration, calls, Some(kid), deadline).await?;
        let policy = TokenPolicy {
            expected_issuer: configuration.issuer.clone(),
            expected_audience: configuration.audience.clone(),
            allowed_algorithms: configuration.algorithms.iter().cloned().collect(),
            leeway_secs: configuration.clock_tolerance_ms / 1000,
        };
        let verified = nasa::oauth::verify_access_token(token, &keys, &policy, SystemTime::now())?;
        let expires = verified
            .exp
            .and_then(|v| v.checked_mul(1000))
            .ok_or_else(|| anyhow::anyhow!("invalid_token_expiry"))?;
        let issued = verified
            .iat
            .and_then(|v| v.checked_mul(1000))
            .ok_or_else(|| anyhow::anyhow!("token_issued_at_required"))?;
        let current = now()?;
        anyhow::ensure!(
            current >= wall
                && expires > current.as_millis()
                && expires > issued
                && expires - issued <= configuration.max_token_ttl_ms,
            "token_expired_or_ttl"
        );
        let expires_at = UnixMillis::new(expires).map_err(anyhow::Error::msg)?;
        let deadline = started + Duration::from_millis(expires - current.as_millis());
        anyhow::ensure!(deadline > Instant::now(), "token_expired");
        let read = |pointer: &str| -> anyhow::Result<String> {
            let value = claims
                .pointer(pointer)
                .and_then(Value::as_str)
                .ok_or_else(|| anyhow::anyhow!("identity_claim_missing"))?;
            identifier(value)?;
            Ok(value.into())
        };
        let uid = read(&configuration.claims.uid)?;
        // 有效签名只证明声明来源；空白主体仍须在消费代理断言或建立会话前拒绝。
        user_identifier(&uid)?;
        Ok(Evidence {
            principal: Principal {
                uid,
                tenant_id: read(&configuration.claims.tenant)?,
            },
            authorization_id: read(&configuration.claims.authorization_id)?,
            version: revision(&read(&configuration.claims.authorization_version)?)?,
            expires_at,
            deadline,
            claims,
        })
    }

    /// 业务作用：合并同一验证策略的密钥读取，保留有期限的有效快照并抑制未知 kid 引起的刷新风暴。
    /// 参数说明：
    /// - `configuration`：已验证的完整密钥来源策略。
    /// - `calls`：应用唯一 HTTP 生命周期。
    /// - `kid`：本次令牌要求的密钥，启动预取时为 None。
    /// - `deadline`：包含排队和网络等待的绝对截止点。
    ///
    /// 返回：仍在新鲜度期限内的快照；读取失败不会把 last-good 的时间重新计起。
    async fn keys(
        &self,
        configuration: &Verifier,
        calls: &RemoteCalls,
        kid: Option<&str>,
        deadline: Instant,
    ) -> anyhow::Result<Arc<JwkSet>> {
        let key: [u8; 32] = Sha256::digest(serde_json::to_vec(configuration)?).into();
        let entry = {
            let mut entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());
            if !entries.contains_key(&key) {
                anyhow::ensure!(entries.len() < 128, "verifier_runtime_capacity");
                entries.insert(key, Arc::new(tokio::sync::Mutex::new(KeyState::default())));
            }
            entries[&key].clone()
        };
        let mut state = tokio::time::timeout_at(deadline, entry.lock()).await?;
        let (refresh, stale) = match configuration.keys {
            Keys::Static {
                ..
            } => (Duration::MAX, Duration::MAX),
            Keys::Remote {
                refresh_interval_ms,
                stale_after_ms,
                ..
            } => (
                Duration::from_millis(refresh_interval_ms),
                Duration::from_millis(stale_after_ms),
            ),
        };
        let needs_refresh = state.snapshot.as_ref().is_none_or(|s| {
            s.observed.elapsed() >= refresh || kid.is_some_and(|kid| s.keys.find(kid).is_none())
        });
        if needs_refresh && state.next_attempt.is_none_or(|time| time <= Instant::now()) {
            // 刷新开始便登记短暂退避，失败和取消都不能让每个未知 kid 立即发起新的请求。
            state.next_attempt = Some(Instant::now() + Duration::from_secs(1));
            let started = Instant::now();
            let value = match &configuration.keys {
                Keys::Static {
                    jwks,
                } => Ok(jwks.clone()),
                Keys::Remote {
                    client_ref,
                    url,
                    timeout_ms,
                    ..
                } => {
                    calls
                        .document(
                            client_ref,
                            url,
                            deadline.min(started + Duration::from_millis(*timeout_ms)),
                        )
                        .await
                }
            };
            if let Ok(value) = value {
                if let Ok(keys) = serde_json::from_value::<JwkSet>(value) {
                    if keys.validate_with_max_keys(32).is_ok() {
                        state.snapshot = Some(Snapshot {
                            keys: Arc::new(keys),
                            observed: started,
                        });
                    }
                }
            }
        }
        let snapshot = state
            .snapshot
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("identity_keys_unavailable"))?;
        anyhow::ensure!(
            snapshot.observed.elapsed() < stale
                && kid.is_none_or(|kid| snapshot.keys.find(kid).is_some()),
            "identity_keys_stale_or_unknown"
        );
        Ok(snapshot.keys.clone())
    }
}
