//! 一次性代理断言的持久消费记录；只保存摘要和固定到期时间，不保存凭据正文。

use crate::domain::message::{envelope::SessionKey, UnixMillis};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

/// 同一持久断言目录最多保留的消费证据数，容量满时拒绝新消费而不淘汰有效记录。
pub const CAPACITY: usize = 16_384;

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
/// 一次性断言的精确消费归属及原到期时间，不保存凭据正文。
pub struct Claim {
    /// 完整会话键与认证尝试 ID 的确定摘要，精确重试之外的消费均拒绝。
    owner: String,
    /// 原断言的 Unix 毫秒截止时刻，重试不能延长消费记录寿命。
    expires_at: UnixMillis,
}

/// 清理记录和推进时间下界必须在同一个持久提交内完成，旧断言不能借节点时钟回退重新使用。
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Replay {
    /// 与清理同次持久提交的 Unix 毫秒时间下界，时钟回退也不能复活已清理断言。
    floor: UnixMillis,
    /// 按受权断言作用域摘要保存的消费 owner，容量满不淘汰仍有效的消费证据。
    claims: BTreeMap<String, Claim>,
}

impl Default for Replay {
    /// 业务作用：建立尚未消费任何代理断言的候选基线。
    /// 参数说明: 无。
    ///
    /// 返回：零时间下界和空目录；恢复已有作用域时不能用此值替代缺失文件。
    fn default() -> Self {
        Self {
            floor: UnixMillis::new(0).expect("zero timestamp"),
            claims: BTreeMap::new(),
        }
    }
}

impl Replay {
    /// 业务作用：校验恢复的消费目录具有完整摘要、有限容量和未清理的有效窗口。
    /// 参数说明: 无。
    ///
    /// 返回：合法持久基线；不完整、超限或早于时间下界的记录拒绝恢复。
    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(self.claims.len() <= CAPACITY, "assertion_capacity");
        for (key, claim) in &self.claims {
            digest(key)?;
            digest(&claim.owner)?;
            anyhow::ensure!(claim.expires_at > self.floor, "assertion_baseline_invalid");
        }
        Ok(())
    }

    /// 业务作用：在候选中占用一次性断言，跨节点及跨重启使用相同去重域。
    /// 参数说明：
    /// - `key`：签发者、受众、集群作用域、路由和断言 ID 的摘要。
    /// - `owner`：精确 SessionKey 与认证 attemptId 的摘要。
    /// - `expires_at`：验签得到的原始整数毫秒到期时刻。
    /// - `now`：当前权威读取的时间，只能推进持久下界。
    ///
    /// 返回：首次消费或同一尝试的等价重试成功；其他使用者、旧时间和容量不足均拒绝。
    pub fn consume(
        &mut self,
        key: &str,
        owner: &str,
        expires_at: UnixMillis,
        now: UnixMillis,
    ) -> anyhow::Result<()> {
        digest(key)?;
        digest(owner)?;
        let floor = self.floor.max(now);
        anyhow::ensure!(
            expires_at > floor
                && expires_at.as_millis().saturating_sub(now.as_millis()) <= 3_630_000,
            "gateway_assertion_expired_or_clock"
        );
        if let Some(previous) = self.claims.get(key) {
            anyhow::ensure!(
                previous.owner == owner && previous.expires_at == expires_at,
                "gateway_assertion_replayed"
            );
        } else {
            anyhow::ensure!(
                self.claims
                    .values()
                    .filter(|claim| claim.expires_at > floor)
                    .count()
                    < CAPACITY,
                "gateway_assertion_capacity"
            );
        }
        // 下界与清理一起提交，删除旧摘要后仍拒绝其原始 expiresAt；时钟超前只会收紧准入。
        self.floor = floor;
        self.claims.retain(|_, claim| claim.expires_at > floor);
        self.claims.insert(
            key.into(),
            Claim {
                owner: owner.into(),
                expires_at,
            },
        );
        Ok(())
    }
}

/// 业务作用：将完整接入尝试映射为固定长度的持久拥有者，不暴露会话身份字段。
/// 参数说明：
/// - `key`：服务端生成并冻结的完整会话键。
/// - `attempt_id`：本次认证请求的关联标识。
///
/// 返回：确定性 SHA-256 摘要；节点、代际、路由或尝试变化都会产生不同拥有者。
pub fn owner(key: &SessionKey, attempt_id: &str) -> anyhow::Result<String> {
    key.validate()?;
    crate::domain::message::envelope::identifier(attempt_id)?;
    Ok(format!(
        "{:x}",
        Sha256::digest(serde_json::to_vec(&(key, attempt_id))?)
    ))
}

/// 业务作用：拒绝非规范摘要，保证消费目录的键长和内存占用有界。
/// 参数说明：
/// - `value`：持久记录或消费请求携带的摘要。
///
/// 返回：64 位小写十六进制字符串有效，其他输入拒绝。
fn digest(value: &str) -> anyhow::Result<()> {
    anyhow::ensure!(
        value.len() == 64
            && value
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)),
        "assertion_digest_invalid"
    );
    Ok(())
}
