use super::Gateway;
use crate::domain::{
    control::{Command, Reply},
    message::{envelope::SessionKey, UnixMillis},
};
use sha2::{Digest, Sha256};
use tokio::time::Instant;

impl Gateway {
    /// 业务作用：在发布身份之前取得一次性断言的持久消费确认，跨节点不能回退到本地去重。
    /// 参数说明：
    /// - `scope`：签发者、受众、集群、作用域、路由和断言 ID 的规范编码。
    /// - `key`：服务端冻结的完整 SessionKey。
    /// - `attempt_id`：本次认证关联标识。
    /// - `expires_at`：已验签断言的原始整数时间。
    /// - `deadline`：接入流程与断言共同限制的等待截止点。
    ///
    /// 返回：唯一消费持久成功；超时或结果未知均拒绝本次接入，消费记录不会撤回。
    pub(super) async fn consume_assertion(
        &self,
        scope: &str,
        key: &SessionKey,
        attempt_id: &str,
        expires_at: UnixMillis,
        deadline: Instant,
    ) -> anyhow::Result<()> {
        let digest = format!("{:x}", Sha256::digest(scope.as_bytes()));
        tokio::time::timeout_at(deadline, async {
            if self.configuration.control.clustered() {
                let control = self
                    .control
                    .get()
                    .ok_or_else(|| anyhow::anyhow!("assertion_authority_unavailable"))?;
                let reply = control
                    .node(Command::ConsumeAssertion {
                        session_key: key.clone(),
                        attempt_id: attempt_id.into(),
                        digest,
                        expires_at,
                    })
                    .await?;
                anyhow::ensure!(
                    matches!(reply, Reply::Recorded {}),
                    "assertion_reply_invalid"
                );
                Ok(())
            } else {
                self.routes
                    .consume_local_assertion(
                        digest,
                        crate::domain::assertion::owner(key, attempt_id)?,
                        expires_at,
                    )
                    .await
            }
        })
        .await?
    }
}
