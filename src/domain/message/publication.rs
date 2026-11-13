//! 业务授权服务固定消息身份，重新连接或跨节点重试不重新分配消息 ID 和存活时间。

use super::{
    envelope::{identifier, validate_digest, ClientRequest, Push, SessionKey, Target},
    UnixMillis,
};
use serde::Deserialize;

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
/// 业务端对一次客户端发布请求的关联裁决，允许结果还须提供可复验的固定发布许可。
pub struct Authorization {
    /// 必须匹配本次远程授权请求的精确关联 ID。
    pub request_id: String,
    /// 业务侧持久操作 ID，重试不得换成新的发送身份。
    pub operation_id: String,
    /// 业务明确允许或拒绝的固定决策文本，未知值不能放行。
    pub decision: String,
    /// 允许时返回的完整固定发布许可，拒绝不能携带可执行下行。
    pub publication: Option<AuthorizedPublication>,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
/// 绑定消息身份、正文摘要、目标与期限的业务发布许可，重试不得扩权或延长存活。
pub struct AuthorizedPublication {
    /// 业务侧固定的消息发布 ID，与 push.id 一致，跨节点重投保持相同身份。
    pub publication_id: String,
    /// 业务服务持久授权记录的 ID，用于精确关联本次发布资格。
    pub grant_id: String,
    /// 发布资格的 Unix 毫秒截止时刻，提交前复验且不延长消息原期限。
    pub expires_at: UnixMillis,
    /// 授权绑定的原始业务字节 SHA-256 摘要，正文不同不得复用许可。
    pub body_digest: String,
    /// 经过授权服务收窄的完整下行信封，主体、定义、schema 和目标仍由节点复验。
    pub push: Push,
}

impl AuthorizedPublication {
    /// 业务作用：确认业务授权只缩小本次请求，所有可影响投递的字段都由同一持久授权记录固定。
    /// 参数说明：
    /// - `key`：真实会话的完整路由绑定。
    /// - `tenant`：已经验证的租户。
    /// - `uid`：已经验证的发送者。
    /// - `request`：本次客户端操作和候选目标。
    /// - `digest`：实际不可变正文的 SHA-256。
    /// - `now`：验证时整数毫秒时钟。
    /// - `maximum_ttl_ms`：路由允许的消息存活上限。
    /// - `future_skew_ms`：路由允许的生产时钟偏差。
    ///
    /// 返回：所有主体、绑定、内容和目标一致时通过；任何扩权或过期凭证都不能发布。
    #[expect(
        clippy::too_many_arguments,
        reason = "发布许可须同时核对主体、精确会话、原文摘要与期限上限。"
    )]
    pub fn validate(
        &self,
        key: &SessionKey,
        tenant: &str,
        uid: &str,
        request: &ClientRequest,
        digest: &str,
        now: UnixMillis,
        maximum_ttl_ms: u64,
        future_skew_ms: u64,
    ) -> anyhow::Result<()> {
        identifier(&self.publication_id)?;
        identifier(&self.grant_id)?;
        validate_digest(&self.body_digest)?;
        self.push.validate(now, maximum_ttl_ms, future_skew_ms)?;
        let context = &self.push.context;
        anyhow::ensure!(
            self.expires_at > now
                && self.body_digest == digest
                && self.push.id == self.publication_id
                && self.push.operation_id == request.operation_id
                && context.cluster_id == key.cluster_id
                && context.scope_epoch == key.scope_epoch
                && context.route_id == key.route_id
                && context.route_revision == key.route_revision
                && context.definition_digest == key.definition_digest
                && context.tenant_id == tenant
                && self.push.event == request.event
                && Some(self.push.schema.as_str()) == request.schema.as_deref(),
            "publication_binding_mismatch"
        );
        let sender = self
            .push
            .sender
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("publication_sender_missing"))?;
        anyhow::ensure!(sender.uid == uid, "publication_sender_mismatch");
        // 重试可以保留授权服务记录的原发送会话，但不能借用其它路由或集群的会话身份。
        if let Some(original) = &sender.session_key {
            anyhow::ensure!(
                original.cluster_id == key.cluster_id
                    && original.scope_epoch == key.scope_epoch
                    && original.route_id == key.route_id
                    && original.route_revision == key.route_revision
                    && original.definition_digest == key.definition_digest,
                "publication_sender_scope_mismatch"
            );
        }
        let within = match (&request.target, &self.push.target) {
            (
                Target::Group {
                    group_id: requested,
                },
                Target::Group {
                    group_id: approved,
                },
            ) => requested == approved,
            (
                Target::Uids {
                    uids: requested,
                },
                Target::Uids {
                    uids: approved,
                },
            ) => approved.iter().all(|uid| requested.contains(uid)),
            _ => false,
        };
        anyhow::ensure!(within, "publication_target_expansion");
        Ok(())
    }
}
