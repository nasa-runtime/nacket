//! 客户端发送须先取得业务授权；本模块只生产 MQ 记录，不能调用本地分发器。

use super::{
    actions::{decode_arguments, reply, Reply},
    authority::CallContext,
    Connected, Gateway,
};
use crate::{
    adapters::outbound::{
        identity::now,
        publishing::{Guard, Outcome, Publication},
    },
    config::publishing::Purpose,
    domain::{
        message::{
            control,
            envelope::{ClientRequest, Effect},
            publication::Authorization,
        },
        route::policies::PayloadAccess,
    },
};
use bytes::Bytes;
use sha2::{Digest, Sha256};
use std::{sync::Arc, time::Duration};
use tokio::time::Instant;

impl Gateway {
    /// 业务作用：为显式开放的客户端发送取得绑定正文和固定消息身份的业务授权，然后只向批准 MQ 发布。
    /// 参数说明：
    /// - `connection`：当前真实主体及冻结路由。
    /// - `request`：已校验的候选目标、事件和 operationId。
    /// - `body`：授权和发布共用的不可变原始正文。
    ///
    /// 返回：拒绝、真实 broker 接受或结果未知，不产生本地直发。
    pub(super) async fn authorized_publication(
        &self,
        connection: &Arc<Connected>,
        request: &ClientRequest,
        body: Bytes,
    ) -> anyhow::Result<Reply> {
        let policy = &connection.plan.definition.client_messages;
        let endpoint = policy
            .endpoint_ref
            .as_ref()
            .and_then(|id| connection.plan.endpoints.get(id))
            .ok_or_else(|| anyhow::anyhow!("publication_authority_missing"))?;
        let publisher = policy
            .publisher_ref
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("publisher_missing"))?;
        let identity = connection.session.identity();
        let request_id = control::nonce();
        let digest = format!("sha256:{:x}", Sha256::digest(&body));
        let arguments = if policy.payload_access == PayloadAccess::Json {
            decode_arguments(connection, request, Some(body.clone()))?
        } else {
            serde_json::Value::Null
        };
        let input = serde_json::json!({
            "requestId": request_id,
            "operation": "authorize_publication",
            "operationId": request.operation_id,
            "context": CallContext {
                key: &connection.session.key,
                attempt_id: &connection.attempt_id
            },
            "principal": {
                "uid": identity.uid,
                "tenantId": identity.tenant
            },
            "target": request.target,
            "event": request.event,
            "schema": request.schema,
            "payload": {
                "bytes": body.len(),
                "digest": digest
            },
            "arguments": arguments
        });
        let started = Instant::now();
        let wall = now()?;
        let raw = (policy.payload_access == PayloadAccess::RawBody).then(|| body.clone());
        let response: Authorization = match self
            .calls
            .call_payload_observed(
                policy.endpoint_ref.as_deref().unwrap_or(""),
                endpoint,
                &connection.facts.input(&input),
                raw,
                identity.deadline,
            )
            .await
        {
            Ok(response) => response,
            Err(_) => {
                return Ok(reply(
                    connection,
                    request,
                    "publication_authorization_unavailable",
                    Effect::None,
                    None,
                    None,
                ))
            }
        };
        anyhow::ensure!(
            response.request_id == request_id
                && Some(&response.operation_id) == request.operation_id.as_ref(),
            "publication_response_mismatch"
        );
        if response.decision == "deny" {
            return Ok(reply(
                connection,
                request,
                "permission_denied",
                Effect::None,
                None,
                None,
            ));
        }
        anyhow::ensure!(response.decision == "allow", "publication_decision_invalid");
        let publication = response
            .publication
            .ok_or_else(|| anyhow::anyhow!("publication_missing"))?;
        let current = now()?;
        anyhow::ensure!(
            current >= wall && publication.expires_at > current,
            "publication_expired"
        );
        let remaining = publication.expires_at.as_millis() - current.as_millis();
        anyhow::ensure!(
            remaining <= endpoint.max_identity_ttl_ms,
            "publication_ttl_invalid"
        );
        let delivery = &connection.plan.definition.delivery;
        publication.validate(
            &connection.session.key,
            &identity.tenant,
            &identity.uid,
            request,
            &digest,
            current,
            delivery.max_ttl_ms,
            delivery.future_skew_ms,
        )?;
        let deadline = (started + Duration::from_millis(remaining))
            .min(identity.deadline)
            .min(
                started
                    + Duration::from_millis(
                        publication.push.delivery.expires_at.as_millis() - current.as_millis(),
                    ),
            );
        // 发布器再次取得当前会话许可；远程批准不能延长路由、凭据或已经取消的操作资格。
        let command = Publication {
            ordering_key: super::dispatch::ordering_key(&publication.push),
            metadata: Bytes::from(serde_json::to_vec(&publication.push)?),
            body,
            deadline,
            guard: Guard::Session(Arc::downgrade(connection)),
        };
        let outcome = self
            .publishers
            .get()
            .ok_or_else(|| anyhow::anyhow!("publisher_unavailable"))?
            .publish(
                publisher,
                Purpose::Push,
                &connection.session.key.route_id,
                command,
            )
            .await;
        Ok(publish_reply(connection, request, outcome))
    }

    /// 业务作用：把白名单通知作为业务命令提交独立 MQ 出口，不附带任意收件人或下行权限。
    /// 参数说明：
    /// - `connection`：已通过通知授权的真实会话。
    /// - `request`：固定操作和持久业务 operationId。
    /// - `body`：按路由唯一 schema 编码的原始正文。
    /// - `publisher`：操作冻结的业务通知出口。
    /// - `deadline`：本次权限的最短截止时间。
    ///
    /// 返回：只向请求者报告发布阶段，业务完成情况由业务服务另行处理。
    pub(super) async fn business_signal(
        &self,
        connection: &Arc<Connected>,
        request: &ClientRequest,
        body: Option<Bytes>,
        publisher: &str,
        deadline: Instant,
    ) -> anyhow::Result<Reply> {
        let operation_id = request
            .operation_id
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("operation_id_required"))?;
        let identity = connection.session.identity();
        let created_at = now()?;
        let remaining = deadline
            .saturating_duration_since(Instant::now())
            .as_millis()
            .min(30_000) as u64;
        anyhow::ensure!(remaining > 0, "operation_permission_expired");
        let input = serde_json::json!({
            "kind": "business_signal",
            "operationId": operation_id,
            "requestId": request.id,
            "context": CallContext {
                key: &connection.session.key,
                attempt_id: &connection.attempt_id
            },
            "principal": {
                "uid": identity.uid,
                "tenantId": identity.tenant
            },
            "event": request.event,
            "schema": request.schema,
            "bodyPresent": body.is_some(),
            "createdAt": created_at,
            "expiresAt": created_at.as_millis() + remaining
        });
        let command = Publication {
            metadata: Bytes::from(serde_json::to_vec(&input)?),
            body: body.unwrap_or_default(),
            ordering_key: serde_json::to_vec(&(
                &connection.session.key.cluster_id,
                &connection.session.key.scope_epoch,
                &connection.session.key.route_id,
                &identity.tenant,
                &identity.uid,
            ))?,
            deadline,
            guard: Guard::Session(Arc::downgrade(connection)),
        };
        let outcome = self
            .publishers
            .get()
            .ok_or_else(|| anyhow::anyhow!("publisher_unavailable"))?
            .publish(
                publisher,
                Purpose::BusinessSignal,
                &connection.session.key.route_id,
                command,
            )
            .await;
        Ok(publish_reply(connection, request, outcome))
    }
}

/// 业务作用：把 broker 阶段投影为本请求结果，不能把发布确认解释成终端接收或业务完成。
/// 参数说明：
/// - `connection`：结果所属会话。
/// - `request`：关联客户端请求。
/// - `outcome`：发布 worker 的确定性观察。
///
/// 返回：固定阶段和副作用层级，不包含 broker 内部位置。
fn publish_reply(connection: &Connected, request: &ClientRequest, outcome: Outcome) -> Reply {
    let (code, effect, phase) = match outcome {
        Outcome::Accepted => ("broker_accepted", Effect::Accepted, "broker_accepted"),
        Outcome::NotDispatched => ("publish_not_dispatched", Effect::None, "publish"),
        Outcome::Unknown => ("publish_unknown", Effect::Unknown, "publish"),
    };
    let mut response = reply(connection, request, code, effect, None, None);
    response
        .metadata
        .result
        .as_mut()
        .expect("operation result")
        .phase = phase.into();
    response
}
