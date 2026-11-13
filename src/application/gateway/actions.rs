use super::{
    authority::{AuthorizationResponse, CallContext, GroupEvidence},
    Connected, Gateway,
};
use crate::{
    adapters::outbound::identity::now,
    domain::{
        message::{
            control::{self, Downstream, PublicContext},
            envelope::{ActionResult, ClientRequest, Effect, Kind, Target},
            wire::Frame,
        },
        route::policies::*,
        session::{GroupGrant, SubscriptionBarrier, SubscriptionIntent},
    },
};
use bytes::Bytes;
use serde::Deserialize;
use std::{collections::BTreeMap, sync::Arc, time::Duration};
use tokio::time::Instant;

/// 接入只取得一次执行责任；重复请求持有原结果观察权。
#[expect(
    clippy::large_enum_variant,
    reason = "动作及正文已经持有节点条数和字节预算，保持唯一执行责任直接移动。"
)]
pub enum PreparedAction {
    /// 首次请求已取得唯一执行责任，实际动作仍须复验权限。
    Execute {
        /// 首次请求独占的执行责任，取消时必须向重复等待者保存终态。
        execution: super::requests::Execution,
        /// 已经建立准确意图或取消屏障的单次动作，执行前仍复验权限。
        work: ActionWork,
    },
    /// 相同请求只观察先前执行，不重复产生副作用。
    Replay(
        /// 同指纹重复请求只能等待的原执行记录。
        Arc<super::requests::Record>,
    ),
    /// 准备阶段已得到终态，直接返回而不再执行动作。
    Completed(
        /// 准备阶段已确定的回复，不再产生业务动作。
        Reply,
    ),
}

/// 读任务按接收顺序先登记意图，再把可取消远程等待交给独立受管任务。
pub enum ActionWork {
    /// 已登记订阅意图，授权完成时还须确认该意图未被取消。
    Subscribe {
        /// 已经核对方向、会话及请求指纹的控制元数据。
        request: ClientRequest,
        /// 本次订阅意图的精确标识，迟到允许结果不能覆盖后来的取消。
        intent: Arc<SubscriptionIntent>,
    },
    /// 订阅资格已经撤销，等待此前取得的写出责任排空。
    Unsubscribe {
        /// 已经核对方向、会话及请求指纹的控制元数据。
        request: ClientRequest,
        /// 取消订阅已经失效的旧写出集合，结束前不报告取消完成。
        barrier: SubscriptionBarrier,
    },
    /// 按路由白名单执行服务动作，不隐式取得向其它客户端发送的权限。
    Notify {
        /// 已经核对方向、会话及请求指纹的控制元数据。
        request: ClientRequest,
        /// 路由唯一模式的原始业务字节，按动作需要才解码。
        body: Option<Bytes>,
    },
    /// 经显式开放的业务校验路径请求发布，不能直接写入其它连接。
    Message {
        /// 已经核对方向、会话及请求指纹的控制元数据。
        request: ClientRequest,
        /// 路由唯一模式的原始业务字节，按动作需要才解码。
        body: Bytes,
    },
}

/// 给原请求者的控制结果与可选正文，重复等待者共享同一业务结果。
#[derive(Clone)]
pub struct Reply {
    /// 投影为客户端允许方向的固定控制回复。
    pub metadata: Downstream,
    /// 可选的路由唯一编码结果正文，通过共享 Bytes 在等待者间复用。
    pub body: Option<Bytes>,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct SubscriptionResponse {
    /// 必须回显当前远程请求 ID，不能接受其它调用的允许结果。
    request_id: String,
    /// 必须回显原订阅意图 ID，保证取消后的迟到响应无效。
    subscription_request_id: String,
    /// 业务明确给出的 allow 或 deny，未知结果拒绝订阅。
    decision: String,
    /// 业务允许的目标资格证据，不能扩展客户端本次请求范围。
    groups: Vec<GroupEvidence>,
    /// 可选有限长度业务结果码，用于解释本次订阅判定。
    code: Option<String>,
    /// 可选有限长度结果说明，不提供额外权限。
    message: Option<String>,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct RemoteResult {
    /// 当前 HTTP 调用关联 ID，响应必须精确匹配。
    request_id: String,
    /// 有副作用操作的业务幂等标识回显，不能用新 ID 重试未知结果。
    operation_id: Option<String>,
    /// 固定合同允许的操作结果状态，不把传输成功当作业务成功。
    status: String,
    /// 可选有限长度业务结果码，受公开回复字段边界约束。
    code: Option<String>,
    /// 可选有限长度结果说明，不作为可执行指令。
    message: Option<String>,
    /// 按声明结果 schema 编码的业务返回值，只有需要结果转换时才物化。
    result: Option<serde_json::Value>,
}

impl Gateway {
    /// 业务作用：在开放业务前完成必要初始化和初始群资格，任一步失败都保持连接不可用。
    /// 参数说明：
    /// - `connection`：已经确认定义但尚未 Ready 的会话。
    /// - `deadline`：整个建连流程共用的原始截止点。
    ///
    /// 返回：会话已准备接收 Ready；必要初始化失败不会开放业务门禁。
    pub async fn initialize(
        &self,
        connection: &Arc<Connected>,
        deadline: Instant,
    ) -> anyhow::Result<()> {
        connection.handshake_attempt.stage("quota_wait");
        let result = self
            .metrics
            .measure(
                crate::observability::Counter::Operations,
                "initialize",
                "initialization",
                "ok",
                async {
                    // 全局名额真正开放后才发起初始化；取消或旧 writer 未结束不能让新会话提前执行副作用。
                    connection.quota.wait(deadline).await?;
                    connection.handshake_attempt.stage("initialization_hook");
                    let hook = &connection.plan.definition.lifecycle.initialization;
                    if hook.enabled {
                        let result = self.initialization_hook(connection, hook, deadline).await;
                        if hook.required {
                            result?;
                        }
                    }
                    anyhow::ensure!(Instant::now() < deadline, "initialization_deadline");
                    connection.handshake_attempt.stage("session_ready");
                    let owner = connection.clone();
                    let intent = connection
                        .uid_events
                        .submit_control(deadline, 65536, move || {
                            // Ready 权限与初始意图在同一主体消费中发布，网络等待不能把迟到授权直接写入会话。
                            anyhow::ensure!(
                                !owner.cancellation.is_cancelled(),
                                "initialization_closed"
                            );
                            owner.session.ready()?;
                            let initial = match &owner.plan.definition.subscriptions.initial {
                                InitialSubscriptions::None {} => Vec::new(),
                                InitialSubscriptions::Configured {
                                    group_ids,
                                } => group_ids.clone(),
                                InitialSubscriptions::AuthorizedSnapshot {} => owner
                                    .grants
                                    .lock()
                                    .unwrap_or_else(|e| e.into_inner())
                                    .groups
                                    .iter()
                                    .map(|g| g.group_id.clone())
                                    .collect(),
                            };
                            if initial.is_empty() {
                                Ok(None)
                            } else {
                                Ok(Some(Arc::new(owner.session.begin_subscribe(initial)?)))
                            }
                        })?
                        .wait()
                        .await?;
                    if let Some(intent) = intent {
                        connection.handshake_attempt.stage("initial_subscriptions");
                        let result =
                            tokio::time::timeout_at(deadline, self.subscribe(connection, &intent))
                                .await;
                        if !matches!(&result, Ok(Ok(()))) {
                            // 失败只能立即收紧权限；已有写出责任仍由原 owner 排空，不能通过失败重开资格。
                            connection.session.fail_subscribe(&intent);
                            connection.session.close();
                        }
                        result??;
                    }
                    Ok(())
                },
            )
            .await;
        if let Err(error) = &result {
            connection.handshake_attempt.reject(error);
        } else {
            connection.handshake_attempt.stage("ready_write");
        }
        result
    }

    /// 业务作用：从固定 HTTP 或 MQ 授权服务取得初始化证据，未知动作或群字段不作为分发指令。
    /// 参数说明：
    /// - `connection`：当前已认证且尚未接流的会话。
    /// - `hook`：冻结的初始化回调与预算。
    /// - `deadline`：原始认证总截止时间。
    ///
    /// 返回：合法初始化结果，失败不替换已有连接授权快照。
    async fn initialization_hook(
        &self,
        connection: &Arc<Connected>,
        hook: &Hook,
        deadline: Instant,
    ) -> anyhow::Result<()> {
        self.metrics
            .measure(
                crate::observability::Counter::Authorization,
                "initialize",
                "authorization",
                "allow",
                async {
                    let identity = connection.session.identity();
                    let (mut response, expires) = match hook.target.as_ref() {
                        Some(HookTarget::Http {
                            endpoint_ref,
                        }) => {
                            let endpoint = connection
                                .plan
                                .endpoints
                                .get(endpoint_ref)
                                .ok_or_else(|| anyhow::anyhow!("initialization_unavailable"))?;
                            let request_id = control::nonce();
                            let started = Instant::now();
                            let wall = now()?;
                            let input = serde_json::json!({
                                "requestId": request_id,
                                "operation": "initialize",
                                "context": CallContext {
                                    key: &connection.session.key,
                                    attempt_id: &connection.attempt_id
                                },
                                "principal": {
                                    "uid": identity.uid,
                                    "tenantId": identity.tenant
                                }
                            });
                            let response: AuthorizationResponse = self
                                .calls
                                .call(
                                    endpoint_ref,
                                    endpoint,
                                    &connection.facts.input(&input),
                                    deadline.min(started + Duration::from_millis(hook.timeout_ms)),
                                )
                                .await?;
                            let (_, _, expires) =
                                response.evidence(&request_id, endpoint, started, wall)?;
                            (response, expires)
                        }
                        Some(HookTarget::Mq {
                            publisher_ref,
                        }) => {
                            self.initialize_mq(connection, publisher_ref, deadline)
                                .await?
                        }
                        None => anyhow::bail!("initialization_target_missing"),
                    };
                    anyhow::ensure!(
                        connection.session.live() && !connection.cancellation.is_cancelled(),
                        "initialization_closed"
                    );
                    anyhow::ensure!(
                        response.principal.is_none()
                            && response.grants.groups.len()
                                <= connection.plan.definition.subscriptions.max_per_session
                            && response.grants.allowed_operations.len() <= 128,
                        "initialization_scope_mismatch"
                    );
                    response.grants.bind(
                        connection.plan.definition.subscriptions.max_per_session,
                        expires.min(identity.deadline),
                    )?;
                    let owner = connection.clone();
                    connection
                        .uid_events
                        .submit_control(deadline.min(expires), 65536, move || {
                            anyhow::ensure!(
                                owner.session.live() && !owner.cancellation.is_cancelled(),
                                "initialization_closed"
                            );
                            *owner.grants.lock().unwrap_or_else(|e| e.into_inner()) =
                                response.grants;
                            owner.sync_receiving();
                            Ok(())
                        })?
                        .wait()
                        .await
                },
            )
            .await
    }

    /// 业务作用：按到达顺序验证客户端固定上行并登记订阅或取消意图，不在读任务里等待网络。
    /// 参数说明：
    /// - `connection`：已经进入 Ready 的当前路由会话。
    /// - `frame`：独立控制头和共享业务正文。
    ///
    /// 返回：有界待执行动作；拒绝不会获得任何跨客户端分发能力。
    pub fn prepare_action(
        &self,
        connection: &Arc<Connected>,
        frame: Frame,
    ) -> anyhow::Result<PreparedAction> {
        let request: ClientRequest = frame.metadata()?;
        request.validate(frame.body.is_some())?;
        let cancelled = reply(
            connection,
            &request,
            "operation_cancelled",
            if matches!(request.kind, Kind::Notify | Kind::Msg) {
                Effect::Unknown
            } else {
                Effect::None
            },
            None,
            None,
        );
        let admission = connection
            .requests
            .begin(&request, frame.body.as_ref(), cancelled)?;
        let execution = match admission {
            super::requests::Admission::Observe(record) => {
                return Ok(PreparedAction::Replay(record))
            }
            super::requests::Admission::Execute(execution) => execution,
        };
        match self.prepare_new_action(connection, frame) {
            Ok(work) => Ok(PreparedAction::Execute {
                execution,
                work,
            }),
            Err(error) => Ok(PreparedAction::Completed(execution.complete(reply(
                connection,
                &request,
                rejection_code(&error),
                Effect::None,
                None,
                None,
            )))),
        }
    }

    /// 业务作用：仅为已取得执行责任的新请求建立订阅意图与动作，缓存命中不进入此路径。
    /// 参数说明：
    /// - `connection`：当前会话和冻结路由。
    /// - `frame`：本次首次提交的原始控制头及业务字节。
    ///
    /// 返回：尚未执行远程业务的动作；本地门禁拒绝时不建立副作用任务。
    fn prepare_new_action(
        &self,
        connection: &Arc<Connected>,
        frame: Frame,
    ) -> anyhow::Result<ActionWork> {
        let request: ClientRequest = frame.metadata()?;
        anyhow::ensure!(
            connection.session.candidate(None).is_some(),
            "session_not_ready"
        );
        anyhow::ensure!(
            request.context.route_revision == connection.session.key.route_revision,
            "route_changed"
        );
        anyhow::ensure!(
            frame.body.is_none()
                || frame.mode == Some(connection.plan.definition.serialization.mode().ordinal()),
            "serialization_mismatch"
        );
        if let Some(body) = &frame.body {
            self.metrics
                .payload(frame.mode.expect("validated mode"), body.len());
        }
        match request.kind {
            Kind::Sub | Kind::Unsub => {
                anyhow::ensure!(
                    request.schema.as_deref() == Some("nacket.subscription"),
                    "schema_mismatch"
                );
                let body = frame.body.expect("validated body");
                let size = body.len();
                let groups = connection.plan.subscription_groups(body);
                self.metrics
                    .payload_decode("subscription", size, groups.is_ok());
                let groups = groups?;
                if request.kind == Kind::Sub {
                    anyhow::ensure!(
                        connection.plan.definition.subscriptions.enabled,
                        "subscriptions_disabled"
                    );
                    let intent = Arc::new(connection.session.begin_subscribe(groups)?);
                    Ok(ActionWork::Subscribe {
                        request,
                        intent,
                    })
                } else {
                    // 取消先阻止未来写出，异步等待仅负责确认已有写出已经结束。
                    let barrier = connection.session.unsubscribe(&groups)?;
                    for group in groups {
                        connection.receipts.cancel(None, Some(&group));
                    }
                    Ok(ActionWork::Unsubscribe {
                        request,
                        barrier,
                    })
                }
            }
            Kind::Notify => Ok(ActionWork::Notify {
                request,
                body: frame.body,
            }),
            Kind::Msg => Ok(ActionWork::Message {
                request,
                body: frame.body.expect("validated body"),
            }),
            _ => anyhow::bail!("invalid_client_profile"),
        }
    }

    /// 业务作用：执行已登记的有界操作，将结果只投影到发起连接。
    /// 参数说明：
    /// - `connection`：操作关联的冻结定义与会话。
    /// - `prepared`：唯一执行责任或既有结果的观察权。
    ///
    /// 返回：明确结果及可选 schema 正文；远程副作用结果未知时保留 unknown。
    pub async fn execute(
        &self,
        connection: &Arc<Connected>,
        prepared: PreparedAction,
    ) -> anyhow::Result<Reply> {
        match prepared {
            PreparedAction::Replay(record) => Ok(record.wait().await),
            PreparedAction::Completed(reply) => Ok(reply),
            PreparedAction::Execute {
                execution,
                work,
            } => {
                let reply = self.execute_once(connection, work).await;
                let session = connection.session.clone();
                connection
                    .uid_events
                    .submit_control(connection.session.identity().deadline, 65536, move || {
                        if reply
                            .metadata
                            .result
                            .as_ref()
                            .is_some_and(|result| result.code == "ok")
                        {
                            session.business_activity();
                        }
                        Ok(execution.complete(reply))
                    })?
                    .wait()
                    .await
            }
        }
    }

    /// 业务作用：执行唯一持有者的动作，重复请求不得再次计数或改变订阅状态。
    /// 参数说明：
    /// - `connection`：已取得执行责任的当前会话。
    /// - `work`：本次首次登记的具体业务动作。
    ///
    /// 返回：实际动作层级，尚未进行有界结果保留。
    async fn execute_once(&self, connection: &Arc<Connected>, work: ActionWork) -> Reply {
        let subject = match &work {
            ActionWork::Subscribe {
                ..
            } => "sub",
            ActionWork::Unsubscribe {
                ..
            } => "unsub",
            ActionWork::Notify {
                ..
            } => "notify",
            ActionWork::Message {
                ..
            } => "msg",
        };
        let mut scoped = self.metrics.scopes.event(
            crate::observability::scopes::Kind::Route,
            &connection.session.key.route_id,
            subject,
        );
        let _inflight = self.metrics.reserve("operations", 1);
        let mut observation = crate::observability::Operation::new(
            self.metrics.clone(),
            crate::observability::Counter::Operations,
            subject,
            "operation",
        );
        let result = match work {
            ActionWork::Subscribe {
                request,
                intent,
            } => {
                let result = self.subscribe(connection, &intent).await;
                if result.is_err() {
                    connection.session.fail_subscribe(&intent);
                }
                reply(
                    connection,
                    &request,
                    if result.is_ok() {
                        "ok"
                    } else {
                        "subscription_denied_or_superseded"
                    },
                    if result.is_ok() {
                        Effect::Completed
                    } else {
                        Effect::None
                    },
                    None,
                    None,
                )
            }
            ActionWork::Unsubscribe {
                request,
                barrier,
            } => {
                barrier.wait().await;
                reply(connection, &request, "ok", Effect::Completed, None, None)
            }
            ActionWork::Notify {
                request,
                body,
            } => match self.notify(connection, &request, body).await {
                Ok(reply) => reply,
                Err(_) => reply(
                    connection,
                    &request,
                    "operation_denied_or_unavailable",
                    Effect::None,
                    None,
                    None,
                ),
            },
            ActionWork::Message {
                request,
                body,
            } => match self.client_message(connection, &request, body).await {
                Ok(reply) => reply,
                Err(_) => reply(
                    connection,
                    &request,
                    "message_denied",
                    Effect::None,
                    None,
                    None,
                ),
            },
        };
        let outcome = match result.metadata.result.as_ref() {
            Some(result) if matches!(&result.effect, Effect::Unknown) => "unknown",
            Some(result) if result.code == "ok" => "ok",
            _ => "rejected",
        };
        observation.finish(outcome);
        scoped.finish(outcome);
        result
    }

    /// 业务作用：从路由唯一读取权威获取完整群批次，原子复验意图后才允许群投递。
    /// 参数说明：
    /// - `connection`：当前真实主体与冻结定义。
    /// - `intent`：先于远程调用登记的订阅意图。
    ///
    /// 返回：全部群一次生效；拒绝、迟到结果或容量变化均不产生部分授权。
    async fn subscribe(
        &self,
        connection: &Arc<Connected>,
        intent: &Arc<SubscriptionIntent>,
    ) -> anyhow::Result<()> {
        self.metrics
            .measure(
                crate::observability::Counter::Authorization,
                "subscribe",
                "authorization",
                "allow",
                async {
                    let policy = &connection.plan.definition.subscriptions;
                    let identity = connection.session.identity();
                    let started = Instant::now();
                    let wall = now()?;
                    let grants = match policy
                        .authorization
                        .as_ref()
                        .ok_or_else(|| anyhow::anyhow!("subscription_policy_missing"))?
                    {
                        SubscriptionAuthorization::PublicAllowlist {
                            groups,
                        } => {
                            anyhow::ensure!(
                                intent.groups.iter().all(|g| groups.contains(g)),
                                "subscription_denied"
                            );
                            intent
                                .groups
                                .iter()
                                .map(|g| {
                                    (
                                        g.clone(),
                                        GroupGrant {
                                            authorization_id: control::nonce(),
                                            version: identity.version,
                                            group_version: 1,
                                            deadline: identity.deadline.min(
                                                started
                                                    + Duration::from_millis(
                                                        policy.max_grant_ttl_ms,
                                                    ),
                                            ),
                                        },
                                    )
                                })
                                .collect()
                        }
                        SubscriptionAuthorization::ConnectionGrants {} => {
                            let snapshot = connection
                                .grants
                                .lock()
                                .unwrap_or_else(|e| e.into_inner())
                                .clone();
                            anyhow::ensure!(snapshot.valid(), "connection_grants_expired");
                            let mut grants = BTreeMap::new();
                            for group in &intent.groups {
                                let evidence = snapshot
                                    .groups
                                    .iter()
                                    .find(|g| &g.group_id == group)
                                    .ok_or_else(|| anyhow::anyhow!("subscription_denied"))?;
                                grants.insert(
                                    group.clone(),
                                    evidence.grant(
                                        policy.max_grant_ttl_ms,
                                        identity
                                            .deadline
                                            .min(snapshot.deadline.expect("validated grants")),
                                        started,
                                        wall,
                                    )?,
                                );
                            }
                            grants
                        }
                        SubscriptionAuthorization::RemoteHttp {
                            endpoint_ref,
                        } => {
                            let endpoint = connection
                                .plan
                                .endpoints
                                .get(endpoint_ref)
                                .ok_or_else(|| anyhow::anyhow!("authorization_unavailable"))?;
                            let request_id = control::nonce();
                            let subscription_request_id = intent.sequence.to_string();
                            let input = serde_json::json!({
                                "requestId": request_id,
                                "operation": "subscribe",
                                "subscriptionRequestId": subscription_request_id,
                                "context": CallContext {
                                    key: &connection.session.key,
                                    attempt_id: &connection.attempt_id
                                },
                                "principal": {
                                    "uid": identity.uid,
                                    "tenantId": identity.tenant
                                },
                                "groupIds": intent.groups,
                                "authorizationVersion": identity.version.to_string()
                            });
                            use super::authorization_cache::Decision;
                            let calls = self.calls.clone();
                            let fetch_endpoint = endpoint.clone();
                            let fetch_ref = endpoint_ref.clone();
                            let fetch_connection = connection.clone();
                            let fetch_identity = identity.clone();
                            let fetch_groups = intent.groups.clone();
                            let max_grant_ttl_ms = policy.max_grant_ttl_ms;
                            let fetch = async move {
                                let response: SubscriptionResponse = calls
                                    .call(
                                        &fetch_ref,
                                        &fetch_endpoint,
                                        &fetch_connection.facts.input(&input),
                                        fetch_identity.deadline,
                                    )
                                    .await?;
                                anyhow::ensure!(
                                    response.request_id == request_id
                                        && response.subscription_request_id
                                            == subscription_request_id
                                        && response
                                            .code
                                            .as_ref()
                                            .is_none_or(|value| value.len() <= 128)
                                        && response
                                            .message
                                            .as_ref()
                                            .is_none_or(|value| value.len() <= 1024),
                                    "subscription_response_invalid"
                                );
                                if response.decision == "deny" {
                                    anyhow::ensure!(
                                        response.groups.is_empty(),
                                        "subscription_denial_scope_invalid"
                                    );
                                    return Ok(Decision::Denied);
                                }
                                anyhow::ensure!(
                                    response.decision == "allow"
                                        && response.groups.len() == fetch_groups.len(),
                                    "subscription_denied"
                                );
                                let mut grants = BTreeMap::new();
                                for evidence in response.groups {
                                    anyhow::ensure!(
                                        fetch_groups.contains(&evidence.group_id),
                                        "subscription_scope_mismatch"
                                    );
                                    let grant = evidence.grant(
                                        max_grant_ttl_ms,
                                        fetch_identity.deadline,
                                        started,
                                        wall,
                                    )?;
                                    anyhow::ensure!(
                                        grants.insert(evidence.group_id, grant).is_none(),
                                        "duplicate_group_grant"
                                    );
                                }
                                Ok(Decision::Allowed(grants))
                            };
                            match connection
                                .authorization_cache
                                .get_or_fetch(connection, &intent.groups, endpoint, fetch)
                                .await?
                            {
                                Decision::Allowed(grants) => grants,
                                Decision::Denied => anyhow::bail!("subscription_denied"),
                            }
                        }
                    };
                    let owner = connection.clone();
                    let intent = intent.clone();
                    connection
                        .uid_events
                        .submit_control(identity.deadline, 65536, move || {
                            anyhow::ensure!(
                                !owner.cancellation.is_cancelled(),
                                "subscription_closed"
                            );
                            owner.session.complete_subscribe(&intent, grants)
                        })?
                        .wait()
                        .await
                },
            )
            .await
    }

    /// 业务作用：执行配置白名单内的通知动作，所有返回仅属于当前连接。
    /// 参数说明：
    /// - `connection`：已经认证的调用主体。
    /// - `request`：不能携带用户身份或广播控制的固定上行。
    /// - `body`：操作 schema 对应的原始正文。
    ///
    /// 返回：本地状态或业务回调结果；不会直接进入下行分发器。
    async fn notify(
        &self,
        connection: &Arc<Connected>,
        request: &ClientRequest,
        body: Option<Bytes>,
    ) -> anyhow::Result<Reply> {
        let policy = &connection.plan.definition.notifications;
        anyhow::ensure!(policy.enabled, "notifications_disabled");
        let operation = policy
            .operations
            .iter()
            .find(|o| o.name == request.event)
            .ok_or_else(|| anyhow::anyhow!("operation_not_found"))?;
        anyhow::ensure!(operation.enabled, "operation_disabled");
        anyhow::ensure!(
            operation.request_schema == request.schema,
            "schema_mismatch"
        );
        let arguments = decode_arguments(connection, request, body.clone())?;
        let mut permission_deadline = connection.session.identity().deadline;
        match &operation.authorization {
            OperationAuthorization::Authenticated {} => {}
            OperationAuthorization::ConnectionGrant {} => {
                let grants = connection.grants.lock().unwrap_or_else(|e| e.into_inner());
                anyhow::ensure!(
                    grants.valid() && grants.allowed_operations.contains(&operation.name),
                    "permission_denied"
                );
                permission_deadline =
                    permission_deadline.min(grants.deadline.expect("validated grants"));
            }
            OperationAuthorization::RemoteHttp {
                endpoint_ref,
            } => {
                let (_, (_, _, deadline)) = self
                    .authorize_operation(connection, endpoint_ref, request, &arguments)
                    .await?;
                permission_deadline = permission_deadline.min(deadline);
            }
        }
        anyhow::ensure!(
            connection.session.candidate(None).is_some() && Instant::now() < permission_deadline,
            "session_not_ready"
        );
        match operation.action {
            Action::SelfStatus => {
                let output = serde_json::json!({
                    "ready": true,
                    "routeId": connection.session.key.route_id,
                    "routeRevision": connection.session.key.route_revision
                });
                let body = encode_result(
                    connection,
                    operation.response_schema.as_deref(),
                    Some(output),
                )?;
                Ok(reply(
                    connection,
                    request,
                    "ok",
                    Effect::Completed,
                    operation.response_schema.clone(),
                    body,
                ))
            }
            Action::RefreshAuthorization => {
                // 同一会话的刷新串行进入远程调用，迟到响应不能覆盖后一次刷新产生的能力快照。
                let _refresh =
                    tokio::time::timeout_at(permission_deadline, connection.refresh.lock()).await?;
                let policy = &connection.plan.definition.authorization.refresh;
                anyhow::ensure!(policy.enabled, "refresh_disabled");
                let endpoint_ref = policy
                    .endpoint_ref
                    .as_deref()
                    .ok_or_else(|| anyhow::anyhow!("authorization_unavailable"))?;
                let (response, evidence) = self
                    .authorize_operation(connection, endpoint_ref, request, &arguments)
                    .await?;
                self.apply_refresh(connection, response, evidence, permission_deadline)
                    .await?;
                Ok(reply(
                    connection,
                    request,
                    "ok",
                    Effect::Completed,
                    None,
                    None,
                ))
            }
            Action::RemoteCall => {
                self.remote_result(
                    connection,
                    request,
                    arguments,
                    operation.endpoint_ref.as_deref().unwrap_or(""),
                    operation.response_schema.as_deref(),
                    "notify",
                    None,
                    permission_deadline,
                )
                .await
            }
            Action::BusinessSignal => {
                self.business_signal(
                    connection,
                    request,
                    body,
                    operation.publisher_ref.as_deref().unwrap_or(""),
                    permission_deadline,
                )
                .await
            }
        }
    }

    /// 业务作用：把显式开放的 msg 交给业务 REST 校验和执行，禁止直接发送给本地其它连接。
    /// 参数说明：
    /// - `connection`：真实主体和冻结路由。
    /// - `request`：带稳定 operationId 的业务请求。
    /// - `body`：经该路由唯一 schema 编码的正文。
    ///
    /// 返回：业务处理层级；MQ 下行必须另外从受信入口到达。
    async fn client_message(
        &self,
        connection: &Arc<Connected>,
        request: &ClientRequest,
        body: Bytes,
    ) -> anyhow::Result<Reply> {
        let policy = &connection.plan.definition.client_messages;
        anyhow::ensure!(policy.enabled, "client_messages_disabled");
        anyhow::ensure!(
            policy.allowed_events.contains(&request.event)
                && body.len() <= policy.max_request_bytes,
            "message_denied"
        );
        let target = match request.target {
            Target::Group {
                ..
            } => TargetKind::Group,
            Target::Uids {
                ..
            } => TargetKind::Uids,
            _ => anyhow::bail!("target_denied"),
        };
        anyhow::ensure!(policy.allowed_targets.contains(&target), "target_denied");
        let event = connection
            .plan
            .definition
            .events
            .get(&request.event)
            .ok_or_else(|| anyhow::anyhow!("event_not_found"))?;
        anyhow::ensure!(
            request.schema.as_deref() == Some(&event.schema) && body.len() <= event.max_bytes,
            "schema_mismatch"
        );
        match policy.execution {
            Some(MessageExecution::BusinessCommand) => {
                let arguments = if policy.payload_access == PayloadAccess::Json {
                    decode_arguments(connection, request, Some(body.clone()))?
                } else {
                    serde_json::Value::Null
                };
                self.remote_result(
                    connection,
                    request,
                    arguments,
                    policy.endpoint_ref.as_deref().unwrap_or(""),
                    None,
                    "message",
                    (policy.payload_access == PayloadAccess::RawBody).then_some(body),
                    connection.session.identity().deadline,
                )
                .await
            }
            Some(MessageExecution::AuthorizeThenPublish) => {
                self.authorized_publication(connection, request, body).await
            }
            None => anyhow::bail!("message_execution_missing"),
        }
    }

    /// 业务作用：为通知或刷新取得受限授权，不允许接口替换当前主体。
    /// 参数说明：
    /// - `connection`：本次会话的固定上下文。
    /// - `endpoint_ref`：冻结端点引用。
    /// - `request`：固定客户端操作信息。
    /// - `arguments`：已经按 schema 验证的参数。
    ///
    /// 返回：显式允许的关联响应；网络错误和拒绝不产生权限。
    async fn authorize_operation(
        &self,
        connection: &Arc<Connected>,
        endpoint_ref: &str,
        request: &ClientRequest,
        arguments: &serde_json::Value,
    ) -> anyhow::Result<(AuthorizationResponse, (String, u64, Instant))> {
        let subject = if connection
            .plan
            .definition
            .notifications
            .operations
            .iter()
            .any(|operation| {
                operation.name == request.event && operation.action == Action::RefreshAuthorization
            }) {
            "refresh"
        } else {
            "notify"
        };
        self.metrics
            .measure(
                crate::observability::Counter::Authorization,
                subject,
                "authorization",
                "allow",
                async {
                    let endpoint = connection
                        .plan
                        .endpoints
                        .get(endpoint_ref)
                        .ok_or_else(|| anyhow::anyhow!("authorization_unavailable"))?;
                    let identity = connection.session.identity();
                    let request_id = control::nonce();
                    let started = Instant::now();
                    let wall = now()?;
                    let input = serde_json::json!({
                        "requestId": request_id,
                        "operation": "authorize",
                        "operationName": request.event,
                        "operationId": request.operation_id,
                        "context": CallContext {
                            key: &connection.session.key,
                            attempt_id: &connection.attempt_id
                        },
                        "principal": {
                            "uid": identity.uid,
                            "tenantId": identity.tenant
                        },
                        "arguments": arguments
                    });
                    let response: AuthorizationResponse = self
                        .calls
                        .call(
                            endpoint_ref,
                            endpoint,
                            &connection.facts.input(&input),
                            identity.deadline,
                        )
                        .await?;
                    let evidence = response.evidence(&request_id, endpoint, started, wall)?;
                    anyhow::ensure!(
                        response.principal.is_none()
                            && connection.session.candidate(None).is_some(),
                        "authorization_scope_changed"
                    );
                    Ok((response, evidence))
                },
            )
            .await
    }

    /// 业务作用：调用一次可能产生业务副作用的端点，关联不明或调用中断不能报告未执行。
    /// 参数说明：
    /// - `connection`：调用者与冻结定义。
    /// - `request`：稳定业务关联信息。
    /// - `arguments`：已验证参数。
    /// - `endpoint_ref`：部署批准的业务服务。
    /// - `response_schema`：只返回本调用者的可选结果 schema。
    /// - `operation`：受控业务操作类别。
    /// - `raw_body`：原始正文布局使用的不可变字节；None 使用已转换的参数。
    /// - `permission_deadline`：本次操作取得的权限截止点，不能被远程执行等待延长。
    ///
    /// 返回：业务明确结果；发起后遇到传输或结构异常报告 effect=unknown。
    #[expect(
        clippy::too_many_arguments,
        reason = "远程执行同时绑定原正文、动作权限和响应 schema，不能从可变状态推定。"
    )]
    async fn remote_result(
        &self,
        connection: &Arc<Connected>,
        request: &ClientRequest,
        arguments: serde_json::Value,
        endpoint_ref: &str,
        response_schema: Option<&str>,
        operation: &str,
        raw_body: Option<Bytes>,
        permission_deadline: Instant,
    ) -> anyhow::Result<Reply> {
        let endpoint = connection
            .plan
            .endpoints
            .get(endpoint_ref)
            .ok_or_else(|| anyhow::anyhow!("remote_unavailable"))?;
        anyhow::ensure!(request.operation_id.is_some(), "operation_id_required");
        let identity = connection.session.identity();
        anyhow::ensure!(
            Instant::now() < permission_deadline && connection.session.candidate(None).is_some(),
            "operation_permission_expired"
        );
        let request_id = control::nonce();
        let input = serde_json::json!({
            "requestId": request_id,
            "operation": operation,
            "operationName": request.event,
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
            "schema": request.schema,
            "payload": raw_body.as_ref().map(|body| {
                use sha2::Digest; serde_json::json!({
                    "bytes": body.len(),
                    "digest": format!("sha256:{:x}", sha2::Sha256::digest(body))
                })
            }),
            "arguments": arguments
        });
        let result = self
            .calls
            .call_payload_observed::<_, RemoteResult>(
                endpoint_ref,
                endpoint,
                &connection.facts.input(&input),
                raw_body,
                identity.deadline.min(permission_deadline),
            )
            .await;
        let response = match result {
            Ok(value)
                if value.request_id == request_id && value.operation_id == request.operation_id =>
            {
                value
            }
            Err(failure) if !failure.dispatched => {
                return Ok(reply(
                    connection,
                    request,
                    "operation_not_dispatched",
                    Effect::None,
                    None,
                    None,
                ))
            }
            _ => {
                return Ok(reply(
                    connection,
                    request,
                    "operation_unknown",
                    Effect::Unknown,
                    None,
                    None,
                ))
            }
        };
        let effect = match response.status.as_str() {
            "accepted" => Effect::Accepted,
            "completed" => Effect::Completed,
            "denied" => Effect::None,
            _ => Effect::Unknown,
        };
        let code = match effect {
            Effect::Accepted => "accepted",
            Effect::Completed => "ok",
            Effect::None => "permission_denied",
            Effect::Unknown => "operation_unknown",
        };
        // 拒绝或未知没有合法业务返回体，不能因缺少成功 schema 把明确拒绝改写成未知。
        if matches!(effect, Effect::None | Effect::Unknown) {
            return Ok(reply(connection, request, code, effect, None, None));
        }
        let body = match encode_result(connection, response_schema, response.result) {
            Ok(value) => value,
            Err(_) => {
                return Ok(reply(
                    connection,
                    request,
                    "operation_unknown",
                    Effect::Unknown,
                    None,
                    None,
                ))
            }
        };
        let _ = (response.code, response.message);
        Ok(reply(
            connection,
            request,
            code,
            effect,
            body.as_ref().and(response_schema).map(str::to_owned),
            body,
        ))
    }
}

/// 业务作用：将接入拒绝归为有限公开错误码，不向客户端透出远端地址或解析内容。
/// 参数说明：
/// - `error`：准备操作时产生的本地错误。
///
/// 返回：已知身份冲突保留精确语义，其余只公开固定拒绝类别。
pub fn rejection_code(error: &anyhow::Error) -> &'static str {
    match error.to_string().as_str() {
        "request_identity_conflict" => "request_identity_conflict",
        "request_capacity" | "operation_capacity" => "rate_limited",
        "schema_mismatch" => "schema_mismatch",
        "route_changed" => "route_changed",
        _ => "operation_rejected",
    }
}

/// 业务作用：在 HTTP 边界按路由 schema 转换业务体，转发路径不调用此转换。
/// 参数说明：
/// - `connection`：持有唯一 codec 和 schema 目录的连接。
/// - `request`：声明本操作 schema 的上行。
/// - `body`：存在时为完整共享业务字节。
///
/// 返回：有界 JSON 参数；不匹配的正文拒绝。
pub(super) fn decode_arguments(
    connection: &Connected,
    request: &ClientRequest,
    body: Option<Bytes>,
) -> anyhow::Result<serde_json::Value> {
    match (request.schema.as_deref(), body) {
        (None, None) => Ok(serde_json::Value::Null),
        (Some(id), Some(body)) => {
            let plan = connection
                .plan
                .schemas
                .get(id)
                .ok_or_else(|| anyhow::anyhow!("schema_not_found"))?;
            let size = body.len();
            let value = plan.decode(body);
            connection
                .metrics
                .payload_decode("operation", size, value.is_ok());
            let encoded = plan.export_json(&value?)?;
            connection.metrics.payload_copy("decode", encoded.len());
            Ok(serde_json::from_slice(&encoded)?)
        }
        _ => anyhow::bail!("schema_mismatch"),
    }
}

/// 业务作用：将业务 HTTP 返回按预先声明的 schema 编回当前路由唯一模式。
/// 参数说明：
/// - `connection`：冻结 schema 目录。
/// - `schema`：固定响应 schema，不能由远端选择。
/// - `value`：业务返回 JSON。
///
/// 返回：有限业务体；无响应 schema 时不透传任意对象。
fn encode_result(
    connection: &Connected,
    schema: Option<&str>,
    value: Option<serde_json::Value>,
) -> anyhow::Result<Option<Bytes>> {
    let Some(schema) = schema else {
        return Ok(None);
    };
    let value = value.ok_or_else(|| anyhow::anyhow!("result_missing"))?;
    let plan = connection
        .plan
        .schemas
        .get(schema)
        .ok_or_else(|| anyhow::anyhow!("schema_not_found"))?;
    let encoded = serde_json::to_vec(&value)?;
    connection.metrics.payload_copy("encode", encoded.len());
    let value = plan.import_json(&encoded)?;
    let encoded = plan.encode(&value)?;
    connection.metrics.payload_copy("encode", encoded.len());
    Ok(Some(encoded))
}

/// 业务作用：生成只对当前请求者可见的固定结果信封，隐藏集群与其它会话信息。
/// 参数说明：
/// - `connection`：结果所属路由会话。
/// - `request`：本次关联请求。
/// - `code`：服务端确定的稳定结果码。
/// - `effect`：实际已知副作用层级。
/// - `schema`：可选结果正文的固定 schema。
/// - `body`：可选结果正文。
///
/// 返回：目标固定为 self 的响应，不具备转发权限。
pub fn reply(
    connection: &Connected,
    request: &ClientRequest,
    code: &str,
    effect: Effect,
    schema: Option<String>,
    body: Option<Bytes>,
) -> Reply {
    Reply {
        metadata: Downstream {
            kind: Kind::Result,
            id: control::nonce(),
            correlation_id: Some(request.id.clone()),
            operation_id: request.operation_id.clone(),
            context: PublicContext {
                route_id: connection.session.key.route_id.clone(),
                route_revision: connection.session.key.route_revision.clone(),
                definition_digest: connection.plan.digest.clone(),
            },
            event: request.event.clone(),
            schema,
            target: Target::Current {},
            sender: None,
            delivery: None,
            result: Some(ActionResult {
                code: code.into(),
                phase: "operation".into(),
                effect,
                retryable: false,
                retry_after_ms: None,
            }),
        },
        body,
    }
}
