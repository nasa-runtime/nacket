//! 标准 JSON 回调的封闭结构合同；自定义字段映射由部署单独约定。

use crate::{
    adapters::outbound::identity::Principal,
    application::gateway::{authority::CallContext, handshake::Metadata},
    domain::message::envelope::Target,
};
use serde_json::Value;
use std::collections::BTreeMap;

/// 每个标准回调都携带同一会话的固定事实，不能提升为权限。
#[derive(schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Common<'a> {
    /// 本次 HTTP 调用关联，响应必须回显。
    pub request_id: &'a str,
    /// 精确连接和认证尝试，不能由客户端覆盖。
    pub context: CallContext<'a>,
    /// 路由批准的接入事实和自报标量，不构成身份。
    pub handshake: &'a Metadata,
    /// 路由白名单保留的请求头，不携带代理信任声明。
    pub headers: BTreeMap<String, String>,
    #[serde(default)]
    /// 部署映射批准的有限附加参数。
    pub parameters: BTreeMap<String, Value>,
}

/// 仅身份认证收到原始 token。
#[derive(schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Credentials<'a> {
    /// 尚待业务认证的原始凭据，不可直接作为主体。
    pub token: &'a str,
}

/// 原始正文描述，不包含复制后的业务体。
#[derive(schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Payload {
    /// 原始字节的准确长度。
    pub bytes: usize,
    /// 原始字节的 sha256 摘要。
    pub digest: String,
}

/// 固定操作方向的请求联合体；未知顶层字段拒绝。
#[derive(schemars::JsonSchema)]
#[serde(
    tag = "operation",
    rename_all = "snake_case",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum Request<'a> {
    /// 固定 Authenticate 回调，不提供默认允许。
    Authenticate {
        #[serde(flatten)]
        /// 原会话和调用事实。
        common: Common<'a>,
        /// 业务必须验证的原始认证材料。
        credentials: Credentials<'a>,
    },
    /// 固定 Connect 回调，不提供默认允许。
    Connect {
        #[serde(flatten)]
        /// 原会话和调用事实。
        common: Common<'a>,
        /// 已认证主体，不允许授权结果替换。
        principal: &'a Principal,
    },
    /// 固定 Initialize 回调，不提供默认允许。
    Initialize {
        #[serde(flatten)]
        /// 原会话和调用事实。
        common: Common<'a>,
        /// 已认证主体，不允许初始化替换。
        principal: &'a Principal,
    },
    /// 固定 Subscribe 回调，不提供默认允许。
    Subscribe {
        #[serde(flatten)]
        /// 原会话和调用事实。
        common: Common<'a>,
        /// 本次申请读取资格的真实主体。
        principal: &'a Principal,
        /// 精确订阅意图，取消后旧结果失效。
        subscription_request_id: &'a str,
        /// 本次有限候选订阅目标。
        group_ids: Vec<String>,
        /// 当前主体授权十进制版本。
        authorization_version: &'a str,
    },
    /// 固定 Authorize 回调，不提供默认允许。
    Authorize {
        #[serde(flatten)]
        /// 原会话和调用事实。
        common: Common<'a>,
        /// 已确认主体。
        principal: &'a Principal,
        /// 固定白名单操作名。
        operation_name: &'a str,
        /// 可选业务幂等身份。
        operation_id: Option<&'a str>,
        /// 路由要求物化的参数或 null。
        arguments: Value,
    },
    /// 固定 Refresh 回调，不提供默认允许。
    Refresh {
        #[serde(flatten)]
        /// 原会话和调用事实。
        common: Common<'a>,
        /// 不可由刷新响应替换的原主体。
        principal: &'a Principal,
        /// 固定身份刷新操作名。
        operation_name: &'a str,
        /// 当前身份授权标识。
        authorization_id: &'a str,
        /// 当前身份授权十进制版本。
        authorization_version: &'a str,
    },
    /// 固定 Notify 回调，不提供默认允许。
    Notify {
        #[serde(flatten)]
        /// 原会话和调用事实。
        common: Common<'a>,
        /// 业务必须检查目标权限的真实主体。
        principal: &'a Principal,
        /// 路由允许的操作名。
        operation_name: &'a str,
        /// 跨重试保持的业务幂等身份。
        operation_id: &'a str,
        /// 候选目标，不构成发送资格。
        target: &'a Target,
        /// 可选业务正文 schema。
        schema: Option<&'a str>,
        /// 原始正文描述或空值。
        payload: Option<Payload>,
        /// 显式物化参数或 null。
        arguments: Value,
    },
    /// 固定 Message 回调，不提供默认允许。
    Message {
        #[serde(flatten)]
        /// 原会话和调用事实。
        common: Common<'a>,
        /// 业务必须检查目标权限的真实主体。
        principal: &'a Principal,
        /// 路由允许的操作名。
        operation_name: &'a str,
        /// 跨重试保持的业务幂等身份。
        operation_id: &'a str,
        /// 候选目标，不构成发送资格。
        target: &'a Target,
        /// 可选业务正文 schema。
        schema: Option<&'a str>,
        /// 原始正文描述或空值。
        payload: Option<Payload>,
        /// 显式物化参数或 null。
        arguments: Value,
    },
    /// 固定 AuthorizePublication 回调，不提供默认允许。
    AuthorizePublication {
        #[serde(flatten)]
        /// 原会话和调用事实。
        common: Common<'a>,
        /// 业务必须授权的真实主体。
        principal: &'a Principal,
        /// 原稳定业务幂等身份。
        operation_id: &'a str,
        /// 允许结果只能收窄的候选目标。
        target: &'a Target,
        /// 当前业务事件。
        event: &'a str,
        /// 本次正文绑定的 schema。
        schema: &'a str,
        /// 原文长度与摘要，不是重新编码结果。
        payload: Payload,
        /// 路由声明物化的参数或 null。
        arguments: Value,
    },
}
