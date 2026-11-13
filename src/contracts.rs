//! 按通信方向导出的结构合同；语义校验、身份与当前权限仍由运行期执行。

use crate::{
    adapters::{inbound::management, outbound::identity},
    application::gateway::{actions, authority, initialization, lifecycle},
    domain::{authorization, control, message, session::notices},
};
pub mod callback;

use schemars::schema_for;
use serde_json::{json, Value};

/// 业务作用：从服务实际使用的类型导出各通信方向的独立 JSON Schema，供 SDK 与业务服务预检。
/// 参数说明: 无。
///
/// 返回：每个 schemas 条目都是自包含结构文档；合法结构不代替来源、方向、期限及权限校验。
pub fn communication() -> Value {
    json!({
        "contract": message::control::CONTRACT,
        "schemas": {
            "Authenticate": schema_for!(message::control::Authenticate),
            "Hello": schema_for!(message::control::Hello),
            "Confirm": schema_for!(message::control::Confirm),
            "Ready": schema_for!(message::control::Ready),
            "ClientRequest": schema_for!(message::envelope::ClientRequest),
            "Downstream": schema_for!(message::control::Downstream),
            "Receipt": schema_for!(message::control::Receipt),
            "Heartbeat": schema_for!(message::control::Heartbeat),
            "Close": schema_for!(message::control::Close),
            "SessionStateNotice": schema_for!(notices::SessionStateNotice),
            "PublicRoute": schema_for!(message::control::PublicRoute),
            "Push": schema_for!(message::envelope::Push),
            "SessionKey": schema_for!(message::envelope::SessionKey),
            "Principal": schema_for!(identity::Principal),
            "CallContext": schema_for!(authority::CallContext),
            "CallbackRequest": schema_for!(callback::Request),
            "CallbackHandshake": schema_for!(crate::application::gateway::handshake::Metadata),
            "AuthorizationResponse": schema_for!(authority::AuthorizationResponse),
            "SubscriptionResponse": schema_for!(actions::SubscriptionResponse),
            "RemoteResult": schema_for!(actions::RemoteResult),
            "PublicationAuthorization": schema_for!(message::publication::Authorization),
            "InitializationReply": schema_for!(initialization::InitializationReply),
            "LifecycleAcknowledgement": schema_for!(lifecycle::Acknowledgement),
            "AuthorizationCommand": schema_for!(authorization::Command),
            "AuthorizationOperation": schema_for!(authorization::Operation),
            "ManifestRecord": schema_for!(control::ManifestRecord),
            "ManagementProposal": schema_for!(management::Proposal),
            "ManagementChange": schema_for!(management::Change),
            "ManagementOwnershipChange": schema_for!(management::OwnershipChange),
            "ManagementRetirement": schema_for!(management::Retirement),
            "RouteOperation": schema_for!(control::Operation),
            "OwnershipAction": schema_for!(control::OwnershipAction),
            "RetirementAction": schema_for!(control::RetirementAction),
            "PublishedAccess": schema_for!(control::access::PublishedAccess)
        }
    })
}
