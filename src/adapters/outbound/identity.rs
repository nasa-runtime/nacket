use crate::domain::message::UnixMillis;
use serde::Deserialize;
use std::time::{SystemTime, UNIX_EPOCH};

/// 由可信身份服务确认的租户与用户，二者均参与会话隔离。
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Principal {
    /// 受信认证确认的非空、非全空白字符串 uid；客户端自报值不能替代它。
    pub uid: String,
    /// 受信确认的租户作用域，路由、订阅和下行必须保持租户隔离。
    pub tenant_id: String,
}

/// 业务作用：取得可精确跨语言传输的当前毫秒时间。
/// 参数说明: 无。
///
/// 返回：合法时钟值；系统时钟早于 epoch 或超出协议范围时拒绝授权。
pub fn now() -> anyhow::Result<UnixMillis> {
    let millis = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();
    UnixMillis::new(u64::try_from(millis)?).map_err(anyhow::Error::msg)
}
