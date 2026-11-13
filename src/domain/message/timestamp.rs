//! nacket 通信合同中的整数毫秒时间及跨语言精度边界。

use serde::{Deserialize, Serialize};

/// Unix 毫秒时间戳，限制在各语言都能精确表示的非负安全整数范围。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "u64", into = "u64")]
pub struct UnixMillis(
    /// 从 Unix epoch 起的毫秒数，构造时已核对 JavaScript 安全整数范围。
    u64,
);

impl UnixMillis {
    /// JSON number 在 JavaScript 中可精确表示的最大整数。
    pub const MAX: u64 = 9_007_199_254_740_991;

    /// 业务作用：验证跨语言传输的绝对时间，避免整数精度丢失改变授权期限。
    /// 参数说明：
    ///
    /// - `value`：从 Unix epoch 起经过的毫秒数。
    ///
    /// 返回：范围有效时保留原整数；超出安全范围时拒绝，不截断或转换单位。
    pub fn new(value: u64) -> Result<Self, &'static str> {
        if value > Self::MAX {
            return Err("timestamp exceeds the safe integer range");
        }
        Ok(Self(value))
    }

    /// 业务作用：读取已验证的毫秒值，供期限比较或固定宽度编码使用。
    /// 参数说明: 无。
    ///
    /// 返回：原始整数，不进行时区或单位转换。
    pub fn as_millis(self) -> u64 {
        self.0
    }

    /// 业务作用：为独立 metadata 字段提供跨语言一致的二进制时间表示。
    /// 参数说明: 无。
    ///
    /// 返回：固定 8 字节大端整数，可直接作为 broker header 或 field 的值。
    pub fn to_be_bytes(self) -> [u8; 8] {
        self.0.to_be_bytes()
    }

    /// 业务作用：从独立 metadata 字段恢复毫秒时间并复验跨语言精度边界。
    /// 参数说明：
    ///
    /// - `bytes`：固定 8 字节大端整数，不接受文本数字。
    ///
    /// 返回：有效时间；超出安全范围时拒绝。
    pub fn from_be_bytes(bytes: [u8; 8]) -> Result<Self, &'static str> {
        Self::new(u64::from_be_bytes(bytes))
    }
}

impl TryFrom<u64> for UnixMillis {
    type Error = &'static str;

    /// 业务作用：为反序列化入口执行统一时间范围校验。
    /// 参数说明：
    ///
    /// - `value`：解析得到的无符号整数毫秒。
    ///
    /// 返回：有效时间；非法范围不进入业务状态。
    fn try_from(value: u64) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<UnixMillis> for u64 {
    /// 业务作用：保持 JSON 时间为整数 token，供各语言无损读取。
    /// 参数说明：
    ///
    /// - `value`：已经验证范围的时间戳。
    ///
    /// 返回：原始毫秒整数，不生成日期字符串。
    fn from(value: UnixMillis) -> Self {
        value.0
    }
}

#[cfg(feature = "schema")]
impl schemars::JsonSchema for UnixMillis {
    /// 业务作用：为所有公开通信时间提供一致的结构名称，防止各方向自行选择字符串时间。
    /// 参数说明: 无。
    ///
    /// 返回：整数毫秒时间的稳定类型名。
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "UnixMillis".into()
    }

    /// 业务作用：向离线调用方公开与运行期一致的跨语言时间精度上限。
    /// 参数说明：
    /// - `generator`：生成当前合同引用的 Schema 宿主。
    ///
    /// 返回：非负且不超过安全整数上限的 JSON integer，不允许日期字符串。
    fn json_schema(generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
        let mut schema = u64::json_schema(generator);
        schema.insert("maximum".into(), Self::MAX.into());
        schema
    }
}
