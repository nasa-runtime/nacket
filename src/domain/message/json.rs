//! 有界控制 JSON；在建立任何权限相关模型前拒绝重复对象键。

use serde::de::{DeserializeOwned, DeserializeSeed, Error, MapAccess, SeqAccess, Visitor};
use serde_json::Value;
use std::fmt;

const MAX_DEPTH: usize = 12;
const MAX_ITEMS: usize = 2048;

/// 业务作用：解析控制头并拒绝重复键、深层嵌套和未知模型字段，避免不同入口解释不同权限。
/// 参数说明：
/// - `bytes`：完整 UTF-8 JSON 控制头，不含业务体。
/// - `maximum`：当前入口允许的控制头字节上限。
///
/// 返回：严格类型化控制信息；容量、JSON 或模型不合法时不产生可用对象。
pub fn decode<T: DeserializeOwned>(bytes: &[u8], maximum: usize) -> anyhow::Result<T> {
    decode_bounded(bytes, maximum, MAX_DEPTH, MAX_ITEMS)
}

/// 业务作用：为持久控制快照使用独立的解析容量，保持客户端元数据的较小预算不变。
/// 参数说明：
/// - `bytes`：完整且已有字节容量上限的 JSON 文档。
/// - `maximum`：入口允许的最大字节数。
/// - `depth`：允许的最大嵌套深度。
/// - `items`：每个对象或数组允许的最大项数。
///
/// 返回：唯一键、有限深度和有限容器的类型化结果；重复键或超限拒绝全文。
pub(crate) fn decode_bounded<T: DeserializeOwned>(
    bytes: &[u8],
    maximum: usize,
    depth: usize,
    items: usize,
) -> anyhow::Result<T> {
    anyhow::ensure!(!bytes.is_empty() && bytes.len() <= maximum, "metadata_size");
    let mut decoder = serde_json::Deserializer::from_slice(bytes);
    let value = Strict(0, depth, items).deserialize(&mut decoder)?;
    decoder.end()?;
    Ok(serde_json::from_value(value)?)
}

struct Strict(
    /// 当前递归深度，根值从零开始。
    usize,
    /// 本入口允许的最大嵌套深度，超过时不继续下降。
    usize,
    /// 每个对象或数组允许的最大项数，约束小字节输入引发的分配。
    usize,
);

impl<'de> DeserializeSeed<'de> for Strict {
    type Value = Value;

    /// 业务作用：在递归下降前限制控制对象深度，防止解析耗尽调用栈。
    /// 参数说明：
    /// - `decoder`：当前 JSON 值的反序列化入口。
    ///
    /// 返回：有限深度的值；超出深度直接拒绝。
    fn deserialize<D: serde::Deserializer<'de>>(self, decoder: D) -> Result<Value, D::Error> {
        if self.0 > self.1 {
            return Err(D::Error::custom("metadata_depth"));
        }
        decoder.deserialize_any(self)
    }
}

impl<'de> Visitor<'de> for Strict {
    type Value = Value;

    /// 业务作用：为控制头解析错误提供固定描述，不回显凭据或正文。
    /// 参数说明：
    /// - `formatter`：错误描述输出目标。
    ///
    /// 返回：固定类型描述的写入结果。
    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("bounded unique-key JSON")
    }

    /// 业务作用：接受 JSON 布尔值供固定控制模型验证。
    /// 参数说明：
    /// - `value`：原始布尔值。
    ///
    /// 返回：保持类型的 JSON 值。
    fn visit_bool<E: Error>(self, value: bool) -> Result<Value, E> {
        Ok(Value::Bool(value))
    }

    /// 业务作用：保留有符号整数类型，交由目标字段执行范围约束。
    /// 参数说明：
    /// - `value`：解析得到的整数。
    ///
    /// 返回：整数 token，不转换为字符串。
    fn visit_i64<E: Error>(self, value: i64) -> Result<Value, E> {
        Ok(value.into())
    }

    /// 业务作用：保留无符号整数精度，避免时间经浮点中转。
    /// 参数说明：
    /// - `value`：解析得到的整数。
    ///
    /// 返回：整数 token，不改变单位或精度。
    fn visit_u64<E: Error>(self, value: u64) -> Result<Value, E> {
        Ok(value.into())
    }

    /// 业务作用：保留有限浮点数类别，使整数时间字段能拒绝浮点输入。
    /// 参数说明：
    /// - `value`：JSON 浮点值。
    ///
    /// 返回：有限数值；不可表示的数拒绝。
    fn visit_f64<E: Error>(self, value: f64) -> Result<Value, E> {
        serde_json::Number::from_f64(value)
            .map(Value::Number)
            .ok_or_else(|| E::custom("metadata_number"))
    }

    /// 业务作用：保留控制字符串，业务模型另行限制标识符长度和语法。
    /// 参数说明：
    /// - `value`：已确认 UTF-8 的字符串。
    ///
    /// 返回：控制头拥有的字符串。
    fn visit_str<E: Error>(self, value: &str) -> Result<Value, E> {
        Ok(Value::String(value.into()))
    }

    /// 业务作用：保留显式 null，避免将其混同为缺省或空业务体。
    /// 参数说明: 无。
    ///
    /// 返回：JSON null，由具体必填字段拒绝或接纳。
    fn visit_unit<E: Error>(self) -> Result<Value, E> {
        Ok(Value::Null)
    }

    /// 业务作用：有界解析列表，避免小控制头产生过多分配项。
    /// 参数说明：
    /// - `sequence`：当前数组的逐项读取器。
    ///
    /// 返回：保持顺序的有限列表；超限不发布部分结果。
    fn visit_seq<A: SeqAccess<'de>>(self, mut sequence: A) -> Result<Value, A::Error> {
        let mut values = Vec::new();
        while let Some(value) = sequence.next_element_seed(Strict(self.0 + 1, self.1, self.2))? {
            if values.len() == self.2 {
                return Err(A::Error::custom("metadata_items"));
            }
            values.push(value);
        }
        Ok(Value::Array(values))
    }

    /// 业务作用：拒绝同一对象中的重复键，禁止覆盖先前已出现的路由或权限字段。
    /// 参数说明：
    /// - `map`：当前对象的逐项读取器。
    ///
    /// 返回：唯一键对象；任何重复或容量超限均拒绝整个控制头。
    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Value, A::Error> {
        let mut values = serde_json::Map::new();
        while let Some(key) = map.next_key::<String>()? {
            if values.len() == self.2 || values.contains_key(&key) {
                return Err(A::Error::custom("metadata_duplicate_or_capacity"));
            }
            values.insert(
                key,
                map.next_value_seed(Strict(self.0 + 1, self.1, self.2))?,
            );
        }
        Ok(Value::Object(values))
    }
}
