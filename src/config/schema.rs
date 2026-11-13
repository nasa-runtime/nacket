//! 产品部署结构的公开 Schema；资源准备、权限交叉引用与持久状态仍由服务端校验。

use schemars::{JsonSchema, Schema};

/// 原生 ProtocolBytes 描述符在产品配置中的结构投影，不承担编码或类型编译。
#[derive(JsonSchema)]
#[schemars(rename_all = "camelCase", deny_unknown_fields)]
pub struct Descriptor {
    /// 当前路由内唯一的协议对象名称，必须与 schemas 中的键一致。
    pub schema_id: String,
    /// 按 tag 定义的字段列表，顺序与唯一性由共享 SchemaPlan 校验。
    pub fields: Vec<Field>,
}

/// 协议字段的配置结构；默认和类型名保持共享动态描述符合同。
#[derive(JsonSchema)]
#[schemars(rename_all = "camelCase", deny_unknown_fields)]
pub struct Field {
    /// 线路字段编号，重复或超出所选模式范围时编译拒绝。
    pub tag: u32,
    /// JSON 模式与类型目录使用的字段名称。
    pub name: String,
    /// 当前字段的唯一类型，嵌套对象仍使用路由确定的统一模式。
    #[schemars(rename = "type")]
    pub typ: Type,
    /// 是否接受空引用；关闭时缺少必需引用字段会被拒绝。
    #[schemars(default)]
    pub nullable: bool,
    /// 当前字段编码区域最大字节数，不代替整帧及节点预算。
    #[schemars(default = "field_bytes")]
    pub max_bytes: usize,
    /// 集合元素个数上限，仍受共享计划的总动态值预算限制。
    #[schemars(default = "field_elements")]
    pub max_elements: usize,
}

/// 共享 ProtocolBytes 动态描述符允许的封闭类型集合。
#[derive(JsonSchema)]
#[schemars(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Type {
    /// 布尔值，按所选 ProtocolBytes 模式编码。
    Bool {},
    /// 有符号八位整数，超出范围不能静默截断。
    I8 {},
    /// 有符号十六位整数。
    I16 {},
    /// 有符号三十二位整数。
    I32 {},
    /// 有符号六十四位整数，JSON 投影遵循共享计划的精确整数合同。
    I64 {},
    /// ProtocolBytes 字符类型，使用共享计划的合法字符范围。
    Char {},
    /// 单精度浮点值，合法表示由共享计划校验。
    F32 {},
    /// 双精度浮点值，合法表示由共享计划校验。
    F64 {},
    /// 受字段字节上限约束的文本。
    String {},
    /// 受字段字节上限约束的原始字节序列。
    Bytes {},
    /// 固定名称及 ordinal 的枚举，排列是线路合同的一部分。
    Enum {
        /// 按 ordinal 排列的合法枚举名，改变排列会改变线路含义。
        variants: Vec<String>,
    },
    /// 引用本路由内已编译对象，嵌套对象沿用同一序列化模式。
    Message {
        /// 当前路由 schemas 中的对象引用，不能读取外部文件或远端地址。
        #[schemars(rename = "schemaRef")]
        schema_ref: String,
    },
    /// 同一元素类型的有界列表，递归仍受总体资源预算限制。
    List {
        /// 所有元素共用的类型，可递归组合受限集合与对象。
        element: Box<Type>,
        /// 是否允许集合元素为空，二进制零长度元素遵循 ProtocolBytes 固定合同。
        #[schemars(default, rename = "nullableElements")]
        nullable_elements: bool,
    },
}

/// 业务作用：保持公开字段默认字节上限与共享 ProtocolBytes 描述符一致。
/// 参数说明: 无。
///
/// 返回：默认单字段最大字节数，实际配置可选择更严格的边界。
fn field_bytes() -> usize {
    65536
}

/// 业务作用：保持公开集合默认个数上限与共享 ProtocolBytes 描述符一致。
/// 参数说明: 无。
///
/// 返回：默认单集合最大元素个数，不表示递归总预算。
fn field_elements() -> usize {
    1024
}

/// 业务作用：从当前部署类型生成字段结构合同，供离线编辑与管理调用预检。
/// 参数说明: 无。
///
/// 返回：nacket 正文的 JSON Schema；不包含框架配置段，也不代替运行期权限和状态校验。
pub fn configuration() -> Schema {
    schemars::schema_for!(super::Deployment)
}

/// 业务作用：在公开路由配置中只提供实时消费，避免编辑器建议业务历史恢复模式。
/// 参数说明：
/// - `_generator`：Schema 派生入口提供的上下文，本字段不引入共享定义。
///
/// 返回：仅允许 realtime 的字符串结构；授权控制入口仍使用独立的恢复策略。
pub fn realtime_delivery(_generator: &mut schemars::SchemaGenerator) -> Schema {
    schemars::json_schema!({"type": "string", "const": "realtime"})
}
