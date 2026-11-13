/// Redis 原始字段名和值都保留为字节；非法 UTF-8 字段名也必须能够原样隔离。
pub struct Record {
    /// 本次读取是否已经由 Redis 从 PEL 确认认领；重复处理不代表新的业务消息。
    pub reclaimed: bool,
    /// 每个原始字段值转为 Bytes 前的 Vec 容量，用于按保留分配量收费。
    pub capacities: Vec<usize>,
    /// 该原文实际来自的 Stream，不从消息自报字段取得。
    pub stream: String,
    /// Redis 原始记录 ID，用于精确确认或隔离。
    pub id: String,
    /// 保持原顺序及重复字段的不可变原始键值，校验前不折叠为 map。
    pub fields: Vec<(bytes::Bytes, bytes::Bytes)>,
}

impl Record {
    /// 业务作用：接管 Redis 响应的原始字节，保持字段顺序、重复字段和任意二进制内容。
    /// 参数说明：
    /// - `stream`：固定实际来源。
    /// - `id`：broker 返回的记录 ID。
    /// - `values`：交替排列的字段名字节和值字节。
    ///
    /// 返回：拥有原始分配的记录；响应不是键值对时拒绝，不进行有损字符转换。
    pub fn from_parts(stream: &str, id: String, values: Vec<Vec<u8>>) -> anyhow::Result<Self> {
        anyhow::ensure!(values.len().is_multiple_of(2), "stream_record_shape");
        let mut values = values.into_iter();
        let mut fields = Vec::new();
        let mut capacities = Vec::new();
        while let Some(name) = values.next() {
            let value = values.next().expect("paired fields");
            capacities.push(value.capacity());
            fields.push((name.into(), value.into()));
        }
        Ok(Self {
            reclaimed: false,
            stream: stream.into(),
            id,
            fields,
            capacities,
        })
    }
}
