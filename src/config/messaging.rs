//! MQ 入口绑定真实来源权限，业务元数据中的自报字段不构成来源身份。

use crate::domain::{message::envelope::identifier, route::policies::DeliveryMode};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "backend", rename_all = "snake_case", deny_unknown_fields)]
/// 部署固定的 MQ 目标及拓扑，消息正文不能选择任意来源。
pub enum Backend {
    /// 使用指定受管 Redis 来源中的一个 Stream。
    RedisStream {
        #[serde(rename = "sourceRef")]
        /// 当前 MQ 后端的受管连接引用，目录外连接不能从消息字段动态创建。
        source_ref: String,
        /// 当前入口唯一读取的 Redis Stream 名称，须由部署权限隔离写入来源。
        stream: String,
    },
    /// 使用指定 Kafka 主题并核对固定分区与主题合同。
    Kafka {
        #[serde(rename = "sourceRef")]
        /// 当前 MQ 后端的受管连接引用，目录外连接不能从消息字段动态创建。
        source_ref: String,
        /// 当前入口唯一读取的 Kafka 主题，必须符合已冻结拓扑。
        topic: String,
        #[serde(rename = "partitionCount")]
        /// 预期 Kafka 分区数，拓扑变化时保护入口而不是按新分区静默继续。
        partition_count: u32,
        #[serde(
            default,
            rename = "topicContract",
            skip_serializing_if = "Option::is_none"
        )]
        /// 可选精确主题规格；省略仍要求实际副本与保留配置有效。
        topic_contract: Option<super::kafka_topic::TopicContract>,
    },
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
/// 验签密钥与生产者权限的固定绑定，签名有效不扩大生产者作用域。
pub struct SigningKey {
    /// 该签名密钥允许代表的业务生产者，不由消息自报扩大。
    pub producer: String,
    /// HMAC 验签材料的 secret 引用，原文签名覆盖规定的完整输入。
    pub secret_ref: String,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
/// 入口确定真实生产者的方式，不能信任消息自报身份。
pub enum SourceIdentity {
    /// 由 broker ACL 保证唯一写入者，部署必须隔离入口写权限。
    Acl {
        /// ACL 隔离入口固定代表的生产者，依赖部署限制该来源的写权限。
        producer: String,
    },
    /// 用预登记密钥验证完整消息，并映射到该密钥允许的生产者。
    HmacSha256 {
        /// 允许的 key ID 到生产者及密钥材料映射，不接受消息自带密钥。
        keys: BTreeMap<String, SigningKey>,
    },
}

/// 一个入口使用固定来源和单一交付合同，禁止共享 consumer group 代替节点广播。
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IngressPurpose {
    /// 实时业务下行，仍须校验路由、有效期及接收资格。
    #[default]
    Push,
    /// 仅交给匹配的待决初始化，不可作为普通下行发布。
    InitializationReply,
    /// 持久授权撤销命令，需要独立来源权限及提交证据。
    AuthorizationControl,
}

/// 来源权限按入口冻结，初始化回复不能借用普通下行通道的生产者资格。
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Ingress {
    #[serde(default)]
    /// 是否创建该入口的消费任务和依赖证据；关闭不会自动信任其它入口。
    pub enabled: bool,
    #[serde(default)]
    /// 固定用途：业务下行、初始化回复或授权控制，三者不能混用记录合同。
    pub purpose: IngressPurpose,
    /// 固定 Redis Stream 或 Kafka 来源及拓扑声明。
    pub transport: Backend,
    /// 生产者认证策略，完成来源校验后仍需复验路由和业务目标。
    pub identity: SourceIdentity,
    #[serde(default)]
    /// 业务推送和初始化回复只能使用实时消费；可恢复消费仅用于持久授权控制。
    pub delivery: DeliveryMode,
    #[serde(default = "metadata_limit")]
    /// 每条记录控制元数据最大字节数，解析前核对容量。
    pub max_metadata_bytes: usize,
    #[serde(default = "body_limit")]
    /// 单条业务正文最大字节数，不能替代路由事件自身的更小上限。
    pub max_body_bytes: usize,
    #[serde(default = "batch_limit")]
    /// 单次读取记录数上限，实际读取还受节点读前字节预算收紧。
    pub batch_size: usize,
    #[serde(default = "dependency_timeout")]
    /// 来源证据的最长新鲜度，单位毫秒；无有效进展时关闭相关路由准入。
    pub dependency_stale_ms: u64,
    #[serde(default)]
    /// 恢复基线的显式初始化资格，不能把缺失持久状态当成空消息源。
    pub recovery: Recovery,
    /// 无效原文的隔离目标，持久隔离完成前不报告源记录已安全处理。
    pub dead_letter: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Kafka 隔离主题的独立规格，其保留期限可以与业务主题不同。
    pub dead_letter_contract: Option<super::kafka_topic::TopicContract>,
}

/// 恢复状态只能显式建立；已有消费组或围栏丢失时不能据此重新初始化。
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Recovery {
    #[serde(default)]
    /// 是否允许首次建立消费基线；已有实例身份或恢复证据时不得跳过缺口。
    pub initialize: bool,
}

impl Ingress {
    /// 业务作用：在启动前冻结 MQ 来源、验签和资源边界，关闭入口也不保留非法潜在配置。
    /// 参数说明: 无。
    ///
    /// 返回：配置有界且来源不含歧义；引用是否可用由受管组件装配继续确认。
    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.purpose != IngressPurpose::AuthorizationControl
                || self.delivery == DeliveryMode::Recoverable,
            "authorization_requires_recoverable_ingress"
        );
        // 节点重新接流只服务当前在线连接，停机期间的业务积压不能成为新会话的突发负载。
        anyhow::ensure!(
            self.purpose == IngressPurpose::AuthorizationControl
                || self.delivery == DeliveryMode::Realtime,
            "data_ingress_requires_realtime_delivery"
        );
        let (source, destination) = match &self.transport {
            Backend::RedisStream {
                source_ref,
                stream,
            } => (source_ref, stream),
            Backend::Kafka {
                source_ref,
                topic,
                partition_count,
                topic_contract,
            } => {
                if let Some(contract) = topic_contract {
                    contract.validate()?;
                }
                anyhow::ensure!(
                    (1..=4096).contains(partition_count),
                    "invalid partition count"
                );
                (source_ref, topic)
            }
        };
        identifier(source)?;
        if let Some(contract) = &self.dead_letter_contract {
            anyhow::ensure!(
                matches!(self.transport, Backend::Kafka { .. }),
                "kafka_contract_requires_kafka"
            );
            contract.validate()?;
        }
        for name in [destination, &self.dead_letter] {
            anyhow::ensure!(
                !name.is_empty() && name.len() <= 256 && !name.chars().any(char::is_control),
                "invalid broker resource"
            );
        }
        anyhow::ensure!(
            destination != &self.dead_letter,
            "dead letter destination conflict"
        );
        if self.delivery == DeliveryMode::Recoverable {
            if let Backend::RedisStream {
                stream,
                ..
            } = &self.transport
            {
                let tag = hash_tag(stream)?;
                anyhow::ensure!(
                    tag == hash_tag(&self.dead_letter)?,
                    "recovery_hash_slot_mismatch"
                );
            }
        } else {
            anyhow::ensure!(
                !self.recovery.initialize,
                "recovery_requires_recoverable_delivery"
            );
        }
        anyhow::ensure!(
            (1024..=65536).contains(&self.max_metadata_bytes)
                && (1..=16 * 1024 * 1024).contains(&self.max_body_bytes)
                && (1..=128).contains(&self.batch_size),
            "invalid ingress capacity"
        );
        anyhow::ensure!(
            (250..=30_000).contains(&self.dependency_stale_ms),
            "invalid_dependency_stale_ms"
        );
        match &self.identity {
            SourceIdentity::Acl {
                producer,
            } => identifier(producer)?,
            SourceIdentity::HmacSha256 {
                keys,
            } => {
                anyhow::ensure!(
                    !keys.is_empty() && keys.len() <= 32,
                    "invalid signing key count"
                );
                for (id, key) in keys {
                    identifier(id)?;
                    identifier(&key.producer)?;
                    anyhow::ensure!(
                        !key.secret_ref.is_empty()
                            && key.secret_ref.len() <= 128
                            && key
                                .secret_ref
                                .bytes()
                                .all(|b| b.is_ascii_alphanumeric() || b == b'_')
                            && !key.secret_ref.as_bytes()[0].is_ascii_digit(),
                        "invalid signing secret reference"
                    );
                }
            }
        }
        Ok(())
    }
}

/// 业务作用：约束恢复脚本的所有资源位于同一 Redis Cluster 槽，保证认领与隔离确认可原子执行。
/// 参数说明：
/// - `key`：部署明确配置的 Redis stream 名称。
///
/// 返回：Redis 实际使用的非空 hash tag；缺失或空 tag 拒绝恢复配置。
pub fn hash_tag(key: &str) -> anyhow::Result<&str> {
    let start = key
        .find('{')
        .ok_or_else(|| anyhow::anyhow!("recovery_hash_tag_required"))?;
    let end = key[start + 1..]
        .find('}')
        .ok_or_else(|| anyhow::anyhow!("recovery_hash_tag_required"))?
        + start
        + 1;
    anyhow::ensure!(end > start + 1, "recovery_hash_tag_required");
    Ok(&key[start + 1..end])
}

/// 业务作用：限制固定控制头的默认容量，不将业务正文计入 JSON。
/// 参数说明: 无。
///
/// 返回：16 KiB 控制头预算。
fn metadata_limit() -> usize {
    16384
}
/// 业务作用：限制默认业务体大小，使多路广播和写出具有有限内存成本。
/// 参数说明: 无。
///
/// 返回：256 KiB 业务体预算。
fn body_limit() -> usize {
    256 * 1024
}
/// 业务作用：限制每轮 broker 拉取占用，不一次读取整个保留日志。
/// 参数说明: 无。
///
/// 返回：每轮最多 32 条记录。
fn batch_limit() -> usize {
    32
}

/// 业务作用：限制消费任务失联后仍能使用的 MQ 可用性证据窗口。
/// 参数说明: 无。
///
/// 返回：默认两秒内必须重新取得依赖证据。
fn dependency_timeout() -> u64 {
    2000
}
