//! Kafka 资源约束只核对 broker 事实，不在服务启动或恢复期间修改主题。

use serde::{Deserialize, Serialize};

/// 可选的精确部署规格；省略预期值时仍拒绝缺失或不合法的 broker 配置。
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase", deny_unknown_fields)]
pub struct TopicContract {
    /// 全部分区必须一致的副本数，空值仅要求实际副本数合法。
    pub replication_factor: Option<u16>,
    /// broker 接受写入所要求的最小同步副本数，不能大于副本数。
    pub min_in_sync_replicas: Option<u16>,
    /// 预期保留毫秒数；正数为有限保留，-1 表示明确不按时间删除。
    pub retention_ms: Option<i64>,
    /// 预期每分区保留字节数；正数为容量上限，-1 表示明确不按容量删除。
    pub retention_bytes: Option<i64>,
}

impl TopicContract {
    /// 业务作用：在访问 broker 前拒绝无法满足的部署资源规格。
    /// 参数说明: 无。
    ///
    /// 返回：字段范围及副本关系合法；零保留和无法解释的负值均拒绝。
    pub fn validate(&self) -> anyhow::Result<()> {
        for value in [self.replication_factor, self.min_in_sync_replicas]
            .into_iter()
            .flatten()
        {
            anyhow::ensure!(
                (1..=32767).contains(&value),
                "kafka_replication_contract_invalid"
            );
        }
        if let (Some(replicas), Some(minimum)) =
            (self.replication_factor, self.min_in_sync_replicas)
        {
            anyhow::ensure!(minimum <= replicas, "kafka_isr_contract_invalid");
        }
        for value in [self.retention_ms, self.retention_bytes]
            .into_iter()
            .flatten()
        {
            anyhow::ensure!(value == -1 || value > 0, "kafka_retention_contract_invalid");
        }
        Ok(())
    }

    /// 业务作用：将实际主题与部署约束逐项比对，缺失配置不能作为通过证据。
    /// 参数说明：
    /// - `actual`：受管 KafkaAdmin 返回的完整只读主题描述。
    ///
    /// 返回：副本、同步副本与保留策略满足约束；不修改主题，不隐藏未知值。
    pub fn verify(&self, actual: &nasa::kafka::TopicDescription) -> anyhow::Result<()> {
        self.validate()?;
        let replicas = actual
            .replication_factor
            .filter(|value| (1..=32767).contains(value))
            .ok_or_else(|| anyhow::anyhow!("kafka_replication_unknown"))?;
        let number = |key: &str| -> anyhow::Result<i64> {
            actual
                .configs
                .get(key)
                .and_then(|value| value.as_deref())
                .and_then(|value| value.parse::<i64>().ok())
                .ok_or_else(|| anyhow::anyhow!("kafka_topic_configuration_unknown: {key}"))
        };
        let minimum = number("min.insync.replicas")?;
        anyhow::ensure!(
            minimum >= 1
                && minimum <= i64::from(replicas)
                && self
                    .replication_factor
                    .is_none_or(|value| i32::from(value) == replicas)
                && self
                    .min_in_sync_replicas
                    .is_none_or(|value| i64::from(value) == minimum),
            "kafka_replication_contract_mismatch"
        );
        for (key, expected) in [
            ("retention.ms", self.retention_ms),
            ("retention.bytes", self.retention_bytes),
        ] {
            let value = number(key)?;
            anyhow::ensure!(
                (value == -1 || value > 0) && expected.is_none_or(|expected| expected == value),
                "kafka_retention_contract_mismatch: {key}"
            );
        }
        Ok(())
    }
}
