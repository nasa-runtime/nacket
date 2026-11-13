use crate::{
    config::{control::Control, Deployment},
    domain::{
        control::{Ledger, Reply},
        message::{json, UnixMillis},
    },
};
use nasa::{
    application::KafkaHandle,
    kafka::{CommittedRead, KafkaAdmin, ProducerLane, Tp},
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, sync::Arc, time::Duration};

/// 一个事务的完整状态投影，同时记录输入水位和有界回复；只从 read_committed 读取。
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Frame {
    /// 固定 Kafka 控制快照合同标识，未知合同拒绝恢复。
    pub contract: String,
    /// 与本事务命令位置一致的完整持久控制账本。
    pub ledger: Ledger,
    /// 已处理输入位置的十进制表示，必须与事务确认保持一致。
    pub consumed_offset: String,
    /// 请求关联到有限期限回复的目录，与账本同一事务发布。
    pub replies: BTreeMap<String, CachedReply>,
    /// 本次提交生成的 Unix 毫秒整数，不单独证明控制权。
    pub updated_at: UnixMillis,
    #[serde(default)]
    /// 可选提交者观测信息，控制权仍由 broker 分配和事务围栏证明。
    pub writer: Option<Writer>,
}

/// 仅用于观测提交者；控制权仍由 broker 分配、producer fencing 和已提交事务证明。
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Writer {
    /// 生成该快照的稳定节点 ID，仅作提交归属观测。
    pub node_id: String,
    /// 提交者精确启动请求 ID，区分同节点不同进程。
    pub startup_request: String,
}

/// 与控制快照原子提交的有限期回复，过期只结束结果复用，不撤销已提交动作。
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CachedReply {
    /// 与同事务账本变化对应的控制结果。
    pub reply: Reply,
    /// 该关联回复缓存的 Unix 毫秒截止点，不改变动作已提交事实。
    pub expires_at: UnixMillis,
}

#[derive(Default)]
struct Projection {
    /// 最近完整读到的已提交快照，不能使用未提交事务内容。
    frame: Option<Frame>,
    /// 下次恢复读取的物理 offset 边界，未知边界不得复用旧投影。
    next: Option<i64>,
    /// 最近完整读取或末尾核对的单调时刻；非强制查询仅在一百毫秒内复用同一观察。
    read_at: Option<tokio::time::Instant>,
}

/// 连接和 producer 归 napp 管理，状态投影与作用域校验属于 nacket。
pub struct Store {
    /// 宿主管理的 Kafka 来源句柄，共用连接及关闭生命周期。
    pub handle: KafkaHandle,
    /// 实际 broker 身份、分区和保留合同的核对入口。
    admin: KafkaAdmin,
    /// 控制专用 producer lane，使用固定持久和事务语义。
    producer: ProducerLane,
    /// 启动时冻结的受管 Kafka 来源名称。
    pub source_ref: String,
    /// 必须与实际 broker 一致的集群身份，防止连入其它集群。
    pub broker_cluster_id: String,
    /// 独立持久控制输入主题，必须保持规定的单分区合同。
    pub command_topic: String,
    /// 完整快照和回复的事务状态主题，恢复只读已提交记录。
    pub state_topic: String,
    /// 部署命名空间、集群及作用域的稳定摘要，用于隔离控制资源身份。
    pub identity: String,
    /// 是否获准首次创建空基线，已有恢复证据时不能重新初始化。
    pub initialize: bool,
    /// 产品控制账本绑定的集群身份，与 broker 集群身份分别核对。
    cluster_id: String,
    /// 产品控制作用域代际，旧代快照不能作为当前权威。
    scope_epoch: String,
    /// 完整恢复观察的串行缓存，读取和替换保持同一提交边界。
    projection: tokio::sync::Mutex<Projection>,
}

impl Store {
    /// 业务作用：冻结独立 Kafka 控制资源并验证实际身份、单分区和保留合同，不自动建 topic。
    /// 参数说明：
    /// - `application`：统一持有来源生命周期的宿主。
    /// - `deployment`：控制资源、作用域及首次建账开关。
    ///
    /// 返回：已验证的存储入口；无法确认 broker 或持久合同则拒绝启动。
    pub async fn open(
        application: &nasa::Application,
        deployment: &Deployment,
    ) -> anyhow::Result<Arc<Self>> {
        let Control::Kafka {
            source_ref,
            namespace,
            broker_cluster_id,
            command_topic,
            state_topic,
            initialize,
            ..
        } = &deployment.control
        else {
            anyhow::bail!("control_backend_mismatch")
        };
        let configuration = application.config();
        let tree = configuration.value();
        let source = tree
            .get("kafkas")
            .and_then(|sources| sources.get(source_ref))
            .or_else(|| {
                tree.get("kafka").filter(|source| {
                    source
                        .get("client_name")
                        .and_then(|v| v.as_str())
                        .unwrap_or("default")
                        == source_ref
                })
            })
            .ok_or_else(|| anyhow::anyhow!("control_kafka_source_missing"))?;
        let mut native = source.clone();
        if let Some(object) = native.as_object_mut() {
            object.remove("container");
            object.remove("enabled");
        }
        let native: nasa::kafka::KafkaConfig = serde_json::from_value(native)?;
        anyhow::ensure!(
            native.consumer.auto_offset_reset == "earliest",
            "control_kafka_requires_earliest"
        );
        let message_limit = native
            .producer
            .properties
            .get("message.max.bytes")
            .or_else(|| native.properties.get("message.max.bytes"))
            .and_then(|value| value.parse::<u64>().ok());
        anyhow::ensure!(
            message_limit.is_some_and(|limit| limit >= 32 * 1024 * 1024),
            "control_kafka_message_capacity"
        );
        let lane = native
            .producer
            .lanes
            .get("control")
            .ok_or_else(|| anyhow::anyhow!("control_producer_lane_required"))?;
        let acks = lane.acks.as_deref().unwrap_or(&native.producer.acks);
        let idempotent = lane
            .enable_idempotence
            .unwrap_or(native.producer.enable_idempotence);
        anyhow::ensure!(
            acks == "all" && idempotent,
            "control_kafka_durability_required"
        );
        let handle = application.kafka(source_ref)?;
        let admin = handle.admin()?;
        anyhow::ensure!(
            admin.cluster_id().await? == *broker_cluster_id,
            "control_broker_identity_mismatch"
        );
        let identity = format!(
            "{:x}",
            Sha256::digest(serde_json::to_vec(&(
                namespace,
                &deployment.cluster_id,
                &deployment.scope_epoch
            ))?)
        );
        let store = Arc::new(Self {
            producer: handle.producer_lane("control")?,
            handle,
            admin,
            source_ref: source_ref.clone(),
            broker_cluster_id: broker_cluster_id.clone(),
            command_topic: command_topic.clone(),
            state_topic: state_topic.clone(),
            identity,
            initialize: *initialize,
            cluster_id: deployment.cluster_id.clone(),
            scope_epoch: deployment.scope_epoch.clone(),
            projection: tokio::sync::Mutex::new(Projection::default()),
        });
        store.verify_topics().await?;
        Ok(store)
    }

    /// 业务作用：复验控制输入与状态投影的实际拓扑和保留策略，禁止通过裁剪丢失恢复依据。
    /// 参数说明: 无。
    ///
    /// 返回：两个单分区资源及明确保留合同一致；扩分区、删除或配置证据缺失均拒绝继续。
    pub async fn verify_topics(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.admin.cluster_id().await? == self.broker_cluster_id,
            "control_broker_identity_mismatch"
        );
        for topic in [&self.command_topic, &self.state_topic] {
            let observed = self.admin.describe_topic(topic, true).await?;
            anyhow::ensure!(observed.partitions == [0], "control_partition_mismatch");
            let value = |key: &str| observed.configs.get(key).and_then(|value| value.as_deref());
            anyhow::ensure!(
                value("unclean.leader.election.enable") == Some("false"),
                "control_unclean_election_forbidden"
            );
            anyhow::ensure!(
                value("max.message.bytes")
                    .and_then(|v| v.parse::<u64>().ok())
                    .is_some_and(|limit| limit >= 32 * 1024 * 1024),
                "control_topic_message_capacity"
            );
            if topic == &self.state_topic {
                anyhow::ensure!(
                    value("cleanup.policy") == Some("compact")
                        && value("retention.ms") == Some("-1")
                        && value("retention.bytes") == Some("-1"),
                    "control_state_retention_mismatch"
                );
            } else {
                anyhow::ensure!(
                    value("cleanup.policy") == Some("delete")
                        && value("retention.ms") == Some("-1")
                        && value("retention.bytes") == Some("-1"),
                    "control_command_retention_mismatch"
                );
            }
        }
        Ok(())
    }

    /// 业务作用：把完整签名请求发布到唯一控制输入分区，等待真实 broker delivery report。
    /// 参数说明：
    /// - `request`：固定控制合同的原始签名字节。
    ///
    /// 返回：已确认输入位置；超时不能证明请求未被接受。
    pub async fn submit(&self, request: &[u8]) -> anyhow::Result<String> {
        anyhow::ensure!(request.len() <= 262144, "control_request_capacity");
        let result = self
            .producer
            .publish_raw(&self.command_topic, request)
            .key(&self.identity)
            .partition(0)
            .send()
            .await?;
        Ok(result.offset.to_string())
    }

    /// 业务作用：在固定已提交末尾恢复完整状态，短暂空读取不能形成有效基线。
    /// 参数说明：
    /// - `force`：事务处理器在取得 producer fencing 后必须强制读取新边界。
    ///
    /// 返回：完整已提交投影或尚未建账；不把事务处理器内的候选放入缓存。
    pub async fn restore(&self, force: bool) -> anyhow::Result<Option<Frame>> {
        let mut cache = self.projection.lock().await;
        if !force
            && cache
                .read_at
                .is_some_and(|at| at.elapsed() < Duration::from_millis(100))
        {
            return Ok(cache.frame.clone());
        }
        if cache.frame.is_some() {
            if let Some(next) = cache.next {
                let partition = Tp::new(&self.state_topic, 0);
                let bounds = self.admin.watermarks(vec![partition.clone()]).await?;
                anyhow::ensure!(
                    bounds.len() == 1
                        && bounds[0].0 == partition
                        && bounds[0].1 <= next
                        && next <= bounds[0].2,
                    "control_projection_boundary_changed"
                );
                if bounds[0].2 == next {
                    // 强制恢复也必须查询当前物理末尾；相同末尾证明已读提交边界后没有新记录，毋须重建读取客户端。
                    // 取得 fencing 后只跳过时间缓存，不跳过 broker 复验；存在新记录或事务标记时仍读取 read_committed。
                    anyhow::ensure!(
                        self.admin.cluster_id().await? == self.broker_cluster_id,
                        "control_broker_identity_mismatch"
                    );
                    anyhow::ensure!(
                        self.admin.partitions_for(&self.state_topic).await? == [0],
                        "control_partition_mismatch"
                    );
                    cache.read_at = Some(tokio::time::Instant::now());
                    return Ok(cache.frame.clone());
                }
            }
        }
        let mut end = None;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            let timeout = deadline.saturating_duration_since(tokio::time::Instant::now());
            anyhow::ensure!(!timeout.is_zero(), "control_restore_timeout");
            let page = self
                .admin
                .read_committed_page(CommittedRead {
                    cluster_id: self.broker_cluster_id.clone(),
                    partition: Tp::new(&self.state_topic, 0),
                    partition_count: 1,
                    start_offset: cache.next,
                    end_offset: end,
                    max_records: 32,
                    max_bytes: 64 * 1024 * 1024,
                    timeout,
                })
                .await?;
            end = Some(page.end_offset);
            for record in page.records {
                anyhow::ensure!(
                    record.key.as_deref() == Some(self.identity.as_bytes()),
                    "control_projection_key_mismatch"
                );
                let body = record
                    .payload
                    .ok_or_else(|| anyhow::anyhow!("control_projection_tombstone"))?;
                let frame: Frame = json::decode_bounded(&body, 32 * 1024 * 1024, 64, 131072)?;
                self.validate_frame(&frame)?;
                if let Some(previous) = &cache.frame {
                    anyhow::ensure!(
                        frame.ledger.sequence > previous.ledger.sequence
                            && parse_offset(&frame.consumed_offset)?
                                >= parse_offset(&previous.consumed_offset)?,
                        "control_projection_regression"
                    );
                }
                cache.frame = Some(frame);
            }
            cache.next = Some(page.next_offset);
            if page.next_offset == page.end_offset {
                cache.read_at = Some(tokio::time::Instant::now());
                return Ok(cache.frame.clone());
            }
        }
    }

    /// 业务作用：以已提交投影构造与本地持久高水位一致的控制快照。
    /// 参数说明: 无。
    ///
    /// 返回：完整基线；未建账时明确拒绝，不能用 YAML 推断权威状态。
    pub async fn read(&self) -> anyhow::Result<super::redis::Snapshot> {
        let frame = self
            .restore(false)
            .await?
            .ok_or_else(|| anyhow::anyhow!("control_baseline_missing"))?;
        let hash = format!("{:x}", Sha256::digest(serde_json::to_vec(&frame.ledger)?));
        Ok(super::redis::Snapshot {
            ledger: frame.ledger,
            cursor: frame.consumed_offset.clone(),
            position: frame.consumed_offset,
            hash,
        })
    }

    /// 业务作用：只从已提交投影查回控制请求结果，不从事务回调提前回复成功。
    /// 参数说明：
    /// - `request_id`：签名请求的完整摘要。
    ///
    /// 返回：持久回复或尚无确认，副作用是否成功仍由控制记录决定。
    pub async fn reply(&self, request_id: &str) -> anyhow::Result<Option<Reply>> {
        Ok(self
            .reply_snapshot(request_id)
            .await?
            .map(|(reply, _)| reply))
    }

    /// 业务作用：从同一已提交投影取得回复及其完整高水位，避免确认结果后再次恢复相同状态。
    /// 参数说明：
    /// - `request_id`：签名请求的唯一摘要。
    ///
    /// 返回：已提交回复及对应快照；没有关联记录时返回 None，不使用内存候选。
    pub async fn reply_snapshot(
        &self,
        request_id: &str,
    ) -> anyhow::Result<Option<(Reply, super::redis::Snapshot)>> {
        let Some(frame) = self.restore(false).await? else {
            return Ok(None);
        };
        let Some(reply) = frame
            .replies
            .get(request_id)
            .map(|entry| entry.reply.clone())
        else {
            return Ok(None);
        };
        let hash = format!("{:x}", Sha256::digest(serde_json::to_vec(&frame.ledger)?));
        Ok(Some((
            reply,
            super::redis::Snapshot {
                ledger: frame.ledger,
                cursor: frame.consumed_offset.clone(),
                position: frame.consumed_offset,
                hash,
            },
        )))
    }

    /// 业务作用：核对恢复投影内的身份、账本引用、输入位点和有限回复容量。
    /// 参数说明：
    /// - `frame`：从已提交记录解析出的完整投影。
    ///
    /// 返回：全部恢复不变量成立时通过；损坏内容不能替代旧基线。
    pub fn validate_frame(&self, frame: &Frame) -> anyhow::Result<()> {
        anyhow::ensure!(
            frame.contract == "nacket-control-state/1"
                && frame.ledger.cluster_id == self.cluster_id
                && frame.ledger.scope_epoch == self.scope_epoch
                && frame.replies.len() <= 4096,
            "control_projection_mismatch"
        );
        parse_offset(&frame.consumed_offset)?;
        frame.ledger.validate()?;
        if let Some(writer) = &frame.writer {
            crate::domain::message::envelope::identifier(&writer.node_id)?;
            crate::domain::message::envelope::identifier(&writer.startup_request)?;
        }
        for key in frame.replies.keys() {
            crate::domain::message::envelope::identifier(key)?;
        }
        Ok(())
    }
}

/// 业务作用：把 Kafka 持久位置的十进制字符串还原为有界整数，不接受隐式格式归一化。
/// 参数说明：
/// - `value`：跨语言保存的下一输入位点。
///
/// 返回：非负规范位点；前导零、负数和溢出均拒绝。
pub fn parse_offset(value: &str) -> anyhow::Result<i64> {
    anyhow::ensure!(
        !value.is_empty()
            && value.len() <= 19
            && value.bytes().all(|byte| byte.is_ascii_digit())
            && (value.len() == 1 || !value.starts_with('0')),
        "control_offset_invalid"
    );
    Ok(value.parse()?)
}
