//! 有界发布队列持有真实发送责任；调用方取消等待不会把已提交记录当作未发送。

use crate::{
    application::gateway::Connected,
    config::{
        messaging::Backend,
        publishing::{Publisher, Purpose},
    },
    domain::message::control::CONTRACT,
    observability::{Counter, Metrics},
};
use bytes::Bytes;
use nasa::redis::{RedisClient, RedisCommand};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    sync::{Arc, Weak},
    time::Duration,
};
use tokio::{
    sync::{mpsc, oneshot, OwnedSemaphorePermit, Semaphore},
    task::JoinSet,
    time::Instant,
};

/// broker 接受只表示本次发布完成，不表示任何终端已经接收。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Outcome {
    /// broker 已明确接受，不证明消费者或终端处理完成。
    Accepted,
    /// 发布尚未交给 broker，调用者可明确区分未发起与结果不明。
    NotDispatched,
    /// 发起后未取得确定结果，不能推断 broker 未接收或安全重复执行。
    Unknown,
}

/// 在投递前再次检查会话；下线事实可独立于已经撤销的会话发布。
pub enum Guard {
    /// 业务发送必须复验 Ready 和当前会话权限。
    Session(
        /// 原业务请求的精确连接，弱引用不能延长连接生命周期。
        Weak<Connected>,
    ),
    /// 初始化发布只允许仍处于初始化阶段的精确连接。
    Initialization(
        /// 原初始化连接，关闭后不能以迟到发布恢复资格。
        Weak<Connected>,
    ),
    /// 连接事实允许在会话关闭后有限排空，不提供业务推送权限。
    Lifecycle,
}

/// 一次有界发布的不可变内容及发送前条件，入队不提前授予发送权限。
pub struct Publication {
    /// 已经按固定 MQ 合同编码的控制区，重试不重建业务身份。
    pub metadata: Bytes,
    /// 路由唯一序列化模式的原始业务正文，不在发布时转换编码。
    pub body: Bytes,
    /// 受信计算的 broker 顺序键，客户端不能任意改变分区作用域。
    pub ordering_key: Vec<u8>,
    /// 原操作的固定单调发布截止点，等待和重试不能刷新。
    pub deadline: Instant,
    /// 实际发送前复验的会话或初始化条件，连接事实使用独立关闭语义。
    pub guard: Guard,
}

struct Job {
    /// 本次准确发布的不可变内容和最终发送条件。
    publication: Publication,
    /// 向原调用者返回 broker 提交结果的一次性通道，接收者取消不等于发送取消。
    reply: oneshot::Sender<Outcome>,
    /// 从入队到实际发送结束保留的发布数量预约。
    _count: OwnedSemaphorePermit,
    /// 从入队到实际发送结束保留的逻辑字节预约。
    _bytes: OwnedSemaphorePermit,
    /// 实际发布数量责任的观测 owner。
    _observed_count: crate::observability::Reservation,
    /// 实际发布保留字节的观测 owner。
    _observed_bytes: crate::observability::Reservation,
}

struct Channel {
    /// 该发布入口冻结的用途、目标、生产者和容量合同。
    configuration: Publisher,
    /// 交给固定受管发布 worker 的有界入口。
    sender: mpsc::Sender<Job>,
    /// 该入口未结束发布的数量预算，排队和传输都计入。
    count: Arc<Semaphore>,
    /// 该入口保留 metadata 和正文的逻辑字节预算。
    bytes: Arc<Semaphore>,
}

enum Transport {
    Redis(
        /// 宿主管理的固定发布来源。
        Arc<RedisClient>,
        /// 固定发布 Stream，消息不能自行替换。
        String,
    ),
    Kafka(
        /// 固定且已验证持久语义的 producer lane。
        nasa::kafka::ProducerLane,
        /// 固定发布主题。
        String,
        /// 已冻结的分区数量，实际拓扑变化时拒绝发送。
        u32,
    ),
}

/// 固定目录只含配置中开启的权限通道，不包含动态 handler 或任意目的地。
pub struct Publishers {
    /// 部署固定的发布器引用目录，消息不能动态创建出口。
    channels: BTreeMap<String, Channel>,
    /// 节点统一发布指标源，不把目标 ID 或消息 ID 放入标签。
    metrics: Arc<Metrics>,
}

impl Publishers {
    /// 业务作用：冻结发布能力并为每个通道登记受管 worker，复用框架的命名 MQ 连接。
    /// 参数说明：
    /// - `application`：仍处于 UserHook 的 napp 应用。
    /// - `configuration`：已验证、需要重启才能改变的发布目录。
    /// - `metrics`：有限标签的产品指标源。
    /// - `lifecycle_shutdown`：连接下线事实排空后才触发的生命周期出口关闭信号。
    ///
    /// 返回：全部通道已登记；来源缺失或 Kafka 持久性合同不足时拒绝启动。
    pub async fn install(
        application: &nasa::Application,
        configuration: &BTreeMap<String, Publisher>,
        metrics: Arc<Metrics>,
        lifecycle_shutdown: tokio_util::sync::CancellationToken,
    ) -> anyhow::Result<Self> {
        let mut channels = BTreeMap::new();
        for (id, policy) in configuration.iter().filter(|(_, policy)| policy.enabled) {
            let transport = Arc::new(Transport::open(application, policy).await?);
            let (sender, mut receiver) = mpsc::channel::<Job>(policy.max_pending);
            let maximum = policy.max_pending.min(16);
            let purpose = policy.purpose;
            let observed = metrics.clone();
            let lifecycle_shutdown = lifecycle_shutdown.clone();
            application.serve_when_ready(format!("socket-publisher-{id}"), move |shutdown| async move {
                // 下线事实在普通会话停止后仍需提交；只有事实协调器完成排空才关闭该专用通道。
                let shutdown = if purpose == Purpose::Lifecycle {
                    lifecycle_shutdown
                } else {
                    shutdown
                };
                let mut tasks = JoinSet::new();
                loop {
                    tokio::select! {
                        biased;
                        _ = shutdown.cancelled() => {
                            // 停机先关闭准入，未交给 broker 的队列项明确拒绝；已开始任务仍持有完成责任。
                            receiver.close();
                            while let Some(job) = receiver.recv().await {
                                let _ = job.reply.send(Outcome::NotDispatched);
                            }
                            while let Some(result) = tasks.join_next().await {
                                result?;
                            }
                            return Ok::<(), anyhow::Error>(());
                        }
                        result = tasks.join_next(), if !tasks.is_empty() => {
                            if let Some(result) = result {
                                result?;
                            }
                        }
                        job = receiver.recv(), if tasks.len() < maximum => {
                            let job = job.ok_or_else(|| anyhow::anyhow!("publisher_queue_closed"))?;
                            let transport = transport.clone();
                            let observed = observed.clone();
                            tasks.spawn(async move {
                                let outcome = transport
                                    .execute(&job.publication, purpose, &observed)
                                    .await;
                                let _ = job.reply.send(outcome);
                            });
                        }
                    }
                }
            }).await?;
            channels.insert(
                id.clone(),
                Channel {
                    configuration: policy.clone(),
                    sender,
                    count: Arc::new(Semaphore::new(policy.max_pending)),
                    bytes: Arc::new(Semaphore::new(policy.max_bytes)),
                },
            );
        }
        Ok(Self {
            channels,
            metrics,
        })
    }

    /// 业务作用：在固定权限与双重容量内移交一条发布，排队和实际发送共用原始截止时间。
    /// 参数说明：
    /// - `reference`：路由明确引用的发布器。
    /// - `purpose`：调用路径的固定权限类别。
    /// - `route_id`：当前冻结路由，不接受客户端指定另一业务域。
    /// - `publication`：已验证的不可变信封、原始正文与授权截止点。
    ///
    /// 返回：真实 broker 接受、确定未提交或结果未知；从不自动生成新消息重试。
    pub async fn publish(
        &self,
        reference: &str,
        purpose: Purpose,
        route_id: &str,
        mut publication: Publication,
    ) -> Outcome {
        let Some(channel) = self.channels.get(reference) else {
            return Outcome::NotDispatched;
        };
        let policy = &channel.configuration;
        let size = publication
            .metadata
            .len()
            .saturating_add(publication.body.len())
            .saturating_add(4096);
        if policy.purpose != purpose
            || !policy.route_ids.contains(route_id)
            || publication.metadata.len() > policy.max_metadata_bytes
            || publication.body.len() > policy.max_body_bytes
            || size > policy.max_bytes
            || publication.ordering_key.len() > 4096
            || Instant::now() >= publication.deadline
            || (matches!(
                publication.guard,
                Guard::Lifecycle | Guard::Initialization(_)
            ) && purpose != Purpose::Lifecycle)
        {
            return Outcome::NotDispatched;
        }
        let Ok(count) = channel.count.clone().try_acquire_owned() else {
            self.metrics.add(Counter::Queue, "publish", "rejected", 1);
            return Outcome::NotDispatched;
        };
        let Ok(bytes) = channel.bytes.clone().try_acquire_many_owned(size as u32) else {
            self.metrics.add(Counter::Queue, "publish", "rejected", 1);
            return Outcome::NotDispatched;
        };
        publication.deadline = publication
            .deadline
            .min(Instant::now() + Duration::from_millis(policy.timeout_ms));
        let (reply, receiver) = oneshot::channel();
        let job = Job {
            publication,
            reply,
            _count: count,
            _bytes: bytes,
            _observed_count: self.metrics.reserve("publish_items", 1),
            _observed_bytes: self.metrics.reserve("publish_bytes", size as u64),
        };
        if channel.sender.try_send(job).is_err() {
            return Outcome::NotDispatched;
        }
        // worker 持有正文与额度直到真实等待结束，调用者断连只丢弃结果观察权。
        receiver.await.unwrap_or(Outcome::Unknown)
    }
}

impl Transport {
    /// 业务作用：解析已批准的框架来源，Kafka 发布必须使用独立、有界且等待全部副本的通道。
    /// 参数说明：
    /// - `application`：统一连接生命周期的宿主。
    /// - `policy`：固定发布目的地和资源约束。
    ///
    /// 返回：借用框架连接的发送入口；无效来源和实际分区不符均拒绝。
    async fn open(application: &nasa::Application, policy: &Publisher) -> anyhow::Result<Self> {
        match &policy.transport {
            Backend::RedisStream {
                source_ref,
                stream,
            } => Ok(Self::Redis(
                application.redis(source_ref).await?,
                stream.clone(),
            )),
            Backend::Kafka {
                source_ref,
                topic,
                partition_count,
                topic_contract,
            } => {
                let configuration = application.config();
                let tree = configuration.value();
                let source = tree
                    .get("kafkas")
                    .and_then(|sources| sources.get(source_ref))
                    .or_else(|| {
                        tree.get("kafka").filter(|source| {
                            source
                                .get("client_name")
                                .and_then(|value| value.as_str())
                                .unwrap_or("default")
                                == source_ref
                        })
                    })
                    .ok_or_else(|| anyhow::anyhow!("publisher_kafka_source_missing"))?;
                let mut native = source.clone();
                if let Some(object) = native.as_object_mut() {
                    object.remove("container");
                    object.remove("enabled");
                }
                let native: nasa::kafka::KafkaConfig = serde_json::from_value(native)?;
                let lane = native
                    .producer
                    .lanes
                    .get("socket-publish")
                    .ok_or_else(|| anyhow::anyhow!("publisher_lane_required"))?;
                anyhow::ensure!(
                    lane.acks.as_deref().unwrap_or(&native.producer.acks) == "all"
                        && lane
                            .enable_idempotence
                            .unwrap_or(native.producer.enable_idempotence),
                    "publisher_durability_required"
                );
                let handle = application.kafka(source_ref)?;
                let actual = handle.admin()?.describe_topic(topic, true).await?;
                topic_contract.clone().unwrap_or_default().verify(&actual)?;
                let partitions = handle.admin()?.partitions_for(topic).await?;
                anyhow::ensure!(
                    partitions.len() == *partition_count as usize,
                    "publisher_partition_mismatch"
                );
                Ok(Self::Kafka(
                    handle.producer_lane("socket-publish")?,
                    topic.clone(),
                    *partition_count,
                ))
            }
        }
    }

    /// 业务作用：在取得最终会话许可后提交同一份正文，broker 失败只给出保守未知结论。
    /// 参数说明：
    /// - `publication`：队列持有的固定消息与截止点。
    /// - `purpose`：实际发布路径，用于有限指标和 wire 合同。
    /// - `metrics`：共享指标源。
    ///
    /// 返回：明确未开始、已确认接受或未知，许可持续覆盖本地发送等待。
    async fn execute(
        &self,
        publication: &Publication,
        purpose: Purpose,
        metrics: &Arc<Metrics>,
    ) -> Outcome {
        let _permit = match &publication.guard {
            Guard::Session(connection) => {
                let Some(connection) = connection.upgrade() else {
                    return Outcome::NotDispatched;
                };
                if connection.cancellation.is_cancelled() {
                    return Outcome::NotDispatched;
                }
                let Ok(Some(permit)) = crate::application::gateway::outbox::DeliveryCheck::direct(
                    connection.session.clone(),
                    connection.uid_events.clone(),
                )
                .acquire(publication.deadline)
                .await
                else {
                    return Outcome::NotDispatched;
                };
                Some(permit)
            }
            Guard::Lifecycle => None,
            Guard::Initialization(connection) => {
                let Some(connection) = connection.upgrade() else {
                    return Outcome::NotDispatched;
                };
                if connection.cancellation.is_cancelled() {
                    return Outcome::NotDispatched;
                }
                let owner = connection.clone();
                let deadline = publication.deadline;
                let pending = connection
                    .uid_events
                    .submit_control(deadline, 65536, move || {
                        Ok(owner.session.acquire_initialization(deadline))
                    });
                let Ok(pending) = pending else {
                    return Outcome::NotDispatched;
                };
                let Ok(Some(permit)) = pending.wait().await else {
                    return Outcome::NotDispatched;
                };
                Some(permit)
            }
        };
        let deadline = _permit.as_ref().map_or(publication.deadline, |permit| {
            publication.deadline.min(permit.deadline)
        });
        if Instant::now() >= deadline || _permit.as_ref().is_some_and(|permit| !permit.valid()) {
            return Outcome::NotDispatched;
        }
        let subject = if purpose == Purpose::Lifecycle {
            "lifecycle"
        } else {
            "business"
        };
        let mut observed = crate::observability::Operation::new(
            metrics.clone(),
            Counter::Producer,
            subject,
            "broker_commit",
        );
        let contract = if purpose == Purpose::Push {
            CONTRACT
        } else {
            "nacket.business/1"
        };
        let result = tokio::time::timeout_at(deadline, async {
            match self {
                Self::Redis(client, stream) => {
                    let mut command = RedisCommand::new();
                    command
                        .arg("XADD")
                        .arg(stream)
                        .arg("*")
                        .arg("contract")
                        .arg(contract)
                        .arg("meta")
                        .arg(publication.metadata.as_ref())
                        .arg("message")
                        .arg(publication.body.as_ref());
                    let _: String = client.execute_raw(&command).await?;
                }
                Self::Kafka(producer, topic, partitions) => {
                    let hash = Sha256::digest(&publication.ordering_key);
                    let partition = u32::from_be_bytes(hash[..4].try_into()?) % partitions;
                    producer
                        .publish_raw(topic, &publication.body)
                        .key_bytes(publication.ordering_key.clone())
                        .partition(partition as i32)
                        .header("nacket-contract", Some(contract.as_bytes()))
                        .header("nacket-meta", Some(&publication.metadata))
                        .send()
                        .await?;
                }
            }
            Ok::<(), anyhow::Error>(())
        })
        .await;
        if matches!(result, Ok(Ok(()))) {
            observed.finish("committed");
            Outcome::Accepted
        } else {
            observed.finish("unknown");
            Outcome::Unknown
        }
    }
}
