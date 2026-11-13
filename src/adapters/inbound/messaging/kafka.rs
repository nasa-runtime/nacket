use super::*;
use crate::application::gateway::dispatch::Deferred;
use crate::domain::control::clock::LeaseTime;
use crate::{
    application::gateway::dispatch::ordering_key,
    domain::{
        message::{control::CONTRACT, envelope::Push, json},
        route::policies::DeliveryMode,
    },
};
use nasa::kafka::{GroupSpec, KafkaRecord, NafkaError, RawPayload, SingleConsumer};
use sha2::{Digest, Sha256};
use std::time::{Duration, SystemTime};

mod readiness;

struct Consumer {
    /// 统一业务分发、预算和异步交接运行域。
    gateway: Arc<Gateway>,
    /// 实际订阅 topic 到受信入口的固定映射，未登记来源不能分发。
    ingresses: BTreeMap<String, Arc<TrustedIngress>>,
    /// 当前节点稳定消费组名，节点广播依赖各节点独立消费责任。
    group: String,
}

/// 业务作用：校验实际 topic 分区后登记节点独占消费组，不修改 broker 拓扑。
/// 参数说明：
/// - `application`：提供受管 Kafka 注册和管理只读视图。
/// - `gateway`：共享本地分发器。
/// - `source`：部署批准的命名来源。
/// - `ingresses`：同一来源的固定 topic 权限。
///
/// 返回：注册完成且分区合同一致；不可用或冲突时不开放产品接流。
pub async fn install(
    application: &nasa::Application,
    gateway: Arc<Gateway>,
    source: String,
    ingresses: Vec<Arc<TrustedIngress>>,
) -> anyhow::Result<()> {
    let handle = application.kafka(&source)?;
    let admin = handle.admin()?;
    let view = application.config();
    let tree = view.value();
    let native = tree
        .get("kafkas")
        .and_then(|values| values.get(&source))
        .or_else(|| {
            tree.get("kafka").filter(|value| {
                value
                    .get("client_name")
                    .and_then(|v| v.as_str())
                    .unwrap_or("default")
                    == source
            })
        })
        .ok_or_else(|| anyhow::anyhow!("data_kafka_source_missing"))?;
    let mut native = native.clone();
    if let Some(value) = native.as_object_mut() {
        value.remove("container");
        value.remove("enabled");
    }
    let native: nasa::kafka::KafkaConfig = serde_json::from_value(native)?;
    let dlt_lane = native
        .producer
        .lanes
        .get(&native.behavior.dlt_producer_lane);
    let dlt_acks = dlt_lane
        .and_then(|lane| lane.acks.as_deref())
        .unwrap_or(&native.producer.acks);
    let dlt_idempotent = dlt_lane
        .and_then(|lane| lane.enable_idempotence)
        .unwrap_or(native.producer.enable_idempotence);
    anyhow::ensure!(
        dlt_acks == "all" && dlt_idempotent,
        "data_kafka_dlt_durability"
    );
    let delivery = ingresses
        .first()
        .ok_or_else(|| anyhow::anyhow!("data_kafka_ingress_empty"))?
        .configuration
        .delivery;
    anyhow::ensure!(
        native.behavior.strict_headers
            && native.behavior.dead_letter_required
            && matches!(
                native.behavior.invalid_record_policy,
                nasa::kafka::InvalidRecordPolicy::DeadLetter
            )
            && matches!(
                native.behavior.tombstone_policy,
                nasa::kafka::TombstonePolicy::DeadLetter | nasa::kafka::TombstonePolicy::Deliver
            )
            && native.consumer.auto_offset_reset
                == if delivery == DeliveryMode::Recoverable {
                    "none"
                } else {
                    "latest"
                }
            && native
                .consumer
                .properties
                .get("isolation.level")
                .or_else(|| native.properties.get("isolation.level"))
                .map(String::as_str)
                == Some("read_committed"),
        "data_kafka_consumer_contract"
    );
    // 异步分发与权威提交的等待不是无效消息，不能用普通重试次数把仍在执行的记录提前隔离。
    anyhow::ensure!(
        native.behavior.max_consume_attempts == 0,
        "data_kafka_requires_unbounded_dependency_retry"
    );
    let cluster = admin.cluster_id().await?;
    let mut topics = BTreeMap::new();
    for ingress in ingresses {
        anyhow::ensure!(
            ingress.configuration.delivery == delivery,
            "data_kafka_source_delivery_conflict"
        );
        anyhow::ensure!(
            !ingress.configuration.recovery.initialize,
            "data_kafka_baseline_requires_provisioning"
        );
        let Backend::Kafka {
            topic,
            partition_count,
            ..
        } = &ingress.configuration.transport
        else {
            anyhow::bail!("invalid Kafka ingress")
        };
        let partitions = admin.partitions_for(topic).await?;
        anyhow::ensure!(
            ingress.configuration.dead_letter
                == format!("{topic}{}", native.behavior.dead_letter_topic_suffix),
            "data_kafka_dlt_mismatch"
        );
        anyhow::ensure!(
            native.behavior.max_record_bytes
                >= ingress.configuration.max_body_bytes
                    + ingress.configuration.max_metadata_bytes
                    + 4096,
            "data_kafka_record_capacity"
        );
        anyhow::ensure!(
            partitions.len() == *partition_count as usize
                && partitions.iter().copied().eq(0..*partition_count as i32),
            "Kafka partition contract mismatch"
        );
        anyhow::ensure!(
            topics.insert(topic.clone(), ingress).is_none(),
            "duplicate Kafka topic ingress"
        );
    }
    let mut hash = Sha256::new();
    for field in [
        &gateway.configuration.cluster_id,
        &gateway.configuration.scope_epoch,
        &gateway.configuration.node_id,
        &source,
    ] {
        hash.update((field.len() as u32).to_be_bytes());
        hash.update(field.as_bytes());
    }
    if delivery == DeliveryMode::Realtime {
        hash.update((gateway.startup_id.len() as u32).to_be_bytes());
        hash.update(gateway.startup_id.as_bytes());
    }
    let group = format!("nacket.data.{:x}", hash.finalize());
    let dependencies = topics
        .values()
        .map(|ingress| Ok((ingress.clone(), gateway.routes.dependency(&ingress.id)?)))
        .collect::<anyhow::Result<Vec<_>>>()?;
    let expected = topics
        .iter()
        .flat_map(|(topic, ingress)| {
            let Backend::Kafka {
                partition_count,
                ..
            } = ingress.configuration.transport
            else {
                unreachable!()
            };
            (0..partition_count as i32).map(move |partition| nasa::kafka::Tp::new(topic, partition))
        })
        .collect::<Vec<_>>();
    if delivery == DeliveryMode::Recoverable {
        // 稳定组必须已有明确提交基线；缺失与越界不能用 latest 或 earliest 默默重置。
        verify_baseline(&admin, &group, &expected).await?;
    }
    verify_resources(&admin, &cluster, &dependencies).await?;
    let monitored_group = group.clone();
    for (topic, ingress) in &topics {
        let observed_dependency = gateway.routes.dependency(&ingress.id)?;
        let observed_admin = admin.clone();
        let observed_group = group.clone();
        let partitions = expected
            .iter()
            .filter(|tp| &tp.topic == topic)
            .cloned()
            .collect::<Vec<_>>();
        crate::observability::broker::Broker::install(
            application,
            gateway.broker_metrics.clone(),
            &ingress.id,
            move |timeout| {
                let dependency = observed_dependency.clone();
                let admin = observed_admin.clone();
                let group = observed_group.clone();
                let partitions = partitions.clone();
                async move {
                    let evidence = dependency.acquire().ok_or_else(|| {
                        anyhow::anyhow!("broker_observation_topology_unavailable")
                    })?;
                    let report = admin
                        .observe_offsets(
                            Some(&group),
                            partitions.clone(),
                            nasa::kafka::OffsetIsolation::ReadCommitted,
                            None,
                            timeout,
                        )
                        .await?;
                    anyhow::ensure!(
                        report.partitions.len() == partitions.len(),
                        "broker_observation_incomplete"
                    );
                    let mut lag = 0u64;
                    for partition in &report.partitions {
                        anyhow::ensure!(
                            partitions.contains(&partition.tp),
                            "broker_observation_scope"
                        );
                        let value = partition
                            .lag
                            .value
                            .and_then(|value| u64::try_from(value).ok())
                            .ok_or_else(|| anyhow::anyhow!("broker_observation_incomplete"))?;
                        lag = lag
                            .checked_add(value)
                            .ok_or_else(|| anyhow::anyhow!("broker_observation_capacity"))?;
                    }
                    Ok(crate::observability::broker::Reading {
                        dependency: Some(evidence),
                        unread_positions: lag,
                        pending_records: None,
                        pending_max_idle: None,
                        partitions: Some(partitions.len() as u64),
                    })
                }
            },
        )
        .await?;
    }
    let stale = dependencies
        .iter()
        .map(|(ingress, _)| ingress.configuration.dependency_stale_ms)
        .min()
        .unwrap_or(2000);
    let consumer = Consumer {
        gateway: gateway.clone(),
        ingresses: topics,
        group,
    };
    application.configure_kafka(&source, move |registry| registry.register(consumer))?;
    // 数据来源必须持续保有实际 assignment 和 broker 拓扑证据，启动一次成功不能永久保持接流。
    application.serve_when_ready(format!("socket-kafka-health-{source}"), move |shutdown| async move {
        let mut protected = false;
        loop {
            let evidence =
                LeaseTime::start(Duration::from_millis(stale))?;
            let valid = tokio::select! {
                biased;
                _ = shutdown.cancelled() => {
                    for (_, dependency) in &dependencies {
                        dependency.deny("shutdown");
                    }
                    return Ok::<(), anyhow::Error>(());
                }
                result = verify_resources(&admin, &cluster, &dependencies) => result,
            };
            let snapshot = handle.readiness();
            let group = snapshot
                .groups
                .iter()
                .find(|group| group.group == monitored_group);
            let group_ready = group.is_some_and(|group| {
                readiness::assignment_available(
                    group,
                    &expected,
                    SystemTime::now(),
                    Duration::from_millis(stale),
                )
            });
            if (!group_ready || valid.is_err()) && !protected {
                // 保护原因只记录固定状态，不输出 broker 错误正文、凭据或消息内容。
                tracing::warn!(
                    source = %source,
                    resources_valid = valid.is_ok(),
                    group_ready,
                    group_state = ?group.map(|group| group.state),
                    assignment_ready = group.is_some_and(|group| group.ready_assignment_epoch.is_some()),
                    assigned_partitions = group.map_or(0, |group| group.assignment.len()),
                    required_partitions = expected.len(),
                    pause_reasons = ?group.map(|group| group.paused.iter().map(|(_, reason)| reason).collect::<Vec<_>>()),
                    poll_age_ms = group.and_then(|group| group.last_poll_at).and_then(|at| SystemTime::now().duration_since(at).ok()).map(|age| age.as_millis()),
                    "socket Kafka ingress entered protection"
                );
            }
            protected = !group_ready || valid.is_err();
            for (_, dependency) in &dependencies {
                if valid.is_ok() && group_ready {
                    dependency.confirm(evidence);
                } else {
                    dependency.deny("kafka_dependency_unavailable");
                }
            }
            tokio::select! {
                _ = shutdown.cancelled() => {},
                _ = tokio::time::sleep(Duration::from_millis((stale / 4).clamp(50, 500))) => {},
            }
        }
    }).await?;
    Ok(())
}

/// 业务作用：启动稳定组前校验完整持久提交基线，不把组过期、手工清除或保留裁剪解释为首次消费。
/// 参数说明：
/// - `admin`：受管 Kafka 只读视图。
/// - `group`：部署作用域和逻辑节点共同确定的稳定消费组。
/// - `partitions`：必须完整恢复的 topic 分区集合。
///
/// 返回：每个提交位置仍处于实际保留范围；缺失基线需显式运维准备。
async fn verify_baseline(
    admin: &nasa::kafka::KafkaAdmin,
    group: &str,
    partitions: &[nasa::kafka::Tp],
) -> anyhow::Result<()> {
    let committed = admin.committed_offsets(group, partitions.to_vec()).await?;
    let ranges = admin.watermarks(partitions.to_vec()).await?;
    anyhow::ensure!(
        committed.len() == partitions.len() && ranges.len() == partitions.len(),
        "data_kafka_baseline_incomplete"
    );
    for partition in partitions {
        let offset = committed
            .iter()
            .find(|(tp, _)| tp == partition)
            .and_then(|(_, offset)| *offset)
            .ok_or_else(|| anyhow::anyhow!("data_kafka_baseline_missing: {group}"))?;
        let (_, low, high) = ranges
            .iter()
            .find(|(tp, _, _)| tp == partition)
            .ok_or_else(|| anyhow::anyhow!("data_kafka_range_missing"))?;
        anyhow::ensure!(
            (*low..=*high).contains(&offset),
            "data_kafka_baseline_out_of_range"
        );
    }
    Ok(())
}

/// 业务作用：持续核对物理集群和数据、隔离 topic 合同，不通过自动创建掩盖资源丢失。
/// 参数说明：
/// - `admin`：受管 Kafka 只读管理视图。
/// - `cluster`：启动时确认的物理集群 ID。
/// - `ingresses`：本来源冻结的业务入口及可用性域。
///
/// 返回：全部 topic 仍满足单一布局和容量；未知配置或拓扑变化撤销依赖证据。
async fn verify_resources(
    admin: &nasa::kafka::KafkaAdmin,
    cluster: &str,
    ingresses: &[(
        Arc<TrustedIngress>,
        Arc<crate::domain::route::dependency::Dependency>,
    )],
) -> anyhow::Result<()> {
    anyhow::ensure!(
        admin.cluster_id().await? == cluster,
        "data_kafka_cluster_changed"
    );
    for (ingress, _) in ingresses {
        let Backend::Kafka {
            topic,
            partition_count,
            topic_contract,
            ..
        } = &ingress.configuration.transport
        else {
            anyhow::bail!("data_kafka_backend");
        };
        for (name, partitions) in [
            (topic, Some(*partition_count)),
            (&ingress.configuration.dead_letter, None),
        ] {
            let actual = admin.describe_topic(name, true).await?;
            // 只读复验失败即失去来源证据，不能靠运行期变更 broker 规格继续开放路由。
            let contract = if name == topic {
                topic_contract
            } else {
                &ingress.configuration.dead_letter_contract
            };
            contract.clone().unwrap_or_default().verify(&actual)?;
            anyhow::ensure!(
                partitions.is_none_or(|expected| actual
                    .partitions
                    .iter()
                    .copied()
                    .eq(0..expected as i32))
                    && !actual.partitions.is_empty(),
                "data_kafka_topology"
            );
            let configuration = |key: &str| actual.configs.get(key).and_then(|v| v.as_deref());
            // 撤权恢复依赖完整保留链，不能让时间或容量裁剪静默抹去尚未应用的安全状态。
            if ingress.configuration.delivery == DeliveryMode::Recoverable {
                anyhow::ensure!(
                    configuration("retention.ms") == Some("-1")
                        && configuration("retention.bytes") == Some("-1"),
                    "authorization_kafka_retention_required"
                );
            }
            let maximum = ingress.configuration.max_body_bytes
                + ingress.configuration.max_metadata_bytes
                + 4096;
            anyhow::ensure!(
                configuration("cleanup.policy") == Some("delete")
                    && configuration("unclean.leader.election.enable") == Some("false")
                    && configuration("max.message.bytes")
                        .and_then(|v| v.parse::<usize>().ok())
                        .is_some_and(|value| value >= maximum),
                "data_kafka_topic_contract"
            );
        }
    }
    Ok(())
}

impl SingleConsumer for Consumer {
    type Message = RawPayload;
    /// 业务作用：复用框架的按键调度与连续提交屏障，使无关业务键独立推进。
    /// 参数说明: 无。
    ///
    /// 返回：业务推送与初始化回复按各自业务键并发；授权控制来源保持分区顺序。
    fn execution_mode(&self) -> nasa::kafka::ExecutionMode {
        if self.ingresses.values().all(|ingress| {
            ingress.configuration.purpose
                != crate::config::messaging::IngressPurpose::AuthorizationControl
        }) {
            nasa::kafka::ExecutionMode::KeyOrdered
        } else {
            nasa::kafka::ExecutionMode::PartitionOrdered
        }
    }
    /// 业务作用：冻结本来源数据 topic 集合，客户端不能动态订阅任意 broker 资源。
    /// 参数说明: 无。
    ///
    /// 返回：唯一配置过的业务数据 topic。
    fn topics(&self) -> Vec<String> {
        self.ingresses.keys().cloned().collect()
    }
    /// 业务作用：选择框架无额外业务包装的默认事件，业务 event 位于严格 metadata 内。
    /// 参数说明: 无。
    ///
    /// 返回：固定 DEFAULT 入口。
    fn event(&self) -> String {
        "DEFAULT".into()
    }
    /// 业务作用：给每个节点实例独立组，使服务推送在各节点本地筛选而非节点间竞争消费。
    /// 参数说明: 无。
    ///
    /// 返回：包含完整部署作用域摘要的节点组。
    fn group(&self) -> GroupSpec {
        GroupSpec::Named(self.group.clone())
    }
    /// 业务作用：在框架异步消费责任内完成来源校验和主体分发，不把正常等待转换为重投。
    /// 参数说明：
    /// - `record`：已经预约来源容量的共享原文及当前消费上下文。
    ///
    /// 返回：全部本地决定完成后交给框架推进连续提交水位；无效记录进入既定隔离策略。
    async fn consume(&self, record: KafkaRecord<RawPayload>) -> nasa::kafka::Result<()> {
        let mut scoped = self.ingresses.get(&record.ctx.topic).map(|ingress| {
            self.gateway.metrics.scopes.event(
                crate::observability::scopes::Kind::Ingress,
                &ingress.id,
                "consume",
            )
        });
        let result = self.consume_record(record).await;
        if let Some(scoped) = &mut scoped {
            scoped.finish(
                result
                    .as_ref()
                    .map(|_| "accepted")
                    .unwrap_or_else(|error| crate::observability::outcome(error)),
            );
        }
        self.gateway.metrics.add(
            crate::observability::Counter::Consumer,
            "kafka",
            result
                .as_ref()
                .map(|_| "accepted")
                .unwrap_or_else(|error| crate::observability::outcome(error)),
            1,
        );
        result.map_err(|error| {
            if error.downcast_ref::<Deferred>().is_some() {
                self.gateway.metrics.add(
                    crate::observability::Counter::Recovery,
                    "kafka_retry",
                    "unavailable",
                    1,
                );
                NafkaError::Broker("socket ingress deferred record".into())
            } else {
                NafkaError::HandlerDeadLetter("socket ingress rejected record".into())
            }
        })
    }
}

impl Consumer {
    /// 业务作用：检查每个单值 header 和实际 record key/partition，防止双布局与任意分区破坏隔离。
    /// 参数说明：
    /// - `record`：经过框架 assignment 门禁的原始输入。
    ///
    /// 返回：明确本地接纳决定；任何结构或来源异常不进入客户端发送路径。
    async fn consume_record(&self, record: KafkaRecord<RawPayload>) -> anyhow::Result<()> {
        let (payload, record) = record.into_parts();
        let ingress = self
            .ingresses
            .get(&record.topic)
            .ok_or_else(|| anyhow::anyhow!("topic_not_allowed"))?;
        anyhow::ensure!(record.headers.len() <= 8, "header_capacity");
        let mut headers = BTreeMap::new();
        for header in record.headers.iter() {
            // 此头由 nafka 在派发前覆盖，属于消费尝试证据，不参与来源身份或业务合同。
            if header.name == nasa::kafka::HEADER_DELIVERY_ATTEMPT {
                continue;
            }
            anyhow::ensure!(
                [
                    "nacket-contract",
                    "nacket-meta",
                    "nacket-key-id",
                    "nacket-signature",
                    "X-Nasa-Event",
                    "X-Nasa-Payload-Mode"
                ]
                .contains(&header.name.as_str())
                    && headers
                        .insert(
                            header.name.as_str(),
                            header
                                .value
                                .as_deref()
                                .ok_or_else(|| anyhow::anyhow!("null_header"))?
                        )
                        .is_none(),
                "invalid_header"
            );
        }
        anyhow::ensure!(
            headers.get("nacket-contract").copied() == Some(CONTRACT.as_bytes()),
            "unsupported_contract"
        );
        if let Some(value) = headers.get("X-Nasa-Event") {
            anyhow::ensure!(*value == b"DEFAULT", "unexpected_event_header");
        }
        if let Some(value) = headers.get("X-Nasa-Payload-Mode") {
            anyhow::ensure!(*value == b"raw", "unexpected_payload_mode");
        }
        let metadata = headers
            .get("nacket-meta")
            .ok_or_else(|| anyhow::anyhow!("metadata_missing"))?;
        let body = payload.as_ref();
        anyhow::ensure!(
            body.len() <= ingress.configuration.max_body_bytes,
            "body_capacity"
        );
        let digest: [u8; 32] = Sha256::digest(body).into();
        let key_id = headers
            .get("nacket-key-id")
            .map(|v| std::str::from_utf8(v))
            .transpose()?;
        let signature = headers
            .get("nacket-signature")
            .map(|v| std::str::from_utf8(v))
            .transpose()?;
        let producer = ingress.producer(metadata, &digest, key_id, signature)?;
        if ingress.configuration.purpose
            == crate::config::messaging::IngressPurpose::AuthorizationControl
        {
            anyhow::ensure!(body.is_empty(), "authorization_body_forbidden");
            let command: crate::domain::authorization::Command =
                json::decode(metadata, ingress.configuration.max_metadata_bytes)?;
            command.validate()?;
            let key = command.ordering_key()?;
            anyhow::ensure!(
                record.raw_key.as_deref() == Some(key.as_slice()),
                "ordering_key_mismatch"
            );
            let Backend::Kafka {
                partition_count,
                ..
            } = &ingress.configuration.transport
            else {
                anyhow::bail!("backend_mismatch");
            };
            let hash = Sha256::digest(&key);
            let partition = u32::from_be_bytes(hash[..4].try_into()?) % partition_count;
            anyhow::ensure!(record.partition == partition as i32, "partition_mismatch");
            // 保留当前 handler 与源记录责任，直到原撤权提交有确定结果；正常等待不触发整组重试保护。
            self.gateway
                .authorization_inbox
                .get()
                .ok_or(Deferred("authorization_unavailable"))?
                .accept_wait(ingress, producer, command)
                .await?;
            return Ok(());
        }
        if ingress.configuration.purpose
            == crate::config::messaging::IngressPurpose::InitializationReply
        {
            anyhow::ensure!(body.is_empty(), "initialization_reply_body_forbidden");
            let reply: crate::application::gateway::initialization::InitializationReply =
                json::decode(metadata, ingress.configuration.max_metadata_bytes)?;
            reply.validate()?;
            let key = reply.ordering_key();
            anyhow::ensure!(
                record.raw_key.as_deref() == Some(key.as_slice()),
                "ordering_key_mismatch"
            );
            let Backend::Kafka {
                partition_count,
                ..
            } = &ingress.configuration.transport
            else {
                anyhow::bail!("backend_mismatch")
            };
            let hash = Sha256::digest(&key);
            let partition = u32::from_be_bytes(hash[..4].try_into()?) % partition_count;
            anyhow::ensure!(record.partition == partition as i32, "partition_mismatch");
            self.gateway
                .initializations
                .resolve(ingress, producer, reply)?;
            return Ok(());
        }
        let push: Push = json::decode(metadata, ingress.configuration.max_metadata_bytes)?;
        push.context.validate()?;
        push.target.validate()?;
        let key = ordering_key(&push);
        anyhow::ensure!(
            record.raw_key.as_deref() == Some(key.as_slice()),
            "ordering_key_mismatch"
        );
        let Backend::Kafka {
            partition_count,
            ..
        } = &ingress.configuration.transport
        else {
            anyhow::bail!("backend_mismatch")
        };
        let hash = Sha256::digest(&key);
        let partition = u32::from_be_bytes(hash[..4].try_into()?) % partition_count;
        anyhow::ensure!(record.partition == partition as i32, "partition_mismatch");
        // 原始业务期限在预约交接容量前生效，容量持续不足也不能让过期记录无限保留分区责任。
        let now = crate::adapters::outbound::identity::now()?;
        if push.delivery.expires_at <= now
            || now
                .as_millis()
                .saturating_sub(push.delivery.created_at.as_millis())
                >= self
                    .gateway
                    .configuration
                    .ingress_processing
                    .max_retry_age_ms
        {
            return Ok(());
        }
        let key =
            serde_json::to_string(&(&ingress.id, record.topic, record.partition, record.offset))?;
        let mut fingerprint = Sha256::new();
        fingerprint.update((producer.len() as u32).to_be_bytes());
        fingerprint.update(producer.as_bytes());
        fingerprint.update(metadata);
        fingerprint.update(digest);
        self.gateway
            .push_inbox
            .get()
            .ok_or(Deferred("fanout_unavailable"))?
            .accept(
                &self.gateway,
                key,
                fingerprint.finalize().into(),
                ingress.clone(),
                producer.into(),
                push,
                bytes::Bytes::from_owner(payload),
                digest,
            )
            .await?;
        Ok(())
    }
}
