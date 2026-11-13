//! 发布通道是部署批准的固定权限，不接受客户端指定 broker、来源或目的地。

use super::{messaging::Backend, Deployment};
use crate::domain::{message::envelope::identifier, route::policies::*};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
/// 发布器的固定权限域，同一通道不得混用业务下行与服务事实。
pub enum Purpose {
    /// 将已经业务授权的消息提交到下行入口。
    Push,
    /// 向业务系统发送受限信号，本身不授予终端投递资格。
    BusinessSignal,
    /// 发布初始化请求或连接生命周期事实。
    Lifecycle,
}

/// 每个发布器只持有一种权限；普通通知和生命周期不能写入下行入口。
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Publisher {
    #[serde(default)]
    /// 是否创建该受管发布入口，业务侧不能动态打开未部署的发布器。
    pub enabled: bool,
    /// 固定发布用途，业务信号、受权下行与连接事实不能越权互换。
    pub purpose: Purpose,
    /// 固定 MQ 目标及拓扑声明，提交时继续使用冻结目标。
    pub transport: Backend,
    /// 服务代表的固定生产者身份，下行仍受入口和路由允许列表约束。
    pub producer: String,
    /// 该发布器允许服务的有限路由集合，不能向目录外路由发布。
    pub route_ids: BTreeSet<String>,
    /// 排队和实际发送仍在途的记录数量上限。
    pub max_pending: usize,
    /// 发布入口保留的控制信息和正文逻辑字节预算。
    pub max_bytes: usize,
    /// 单次发布的控制区字节上限，编码后必须满足固定合同。
    pub max_metadata_bytes: usize,
    /// 单次发布的原始正文字节上限，不隐式重新编码为其它路由模式。
    pub max_body_bytes: usize,
    /// 一次 broker 提交的最长等待毫秒数，不能延长原业务权限或消息期限。
    pub timeout_ms: u64,
}

impl Publisher {
    /// 业务作用：约束发布资源与权限边界，防止通知借同名 MQ 资源取得下行能力。
    /// 参数说明：
    /// - `deployment`：本节点冻结的全部入口和控制配置。
    ///
    /// 返回：固定身份、目的地与预算均有效；不建立网络连接。
    pub fn validate(&self, deployment: &Deployment) -> anyhow::Result<()> {
        identifier(&self.producer)?;
        anyhow::ensure!(
            !self.route_ids.is_empty() && self.route_ids.len() <= 1024,
            "publisher_route_capacity"
        );
        for route in &self.route_ids {
            identifier(route)?;
        }
        let (source, destination) = self.destination();
        identifier(source)?;
        anyhow::ensure!(
            !destination.is_empty()
                && destination.len() <= 256
                && !destination.chars().any(char::is_control),
            "publisher_destination_invalid"
        );
        if let Backend::Kafka {
            partition_count,
            topic_contract,
            ..
        } = &self.transport
        {
            if let Some(contract) = topic_contract {
                contract.validate()?;
            }
            anyhow::ensure!(
                (1..=4096).contains(partition_count),
                "publisher_partitions_invalid"
            );
        }
        anyhow::ensure!(
            (1..=4096).contains(&self.max_pending)
                && (4096..=64 * 1024 * 1024).contains(&self.max_bytes)
                && (1024..=65536).contains(&self.max_metadata_bytes)
                && (1..=16 * 1024 * 1024).contains(&self.max_body_bytes)
                && self.max_metadata_bytes + self.max_body_bytes + 4096 <= self.max_bytes
                && (1..=30_000).contains(&self.timeout_ms),
            "publisher_capacity_invalid"
        );
        // 下行、隔离和控制资源不能作为普通业务通知的出口；真实 broker ACL 仍须禁止别名绕行。
        for ingress in deployment.ingresses.values() {
            let name = match &ingress.transport {
                Backend::RedisStream {
                    stream,
                    ..
                } => stream,
                Backend::Kafka {
                    topic,
                    ..
                } => topic,
            };
            anyhow::ensure!(
                destination != ingress.dead_letter
                    && (self.purpose == Purpose::Push || destination != name),
                "publisher_privilege_overlap"
            );
        }
        match &deployment.control {
            super::control::Control::Kafka {
                command_topic,
                state_topic,
                ..
            } => {
                anyhow::ensure!(
                    destination != command_topic && destination != state_topic,
                    "publisher_control_overlap"
                );
            }
            _ => anyhow::ensure!(
                !destination.starts_with("socket-control:"),
                "publisher_control_overlap"
            ),
        }
        Ok(())
    }

    /// 业务作用：统一取得已配置的 broker 来源和目的地，不构造客户端可写的资源名。
    /// 参数说明: 无。
    ///
    /// 返回：配置内的固定引用与资源名。
    pub fn destination(&self) -> (&str, &str) {
        match &self.transport {
            Backend::RedisStream {
                source_ref,
                stream,
            } => (source_ref, stream),
            Backend::Kafka {
                source_ref,
                topic,
                ..
            } => (source_ref, topic),
        }
    }
}

impl Deployment {
    /// 业务作用：验证路由引用的发布器权限，动态定义同样只能使用本节点已经批准的通道。
    /// 参数说明：
    /// - `route`：将要应用的冻结路由定义。
    ///
    /// 返回：全部启用路径具有目的明确的发布器；关闭路径不取得发布能力。
    pub fn validate_publishers(
        &self,
        route: &crate::domain::route::RouteDefinition,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(
            route.connections.scope != crate::domain::route::policies::QuotaScope::Cluster
                || self.control.clustered(),
            "cluster_quota_requires_control_authority"
        );
        route.authorization.control.validate()?;
        if route.authorization.control.enabled {
            anyhow::ensure!(
                self.control.clustered(),
                "authorization_requires_control_authority"
            );
            let ingress = route
                .authorization
                .control
                .ingress_ref
                .as_ref()
                .and_then(|id| self.ingresses.get(id))
                .ok_or_else(|| anyhow::anyhow!("authorization_ingress_missing"))?;
            anyhow::ensure!(
                ingress.enabled
                    && ingress.purpose == super::messaging::IngressPurpose::AuthorizationControl,
                "authorization_ingress_purpose_mismatch"
            );
        }
        if route.service_push.enabled {
            let ingress = route
                .service_push
                .ingress_ref
                .as_ref()
                .and_then(|id| self.ingresses.get(id))
                .ok_or_else(|| anyhow::anyhow!("push_ingress_missing"))?;
            anyhow::ensure!(
                ingress.purpose == super::messaging::IngressPurpose::Push,
                "push_ingress_purpose_mismatch"
            );
        }
        let initialization = &route.lifecycle.initialization;
        if initialization.enabled {
            match &initialization.target {
                Some(HookTarget::Mq {
                    ..
                }) => {
                    let reply = initialization
                        .reply
                        .as_ref()
                        .ok_or_else(|| anyhow::anyhow!("initialization_reply_required"))?;
                    identifier(&reply.ingress_ref)?;
                    anyhow::ensure!(
                        !reply.allowed_producers.is_empty()
                            && reply.allowed_producers.len() <= 64
                            && (1..=3_600_000).contains(&reply.max_grant_ttl_ms),
                        "initialization_reply_policy_invalid"
                    );
                    for producer in &reply.allowed_producers {
                        identifier(producer)?;
                    }
                    let ingress = self
                        .ingresses
                        .get(&reply.ingress_ref)
                        .ok_or_else(|| anyhow::anyhow!("initialization_ingress_missing"))?;
                    anyhow::ensure!(
                        ingress.enabled
                            && ingress.purpose
                                == super::messaging::IngressPurpose::InitializationReply,
                        "initialization_ingress_purpose_mismatch"
                    );
                }
                Some(HookTarget::Http {
                    ..
                }) => anyhow::ensure!(
                    initialization.reply.is_none(),
                    "http_initialization_has_no_mq_reply"
                ),
                None => anyhow::bail!("initialization_target_required"),
            }
        }
        let mut required = Vec::new();
        if route.client_messages.enabled
            && route.client_messages.execution == Some(MessageExecution::AuthorizeThenPublish)
        {
            required.push((
                route.client_messages.publisher_ref.as_deref(),
                Purpose::Push,
            ));
        }
        if route.notifications.enabled {
            for operation in &route.notifications.operations {
                if operation.enabled && operation.action == Action::BusinessSignal {
                    required.push((operation.publisher_ref.as_deref(), Purpose::BusinessSignal));
                }
            }
        }
        for (enabled, target, _timeout_ms) in [
            (
                route.lifecycle.initialization.enabled,
                &route.lifecycle.initialization.target,
                route.lifecycle.initialization.timeout_ms,
            ),
            (
                route.lifecycle.online.enabled,
                &route.lifecycle.online.target,
                route.lifecycle.online.timeout_ms,
            ),
            (
                route.lifecycle.offline.enabled,
                &route.lifecycle.offline.target,
                route.lifecycle.offline.timeout_ms,
            ),
        ] {
            if enabled {
                if let Some(HookTarget::Mq {
                    publisher_ref,
                }) = target
                {
                    required.push((Some(publisher_ref.as_str()), Purpose::Lifecycle));
                }
            }
        }
        for (reference, purpose) in required {
            let publisher = reference
                .and_then(|id| self.publishers.get(id))
                .ok_or_else(|| anyhow::anyhow!("publisher_missing"))?;
            anyhow::ensure!(
                publisher.enabled
                    && publisher.purpose == purpose
                    && publisher.route_ids.contains(&route.route_id),
                "publisher_scope_denied"
            );
            if purpose == Purpose::Push {
                let ingress = route
                    .service_push
                    .ingress_ref
                    .as_deref()
                    .and_then(|id| self.ingresses.get(id))
                    .ok_or_else(|| anyhow::anyhow!("publisher_ingress_missing"))?;
                // 固定发布 profile 使用独立 ACL 身份，不能以自报 producer 取得共享签名入口资格。
                anyhow::ensure!(
                    route.service_push.enabled
                        && ingress.enabled
                        && publisher.transport == ingress.transport
                        && matches!(&ingress.identity, super::messaging::SourceIdentity::Acl { producer } if producer == &publisher.producer)
                        && route
                            .service_push
                            .allowed_producers
                            .contains(&publisher.producer),
                    "publisher_ingress_identity_mismatch"
                );
            }
        }
        Ok(())
    }
}
