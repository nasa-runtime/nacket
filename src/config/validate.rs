use super::{Deployment, Parameter, RemoteMethod, RemoteTarget};
use crate::domain::route::Identity;
use std::collections::BTreeSet;

impl Deployment {
    /// 业务作用：在配置发布前验证产品边界，禁止不完整认证或含混路由进入生效态。
    /// 参数说明: 无。
    ///
    /// 返回：字段和引用合法时成功；无效候选不会替换当前路由状态。
    pub fn validate(&self) -> anyhow::Result<()> {
        self.transport.validate()?;
        self.metric_scopes.validate()?;
        self.broker_metrics.validate()?;
        self.session_metrics.validate()?;
        self.remote_calls.validate()?;
        self.ingress_processing.validate()?;
        self.lifecycle_delivery.validate()?;
        self.readiness.validate()?;
        self.control.validate(&self.node_id)?;
        self.management.validate()?;
        self.route_provisioning.validate(self)?;
        anyhow::ensure!(
            !self.management.enabled || self.control.clustered(),
            "management_requires_control_authority"
        );
        anyhow::ensure!(self.verifiers.len() <= 64, "verifier capacity");
        for (id, verifier) in &self.verifiers {
            crate::domain::message::envelope::identifier(id)?;
            verifier.validate()?;
        }
        anyhow::ensure!(self.listeners.len() <= 16, "listener template capacity");
        if !self.listeners.is_empty() {
            anyhow::ensure!(
                self.listeners.contains_key(&self.primary_listener),
                "primary listener missing"
            );
        }
        for (id, template) in &self.listeners {
            crate::domain::message::envelope::identifier(id)?;
            template.validate()?;
        }
        anyhow::ensure!(self.ingresses.len() <= 64, "ingress capacity");
        anyhow::ensure!(self.publishers.len() <= 32, "publisher_capacity");
        for (id, publisher) in &self.publishers {
            crate::domain::message::envelope::identifier(id)?;
            publisher.validate(self)?;
        }
        for (id, ingress) in &self.ingresses {
            crate::domain::message::envelope::identifier(id)?;
            ingress.validate()?;
            anyhow::ensure!(
                ingress.delivery != crate::domain::route::policies::DeliveryMode::Recoverable
                    || self.control.clustered(),
                "recoverable_ingress_requires_control_authority"
            );
        }
        for route in &self.routes {
            self.validate_publishers(route)?;
            if route.enabled && route.service_push.enabled {
                let ingress = route
                    .service_push
                    .ingress_ref
                    .as_deref()
                    .and_then(|id| self.ingresses.get(id))
                    .ok_or_else(|| anyhow::anyhow!("route ingress is missing"))?;
                anyhow::ensure!(
                    ingress.enabled && ingress.delivery == route.delivery.mode,
                    "route ingress unavailable or inconsistent"
                );
            }
        }
        crate::domain::message::envelope::identifier(&self.cluster_id)?;
        crate::domain::message::envelope::revision(&self.scope_epoch)?;
        anyhow::ensure!(
            !self.node_id.is_empty() && self.node_id.len() <= 128,
            "invalid nodeId"
        );
        anyhow::ensure!(!self.route_ledger.is_empty(), "routeLedger is required");
        anyhow::ensure!(
            self.routes.len() <= 1024 && self.endpoints.len() <= 256,
            "deployment resource limit exceeded"
        );
        for endpoint in self.endpoints.values() {
            anyhow::ensure!(
                (1..=4096).contains(&endpoint.call.max_in_flight)
                    && endpoint.call.parameters.len() <= 32
                    && endpoint.call.response_fields.len() <= 32,
                "invalid remote call capacity"
            );
            anyhow::ensure!(
                !endpoint.call.allow_value.is_empty()
                    && endpoint.call.allow_value.len() <= 64
                    && !endpoint.call.deny_value.is_empty()
                    && endpoint.call.deny_value.len() <= 64
                    && endpoint.call.allow_value != endpoint.call.deny_value,
                "invalid remote decision mapping"
            );
            if let Some(reference) = &endpoint.call.service_token_secret_ref {
                anyhow::ensure!(
                    !reference.is_empty()
                        && reference.len() <= 128
                        && reference
                            .bytes()
                            .all(|b| b.is_ascii_alphanumeric() || b == b'_')
                        && !reference.as_bytes()[0].is_ascii_digit(),
                    "invalid service token secretRef"
                );
            }
            for (name, parameter) in &endpoint.call.parameters {
                crate::domain::message::envelope::identifier(name)?;
                match parameter {
                    Parameter::Constant {
                        value,
                    } => anyhow::ensure!(
                        !value.is_array()
                            && !value.is_object()
                            && !value.is_null()
                            && serde_json::to_vec(value)?.len() <= 4096,
                        "invalid constant parameter"
                    ),
                    Parameter::Field {
                        pointer,
                        max_bytes,
                        ..
                    } => anyhow::ensure!(
                        pointer.len() <= 512
                            && [
                                "/context/",
                                "/principal/",
                                "/arguments/",
                                "/handshake/",
                                "/headers/"
                            ]
                            .iter()
                            .any(|prefix| pointer.starts_with(prefix))
                            && (1..=4096).contains(max_bytes),
                        "invalid parameter field source"
                    ),
                }
            }
            for (name, pointer) in &endpoint.call.response_fields {
                anyhow::ensure!(
                    [
                        "requestId",
                        "decision",
                        "principal",
                        "authorizationId",
                        "authorizationVersion",
                        "expiresAt",
                        "code",
                        "message",
                        "grants",
                        "groups",
                        "phase",
                        "effect",
                        "operationId",
                        "publicationId",
                        "messageId",
                        "messageDigest",
                        "result",
                        "response",
                        "schema"
                    ]
                    .contains(&name.as_str())
                        && pointer.starts_with('/')
                        && pointer.len() <= 512,
                    "invalid response field mapping"
                );
            }
            anyhow::ensure!(
                (1..=30_000).contains(&endpoint.timeout_ms),
                "invalid endpoint timeoutMs"
            );
            anyhow::ensure!(
                (128..=65_536).contains(&endpoint.max_response_bytes),
                "invalid endpoint maxResponseBytes"
            );
            anyhow::ensure!(
                (1..=3_600_000).contains(&endpoint.max_identity_ttl_ms),
                "invalid endpoint maxIdentityTtlMs"
            );
            match &endpoint.target {
                RemoteTarget::Direct {
                    client_ref,
                    url,
                } => {
                    let parsed = url::Url::parse(url)?;
                    anyhow::ensure!(
                        !client_ref.is_empty()
                            && url.len() <= 2048
                            && matches!(parsed.scheme(), "http" | "https")
                            && parsed.has_host()
                            && parsed.username().is_empty()
                            && parsed.password().is_none()
                            && parsed.fragment().is_none(),
                        "invalid direct endpoint"
                    );
                }
                RemoteTarget::Discovery {
                    connection,
                    service,
                    path,
                } => {
                    anyhow::ensure!(
                        !connection.is_empty()
                            && !service.is_empty()
                            && path.starts_with('/')
                            && !path.starts_with("//"),
                        "invalid discovery endpoint"
                    );
                }
            }
        }
        let mut ids = BTreeSet::new();
        let mut paths = BTreeSet::new();
        for route in &self.routes {
            route.validate()?;
            if let Identity::LocalJwt {
                verifier_ref,
            }
            | Identity::TrustedGateway {
                verifier_ref,
                ..
            } = &route.identity
            {
                anyhow::ensure!(
                    self.verifiers.contains_key(verifier_ref),
                    "identity verifierRef missing"
                );
            }
            anyhow::ensure!(
                if self.listeners.is_empty() {
                    route.access.listener_ref == self.primary_listener
                } else {
                    self.listeners.contains_key(&route.access.listener_ref)
                },
                "route listener missing"
            );
            anyhow::ensure!(ids.insert(&route.route_id), "duplicate routeId");
            if route.compatibility.legacy_path {
                let namespace = route
                    .access
                    .socketio_namespace()
                    .expect("validated legacy namespace");
                // 固定传输路径具有唯一协议含义，兼容别名不得遮蔽原生或标准 Socket.IO 入口。
                anyhow::ensure!(
                    [&self.transport.native_path, &self.transport.socketio_path]
                        .iter()
                        .all(|path| path.strip_suffix('/').unwrap_or(path) != namespace),
                    "legacy path conflicts with a fixed transport path"
                );
            }
            for (kind, path) in [
                ("native", route.access.native_endpoint()),
                ("socketio", route.access.socketio_namespace()),
            ] {
                if let Some(path) = path {
                    let dedicated = route
                        .access
                        .dedicated_port
                        .enabled
                        .then_some(route.route_id.as_str());
                    anyhow::ensure!(
                        paths.insert((&route.access.listener_ref, dedicated, kind, path)),
                        "duplicate route endpoint"
                    );
                }
            }
            if let Identity::Remote {
                endpoint_ref,
            } = &route.identity
            {
                anyhow::ensure!(
                    self.endpoints.contains_key(endpoint_ref),
                    "identity endpointRef does not exist"
                );
                anyhow::ensure!(
                    self.endpoints[endpoint_ref].call.method == RemoteMethod::Post,
                    "identity credentials require POST"
                );
            }
        }
        Ok(())
    }
}
