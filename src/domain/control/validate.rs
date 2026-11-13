use super::*;
use crate::domain::message::envelope::{identifier, revision};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};

impl Ledger {
    /// 业务作用：恢复控制快照前检查记录间的完整关系，缺失操作、伪造许可和断裂清单不能成为新的授予依据。
    /// 参数说明: 无。
    ///
    /// 返回：结构、容量、作用域关联和持久历史一致；任何不完整状态均拒绝进入控制运行域。
    pub fn validate(&self) -> anyhow::Result<()> {
        identifier(&self.cluster_id)?;
        revision(&self.scope_epoch)?;
        self.assertions.validate()?;
        self.validate_quotas()?;
        self.validate_access()?;
        anyhow::ensure!(
            self.authorizations.len() <= 1024
                && self
                    .authorizations
                    .values()
                    .map(|state| state.operations.len())
                    .sum::<usize>()
                    <= crate::domain::authorization::MAXIMUM_RECORDS,
            "authorization_baseline_capacity"
        );
        for (route_id, state) in &self.authorizations {
            state.validate()?;
            anyhow::ensure!(
                self.routes.contains_key(route_id),
                "authorization_route_missing"
            );
            for action in state.records.values() {
                if let crate::domain::authorization::Action::Kick {
                    session_key,
                    ..
                } = action
                {
                    // 持久精确会话只能属于账本原作用域，不能借完整但跨域的键恢复撤权事实。
                    anyhow::ensure!(
                        session_key.cluster_id == self.cluster_id
                            && session_key.scope_epoch == self.scope_epoch
                            && session_key.route_id == *route_id,
                        "authorization_session_scope_mismatch"
                    );
                }
            }
            for operation in state.operations.values() {
                anyhow::ensure!(
                    operation.barrier.iter().all(|id| self
                        .permits
                        .get(id)
                        .is_some_and(|permit| permit.binding.route_id == *route_id)),
                    "authorization_barrier_invalid"
                );
            }
        }
        anyhow::ensure!(
            self.routes.len() <= 1024
                && self.nodes.len() <= 256
                && self.permits.len() <= 65536
                && self.operations.len() + self.ownership_actions.len() + self.retirements.len()
                    <= 65536
                && self.manifests.len() <= 64
                && self.manifests.values().map(Vec::len).sum::<usize>() <= 65536,
            "control_baseline_capacity"
        );
        anyhow::ensure!(
            self.policy_digest.is_empty()
                || (self.policy_digest.len() == 64
                    && self.policy_digest.bytes().all(|b| b.is_ascii_hexdigit())),
            "control_policy_digest_invalid"
        );
        if self.policy_digest.is_empty() {
            anyhow::ensure!(
                self.sequence == 0
                    && self.routes.is_empty()
                    && self.nodes.is_empty()
                    && self.permits.is_empty()
                    && self.operations.is_empty()
                    && self.ownership_actions.is_empty()
                    && self.retirements.is_empty()
                    && self.authorizations.is_empty()
                    && self.reservations.is_empty()
                    && self.manifests.is_empty(),
                "control_policy_baseline_missing"
            );
        }
        let mut known_nodes = BTreeSet::new();
        for (id, node) in &self.nodes {
            anyhow::ensure!(
                node.authorizations.len() <= 1024
                    && node
                        .authorizations
                        .iter()
                        .all(|(route, revision)| *revision > 0
                            && self
                                .authorizations
                                .get(route)
                                .is_some_and(|state| *revision <= state.revision)),
                "authorization_node_projection_invalid"
            );
            identifier(id)?;
            identifier(&node.startup_request)?;
            anyhow::ensure!(
                node.incarnation > 0
                    && node.startups.len() <= 1024
                    && node.startups.get(&node.startup_request) == Some(&node.incarnation)
                    && node.startups.len() as u64 == node.incarnation
                    && node.prepared.len() <= 1024,
                "control_node_history_invalid"
            );
            let mut counters = BTreeSet::new();
            for (startup, incarnation) in &node.startups {
                identifier(startup)?;
                anyhow::ensure!(
                    *incarnation > 0
                        && *incarnation <= node.incarnation
                        && counters.insert(*incarnation),
                    "control_incarnation_history_invalid"
                );
                known_nodes.insert(super::transition::node_key(id, *incarnation));
            }
            for (route_id, prepared) in &node.prepared {
                prepared.validate()?;
                anyhow::ensure!(
                    *route_id == prepared.route_id
                        && self.routes.get(route_id).is_some_and(
                            |route| prepared.revision <= route.definition.route.revision
                        ),
                    "control_preparation_invalid"
                );
            }
        }
        for (id, route) in &self.routes {
            Binding::of(&route.definition).validate()?;
            identifier(&route.owner)?;
            anyhow::ensure!(
                *id == route.definition.route.route_id
                    && route.owner_revision > 0
                    && (!route.suspended || !route.definition.route.enabled),
                "control_route_invalid"
            );
            let operation = self
                .operations
                .get(&route.operation_id)
                .ok_or_else(|| anyhow::anyhow!("control_route_operation_missing"))?;
            anyhow::ensure!(
                operation.binding == Binding::of(&route.definition)
                    && operation.phase != Phase::Superseded,
                "control_route_operation_mismatch"
            );
            for previous in &route.previous {
                Binding::of(previous).validate()?;
                anyhow::ensure!(
                    previous.route.route_id == *id
                        && previous.route.revision < route.definition.route.revision,
                    "control_previous_definition_invalid"
                );
            }
        }
        for (id, operation) in &self.operations {
            identifier(id)?;
            identifier(&operation.actor)?;
            operation.binding.validate()?;
            anyhow::ensure!(
                *id == operation.id
                    && !self.ownership_actions.contains_key(id)
                    && operation.fingerprint.len() == 64
                    && operation.minimum_ready > 0
                    && operation.minimum_ready <= 256
                    && self
                        .routes
                        .get(&operation.binding.route_id)
                        .is_some_and(
                            |route| operation.binding.revision <= route.definition.route.revision
                        ),
                "control_operation_invalid"
            );
            anyhow::ensure!(
                operation.applied.is_subset(&operation.prepared)
                    && operation
                        .targets
                        .union(&operation.prepared)
                        .all(|key| known_nodes.contains(key))
                    && operation
                        .failures
                        .keys()
                        .all(|key| known_nodes.contains(key)),
                "control_operation_node_invalid"
            );
            anyhow::ensure!(
                operation
                    .barrier
                    .iter()
                    .all(|id| self.permits.get(id).is_some_and(
                        |permit| permit.binding.route_id == operation.binding.route_id
                    )),
                "control_barrier_invalid"
            );
            match operation.phase {
                Phase::Quiescing => {
                    anyhow::ensure!(!operation.barrier.is_empty(), "control_barrier_missing")
                }
                Phase::Preparing | Phase::Activating | Phase::Effective => {
                    anyhow::ensure!(operation.barrier.is_empty(), "control_barrier_incomplete")
                }
                Phase::Superseded => anyhow::ensure!(
                    operation
                        .superseded_by
                        .as_ref()
                        .and_then(|id| self.operations.get(id))
                        .is_some_and(|next| next.binding.route_id == operation.binding.route_id
                            && next.binding.revision > operation.binding.revision),
                    "control_supersession_invalid"
                ),
            }
            anyhow::ensure!(
                operation.completed_at.is_some() == (operation.phase == Phase::Effective),
                "control_completion_invalid"
            );
        }
        for (id, permit) in &self.permits {
            identifier(id)?;
            identifier(&permit.request_nonce)?;
            permit.binding.validate()?;
            anyhow::ensure!(
                *id == permit.id
                    && (1000..=60000).contains(&permit.ttl_ms)
                    && self
                        .nodes
                        .get(&permit.node_id)
                        .is_some_and(|node| node.startups.get(&permit.startup_request)
                            == Some(&permit.incarnation)),
                "control_permit_identity_invalid"
            );
            let route = self
                .routes
                .get(&permit.binding.route_id)
                .ok_or_else(|| anyhow::anyhow!("control_permit_route_missing"))?;
            anyhow::ensure!(
                std::iter::once(&route.definition)
                    .chain(&route.previous)
                    .any(|definition| Binding::of(definition) == permit.binding),
                "control_permit_definition_missing"
            );
        }
        for (id, action) in &self.ownership_actions {
            identifier(id)?;
            identifier(&action.actor)?;
            anyhow::ensure!(
                *id == action.id
                    && action.fingerprint.len() == 64
                    && self
                        .routes
                        .get(&action.route_id)
                        .is_some_and(|route| action.owner_revision > 0
                            && action.owner_revision <= route.owner_revision),
                "control_ownership_action_invalid"
            );
        }
        for (source, history) in &self.manifests {
            identifier(source)?;
            anyhow::ensure!(!history.is_empty(), "control_manifest_history_empty");
            let mut last = 0;
            for manifest in history {
                let current = revision(&manifest.generation)?;
                anyhow::ensure!(
                    manifest.source_id == *source
                        && manifest.actor == *source
                        && current > last
                        && manifest.entries.len() <= 1024,
                    "control_manifest_history_invalid"
                );
                last = current;
                let proposals = manifest
                    .entries
                    .iter()
                    .map(|(id, entry)| (id, &entry.proposal))
                    .collect::<BTreeMap<_, _>>();
                let digest = format!(
                    "sha256:{:x}",
                    Sha256::digest(serde_json::to_vec(&(
                        source,
                        &manifest.generation,
                        proposals
                    ))?)
                );
                anyhow::ensure!(digest == manifest.digest, "control_manifest_digest_invalid");
                for (id, entry) in &manifest.entries {
                    entry.proposal.binding.validate()?;
                    anyhow::ensure!(
                        *id == entry.proposal.binding.route_id,
                        "control_manifest_entry_invalid"
                    );
                    if let ManifestResult::Applied {
                        operation_id,
                    } = &entry.result
                    {
                        anyhow::ensure!(
                            self.operations
                                .get(operation_id)
                                .is_some_and(|operation| operation.binding
                                    == entry.proposal.binding
                                    && operation.actor == *source),
                            "control_manifest_operation_missing"
                        );
                    }
                }
            }
        }
        for (id, action) in &self.retirements {
            identifier(id)?;
            identifier(&action.actor)?;
            let incarnation = revision(&action.incarnation)?;
            anyhow::ensure!(
                *id == action.id
                    && action.fingerprint.len() == 64
                    && !self.operations.contains_key(id)
                    && !self.ownership_actions.contains_key(id)
                    && !action.routes.is_empty()
                    && action
                        .routes
                        .iter()
                        .all(|route| self.routes.contains_key(route))
                    && self.nodes.get(&action.node_id).is_some_and(|node| node
                        .startups
                        .values()
                        .any(|value| *value == incarnation)),
                "control_retirement_invalid"
            );
        }
        Ok(())
    }
}

impl Binding {
    /// 业务作用：核对恢复的控制绑定具有规范路由标识、正代次和完整定义摘要。
    /// 参数说明: 无。
    ///
    /// 返回：绑定可用于精确关联；非法摘要不能参与许可或清单比较。
    pub(super) fn validate(&self) -> anyhow::Result<()> {
        identifier(&self.route_id)?;
        anyhow::ensure!(
            self.revision > 0
                && self.digest.len() == 71
                && self.digest.starts_with("sha256:")
                && self.digest[7..]
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
            "control_binding_invalid"
        );
        Ok(())
    }
}
