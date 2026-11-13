use super::*;
use crate::domain::message::{
    envelope::{identifier, revision},
    UnixMillis,
};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

impl Ledger {
    /// 业务作用：受理单调配置清单并逐项推进已有进度，使重试、接管和人工暂停使用同一持久边界。
    /// 参数说明：
    /// - `principal`：部署验证的配置发布来源及路由范围。
    /// - `command`：完整清单摘要声明或其中一项的冻结定义。
    /// - `now`：受理时间，只用于诊断和审计关联。
    /// - `limits`：清单历史、路由和操作的共同容量限制。
    ///
    /// 返回：待提交清单记录；条目拒绝被持久保存，不因之后解除暂停而自动重试。
    pub(super) fn manifest(
        &mut self,
        principal: &Principal,
        command: &Command,
        now: UnixMillis,
        limits: &Limits,
    ) -> anyhow::Result<Reply> {
        let Principal::Provisioner {
            subject,
            routes,
        } = principal
        else {
            anyhow::bail!("provisioner_required");
        };
        let (source, generation) = match command {
            Command::Manifest {
                source_id,
                generation,
                ..
            }
            | Command::ApplyManifest {
                source_id,
                generation,
                ..
            } => (source_id, generation),
            _ => anyhow::bail!("manifest_command_required"),
        };
        identifier(source)?;
        revision(generation)?;
        anyhow::ensure!(source == subject, "manifest_source_denied");
        let number = generation.parse::<u64>()?;
        match command {
            Command::Manifest {
                entries,
                ..
            } => {
                anyhow::ensure!(entries.len() <= limits.routes, "manifest_capacity");
                let mut ordered = BTreeMap::new();
                for entry in entries {
                    let id = &entry.binding.route_id;
                    identifier(id)?;
                    anyhow::ensure!(routes.contains(id), "route_control_denied");
                    anyhow::ensure!(
                        entry.binding.revision > 0
                            && entry.binding.digest.len() == 71
                            && entry.binding.digest.starts_with("sha256:")
                            && entry.binding.digest[7..]
                                .bytes()
                                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
                        "manifest_binding_invalid"
                    );
                    anyhow::ensure!(
                        ordered.insert(id.clone(), entry.clone()).is_none(),
                        "manifest_duplicate_route"
                    );
                }
                let digest = format!(
                    "sha256:{:x}",
                    Sha256::digest(serde_json::to_vec(&(source, generation, &ordered))?)
                );
                if let Some(history) = self.manifests.get(source) {
                    if let Some(old) = history.iter().find(|old| old.generation == *generation) {
                        anyhow::ensure!(
                            old.digest == digest && old.actor == *subject,
                            "manifest_generation_conflict"
                        );
                        return Ok(Reply::Manifest {
                            manifest: old.clone(),
                        });
                    }
                    anyhow::ensure!(
                        history.last().is_none_or(|old| old
                            .generation
                            .parse::<u64>()
                            .is_ok_and(|old| number > old)),
                        "manifest_generation_stale"
                    );
                }
                let count: usize = self.manifests.values().map(Vec::len).sum();
                anyhow::ensure!(
                    count < limits.operations
                        && (self.manifests.contains_key(source) || self.manifests.len() < 64),
                    "manifest_history_capacity"
                );
                let entries = ordered
                    .into_iter()
                    .map(|(id, proposal)| {
                        let result = match self.admit_manifest_entry(source, &proposal) {
                            Ok(result) => result,
                            Err(error) => ManifestResult::Rejected {
                                code: error.to_string(),
                            },
                        };
                        (
                            id,
                            ManifestEntry {
                                proposal,
                                result,
                            },
                        )
                    })
                    .collect();
                let manifest = ManifestRecord {
                    source_id: source.clone(),
                    generation: generation.clone(),
                    digest,
                    actor: subject.clone(),
                    accepted_at: now,
                    entries,
                };
                // 先保存逐项 CAS 和受理结果，之后才执行定义动作；缺项不产生任何删除或恢复授权。
                self.manifests
                    .entry(source.clone())
                    .or_default()
                    .push(manifest.clone());
                Ok(Reply::Manifest {
                    manifest,
                })
            }
            Command::ApplyManifest {
                definition,
                minimum_ready,
                ..
            } => {
                let id = &definition.route.route_id;
                anyhow::ensure!(routes.contains(id), "route_control_denied");
                let history = self
                    .manifests
                    .get(source)
                    .ok_or_else(|| anyhow::anyhow!("manifest_missing"))?;
                let index = history
                    .iter()
                    .position(|record| record.generation == *generation)
                    .ok_or_else(|| anyhow::anyhow!("manifest_missing"))?;
                let record = &history[index];
                let entry = record
                    .entries
                    .get(id)
                    .ok_or_else(|| anyhow::anyhow!("manifest_entry_missing"))?;
                anyhow::ensure!(
                    entry.proposal.binding == Binding::of(definition)
                        && entry.proposal.enabled == definition.route.enabled,
                    "manifest_definition_mismatch"
                );
                let ManifestResult::Pending {
                    expected_owner_revision,
                } = entry.result
                else {
                    return Ok(Reply::Manifest {
                        manifest: record.clone(),
                    });
                };
                // 新清单受理后旧清单只能查询，不能迟到执行尚未提交的业务变更。
                anyhow::ensure!(index + 1 == history.len(), "manifest_generation_stale");
                let command = Command::Define {
                    operation_id: format!(
                        "{:x}",
                        Sha256::digest(serde_json::to_vec(&(
                            source,
                            generation,
                            id,
                            &record.digest
                        ))?)
                    ),
                    expected_revision: entry.proposal.expected_revision,
                    definition: definition.clone(),
                    minimum_ready: *minimum_ready,
                };
                let current_owner_revision =
                    self.routes.get(id).map_or(0, |route| route.owner_revision);
                let result = if current_owner_revision != expected_owner_revision {
                    ManifestResult::Rejected {
                        code: "owner_revision_conflict".into(),
                    }
                } else {
                    // 条目单独构造候选，冻结定义或资源容量校验失败不能留下半个操作。
                    let mut candidate = self.clone();
                    match candidate.define(principal, &command, now, limits) {
                        Ok(Reply::Operation {
                            operation,
                        }) => {
                            *self = candidate;
                            ManifestResult::Applied {
                                operation_id: operation.id,
                            }
                        }
                        Ok(_) => anyhow::bail!("manifest_operation_invalid"),
                        Err(error) => ManifestResult::Rejected {
                            code: error.to_string(),
                        },
                    }
                };
                let record = &mut self.manifests.get_mut(source).expect("accepted manifest")[index];
                record.entries.get_mut(id).expect("accepted entry").result = result;
                Ok(Reply::Manifest {
                    manifest: record.clone(),
                })
            }
            _ => anyhow::bail!("manifest_command_required"),
        }
    }

    /// 业务作用：在清单受理时固定条目的归属和 revision 前提，并识别内容完全相同的定义。
    /// 参数说明：
    /// - `source`：受权配置来源标识。
    /// - `entry`：期望定义摘要、路由 CAS 与上下线状态。
    ///
    /// 返回：待执行或未改变；暂停、归属冲突与过期定义成为该条目的持久拒绝结果。
    fn admit_manifest_entry(
        &self,
        source: &str,
        entry: &ManifestProposal,
    ) -> anyhow::Result<ManifestResult> {
        let current = self.routes.get(&entry.binding.route_id);
        if let Some(route) = current {
            anyhow::ensure!(
                route.owner_kind == OwnerKind::Config && route.owner == source,
                "route_source_conflict"
            );
            anyhow::ensure!(!route.suspended || !entry.enabled, "route_suspended");
            if Binding::of(&route.definition) == entry.binding
                && route.definition.route.enabled == entry.enabled
            {
                anyhow::ensure!(
                    entry.expected_revision == entry.binding.revision
                        || entry.expected_revision.checked_add(1) == Some(entry.binding.revision),
                    "route_revision_conflict"
                );
                return Ok(ManifestResult::Unchanged {});
            }
            anyhow::ensure!(
                self.operations
                    .get(&route.operation_id)
                    .is_some_and(|op| op.phase == Phase::Effective)
                    || !entry.enabled,
                "route_operation_pending"
            );
        }
        anyhow::ensure!(
            current.map_or(0, |route| route.definition.route.revision) == entry.expected_revision
                && entry.expected_revision.checked_add(1) == Some(entry.binding.revision),
            "route_revision_conflict"
        );
        Ok(ManifestResult::Pending {
            expected_owner_revision: current.map_or(0, |route| route.owner_revision),
        })
    }
}
