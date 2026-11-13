use super::{control::Principal, Deployment};
use crate::domain::message::envelope::{identifier, revision};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

/// YAML 仅在明确启用清单来源时提案；关闭开关不会删除、恢复或转移任何持久路由。
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Default, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase", deny_unknown_fields)]
pub struct Provisioning {
    /// 是否把本次配置作为集群路由清单提案，关闭不撤销已提交路由。
    pub enabled: bool,
    /// 清单归属来源标识，权威按来源维护配置所有权。
    pub source_id: Option<String>,
    /// 该来源的单调十进制清单代次，旧代不能覆盖已接受的更新。
    pub generation: Option<String>,
    /// 本次清单的路由及预期旧 revision；权威逐项检查归属与并发条件。
    pub entries: Vec<Entry>,
}

/// 条目引用同一完整配置里的路由定义，CAS 必须由发布者明确声明。
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Entry {
    /// 当前清单条目对应的路由 ID，必须在完整候选定义中存在。
    pub route_id: String,
    /// 提交前要求权威仍持有的十进制 revision，冲突时拒绝覆盖。
    pub expected_revision: String,
}

impl Provisioning {
    /// 业务作用：检查配置发布开关、固定来源和完整条目引用，禁止以 YAML 自行授予管理权限。
    /// 参数说明：
    /// - `deployment`：同一 naml 快照中的路由与控制来源目录。
    ///
    /// 返回：关闭时只停止提案；开启时必须具有集群权威、受权签名身份、合法代次与逐项 CAS。
    pub fn validate(&self, deployment: &Deployment) -> anyhow::Result<()> {
        anyhow::ensure!(self.entries.len() <= 1024, "manifest_capacity");
        if !self.enabled {
            return Ok(());
        }
        let settings = deployment
            .control
            .settings()
            .ok_or_else(|| anyhow::anyhow!("provisioning_requires_control_authority"))?;
        let source = self
            .source_id
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("manifest_source_required"))?;
        identifier(source)?;
        revision(
            self.generation
                .as_deref()
                .ok_or_else(|| anyhow::anyhow!("manifest_generation_required"))?,
        )?;
        let key = settings
            .provisioner_key_id
            .as_ref()
            .and_then(|key| settings.keys.get(key))
            .ok_or_else(|| anyhow::anyhow!("route_provisioner_required"))?;
        let Principal::Administrator {
            subject,
            routes,
        } = &key.principal
        else {
            anyhow::bail!("route_provisioner_required");
        };
        anyhow::ensure!(subject == source, "manifest_source_denied");
        let mut seen = BTreeSet::new();
        for entry in &self.entries {
            identifier(&entry.route_id)?;
            if entry.expected_revision != "0" {
                revision(&entry.expected_revision)?;
            }
            anyhow::ensure!(
                seen.insert(&entry.route_id) && routes.contains(&entry.route_id),
                "manifest_route_denied_or_duplicate"
            );
            anyhow::ensure!(
                deployment
                    .routes
                    .iter()
                    .any(|route| route.route_id == entry.route_id),
                "manifest_definition_missing"
            );
        }
        Ok(())
    }
}
