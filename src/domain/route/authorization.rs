use crate::domain::message::envelope::identifier;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

/// 业务撤权只从明确批准的专用来源进入，关闭时不接受任何授权控制动作。
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase", deny_unknown_fields)]
pub struct ControlPolicy {
    /// 是否允许本路由从独立受信入口接受业务撤权，关闭不清除已有持久下界。
    pub enabled: bool,
    /// 固定授权控制入口引用，不能使用普通业务下行来源代替。
    pub ingress_ref: Option<String>,
    /// 允许发送撤权命令的生产者及其租户、动作范围。
    pub producers: BTreeMap<String, Permissions>,
}

/// 生产者仅能操作显式租户和固定动作集合，普通下行权限不蕴含这些权限。
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Permissions {
    /// 该生产者允许撤权的租户集合，不能由命令自报扩大。
    pub tenant_ids: BTreeSet<String>,
    /// 该生产者允许执行的有限撤权动作集合，签名有效仍需逐项检查。
    pub actions: BTreeSet<String>,
}

impl ControlPolicy {
    /// 业务作用：编译路由授权控制开关及其最小来源范围，拒绝通配和未知动作。
    /// 参数说明: 无。
    ///
    /// 返回：开启时引用和权限完整；关闭配置不产生隐式权限。
    pub fn validate(&self) -> anyhow::Result<()> {
        if let Some(reference) = &self.ingress_ref {
            identifier(reference)?;
        }
        anyhow::ensure!(
            !self.enabled || (self.ingress_ref.is_some() && !self.producers.is_empty()),
            "authorization_control_source_required"
        );
        anyhow::ensure!(
            self.producers.len() <= 32,
            "authorization_producer_capacity"
        );
        for (producer, permissions) in &self.producers {
            identifier(producer)?;
            anyhow::ensure!(
                (1..=128).contains(&permissions.tenant_ids.len())
                    && (1..=4).contains(&permissions.actions.len()),
                "authorization_permission_capacity"
            );
            for tenant in &permissions.tenant_ids {
                identifier(tenant)?;
            }
            anyhow::ensure!(
                permissions.actions.iter().all(|action| matches!(
                    action.as_str(),
                    "revoke_subject" | "revoke_subscription" | "remove_group" | "kick"
                )),
                "authorization_action_invalid"
            );
        }
        Ok(())
    }

    /// 业务作用：在当前权威定义中复验生产者的租户与动作范围，旧配置不能继续授权控制命令。
    /// 参数说明：
    /// - `ingress`：节点实际验证的 MQ 入口。
    /// - `producer`：由 ACL 或签名取得的来源身份。
    /// - `action`：需要执行的单值业务动作。
    ///
    /// 返回：当前开关与全部范围同时允许时为 true。
    pub fn allows(
        &self,
        ingress: &str,
        producer: &str,
        action: &crate::domain::authorization::Action,
    ) -> bool {
        self.enabled
            && self.ingress_ref.as_deref() == Some(ingress)
            && self.producers.get(producer).is_some_and(|permissions| {
                permissions.tenant_ids.contains(action.tenant())
                    && permissions.actions.contains(action.kind())
            })
    }
}
