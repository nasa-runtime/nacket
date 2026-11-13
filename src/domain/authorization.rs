//! 业务授权版本与路由定义分别持久保存；时间到期不删除已知撤权下界。

use crate::domain::message::{
    envelope::{identifier, revision, validate_digest, SessionKey},
    UnixMillis,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};

/// 单个撤权状态目录的最大记录数，超过时保护权限域，不丢弃已有下界。
pub const MAXIMUM_RECORDS: usize = 16_384;

/// 动作只收窄权限；三种业务版本互不替代，踢出只针对完整会话。
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Action {
    /// 推进指定租户与用户的身份撤权下界。
    RevokeSubject {
        #[serde(rename = "tenantId")]
        /// 当前撤权动作明确限定的租户，来源授权与实际会话均需核对。
        tenant_id: String,
        /// 身份、成员或精确踢出动作限定的单个用户，不能扩大到其它用户。
        uid: String,
        #[serde(rename = "throughVersion")]
        /// 撤销到此值为止的十进制版本，按主体、成员或群的独立版本域解释。
        through_version: String,
    },
    /// 推进指定用户对单个订阅目标的成员授权下界。
    RevokeSubscription {
        #[serde(rename = "tenantId")]
        /// 当前撤权动作明确限定的租户，来源授权与实际会话均需核对。
        tenant_id: String,
        /// 身份、成员或精确踢出动作限定的单个用户，不能扩大到其它用户。
        uid: String,
        #[serde(rename = "groupId")]
        /// 当前成员或群撤权限定的单群 ID，不能跨租户合并同名群。
        group_id: String,
        #[serde(rename = "throughVersion")]
        /// 撤销到此值为止的十进制版本，按主体、成员或群的独立版本域解释。
        through_version: String,
    },
    /// 推进订阅目标自身的版本下界，使旧目标资格失效。
    RemoveGroup {
        #[serde(rename = "tenantId")]
        /// 当前撤权动作明确限定的租户，来源授权与实际会话均需核对。
        tenant_id: String,
        #[serde(rename = "groupId")]
        /// 当前成员或群撤权限定的单群 ID，不能跨租户合并同名群。
        group_id: String,
        #[serde(rename = "throughVersion")]
        /// 撤销到此值为止的十进制版本，按主体、成员或群的独立版本域解释。
        through_version: String,
    },
    /// 只关闭完整会话键对应的连接，不影响后续重新认证的新会话。
    Kick {
        #[serde(rename = "tenantId")]
        /// 当前撤权动作明确限定的租户，来源授权与实际会话均需核对。
        tenant_id: String,
        /// 身份、成员或精确踢出动作限定的单个用户，不能扩大到其它用户。
        uid: String,
        #[serde(rename = "sessionKey")]
        /// 精确踢出动作的完整会话键，不影响后续重连的其它会话。
        session_key: SessionKey,
    },
}

/// 业务服务先完成权限校验，再经专用 MQ 来源提交固定命令。
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Command {
    /// 固定业务撤权合同标识，普通推送和初始化回复不能进入此通道。
    pub contract: String,
    /// 原业务动作稳定的幂等 ID，同 ID 内容不一致时拒绝。
    pub command_id: String,
    /// 撤权所属部署集群，权威不能替其它集群接纳命令。
    pub cluster_id: String,
    /// 原控制基线的十进制作用域代际，旧基线命令不能作用于新环境。
    pub scope_epoch: String,
    /// 单一路由权限域，命令不会跨路由传播同名主体下界。
    pub route_id: String,
    /// 原路由定义的十进制 revision，接纳时与当前权威核对。
    pub route_revision: String,
    /// 原完整定义摘要，防止同 revision 的不同权限配置混用。
    pub definition_digest: String,
    /// 原业务生产者身份，须与入口认证结果及路由允许范围一致。
    pub producer: String,
    /// 业务首次产生命令的 Unix 毫秒时刻，重试保持不变。
    pub created_at: UnixMillis,
    /// 首次接纳命令的 Unix 毫秒截止时刻，命令到期不能撤回已经持久接受的撤权事实。
    pub expires_at: UnixMillis,
    /// 受来源范围约束的单个身份、成员、群或精确会话撤权动作。
    pub action: Action,
}

/// 同一资源的最高撤权版本，不能因客户端离线或路由换代而回退。
#[derive(Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct State {
    /// 该路由撤权投影的单调提交 revision，节点续期前必须应用到当前值。
    pub revision: u64,
    /// 各精确撤权键对应的最高业务下界或踢出事实，不因操作完成自动清理。
    pub records: BTreeMap<String, Action>,
    /// 稳定命令 ID 对应的持久幂等记录及旧许可屏障。
    pub operations: BTreeMap<String, Operation>,
}

/// 持久接受与全局完成分开；屏障只等待命令受理时仍可写出的精确许可。
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Operation {
    /// 权威首次接受的业务命令 ID，重试查询同一操作。
    pub command_id: String,
    /// 当前动作精确定位的撤权键，区分身份、成员、群和会话版本域。
    pub record_key: String,
    /// 完整原命令的确定指纹，防止同 ID 修改作用对象、版本或期限。
    pub fingerprint: String,
    /// 接纳该操作后产生的撤权投影 revision，节点据此证明应用进度。
    pub revision: u64,
    /// 权威接受该撤权的 Unix 毫秒时刻，早于全局完成。
    pub accepted_at: UnixMillis,
    /// 全部旧责任合法结束后的 Unix 毫秒时刻，缺乏证据时保持待决。
    pub completed_at: Option<UnixMillis>,
    /// 接纳时捕获的旧路由许可 ID 集合，节点应用与真实排空或合法失效共同解除。
    pub barrier: BTreeSet<String>,
}

impl Action {
    /// 业务作用：核对动作的单值范围与单调版本，禁止由时间戳或通配目标扩大撤权域。
    /// 参数说明: 无。
    ///
    /// 返回：动作字段合法；会话目标的完整隔离字段必须同时有效。
    pub fn validate(&self) -> anyhow::Result<()> {
        identifier(self.tenant())?;
        match self {
            Self::RevokeSubject {
                uid,
                through_version,
                ..
            } => {
                identifier(uid)?;
                revision(through_version)?;
            }
            Self::RevokeSubscription {
                uid,
                group_id,
                through_version,
                ..
            } => {
                identifier(uid)?;
                identifier(group_id)?;
                revision(through_version)?;
            }
            Self::RemoveGroup {
                group_id,
                through_version,
                ..
            } => {
                identifier(group_id)?;
                revision(through_version)?;
            }
            Self::Kick {
                uid,
                session_key,
                ..
            } => {
                identifier(uid)?;
                session_key.validate()?;
            }
        }
        Ok(())
    }

    /// 业务作用：选择固定动作权限，不接受生产者自报的新动作名称。
    /// 参数说明: 无。
    ///
    /// 返回：部署权限目录使用的封闭动作名。
    pub fn kind(&self) -> &'static str {
        match self {
            Self::RevokeSubject {
                ..
            } => "revoke_subject",
            Self::RevokeSubscription {
                ..
            } => "revoke_subscription",
            Self::RemoveGroup {
                ..
            } => "remove_group",
            Self::Kick {
                ..
            } => "kick",
        }
    }

    /// 业务作用：从已解析的单值目标读取租户，供来源范围检查使用。
    /// 参数说明: 无。
    ///
    /// 返回：动作明确指定的租户，不能从正文或客户端补齐。
    pub fn tenant(&self) -> &str {
        match self {
            Self::RevokeSubject {
                tenant_id,
                ..
            }
            | Self::RevokeSubscription {
                tenant_id,
                ..
            }
            | Self::RemoveGroup {
                tenant_id,
                ..
            }
            | Self::Kick {
                tenant_id,
                ..
            } => tenant_id,
        }
    }

    /// 业务作用：为相同业务版本域生成稳定存储键，路由换代不改变资源身份。
    /// 参数说明: 无。
    ///
    /// 返回：规范字段编码的 SHA-256；不把版本、授权期限混入资源键。
    pub fn key(&self) -> anyhow::Result<String> {
        let parts = match self {
            Self::RevokeSubject {
                tenant_id,
                uid,
                ..
            } => serde_json::to_vec(&(self.kind(), tenant_id, uid))?,
            Self::RevokeSubscription {
                tenant_id,
                uid,
                group_id,
                ..
            } => serde_json::to_vec(&(self.kind(), tenant_id, uid, group_id))?,
            Self::RemoveGroup {
                tenant_id,
                group_id,
                ..
            } => serde_json::to_vec(&(self.kind(), tenant_id, group_id))?,
            Self::Kick {
                tenant_id,
                uid,
                session_key,
            } => serde_json::to_vec(&(self.kind(), tenant_id, uid, session_key))?,
        };
        Ok(format!("{:x}", Sha256::digest(parts)))
    }

    /// 业务作用：在同一资源域内比较撤销下界，不用字符串字典序决定业务新旧。
    /// 参数说明: 无。
    ///
    /// 返回：经过范围检查的计数；精确会话踢出没有可递增的业务版本，固定为一。
    pub fn version(&self) -> anyhow::Result<u64> {
        match self {
            Self::RevokeSubject {
                through_version,
                ..
            }
            | Self::RevokeSubscription {
                through_version,
                ..
            }
            | Self::RemoveGroup {
                through_version,
                ..
            } => revision(through_version),
            Self::Kick {
                ..
            } => Ok(1),
        }
    }
}

impl Command {
    /// 业务作用：使同一路由租户的用户、成员和群撤权进入同一 Kafka 顺序域。
    /// 参数说明: 无。
    ///
    /// 返回：规范作用域摘要的 ASCII 字节，不包含命令 ID 或可变时间。
    pub fn ordering_key(&self) -> anyhow::Result<Vec<u8>> {
        Ok(format!(
            "authorization.{:x}",
            Sha256::digest(serde_json::to_vec(&(
                &self.cluster_id,
                &self.scope_epoch,
                &self.route_id,
                self.action.tenant()
            ))?)
        )
        .into_bytes())
    }
    /// 业务作用：冻结业务命令的隔离、定义和时间范围，防止重投延长原始受理期限。
    /// 参数说明: 无。
    ///
    /// 返回：完整合同合法；生效时还必须核对实际生产者、当前定义与权威时间。
    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.contract == "nacket.authorization/1",
            "authorization_contract_invalid"
        );
        for value in [
            &self.command_id,
            &self.cluster_id,
            &self.route_id,
            &self.producer,
        ] {
            identifier(value)?;
        }
        revision(&self.scope_epoch)?;
        revision(&self.route_revision)?;
        validate_digest(&self.definition_digest)?;
        anyhow::ensure!(
            self.expires_at > self.created_at
                && self.expires_at.as_millis() - self.created_at.as_millis() <= 300_000,
            "authorization_lifetime_invalid"
        );
        self.action.validate()?;
        if let Action::Kick {
            session_key,
            ..
        } = &self.action
        {
            anyhow::ensure!(
                session_key.cluster_id == self.cluster_id
                    && session_key.scope_epoch == self.scope_epoch
                    && session_key.route_id == self.route_id,
                "authorization_session_scope"
            );
        }
        Ok(())
    }

    /// 业务作用：绑定命令 ID 的全部内容，网络重试和多节点消费不能替换目标或更新期限。
    /// 参数说明: 无。
    ///
    /// 返回：规范结构的摘要；来源材料仍由入口单独认证。
    pub fn fingerprint(&self) -> anyhow::Result<String> {
        Ok(format!("{:x}", Sha256::digest(serde_json::to_vec(self)?)))
    }
}

impl State {
    /// 业务作用：在控制账本恢复前检查撤权记录和操作历史的关联，不允许缺失下界继续接流。
    /// 参数说明: 无。
    ///
    /// 返回：记录有界、键与内容一致、操作完成与屏障一致时通过。
    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.records.len() <= MAXIMUM_RECORDS && self.operations.len() <= MAXIMUM_RECORDS,
            "authorization_state_capacity"
        );
        for (key, action) in &self.records {
            action.validate()?;
            anyhow::ensure!(*key == action.key()?, "authorization_record_mismatch");
        }
        let mut revisions = BTreeSet::new();
        let mut resources = BTreeSet::new();
        for (id, operation) in &self.operations {
            identifier(id)?;
            resources.insert(operation.record_key.clone());
            anyhow::ensure!(
                *id == operation.command_id
                    && self.records.contains_key(&operation.record_key)
                    && revisions.insert(operation.revision)
                    && operation.revision > 0
                    && operation.revision <= self.revision
                    && operation.fingerprint.len() == 64
                    && operation
                        .fingerprint
                        .bytes()
                        .all(|byte| byte.is_ascii_hexdigit())
                    && operation.completed_at.is_some() == operation.barrier.is_empty(),
                "authorization_operation_invalid"
            );
        }
        anyhow::ensure!(
            self.revision == self.operations.len() as u64 && resources.len() == self.records.len(),
            "authorization_history_missing"
        );
        Ok(())
    }

    /// 业务作用：受理已经完成来源授权的撤权候选，先保存版本下界再等待旧写出屏障。
    /// 参数说明：
    /// - `command`：完整且通过权限检查的业务命令。
    /// - `barrier`：受理时实际存在的路由写出许可集合。
    /// - `now`：权威受理时间，不用于推断节点失权。
    ///
    /// 返回：持久候选中的操作状态；未到创建时刻可按原内容重试，冲突、过期或容量不足不改变当前状态。
    pub fn accept(
        &mut self,
        command: &Command,
        barrier: BTreeSet<String>,
        now: UnixMillis,
    ) -> anyhow::Result<Operation> {
        command.validate()?;
        let fingerprint = command.fingerprint()?;
        if let Some(previous) = self.operations.get(&command.command_id) {
            anyhow::ensure!(
                previous.fingerprint == fingerprint,
                "authorization_command_conflict"
            );
            return Ok(previous.clone());
        }
        anyhow::ensure!(now < command.expires_at, "authorization_command_expired");
        // 生产者时钟可能稍快；尚未生效不能解释为永久过期，重试仍受原命令期限和入口进度保护约束。
        anyhow::ensure!(
            command.created_at <= now,
            "authorization_command_not_yet_valid"
        );
        let key = command.action.key()?;
        anyhow::ensure!(
            self.operations.len() < MAXIMUM_RECORDS
                && (self.records.contains_key(&key) || self.records.len() < MAXIMUM_RECORDS),
            "authorization_state_capacity"
        );
        let version = command.action.version()?;
        let replace = self
            .records
            .get(&key)
            .map(|previous| previous.version().map(|previous| version > previous))
            .transpose()?
            .unwrap_or(true);
        let revision = self
            .revision
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("authorization_revision_exhausted"))?;
        let operation = Operation {
            command_id: command.command_id.clone(),
            record_key: key.clone(),
            fingerprint,
            revision,
            accepted_at: now,
            completed_at: barrier.is_empty().then_some(now),
            barrier,
        };
        // 同一事务保存下界、操作和屏障；不能先回复已接受再依赖进程内缓存补记撤权。
        if replace {
            self.records.insert(key, command.action.clone());
        }
        self.revision = revision;
        self.operations
            .insert(command.command_id.clone(), operation.clone());
        Ok(operation)
    }
}
