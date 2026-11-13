//! 持久节点组的读取、认领与确认共用同一 Redis 围栏，业务正文不进入控制账本。

use super::record::Record;
use crate::config::{messaging::Ingress, Deployment};
use nasa::redis::{RedisClient, RedisCommand};
use sha2::{Digest, Sha256};
use std::sync::Arc;

#[derive(Clone)]
pub struct Recovery {
    /// 由宿主管理生命周期的固定 Redis 数据来源。
    client: Arc<RedisClient>,
    /// 本恢复执行器唯一读取的业务或控制 Stream。
    stream: String,
    /// 持久消费基线及围栏记录的同槽 Redis key。
    owner: String,
    /// 无效原文的固定隔离 Stream，与输入和围栏记录处于同槽。
    destination: String,
    /// 绑定部署、节点和入口的稳定消费组名，重启继续原 PEL。
    group: String,
    /// 控制权威确认的当前节点代际，每次读取和确认都复验。
    incarnation: String,
    /// 本进程精确启动请求 ID，不能用相同代际的其它进程接管责任。
    startup: String,
    /// 实际单次读取的记录数量上限，受入口和读前预算共同限制。
    batch: usize,
    /// 单条完整原始记录及字段结构的字节上限，读取和隔离前核对。
    maximum: usize,
}

impl Recovery {
    /// 业务作用：只读核对精确恢复围栏并取得积压和完整有限 PEL 的空闲时间。
    /// 参数说明: 无。
    ///
    /// 返回：同一 Redis 执行中的位置证据；缺口、失权、缺少 ACL 或 PEL 超界均为未知，不推进消费。
    pub async fn observe(&self) -> anyhow::Result<crate::observability::broker::Reading> {
        let (unread, pending, idle): (u64, u64, u64) =
            self.client.execute_raw(&self.command("observe")).await?;
        Ok(crate::observability::broker::Reading {
            dependency: None,
            unread_positions: unread,
            pending_records: Some(pending),
            pending_max_idle: Some(std::time::Duration::from_millis(idle)),
            partitions: None,
        })
    }
    /// 业务作用：从已确认节点身份建立稳定消费组名并取得单调围栏，不修改其它节点组。
    /// 参数说明：
    /// - `client`：应用受管的 Redis 数据来源。
    /// - `deployment`：固定部署作用域和稳定逻辑节点。
    /// - `ingress_id`：该来源的稳定产品入口名称。
    /// - `ingress`：恢复、隔离和容量合同。
    /// - `incarnation`：控制账本已经确认的本次节点代际。
    /// - `startup`：本进程唯一启动申请，不能充当代际。
    ///
    /// 返回：取得围栏的恢复执行器；已有基线不完整或旧实例申请均拒绝。
    pub async fn open(
        client: Arc<RedisClient>,
        deployment: &Deployment,
        ingress_id: &str,
        ingress: &Ingress,
        incarnation: &str,
        startup: &str,
    ) -> anyhow::Result<Self> {
        anyhow::ensure!(deployment.control.clustered(), "recovery_control_required");
        let crate::config::messaging::Backend::RedisStream {
            stream,
            ..
        } = &ingress.transport
        else {
            anyhow::bail!("recovery_backend_invalid");
        };
        crate::domain::message::envelope::revision(incarnation)?;
        crate::domain::message::envelope::identifier(startup)?;
        let mut hash = Sha256::new();
        for value in [
            &deployment.cluster_id,
            &deployment.scope_epoch,
            &deployment.node_id,
            ingress_id,
            stream,
        ] {
            hash.update((value.len() as u32).to_be_bytes());
            hash.update(value.as_bytes());
        }
        let identity = format!("{:x}", hash.finalize());
        let tag = crate::config::messaging::hash_tag(stream)?;
        let maximum = ingress.max_metadata_bytes + ingress.max_body_bytes + 4096;
        let recovery = Self {
            client,
            stream: stream.clone(),
            owner: format!("nacket.recovery:{{{tag}}}:{identity}"),
            destination: ingress.dead_letter.clone(),
            group: format!("nacket.data.{identity}"),
            incarnation: incarnation.into(),
            startup: startup.into(),
            batch: ingress.batch_size.min((32 * 1024 * 1024 / maximum).max(1)),
            maximum,
        };
        let mut command = recovery.command("acquire");
        command.arg(if ingress.recovery.initialize {
            "1"
        } else {
            "0"
        });
        let _: String = recovery.client.execute_raw(&command).await?;
        recovery.cleanup().await?;
        Ok(recovery)
    }

    /// 业务作用：优先认领未完成记录，PEL 清空后才读取新记录，保持节点组的连续处理顺序。
    /// 参数说明: 无。
    ///
    /// 返回：有界且保持原字段顺序的记录；缺口、失权或基线丢失时停止推进。
    pub async fn read(&self) -> anyhow::Result<Vec<Record>> {
        let mut command = self.command("read");
        command.arg(self.batch).arg(self.maximum);
        type Entries = Vec<(String, Vec<Vec<u8>>)>;
        let (entries, claimed): (Entries, usize) = self.client.execute_raw(&command).await?;
        anyhow::ensure!(
            claimed == 0 || claimed == entries.len(),
            "recovery_claim_response_invalid"
        );
        entries
            .into_iter()
            .map(|(id, values)| {
                let mut record = Record::from_parts(&self.stream, id, values)?;
                record.reclaimed = claimed > 0;
                Ok(record)
            })
            .collect()
    }

    /// 业务作用：仅在全部本地决定完成后确认精确记录；无效记录先在同槽脚本中保留原文。
    /// 参数说明：
    /// - `id`：本次已经认领的原始记录 ID。
    /// - `reject`：是否需要隔离，暂时依赖失败不能设为 true。
    ///
    /// 返回：确认成功或已确认的幂等成功；旧代际不得确认，隔离失败保留 PEL。
    pub async fn complete(&self, id: &str, reject: bool) -> anyhow::Result<()> {
        let mut command = self.command(if reject {
            "reject"
        } else {
            "ack"
        });
        command.arg(id).arg(self.maximum);
        let _: u64 = self.client.execute_raw(&command).await?;
        Ok(())
    }

    /// 业务作用：回收当前逻辑节点组中已无 pending 的旧 consumer，不删除恢复基线。
    /// 参数说明: 无。
    ///
    /// 返回：空旧 consumer 已回收；当前 consumer 和其它节点组不受影响。
    pub async fn cleanup(&self) -> anyhow::Result<()> {
        let command = self.command("cleanup");
        let _: u64 = self.client.execute_raw(&command).await?;
        Ok(())
    }

    /// 业务作用：为每个恢复动作携带完全相同的资源与身份，禁止调用点遗漏围栏参数。
    /// 参数说明：
    /// - `action`：执行器内部选择的固定动作。
    ///
    /// 返回：尚未执行的同槽 Lua 命令。
    fn command(&self, action: &str) -> RedisCommand {
        let mut command = RedisCommand::new();
        command
            .arg(if action == "observe" {
                "EVAL_RO"
            } else {
                "EVAL"
            })
            .arg(include_str!("recovery.lua"))
            .arg(3)
            .arg(&self.stream)
            .arg(&self.owner)
            .arg(&self.destination)
            .arg(action)
            .arg(&self.group)
            .arg(&self.incarnation)
            .arg(&self.startup);
        command
    }
}
