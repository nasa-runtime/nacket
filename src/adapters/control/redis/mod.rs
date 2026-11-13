use crate::domain::{
    control::{Ledger, Reply, Transition},
    message::envelope::identifier,
};
use nasa::redis::{RedisClient, RedisCommand};
use sha2::{Digest, Sha256};
use std::sync::Arc;

/// 同一次 Redis 原子读取的账本和位置证据，恢复时不得混用不同提交的水位。
pub struct Snapshot {
    /// 同一次原子读取获得的完整持久控制账本。
    pub ledger: Ledger,
    /// 该快照已处理的控制 Stream 记录 ID，后续从此位置推进。
    pub cursor: String,
    /// 控制输入的逻辑位置证据，用于检测裁剪或基线回退。
    pub position: String,
    /// 原始账本字节摘要，用于 CAS 和节点持久高水位核对。
    pub hash: String,
}

/// 所有控制键共用同一 Redis Cluster hash slot，客户端与连接池由 napp 持有。
pub struct Store {
    /// 由统一宿主管理的固定 Redis 控制来源。
    client: Arc<RedisClient>,
    /// 部署命名空间及作用域派生的同槽控制 key 前缀。
    prefix: String,
    /// 持久账本必须匹配的产品控制集群身份。
    cluster_id: String,
    /// 持久账本必须匹配的控制作用域代际。
    scope_epoch: String,
}

impl Store {
    /// 业务作用：向有界持久控制输入追加已经签名的请求，业务消息不能使用此队列。
    /// 参数说明：
    /// - `request`：固定控制合同、来源及命令的完整签名封装。
    ///
    /// 返回：Redis 已确认的输入位置；数量或字节超过部署合同则拒绝。
    pub async fn submit(&self, request: &[u8]) -> anyhow::Result<String> {
        anyhow::ensure!(request.len() <= 262144, "control_request_capacity");
        let mut command = self.script(include_str!("submit.lua"), &["commands"]);
        command.arg(request);
        Ok(self.client.execute_raw(&command).await?)
    }

    /// 业务作用：按已提交游标读取下一批原始控制请求，不在读取时确认或推进位置。
    /// 参数说明：
    /// - `cursor`：已提交快照记录的精确输入水位。
    ///
    /// 返回：最多 32 条持久输入；解析和来源验证由控制入口处理。
    pub async fn commands(&self, cursor: &str) -> anyhow::Result<Vec<nasa::redis::StreamEntry>> {
        validate_cursor(cursor)?;
        Ok(self
            .client
            .x_read(&format!("{}:commands", self.prefix), cursor, 32, None)
            .await?)
    }

    /// 业务作用：派生完整控制隔离域的固定键前缀，不复用业务 Stream 或缓存键空间。
    /// 参数说明：
    /// - `client`：已受管的控制 Redis 来源。
    /// - `namespace`：部署批准的控制命名空间。
    /// - `cluster_id`：产品集群标识。
    /// - `scope_epoch`：受信控制基线代际。
    ///
    /// 返回：无网络副作用的存储适配器。
    pub fn new(
        client: Arc<RedisClient>,
        namespace: &str,
        cluster_id: String,
        scope_epoch: String,
    ) -> anyhow::Result<Self> {
        identifier(namespace)?;
        Ledger::empty(cluster_id.clone(), scope_epoch.clone())?;
        let hash = format!(
            "{:x}",
            Sha256::digest(serde_json::to_vec(&(namespace, &cluster_id, &scope_epoch))?)
        );
        Ok(Self {
            client,
            prefix: format!("socket-control:{{{hash}}}"),
            cluster_id,
            scope_epoch,
        })
    }

    /// 业务作用：通过原子 owner 比较取得或续约控制提交权，新的 term 不等价于旧节点许可已结束。
    /// 参数说明：
    /// - `previous`：本进程已取得的完整 owner token，首次申请为空。
    /// - `instance`：本次 controller 启动的唯一申请标识。
    /// - `ttl_ms`：有限 owner 租期。
    /// - `initialize`：是否显式批准首次建立全新控制作用域。
    ///
    /// 返回：当前 owner token，或被其它 owner 持有；丢失持久基线时拒绝自动重建。
    pub async fn acquire(
        &self,
        previous: Option<&str>,
        instance: &str,
        ttl_ms: u64,
        initialize: bool,
    ) -> anyhow::Result<Option<String>> {
        identifier(instance)?;
        anyhow::ensure!(
            (1000..=60_000).contains(&ttl_ms),
            "invalid_control_owner_ttl"
        );
        let empty = serde_json::to_vec(&Ledger::empty(
            self.cluster_id.clone(),
            self.scope_epoch.clone(),
        )?)?;
        let mut command = self.script(
            include_str!("acquire.lua"),
            &["owner", "term", "state", "events", "cursor", "checkpoint"],
        );
        command
            .arg(previous.unwrap_or(""))
            .arg(instance)
            .arg(ttl_ms)
            .arg(if initialize {
                "initialize"
            } else {
                "existing"
            })
            .arg(empty);
        Ok(self.client.execute_raw(&command).await?)
    }

    /// 业务作用：在一个 Redis 执行边界内读取完整快照与对应日志水位，拒绝部分持久状态。
    /// 参数说明: 无。
    ///
    /// 返回：可用于下一次 CAS 的冻结快照；损坏、缺口或作用域不一致时保持保护。
    pub async fn read(&self) -> anyhow::Result<Snapshot> {
        let command = self.script(
            include_str!("read.lua"),
            &["state", "cursor", "checkpoint", "events"],
        );
        let mut values: Vec<Vec<u8>> = self.client.execute_raw(&command).await?;
        anyhow::ensure!(
            values.len() == 4 && values[0].len() <= 16 * 1024 * 1024,
            "control_snapshot_capacity"
        );
        let hash = String::from_utf8(values.pop().expect("hash"))?;
        let position = String::from_utf8(values.pop().expect("position"))?;
        let cursor = String::from_utf8(values.pop().expect("cursor"))?;
        let ledger: Ledger =
            crate::domain::message::json::decode_bounded(&values[0], 16 * 1024 * 1024, 48, 65536)?;
        ledger.validate()?;
        anyhow::ensure!(
            ledger.cluster_id == self.cluster_id && ledger.scope_epoch == self.scope_epoch,
            "control_scope_mismatch"
        );
        Ok(Snapshot {
            ledger,
            cursor,
            position,
            hash,
        })
    }

    /// 业务作用：在同一脚本内复验真实 owner 和快照 CAS，提交候选、输入游标、事件及查询回复。
    /// 参数说明：
    /// - `owner`：本 controller 的完整 fencing token。
    /// - `base`：计算候选时使用的已提交快照。
    /// - `transition`：尚未对外生效的完整候选。
    /// - `cursor`：本次已处理命令在控制 Stream 的精确位置。
    /// - `request_id`：用于未知提交查回的唯一请求关联。
    /// - `reply_ttl_ms`：查询回复的有限保留期限。
    ///
    /// 返回：后端已确认持久提交的业务回复；网络中断或脚本错误必须先查回而不能声明未执行。
    pub async fn commit(
        &self,
        owner: &str,
        base: &Snapshot,
        transition: Transition,
        cursor: &str,
        request_id: &str,
        reply_ttl_ms: u64,
    ) -> anyhow::Result<Reply> {
        identifier(request_id)?;
        anyhow::ensure!(
            (1000..=300_000).contains(&reply_ttl_ms),
            "invalid_reply_ttl"
        );
        anyhow::ensure!(
            validate_cursor(cursor)? >= validate_cursor(&base.cursor)?,
            "control_cursor_regression"
        );
        anyhow::ensure!(
            transition.ledger.cluster_id == self.cluster_id
                && transition.ledger.scope_epoch == self.scope_epoch
                && base.ledger.sequence.checked_add(1) == Some(transition.ledger.sequence),
            "control_transition_mismatch"
        );
        let bytes = serde_json::to_vec(&transition.ledger)?;
        anyhow::ensure!(bytes.len() <= 16 * 1024 * 1024, "control_snapshot_capacity");
        let reply = serde_json::to_vec(&transition.reply)?;
        let reply_key = format!("reply:{request_id}");
        let mut command = self.script(
            include_str!("commit.lua"),
            &[
                "owner",
                "state",
                "cursor",
                "events",
                "checkpoint",
                &reply_key,
                "commands",
            ],
        );
        command
            .arg(owner)
            .arg(&base.hash)
            .arg(&base.cursor)
            .arg(bytes)
            .arg(cursor)
            .arg(reply)
            .arg(reply_ttl_ms);
        let result: Vec<String> = self.client.execute_raw(&command).await?;
        anyhow::ensure!(result.len() == 2, "control_commit_unknown");
        Ok(transition.reply)
    }

    /// 业务作用：按原请求关联读取已经提交的控制回复，调用方超时不撤销持久操作。
    /// 参数说明：
    /// - `request_id`：本次请求保存的精确关联标识。
    ///
    /// 返回：已提交回复或尚无可确认结果；不存在不能证明从未发生副作用。
    pub async fn reply(&self, request_id: &str) -> anyhow::Result<Option<Reply>> {
        identifier(request_id)?;
        let mut command = RedisCommand::new();
        command
            .arg("GET")
            .arg(format!("{}:reply:{request_id}", self.prefix));
        let bytes: Option<Vec<u8>> = self.client.execute_raw(&command).await?;
        bytes
            .map(|bytes| {
                anyhow::ensure!(bytes.len() <= 16 * 1024 * 1024, "control_reply_capacity");
                serde_json::from_slice(&bytes).map_err(anyhow::Error::from)
            })
            .transpose()
    }

    /// 业务作用：归还本实例仍持有的控制租约，旧 owner 的关闭不能影响继任者。
    /// 参数说明：
    /// - `owner`：本实例保存的完整租约 token。
    ///
    /// 返回：已删除自己的 owner 或已不再持有；任何情况都不撤销账本中的节点写出责任。
    pub async fn release(&self, owner: &str) -> anyhow::Result<()> {
        let mut command = self.script(include_str!("release.lua"), &["owner"]);
        command.arg(owner);
        let _: u64 = self.client.execute_raw(&command).await?;
        Ok(())
    }

    /// 业务作用：构造只访问同一控制 hash slot 的脚本调用，键名不由客户端原样拼接。
    /// 参数说明：
    /// - `script`：产品内置且无动态代码拼接的控制脚本。
    /// - `keys`：内置键后缀及已经验证的请求关联。
    ///
    /// 返回：等待添加有界普通参数的 Redis 命令。
    fn script(&self, script: &str, keys: &[&str]) -> RedisCommand {
        let mut command = RedisCommand::new();
        command.arg("EVAL").arg(script).arg(keys.len());
        for key in keys {
            command.arg(format!("{}:{key}", self.prefix));
        }
        command
    }
}

/// 业务作用：校验输入日志位置的十进制形式，避免游标注入其它 Redis 范围语义。
/// 参数说明：
/// - `cursor`：已读取控制记录的完整 Stream ID。
///
/// 返回：规范有界位置；范围表达式、负数和溢出拒绝。
fn validate_cursor(cursor: &str) -> anyhow::Result<(u64, u64)> {
    let (time, sequence) = cursor
        .split_once('-')
        .ok_or_else(|| anyhow::anyhow!("invalid_control_cursor"))?;
    for part in [time, sequence] {
        anyhow::ensure!(
            !part.is_empty()
                && part.len() <= 20
                && part.bytes().all(|byte| byte.is_ascii_digit())
                && (part.len() == 1 || !part.starts_with('0')),
            "invalid_control_cursor"
        );
        let _: u64 = part.parse()?;
    }
    Ok((time.parse()?, sequence.parse()?))
}
