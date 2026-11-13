use super::*;

/// 存储选择在启动时固定，读取和回复都必须来自所选后端的已提交状态。
pub(super) enum Backend {
    Redis(
        /// 原子账本、控制输入及租约围栏的 Redis 权威入口。
        RedisStore,
    ),
    Kafka(
        /// 只接受已提交事务投影的 Kafka 权威入口。
        Arc<crate::adapters::control::kafka::Store>,
    ),
}

impl Backend {
    /// 业务作用：向启动固定的控制输入发布请求，不进行后端自动切换。
    /// 参数说明：
    /// - `request`：已经签名的完整控制合同。
    ///
    /// 返回：后端实际确认的位置；失败保留未知结果语义。
    pub async fn submit(&self, request: &[u8]) -> anyhow::Result<String> {
        match self {
            Self::Redis(store) => store.submit(request).await,
            Self::Kafka(store) => store.submit(request).await,
        }
    }
    /// 业务作用：读取完整已提交基线，为持久高水位检查提供统一输入。
    /// 参数说明: 无。
    ///
    /// 返回：完整冻结快照；后端未建账、缺口或不可用时拒绝。
    pub async fn read(&self) -> anyhow::Result<Snapshot> {
        match self {
            Self::Redis(store) => store.read().await,
            Self::Kafka(store) => store.read().await,
        }
    }
    /// 业务作用：查回实际提交的控制结果，不返回处理器内的候选状态。
    /// 参数说明：
    /// - `id`：完整请求关联摘要。
    ///
    /// 返回：已提交结果及不早于该结果的控制快照，或尚无可确认记录。
    pub async fn reply_snapshot(&self, id: &str) -> anyhow::Result<Option<(Reply, Snapshot)>> {
        match self {
            Self::Redis(store) => match store.reply(id).await? {
                Some(reply) => Ok(Some((reply, store.read().await?))),
                None => Ok(None),
            },
            Self::Kafka(store) => store.reply_snapshot(id).await,
        }
    }
    /// 业务作用：在有显式 owner token 的后端归还本实例控制权，Kafka 由事务任务退出归还分配。
    /// 参数说明：
    /// - `owner`：当前 Redis fencing token。
    ///
    /// 返回：只影响本实例的控制租约，不撤销任何节点仍承担的写出责任。
    pub async fn release(&self, owner: &str) -> anyhow::Result<()> {
        match self {
            Self::Redis(store) => store.release(owner).await,
            Self::Kafka(_) => Ok(()),
        }
    }
}
