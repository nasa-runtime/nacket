use super::record::Record;
use nasa::redis::{RedisClient, RedisCommand};
use std::sync::Arc;

pub struct Realtime {
    /// 受管 Redis 数据来源，不独立创建连接生命周期。
    client: Arc<RedisClient>,
    /// 本执行器唯一读取的实时 Stream。
    stream: String,
    /// 最后完成本地决定的记录 ID，断线重连不重置为末尾。
    cursor: String,
    /// 与 cursor 对应的逻辑消费位置，结合服务端信息检测裁剪缺口。
    consumed: u64,
    /// 已经有界接管原文的预取位置，不代表本地处理或隔离已经完成。
    fetched: String,
    /// 预取位置对应的累计读取数量，检测未接管区域的裁剪缺口。
    fetched_count: u64,
    /// 单次读取最多返回的记录数量。
    batch: usize,
    /// 单条原始记录及字段结构的字节容量上限。
    maximum: usize,
    /// 仅启用主动采样后建立的当前游标镜像，不与网络请求共持有锁。
    observation: Option<Arc<std::sync::Mutex<(String, u64)>>>,
}

#[derive(Clone)]
pub struct Observer {
    /// 与消费任务共用的受管 Redis 客户端。
    client: Arc<RedisClient>,
    /// 该观察器唯一获准读取的 Stream。
    stream: String,
    /// 当前已完成本地决定的游标及累计位置，不允许观察器更改。
    progress: Arc<std::sync::Mutex<(String, u64)>>,
}

impl Observer {
    /// 业务作用：对当前实时消费位置执行只读积压检查，不复制业务正文。
    /// 参数说明: 无。
    ///
    /// 返回：未读位置距离；状态缺口或资源不可用时不生成健康零值。
    pub async fn read(&self) -> anyhow::Result<crate::observability::broker::Reading> {
        let (cursor, consumed) = self
            .progress
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clone();
        let mut command = RedisCommand::new();
        command
            .arg("EVAL_RO")
            .arg(include_str!("realtime.lua"))
            .arg(1)
            .arg(&self.stream)
            .arg("observe")
            .arg(cursor)
            .arg(consumed);
        let unread: u64 = self.client.execute_raw(&command).await?;
        Ok(crate::observability::broker::Reading {
            dependency: None,
            unread_positions: unread,
            pending_records: None,
            pending_max_idle: None,
            partitions: None,
        })
    }
}

impl Realtime {
    /// 业务作用：在接流之前固定本次实时消费的确定末尾和逻辑位置，重连不重置游标。
    /// 参数说明：
    /// - `client`：受管 Redis 来源。
    /// - `stream`：固定业务 stream。
    /// - `batch`：每次读取的最大条数。
    /// - `maximum`：单条完整原始记录及字段结构的容量上限。
    ///
    /// 返回：已确认的初始基线；服务端不具备缺口检测信息时拒绝启动。
    pub async fn open(
        client: Arc<RedisClient>,
        stream: &str,
        batch: usize,
        maximum: usize,
    ) -> anyhow::Result<Self> {
        let mut reader = Self {
            client,
            stream: stream.into(),
            cursor: "0-0".into(),
            consumed: 0,
            fetched: "0-0".into(),
            fetched_count: 0,
            batch,
            maximum,
            observation: None,
        };
        let (cursor, consumed): (String, u64) = reader
            .client
            .execute_raw(&reader.command("initialize"))
            .await?;
        reader.fetched = cursor.clone();
        reader.fetched_count = consumed;
        reader.cursor = cursor;
        reader.consumed = consumed;
        Ok(reader)
    }

    /// 业务作用：在受管任务实际开始消费时建立当前末尾，启动准备期间的积压不进入在线投递。
    /// 参数说明: 无。
    ///
    /// 返回：游标和观测位置同时移到当前末尾；读取失败时保持依赖未就绪。
    pub async fn start_at_tail(&mut self) -> anyhow::Result<()> {
        let (cursor, consumed): (String, u64) =
            self.client.execute_raw(&self.command("initialize")).await?;
        self.fetched = cursor.clone();
        self.fetched_count = consumed;
        self.cursor = cursor;
        self.consumed = consumed;
        if let Some(progress) = &self.observation {
            *progress.lock().unwrap_or_else(|error| error.into_inner()) =
                (self.cursor.clone(), self.consumed);
        }
        Ok(())
    }

    /// 业务作用：在同一次服务端执行中验证缺口并读取记录，消除预检与裁剪之间的窗口。
    /// 参数说明: 无。
    ///
    /// 返回：未推进游标的原始记录；裁剪、删除、基线回退均拒绝消费。
    pub async fn read(&self) -> anyhow::Result<Vec<Record>> {
        let values: Vec<(String, Vec<Vec<u8>>)> =
            self.client.execute_raw(&self.command("read")).await?;
        values
            .into_iter()
            .map(|(id, values)| Record::from_parts(&self.stream, id, values))
            .collect()
    }

    /// 业务作用：仅在本地决定或必要隔离完成后前移实时游标，失败仍从原记录重试。
    /// 参数说明：
    /// - `id`：刚刚按读取顺序完成的记录 ID。
    ///
    /// 返回：本进程进度已前移；进程重启按 realtime 合同重新建立末尾基线。
    pub fn complete(&mut self, id: String) {
        if self.fetched == self.cursor {
            self.fetched = id.clone();
            self.fetched_count += 1;
        }
        self.cursor = id;
        self.consumed += 1;
        if let Some(progress) = &self.observation {
            *progress.lock().unwrap_or_else(|error| error.into_inner()) =
                (self.cursor.clone(), self.consumed);
        }
    }

    /// 业务作用：在原文与完成证据取得有界责任后推进预取位置，与已完成水位分离。
    /// 参数说明：
    /// - `id`：按输入顺序已经接管的记录 ID。
    ///
    /// 返回：下一次读取越过已拥有原文；完成水位只由连续完成项推进。
    pub fn admit(&mut self, id: &str) {
        self.fetched = id.to_owned();
        self.fetched_count += 1;
    }

    /// 业务作用：在业务反压期间检查实际来源与未接管区域的完整性，不额外读取正文。
    /// 参数说明: 无。
    ///
    /// 返回：来源仍可用且无读取缺口；拒绝时撤销来源就绪证据。
    pub async fn check(&self) -> anyhow::Result<()> {
        let _: u64 = self.client.execute_raw(&self.command("observe")).await?;
        Ok(())
    }

    /// 业务作用：仅在部署开启采样时提供独立只读游标镜像，关闭时不增加逐记录克隆。
    /// 参数说明: 无。
    ///
    /// 返回：共用有限进度镜像的观察器，不能推进真正消费位置。
    pub fn observer(&mut self) -> Observer {
        let progress = self.observation.get_or_insert_with(|| {
            Arc::new(std::sync::Mutex::new((self.cursor.clone(), self.consumed)))
        });
        Observer {
            client: self.client.clone(),
            stream: self.stream.clone(),
            progress: progress.clone(),
        }
    }

    /// 业务作用：使读取与初始化共用同一个固定资源和检查脚本。
    /// 参数说明：
    /// - `action`：内部固定初始化或读取动作。
    ///
    /// 返回：尚未执行的只读 Redis 命令。
    fn command(&self, action: &str) -> RedisCommand {
        let mut command = RedisCommand::new();
        command
            .arg("EVAL_RO")
            .arg(include_str!("realtime.lua"))
            .arg(1)
            .arg(&self.stream)
            .arg(action)
            .arg(&self.fetched)
            .arg(self.fetched_count)
            .arg(self.batch)
            .arg(self.maximum);
        command
    }
}
