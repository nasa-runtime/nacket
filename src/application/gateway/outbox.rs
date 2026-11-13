//! 逻辑字节预算覆盖整个排队和实际写出周期，业务体以共享 Bytes 保存。

use crate::domain::{
    message::wire::{Frame, FrameType},
    session::Session,
};
use bytes::Bytes;
use nasa::ws::transport::{Outbound, Writer};
use serde::Serialize;
use std::sync::Arc;
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore},
    time::Instant,
};

/// 同一物理 writer 不可交错的完整包，许可直到最终写出结束才释放。
pub struct Batch {
    /// 已经编码的原生帧头和控制区，正文保持单独共享存储。
    pub header: Bytes,
    /// 可选不可变业务正文，广播到多个接收者不重复反序列化。
    pub body: Option<Bytes>,
    /// 产品固定帧类别，供原生与 Socket.IO 写出适配使用。
    pub typ: FrameType,
    /// 固定控制区的编码字节，Socket.IO 映射时不解析业务正文。
    pub metadata: Bytes,
    /// 完整批次的原始单调写出截止点，排队和重试不延长。
    pub deadline: Instant,
    /// 最终写出前必须复验的会话、订阅及回执关联。
    pub delivery: Option<DeliveryCheck>,
    /// 当前 namespace 内 Socket.IO 操作 ACK 编号，与终端接收回执分开。
    pub ack_id: Option<u64>,
    /// 允许覆盖的完整状态键，仅能替换同键尚未开始写出的批次。
    pub(super) coalesce: Option<String>,
    /// 是否已记录终态，防止显式完成和 Drop 重复结算。
    finished: bool,
    /// 从排队到写出结束的计数、字节和时长观测责任。
    observed: Option<crate::observability::batch::Batch>,
    /// 保持操作 ACK 编号独占的 owner，完成写出前不可复用编号。
    _ack_lease: Option<Arc<()>>,
    /// 本会话逻辑字节预约，写出期间继续持有。
    _local: OwnedSemaphorePermit,
    /// 节点逻辑字节预约，最后实际批次 owner 释放后归还。
    _global: OwnedSemaphorePermit,
}

/// 下行候选只保存可复验关联，不能把排队资格当成最终写出权限。
#[derive(Clone)]
pub struct DeliveryCheck {
    /// 已验证主体的固定执行入口，最终许可与投递完成使用同一分区。
    pub uid_events: crate::application::uid_events::Lane,
    /// 本次投递唯一对应的真实会话，重连不继承旧候选。
    pub session: Arc<Session>,
    /// 订阅目标；无目标时按定向业务或控制回复规则检查。
    pub group: Option<String>,
    /// 排队时取得的本地订阅代次，取消再订阅不能复活旧包。
    pub generation: u64,
    /// 需要终端回执时的精确回执目录和投递 ID，写出失败需结束其责任。
    pub receipt: Option<(Arc<crate::domain::session::receipts::Receipts>, String)>,
    /// 是否要求业务定向接收权限，控制回复不借此取得业务接收资格。
    pub direct_receiving: bool,
}

impl DeliveryCheck {
    /// 业务作用：为当前连接控制回复建立同样的最终权限复验，不借 ACK 绕过路由下线。
    /// 参数说明：
    /// - `session`：响应所属的真实会话。
    /// - `uid_events`：与会话可信身份绑定的主体顺序域。
    ///
    /// 返回：无需群资格且不创建终端回执的写出检查。
    pub fn direct(session: Arc<Session>, uid_events: crate::application::uid_events::Lane) -> Self {
        Self {
            uid_events,
            session,
            group: None,
            generation: 0,
            receipt: None,
            direct_receiving: false,
        }
    }

    /// 业务作用：从最终写出点取得当前权限许可，区分业务定向消息与控制回复。
    /// 参数说明：
    /// - `deadline`：该完整包不可延长的投递期限。
    ///
    /// 返回：权限仍有效时登记写出责任，否则拒绝该包。
    pub async fn acquire(
        &self,
        deadline: Instant,
    ) -> anyhow::Result<Option<crate::domain::session::DeliveryPermit>> {
        let check = self.clone();
        self.uid_events
            .submit_control(deadline, 65536, move || {
                let permit = if check.direct_receiving {
                    check.session.acquire_direct(deadline)
                } else {
                    check
                        .session
                        .acquire(check.group.as_deref(), check.generation, deadline)
                };
                if permit.is_none() {
                    if let Some((receipts, token)) = &check.receipt {
                        receipts.cancel(Some(token), None);
                    }
                    return Ok(None);
                }
                if let Some((receipts, token)) = &check.receipt {
                    if !receipts.writing(token) {
                        return Ok(None);
                    }
                }
                Ok(permit)
            })?
            .wait()
            .await
    }

    /// 业务作用：把完整写出事实交回原主体消费域，网络失败只撤销票据，不授予任何新权限。
    /// 参数说明：
    /// - `written`：唯一物理 writer 是否已经完整发送该包。
    ///
    /// 返回：成功事实已消费；执行域失效撤销未确认票据并要求传输关闭。
    async fn finish(&self, written: bool) -> anyhow::Result<()> {
        if !written {
            if let Some((receipts, token)) = &self.receipt {
                receipts.cancel(Some(token), None);
            }
            return Ok(());
        }
        let business = self.direct_receiving || self.group.is_some();
        if !business && self.receipt.is_none() {
            return Ok(());
        }
        let receipt = self.receipt.clone();
        let session = self.session.clone();
        let pending = self.uid_events.submit_control(
            Instant::now() + std::time::Duration::from_secs(30),
            65536,
            move || {
                if let Some((receipts, token)) = receipt {
                    receipts.written(&token);
                }
                if business {
                    session.business_activity();
                }
                Ok(())
            },
        );
        let result = match pending {
            Ok(pending) => pending.wait().await,
            Err(error) => Err(error),
        };
        if result.is_err() {
            if let Some((receipts, token)) = &self.receipt {
                receipts.cancel(Some(token), None);
            }
        }
        result
    }
}

/// 会话发送入口，排队和实际写出共同占用连接及节点字节预算。
pub struct Outbox {
    /// 与唯一物理写出路径关联的有界完整批次队列。
    queue: Arc<super::queue::Queue>,
    /// 当前会话的逻辑输出字节池，覆盖排队和 writer 持有阶段。
    bytes: Arc<Semaphore>,
    /// 全部会话共用的节点业务输出字节池。
    global: Arc<Semaphore>,
    /// 协商后的完整帧字节上限，入队编码后核对。
    maximum_frame: usize,
    /// 固定控制区字节上限，不能用 metadata 携带无限业务体。
    maximum_metadata: usize,
    /// 可选统一指标源，克隆发送入口仍共用同一实例。
    metrics: Option<Arc<crate::observability::Metrics>>,
}

impl Outbox {
    /// 业务作用：建立连接独占的有界发送队列，共享节点逻辑字节预算。
    /// 参数说明：
    /// - `count`：最多等待写出的完整消息数量。
    /// - `bytes`：连接可保留的逻辑字节容量。
    /// - `global`：节点总出站预算。
    /// - `maximum_frame`：协商后的整帧上限。
    /// - `maximum_metadata`：固定控制区上限。
    ///
    /// 返回：可克隆发送入口和只交给一个 writer 的接收端。
    pub fn new(
        count: usize,
        bytes: usize,
        global: Arc<Semaphore>,
        maximum_frame: usize,
        maximum_metadata: usize,
    ) -> (Self, super::queue::Receiver) {
        let queue = super::queue::Queue::new(count);
        (
            Self {
                queue: queue.clone(),
                bytes: Arc::new(Semaphore::new(bytes)),
                global,
                maximum_frame,
                maximum_metadata,
                metrics: None,
            },
            super::queue::Receiver(queue),
        )
    }

    /// 业务作用：在队列首次使用前绑定节点指标源，后续克隆共用同一事实目录。
    /// 参数说明：
    /// - `metrics`：节点统一指标来源。
    ///
    /// 返回：以后入队的完整包携带自身观察责任。
    pub fn observe(&mut self, metrics: Arc<crate::observability::Metrics>) {
        self.metrics = Some(metrics);
    }

    /// 业务作用：先取得数量和字节容量再生成待写包，拒绝时不保留正文或半个包。
    /// 参数说明：
    /// - `typ`：服务端固定帧类别。
    /// - `metadata`：已经按方向投影的控制模型。
    /// - `body`：唯一业务编码和共享正文。
    /// - `deadline`：不可因重试延长的截止时间。
    /// - `delivery`：业务下行的最终写出复验信息，控制响应为 None。
    ///
    /// 返回：本地排队成功；不表示传输写出或终端接收。
    pub fn enqueue<M: Serialize>(
        &self,
        typ: FrameType,
        metadata: &M,
        body: Option<(u8, Bytes)>,
        deadline: Instant,
        delivery: Option<DeliveryCheck>,
    ) -> anyhow::Result<()> {
        self.enqueue_ack(typ, metadata, body, deadline, delivery, None)
    }

    /// 业务作用：把 Socket.IO 的单次 ACK 关联随完整包入队，避免两个 namespace 的编号混用。
    /// 参数说明：
    /// - `typ`：固定产品帧类别。
    /// - `metadata`：当前方向允许的控制模型。
    /// - `body`：可选唯一编码的业务字节。
    /// - `deadline`：完整包的最晚写出点。
    /// - `delivery`：可选下行权限复验材料。
    /// - `ack_id`：当前 namespace 的标准 Socket.IO ACK 编号。
    ///
    /// 返回：完整消息入队；容量、帧上限和截止点不因适配器改变。
    pub fn enqueue_ack<M: Serialize>(
        &self,
        typ: FrameType,
        metadata: &M,
        body: Option<(u8, Bytes)>,
        deadline: Instant,
        delivery: Option<DeliveryCheck>,
        ack_id: Option<u64>,
    ) -> anyhow::Result<()> {
        self.insert(typ, metadata, body, deadline, delivery, ack_id, None, None)
    }

    /// 业务作用：保持 Socket.IO ACK 编号的独占责任直到完整写出或明确取消。
    /// 参数说明：
    /// - `metadata`：当前会话的操作结果。
    /// - `body`：可选结果 schema 字节。
    /// - `deadline`：当前操作回复的固定截止点。
    /// - `delivery`：控制回复的最终路由与身份检查。
    /// - `ack_id`：namespace 内关联编号。
    /// - `lease`：本操作独占编号的生命周期标识。
    ///
    /// 返回：整个 ACK 批次已进入队列；编号在 writer 完成前不能重新使用。
    pub fn enqueue_tracked<M: Serialize>(
        &self,
        metadata: &M,
        body: Option<(u8, Bytes)>,
        deadline: Instant,
        delivery: DeliveryCheck,
        ack_id: u64,
        lease: Arc<()>,
    ) -> anyhow::Result<()> {
        self.insert(
            FrameType::EventAck,
            metadata,
            body,
            deadline,
            Some(delivery),
            Some(ack_id),
            None,
            Some(lease),
        )
    }

    /// 业务作用：将声明可覆盖的状态消息合并到同一接收目标的待写位置。
    /// 参数说明：
    /// - `metadata`：固定下行控制信息。
    /// - `body`：路由唯一编码和共享正文。
    /// - `deadline`：新状态本身的有效期限。
    /// - `delivery`：最终写出权限与回执。
    /// - `key`：冻结作用域内的事件及目标状态键。
    ///
    /// 返回：新状态入队或覆盖同键未写状态；已开始的写出不会被替换。
    pub fn enqueue_latest<M: Serialize>(
        &self,
        metadata: &M,
        body: (u8, Bytes),
        deadline: Instant,
        delivery: DeliveryCheck,
        key: String,
    ) -> anyhow::Result<()> {
        self.insert(
            FrameType::Broadcast,
            metadata,
            Some(body),
            deadline,
            Some(delivery),
            None,
            Some(key),
            None,
        )
    }

    /// 业务作用：在同一队列锁内预约数量和逻辑字节，替换时仅申请增长部分以避免双重占额。
    /// 参数说明：
    /// - `typ`：产品固定帧类别。
    /// - `metadata`：按当前方向投影的控制字段。
    /// - `body`：可选原始业务正文。
    /// - `deadline`：完整批次最晚写出点。
    /// - `delivery`：最终权限复验材料。
    /// - `ack_id`：标准 Socket.IO 操作关联。
    /// - `coalesce`：允许覆盖时的完整状态键。
    /// - `ack_lease`：操作编号的写出责任，没有编号时为 None。
    ///
    /// 返回：原子入队；预算不足保留原有消息及其回执。
    #[expect(
        clippy::too_many_arguments,
        reason = "排队项须同时持有原正文、写出许可、期限与来源。"
    )]
    fn insert<M: Serialize>(
        &self,
        typ: FrameType,
        metadata: &M,
        body: Option<(u8, Bytes)>,
        deadline: Instant,
        delivery: Option<DeliveryCheck>,
        ack_id: Option<u64>,
        coalesce: Option<String>,
        ack_lease: Option<Arc<()>>,
    ) -> anyhow::Result<()> {
        let mut observation = self.metrics.as_ref().map(|metrics| {
            let mut operation = crate::observability::Operation::new(
                metrics.clone(),
                crate::observability::Counter::Queue,
                "outbox",
                "",
            );
            operation.finish("rejected");
            operation
        });
        anyhow::ensure!(deadline > Instant::now(), "delivery_expired");
        let (header, body) = Frame::encode(typ, metadata, body)?;
        let size = header
            .len()
            .checked_add(body.as_ref().map_or(0, Bytes::len))
            .ok_or_else(|| anyhow::anyhow!("frame_size"))?;
        anyhow::ensure!(
            size <= self.maximum_frame && header.len() - 11 <= self.maximum_metadata,
            "frame_size"
        );
        let mut state = self.queue.state.lock().unwrap_or_else(|e| e.into_inner());
        anyhow::ensure!(!state.closed, "outbox_closed");
        let replacing = coalesce.as_ref().and_then(|key| {
            state
                .items
                .iter()
                .position(|batch| batch.coalesce.as_ref() == Some(key))
        });
        anyhow::ensure!(
            replacing.is_some() || state.items.len() < self.queue.capacity,
            "outbox_full"
        );
        let previous_size = replacing.map_or(0, |index| state.items[index]._local.num_permits());
        let growth = size.saturating_sub(previous_size);
        let mut local = self
            .bytes
            .clone()
            .try_acquire_many_owned(u32::try_from(growth)?)
            .map_err(|_| anyhow::anyhow!("outbox_bytes"))?;
        let mut global = self
            .global
            .clone()
            .try_acquire_many_owned(u32::try_from(growth)?)
            .map_err(|_| anyhow::anyhow!("node_outbox_bytes"))?;
        let metadata = header.slice(11..);
        if let Some(index) = replacing {
            let old = &mut state.items[index];
            let retain = size.min(previous_size);
            local.merge(old._local.split(retain).expect("local reservation"));
            global.merge(old._global.split(retain).expect("global reservation"));
        }
        let batch = Batch {
            header,
            body,
            typ,
            metadata,
            deadline,
            delivery,
            ack_id,
            coalesce,
            finished: false,
            observed: self.metrics.as_ref().map(|metrics| {
                crate::observability::batch::Batch::new(
                    metrics.clone(),
                    size,
                    typ == FrameType::Broadcast,
                )
            }),
            _ack_lease: ack_lease,
            _local: local,
            _global: global,
        };
        if let Some(index) = replacing {
            state.items[index] = batch;
        } else {
            state.items.push_back(batch);
        }
        drop(state);
        self.queue.changed.notify_one();
        if let Some(observation) = observation.as_mut() {
            observation.finish("accepted");
        }
        Ok(())
    }
}

impl Batch {
    /// 业务作用：以标准 Socket.IO 包头和单附件原子写出，共用原生路径的最终权限门禁。
    /// 参数说明：
    /// - `writer`：整个 Engine.IO 物理连接唯一 writer。
    /// - `namespace`：该批次所属的真实逻辑会话。
    ///
    /// 返回：头和附件均写完，或在取得权限前丢弃；中途错误要求关闭整个 transport。
    pub async fn write_socketio(
        mut self,
        writer: &mut Writer,
        namespace: &str,
    ) -> anyhow::Result<()> {
        use nasa::ws::socketio::packet::{Kind, Packet};
        if let Some(observed) = self.observed.as_mut() {
            observed.writing();
        }
        let permit = if let Some(check) = &self.delivery {
            let Some(permit) = check.acquire(self.deadline).await? else {
                if let Some((session, token)) = &check.receipt {
                    session.cancel(Some(token), None);
                }
                return Ok(());
            };
            Some(permit)
        } else {
            None
        };
        let deadline = permit.as_ref().map_or(self.deadline, |p| p.deadline);
        let mut envelope: serde_json::Value = serde_json::from_slice(&self.metadata)?;
        if self.body.is_some() {
            envelope
                .as_object_mut()
                .ok_or_else(|| anyhow::anyhow!("invalid_envelope"))?
                .insert(
                    "message".into(),
                    serde_json::json!({
                        "_placeholder": true,
                        "num": 0
                    }),
                );
        }
        let ack = self.typ == FrameType::EventAck;
        anyhow::ensure!(!ack || self.ack_id.is_some(), "ack_id_required");
        let data = if ack {
            serde_json::json!([envelope])
        } else {
            let event = match self.typ {
                FrameType::Broadcast => "nacket:push",
                FrameType::State => "nacket:state",
                FrameType::Ready => "nacket:ready",
                _ => anyhow::bail!("unsupported_socketio_output"),
            };
            serde_json::json!([event, envelope])
        };
        let json = serde_json::to_string(&data)?;
        let kind = match (ack, self.body.is_some()) {
            (true, true) => Kind::BinaryAck,
            (true, false) => Kind::Ack,
            (false, true) => Kind::BinaryEvent,
            (false, false) => Kind::Event,
        };
        let header = Packet {
            kind,
            namespace,
            ack_id: self.ack_id,
            attachments: usize::from(self.body.is_some()),
            json: &json,
        }
        .encode()
        .ok_or_else(|| anyhow::anyhow!("socketio_packet"))?;
        let result = guard_write(permit.as_ref(), async {
            writer
                .send(Outbound::Text(format!("4{header}")), deadline)
                .await?;
            if let Some(body) = &self.body {
                writer
                    .send(
                        Outbound::Binary {
                            header: Bytes::new(),
                            body: Some(body.clone()),
                        },
                        deadline,
                    )
                    .await?;
            }
            Ok::<(), anyhow::Error>(())
        })
        .await;
        if let Some(check) = &self.delivery {
            check.finish(result.is_ok()).await?;
        }
        self.finished = result.is_ok();
        if let Some(observed) = self.observed.as_mut() {
            observed.finish(
                result.is_ok(),
                header.len() + 1 + self.body.as_ref().map_or(0, Bytes::len),
                1 + usize::from(self.body.is_some()),
            );
        }
        result
    }

    /// 业务作用：在实际原生写出前复验路由、身份和群意图，许可覆盖完整传输操作。
    /// 参数说明：
    /// - `writer`：当前物理连接唯一写入 owner。
    ///
    /// 返回：写出完成或合法丢弃；传输错误要求关闭连接，不重发半个包。
    pub async fn write_native(mut self, writer: &mut Writer) -> anyhow::Result<()> {
        if let Some(observed) = self.observed.as_mut() {
            observed.writing();
        }
        let permit = if let Some(check) = &self.delivery {
            let Some(permit) = check.acquire(self.deadline).await? else {
                if let Some((session, token)) = &check.receipt {
                    session.cancel(Some(token), None);
                }
                return Ok(());
            };
            Some(permit)
        } else {
            None
        };
        let deadline = permit.as_ref().map_or(self.deadline, |p| p.deadline);
        let result = guard_write(
            permit.as_ref(),
            writer.send(
                Outbound::Binary {
                    header: self.header.clone(),
                    body: self.body.clone(),
                },
                deadline,
            ),
        )
        .await;
        if let Some(check) = &self.delivery {
            check.finish(result.is_ok()).await?;
        }
        if let Some(observed) = self.observed.as_mut() {
            observed.finish(
                result.is_ok(),
                self.header.len() + self.body.as_ref().map_or(0, Bytes::len),
                1,
            );
        }
        result?;
        self.finished = true;
        Ok(())
    }
}

/// 业务作用：每次推进传输 future 前核对已登记批次的原许可，失效时丢弃该 future 并要求关闭传输。
/// 参数说明：
/// - `permit`：业务批次的原始权限责任，纯物理控制包没有业务许可。
/// - `write`：唯一 writer 持有的完整帧或头与附件写出过程。
///
/// 返回：完整写出结果；期限已过时停止后续 socket 写入，不复用可能只写了一部分的连接。
pub(crate) async fn guard_write<F, E>(
    permit: Option<&crate::domain::session::DeliveryPermit>,
    write: F,
) -> anyhow::Result<()>
where
    F: std::future::Future<Output = Result<(), E>>,
    E: Into<anyhow::Error>,
{
    let mut write = std::pin::pin!(write);
    std::future::poll_fn(|cx| {
        if permit.is_some_and(|permit| !permit.valid()) {
            return std::task::Poll::Ready(Err(anyhow::anyhow!("write_permit_expired")));
        }
        write
            .as_mut()
            .poll(cx)
            .map(|result| result.map_err(Into::into))
    })
    .await
}

impl Drop for Batch {
    /// 业务作用：未完成完整写出的批次被替换、取消或丢弃时撤销对应回执票据。
    /// 参数说明: 无。
    ///
    /// 返回：不会遗留永远无法到达终端的待确认回执，已完成写出的票据继续等待终端。
    fn drop(&mut self) {
        if !self.finished {
            if let Some((owner, token)) = self
                .delivery
                .as_ref()
                .and_then(|check| check.receipt.as_ref())
            {
                owner.cancel(Some(token), None);
            }
        }
    }
}

impl Clone for Outbox {
    /// 业务作用：复制发送入口并保留其队列生命周期，不复制业务正文。
    /// 参数说明: 无。
    ///
    /// 返回：共享同一数量和字节预算的发送入口。
    fn clone(&self) -> Self {
        self.queue.retain();
        Self {
            queue: self.queue.clone(),
            bytes: self.bytes.clone(),
            global: self.global.clone(),
            maximum_frame: self.maximum_frame,
            maximum_metadata: self.maximum_metadata,
            metrics: self.metrics.clone(),
        }
    }
}
impl Drop for Outbox {
    /// 业务作用：最后一个发送入口退出后允许唯一接收端结束。
    /// 参数说明: 无。
    ///
    /// 返回：归还入口引用，不提前丢弃仍等待写出的完整消息。
    fn drop(&mut self) {
        self.queue.release();
    }
}
