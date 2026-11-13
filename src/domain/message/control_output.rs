//! 控制消息使用独立节点预算，权限通知不与业务正文争用队列容量。

use crate::observability::{Counter, Metrics, Reservation};
use nasa::ws::transport::{Outbound, Writer};
use std::sync::Arc;
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore},
    time::Instant,
};

/// 控制输出的节点保留池，独立于业务队列容量但仍设字节硬上限。
pub struct Budget {
    /// 节点共享的控制写出字节容量，独立于业务正文预算。
    capacity: Arc<Semaphore>,
    /// 控制责任的节点观察来源，随真实 owner 释放资源计数。
    metrics: Arc<Metrics>,
}

/// 从控制对象构造到实际写出结束的资源责任，取消时也归还份额。
pub struct Lease {
    /// 实际占用的控制字节份额，移交 writer 后仍保留直到写出或取消结束。
    _capacity: OwnedSemaphorePermit,
    /// 本份额对应的保守逻辑字节观察，包含状态对象及编码临时空间。
    _bytes: Reservation,
    /// 一条控制输出责任的观察，不表示终端已经接收。
    _item: Reservation,
    /// 完整控制帧写出成功或失败时使用的同一节点指标源。
    metrics: Arc<Metrics>,
}

/// 携带控制预算的完整写出包，所有权必须随真实 writer 一起移交。
pub struct Packet {
    /// 交给唯一物理 writer 的固定协议控制包，不能携带任意业务正文。
    output: Outbound,
    /// 与整个写出过程绑定的预算和观察责任，不能在入队后提前释放。
    lease: Lease,
}

impl Budget {
    /// 业务作用：建立节点共享控制写出预算，业务队列拥塞不会耗尽该保留池。
    /// 参数说明：
    /// - `maximum`：经过配置验证的有限字节数。
    /// - `metrics`：随实际责任释放的节点指标。
    ///
    /// 返回：可跨会话共享的控制预算。
    pub fn new(maximum: usize, metrics: Arc<Metrics>) -> Arc<Self> {
        Arc::new(Self {
            capacity: Arc::new(Semaphore::new(maximum)),
            metrics,
        })
    }

    /// 业务作用：为待保存的控制对象及其编码临时空间预约容量，不等待业务资源释放。
    /// 参数说明：
    /// - `bytes`：包含临时编码空间的保守保留量。
    ///
    /// 返回：独占责任；容量不足立即拒绝，由调用方关闭无法保持状态顺序的会话。
    pub fn reserve(&self, bytes: usize) -> anyhow::Result<Lease> {
        let capacity = self
            .capacity
            .clone()
            .try_acquire_many_owned(u32::try_from(bytes)?)
            .map_err(|_| anyhow::anyhow!("control_byte_capacity"))?;
        Ok(Lease {
            _capacity: capacity,
            _bytes: self.metrics.reserve("control_output_bytes", bytes as u64),
            _item: self.metrics.reserve("control_output_items", 1),
            metrics: self.metrics.clone(),
        })
    }

    /// 业务作用：把固定协议控制帧与预算绑定，直到实际写出结束才归还容量。
    /// 参数说明：
    /// - `output`：已经受协议大小限制的握手、心跳或状态包。
    ///
    /// 返回：可移交唯一 writer 的包；正文不能借此绕过业务门禁。
    pub fn packet(&self, output: Outbound) -> anyhow::Result<Packet> {
        let bytes = match &output {
            Outbound::Text(value) => value.capacity(),
            Outbound::Binary {
                header,
                body,
            } => {
                anyhow::ensure!(body.is_none(), "control_body_forbidden");
                header.len()
            }
            _ => 0,
        };
        let lease = self.reserve(bytes.saturating_mul(2).saturating_add(256))?;
        Ok(Packet {
            output,
            lease,
        })
    }
}

impl Lease {
    /// 业务作用：在已预约空间内完成状态包编码后转移责任，避免重复预约造成互相等待。
    /// 参数说明：
    /// - `output`：由有限状态模型生成的控制包。
    ///
    /// 返回：与原预约共享生命周期的完整写出责任。
    pub fn packet(self, output: Outbound) -> Packet {
        Packet {
            output,
            lease: self,
        }
    }
}

impl Packet {
    /// 业务作用：串行写出完整控制消息并统计真实完成量，不把排队视为终端已经接收。
    /// 参数说明：
    /// - `writer`：物理连接唯一写入 owner。
    /// - `deadline`：本次控制的固定截止点。
    ///
    /// 返回：完整写出成功；失败释放预算且不记完整发送量。
    pub async fn write(self, writer: &mut Writer, deadline: Instant) -> anyhow::Result<()> {
        let size = match &self.output {
            Outbound::Text(value) => Some(value.len()),
            Outbound::Binary {
                header,
                body,
            } => Some(header.len() + body.as_ref().map_or(0, |v| v.len())),
            _ => None,
        };
        if let Err(error) = writer.send(self.output, deadline).await {
            self.lease
                .metrics
                .add(Counter::TransportFrames, "transmit", "unavailable", 1);
            return Err(error.into());
        }
        if let Some(size) = size {
            self.lease
                .metrics
                .add(Counter::TransportBytes, "transmit", "ok", size as u64);
            self.lease
                .metrics
                .add(Counter::TransportFrames, "transmit", "ok", 1);
        }
        Ok(())
    }
}
