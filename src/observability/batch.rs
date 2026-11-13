use super::{Counter, Metrics, Reservation};
use std::{sync::Arc, time::Instant};

/// 完整出站包的指标责任，覆盖命名空间队列、物理 writer 排队及实际写出。
pub struct Batch {
    /// 完整包从排队到退出共用的节点指标来源。
    metrics: Arc<Metrics>,
    /// 一份完整包责任，在 writer 接管时从 outbox_items 转为 writing_items。
    items: Reservation,
    /// 本接收者保留的逻辑字节责任，即使正文共享也独立计费。
    bytes: Reservation,
    /// 首次成功入队的单调时刻，用于统计取得 writer 前的等待。
    queued: Instant,
    /// 唯一 writer 首次接管的单调时刻；排队阶段为 None。
    writing: Option<Instant>,
    /// 是否属于业务推送，控制回复不增加业务投递阶段计数。
    business: bool,
    /// 是否已经记录完整写出结果，避免释放时重复计为丢弃。
    completed: bool,
}

impl Batch {
    /// 业务作用：在完整包真正入队时登记逻辑保留量，共享正文仍按接收者计费。
    /// 参数说明：
    /// - `metrics`：节点事实源。
    /// - `bytes`：该完整包的逻辑字节数。
    /// - `business`：是否为服务端业务推送。
    ///
    /// 返回：随完整包移动的责任，控制回复不计业务消息投递。
    pub fn new(metrics: Arc<Metrics>, bytes: usize, business: bool) -> Self {
        if business {
            metrics.add(Counter::Deliveries, "queued", "accepted", 1);
        }
        Self {
            items: metrics.reserve("outbox_items", 1),
            bytes: metrics.reserve("outbox_bytes", bytes as u64),
            metrics,
            queued: Instant::now(),
            writing: None,
            business,
            completed: false,
        }
    }

    /// 业务作用：唯一物理 writer 取得批次后转移观察量，等待其它队列不冒充实际写出。
    /// 参数说明: 无。
    ///
    /// 返回：排队耗时已记录，资源进入 writing 类别。
    pub fn writing(&mut self) {
        if self.writing.is_none() {
            self.metrics.duration("queue_wait", self.queued.elapsed());
            self.items.transfer("writing_items");
            self.bytes.transfer("writing_bytes");
            self.writing = Some(Instant::now());
        }
    }

    /// 业务作用：完整传输结束后记录实际结局，不将半包写入计为完整消息。
    /// 参数说明：
    /// - `success`：头与所有附件是否均写入成功。
    /// - `bytes`：本适配器完整消息字节，包含自身协议头。
    /// - `frames`：实际完整消息数量，二进制附件单独计一条。
    ///
    /// 返回：最多统计一次完整写出；失败仍归还观察资源。
    pub fn finish(&mut self, success: bool, bytes: usize, frames: usize) {
        if self.completed {
            return;
        }
        self.completed = true;
        if self.business {
            self.metrics.add(
                Counter::Deliveries,
                "written",
                if success {
                    "ok"
                } else {
                    "unavailable"
                },
                1,
            );
        }
        if let Some(started) = self.writing {
            self.metrics.duration("write", started.elapsed());
        }
        if success {
            self.metrics
                .add(Counter::TransportBytes, "transmit", "ok", bytes as u64);
            self.metrics
                .add(Counter::TransportFrames, "transmit", "ok", frames as u64);
        } else {
            self.metrics
                .add(Counter::TransportFrames, "transmit", "unavailable", 1);
        }
    }
}

impl Drop for Batch {
    /// 业务作用：覆盖队列替换、权限撤销、取消及 writer 异常等未完成出口。
    /// 参数说明: 无。
    ///
    /// 返回：未完成批次计 dropped，完整写出过的批次不会重复统计。
    fn drop(&mut self) {
        if !self.completed {
            self.metrics.add(Counter::Queue, "outbox", "dropped", 1);
            if self.business {
                self.metrics
                    .add(Counter::Deliveries, "written", "dropped", 1);
            }
        }
    }
}
