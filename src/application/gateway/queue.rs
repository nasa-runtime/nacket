//! 单接收者队列只覆盖尚未开始写出的消息，已移交 writer 的批次保持原始顺序和责任。

use super::outbox::Batch;
use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
};
use tokio::sync::Notify;

pub(super) struct State {
    /// 尚未交给唯一 writer 的完整批次，数量与字节各自受限。
    pub items: VecDeque<Batch>,
    /// 队列永久关闭事实，后续发送不能重新打开。
    pub closed: bool,
    /// 仍存活的发送入口数量，最后发送者释放后唤醒接收端。
    senders: usize,
}

pub(super) struct Queue {
    /// 队列内容、关闭事实和发送者计数的同一临界区。
    pub state: Mutex<State>,
    /// 入队或关闭时唤醒唯一接收端的信号。
    pub changed: Notify,
    /// 最多等待写出的完整批次数量，不包含已交给 writer 的批次。
    pub capacity: usize,
}

impl Queue {
    /// 业务作用：建立数量有界的完整包队列，接收端只有一个 owner。
    /// 参数说明：
    /// - `capacity`：尚未移交 writer 的最大批次数。
    ///
    /// 返回：空队列，初始发送端引用数为一。
    pub fn new(capacity: usize) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(State {
                items: VecDeque::new(),
                closed: false,
                senders: 1,
            }),
            changed: Notify::new(),
            capacity,
        })
    }

    /// 业务作用：记录一个发送入口引用，接收端能准确识别最后的生产者退出。
    /// 参数说明: 无。
    ///
    /// 返回：引用数增加，不改变消息队列。
    pub fn retain(&self) {
        self.state.lock().unwrap_or_else(|e| e.into_inner()).senders += 1;
    }

    /// 业务作用：归还发送入口引用并唤醒等待者，防止生产者全部退出后遗留空等待。
    /// 参数说明: 无。
    ///
    /// 返回：引用数归零后接收端读尽已有批次即结束。
    pub fn release(&self) {
        self.state.lock().unwrap_or_else(|e| e.into_inner()).senders -= 1;
        self.changed.notify_one();
    }
}

/// 唯一 writer 的批次接收责任，释放时关闭队列并结束尚未写出的批次。
pub struct Receiver(
    /// 只交给一个实际写出路径的队列 owner，关闭时负责结束待写批次。
    pub(super) Arc<Queue>,
);
impl Receiver {
    /// 业务作用：按入队顺序移交完整批次，移交后该消息不再允许被新状态替换。
    /// 参数说明: 无。
    ///
    /// 返回：下一个完整包，或所有发送入口退出后的结束标志；取消等待不会丢弃消息。
    pub async fn recv(&mut self) -> Option<Batch> {
        loop {
            let changed = self.0.changed.notified();
            {
                let mut state = self.0.state.lock().unwrap_or_else(|e| e.into_inner());
                if let Some(batch) = state.items.pop_front() {
                    return Some(batch);
                }
                if state.closed || state.senders == 0 {
                    return None;
                }
            }
            changed.await;
        }
    }
}
impl Drop for Receiver {
    /// 业务作用：writer 退出时取消所有尚未写出的回执并归还逻辑字节预算。
    /// 参数说明: 无。
    ///
    /// 返回：队列永久关闭，后续发送入口不能继续保留业务正文。
    fn drop(&mut self) {
        let mut state = self.0.state.lock().unwrap_or_else(|e| e.into_inner());
        state.closed = true;
        state.items.clear();
    }
}
