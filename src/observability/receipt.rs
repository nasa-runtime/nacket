use super::{Counter, Metrics, Reservation};
use std::{sync::Arc, time::Instant};

/// 单个真实回执票据的观察责任，终态不代表用户已经阅读消息。
pub struct Receipt {
    /// 票据建立与终止共用的节点指标来源。
    metrics: Arc<Metrics>,
    /// 一张实际待确认票据的资源责任，随票据 owner 释放。
    _pending: Reservation,
    /// 协商后的固定确认层级 received 或 processed，不代表用户阅读。
    level: &'static str,
    /// 实际开始写出时的单调起点；尚未取得写出资格时不计终端等待时间。
    started: Option<Instant>,
    /// 唯一终态标记，确保确认、过期和取消至多记录一次。
    finished: bool,
}

impl Receipt {
    /// 业务作用：在真实票据建立后登记待确认责任。
    /// 参数说明：
    /// - `metrics`：节点统一事实源。
    /// - `level`：服务器协商的 received 或 processed 层级。
    ///
    /// 返回：直到确认、到期或取消才释放的观察责任。
    pub fn new(metrics: Arc<Metrics>, level: &'static str) -> Self {
        Self {
            _pending: metrics.reserve("receipts", 1),
            metrics,
            level,
            started: None,
            finished: false,
        }
    }

    /// 业务作用：取得最终写出许可时启动回执等待计时，排队耗时不混入终端确认时间。
    /// 参数说明: 无。
    ///
    /// 返回：只保存首次写出的本地起点。
    pub fn writing(&mut self) {
        self.started.get_or_insert_with(Instant::now);
    }

    /// 业务作用：记录本票据唯一终态，拒绝、过期与成功分别统计。
    /// 参数说明：
    /// - `outcome`：固定票据结局。
    ///
    /// 返回：仅首次完成计数，确认成功额外计入对应终端投递阶段。
    pub fn finish(&mut self, outcome: &'static str) {
        if self.finished {
            return;
        }
        self.finished = true;
        self._pending.finish_pending();
        self.metrics.add(Counter::Receipts, self.level, outcome, 1);
        if let Some(started) = self.started {
            self.metrics.duration("receipt", started.elapsed());
        }
        if outcome == "ok" {
            self.metrics.add(Counter::Deliveries, self.level, "ok", 1);
        }
    }
}

impl Drop for Receipt {
    /// 业务作用：未明确完成的票据随 owner 退出时归类为取消，不能保留永久等待数量。
    /// 参数说明: 无。
    ///
    /// 返回：只记录尚未完成的取消终态，待决 gauge 自动归还。
    fn drop(&mut self) {
        self.finish("canceled");
    }
}
