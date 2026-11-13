use super::{Session, State};
use tokio::time::Instant;

/// 首次关闭时冻结的本地事实；清理和迟到通知不能覆盖原始数量与原因。
#[derive(Clone, Copy)]
pub struct Closure {
    /// 会话首次失去全部权限的单调时刻，不采用分区队列完成时间。
    pub closed_at: Instant,
    /// 服务端已确认的有限关闭原因，不携带远程响应或凭据。
    pub reason: &'static str,
    /// 关闭时仍有订阅意图的数量，包含授权尚未完成的订阅。
    pub cancelled_subscriptions: usize,
    /// 被取消意图中尚未取得授权的数量，不能解释为已生效订阅。
    pub pending_subscriptions: usize,
}

impl State {
    /// 业务作用：在同一临界区冻结关闭事实并撤销全部订阅，阻止迟到授权重新生效。
    /// 参数说明：
    /// - `reason`：调用方从实际失权条件选择的固定原因。
    ///
    /// 返回：会话永久关闭，全部订阅索引释放；重复调用保留首次事实。
    pub(super) fn close(&mut self, reason: &'static str) {
        if self.closure.is_none() {
            self.closure = Some(Closure {
                closed_at: Instant::now(),
                reason,
                cancelled_subscriptions: self
                    .subscriptions
                    .values()
                    .filter(|entry| entry.desired)
                    .count(),
                pending_subscriptions: self
                    .subscriptions
                    .values()
                    .filter(|entry| entry.desired && entry.grant.is_none())
                    .count(),
            });
        }
        // 撤销资格和释放索引必须一起完成；观测开关不能跳过任何权限清理。
        self.open = false;
        self.ready = false;
        self.direct = None;
        self.subscriptions.clear();
        self.next_subscription_expiry = None;
    }
}

impl Session {
    /// 业务作用：读取首次关闭的本地事实，避免重复关闭将取消订阅数量覆盖为零。
    /// 参数说明: 无。
    ///
    /// 返回：关闭后返回固定快照；仍开放时返回 None。
    pub fn closure(&self) -> Option<Closure> {
        self.state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .closure
    }

    /// 业务作用：按单调时钟报告当前会话存续时间，关闭后停止累计清理队列等待。
    /// 参数说明: 无。
    ///
    /// 返回：从身份会话创建到首次关闭或当前时刻的毫秒数，包含确认和初始化等待。
    pub fn duration_ms(&self) -> u64 {
        let end = self
            .closure()
            .map_or_else(Instant::now, |closed| closed.closed_at);
        end.saturating_duration_since(self.created_at)
            .as_millis()
            .min(u64::MAX as u128) as u64
    }
}
