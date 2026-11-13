use nasa::kafka::{GroupHealth, GroupState, PauseReason, Tp};
use std::time::{Duration, SystemTime};

/// 业务作用：持续核对数据入口的分配与协调存活，允许已有消费责任在有界背压期间排空。
/// 参数说明：
/// - `group`：受管消费者汇总的分配、状态和暂停证据。
/// - `expected`：此节点必须独占消费的全部业务分区。
/// - `now`：本次核对时刻，用于拒绝过期或倒退的 poll 证据。
/// - `maximum_age`：部署允许的最大协调证据年龄。
///
/// 返回：完整分配与新鲜 poll 均有效且仅有可自动推进的暂停时为 true；失权、硬暂停或停机均拒绝。
pub(crate) fn assignment_available(
    group: &GroupHealth,
    expected: &[Tp],
    now: SystemTime,
    maximum_age: Duration,
) -> bool {
    // 重试与有限输入、进度额度不会撤销 assignment；在这里关闭分发会阻断释放同一额度所需的处理。
    // 正在隔离一条无效记录仍持有原消费责任，不能让该记录借短暂 DLT 等待关闭其它合法会话。
    // 硬暂停、提交失败及失权不能借用残留 assignment 继续授权，因此按状态和暂停原因双重白名单核对。
    matches!(
        group.state,
        GroupState::Running | GroupState::Retrying | GroupState::ProgressBackpressure
    ) && group.ready_assignment_epoch.is_some()
        && !expected.is_empty()
        && expected
            .iter()
            .all(|partition| group.assignment.contains(partition))
        && group.paused.iter().all(|(_, reason)| {
            matches!(
                reason,
                PauseReason::RetryBackoff
                    | PauseReason::DltPending
                    | PauseReason::HandlerInFlight
                    | PauseReason::InputBackpressure
                    | PauseReason::ProgressBackpressure
            )
        })
        && group
            .last_poll_at
            .is_some_and(|at| now.duration_since(at).is_ok_and(|age| age <= maximum_age))
}
