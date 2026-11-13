use nasa::application::{ApplicationState, DependencyState, ReadinessSnapshot};
use std::sync::Arc;

#[derive(PartialEq, Eq)]
struct Blocker {
    /// 启动期冻结的组件身份，只用于关联其就绪证据。
    component: Arc<str>,
    /// 注册表中的稳定贡献项名称，不包含连接凭据。
    dependency: Arc<str>,
    /// 已由注册表复验新鲜度和有效期限的拒绝状态。
    state: DependencyState,
    /// 贡献项提供的固定原因码，不携带远端错误正文。
    reason: &'static str,
}

#[derive(Default)]
pub(super) struct Observation {
    /// 上次实际观察的生命周期、聚合结果与阻断项，稳定状态不重复输出。
    previous: Option<(ApplicationState, bool, Vec<Blocker>)>,
}

impl Observation {
    /// 业务作用：记录本地接流条件的变化及关键依赖原因，让失效与恢复可被定位。
    /// 参数说明：
    /// - `state`：宿主当前生命周期，只有 Ready 允许接流。
    /// - `snapshot`：本次读取时已经裁决过期的就绪证据，不触发远端探测。
    ///
    /// 返回：生命周期与依赖聚合共同允许接流时为 true；仅变化时输出日志。
    /// 各贡献项和生命周期分别读取，记录不构成后续接流权限，也不改变任何门禁。
    pub(super) fn observe(
        &mut self,
        state: ApplicationState,
        snapshot: &ReadinessSnapshot,
    ) -> bool {
        let ready = state == ApplicationState::Ready && snapshot.ready;
        let blockers = snapshot
            .entries
            .iter()
            .filter(|entry| {
                entry.affects_ready
                    && matches!(
                        entry.state,
                        DependencyState::Unknown | DependencyState::NotReady
                    )
            })
            .map(|entry| Blocker {
                component: Arc::clone(&entry.component),
                dependency: Arc::clone(&entry.name),
                state: entry.state,
                reason: entry.reason,
            })
            .collect();
        let current = (state, ready, blockers);
        if self.previous.as_ref() != Some(&current) {
            if ready {
                tracing::info!(
                    event = "readiness_transition",
                    ready,
                    application_state = ?state,
                    "本地就绪条件满足"
                );
            } else {
                // 只报告已生效的关键拒绝项；非关键降级不能被描述成关闭接流的原因。
                tracing::warn!(
                    event = "readiness_transition",
                    ready,
                    application_state = ?state,
                    "本地就绪条件不满足"
                );
                for blocker in &current.2 {
                    tracing::warn!(
                        event = "readiness_dependency_unavailable",
                        component = ?blocker.component,
                        dependency = ?blocker.dependency,
                        state = ?blocker.state,
                        reason = blocker.reason,
                        "关键依赖证据不可用"
                    );
                }
            }
            self.previous = Some(current);
        }
        ready
    }
}
