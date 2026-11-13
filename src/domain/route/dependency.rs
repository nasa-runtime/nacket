use crate::domain::control::clock::LeaseTime;
use std::sync::{Arc, Mutex};

/// 依赖证据具有本地有限期限，后台任务停顿不能无限保留接流资格。
#[derive(Default)]
pub struct Dependency {
    /// 必要来源的有限有效证据及连续有效代次，读写复验共用同一事实域。
    state: Mutex<State>,
}

#[derive(Default)]
struct State {
    /// 最近受信来源操作建立的本地期限，响应迟到不能从接收时重新计时。
    evidence: Option<LeaseTime>,
    /// 连续有效区间的代次，来源失效和恢复后旧写出不能自动重新有效。
    generation: u64,
    /// 当前固定可用性原因码，不保存 broker 地址、凭据或自由错误文本。
    reason: &'static str,
}

/// 绑定依赖连续有效代次的写出证据，故障恢复不会使旧证据重新有效。
pub struct DependencyPermit {
    /// 取得本次证据的原依赖 owner，恢复后不能改查其它来源。
    owner: Arc<Dependency>,
    /// 取得许可时冻结的连续有效代次，每次推进写出均复验。
    generation: u64,
}

impl Dependency {
    /// 业务作用：只用已完成探测的固定期限恢复依赖资格，旧批次不能借恢复重新获得写权。
    /// 参数说明：
    /// - `evidence`：在实际依赖操作发起前建立的本地期限。
    ///
    /// 返回：仍有效的证据发布；已过期响应保持不可用。
    pub fn confirm(&self, evidence: LeaseTime) {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if state
            .evidence
            .is_none_or(|previous| previous.deadline().is_none())
        {
            state.generation = state.generation.saturating_add(1);
        }
        state.evidence = evidence.deadline().is_some().then_some(evidence);
        state.reason = if state.evidence.is_some() {
            "ready"
        } else {
            "dependency_stale"
        };
    }

    /// 业务作用：依赖故障先撤销准入，再执行退避或清理，已取得的旧批次也失去后续推进资格。
    /// 参数说明：
    /// - `reason`：产品固定原因码，不包含凭据或自由文本。
    ///
    /// 返回：依赖保持不可用，后续必须取得新证据才能恢复。
    pub fn deny(&self, reason: &'static str) {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if state.evidence.take().is_some() {
            state.generation = state.generation.saturating_add(1);
        }
        state.reason = reason;
    }

    /// 业务作用：为最终写出捕获依赖连续有效区间，不能跨故障和恢复使用同一批次。
    /// 参数说明: 无。
    ///
    /// 返回：证据仍有效时的区间许可；未知或到期返回 None。
    pub fn acquire(self: &Arc<Self>) -> Option<DependencyPermit> {
        let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        state.evidence?.deadline()?;
        Some(DependencyPermit {
            owner: self.clone(),
            generation: state.generation,
        })
    }

    /// 业务作用：输出固定依赖状态供 readiness 和指标使用，不把未知状态报告为健康。
    /// 参数说明: 无。
    ///
    /// 返回：ready、stale 或最近明确拒绝原因。
    pub fn status(&self) -> &'static str {
        let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        match state.evidence {
            Some(evidence) if evidence.deadline().is_some() => "ready",
            Some(_) => "dependency_stale",
            None if state.reason.is_empty() => "dependency_uninitialized",
            None => state.reason,
        }
    }
}

impl DependencyPermit {
    /// 业务作用：每次推进写出前同时核对证据期限和故障代次，恢复不会授权旧批次。
    /// 参数说明: 无。
    ///
    /// 返回：原连续有效区间仍成立时为 true。
    pub fn valid(&self) -> bool {
        let state = self
            .owner
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        state.generation == self.generation
            && state
                .evidence
                .is_some_and(|evidence| evidence.deadline().is_some())
    }
}
