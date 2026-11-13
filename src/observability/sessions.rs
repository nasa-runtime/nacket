//! 分批观察本节点业务连接与订阅目标；不从指标结果推断授权或集群全量。

use crate::{application::gateway::Gateway, config::session_metrics::SessionMetrics};
use std::{collections::BTreeSet, ops::Bound, sync::Arc, time::Duration};
use tokio::time::Instant;

/// 只保留一轮采样的有界目录，抓取线程不扫描会话。
pub struct Sampler {
    /// 启动时冻结的成本和窗口约束。
    policy: SessionMetrics,
    /// 已扫描的最后一个会话目录键，下一批从严格后继继续。
    cursor: Option<String>,
    /// 当前扫描起点，控制观察跨度。
    started: Instant,
    /// 本轮观察到的有效且最近有业务活动的会话数。
    active: u64,
    /// 本轮观察到的有效但活动窗口内无业务动作的会话数。
    idle: u64,
    /// 不对外输出目标标签，仅在内存中按路由和租户区分相同目标 ID。
    targets: BTreeSet<(String, String, String)>,
    /// 目录保守保留开销，包含字符串和树节点。
    target_bytes: usize,
    /// 目录预算不足时整轮目标数不可用，不能把部分计数当作总量。
    targets_complete: bool,
}

impl Sampler {
    /// 业务作用：建立空采样游标，不复制会话目录。
    /// 参数说明：
    /// - `policy`：已验证的固定采样预算。
    ///
    /// 返回：从目录起点开始的一轮观察。
    pub fn new(policy: SessionMetrics) -> Self {
        Self {
            policy,
            cursor: None,
            started: Instant::now(),
            active: 0,
            idle: 0,
            targets: BTreeSet::new(),
            target_bytes: 0,
            targets_complete: true,
        }
    }

    /// 业务作用：在有界集合内记录本节点仍有效的订阅目标。
    /// 参数说明：
    /// - `route`：冻结路由 ID。
    /// - `tenant`：可信租户身份。
    /// - `target`：授权尚未过期的订阅目标。
    ///
    /// 返回：重复目标不增加计数；容量不足停止保留并标记本轮目标观察不可用。
    fn target(&mut self, route: &str, tenant: &str, target: &str) {
        if !self.targets_complete {
            return;
        }
        let key = (route.to_owned(), tenant.to_owned(), target.to_owned());
        if self.targets.contains(&key) {
            return;
        }
        let bytes = route.len() + tenant.len() + target.len() + 256;
        if self.targets.len() >= self.policy.max_targets
            || self.target_bytes.saturating_add(bytes) > self.policy.max_target_bytes
        {
            self.targets_complete = false;
            self.targets.clear();
            self.target_bytes = 0;
            return;
        }
        self.target_bytes += bytes;
        self.targets.insert(key);
    }

    /// 业务作用：消费固定数量的会话观察，只有完整目录扫描才能发布本轮总量。
    /// 参数说明：
    /// - `gateway`：当前节点会话目录和固定指标源。
    ///
    /// 返回：发布完整观察或不可用状态；并发建连与断连可能落在相邻轮次，不构成原子快照。
    pub fn step(&mut self, gateway: &Gateway) {
        let batch = {
            let sessions = gateway
                .sessions
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            let range = (
                self.cursor
                    .as_ref()
                    .map_or(Bound::Unbounded, Bound::Excluded),
                Bound::Unbounded,
            );
            sessions
                .range::<String, _>(range)
                .take(self.policy.batch_size)
                .map(|(key, session)| (key.clone(), session.clone()))
                .collect::<Vec<_>>()
        };
        let complete = batch.len() < self.policy.batch_size;
        let window = Duration::from_millis(self.policy.activity_window_ms);
        for (key, connection) in batch {
            self.cursor = Some(key);
            if !connection
                .wire_ready
                .load(std::sync::atomic::Ordering::Acquire)
            {
                continue;
            }
            match connection
                .session
                .observe_activity(window, |route, tenant, target| {
                    self.target(route, tenant, target)
                }) {
                Some(true) => self.active += 1,
                Some(false) => self.idle += 1,
                None => {}
            }
        }
        let elapsed = self.started.elapsed();
        let fresh = elapsed <= Duration::from_millis(self.policy.max_cycle_ms);
        if complete || !fresh {
            gateway
                .metrics
                .set("session_sample_available", u64::from(complete && fresh));
            gateway.metrics.set(
                "subscription_target_sample_available",
                u64::from(complete && fresh && self.targets_complete),
            );
            gateway
                .metrics
                .set("session_sample_span_seconds", elapsed.as_millis() as u64);
            if complete && fresh {
                gateway.metrics.set("sessions_active", self.active);
                gateway.metrics.set("sessions_idle", self.idle);
                if self.targets_complete {
                    gateway
                        .metrics
                        .set("subscription_targets", self.targets.len() as u64);
                }
                gateway.metrics.set(
                    "session_sample_timestamp_seconds",
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis() as u64,
                );
            }
            *self = Self::new(self.policy.clone());
        }
    }
}

/// 业务作用：把有界采样交给统一应用生命周期，指标抓取只读取已发布值。
/// 参数说明：
/// - `application`：任务监督与关闭入口。
/// - `gateway`：当前节点的共享运行事实。
///
/// 返回：开启时安装固定周期任务；关闭开关不扫描目录且可用性指标保持零。
pub async fn install(application: &nasa::Application, gateway: Arc<Gateway>) -> anyhow::Result<()> {
    let policy = gateway.configuration.session_metrics.clone();
    if !policy.enabled {
        return Ok(());
    }
    application
        .serve_when_ready("socket-session-metrics", move |shutdown| async move {
            let mut timer = tokio::time::interval(Duration::from_millis(policy.interval_ms));
            timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            let mut sampler = Sampler::new(policy);
            loop {
                tokio::select! {
                    biased;
                    _ = shutdown.cancelled() => {
                        gateway.metrics.set("session_sample_available", 0);
                        gateway.metrics.set("subscription_target_sample_available", 0);
                        return Ok::<(), anyhow::Error>(());
                    }
                    _ = timer.tick() => sampler.step(&gateway),
                }
            }
        })
        .await?;
    Ok(())
}
