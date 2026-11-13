use crate::domain::{
    route::policies::{Connections, SameUserPolicy},
    session::Session,
};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};
use tokio::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;

struct Entry {
    /// 节点名额对应的真实会话，旧 writer 未归还仍占用替换屏障。
    session: Arc<Session>,
    /// 替换旧连接时撤销该会话的取消信号。
    cancellation: CancellationToken,
}

/// 节点名额目录保留从连接目录移除但仍有 writer 的会话，连续替换不能跳过更早责任。
#[derive(Default)]
pub struct Local {
    /// 节点作用域内仍承担名额或排空责任的会话目录。
    entries: Mutex<BTreeMap<String, Arc<Entry>>>,
}

/// 新会话名额及其全部前任写出责任，旧会话排空前不能开放新会话。
pub struct LocalLease {
    /// 新连接自身持有的节点名额责任。
    entry: Arc<Entry>,
    /// 被本次替换淘汰但仍需排空的旧连接 owner，不能只等待最近一个前任。
    previous: Vec<Arc<Entry>>,
}

impl Local {
    /// 业务作用：在空闲期间回收已经关闭且写出结束的节点名额，保留所有仍有真实责任的会话。
    /// 参数说明: 无。
    ///
    /// 返回：只删除已排空条目，不以连接目录或取消标记推测完成。
    pub fn reap(&self) {
        self.entries
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .retain(|_, entry| !entry.session.drained());
    }

    /// 业务作用：在同一节点锁内校验路由与主体配额，并为替换捕获全部尚未排空的旧会话。
    /// 参数说明：
    /// - `session`：尚未 Ready 的已认证会话。
    /// - `cancellation`：路由会话的网络取消域。
    /// - `policy`：当前冻结定义中的节点配额。
    /// - `capacity`：节点部署允许的最大实际连接责任数。
    ///
    /// 返回：持有名额与旧写出屏障的责任；容量不足不改动既有会话。
    pub fn register(
        &self,
        session: Arc<Session>,
        cancellation: CancellationToken,
        policy: &Connections,
        capacity: usize,
    ) -> anyhow::Result<LocalLease> {
        let mut entries = self
            .entries
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        entries.retain(|_, entry| !entry.session.drained());
        anyhow::ensure!(entries.len() < capacity, "node_connection_capacity");
        let identity = session.identity();
        let current = entries
            .values()
            .filter(|entry| entry.session.key.route_id == session.key.route_id)
            .collect::<Vec<_>>();
        let same = current
            .iter()
            .filter(|entry| {
                let previous = entry.session.identity();
                previous.tenant == identity.tenant && previous.uid == identity.uid
            })
            .map(|entry| (*entry).clone())
            .collect::<Vec<_>>();
        let replacement = policy.same_user_policy == SameUserPolicy::ReplaceOld;
        let retained = current.len()
            - if replacement {
                same.len()
            } else {
                0
            };
        anyhow::ensure!(
            !policy.limit_enabled || retained < policy.max_connections,
            "route_connection_capacity"
        );
        match policy.same_user_policy {
            SameUserPolicy::RejectNew => anyhow::ensure!(same.is_empty(), "user_already_connected"),
            SameUserPolicy::AllowMultiple => anyhow::ensure!(
                !policy.limit_enabled || same.len() < policy.max_per_uid,
                "user_connection_capacity"
            ),
            SameUserPolicy::ReplaceOld => {}
        }
        let previous = if replacement {
            same
        } else {
            Vec::new()
        };
        // 捕获未排空的全部祖先后才关闭旧连接，后续替换仍可看到它们持有的实际写出责任。
        for old in &previous {
            old.session.close();
            old.cancellation.cancel();
        }
        let entry = Arc::new(Entry {
            session,
            cancellation,
        });
        entries.insert(entry.session.key.route_session_id.clone(), entry.clone());
        Ok(LocalLease {
            entry,
            previous,
        })
    }
}

impl LocalLease {
    /// 业务作用：等待节点内全部旧写出结束，连续替换也不能同时开放两个排他会话。
    /// 参数说明：
    /// - `deadline`：原始握手总期限。
    ///
    /// 返回：会话仍有效且全部旧责任结束；超时或被下一次替换取消时不进入 Ready。
    pub async fn wait(&self, deadline: Instant) -> anyhow::Result<()> {
        loop {
            anyhow::ensure!(
                !self.entry.cancellation.is_cancelled() && self.entry.session.live(),
                "quota_session_closed"
            );
            if self.previous.iter().all(|entry| entry.session.drained()) {
                return Ok(());
            }
            anyhow::ensure!(Instant::now() < deadline, "quota_admission_timeout");
            tokio::select! {
                biased;
                _ = self.entry.cancellation.cancelled() => anyhow::bail!("quota_session_closed"),
                _ = tokio::time::sleep_until(deadline.min(Instant::now() + Duration::from_millis(25))) => {},
            }
        }
    }
}

impl Drop for LocalLease {
    /// 业务作用：归还连接责任之前永久关闭会话资格，目录只在真实 writer 全部结束后回收名额。
    /// 参数说明: 无。
    ///
    /// 返回：网络 owner 被通知结束；未完成写出仍阻止后续排他准入。
    fn drop(&mut self) {
        self.entry.session.close();
        self.entry.cancellation.cancel();
    }
}
