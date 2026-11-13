use super::{notices::Reason, GroupGrant, IdentityGrant, Revocations, Session};
use crate::domain::message::UnixMillis;
use tokio::time::Instant;

impl Revocations {
    /// 业务作用：只为已失效的本会话订阅选择固定说明，不公开其它成员或撤权目录。
    /// 参数说明：
    /// - `identity`：本会话可信主体。
    /// - `group`：已经不能继续投递的订阅。
    /// - `grant`：被撤销或到期的旧授权。
    ///
    /// 返回：群删除、成员撤权或授权到期的有限原因。
    pub(super) fn subscription_reason(
        &self,
        identity: &IdentityGrant,
        group: &str,
        grant: &GroupGrant,
    ) -> Reason {
        let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if state
            .groups
            .get(&(identity.tenant.clone(), group.into()))
            .is_some_and(|version| *version >= grant.group_version)
        {
            Reason::GroupRemoved
        } else if grant.deadline <= Instant::now() {
            Reason::SubscriptionExpired
        } else {
            Reason::SubscriptionRevoked
        }
    }
}

impl Session {
    /// 业务作用：连接 owner 定期移除过期资格，并在当前身份到期前最多提示一次。
    /// 参数说明: 无。
    ///
    /// 返回：权限先行失效；通知容量不足立即关闭会话，提示不延长任何授权期限。
    pub fn maintain_notices(self: &std::sync::Arc<Self>) {
        let due = {
            let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            if !state.open || !state.ready {
                return;
            }
            state.identity.deadline <= Instant::now()
                || state
                    .next_subscription_expiry
                    .is_some_and(|deadline| deadline <= Instant::now())
        };
        // 平时只检查最早期限；完整订阅扫描只发生在到期或权威撤权投影时。
        if due {
            self.apply_revocations();
        }
        if self.route.unavailable_reason().is_some() {
            self.close();
            return;
        }
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if !state.open || !state.ready {
            return;
        }
        let deadline = state.identity.deadline;
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining > std::time::Duration::from_secs(5) || state.warned_deadline == Some(deadline)
        {
            return;
        }
        let Some(notices) = self.notices.get() else {
            return;
        };
        state.warned_deadline = Some(deadline);
        let expires = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .ok()
            .and_then(|now| u64::try_from((now + remaining).as_millis()).ok())
            .and_then(|value| UnixMillis::new(value).ok());
        if notices
            .push(
                Reason::AuthorizationExpiring,
                None,
                Some((state.identity.version, None)),
                expires,
                false,
            )
            .is_err()
        {
            // 无法保留连续通知时不继续维持可能被终端误读的权限基线。
            state.close("state_notification_unavailable");
            self.changes
                .send_modify(|value| *value = value.wrapping_add(1));
        }
    }
}
