//! 回执票据只证明当前连接的一次接纳或处理结果，不作为业务权限。

use crate::domain::message::control::{nonce, AckLevel, Receipt};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, OnceLock},
};
use tokio::time::Instant;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Phase {
    Reserved,
    Writing,
    Awaiting,
}
struct Ticket {
    /// 票据精确绑定的原消息稳定键，不允许确认其它消息。
    message_key: String,
    /// 服务器要求的确认层级，接收与处理不能互相替代。
    level: AckLevel,
    /// 不超过消息及资格期限的本地单调截止点，重复 ACK 不刷新。
    deadline: Instant,
    /// 当前处于预约、实际写出或等待确认阶段，预约阶段不能接受终端确认。
    phase: Phase,
    /// 可选单群关联，用于取消订阅时只终止相应票据。
    group: Option<String>,
    /// 跟随本票据真实生命周期的资源及唯一终态观察责任。
    observed: Option<crate::observability::receipt::Receipt>,
}

/// 每个会话拥有自己的票据表，token 从不跨会话共享。
pub struct Receipts {
    /// 当前会话独有的随机 token 到票据目录，不跨会话共享回执权。
    tickets: Mutex<BTreeMap<String, Ticket>>,
    /// 此会话最多同时保存的待决票据数量，零容量不能启用回执。
    maximum: usize,
    /// 首次安装的节点指标来源，用于新票据接续同一观察目录。
    metrics: OnceLock<Arc<crate::observability::Metrics>>,
}

/// 固定回执处理分类，未知或迟到回执不能触发新的投递。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReceiptOutcome {
    /// 票据与消息及层级匹配，终端明确确认成功。
    Confirmed,
    /// 票据匹配，但终端明确报告未接受。
    Rejected,
    /// 票据已不存在或已过期，不影响当前投递权限。
    Stale,
    /// 写出尚未开始或关联材料不匹配，保留原合法票据。
    Invalid,
}

impl Receipts {
    /// 业务作用：建立受限的本连接终端回执表，不保留业务体。
    /// 参数说明：
    /// - `maximum`：同时待决的最大投递尝试数量。
    ///
    /// 返回：空票据表，零容量表示不能启用回执。
    pub fn new(maximum: usize) -> Self {
        Self {
            tickets: Mutex::new(BTreeMap::new()),
            maximum,
            metrics: OnceLock::new(),
        }
    }

    /// 业务作用：在接入实际终端前绑定节点指标来源，票据不创建独立 registry。
    /// 参数说明：
    /// - `metrics`：节点静态指标目录。
    ///
    /// 返回：只接受首次绑定，后续调用不迁移既有票据观察责任。
    pub fn observe(&self, metrics: Arc<crate::observability::Metrics>) {
        let _ = self.metrics.set(metrics);
    }

    /// 业务作用：在入队前预约不可猜测的回执 token，避免终端快速 ACK 早于记录建立。
    /// 参数说明：
    /// - `message_key`：本条推送的受信去重键。
    /// - `level`：路由要求的确认层级。
    /// - `deadline`：不超过消息和身份期限的单调截止点。
    /// - `group`：群推送的取消关联，用户定向为 None。
    ///
    /// 返回：待写 token；容量不足或期限已到不创建票据。
    pub fn reserve(
        &self,
        message_key: String,
        level: AckLevel,
        deadline: Instant,
        group: Option<String>,
    ) -> anyhow::Result<String> {
        let mut tickets = self.tickets.lock().unwrap_or_else(|e| e.into_inner());
        tickets.retain(|_, ticket| {
            let valid = ticket.deadline > Instant::now();
            if !valid {
                if let Some(observed) = ticket.observed.as_mut() {
                    observed.finish("expired");
                }
            }
            valid
        });
        anyhow::ensure!(
            tickets.len() < self.maximum && deadline > Instant::now(),
            "receipt_capacity_or_expiry"
        );
        let token = nonce();
        tickets.insert(
            token.clone(),
            Ticket {
                message_key,
                level,
                deadline,
                phase: Phase::Reserved,
                group,
                observed: self.metrics.get().map(|metrics| {
                    crate::observability::receipt::Receipt::new(metrics.clone(), level_name(level))
                }),
            },
        );
        Ok(token)
    }

    /// 业务作用：在取得最终投递许可之后、实际写出之前激活回执匹配。
    /// 参数说明：
    /// - `token`：本连接预约的投递尝试。
    ///
    /// 返回：尚未过期且只启动一次的票据被激活；其它情况拒绝写出。
    pub fn writing(&self, token: &str) -> bool {
        let mut tickets = self.tickets.lock().unwrap_or_else(|e| e.into_inner());
        let Some(ticket) = tickets.get_mut(token) else {
            return false;
        };
        if ticket.phase != Phase::Reserved || ticket.deadline <= Instant::now() {
            return false;
        }
        ticket.phase = Phase::Writing;
        if let Some(observed) = ticket.observed.as_mut() {
            observed.writing();
        }
        true
    }

    /// 业务作用：写出完成后保留有限回执元数据，允许此前 Writing 阶段已到达的真实 ACK。
    /// 参数说明：
    /// - `token`：已经提交到传输 writer 的尝试。
    ///
    /// 返回：仍待决时进入 Awaiting；已确认或失效时不重新创建票据。
    pub fn written(&self, token: &str) {
        let mut tickets = self.tickets.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(ticket) = tickets.get_mut(token) {
            if ticket.phase == Phase::Writing {
                ticket.phase = Phase::Awaiting;
            }
        }
    }

    /// 业务作用：按当前会话、消息键、层级和有效期限接纳终端回执。
    /// 参数说明：
    /// - `receipt`：客户端固定控制回执，不携带新消息目标。
    ///
    /// 返回：确认、拒绝、过期或无效分类，不发布 MQ 或触发自动重投。
    pub fn confirm(&self, receipt: &Receipt) -> ReceiptOutcome {
        let outcome = self.confirm_ticket(receipt);
        if let Some(metrics) = self.metrics.get() {
            metrics.add(
                crate::observability::Counter::ReceiptInputs,
                level_name(receipt.level),
                match outcome {
                    ReceiptOutcome::Confirmed => "accepted",
                    ReceiptOutcome::Rejected => "rejected",
                    ReceiptOutcome::Stale => "unknown",
                    ReceiptOutcome::Invalid => "invalid",
                },
                1,
            );
        }
        outcome
    }

    /// 业务作用：在票据目录的同一互斥边界内核对写出阶段和关联材料，仅合法回执终止当前票据。
    /// 参数说明：
    /// - `receipt`：当前会话收到的固定回执控制头。
    ///
    /// 返回：精确匹配的终态或不改变权限的拒绝分类；错误关联不移除仍待确认的合法票据。
    fn confirm_ticket(&self, receipt: &Receipt) -> ReceiptOutcome {
        let mut tickets = self.tickets.lock().unwrap_or_else(|e| e.into_inner());
        let Some(ticket) = tickets.get(&receipt.token) else {
            return ReceiptOutcome::Stale;
        };
        if ticket.deadline <= Instant::now() {
            if let Some(mut ticket) = tickets.remove(&receipt.token) {
                if let Some(observed) = ticket.observed.as_mut() {
                    observed.finish("expired");
                }
            }
            return ReceiptOutcome::Stale;
        }
        if ticket.phase == Phase::Reserved
            || ticket.message_key != receipt.message_key
            || ticket.level != receipt.level
        {
            return ReceiptOutcome::Invalid;
        }
        if let Some(mut ticket) = tickets.remove(&receipt.token) {
            if let Some(observed) = ticket.observed.as_mut() {
                observed.finish(if receipt.accepted {
                    "ok"
                } else {
                    "rejected"
                });
            }
        }
        if receipt.accepted {
            ReceiptOutcome::Confirmed
        } else {
            ReceiptOutcome::Rejected
        }
    }

    /// 业务作用：写出失败或群取消时终止相应票据，迟到回执不得恢复它。
    /// 参数说明：
    /// - `token`：单次失败的 token，None 时按群或全部清理。
    /// - `group`：需要取消的群，两个参数都为空时关闭全部票据。
    ///
    /// 返回：匹配票据移除，释放容量且不改变业务处理结果。
    pub fn cancel(&self, token: Option<&str>, group: Option<&str>) {
        let mut tickets = self.tickets.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(token) = token {
            tickets.remove(token);
        } else if let Some(group) = group {
            tickets.retain(|_, ticket| ticket.group.as_deref() != Some(group));
        } else {
            tickets.clear();
        }
    }

    /// 业务作用：回收未确认的到期票据，超时只形成观察结果，不延长投递有效期。
    /// 参数说明: 无。
    ///
    /// 返回：本次终止的票据数量，供有界指标累加。
    pub fn expire(&self) -> usize {
        let mut tickets = self.tickets.lock().unwrap_or_else(|e| e.into_inner());
        let before = tickets.len();
        let now = Instant::now();
        tickets.retain(|_, ticket| {
            let valid = ticket.deadline > now;
            if !valid {
                if let Some(observed) = ticket.observed.as_mut() {
                    observed.finish("expired");
                }
            }
            valid
        });
        before - tickets.len()
    }
}

/// 业务作用：将协议确认层级映射到固定指标目录，不从客户端内容创建标签。
/// 参数说明：
/// - `level`：服务器已经验证的确认层级。
///
/// 返回：有限名称 received 或 processed。
fn level_name(level: AckLevel) -> &'static str {
    match level {
        AckLevel::SdkReceived => "received",
        AckLevel::ApplicationProcessed => "processed",
    }
}
