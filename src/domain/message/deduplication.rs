//! 去重的接纳与完成点分开；没有完成消费决定的记录不能提前吞掉重投。

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};
use tokio::time::Instant;

struct Entry {
    /// 首次接纳的消息内容摘要，同 key 内容不一致时拒绝冲突。
    fingerprint: [u8; 32],
    /// 原消息的单调截止点，重复接纳不能刷新去重存活期。
    deadline: Instant,
    /// 本节点全部接收者已有明确处理决定的标志，不代表终端收到。
    completed: bool,
    /// 是否由某个实际 Attempt 持有执行权，避免并发重复处理。
    in_flight: bool,
    /// 当前拥有执行权的尝试序号，旧 Attempt 不能完成或解除新责任。
    attempt: u64,
}
struct State {
    /// 受条数和键字节预算约束的去重目录，保留原消息截止点。
    entries: BTreeMap<String, Entry>,
    /// 下一次唯一执行序号，溢出不能回绕复用旧权威。
    next_attempt: u64,
    /// 当前目录逻辑字节计费总量，条目退出时按同一口径归还。
    bytes: usize,
}

/// 有界去重窗口，处理中的条目也占用容量，不以淘汰在途记录绕过冲突检查。
pub struct Window {
    /// 键目录、序号和字节预算的统一短临界区，不跨网络或磁盘操作持有。
    state: Mutex<State>,
    /// 此去重窗口最多保留的消息键数量，容量满不淘汰有效证据。
    maximum_entries: usize,
    /// 此去重窗口允许持有的逻辑字节上限。
    maximum_bytes: usize,
}

/// 接纳结果明确区分已完成、仍在途和内容冲突，不能将它们都视为重复成功。
pub enum Admission {
    /// 首次接纳或前次未完成责任已退出，授予一次分发尝试。
    Accepted(
        /// 首次输入独占的分发尝试责任，结束后才发布去重终态。
        Attempt,
    ),
    /// 同键同内容已有明确完成证据，不再次分发。
    Completed,
    /// 同键同内容仍有执行者，不能把等待中当作完成。
    InFlight,
    /// 相同消息键对应不同内容，拒绝复用身份。
    Conflict,
    /// 条目或字节预算不足，不淘汰有效去重证据来接纳。
    Capacity,
    /// 消息期限已过，不产生新的执行责任。
    Expired,
}

/// 持有一次消费决定；只有 complete 后才可把同键重投视为已处理。
pub struct Attempt {
    /// 当前尝试所属窗口，直到完成或取消仍保留同一目录 owner。
    window: Arc<Window>,
    /// 首次接纳的稳定消息键，不能在执行过程中更换。
    key: String,
    /// 取得执行责任时冻结的序号，Drop 和完成均须精确核对。
    attempt: u64,
    /// 当前 Attempt 是否已明确完成，未完成退出只撤回执行占用而不伪造完成。
    completed: bool,
}

impl Window {
    /// 业务作用：创建同时限制条数和键元数据字节的去重窗口。
    /// 参数说明：
    /// - `maximum_entries`：包括在途记录的最大条数。
    /// - `maximum_bytes`：条目固定成本与键字节总预算。
    ///
    /// 返回：空窗口；零容量会拒绝接纳而不是无限保存。
    pub fn new(maximum_entries: usize, maximum_bytes: usize) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(State {
                entries: BTreeMap::new(),
                next_attempt: 0,
                bytes: 0,
            }),
            maximum_entries,
            maximum_bytes,
        })
    }

    /// 业务作用：在相同来源作用域内建立一次消费责任，检查稳定指纹及原始消息期限。
    /// 参数说明：
    /// - `key`：服务端按完整受信作用域派生的 messageKey。
    /// - `fingerprint`：规范化元数据与实际业务体摘要。
    /// - `deadline`：首轮消费确定的单调截止点，重投不得延长。
    ///
    /// 返回：可完成的消费票据或明确拒绝分类；容量不足不淘汰未过期记录。
    pub fn admit(
        self: &Arc<Self>,
        key: String,
        fingerprint: [u8; 32],
        mut deadline: Instant,
    ) -> Admission {
        let now = Instant::now();
        if deadline <= now {
            return Admission::Expired;
        }
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let expired: Vec<_> = state
            .entries
            .iter()
            .filter(|(_, entry)| entry.deadline <= now)
            .map(|(key, _)| key.clone())
            .collect();
        for key in expired {
            state.entries.remove(&key);
            state.bytes -= cost(&key);
        }
        if let Some(existing) = state.entries.get_mut(&key) {
            if existing.fingerprint != fingerprint {
                return Admission::Conflict;
            }
            existing.deadline = existing.deadline.min(deadline);
            if existing.completed {
                return Admission::Completed;
            }
            if existing.in_flight {
                return Admission::InFlight;
            }
            deadline = existing.deadline;
        }
        let added = !state.entries.contains_key(&key);
        let cost = if added {
            cost(&key)
        } else {
            0
        };
        if (added && state.entries.len() >= self.maximum_entries)
            || cost > self.maximum_bytes.saturating_sub(state.bytes)
        {
            return Admission::Capacity;
        }
        let Some(attempt) = state.next_attempt.checked_add(1) else {
            return Admission::Capacity;
        };
        state.next_attempt = attempt;
        state.bytes += cost;
        state.entries.insert(
            key.clone(),
            Entry {
                fingerprint,
                deadline,
                completed: false,
                in_flight: true,
                attempt,
            },
        );
        Admission::Accepted(Attempt {
            window: self.clone(),
            key,
            attempt,
            completed: false,
        })
    }
}

impl Attempt {
    /// 业务作用：在全部本地目标已有完成或终止决定后发布去重完成事实。
    /// 参数说明: 无。
    ///
    /// 返回：当前票据仍有效时标记 Completed；已过期或被替换的责任不会影响新消费。
    pub fn complete(mut self) {
        let mut state = self.window.state.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(entry) = state.entries.get_mut(&self.key) {
            if entry.attempt == self.attempt {
                entry.completed = true;
            }
        }
        self.completed = true;
    }
}

impl Drop for Attempt {
    /// 业务作用：消费失败或任务取消时释放未完成责任，允许 broker 重投重新执行。
    /// 参数说明: 无。
    ///
    /// 返回：转为可重试并保留原指纹及最短期限，不把失败处理标记为完成。
    fn drop(&mut self) {
        if self.completed {
            return;
        }
        let mut state = self.window.state.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(entry) = state.entries.get_mut(&self.key) {
            if entry.attempt == self.attempt {
                entry.in_flight = false;
            }
        }
    }
}

/// 业务作用：将键长度与固定状态成本计入去重容量，不按消息正文大小保留缓冲。
/// 参数说明：
/// - `key`：去重键原文。
///
/// 返回：保守固定元数据成本加键字节数。
fn cost(key: &str) -> usize {
    128usize.saturating_add(key.len())
}
