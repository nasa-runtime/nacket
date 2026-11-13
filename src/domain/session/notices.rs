//! 状态通知只投影本会话事实，独立于业务投递权限和正文队列。

use crate::domain::message::{
    control::PublicContext,
    control_output::{Budget, Lease, Packet},
    wire::{Frame, FrameType},
    UnixMillis,
};
use nasa::ws::{
    socketio::packet::{Kind, Packet as SocketPacket},
    transport::Outbound,
};
use serde::Serialize;
use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
};
use tokio::sync::Notify;

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
/// 对终端公开的有限状态原因，描述当前会话事实，不携带新的授权。
pub enum Reason {
    /// 指定订阅的成员授权已被撤销。
    SubscriptionRevoked,
    /// 指定订阅资格已到期，继续接收需重新获权。
    SubscriptionExpired,
    /// 订阅目标已撤销，旧目标版本不能继续接收。
    GroupRemoved,
    /// 身份接近截止点，提示本身不延长身份期限。
    AuthorizationExpiring,
    /// 新授权已应用，后续操作仍须满足新的期限与能力范围。
    AuthorizationRefreshed,
    /// 身份已到期，不能继续处理业务动作。
    AuthorizationExpired,
    /// 主体授权已被撤销，旧凭据不能恢复当前会话。
    AuthorizationRevoked,
    /// 当前精确会话已被踢出。
    SessionRevoked,
    /// 有限控制许可暂不可用；旧会话已关闭，只能按重连预算重新认证。
    RouteUnavailable,
    /// 路由正在关闭旧责任，当前会话不能继续接流。
    RouteDraining,
    /// 当前会话已结束，迟到通知不能影响其它连接。
    SessionClosed,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
/// 仅向当前会话发送的有序状态投影，使用尝试、会话和序号拒绝迟到状态。
pub struct SessionStateNotice {
    /// 只包含当前公开路由 ID、revision 和定义摘要，隐藏集群内部控制材料。
    pub context: PublicContext,
    /// 本次握手尝试的关联 ID，终端据此拒绝旧连接的迟到通知。
    pub attempt_id: String,
    /// 当前已确认会话 ID，通知不能用于影响重连后的其它会话。
    pub session_id: String,
    /// 本会话严格递增的十进制序号，保持跨语言整数精度。
    pub state_seq: String,
    /// 服务端确定的有限状态原因，不能携带任意业务控制指令。
    pub reason: Reason,
    /// 通知生成时的 Unix 毫秒整数，仅作事实时间，不授予或延长权限。
    pub issued_at: UnixMillis,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// 当前会话受影响的单个订阅标识；无订阅作用域时省略。
    pub group_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// 相关身份或成员授权的十进制版本，不与群代际互相替代。
    pub authorization_version: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// 相关群状态的十进制版本，仅在通知涉及该版本域时提供。
    pub group_version: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// 已知资格截止时刻的 Unix 毫秒整数；缺省不代表无限授权。
    pub expires_at: Option<UnixMillis>,
}

/// 一条状态通知及其控制输出份额，责任持续到实际写出结束。
pub struct Notice {
    /// 已经按方向收窄的状态模型，不包含业务正文、凭据或其它会话资料。
    value: SessionStateNotice,
    /// 随通知对象、编码和实际写出移交的独立控制字节预约。
    lease: Lease,
}

struct State {
    /// 最多三十二条尚未交给传输的通知，不能丢弃旧序号后继续伪装连续状态。
    queue: VecDeque<Notice>,
    /// 最后生成的本会话序号，溢出时关闭通知队列而不是回绕。
    sequence: u64,
    /// 终态或容量失败后永久封闭的标志，迟到调用不能重开通知序列。
    sealed: bool,
}

/// 精确会话的有限状态队列，终态或容量耗尽后封闭，禁止丢序后继续使用。
pub struct Notices {
    /// 会话建立时冻结的公开路由绑定，后续通知不能切换定义。
    context: PublicContext,
    /// 与当前认证确认绑定的精确尝试 ID。
    attempt_id: String,
    /// 当前会话唯一标识，重连创建新的通知 owner。
    session_id: String,
    /// 节点共享的独立控制输出预算，业务队列满不消耗此保留池。
    budget: Arc<Budget>,
    /// 通知队列、序号与终态的同一短临界区，确保生成次序一致。
    state: Mutex<State>,
    /// 唤醒唯一写出路径的通知信号，不承诺客户端已经收到状态。
    pub changed: Notify,
}

impl Notices {
    /// 业务作用：为精确连接建立独立状态序列，断线重连不继承旧序号或通知。
    /// 参数说明：
    /// - `context`：当前公开路由绑定，不含集群内部权威。
    /// - `attempt_id`：本次握手关联材料。
    /// - `session_id`：本次确认的精确会话。
    /// - `budget`：节点控制输出保留池。
    ///
    /// 返回：最多保存三十二条通知的会话队列。
    pub fn new(
        context: PublicContext,
        attempt_id: String,
        session_id: String,
        budget: Arc<Budget>,
    ) -> Arc<Self> {
        Arc::new(Self {
            context,
            attempt_id,
            session_id,
            budget,
            state: Mutex::new(State {
                queue: VecDeque::new(),
                sequence: 0,
                sealed: false,
            }),
            changed: Notify::new(),
        })
    }

    /// 业务作用：权限事实生效后保存有限状态投影，容量失败封闭队列，不能丢弃旧序号后继续运行。
    /// 参数说明：
    /// - `reason`：固定状态原因。
    /// - `group`：只可包含当前会话受影响的一个订阅。
    /// - `versions`：对应身份或成员授权版本，以及可选群状态版本。
    /// - `expires_at`：服务端派生的整数毫秒期限，仅供终端观察。
    /// - `terminal`：是否禁止后续通知，终止状态最多一条。
    ///
    /// 返回：已保留或终态后幂等忽略；失败要求调用方立即关闭会话。
    pub fn push(
        &self,
        reason: Reason,
        group: Option<&str>,
        versions: Option<(u64, Option<u64>)>,
        expires_at: Option<UnixMillis>,
        terminal: bool,
    ) -> anyhow::Result<()> {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if state.sealed {
            return Ok(());
        }
        let result = (|| {
            anyhow::ensure!(state.queue.len() < 32, "state_notice_capacity");
            // 标识符均最多一百二十八字节，预约覆盖对象和两份转义后的控制编码。
            let lease = self.budget.reserve(16 * 1024)?;
            let sequence = state
                .sequence
                .checked_add(1)
                .ok_or_else(|| anyhow::anyhow!("state_sequence_exhausted"))?;
            let value = SessionStateNotice {
                context: self.context.clone(),
                attempt_id: self.attempt_id.clone(),
                session_id: self.session_id.clone(),
                state_seq: sequence.to_string(),
                reason,
                issued_at: UnixMillis::new(u64::try_from(
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)?
                        .as_millis(),
                )?)
                .map_err(anyhow::Error::msg)?,
                group_id: group.map(str::to_owned),
                authorization_version: versions.map(|(version, _)| version.to_string()),
                group_version: versions
                    .and_then(|(_, version)| version.map(|version| version.to_string())),
                expires_at,
            };
            state.queue.push_back(Notice {
                value,
                lease,
            });
            state.sequence = sequence;
            Ok(())
        })();
        state.sealed = terminal || result.is_err();
        self.changed.notify_one();
        result
    }

    /// 业务作用：唯一传输 owner 按序取得通知，不由业务 writer 资格决定是否可读。
    /// 参数说明: 无。
    ///
    /// 返回：最早保留的通知；空队列返回 None。
    pub fn pop(&self) -> Option<Notice> {
        self.state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .queue
            .pop_front()
    }
}

impl Notice {
    /// 业务作用：将有限状态对象编码成原生 STATE 控制帧，不携带业务体或可切换编码。
    /// 参数说明: 无。
    ///
    /// 返回：由原控制预算覆盖的完整写出包。
    pub fn native(self) -> anyhow::Result<Packet> {
        let (header, body) = Frame::encode(FrameType::State, &self.value, None)?;
        anyhow::ensure!(header.len() <= 8192, "state_notice_size");
        Ok(self.lease.packet(Outbound::Binary {
            header,
            body,
        }))
    }

    /// 业务作用：把同一状态合同映射为 namespace 内的 nacket:state 事件，不申请业务 ACK。
    /// 参数说明：
    /// - `namespace`：本通知所属的精确 namespace。
    ///
    /// 返回：无附件的标准控制事件，保留原预算直到写出结束。
    pub fn socketio(self, namespace: &str) -> anyhow::Result<Packet> {
        let json = format!("[\"nacket:state\",{}]", serde_json::to_string(&self.value)?);
        let packet = SocketPacket {
            kind: Kind::Event,
            namespace,
            ack_id: None,
            attachments: 0,
            json: &json,
        }
        .encode()
        .ok_or_else(|| anyhow::anyhow!("state_notice_packet"))?;
        anyhow::ensure!(packet.len() <= 8192, "state_notice_size");
        Ok(self.lease.packet(Outbound::Text(format!("4{packet}"))))
    }
}
