//! Engine.IO WebSocket 与 Socket.IO namespace 适配，权限和业务编码由产品核心统一决定。

use crate::{
    adapters::outbound::identity::now,
    application::gateway::{actions, outbox::Batch, Connected, Gateway, Handshake, PendingSession},
    domain::message::{
        control::{self, Authenticate, Confirm, Ready, Receipt},
        envelope::{ClientRequest, Effect, Kind as MessageKind},
        json,
        wire::{Frame, FrameType},
    },
};
use bytes::Bytes;
use nasa::ws::{
    server::ProtocolContext,
    socketio::packet::{Kind, Packet},
    transport::{budget::Received as Inbound, Connection, Outbound},
};
use std::{collections::BTreeMap, sync::Arc, time::Duration};
use tokio::{sync::mpsc, time::Instant};
use tokio_util::sync::CancellationToken;

enum Output {
    /// 控制消息按独立 FIFO 写出，Ready 仍需复验会话资格。
    Control(
        /// 已持有独立控制字节预算的完整传输包。
        crate::domain::message::control_output::Packet,
        /// 原始写出截止点，队列等待不能延长。
        Instant,
        /// Ready 必须绑定的会话；其它无授权副作用控制包可为空。
        Option<crate::application::gateway::outbox::DeliveryCheck>,
    ),
    /// 业务消息连同附件作为完整批次交给唯一物理 writer。
    Business(
        /// 该批次唯一对应的线协议 namespace。
        String,
        /// 持有字节预约和最终权限复验条件的业务批次。
        Batch,
    ),
}
#[derive(Clone)]
struct OutputSender {
    /// 握手、Ready 和状态通知的独立有界 FIFO，保证控制消息相对次序。
    controls: mpsc::Sender<Output>,
    /// 业务完整批次的有界入口，与控制容量分开。
    business: mpsc::Sender<Output>,
    /// 节点共享的独立控制输出字节预算，实际写出结束才归还。
    budget: Arc<crate::domain::message::control_output::Budget>,
}
#[expect(
    clippy::large_enum_variant,
    reason = "认证结果经有界队列转移唯一会话责任，不额外分配间接 owner。"
)]
enum Signal {
    Authentication {
        /// 异步结果对应的准确 namespace，不能投递给其它逻辑会话。
        namespace: String,
        /// 该 namespace 的本地认证尝试序号，拒绝关闭重建后的迟到结果。
        attempt: u64,
        /// 原认证或初始化任务的完成结果，处理时仍复验当前尝试和资格。
        result: anyhow::Result<PendingSession>,
    },
    Initialized {
        /// 异步结果对应的准确 namespace，不能投递给其它逻辑会话。
        namespace: String,
        /// 该 namespace 的本地认证尝试序号，拒绝关闭重建后的迟到结果。
        attempt: u64,
        /// 原认证或初始化任务的完成结果，处理时仍复验当前尝试和资格。
        result: anyhow::Result<()>,
    },
}
struct Namespace {
    /// 当前 namespace 精确尝试序号，不与客户端 requestId 混用。
    attempt: u64,
    /// 该 namespace 独立取消域，不影响同一物理连接的其它 namespace。
    cancellation: CancellationToken,
    /// 首次发起该 namespace 认证的固定单调截止点，确认和初始化共用。
    deadline: Instant,
    /// 认证完成但尚未确认的会话，持有容量却不能接收业务推送。
    pending: Option<PendingSession>,
    /// 已经登记的精确逻辑会话，业务处理仍等待 Ready 实际写出。
    connection: Option<Arc<Connected>>,
    /// 交给此 namespace 转发任务的唯一业务队列接收端。
    outgoing: Option<crate::application::gateway::queue::Receiver>,
    /// 适配层初始化及 Ready 排队已完成的状态，最终写出另行核对权限。
    ready: bool,
    /// 标准 Socket.IO namespace 连接响应是否已生成，决定断连响应形式。
    connected: bool,
    /// 在途操作 ACK 编号的弱 owner 索引，实际批次结束前不能复用编号。
    acks: BTreeMap<u64, std::sync::Weak<()>>,
    /// 当前 namespace 请求速率窗口的单调起点。
    rate_start: Instant,
    /// 该速率窗口内已接纳的客户端请求次数。
    requests: u32,
}
struct Attachment {
    /// 尚未组装完成附件唯一所属的 namespace。
    namespace: String,
    /// 已经识别的固定客户端事件名，附件不能另行更换事件。
    event: String,
    /// 与此附件头绑定的标准 Socket.IO ACK 编号。
    ack: u64,
    /// 附件头携带的固定控制区字节，业务正文仍待独立接收。
    metadata: Bytes,
    /// 从附件头开始固定的组装截止点，碎片不能刷新预算。
    deadline: Instant,
}
struct Owner {
    /// 全部 namespace 共用的会话登记和关闭运行域。
    gateway: Arc<Gateway>,
    /// 本物理连接的有限逻辑会话目录，物理连接退出时全部撤销。
    namespaces: BTreeMap<String, Namespace>,
}
impl Drop for Owner {
    /// 业务作用：物理连接退出时同时撤销全部 namespace，迟到任务不能重新登记旧连接。
    /// 参数说明: 无。
    ///
    /// 返回：取消认证、初始化与业务任务，并移除已登记的精确会话。
    fn drop(&mut self) {
        for namespace in self.namespaces.values() {
            namespace.cancellation.cancel();
            if let Some(connection) = &namespace.connection {
                self.gateway
                    .disconnect(&connection.session.key.route_session_id);
            }
        }
    }
}

/// 业务作用：承载标准 Socket.IO namespace 和二进制附件，物理 writer 串行发送完整批次。
/// 参数说明：
/// - `gateway`：各接入共用的身份、订阅和业务操作服务。
/// - `transport`：已经验证路径和 Engine.IO 参数的 WebSocket。
/// - `context`：受管物理连接的原始认证期限和子任务能力。
/// - `handshake`：真实来源与独立端口绑定。
/// - `admission`：随真实读写任务保留的节点准入责任。
///
/// 返回：物理连接结束且所有逻辑资格撤销；附件歧义关闭整个 transport。
pub async fn run(
    gateway: Arc<Gateway>,
    transport: Connection,
    mut context: ProtocolContext,
    handshake: Handshake,
    admission: Arc<crate::application::admission::Admission>,
) -> anyhow::Result<()> {
    let settings = gateway.configuration.transport.clone();
    let cancellation = context.cancellation();
    let (mut reader, mut writer) = transport.split();
    let open = serde_json::json!({
        "sid": control::nonce(),
        "upgrades": [],
        "pingInterval": settings.heartbeat_interval_ms,
        "pingTimeout": settings.heartbeat_timeout_ms,
        "maxPayload": settings.max_frame_bytes
    });
    gateway
        .control_output
        .packet(Outbound::Text(format!("0{open}")))?
        .write(&mut writer, context.auth_deadline())
        .await?;
    let (controls, mut control_writes) = mpsc::channel::<Output>(256);
    let (business, mut writes) = mpsc::channel::<Output>(256);
    let output = OutputSender {
        controls,
        business,
        budget: gateway.control_output.clone(),
    };
    let cancel = cancellation.clone();
    let io_admission = admission.clone();
    context.spawn(async move {
        let _admission = io_admission;
        let _cancel = cancel.clone().drop_guard();
        loop {
            let value = tokio::select! {
                biased;
                _ = cancel.cancelled() => break,
                value = control_writes.recv() => value,
                value = writes.recv() => value,
            };
            let Some(value) = value else {
                break;
            };
            let result = match value {
                Output::Control(packet, deadline, session) => {
                    if let Some(session) = session {
                        if let Some(permit) = session.acquire(deadline).await? {
                            crate::application::gateway::outbox::guard_write(
                                Some(&permit),
                                packet.write(&mut writer, permit.deadline.min(deadline)),
                            )
                            .await
                        } else {
                            // 尚未写出任何字节时丢弃过期 Ready，由 namespace owner 发出关闭状态。
                            Ok(())
                        }
                    } else {
                        packet.write(&mut writer, deadline).await
                    }
                }
                Output::Business(namespace, batch) => {
                    batch.write_socketio(&mut writer, &namespace).await
                }
            };
            if result.is_err() {
                break;
            }
        }
        cancel.cancel();
        Ok::<(), anyhow::Error>(())
    });
    let (incoming, mut reads) = mpsc::channel(2);
    let cancel = cancellation.clone();
    let metrics = gateway.metrics.clone();
    let io_admission = admission.clone();
    context.spawn(async move {
        let _admission = io_admission;
        loop {
            let value = tokio::select! {
                biased;
                _ = cancel.cancelled() => break,
                value = reader.receive_bytes(Instant::now() + Duration::from_secs(120)) => value,
            };
            let value = match value {
                Ok(value) => value,
                Err(_) => break,
            };
            metrics.received(&value);
            if tokio::select! {
                biased;
                _ = cancel.cancelled() => true,
                result = incoming.send(value) => result.is_err(),
            } {
                break;
            }
        }
        cancel.cancel();
    });
    let (signals, mut outcomes) = mpsc::channel(128);
    let mut owner = Owner {
        gateway: gateway.clone(),
        namespaces: BTreeMap::new(),
    };
    let mut sequence = 0u64;
    let mut admitted = false;
    let mut attachment: Option<Attachment> = None;
    let mut next_ping = Instant::now() + Duration::from_millis(settings.heartbeat_interval_ms);
    let mut pong_deadline = None;
    let mut timer = tokio::time::interval(Duration::from_millis(50));
    timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            biased;
            _ = cancellation.cancelled() => break,
            _ = timer.tick() => {
                let current = Instant::now();
                if pong_deadline.is_some_and(|deadline| deadline <= current) {
                    gateway.metrics.add(crate::observability::Counter::Heartbeats, "socketio", "timeout", 1);
                    anyhow::bail!("heartbeat_timeout");
                }
                anyhow::ensure!(
                    attachment
                        .as_ref()
                        .is_none_or(|part| part.deadline > current),
                    "attachment_timeout"
                );
                if pong_deadline.is_none() && current >= next_ping {
                    let deadline = current + Duration::from_millis(settings.heartbeat_timeout_ms);
                    control_output(&output, Outbound::Text("2".into()), deadline)?;
                    pong_deadline = Some(deadline);
                }
                for (namespace, entry) in &owner.namespaces {
                    if entry.ready {
                        if let Some(connection) = &entry.connection {
                            if connection.maintain().await.is_err() {
                                connection.session.close();
                                connection.cancellation.cancel();
                            }
                            state_output(&output, connection, namespace)?;
                        }
                    }
                }
                let closing: Vec<_> = owner
                    .namespaces
                    .iter()
                    .filter(|(_, entry)| {
                        if entry.ready {
                            entry.connection.as_ref().is_none_or(|c| {
                                c.cancellation.is_cancelled() || c.session.candidate(None).is_none()
                            })
                        } else {
                            entry.deadline <= current
                        }
                    })
                    .map(|(name, _)| name.clone())
                    .collect();
                for namespace in closing {
                    close_namespace(&mut owner, &output, &namespace, "session_unavailable")?;
                }
            }
            outcome = outcomes.recv() => {
                let Some(outcome) = outcome else {
                    break;
                };
                match outcome {
                    Signal::Authentication {
                        namespace,
                        attempt,
                        result,
                    } => {
                        let Some(entry) = owner
                            .namespaces
                            .get_mut(&namespace)
                            .filter(|e| e.attempt == attempt && !e.cancellation.is_cancelled())
                        else {
                            continue;
                        };
                        match result {
                            Ok(pending) if Instant::now() < entry.deadline => {
                                pending.handshake_attempt.stage("hello_queue");
                                socket_packet(
                                    &output,
                                    Kind::Connect,
                                    &namespace,
                                    None,
                                    &serde_json::json!({
                                        "sid": pending.session.key.route_session_id
                                    }),
                                    entry.deadline,
                                )?;
                                socket_packet(
                                    &output,
                                    Kind::Event,
                                    &namespace,
                                    None,
                                    &serde_json::json!(["nacket:hello", pending.hello]),
                                    entry.deadline,
                                )?;
                                entry.connected = true;
                                pending.handshake_attempt.stage("confirm_read");
                                entry.pending = Some(pending);
                            }
                            Err(error) if error.is::<crate::application::gateway::authority::TemporarilyUnavailable>() => {
                                close_namespace(&mut owner, &output, &namespace, "route_unavailable")?;
                            }
                            Err(error) if error.is::<crate::application::gateway::authority::StaleBinding>() => {
                                close_namespace(&mut owner, &output, &namespace, "directory_binding_stale")?;
                            }
                            _ => {
                                close_namespace(&mut owner, &output, &namespace, "authentication_failed")?;
                            }
                        }
                    }
                    Signal::Initialized {
                        namespace,
                        attempt,
                        result,
                    } => {
                        let Some(entry) = owner
                            .namespaces
                            .get_mut(&namespace)
                            .filter(|e| e.attempt == attempt)
                        else {
                            continue;
                        };
                        if result.is_err() || Instant::now() >= entry.deadline {
                            close_namespace(&mut owner, &output, &namespace, "initialization_failed")?;
                            continue;
                        }
                        let connection = entry
                            .connection
                            .as_ref()
                            .ok_or_else(|| anyhow::anyhow!("session_missing"))?;
                        if connection.session.candidate(None).is_none() {
                            connection.handshake_attempt.reject(&anyhow::anyhow!("session_not_ready"));
                            close_namespace(&mut owner, &output, &namespace, "session_not_ready")?;
                            continue;
                        }
                        let ready = Ready {
                            attempt_id: connection.attempt_id.clone(),
                            session_id: connection.session.key.route_session_id.clone(),
                            route_revision: connection.session.key.route_revision.clone(),
                            issued_at: now()?,
                        };
                        // Ready 与后续状态使用同一控制 FIFO；业务写出仍独立复验当前会话。
                        connection.handshake_attempt.stage("ready_queue");
                        if let Err(error) = guarded_socket_packet(
                            &output, Kind::Event, &namespace, None,
                            &serde_json::json!(["nacket:ready", ready]), entry.deadline,
                            Some(crate::application::gateway::outbox::DeliveryCheck::direct(connection.session.clone(), connection.uid_events.clone())),
                        ) {
                            connection.handshake_attempt.reject(&error);
                            close_namespace(&mut owner, &output, &namespace, "ready_queue_unavailable")?;
                            continue;
                        }
                        if !admitted {
                            anyhow::ensure!(context.authenticated(), "authentication_deadline");
                            admission.authenticated();
                            admitted = true;
                        }
                        entry.ready = true;
                        if gateway.publish_ready(connection).await.is_err() {
                            close_namespace(&mut owner, &output, &namespace, "session_closed")?;
                            continue;
                        }
                        let mut outgoing = entry
                            .outgoing
                            .take()
                            .ok_or_else(|| anyhow::anyhow!("writer_missing"))?;
                        let target = output.clone();
                        let cancel = entry.cancellation.clone();
                        let physical = cancellation.clone();
                        context.spawn(async move {
                            loop {
                                let batch = tokio::select! {
                                    biased;
                                    _ = cancel.cancelled() => break,
                                    batch = outgoing.recv() => batch,
                                };
                                let Some(batch) = batch else {
                                    break;
                                };
                                let sent = tokio::select! {
                                    biased;
                                    _ = cancel.cancelled() => break,
                                    result = target.business.send(Output::Business(namespace.clone(), batch)) => result,
                                };
                                if sent.is_err() {
                                    physical.cancel();
                                    break;
                                }
                            }
                        });
                    }
                }
            }
            incoming = reads.recv() => {
                let Some(incoming) = incoming else {
                    break;
                };
                match incoming {
                    Inbound::Closed => break,
                    Inbound::Ping(_) => control_output(
                        &output,
                        Outbound::Flush,
                        Instant::now() + Duration::from_millis(settings.write_timeout_ms),
                    )?,
                    Inbound::Pong(_) => {}
                    Inbound::Binary(body) => {
                        let part = attachment
                            .take()
                            .ok_or_else(|| anyhow::anyhow!("unexpected_attachment"))?;
                        anyhow::ensure!(
                            Instant::now() < part.deadline
                                && body.len() + part.metadata.len() + 11 <= settings.max_frame_bytes,
                            "attachment_capacity_or_deadline"
                        );
                        let entry = owner
                            .namespaces
                            .get_mut(&part.namespace)
                            .filter(|e| e.ready)
                            .ok_or_else(|| anyhow::anyhow!("namespace_not_ready"))?;
                        let mode = entry
                            .connection
                            .as_ref()
                            .expect("ready session")
                            .plan
                            .definition
                            .serialization
                            .mode()
                            .ordinal();
                        if dispatch(
                            &gateway,
                            entry,
                            &part.event,
                            part.ack,
                            Frame {
                                typ: FrameType::Event,
                                mode: Some(mode),
                                metadata: part.metadata,
                                body: Some(body),
                            },
                            &context,
                        )
                        .is_err()
                        {
                            close_namespace(
                                &mut owner,
                                &output,
                                &part.namespace,
                                "namespace_request_rejected",
                            )?;
                        }
                    }
                    Inbound::Text(text) => {
                        let text = std::str::from_utf8(&text)?;
                        if text == "3" {
                            anyhow::ensure!(
                                pong_deadline.is_some_and(|deadline| deadline > Instant::now()),
                                "unexpected_pong"
                            );
                            if let Some(deadline) = pong_deadline {
                                let started = deadline - Duration::from_millis(settings.heartbeat_timeout_ms);
                                gateway.metrics.duration("heartbeat_rtt", started.elapsed());
                                gateway.metrics.add(crate::observability::Counter::Heartbeats, "socketio", "ok", 1);
                            }
                            pong_deadline = None;
                            next_ping = Instant::now() + Duration::from_millis(settings.heartbeat_interval_ms);
                            continue;
                        }
                        if text == "1" {
                            break;
                        }
                        anyhow::ensure!(
                            text.starts_with('4') && attachment.is_none(),
                            "overlapping_packet"
                        );
                        let packet = Packet::decode(&text[1..], 1)
                            .ok_or_else(|| anyhow::anyhow!("invalid_socketio_packet"))?;
                        anyhow::ensure!(
                            packet.json.len() <= settings.max_metadata_bytes,
                            "metadata_capacity"
                        );
                        let handled = async {
                            match packet.kind {
                                Kind::Connect => {
                                    anyhow::ensure!(
                                        !owner.namespaces.contains_key(packet.namespace)
                                            && owner.namespaces.len() < settings.max_namespaces,
                                        "namespace_capacity_or_duplicate"
                                    );
                                    let mut request: Authenticate =
                                        json::decode(packet.json.as_bytes(), settings.max_metadata_bytes)?;
                                    anyhow::ensure!(request.endpoint == packet.namespace, "namespace_mismatch");
                                    if let Some(legacy) = &handshake.legacy_path {
                                        // 旧路径连接仅承载根 namespace；核心仍用冻结的标准 namespace 校验身份与授权。
                                        anyhow::ensure!(packet.namespace == "/", "legacy_path_namespace_denied");
                                        request.endpoint = legacy.namespace.clone();
                                    }
                                    sequence = sequence
                                        .checked_add(1)
                                        .ok_or_else(|| anyhow::anyhow!("attempt_exhausted"))?;
                                    let deadline = if admitted {
                                        Instant::now()
                                            + Duration::from_millis(handshake.binding.authentication_timeout_ms)
                                    } else {
                                        context.auth_deadline()
                                    };
                                    let cancel = cancellation.child_token();
                                    owner.namespaces.insert(
                                        packet.namespace.into(),
                                        Namespace {
                                            attempt: sequence,
                                            cancellation: cancel.clone(),
                                            deadline,
                                            pending: None,
                                            connection: None,
                                            outgoing: None,
                                            ready: false,
                                            connected: false,
                                            acks: BTreeMap::new(),
                                            rate_start: Instant::now(),
                                            requests: 0,
                                        },
                                    );
                                    let target = signals.clone();
                                    let gateway = gateway.clone();
                                    let facts = handshake.clone();
                                    let namespace = packet.namespace.to_owned();
                                    let attempt = sequence;
                                    context.spawn(async move {
                                        let result = tokio::select! {
                                            biased;
                                            _ = cancel.cancelled() => return,
                                            result = gateway.authenticate(request, &facts, deadline) => result,
                                        };
                                        let _ = tokio::select! {
                                            biased;
                                            _ = cancel.cancelled() => return,
                                            result = target.send(Signal::Authentication {
                                                namespace,
                                                attempt,
                                                result,
                                            }) => result,
                                        };
                                    });
                                }
                                Kind::Disconnect => {
                                    remove_namespace(&mut owner, packet.namespace);
                                }
                                Kind::Event | Kind::BinaryEvent => {
                                    let mut values: Vec<serde_json::Value> =
                                        json::decode(packet.json.as_bytes(), settings.max_metadata_bytes)?;
                                    anyhow::ensure!(values.len() == 2, "invalid_event_arguments");
                                    let envelope = values.pop().expect("two arguments");
                                    let event = values
                                        .pop()
                                        .and_then(|v| v.as_str().map(str::to_owned))
                                        .ok_or_else(|| anyhow::anyhow!("invalid_event_name"))?;
                                    let entry = owner
                                        .namespaces
                                        .get_mut(packet.namespace)
                                        .ok_or_else(|| anyhow::anyhow!("unknown_namespace"))?;
                                    if event == "nacket:confirm" {
                                        anyhow::ensure!(
                                            !entry.ready
                                                && packet.kind == Kind::Event
                                                && packet.ack_id.is_none()
                                                && entry.connection.is_none(),
                                            "unexpected_confirmation"
                                        );
                                        let confirmation: Confirm = serde_json::from_value(envelope)?;
                                        let pending = entry
                                            .pending
                                            .take()
                                            .ok_or_else(|| anyhow::anyhow!("hello_required"))?;
                                        let (connection, outgoing) = gateway.confirm(
                                            pending,
                                            confirmation,
                                            entry.cancellation.clone(),
                                            entry.deadline,
                                        ).await?;
                                        entry.connection = Some(connection.clone());
                                        entry.outgoing = Some(outgoing);
                                        let namespace = packet.namespace.to_owned();
                                        let attempt = entry.attempt;
                                        let deadline = entry.deadline;
                                        let target = signals.clone();
                                        let gateway = gateway.clone();
                                        let cancel = entry.cancellation.clone();
                                        context.spawn(async move {
                                            let result = tokio::select! {
                                                biased;
                                                _ = cancel.cancelled() => return,
                                                result = gateway.initialize(&connection, deadline) => result,
                                            };
                                            let _ = tokio::select! {
                                                biased;
                                                _ = cancel.cancelled() => return,
                                                result = target.send(Signal::Initialized {
                                                    namespace,
                                                    attempt,
                                                    result,
                                                }) => result,
                                            };
                                        });
                                    } else if event == "nacket:receipt" {
                                        anyhow::ensure!(
                                            entry.ready
                                                && packet.kind == Kind::Event
                                                && packet.ack_id.is_none(),
                                            "receipt_not_allowed"
                                        );
                                        let receipt: Receipt = serde_json::from_value(envelope)?;
                                        entry
                                            .connection
                                            .as_ref()
                                            .expect("ready session")
                                            .confirm_receipt(receipt).await?;
                                    } else {
                                        anyhow::ensure!(entry.ready, "namespace_not_ready");
                                        let ack = packet
                                            .ack_id
                                            .ok_or_else(|| anyhow::anyhow!("ack_id_required"))?;
                                        let mut envelope = envelope
                                            .as_object()
                                            .cloned()
                                            .ok_or_else(|| anyhow::anyhow!("invalid_envelope"))?;
                                        let placeholder = envelope.remove("message");
                                        let metadata = serde_json::to_vec(&envelope)?;
                                        let capacity = metadata.capacity();
                                        let metadata = gateway.inbound_bytes.retain(metadata.into(), capacity)?;
                                        if packet.kind == Kind::BinaryEvent {
                                            anyhow::ensure!(
                                                placeholder
                                                    == Some(serde_json::json!({
                                                        "_placeholder": true,
                                                        "num": 0
                                                    }))
                                                    && packet.attachments == 1,
                                                "invalid_attachment_placeholder"
                                            );
                                            // 验证 namespace 和方向后才保留附件头，孤立或交错附件不能绑定到其它会话。
                                            validate_event(entry, &event, &metadata, true)?;
                                            attachment = Some(Attachment {
                                                namespace: packet.namespace.into(),
                                                event,
                                                ack,
                                                metadata,
                                                deadline: Instant::now()
                                                    + Duration::from_millis(settings.assembly_timeout_ms),
                                            });
                                        } else {
                                            anyhow::ensure!(placeholder.is_none(), "unexpected_message");
                                            dispatch(
                                                &gateway,
                                                entry,
                                                &event,
                                                ack,
                                                Frame {
                                                    typ: FrameType::Event,
                                                    mode: None,
                                                    metadata,
                                                    body: None,
                                                },
                                                &context,
                                            )?;
                                        }
                                    }
                                }
                                _ => anyhow::bail!("unexpected_socketio_packet"),
                            }
                            Ok::<(), anyhow::Error>(())
                        }.await;
                        if handled.is_err() {
                            // 附件归属不明确会破坏整个物理流；完整文本包的业务拒绝只撤销所属 namespace。
                            anyhow::ensure!(packet.kind != Kind::BinaryEvent, "invalid_attachment_scope");
                            close_namespace(
                                &mut owner,
                                &output,
                                packet.namespace,
                                "namespace_request_rejected",
                            )?;
                        }
                    }
                }
            }
        }
    }
    cancellation.cancel();
    Ok(())
}

/// 业务作用：把固定控制包加入唯一物理 writer，队列满时终止连接以免丢失状态顺序。
/// 参数说明：
/// - `output`：物理 writer 的有限队列。
/// - `packet`：无需业务附件的控制消息。
/// - `deadline`：最晚写出时刻。
///
/// 返回：已排队；失败不报告控制消息已经送达。
fn control_output(
    output: &OutputSender,
    packet: Outbound,
    deadline: Instant,
) -> anyhow::Result<()> {
    output
        .controls
        .try_send(Output::Control(
            output.budget.packet(packet)?,
            deadline,
            None,
        ))
        .map_err(|_| anyhow::anyhow!("control_queue_full"))
}

/// 业务作用：以标准 Socket.IO 包头发送固定控制对象，不把业务 schema 对象混入控制 JSON。
/// 参数说明：
/// - `output`：物理 writer 队列。
/// - `kind`：标准包类型。
/// - `namespace`：该逻辑会话的固定 namespace。
/// - `ack_id`：可选标准确认编号。
/// - `data`：固定控制参数。
/// - `deadline`：本次控制的截止点。
///
/// 返回：完整文本包排队成功。
fn socket_packet(
    output: &OutputSender,
    kind: Kind,
    namespace: &str,
    ack_id: Option<u64>,
    data: &serde_json::Value,
    deadline: Instant,
) -> anyhow::Result<()> {
    guarded_socket_packet(output, kind, namespace, ack_id, data, deadline, None)
}

/// 业务作用：将控制对象排入独立 FIFO，Ready 另外携带写出时必须复验的会话门禁。
/// 参数说明：
/// - `output`：唯一物理 writer 的独立控制队列。
/// - `kind`：标准 Socket.IO 包类型。
/// - `namespace`：当前逻辑连接名称。
/// - `ack_id`：可选确认编号。
/// - `data`：有限控制元数据。
/// - `deadline`：首次固定的最晚写出时刻。
/// - `session`：Ready 所属的权限 owner，其它控制提示不授予业务资格。
///
/// 返回：完整控制包已排队；容量不足要求关闭连接。
fn guarded_socket_packet(
    output: &OutputSender,
    kind: Kind,
    namespace: &str,
    ack_id: Option<u64>,
    data: &serde_json::Value,
    deadline: Instant,
    session: Option<crate::application::gateway::outbox::DeliveryCheck>,
) -> anyhow::Result<()> {
    let json = if kind == Kind::Disconnect {
        String::new()
    } else {
        serde_json::to_string(data)?
    };
    let packet = Packet {
        kind,
        namespace,
        ack_id,
        attachments: 0,
        json: &json,
    }
    .encode()
    .ok_or_else(|| anyhow::anyhow!("socketio_packet"))?;
    let packet = output.budget.packet(Outbound::Text(format!("4{packet}")))?;
    output
        .controls
        .try_send(Output::Control(packet, deadline, session))
        .map_err(|_| anyhow::anyhow!("control_queue_full"))
}

/// 业务作用：从物理连接中删除单个 namespace 的权限，其它 namespace 保持各自身份。
/// 参数说明：
/// - `owner`：当前 transport 的逻辑会话目录。
/// - `namespace`：待移除的准确名称。
///
/// 返回：该 namespace 的任务取消并退出产品会话目录。
fn remove_namespace(owner: &mut Owner, namespace: &str) {
    if let Some(entry) = owner.namespaces.remove(namespace) {
        entry.cancellation.cancel();
        if let Some(connection) = entry.connection {
            owner
                .gateway
                .disconnect(&connection.session.key.route_session_id);
        }
    }
}

/// 业务作用：先撤销 namespace 权限，再发出标准拒绝或断开包，输出失败不延迟撤权。
/// 参数说明：
/// - `owner`：物理连接会话目录。
/// - `output`：唯一 writer 的控制队列。
/// - `namespace`：失败逻辑会话。
/// - `code`：不会泄漏远程响应的固定原因。
///
/// 返回：权限已撤销；控制包无法排队时要求关闭 transport。
fn close_namespace(
    owner: &mut Owner,
    output: &OutputSender,
    namespace: &str,
    code: &str,
) -> anyhow::Result<()> {
    let connected = owner.namespaces.get(namespace).is_some_and(|e| e.connected);
    let ready = owner.namespaces.get(namespace).is_some_and(|e| e.ready);
    if let Some(connection) = owner
        .namespaces
        .get(namespace)
        .and_then(|entry| entry.connection.as_ref())
    {
        // 先撤销权限，再排定状态与 Disconnect；任何排队失败都不能恢复旧资格。
        connection.session.close();
        if ready {
            state_output(output, connection, namespace)?;
        }
    }
    remove_namespace(owner, namespace);
    socket_packet(
        output,
        if connected {
            Kind::Disconnect
        } else {
            Kind::ConnectError
        },
        namespace,
        None,
        &serde_json::json!({
            "message": code,
            "data": {
                "code": code
            }
        }),
        Instant::now() + Duration::from_secs(1),
    )
}

/// 业务作用：验证外层事件名和内层信封方向一致，别名也必须通过同一授权入口。
/// 参数说明：
/// - `entry`：已经 Ready 的精确 namespace。
/// - `event`：Socket.IO 外层事件名。
/// - `metadata`：移除唯一附件 placeholder 后的固定控制头。
/// - `present`：业务体存在标志，空附件仍为 true。
///
/// 返回：规范客户端请求；不得以更换事件名获取其它能力。
fn validate_event(
    entry: &Namespace,
    event: &str,
    metadata: &Bytes,
    present: bool,
) -> anyhow::Result<ClientRequest> {
    let request: ClientRequest = json::decode(metadata, metadata.len())?;
    request.validate(present)?;
    let connection = entry
        .connection
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("namespace_not_ready"))?;
    let event = if connection.plan.definition.compatibility.join_leave_aliases {
        match event {
            "join" => "sub",
            "leave" => "unsub",
            other => other,
        }
    } else {
        event
    };
    anyhow::ensure!(
        matches!(
            (event, request.kind),
            ("sub" | "nacket:sub", MessageKind::Sub)
                | ("unsub" | "nacket:unsub", MessageKind::Unsub)
                | ("msg" | "nacket:msg", MessageKind::Msg)
                | ("nacket:notify", MessageKind::Notify)
        ),
        "event_profile_mismatch"
    );
    Ok(request)
}

/// 业务作用：登记唯一 ACK 与按序订阅意图，再异步执行操作，保持后续取消可及时处理。
/// 参数说明：
/// - `gateway`：共享操作执行服务。
/// - `entry`：当前 namespace 的 ACK 与速率状态。
/// - `event`：已经匹配方向的外层事件。
/// - `ack`：当前 namespace 内唯一待决的确认编号。
/// - `frame`：适配后的固定产品信封与原始正文。
/// - `context`：受管子任务登记能力。
///
/// 返回：单次执行已登记或标准拒绝已入队，不产生空成功 ACK。
fn dispatch(
    gateway: &Arc<Gateway>,
    entry: &mut Namespace,
    event: &str,
    ack: u64,
    frame: Frame,
    context: &ProtocolContext,
) -> anyhow::Result<()> {
    let request = validate_event(entry, event, &frame.metadata, frame.body.is_some())?;
    let connection = entry
        .connection
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("namespace_not_ready"))?;
    entry.acks.retain(|_, lease| lease.strong_count() > 0);
    anyhow::ensure!(!entry.acks.contains_key(&ack), "duplicate_ack_id");
    anyhow::ensure!(
        entry.acks.len()
            < connection
                .plan
                .definition
                .connections
                .max_pending_operations
                + connection.plan.definition.delivery.max_queue_messages
                + 256,
        "ack_capacity"
    );
    let ack_lease = Arc::new(());
    entry.acks.insert(ack, Arc::downgrade(&ack_lease));
    if entry.rate_start.elapsed() >= Duration::from_secs(1) {
        entry.rate_start = Instant::now();
        entry.requests = 0;
    }
    entry.requests = entry.requests.saturating_add(1);
    anyhow::ensure!(
        entry.requests <= connection.plan.definition.connections.operations_per_second,
        "operation_rate"
    );
    let cleanup = request.kind == crate::domain::message::envelope::Kind::Unsub;
    let retained = frame
        .metadata
        .len()
        .saturating_add(frame.body.as_ref().map_or(0, bytes::Bytes::len))
        .saturating_add(65536);
    let local = if cleanup {
        connection.control_operations.clone()
    } else {
        connection.operations.clone()
    }
    .try_acquire_owned();
    let global = if cleanup {
        gateway.control_operation_capacity.clone()
    } else {
        gateway.operation_capacity.clone()
    }
    .try_acquire_owned();
    let prepared = if local.is_err() || global.is_err() {
        Err(anyhow::anyhow!("operation_capacity"))
    } else {
        let gateway = gateway.clone();
        let owner = connection.clone();
        connection.uid_events.submit_counted(
            connection.session.identity().deadline,
            retained,
            cleanup,
            move || {
                anyhow::ensure!(!owner.cancellation.is_cancelled(), "session_closed");
                // 有效主体下的业务拒绝应返回对应 ACK；执行域失权仍由外层错误终止会话。
                Ok(gateway.prepare_action(&owner, frame))
            },
        )
    };
    let work = match prepared {
        Ok(work) => work,
        Err(error) => {
            let reply = actions::reply(
                connection,
                &request,
                actions::rejection_code(&error),
                Effect::None,
                None,
                None,
            );
            let queued = connection.enqueue_reply(reply, Some((ack, ack_lease)))?;
            let owner = connection.clone();
            context.spawn(async move {
                if queued.wait().await.is_err() {
                    // 拒绝结果也必须经过有效写出屏障；交付失败后关闭该会话，避免继续接纳无法返回结果的动作。
                    owner.session.close();
                    owner.cancellation.cancel();
                }
            });
            return Ok(());
        }
    };
    let gateway = gateway.clone();
    let connection = connection.clone();
    context.spawn(async move {
        let (_local, _global) = (local, global);
        let work = tokio::select! {
            biased;
            _ = connection.cancellation.cancelled() => return,
            work = work.wait() => work,
        };
        let work = match work {
            Ok(work) => work,
            Err(_) => {
                // 失权只收紧当前会话，禁止绕过主体消费域重新执行已经登记的动作。
                connection.session.close();
                connection.cancellation.cancel();
                return;
            }
        };
        let reply = match work {
            Ok(work) => tokio::select! {
                biased;
                _ = connection.cancellation.cancelled() => return,
                reply = gateway.execute(&connection, work) => reply,
            },
            Err(error) => Ok(actions::reply(
                &connection,
                &request,
                actions::rejection_code(&error),
                Effect::None,
                None,
                None,
            )),
        };
        let reply = match reply {
            Ok(reply) => reply,
            Err(_) => {
                // 结果无法在主体执行域内完成时不再具备安全返回资格，关闭旧会话且不重新执行外部动作。
                connection.session.close();
                connection.cancellation.cancel();
                return;
            }
        };
        let queued = connection.enqueue_reply(reply, Some((ack, ack_lease)));
        let result = match queued {
            Ok(queued) => queued.wait().await,
            Err(error) => Err(error),
        };
        if result.is_err() {
            // ACK 排队或写出失败意味着本次结果无法安全交付，终止该会话以阻止后续动作继续进入。
            connection.session.close();
            connection.cancellation.cancel();
        }
    });
    Ok(())
}

/// 业务作用：将本会话已生效的通知按序转入独立物理控制队列，失败要求终止 transport。
/// 参数说明：
/// - `output`：共享物理 writer 的控制入口。
/// - `connection`：通知所属的精确逻辑会话。
/// - `namespace`：已绑定的标准 namespace。
///
/// 返回：现有通知全部已移交；不等待网络，也不改变已生效的撤权事实。
fn state_output(
    output: &OutputSender,
    connection: &Connected,
    namespace: &str,
) -> anyhow::Result<()> {
    let notices = connection
        .session
        .notices
        .get()
        .ok_or_else(|| anyhow::anyhow!("state_notices_missing"))?;
    let deadline = Instant::now() + Duration::from_secs(1);
    while let Some(notice) = notices.pop() {
        output
            .controls
            .try_send(Output::Control(notice.socketio(namespace)?, deadline, None))
            .map_err(|_| anyhow::anyhow!("control_queue_full"))?;
    }
    Ok(())
}
