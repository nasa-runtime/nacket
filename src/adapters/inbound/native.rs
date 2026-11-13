use crate::{
    adapters::outbound::identity::now,
    application::gateway::{actions, Connected, Gateway, Handshake},
    domain::message::{
        control::{self, Authenticate, Confirm, Heartbeat, Ready, Receipt},
        envelope::{ClientRequest, Effect},
        wire::{Frame, FrameType},
    },
};
use nasa::ws::{
    server::ProtocolContext,
    transport::{budget::Received as Inbound, Connection, Outbound, Reader, Writer},
};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{sync::mpsc, time::Instant};

struct SessionOwner {
    /// 会话登记目录和关闭入口，保证传输退出时撤销业务资格。
    gateway: Arc<Gateway>,
    /// 本次传输建立的精确会话 ID，关闭不能影响重连后的新会话。
    session_id: String,
}
impl Drop for SessionOwner {
    /// 业务作用：所有错误、超时和取消出口共用精确会话清理，避免半完成认证遗留权限。
    /// 参数说明: 无。
    ///
    /// 返回：移除目录并撤销该连接全部资格。
    fn drop(&mut self) {
        self.gateway.disconnect(&self.session_id);
    }
}

/// 业务作用：完成原生分区协议握手并运行独立读写任务，所有子任务纳入宿主关闭屏障。
/// 参数说明：
/// - `gateway`：节点共享运行域。
/// - `transport`：已完成传输协商的唯一连接。
/// - `context`：框架容量、原始截止时间与任务登记能力。
/// - `handshake`：真实路径、来源和监听绑定。
/// - `admission`：随真实读写任务保留的节点准入责任。
///
/// 返回：连接退出，目录和权限被精确清理；不会在部分帧取消后继续读取。
pub async fn run(
    gateway: Arc<Gateway>,
    transport: Connection,
    mut context: ProtocolContext,
    handshake: Handshake,
    admission: Arc<crate::application::admission::Admission>,
) -> anyhow::Result<()> {
    let settings = &gateway.configuration.transport;
    let (mut reader, mut writer) = transport.split();
    let frame = receive(
        &mut reader,
        &mut writer,
        settings.max_frame_bytes,
        settings.max_metadata_bytes,
        context.auth_deadline(),
        &gateway,
    )
    .await?;
    anyhow::ensure!(
        frame.typ == FrameType::Auth && frame.body.is_none(),
        "authentication_required"
    );
    let request: Authenticate = frame.metadata()?;
    let pending = match gateway
        .authenticate(request, &handshake, context.auth_deadline())
        .await
    {
        Ok(pending) => pending,
        Err(error) => {
            let code = if error.is::<crate::application::gateway::authority::StaleBinding>() {
                Some("directory_binding_stale")
            } else if error.is::<crate::application::gateway::authority::TemporarilyUnavailable>() {
                Some("route_unavailable")
            } else {
                None
            };
            if let Some(code) = code {
                // 只有认证前确定的可用性或目录事实允许重试；认证拒绝及无说明断开不产生恢复许可。
                let (header, body) = Frame::encode(
                    FrameType::Close,
                    &control::Close {
                        code: code.into(),
                    },
                    None,
                )?;
                gateway
                    .control_output
                    .packet(Outbound::Binary {
                        header,
                        body,
                    })?
                    .write(&mut writer, context.auth_deadline())
                    .await?;
            }
            return Err(error);
        }
    };
    let attempt = pending.handshake_attempt.clone();
    let cancellation = context.cancellation();
    let handshake_result = async {
        let (header, body) = Frame::encode(FrameType::AuthResponse, &pending.hello, None)?;
        attempt.check(
            gateway
                .control_output
                .packet(Outbound::Binary {
                    header,
                    body,
                })?
                .write(&mut writer, context.auth_deadline())
                .await,
        )?;
        attempt.stage("confirm_read");
        let frame = attempt.check(
            receive(
                &mut reader,
                &mut writer,
                settings.max_frame_bytes,
                settings.max_metadata_bytes,
                context.auth_deadline(),
                &gateway,
            )
            .await,
        )?;
        attempt.stage("confirm_validate");
        anyhow::ensure!(
            frame.typ == FrameType::Confirm && frame.body.is_none(),
            "confirmation_required"
        );
        let confirmation: Confirm = attempt.check(frame.metadata())?;
        let (connection, outbound) = gateway
            .confirm(
                pending,
                confirmation,
                cancellation.child_token(),
                context.auth_deadline(),
            )
            .await?;
        let owner = SessionOwner {
            gateway: gateway.clone(),
            session_id: connection.session.key.route_session_id.clone(),
        };
        gateway
            .initialize(&connection, context.auth_deadline())
            .await?;
        let ready = Ready {
            attempt_id: connection.attempt_id.clone(),
            session_id: connection.session.key.route_session_id.clone(),
            route_revision: connection.session.key.route_revision.clone(),
            issued_at: now()?,
        };
        let (header, body) = Frame::encode(FrameType::Ready, &ready, None)?;
        attempt.stage("ready_permit");
        let permit = attempt.check(
            crate::application::gateway::outbox::DeliveryCheck::direct(
                connection.session.clone(),
                connection.uid_events.clone(),
            )
            .acquire(context.auth_deadline())
            .await
            .and_then(|permit| permit.ok_or_else(|| anyhow::anyhow!("session_not_ready"))),
        )?;
        attempt.stage("ready_write");
        attempt.check(
            crate::application::gateway::outbox::guard_write(
                Some(&permit),
                gateway
                    .control_output
                    .packet(Outbound::Binary {
                        header,
                        body,
                    })?
                    .write(&mut writer, permit.deadline.min(context.auth_deadline())),
            )
            .await,
        )?;
        drop(permit);
        // Ready 已写出仍需守住物理接入期限，失效连接不能先发布上线事实。
        attempt.stage("transport_admission");
        anyhow::ensure!(context.authenticated(), "authentication_deadline");
        gateway.publish_ready(&connection).await?;
        admission.authenticated();
        Ok((owner, connection, outbound))
    }
    .await;
    let (_owner, connection, mut outbound) = attempt.check(handshake_result)?;
    let cancel = connection.cancellation.clone();
    let writer_done = cancel.clone();
    let writer_session = connection.session.clone();
    let notices = connection
        .session
        .notices
        .get()
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("state_notices_missing"))?;
    let flush = Arc::new(tokio::sync::Notify::new());
    let writer_flush = flush.clone();
    let io_admission = admission.clone();
    context.spawn(async move {
        let _admission = io_admission;
        let healthy = loop {
            tokio::select! {
                biased;
                _ = cancel.cancelled() => break true,
                _ = notices.changed.notified() => {
                    if write_notices(&notices, &mut writer).await.is_err() {
                        break false;
                    }
                }
                _ = writer_flush.notified() => {
                    if writer
                        .send(Outbound::Flush, Instant::now() + Duration::from_secs(5))
                        .await
                        .is_err()
                    {
                        break false;
                    }
                }
                batch = outbound.recv() => {
                    let Some(batch) = batch else {
                        break true;
                    };
                    if batch.write_native(&mut writer).await.is_err() {
                        break false;
                    }
                }
            }
        };
        // writer 也先关闭资格，避免宿主取消先唤醒写任务时遗漏终态；已发生半包失败则不能继续写。
        writer_session.close();
        if healthy {
            let _ = write_notices(&notices, &mut writer).await;
        }
        writer_done.cancel();
    });
    let (incoming, mut inbound) = mpsc::channel(2);
    let cancel = connection.cancellation.clone();
    let maximum_frame = settings.max_frame_bytes;
    let maximum_metadata = settings.max_metadata_bytes;
    let metrics = gateway.metrics.clone();
    let io_admission = admission.clone();
    context.spawn(async move {
        let _admission = io_admission;
        loop {
            let received = tokio::select! {
                biased;
                _ = cancel.cancelled() => break,
                value = reader.receive_bytes(Instant::now() + Duration::from_secs(120)) => value,
            };
            if let Ok(message) = &received {
                metrics.received(message);
            }
            let value = match received {
                Ok(Inbound::Binary(bytes)) => Frame::decode(bytes, maximum_frame, maximum_metadata),
                Ok(Inbound::Ping(_)) => {
                    flush.notify_one();
                    continue;
                }
                Ok(Inbound::Pong(_)) => continue,
                _ => break,
            };
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
    let mut timer = tokio::time::interval(Duration::from_millis(50));
    timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let heartbeat = Arc::new(Mutex::new(HeartbeatState {
        next_ping: Instant::now() + Duration::from_millis(settings.heartbeat_interval_ms),
        outstanding: None,
    }));
    let mut rate_start = Instant::now();
    let mut requests = 0u32;
    loop {
        tokio::select! {
            biased;
            _ = connection.cancellation.cancelled() => break,
            _ = cancellation.cancelled() => break,
            _ = timer.tick() => {
                connection.maintain().await?;
                if connection.session.candidate(None).is_none() {
                    tracing::debug!(
                        reason = connection
                            .session
                            .unavailable_reason()
                            .unwrap_or("session_unavailable"),
                        "socket session eligibility ended"
                    );
                    break;
                }
                let owner = connection.clone();
                let state = heartbeat.clone();
                let timeout = Duration::from_millis(settings.heartbeat_timeout_ms);
                connection.uid_events.submit_control(connection.session.identity().deadline, 65536, move || {
                    state.lock().unwrap_or_else(|error| error.into_inner()).tick(&owner, timeout)
                })?.wait().await?;
            }
            frame = inbound.recv() => {
                let Some(frame) = frame else {
                    break;
                };
                let frame = frame?;
                match frame.typ {
                    FrameType::Pong => {
                        anyhow::ensure!(frame.body.is_none(), "unexpected_body");
                        let pong: Heartbeat = frame.metadata()?;
                        let owner = connection.clone();
                        let state = heartbeat.clone();
                        let timeout = Duration::from_millis(settings.heartbeat_timeout_ms);
                        let interval = Duration::from_millis(settings.heartbeat_interval_ms);
                        connection.uid_events.submit_control(connection.session.identity().deadline, 65536, move || {
                            state.lock().unwrap_or_else(|error| error.into_inner()).pong(&owner, pong, timeout, interval)
                        })?.wait().await?;
                    }
                    FrameType::Receipt => {
                        anyhow::ensure!(frame.body.is_none(), "unexpected_body");
                        let receipt: Receipt = frame.metadata()?;
                        connection.confirm_receipt(receipt).await?;
                    }
                    FrameType::Close => break,
                    FrameType::Event => {
                        if rate_start.elapsed() >= Duration::from_secs(1) {
                            rate_start = Instant::now();
                            requests = 0;
                        }
                        requests = requests.saturating_add(1);
                        anyhow::ensure!(
                            requests <= connection.plan.definition.connections.operations_per_second,
                            "operation_rate"
                        );
                        schedule(&gateway, &connection, &context, frame)?;
                    }
                    _ => anyhow::bail!("unexpected_frame"),
                }
            }
        }
    }
    connection.session.close();
    connection.cancellation.cancel();
    Ok(())
}

/// 业务作用：将操作等待从读取 owner 分离，使较新的取消能够使迟到订阅授权失效。
/// 参数说明：
/// - `gateway`：共享操作服务和节点预算。
/// - `connection`：请求所在的精确会话。
/// - `context`：宿主子任务登记入口。
/// - `frame`：已经完整组装的客户端事件。
///
/// 返回：已登记执行或固定拒绝结果；容量不足不会建立订阅意图。
fn schedule(
    gateway: &Arc<Gateway>,
    connection: &Arc<Connected>,
    context: &ProtocolContext,
    frame: Frame,
) -> anyhow::Result<()> {
    let request: ClientRequest = frame.metadata()?;
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
                // 动作结构拒绝与分区失权分开返回；有效 owner 下的业务拒绝可以回执，失权仍关闭会话。
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
            let queued = connection.enqueue_reply(reply, None)?;
            let owner = connection.clone();
            context.spawn(async move {
                if queued.wait().await.is_err() {
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
                // 排队或执行权威失效只能关闭旧会话，不能在网络任务中绕过分区重做原动作。
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
                connection.session.close();
                connection.cancellation.cancel();
                return;
            }
        };
        let queued = connection.enqueue_reply(reply, None);
        let result = match queued {
            Ok(queued) => queued.wait().await,
            Err(error) => Err(error),
        };
        if result.is_err() {
            connection.session.close();
            connection.cancellation.cancel();
        }
    });
    Ok(())
}

/// 业务作用：在原始握手截止时间内取得唯一业务二进制帧，控制 ping 不重置认证期限。
/// 参数说明：
/// - `reader`：传输独占读取 owner。
/// - `writer`：尚未移交 writer 任务的握手写入 owner。
/// - `maximum_frame`：完整消息预算。
/// - `maximum_metadata`：控制 JSON 预算。
/// - `deadline`：accept 时固定的截止点。
/// - `gateway`：节点固定指标与独立控制预算。
///
/// 返回：合法分区帧；文本、关闭和超时终止连接。
async fn receive(
    reader: &mut Reader,
    writer: &mut Writer,
    maximum_frame: usize,
    maximum_metadata: usize,
    deadline: Instant,
    gateway: &Arc<Gateway>,
) -> anyhow::Result<Frame> {
    loop {
        let message = reader.receive_bytes(deadline).await?;
        gateway.metrics.received(&message);
        match message {
            Inbound::Binary(bytes) => return Frame::decode(bytes, maximum_frame, maximum_metadata),
            Inbound::Ping(_) => {
                gateway
                    .control_output
                    .packet(Outbound::Flush)?
                    .write(writer, deadline)
                    .await?
            }
            Inbound::Pong(_) => {}
            _ => anyhow::bail!("binary_frame_required"),
        }
    }
}

/// 业务作用：按顺序写出已生效的权限状态，关闭路径也只能使用有限控制时间。
/// 参数说明：
/// - `notices`：当前精确会话的有限状态队列。
/// - `writer`：物理连接唯一写入 owner。
///
/// 返回：现有通知全部尝试完成；失败不恢复权限或重放业务包。
async fn write_notices(
    notices: &crate::domain::session::notices::Notices,
    writer: &mut Writer,
) -> anyhow::Result<()> {
    let deadline = Instant::now() + Duration::from_secs(1);
    while let Some(notice) = notices.pop() {
        notice.native()?.write(writer, deadline).await?;
    }
    Ok(())
}

/// 原生心跳只在主体消费域推进，物理 WS ping/pong 不参与身份或订阅授权。
struct HeartbeatState {
    /// 下一次允许发起探测的单调时刻。
    next_ping: Instant,
    /// 当前唯一 nonce 与固定响应期限，不能用其它帧续期。
    outstanding: Option<(String, Instant)>,
}

impl HeartbeatState {
    /// 业务作用：在主体消费域判断心跳期限并登记唯一探测，积压不能延长旧探测期限。
    /// 参数说明：
    /// - `connection`：当前精确路由会话。
    /// - `timeout`：部署冻结的单次响应预算。
    ///
    /// 返回：探测已排队或尚未到期；超时及容量不足要求关闭连接。
    fn tick(&mut self, connection: &Connected, timeout: Duration) -> anyhow::Result<()> {
        anyhow::ensure!(!connection.cancellation.is_cancelled(), "session_closed");
        if self
            .outstanding
            .as_ref()
            .is_some_and(|(_, deadline)| *deadline <= Instant::now())
        {
            connection.metrics.add(
                crate::observability::Counter::Heartbeats,
                "native",
                "timeout",
                1,
            );
            anyhow::bail!("heartbeat_timeout");
        }
        if self.outstanding.is_none() && Instant::now() >= self.next_ping {
            let nonce = control::nonce();
            let deadline = Instant::now() + timeout;
            connection.outbox.enqueue(
                FrameType::Ping,
                &Heartbeat {
                    nonce: nonce.clone(),
                },
                None,
                deadline,
                None,
            )?;
            self.outstanding = Some((nonce, deadline));
        }
        Ok(())
    }

    /// 业务作用：只接受当前探测的及时回应，不把心跳存活解释为身份授权续期。
    /// 参数说明：
    /// - `connection`：回应所属的精确会话。
    /// - `pong`：已解码的固定 nonce。
    /// - `timeout`：原探测使用的固定预算。
    /// - `interval`：下一次探测的间隔。
    ///
    /// 返回：当前探测结束；迟到、重复或不匹配的回应拒绝。
    fn pong(
        &mut self,
        connection: &Connected,
        pong: Heartbeat,
        timeout: Duration,
        interval: Duration,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(self.outstanding.as_ref().is_some_and(|(nonce, deadline)| nonce == &pong.nonce && *deadline > Instant::now()), "unexpected_pong");
        if let Some((_, deadline)) = self.outstanding.take() {
            connection
                .metrics
                .duration("heartbeat_rtt", (deadline - timeout).elapsed());
            connection
                .metrics
                .add(crate::observability::Counter::Heartbeats, "native", "ok", 1);
        }
        self.next_ping = Instant::now() + interval;
        Ok(())
    }
}
