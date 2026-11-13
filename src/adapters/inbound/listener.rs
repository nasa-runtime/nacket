//! 有界独立端口分配；实际 bind 成功才取得端口所有权。

use std::collections::BTreeSet;
use std::io;
use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::time::Instant;

/// 部署批准的端口范围与总 listener 预算。
pub struct ListenerPool {
    /// 本模板真实监听的本地 IP，不代表外部可访问地址。
    address: IpAddr,
    /// 自动端口搜索的包含式起点，候选必须实际 bind 成功。
    start: u16,
    /// 自动端口搜索的包含式终点，耗尽不能回退到共享端口。
    end: u16,
    /// 允许同时保留的独立监听数量，准备和排空阶段仍占额。
    maximum: usize,
    /// 部署已保留的共享、管理和监控端口，自动分配必须跳过。
    reserved: BTreeSet<u16>,
    /// 本分配器仍由实际 owner 持有的端口集合，释放责任前不能重复分配。
    assigned: Mutex<BTreeSet<u16>>,
}

/// 持续占用端口的真实 listener 和配额凭据。
pub struct BoundListener {
    /// 已经 bind 成功的真实 TCP socket，避免探测后重新绑定的竞争。
    listener: TcpListener,
    /// 与真实 socket 同生命周期的端口和容量预约。
    reservation: PortReservation,
}

/// 与 listener 生命周期一起保存；释放后允许后续尝试重新 bind。
pub struct PortReservation {
    /// 归还端口登记的原分配器，保持模板容量一致。
    pool: Arc<ListenerPool>,
    /// 本次实际预约的精确端口，不按路由变更重新推算。
    port: u16,
}

impl ListenerPool {
    /// 业务作用：冻结部署允许的独立端口范围，保留管理和共享入口。
    /// 参数说明：
    ///
    /// - `address`：部署批准的监听 IP。
    /// - `range`：自动分配端口的闭区间。
    /// - `maximum`：同时保留的独立 listener 上限。
    /// - `reserved`：共享、管理、监控等入口已经保留的端口。
    ///
    /// 返回：有效的分配器；拒绝零端口、逆序范围与无容量配置，不产生网络副作用。
    pub fn new(
        address: IpAddr,
        range: (u16, u16),
        maximum: usize,
        reserved: BTreeSet<u16>,
    ) -> io::Result<Arc<Self>> {
        if range.0 == 0 || range.0 > range.1 || maximum == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid listener allocation bounds",
            ));
        }
        Ok(Arc::new(Self {
            address,
            start: range.0,
            end: range.1,
            maximum,
            reserved,
            assigned: Mutex::new(BTreeSet::new()),
        }))
    }

    /// 业务作用：直接绑定指定端口或从范围起点逐个绑定，避免探测后重新打开的竞争窗口。
    /// 参数说明：
    ///
    /// - `requested`：指定端口；None 表示在部署范围内自动分配。
    /// - `fixed_ports`：完整路由快照中指定的端口，自动分配必须为尚未准备的路由保留。
    /// - `budget`：整轮端口分配共享的时间预算。
    ///
    /// 返回：持有真实 socket 的绑定；明确端口失败不改用其它端口，自动模式仅跳过地址占用。
    pub async fn bind(
        self: &Arc<Self>,
        requested: Option<u16>,
        fixed_ports: &BTreeSet<u16>,
        budget: Duration,
    ) -> io::Result<BoundListener> {
        let deadline = Instant::now().checked_add(budget).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "invalid listener deadline")
        })?;
        if let Some(port) = requested {
            if port == 0 || self.reserved.contains(&port) {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "requested port is reserved",
                ));
            }
        }
        let range = requested
            .map(|port| port..=port)
            .unwrap_or(self.start..=self.end);
        for port in range {
            if Instant::now() >= deadline {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "listener allocation deadline exceeded",
                ));
            }
            // 自动分配不能抢占同一快照中其它路由的指定端口，显式请求仍需实际 bind 取得所有权。
            if self.reserved.contains(&port) || (requested.is_none() && fixed_ports.contains(&port))
            {
                continue;
            }
            let reservation = {
                let mut assigned = self.assigned.lock().unwrap_or_else(|e| e.into_inner());
                if assigned.len() >= self.maximum {
                    return Err(io::Error::other("dedicated listener capacity exhausted"));
                }
                if !assigned.insert(port) {
                    if requested.is_some() {
                        return Err(io::Error::new(
                            io::ErrorKind::AddrInUse,
                            "requested port is already assigned",
                        ));
                    }
                    continue;
                }
                PortReservation {
                    pool: self.clone(),
                    port,
                }
            };
            // 预约失败或 future 被取消时，凭据释放；成功后 socket 始终由调用方持有。
            match tokio::time::timeout_at(
                deadline,
                TcpListener::bind(SocketAddr::new(self.address, port)),
            )
            .await
            {
                Ok(Ok(listener)) => {
                    return Ok(BoundListener {
                        listener,
                        reservation,
                    })
                }
                Ok(Err(error))
                    if requested.is_none() && error.kind() == io::ErrorKind::AddrInUse => {}
                Ok(Err(error)) => return Err(error),
                Err(_) => {
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "listener bind deadline exceeded",
                    ))
                }
            }
        }
        Err(io::Error::new(
            io::ErrorKind::AddrNotAvailable,
            "dedicated port range exhausted",
        ))
    }

    /// 业务作用：提供当前持有的独立 listener 配额数量。
    /// 参数说明：无。
    ///
    /// 返回：本分配器内尚未释放的端口预约数，不推断其它进程的使用量。
    pub fn allocated(&self) -> usize {
        self.assigned
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .len()
    }
}

impl BoundListener {
    /// 业务作用：返回已经绑定的真实地址，供路由状态与外部地址准备使用。
    /// 参数说明：无。
    ///
    /// 返回：内核确认的本地地址；不表示外部映射或路由已经就绪。
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.listener.local_addr()
    }

    /// 业务作用：把 socket 与预约一起转交受管服务，避免先释放再重新 bind。
    /// 参数说明：无。
    ///
    /// 返回：调用方负责持有预约直到 listener 关闭，并等待已接入任务完成。
    pub fn into_parts(self) -> (TcpListener, PortReservation) {
        (self.listener, self.reservation)
    }
}

impl Drop for PortReservation {
    /// 业务作用：释放已结束 listener 的端口配额。
    /// 参数说明：无。
    ///
    /// 返回：移除本次端口预约；实际 socket 是否关闭仍由其唯一 owner 负责。
    fn drop(&mut self) {
        self.pool
            .assigned
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&self.port);
    }
}
