//! 产品运行事实与统一 nafana 出口的有界适配；指标不参与权限裁决。

mod ages;
/// 汇集一次 MQ 批次的局部计数，在交接时归并到节点指标。
pub mod batch;
pub mod broker;
mod catalog;
mod control;
/// 观察终端回执的到达与超时，不将写入连接等同于客户端已处理。
pub mod receipt;
pub mod scopes;
pub mod sessions;
pub use catalog::Counter;
use catalog::*;
pub use control::Control;
use nametrics_core::atomic::{add, render_snapshot, AtomicHistogram};
use nasa::application::{LegacyMetricsSource, MetricDescriptor, MetricSample, MetricValue};
use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

/// 原子计数使用静态目录索引，热路径不保留正文或构造用户标签。
pub struct Metrics {
    /// 持久登记完成后冻结的单一节点身份，不随路由定义和客户端输入增加标签。
    identity: std::sync::OnceLock<Vec<(&'static str, String)>>,
    /// 已验证控制快照和本地有限许可的固定观察，不能用来授予业务权限。
    pub control: control::Control,
    /// 固定资源待决年龄目录，槽容量不足明确输出未知，不改变业务容量。
    ages: Arc<ages::Ages>,
    /// 启动时冻结的详细名称目录；关闭时不创建路由、端点和入口细分系列。
    pub scopes: Arc<scopes::Scopes>,
    /// 按固定指标族及其类别、结局索引保存的累计值，禁止用外部名称扩展。
    counters: Vec<Vec<AtomicU64>>,
    /// 当前真实 owner 持有的资源数量，由相应 Reservation 取得、转移和归还。
    resources: Vec<AtomicU64>,
    /// 固定本地处理阶段的单调时间分布，记录时使用 Duration，导出时使用秒。
    durations: Vec<AtomicHistogram>,
    /// 后台发布的运行目录观察量；时间内部保存毫秒，不能作为业务权限依据。
    projection: Vec<AtomicU64>,
}

/// 实际资源责任；移交所有者时一起移动，取消与异常也能归还 gauge。
pub struct Reservation {
    /// 本资源仍在等待或处理时的年龄份额，完成缓存不继续累计待决年龄。
    age: Option<ages::Lease>,
    /// 与实际资源 owner 同寿命的节点指标来源，归还时仍可访问原资源计数。
    metrics: Arc<Metrics>,
    /// 当前资源在固定 RESOURCES 目录中的索引，状态迁移时随归属一起更新。
    index: usize,
    /// 本责任持有的数量或字节，释放时只归还这一份额。
    amount: u64,
    /// 可选路由细分责任，与节点总量一起迁移并在同一 owner 退出时归还。
    scoped: Option<scopes::Resource>,
}

/// 只观察本地阶段持续时间，取消时保留实际等待长度，不推断完成结果。
pub struct Timer {
    /// 节点共享指标来源，计时器释放时向原来源写入耗时样本。
    metrics: Arc<Metrics>,
    /// 静态阶段名称，只能匹配已登记的耗时目录，不接受客户端自定义标签。
    stage: &'static str,
    /// 本地单调时钟起点；结束时计算 Duration，不参与跨节点时间比较。
    started: Instant,
}

impl Drop for Timer {
    /// 业务作用：阶段责任结束时记录耗时，异常和取消也不会丢失已发生的等待。
    /// 参数说明: 无。
    ///
    /// 返回：追加耗时样本；成功与否由对应业务结果单独表达。
    fn drop(&mut self) {
        self.metrics.duration(self.stage, self.started.elapsed());
    }
}

/// 物理连接从传输未定到实际适配器的唯一观察责任，不把 namespace 当成物理连接。
pub struct Connection {
    /// 接纳与关闭使用同一节点计数源，不因协议归类而创建新来源。
    metrics: Arc<Metrics>,
    /// 当前物理连接的唯一资源责任，从 unknown 迁移到已识别适配器。
    reservation: Reservation,
    /// 已确定的物理协议类别；namespace 不改变此值或重复取得连接份额。
    adapter: &'static str,
    /// 连接被受管 accept 接纳时的单调起点，用于结束后的存活时间分布。
    started: Instant,
}

impl Connection {
    /// 业务作用：在受管 accept 已经接纳连接后登记实际物理责任。
    /// 参数说明：
    /// - `metrics`：节点事实源。
    ///
    /// 返回：尚未确定传输类别的连接观察。
    pub fn new(metrics: Arc<Metrics>) -> Self {
        Self {
            reservation: metrics.reserve("connections_unknown", 1),
            metrics,
            adapter: "unknown",
            started: Instant::now(),
        }
    }

    /// 业务作用：传输协商成功后把物理责任归入唯一真实适配器。
    /// 参数说明：
    /// - `adapter`：服务器确定的固定传输类别。
    ///
    /// 返回：仅首次归类计 accepted，后续 namespace 不重复计物理连接。
    pub fn identify(&mut self, adapter: &'static str) {
        if self.adapter != "unknown" {
            return;
        }
        let resource = match adapter {
            "native_tcp" => "connections_native_tcp",
            "native_ws" => "connections_native_ws",
            "socketio" => "connections_socketio",
            _ => return,
        };
        self.adapter = adapter;
        self.reservation.transfer(resource);
        self.metrics
            .add(Counter::Connections, adapter, "accepted", 1);
    }
}

impl Drop for Connection {
    /// 业务作用：物理连接任务退出时记录真实结束，握手前退出计 rejected。
    /// 参数说明: 无。
    ///
    /// 返回：累计结局和存活时间，资源 gauge 随责任自动归还。
    fn drop(&mut self) {
        self.metrics.add(
            Counter::Connections,
            self.adapter,
            if self.adapter == "unknown" {
                "rejected"
            } else {
                "closed"
            },
            1,
        );
        self.metrics
            .duration("connection_lifetime", self.started.elapsed());
    }
}

/// 业务作用：在受管任务中刷新有限运行目录，抓取请求只读取原子快照。
/// 参数说明：
/// - `application`：napp 生命周期和任务监督入口。
/// - `gateway`：持有静态指标源与运行目录的产品实例。
///
/// 返回：周期任务已登记；关闭优先清除就绪观察，不改变任何业务权限。
pub async fn install(
    application: &nasa::Application,
    gateway: Arc<crate::application::gateway::Gateway>,
) -> anyhow::Result<()> {
    sessions::install(application, gateway.clone()).await?;
    application
        .serve_when_ready("nacket-metrics", move |shutdown| async move {
            let started = Instant::now();
            if let Some(incarnation) = gateway.incarnation.get() {
                gateway.metrics.identify_node(&gateway.configuration, incarnation);
            }
            let mut timer = tokio::time::interval(Duration::from_secs(1));
            timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    biased;
                    _ = shutdown.cancelled() => {
                        gateway.metrics.set("ready", 0);
                        return Ok::<(), anyhow::Error>(());
                    }
                    _ = timer.tick() => {
                        gateway.routes.project_scopes(&gateway.metrics.scopes);
                        if gateway.configuration.control.settings().is_some_and(|settings| settings.role != crate::config::control::Role::Controller) {
                            gateway.routes.project_control_permits(&gateway.metrics.control);
                        }
                        gateway.metrics.set("persistence_jobs", gateway.routes.persistence.active() as u64);
                        gateway.metrics.set("inbound_bytes", gateway.inbound_bytes.used_bytes() as u64);
                        gateway.metrics.set("ingress_retained_bytes", gateway.ingress_bytes.used_bytes() as u64);
                        gateway.metrics.set("ingress_byte_capacity", gateway.ingress_bytes.capacity() as u64);
                        let counts = gateway.routes.observation();
                        for (key, value) in [
                            "routes_online",
                            "routes_protected",
                            "routes_offline",
                            "writers",
                        ]
                        .into_iter()
                        .zip(counts)
                        {
                            gateway.metrics.set(key, value);
                        }
                        let dependencies = gateway.routes.dependency_status();
                        let ready = dependencies
                            .values()
                            .filter(|value| **value == "ready")
                            .count();
                        gateway.metrics.set("dependencies_ready", ready as u64);
                        gateway.metrics.set(
                            "dependencies_unavailable",
                            (dependencies.len() - ready) as u64,
                        );
                        gateway.metrics.set(
                            "uptime_seconds",
                            started.elapsed().as_millis().min(u64::MAX as u128) as u64,
                        );
                    }
                }
            }
        })
        .await?;
    Ok(())
}

impl Metrics {
    /// 业务作用：把已经持久确认的节点代际关联到平台进程身份，禁止运行期替换旧标签。
    /// 参数说明：
    /// - `deployment`：启动固定且已通过校验的节点与角色配置。
    /// - `incarnation`：本地账本或集群权威已经确认的本次节点代际。
    ///
    /// 返回：仅首次建立一组固定身份，后续调用保留原值；不取得控制或写出权限。
    pub fn identify_node(&self, deployment: &crate::config::Deployment, incarnation: &str) {
        use crate::config::control::{Control, Role};
        self.identity.get_or_init(|| {
            let backend = match deployment.control {
                Control::Local => "local",
                Control::Redis {
                    ..
                } => "redis",
                Control::Kafka {
                    ..
                } => "kafka",
            };
            let role = match deployment.control.settings().map(|settings| settings.role) {
                None | Some(Role::Socket) => "socket",
                Some(Role::Combined) => "combined",
                Some(Role::Controller) => "controller",
            };
            vec![
                ("node_id", deployment.node_id.clone()),
                ("incarnation", incarnation.into()),
                ("role", role.into()),
                ("control_backend", backend.into()),
            ]
        });
    }

    /// 业务作用：为没有重复事件计数需要的受管阶段建立单调计时责任。
    /// 参数说明：
    /// - `stage`：固定目录中的本地处理阶段。
    ///
    /// 返回：随实际等待责任移交和释放的计时器。
    pub fn timer(self: &Arc<Self>, stage: &'static str) -> Timer {
        Timer {
            metrics: self.clone(),
            stage,
            started: Instant::now(),
        }
    }
    /// 业务作用：统计已到达业务处理边界的正文模式和字节，重试保持实际入口次数口径。
    /// 参数说明：
    /// - `mode`：报文声明且已经与当前路由比对的模式。
    /// - `size`：共享正文长度，不为计数复制字节。
    ///
    /// 返回：增加固定模式系列；不表示 schema 有效、授权通过或业务完成。
    pub fn payload(&self, mode: u8, size: usize) {
        let mode = match mode {
            0 => "JSON_BYTES",
            1 => "VARINT_TLV",
            2 => "BITPACK_TLV",
            3 => "FAST_FIXED",
            _ => "unknown",
        };
        self.add(Counter::Payloads, mode, "accepted", 1);
        self.add(Counter::PayloadBytes, mode, "accepted", size as u64);
    }

    /// 业务作用：在实际正文复制或编码分配完成处同时记录次数和字节。
    /// 参数说明：
    /// - `boundary`：固定实现边界。
    /// - `size`：该次完整输出字节数。
    ///
    /// 返回：更新显式分配观察，不将共享 Bytes 克隆计为正文复制。
    pub fn payload_copy(&self, boundary: &str, size: usize) {
        self.add(Counter::PayloadCopies, boundary, "ok", 1);
        self.add(Counter::PayloadCopyBytes, boundary, "ok", size as u64);
    }

    /// 业务作用：在正文物化尝试结束后同时记录输入大小及成功或结构拒绝。
    /// 参数说明：
    /// - `purpose`：固定业务目的。
    /// - `size`：物化前的编码字节长度。
    /// - `success`：codec 是否接受该输入。
    ///
    /// 返回：尝试失败也可观测；固定控制 JSON 不进入此口径。
    pub fn payload_decode(&self, purpose: &str, size: usize, success: bool) {
        let outcome = if success {
            "ok"
        } else {
            "invalid"
        };
        self.add(Counter::PayloadDecodes, purpose, outcome, 1);
        self.add(Counter::PayloadDecodeBytes, purpose, outcome, size as u64);
    }

    /// 业务作用：在传输读取完整消息后记录实际接收量，格式不合法的完整包也占用过带宽。
    /// 参数说明：
    /// - `message`：naws 已按容量和组装期限取得的完整消息。
    ///
    /// 返回：累计一次完整帧及其应用可见字节；关闭、半帧与 TCP/TLS 开销不计入。
    pub fn received(&self, message: &nasa::ws::transport::budget::Received) {
        use nasa::ws::transport::budget::Received as Inbound;
        let size = match message {
            Inbound::Binary(bytes) | Inbound::Ping(bytes) | Inbound::Pong(bytes) => bytes.len(),
            Inbound::Text(text) => text.len(),
            Inbound::Closed => return,
        };
        self.add(Counter::TransportBytes, "receive", "ok", size as u64);
        self.add(Counter::TransportFrames, "receive", "ok", 1);
    }

    /// 业务作用：把完整异步检查的结果和取消责任绑定，不把网络成功误当作权限允许。
    /// 参数说明：
    /// - `family`：固定业务领域。
    /// - `subject`：预登记检查类别。
    /// - `stage`：本地耗时阶段。
    /// - `success`：调用者在完成所有校验后采用的固定结局。
    /// - `work`：包含关联、权限和有效期复验的完整操作。
    ///
    /// 返回：原始业务结果不变；取消 future 时记录 canceled，不改变授权或提交行为。
    pub async fn measure<T>(
        self: &Arc<Self>,
        family: Counter,
        subject: &'static str,
        stage: &'static str,
        success: &'static str,
        work: impl std::future::Future<Output = anyhow::Result<T>>,
    ) -> anyhow::Result<T> {
        let mut observation = Operation::new(self.clone(), family, subject, stage);
        let result = work.await;
        observation.finish(
            result
                .as_ref()
                .map(|_| success)
                .unwrap_or_else(|error| outcome(error)),
        );
        result
    }

    /// 业务作用：预分配固定领域的原子单元，不在事件发生时动态创建时间序列。
    /// 参数说明: 无。
    ///
    /// 返回：可登记到统一出口的空事实源。
    pub fn new() -> Arc<Self> {
        Self::configured(&crate::config::metric_scopes::MetricScopes::default())
    }

    /// 业务作用：同时冻结节点总量与详细名称目录，运行期不能重新分配标签身份。
    /// 参数说明：
    /// - `policy`：启动时验证的详细指标开关和受限名称。
    ///
    /// 返回：两个共享同一业务观察位置的指标来源。
    pub fn configured(policy: &crate::config::metric_scopes::MetricScopes) -> Arc<Self> {
        Arc::new(Self {
            identity: std::sync::OnceLock::new(),
            control: control::Control::default(),
            ages: ages::Ages::new(),
            scopes: scopes::Scopes::new(policy),
            counters: COUNTERS
                .iter()
                .map(|(_, subjects)| {
                    (0..subjects.len() * OUTCOMES.len())
                        .map(|_| AtomicU64::new(0))
                        .collect()
                })
                .collect(),
            resources: RESOURCES.iter().map(|_| AtomicU64::new(0)).collect(),
            durations: STAGES
                .iter()
                .map(|_| AtomicHistogram::new(DURATION.histogram_bounds))
                .collect(),
            projection: PROJECTIONS.iter().map(|_| AtomicU64::new(0)).collect(),
        })
    }

    /// 业务作用：登记前计算全部计数和 Histogram 展开的最坏容量。
    /// 参数说明: 无。
    ///
    /// 返回：固定上界，不依赖启动时是否已有连接或路由。
    pub fn worst_case_series(&self) -> usize {
        self.counters.iter().map(Vec::len).sum::<usize>()
            + RESOURCES.len()
            + PROJECTIONS.len()
            + ages::RESOURCES.len() * 2
            + control::Control::worst_case_series()
            + 1
            + STAGES.len() * (DURATION.histogram_bounds.len() + 3)
    }

    /// 业务作用：同时登记节点与路由资源，使详细标签随同一个实际 owner 释放。
    /// 参数说明：
    /// - `route`：当前冻结路由 ID。
    /// - `resource`：固定资源分类。
    /// - `amount`：取得的真实数量。
    ///
    /// 返回：保留两种口径的同一观察责任，不改变业务容量。
    pub fn reserve_route(
        self: &Arc<Self>,
        route: &str,
        resource: &str,
        amount: u64,
    ) -> Reservation {
        let mut reservation = self.reserve(resource, amount);
        reservation.scoped =
            Some(
                self.scopes
                    .reserve(scopes::Kind::Route, route, resource, amount),
            );
        reservation
    }

    /// 业务作用：在实际业务动作处记录有限分类的累计事实，不改变动作结果。
    /// 参数说明：
    /// - `family`：固定领域。
    /// - `subject`：领域内预登记类别，不使用客户端任意事件名。
    /// - `outcome`：固定结局分类。
    /// - `amount`：本次实际发生次数或字节量。
    ///
    /// 返回：已知分类饱和累加；目录之外的输入不增加序列。
    pub fn add(&self, family: Counter, subject: &str, outcome: &str, amount: u64) {
        let family = family as usize;
        if let (Some(subject), Some(outcome)) = (
            COUNTERS[family]
                .1
                .iter()
                .position(|value| *value == subject),
            OUTCOMES.iter().position(|value| *value == outcome),
        ) {
            add(
                &self.counters[family][subject * OUTCOMES.len() + outcome],
                amount,
            );
        }
    }

    /// 业务作用：让资源使用量与实际 owner 生命周期一致，取消不会遗留加一记录。
    /// 参数说明：
    /// - `resource`：固定资源类别。
    /// - `amount`：此次取得的真实资源量。
    ///
    /// 返回：必须随 owner 保存的责任；类别错误属于产品内部合同错误。
    pub fn reserve(self: &Arc<Self>, resource: &str, amount: u64) -> Reservation {
        let index = RESOURCES
            .iter()
            .position(|value| *value == resource)
            .expect("fixed resource catalog");
        add(&self.resources[index], amount);
        Reservation {
            age: (amount > 0).then(|| self.ages.acquire(resource)).flatten(),
            metrics: self.clone(),
            index,
            amount,
            scoped: None,
        }
    }

    /// 业务作用：记录明确起止点的本地耗时，不为指标创建远程调用。
    /// 参数说明：
    /// - `stage`：固定业务阶段。
    /// - `duration`：同一进程单调时钟计算的实际时间。
    ///
    /// 返回：更新唯一非累积桶及微秒总量。
    pub fn duration(&self, stage: &str, duration: Duration) {
        if let Some(index) = STAGES.iter().position(|value| *value == stage) {
            self.durations[index].observe(duration);
        }
    }

    /// 业务作用：发布从有限运行目录取得的观察量，观察不能反过来授权业务。
    /// 参数说明：
    /// - `state`：固定状态名称。
    /// - `value`：实际当前数量或以毫秒保存的时间。
    ///
    /// 返回：替换观察值；时间样本由出口转换为秒。
    pub fn set(&self, state: &str, value: u64) {
        if let Some(index) = PROJECTIONS.iter().position(|name| *name == state) {
            self.projection[index].store(value, Ordering::Relaxed);
        }
    }
}

/// 业务作用：把本地错误收敛为有限的监控分类，原始错误文本不进入标签。
/// 参数说明：
/// - `error`：已由业务操作产生的失败，不参与重新裁决。
///
/// 返回：固定结局；没有明确可用性证据的结构或关联失败保守计 invalid。
pub fn outcome(error: &anyhow::Error) -> &'static str {
    if error.is::<tokio::time::error::Elapsed>() {
        return "timeout";
    }
    if error.is::<std::io::Error>()
        || error.is::<crate::application::gateway::dispatch::Deferred>()
        || error.is::<nasa::application::ApplicationError>()
    {
        return "unavailable";
    }
    let code = error.to_string();
    if code.ends_with("_unknown") {
        "unknown"
    } else if code.contains("timeout") || code.ends_with("_deadline") {
        "timeout"
    } else if code.ends_with("_expired") || code.ends_with("_stale") {
        "expired"
    } else if code.contains("unavailable") {
        "unavailable"
    } else if code.contains("denied") || code.ends_with("_revoked") {
        "deny"
    } else if code.contains("capacity") || code.ends_with("_exhausted") {
        "rejected"
    } else if code.ends_with("_conflict") || code.ends_with("_superseded") {
        "conflict"
    } else {
        "invalid"
    }
}

impl Reservation {
    /// 业务作用：结束真实待决年龄但继续保留记录容量，完成结果缓存不能被算成仍在执行。
    /// 参数说明: 无。
    ///
    /// 返回：归还年龄责任；数量及字节 gauge 仍由原 owner 持有到实际释放。
    pub fn finish_pending(&mut self) {
        self.age.take();
    }

    /// 业务作用：在同一资源责任进入下一状态时转移观察量，不能复制资源份额。
    /// 参数说明：
    /// - `resource`：目标固定资源类别。
    ///
    /// 返回：原类别减少、新类别增加；总责任仍由同一 owner 持有。
    pub fn transfer(&mut self, resource: &str) {
        if let Some(scoped) = &mut self.scoped {
            scoped.transfer(resource);
        }
        let index = RESOURCES
            .iter()
            .position(|name| *name == resource)
            .expect("fixed resource catalog");
        if index != self.index {
            self.age = (self.amount > 0)
                .then(|| self.metrics.ages.acquire(resource))
                .flatten();
            self.metrics.resources[self.index].fetch_sub(self.amount, Ordering::Relaxed);
            add(&self.metrics.resources[index], self.amount);
            self.index = index;
        }
    }
}
impl Drop for Reservation {
    /// 业务作用：实际资源退出时同步归还观察量。
    /// 参数说明: 无。
    ///
    /// 返回：只归还本 owner 持有的份额，不影响业务容量信号量。
    fn drop(&mut self) {
        self.metrics.resources[self.index].fetch_sub(self.amount, Ordering::Relaxed);
    }
}

/// 异步操作的计时与取消责任；只有明确结果才能覆盖默认 canceled。
pub struct Operation {
    /// 当前节点的固定指标目录，在完成或取消时统一记账。
    metrics: Arc<Metrics>,
    /// 本次操作所属的固定业务指标族。
    family: Counter,
    /// 指标族内的静态操作名称，不保留客户端任意事件名。
    subject: &'static str,
    /// 与本次实际等待对应的固定耗时阶段。
    stage: &'static str,
    /// 操作首次开始的单调起点，排队和网络等待都占用同一计时区间。
    started: Instant,
    /// 默认 canceled；只有取得明确结果才覆盖，释放时统计一次。
    outcome: &'static str,
}
impl Operation {
    /// 业务作用：在操作真正开始时记录起点，取消 future 后仍能记入未完成结局。
    /// 参数说明：
    /// - `metrics`：节点固定事实源。
    /// - `family`：业务领域。
    /// - `subject`：预登记的操作类别。
    /// - `stage`：耗时所在的固定阶段。
    ///
    /// 返回：默认 canceled 的生命周期责任。
    pub fn new(
        metrics: Arc<Metrics>,
        family: Counter,
        subject: &'static str,
        stage: &'static str,
    ) -> Self {
        Self {
            metrics,
            family,
            subject,
            stage,
            started: Instant::now(),
            outcome: "canceled",
        }
    }

    /// 业务作用：在实际操作得到确定结局后替换取消分类。
    /// 参数说明：
    /// - `outcome`：有限目录中的实际结局。
    ///
    /// 返回：责任释放时只统计一次该结局。
    pub fn finish(&mut self, outcome: &'static str) {
        self.outcome = outcome;
    }
}
impl Drop for Operation {
    /// 业务作用：记录操作的唯一终态及实际持续时间。
    /// 参数说明: 无。
    ///
    /// 返回：追加一个累计事件和一个耗时样本，不进行网络操作。
    fn drop(&mut self) {
        self.metrics.add(self.family, self.subject, self.outcome, 1);
        self.metrics.duration(self.stage, self.started.elapsed());
    }
}

impl LegacyMetricsSource for Metrics {
    /// 业务作用：向 nafana 统一目录提供产品事实合同。
    /// 参数说明: 无。
    ///
    /// 返回：固定描述符，不随运行用户或消息变化。
    fn descriptors(&self) -> &'static [&'static MetricDescriptor] {
        catalog::descriptors()
    }

    /// 业务作用：从预分配原子单元生成有限快照，不扫描会话或查询 broker。
    /// 参数说明: 无。
    ///
    /// 返回：同一来源的结构化样本，Histogram 数量与本轮桶之和一致。
    fn snapshot(&self) -> Option<Vec<MetricSample>> {
        let mut samples = Vec::new();
        if let Some(labels) = self.identity.get() {
            samples.push(MetricSample {
                name: NODE.name,
                labels: labels.clone(),
                value: MetricValue::Gauge(1.0),
            });
        }
        for (index, (descriptor, subjects)) in COUNTERS.iter().enumerate() {
            for (subject_index, subject) in subjects.iter().enumerate() {
                for (outcome_index, outcome) in OUTCOMES.iter().enumerate() {
                    samples.push(MetricSample {
                        name: descriptor.name,
                        labels: vec![
                            (descriptor.label_names[0], (*subject).into()),
                            ("outcome", (*outcome).into()),
                        ],
                        value: MetricValue::Counter(
                            self.counters[index][subject_index * OUTCOMES.len() + outcome_index]
                                .load(Ordering::Relaxed),
                        ),
                    });
                }
            }
        }
        for (index, resource) in RESOURCES.iter().enumerate() {
            samples.push(MetricSample {
                name: LIVE.name,
                labels: vec![("resource", (*resource).into())],
                value: MetricValue::Gauge(self.resources[index].load(Ordering::Relaxed) as f64),
            });
        }
        for (index, stage) in STAGES.iter().enumerate() {
            samples.push(MetricSample {
                name: DURATION.name,
                labels: vec![("stage", (*stage).into())],
                value: self.durations[index].snapshot(),
            });
        }
        for (index, state) in PROJECTIONS.iter().enumerate() {
            let value = self.projection[index].load(Ordering::Relaxed) as f64;
            samples.push(MetricSample {
                name: PROJECTION.name,
                labels: vec![("state", (*state).into())],
                value: MetricValue::Gauge(if state.ends_with("_seconds") {
                    value / 1000.0
                } else {
                    value
                }),
            });
        }
        for (resource, age) in ages::RESOURCES.iter().zip(self.ages.snapshot()) {
            samples.push(MetricSample {
                name: AGE_COMPLETE.name,
                labels: vec![("resource", (*resource).into())],
                value: MetricValue::Gauge(if age.is_some() {
                    1.0
                } else {
                    0.0
                }),
            });
            if let Some(age) = age {
                samples.push(MetricSample {
                    name: AGE.name,
                    labels: vec![("resource", (*resource).into())],
                    value: MetricValue::Gauge(age.as_secs_f64()),
                });
            }
        }
        samples.extend(self.control.snapshot());
        Some(samples)
    }

    /// 业务作用：提供同源文本出口，受管运行仍由 nafana 统一编码。
    /// 参数说明：
    /// - `output`：追加 Prometheus 文本的目标。
    ///
    /// 返回：完整静态目录及结构化快照的文本，不创建第二个 registry。
    fn render_prometheus(&self, output: &mut String) {
        render_snapshot(
            catalog::descriptors(),
            &self.snapshot().unwrap_or_default(),
            output,
        );
    }
}
