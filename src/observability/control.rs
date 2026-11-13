use crate::domain::control::{Ledger, Phase};
use nasa::application::{MetricDescriptor, MetricKind, MetricSample, MetricValue};
use std::{
    collections::BTreeSet,
    sync::{
        atomic::{AtomicBool, Ordering},
        Mutex,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

const STATES: &[&str] = &[
    "nodes_active",
    "nodes_serving",
    "routes",
    "permits_retained",
    "operations_quiescing",
    "operations_preparing",
    "operations_activating",
    "authorization_operations_pending",
    "barrier_permits",
    "barrier_nodes",
];

pub(super) static COUNTS: MetricDescriptor = MetricDescriptor {
    name: "nacket_control_snapshot",
    help: "最近有效持久快照的集群控制数量；各节点是重复观察，不能相加。",
    unit: "",
    kind: MetricKind::Gauge,
    label_names: &["state"],
    histogram_bounds: &[],
};
pub(super) static FRESH: MetricDescriptor = MetricDescriptor {
    name: "nacket_control_snapshot_fresh",
    help: "最近控制快照是否仍在有限观察期限内且未经历读取失败，不提供业务控制权。",
    unit: "",
    kind: MetricKind::Gauge,
    label_names: &[],
    histogram_bounds: &[],
};
pub(super) static AGE: MetricDescriptor = MetricDescriptor {
    name: "nacket_control_snapshot_age_seconds",
    help: "距最近完整验证控制快照的本地单调时间，未知时不输出。",
    unit: "seconds",
    kind: MetricKind::Gauge,
    label_names: &[],
    histogram_bounds: &[],
};
pub(super) static PENDING: MetricDescriptor = MetricDescriptor {
    name: "nacket_control_oldest_operation_age_seconds",
    help: "持久路由及授权操作中最旧未完成操作的诊断年龄，跨节点墙钟不用于屏障裁决。",
    unit: "seconds",
    kind: MetricKind::Gauge,
    label_names: &[],
    histogram_bounds: &[],
};
pub(super) static LOCAL: MetricDescriptor = MetricDescriptor {
    name: "nacket_control_local_routes",
    help: "本节点期望在线且需要集群控制许可的路由数，不表示已获准接流。",
    unit: "",
    kind: MetricKind::Gauge,
    label_names: &[],
    histogram_bounds: &[],
};
pub(super) static PERMIT: MetricDescriptor = MetricDescriptor {
    name: "nacket_control_permit_min_remaining_seconds",
    help: "本节点期望在线路由中最小剩余控制许可时间；缺少许可或门禁关闭为零。",
    unit: "seconds",
    kind: MetricKind::Gauge,
    label_names: &[],
    histogram_bounds: &[],
};

pub(super) static OWNER_COUNTS: MetricDescriptor = MetricDescriptor {
    name: "nacket_control_authority_snapshot",
    help: "有效控制 owner 最近确认的持久目录数量；跨采集时刻仍需去重，不能累加节点副本。",
    unit: "",
    kind: MetricKind::Gauge,
    label_names: &["state"],
    histogram_bounds: &[],
};
pub(super) static OWNER_FRESH: MetricDescriptor = MetricDescriptor {
    name: "nacket_control_authority_fresh",
    help: "本进程是否同时具有有限控制资格证据和新鲜已提交目录；备用及数据节点为零。",
    unit: "",
    kind: MetricKind::Gauge,
    label_names: &[],
    histogram_bounds: &[],
};
pub(super) static OWNER_AGE: MetricDescriptor = MetricDescriptor {
    name: "nacket_control_authority_age_seconds",
    help: "有效 owner 最近完整确认持久目录后的单调时间，不因抓取刷新。",
    unit: "seconds",
    kind: MetricKind::Gauge,
    label_names: &[],
    histogram_bounds: &[],
};
pub(super) static OWNER_REMAINING: MetricDescriptor = MetricDescriptor {
    name: "nacket_control_authority_remaining_seconds",
    help: "当前 owner 观察证据的剩余有限寿命，不授予实际控制权限。",
    unit: "seconds",
    kind: MetricKind::Gauge,
    label_names: &[],
    histogram_bounds: &[],
};
pub(super) static OWNER_PENDING: MetricDescriptor = MetricDescriptor {
    name: "nacket_control_authority_oldest_operation_age_seconds",
    help: "有效 owner 持久目录中最旧未完成操作的诊断年龄，不能用于屏障裁决。",
    unit: "seconds",
    kind: MetricKind::Gauge,
    label_names: &[],
    histogram_bounds: &[],
};

struct AuthorityView {
    /// 从已提交账本计算的固定目录，禁止把尚未提交的事务候选发布给监控。
    view: View,
    /// 从真正权威请求起点计算的截止时间，网络等待和抓取不能延长它。
    deadline: Instant,
    /// Kafka 分配与事务资格句柄；Redis 使用已取得的有限 owner 期限。
    kafka: Option<(nasa::kafka::TransactionHandle, u64)>,
}

impl AuthorityView {
    /// 业务作用：在每次导出前收窄权威观察，Kafka 分配撤销无需等待下一轮采样。
    /// 参数说明: 无。
    ///
    /// 返回：原观察期限尚有效且当前分配未撤销；不发起网络请求。
    fn valid(&self) -> bool {
        if !self.view.available || Instant::now() >= self.deadline {
            return false;
        }
        self.kafka.as_ref().is_none_or(|(handle, epoch)| {
            let status = handle.status();
            status.ready && status.failure.is_none() && status.assignment_epoch == *epoch
        })
    }
}

struct View {
    /// 该完整快照验证完成的单调时刻，观察不能因抓取而刷新。
    observed: Instant,
    /// 观察允许保持有效的有限时长，与真正业务许可分别核对。
    ttl: Duration,
    /// 最近读取是否明确失败，失败后旧数量不能继续作为当前事实展示。
    available: bool,
    /// 由同一持久账本计算的固定数量目录，不保留原账本或业务正文。
    counts: [u64; STATES.len()],
    /// 观察时最旧待决操作的诊断年龄；墙钟异常或未来时间使其未知。
    oldest: Option<Duration>,
    /// 当前快照是否存在待决操作，空闲时年龄保持零而不随观察时间增长。
    pending: bool,
}

struct Local {
    /// 本次本地路由目录投影的单调时刻。
    observed: Instant,
    /// 期望在线且需要集群许可的路由数量。
    routes: usize,
    /// 该观察时刻最小剩余许可寿命，抓取时继续扣除已过去时间。
    remaining: Duration,
}

/// 固定大小控制观察，不发起额外 broker 请求，也不参与许可和屏障决定。
#[derive(Default)]
pub struct Control {
    /// 当前部署是否装配集群控制，关闭时不生成伪造的控制不可用样本。
    enabled: AtomicBool,
    /// 最近完整持久账本的有限观察，缺失时不生成虚假的零数量。
    view: Mutex<Option<View>>,
    /// 集群接入节点本地许可观察，本地模式保持缺失。
    local: Mutex<Option<Local>>,
    /// 仅当前有控制资格的进程发布的权威观察，失权或证据到期后停止输出数量。
    authority: Mutex<Option<AuthorityView>>,
}

impl Control {
    /// 业务作用：由已取得 Redis 有限 owner 的控制器发布完整持久目录。
    /// 参数说明：
    /// - `ledger`：当前 owner 读取并验证的完整已提交账本。
    /// - `deadline`：从取得或续期请求开始计算且扣除时钟余量的本地截止点。
    ///
    /// 返回：替换权威观察；已到期证据不会生成目录数量，不能续发业务许可。
    pub fn redis_owner(&self, ledger: &Ledger, deadline: Instant) {
        *self
            .authority
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = Some(AuthorityView {
            view: project(ledger, deadline.saturating_duration_since(Instant::now())),
            deadline,
            kafka: None,
        });
    }

    /// 业务作用：由真实事务 owner 发布已提交且属于本进程的完整目录，并保留分配失效检查。
    /// 参数说明：
    /// - `ledger`：read_committed 读取且提交者已匹配本次启动的账本。
    /// - `deadline`：本次观测命令开始时冻结的有限证据截止点。
    /// - `handle`：真正执行事务的受管句柄，只同步查询本地资格。
    /// - `epoch`：观测命令开始时的分配代次，后续换代不能延用旧样本。
    ///
    /// 返回：替换权威观察；分配改变、停止或期限耗尽后立即省略数量。
    pub fn kafka_owner(
        &self,
        ledger: &Ledger,
        deadline: Instant,
        handle: nasa::kafka::TransactionHandle,
        epoch: u64,
    ) {
        *self
            .authority
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = Some(AuthorityView {
            view: project(ledger, deadline.saturating_duration_since(Instant::now())),
            deadline,
            kafka: Some((handle, epoch)),
        });
    }

    /// 业务作用：在失权、关闭或未知控制结果时撤回权威观察，不让后台退避保留旧目录。
    /// 参数说明: 无。
    ///
    /// 返回：下次抓取明确输出权威不可用，原节点观察不因此被误认为控制 owner。
    pub fn owner_unavailable(&self) {
        if let Some(view) = self
            .authority
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .as_mut()
        {
            view.view.available = false;
        }
    }

    /// 业务作用：在集群控制组件装配时启用观测，本地模式不假装存在集群控制权威。
    /// 参数说明: 无。
    ///
    /// 返回：后续快照输出控制新鲜度，尚未观察到权威时明确为零。
    pub fn enable(&self) {
        self.enabled.store(true, Ordering::Release);
    }

    /// 业务作用：从已经验证的持久快照计算控制屏障数量，禁止额外读取 broker 来生成指标。
    /// 参数说明：
    /// - `ledger`：调用方已完成身份、作用域及高水位检查的完整账本。
    /// - `ttl`：本轮观察允许使用的最长本地时长。
    ///
    /// 返回：替换固定观察；不改变任何节点、路由或操作状态。
    pub fn observe(&self, ledger: &Ledger, ttl: Duration) {
        // 只在全部数量来自同一已提交快照后发布；读取失败或超期时出口省略旧数量。
        *self.view.lock().unwrap_or_else(|error| error.into_inner()) = Some(project(ledger, ttl));
    }

    /// 业务作用：读取控制权威失败时立即使旧观察失效，不把旧数量当作当前健康状态。
    /// 参数说明: 无。
    ///
    /// 返回：保留最近观察时间供诊断，但不再输出旧快照数量和操作年龄。
    pub fn unavailable(&self) {
        if let Some(view) = self
            .view
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .as_mut()
        {
            view.available = false;
        }
    }

    /// 业务作用：发布本地全部期望在线路由的最小控制许可寿命，不取得或续发许可。
    /// 参数说明：
    /// - `routes`：本节点需要集群许可的在线期望路由数量。
    /// - `remaining`：所有这些门禁复验后的最小剩余时长。
    ///
    /// 返回：替换本地观察；没有对应路由时出口不生成最小许可样本。
    pub fn permits(&self, routes: usize, remaining: Duration) {
        *self.local.lock().unwrap_or_else(|error| error.into_inner()) = Some(Local {
            observed: Instant::now(),
            routes,
            remaining,
        });
    }

    /// 业务作用：读取固定控制观察并执行新鲜度收窄，抓取不访问业务目录或 broker。
    /// 参数说明: 无。
    ///
    /// 返回：只为有效快照输出集群数量；有限本地许可时间继续减少，不因重复抓取增长。
    pub fn snapshot(&self) -> Vec<MetricSample> {
        if !self.enabled.load(Ordering::Acquire) {
            return Vec::new();
        }
        let mut samples = Vec::new();
        let view = self.view.lock().unwrap_or_else(|error| error.into_inner());
        let fresh = view
            .as_ref()
            .is_some_and(|view| view.available && view.observed.elapsed() < view.ttl);
        samples.push(gauge(
            &FRESH,
            if fresh {
                1.0
            } else {
                0.0
            },
        ));
        if let Some(view) = view.as_ref() {
            let elapsed = view.observed.elapsed();
            samples.push(gauge(&AGE, elapsed.as_secs_f64()));
            if fresh {
                for (state, value) in STATES.iter().zip(view.counts) {
                    let mut sample = gauge(&COUNTS, value as f64);
                    sample.labels.push(("state", (*state).into()));
                    samples.push(sample);
                }
                if let Some(age) = view.oldest {
                    samples.push(gauge(
                        &PENDING,
                        if view.pending {
                            age.saturating_add(elapsed).as_secs_f64()
                        } else {
                            0.0
                        },
                    ));
                }
            }
        }
        drop(view);
        let authority = self
            .authority
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let fresh = authority.as_ref().is_some_and(AuthorityView::valid);
        samples.push(gauge(
            &OWNER_FRESH,
            if fresh {
                1.0
            } else {
                0.0
            },
        ));
        if let Some(authority) = authority.as_ref().filter(|_| fresh) {
            let elapsed = authority.view.observed.elapsed();
            for (state, value) in STATES.iter().zip(authority.view.counts) {
                let mut sample = gauge(&OWNER_COUNTS, value as f64);
                sample.labels.push(("state", (*state).into()));
                samples.push(sample);
            }
            samples.push(gauge(&OWNER_AGE, elapsed.as_secs_f64()));
            samples.push(gauge(
                &OWNER_REMAINING,
                authority
                    .deadline
                    .saturating_duration_since(Instant::now())
                    .as_secs_f64(),
            ));
            if let Some(age) = authority.view.oldest {
                samples.push(gauge(
                    &OWNER_PENDING,
                    if authority.view.pending {
                        age.saturating_add(elapsed).as_secs_f64()
                    } else {
                        0.0
                    },
                ));
            }
        }
        drop(authority);
        if let Some(local) = self
            .local
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .as_ref()
        {
            samples.push(gauge(&LOCAL, local.routes as f64));
            if local.routes != 0 {
                samples.push(gauge(
                    &PERMIT,
                    local
                        .remaining
                        .saturating_sub(local.observed.elapsed())
                        .as_secs_f64(),
                ));
            }
        }
        samples
    }

    /// 业务作用：为全部固定控制观察预留最大展开容量，初次无快照也不能少算预算。
    /// 参数说明: 无。
    ///
    /// 返回：节点与权威各十类数量，以及九项固定时间或可用性系列的总上界。
    pub fn worst_case_series() -> usize {
        2 * STATES.len() + 9
    }
}

/// 业务作用：构造固定控制描述符的无标签数值样本，不从业务数据生成名称。
/// 参数说明：
/// - `descriptor`：固定指标合同。
/// - `value`：实际数量或秒时间。
///
/// 返回：可由统一出口编码的结构化样本。
fn gauge(descriptor: &'static MetricDescriptor, value: f64) -> MetricSample {
    MetricSample {
        name: descriptor.name,
        labels: Vec::new(),
        value: MetricValue::Gauge(value),
    }
}

/// 业务作用：从单一已验证账本提取固定目录，节点观察与权威观察保持同一数量定义。
/// 参数说明：
/// - `ledger`：已经完成持久来源验证的账本。
/// - `ttl`：该观察允许保持新鲜的有限时间。
///
/// 返回：独立于业务状态的有限数量和诊断年龄，不保留原账本。
fn project(ledger: &Ledger, ttl: Duration) -> View {
    let mut counts = [0u64; STATES.len()];
    counts[0] = ledger.nodes.values().filter(|node| node.active).count() as u64;
    counts[1] = ledger
        .nodes
        .values()
        .filter(|node| node.active && node.serving)
        .count() as u64;
    counts[2] = ledger.routes.len() as u64;
    counts[3] = ledger.permits.len() as u64;
    let mut permits = BTreeSet::new();
    let mut nodes = BTreeSet::new();
    let mut oldest = None;
    for operation in ledger.operations.values() {
        let index = match operation.phase {
            Phase::Quiescing => 4,
            Phase::Preparing => 5,
            Phase::Activating => 6,
            Phase::Effective | Phase::Superseded => continue,
        };
        counts[index] += 1;
        oldest = Some(oldest.map_or(
            operation.created_at,
            |value: crate::domain::message::UnixMillis| value.min(operation.created_at),
        ));
        permits.extend(operation.barrier.iter());
        if operation.phase == Phase::Preparing {
            nodes.extend(operation.targets.difference(&operation.prepared));
        } else if operation.phase == Phase::Activating {
            nodes.extend(operation.targets.difference(&operation.applied));
        }
    }
    for operation in ledger
        .authorizations
        .values()
        .flat_map(|state| state.operations.values())
    {
        if operation.completed_at.is_some() {
            continue;
        }
        counts[7] += 1;
        oldest = Some(oldest.map_or(operation.accepted_at, |value| {
            value.min(operation.accepted_at)
        }));
        permits.extend(operation.barrier.iter());
    }
    for permit in &permits {
        if let Some(permit) = ledger.permits.get(*permit) {
            nodes.insert(&permit.node_id);
        }
    }
    counts[8] = permits.len() as u64;
    counts[9] = nodes.len() as u64;
    let age = match oldest {
        None => Some(Duration::ZERO),
        Some(oldest) => SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .ok()
            .and_then(|now| now.checked_sub(Duration::from_millis(oldest.as_millis()))),
    };
    View {
        observed: Instant::now(),
        ttl,
        available: true,
        counts,
        oldest: age,
        pending: oldest.is_some(),
    }
}
