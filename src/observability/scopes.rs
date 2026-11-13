//! 启动固定的路由、远程端点和入口指标目录；未列出的名称进入同一个保留聚合桶。

use super::catalog::OUTCOMES;
use nametrics_core::atomic::{add, render_snapshot, AtomicHistogram};
use nasa::application::{
    LegacyMetricsSource, MetricDescriptor, MetricKind, MetricSample, MetricValue,
};
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Instant,
};

#[derive(Clone, Copy)]
#[repr(usize)]
/// 详细指标的封闭作用域目录，不允许客户端创建新的标签类别。
pub enum Kind {
    /// 当前冻结路由上的会话、订阅、客户端操作和运行资格。
    Route,
    /// 受权部署端点的远程请求，不以实际 URL 生成系列。
    Endpoint,
    /// 受信 MQ 入口的处理尝试与可用性。
    Ingress,
}

const KINDS: &[&str] = &["route", "endpoint", "ingress"];
const OPERATIONS: &[&[&str]] = &[
    &["sub", "unsub", "notify", "msg"],
    &["request"],
    &["consume"],
];
const RESOURCES: &[&[&str]] = &[
    &[
        "inflight",
        "sessions_initializing",
        "sessions_ready",
        "subscriptions",
        "subscriptions_pending",
        "writers",
        "online",
        "protected",
        "offline",
    ],
    &["inflight"],
    &["inflight", "ready", "unavailable"],
];
static EVENTS: MetricDescriptor = MetricDescriptor {
    name: "nacket_scope_events_total",
    help: "固定名称目录中的实际操作结局，未列出名称聚合到保留桶。",
    unit: "",
    kind: MetricKind::Counter,
    label_names: &["kind", "name", "operation", "outcome"],
    histogram_bounds: &[],
};
static LIVE: MetricDescriptor = MetricDescriptor {
    name: "nacket_scope_resources",
    help: "固定名称目录中的真实资源或周期运行状态。",
    unit: "",
    kind: MetricKind::Gauge,
    label_names: &["kind", "name", "resource"],
    histogram_bounds: &[],
};
static LATENCY: MetricDescriptor = MetricDescriptor {
    name: "nacket_scope_duration_seconds",
    help: "当前固定名称操作的单调时间耗时，包含本地容量等待。",
    unit: "seconds",
    kind: MetricKind::Histogram,
    label_names: &["kind", "name", "operation"],
    histogram_bounds: &[
        0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0,
    ],
};
static DESCRIPTORS: &[&MetricDescriptor] = &[&EVENTS, &LIVE, &LATENCY];

struct Entry {
    /// 领域在固定操作、资源和标签目录中的索引。
    kind: usize,
    /// 启动允许的唯一名称或 _other；未知输入的原名称不会保存到此处。
    name: String,
    /// 按操作和固定结局展开的累计计数，不在事件发生时扩容。
    counters: Vec<AtomicU64>,
    /// 本领域实际责任量及后台状态投影；两种更新方式使用不同资源项。
    resources: Vec<AtomicU64>,
    /// 每个固定操作独立的单调耗时分布，导出单位为秒。
    durations: Vec<AtomicHistogram>,
}

/// 启动时冻结的详细指标来源，未登记名称归入有限聚合桶。
pub struct Scopes {
    /// 按 Route、Endpoint、Ingress 排列的启动目录，每类最多三十二项及一个聚合桶。
    entries: [BTreeMap<String, Arc<Entry>>; 3],
}

/// 真实资源的观察责任，迁移或退出时按同一份额归还计数。
pub struct Resource {
    /// 本责任归属的固定条目；详细指标关闭或类别无效时不建立观察责任。
    entry: Option<Arc<Entry>>,
    /// 当前资源在所属领域 RESOURCES 目录中的位置。
    index: usize,
    /// 由本 owner 实际持有的份额，迁移及释放时按同一数量记账。
    amount: u64,
}

/// 一次操作的在途、耗时与终态观察，未明确完成退出时按取消记录。
pub struct Event {
    /// 本次操作的固定名称或聚合条目，不持有业务正文或外部名称。
    entry: Option<Arc<Entry>>,
    /// 领域内已登记的操作索引，决定计数与耗时的共同归属。
    operation: usize,
    /// 从操作进入处理边界开始的单调时间，包含其后的容量等待。
    started: Instant,
    /// 结局是否已记录，防止明确完成与 Drop 的取消出口重复计数。
    completed: bool,
    /// 持有一次实际在途观察，随 Event owner 退出自动归还。
    _inflight: Resource,
}

impl Scopes {
    /// 业务作用：在启动阶段预分配全部详细时间序列，热路径不创建任何新标签。
    /// 参数说明：
    /// - `policy`：已经验证的固定名称目录及开关。
    ///
    /// 返回：关闭时为空来源，开启时每类另有唯一 _other 聚合桶。
    pub fn new(policy: &crate::config::metric_scopes::MetricScopes) -> Arc<Self> {
        let names = [
            &policy.route_ids,
            &policy.endpoint_refs,
            &policy.ingress_refs,
        ];
        Arc::new(Self {
            entries: std::array::from_fn(|kind| {
                if !policy.enabled {
                    return BTreeMap::new();
                }
                names[kind]
                    .iter()
                    .cloned()
                    .chain(std::iter::once("_other".into()))
                    .map(|name| {
                        let entry = Arc::new(Entry {
                            kind,
                            name: name.clone(),
                            counters: (0..OPERATIONS[kind].len() * OUTCOMES.len())
                                .map(|_| AtomicU64::new(0))
                                .collect(),
                            resources: RESOURCES[kind].iter().map(|_| AtomicU64::new(0)).collect(),
                            durations: OPERATIONS[kind]
                                .iter()
                                .map(|_| AtomicHistogram::new(LATENCY.histogram_bounds))
                                .collect(),
                        });
                        (name, entry)
                    })
                    .collect()
            }),
        })
    }

    /// 业务作用：计算所有预留名称和 Histogram 展开后的系列上界，供统一 nafana 预算接纳。
    /// 参数说明: 无。
    ///
    /// 返回：固定容量，未发生流量也不会低估未来系列数量。
    pub fn worst_case_series(&self) -> usize {
        self.entries
            .iter()
            .enumerate()
            .map(|(kind, entries)| {
                entries.len()
                    * (OPERATIONS[kind].len()
                        * (OUTCOMES.len() + LATENCY.histogram_bounds.len() + 3)
                        + RESOURCES[kind].len())
            })
            .sum()
    }

    /// 业务作用：关闭详细指标时跳过后台目录投影和临时分配。
    /// 参数说明: 无。
    ///
    /// 返回：只有启动时启用过固定目录才返回 true。
    pub fn enabled(&self) -> bool {
        !self.entries[0].is_empty()
    }

    /// 业务作用：将实际业务名称映射到启动目录，未知名称不保留原文。
    /// 参数说明：
    /// - `kind`：固定领域。
    /// - `name`：内部可信计划中的名称，不用客户端任意字段生成标签。
    ///
    /// 返回：固定条目或唯一聚合桶；详细指标关闭时为 None。
    fn entry(&self, kind: Kind, name: &str) -> Option<Arc<Entry>> {
        let entries = &self.entries[kind as usize];
        entries.get(name).or_else(|| entries.get("_other")).cloned()
    }

    /// 业务作用：让详细资源量跟随真实 owner 移交和释放，与全节点资源口径一致。
    /// 参数说明：
    /// - `kind`：固定领域。
    /// - `name`：内部计划名称。
    /// - `resource`：该领域已有的固定资源类型。
    /// - `amount`：此次取得的实际责任数。
    ///
    /// 返回：自动归还的观察责任；未知类型不产生系列。
    pub fn reserve(&self, kind: Kind, name: &str, resource: &str, amount: u64) -> Resource {
        let Some(index) = RESOURCES[kind as usize]
            .iter()
            .position(|value| *value == resource)
        else {
            return Resource {
                entry: None,
                index: 0,
                amount: 0,
            };
        };
        let entry = self.entry(kind, name);
        if let Some(entry) = &entry {
            add(&entry.resources[index], amount);
        }
        Resource {
            entry,
            index,
            amount,
        }
    }

    /// 业务作用：登记一次真实调用及取消责任，不把重复缓存读取冒充新业务执行。
    /// 参数说明：
    /// - `kind`：固定领域。
    /// - `name`：当前冻结计划的内部名称。
    /// - `operation`：该领域允许的固定动作。
    ///
    /// 返回：由最终完成或取消记录一次结局的观察者。
    pub fn event(&self, kind: Kind, name: &str, operation: &str) -> Event {
        let index = OPERATIONS[kind as usize]
            .iter()
            .position(|value| *value == operation);
        Event {
            entry: index.and_then(|_| self.entry(kind, name)),
            operation: index.unwrap_or(0),
            started: Instant::now(),
            completed: false,
            _inflight: self.reserve(kind, name, "inflight", 1),
        }
    }

    /// 业务作用：一次性更新有限运行目录投影，多个未列出名称在同一保留桶内累加。
    /// 参数说明：
    /// - `kind`：只更新指定领域。
    /// - `resources`：本次完整替换的固定资源列表。
    /// - `values`：后台维护取得的完整有限目录，不在抓取路径读取业务状态。
    ///
    /// 返回：该批资源已更新；未出现的受权名称归零，累计操作计数不受影响。
    pub fn project(
        &self,
        kind: Kind,
        resources: &[&str],
        values: impl IntoIterator<Item = (String, &'static str, u64)>,
    ) {
        let entries = &self.entries[kind as usize];
        if entries.is_empty() {
            return;
        }
        let mut totals: BTreeMap<&str, Vec<u64>> = entries
            .keys()
            .map(|name| (name.as_str(), vec![0; RESOURCES[kind as usize].len()]))
            .collect();
        for (name, resource, amount) in values {
            if !resources.contains(&resource) {
                continue;
            }
            let key = if entries.contains_key(&name) {
                name.as_str()
            } else {
                "_other"
            };
            if let Some(index) = RESOURCES[kind as usize]
                .iter()
                .position(|value| *value == resource)
            {
                let total = &mut totals.get_mut(key).expect("fixed metric directory")[index];
                *total = total.saturating_add(amount);
            }
        }
        // 先完成聚合再替换各观察量，抓取不会看到清零与逐项累加的中间过程。
        for (name, values) in totals {
            for resource in resources {
                if let Some(index) = RESOURCES[kind as usize]
                    .iter()
                    .position(|value| value == resource)
                {
                    entries[name].resources[index].store(values[index], Ordering::Relaxed);
                }
            }
        }
    }
}

impl Resource {
    /// 业务作用：在同一真实责任的生命周期内迁移状态，避免事件丢失造成重复计数。
    /// 参数说明：
    /// - `resource`：同领域内的新固定状态。
    ///
    /// 返回：原资源已归还，新资源接续持有；未知状态保持原归属。
    pub fn transfer(&mut self, resource: &str) {
        if let Some(entry) = &self.entry {
            if let Some(index) = RESOURCES[entry.kind]
                .iter()
                .position(|value| *value == resource)
            {
                entry.resources[self.index].fetch_sub(self.amount, Ordering::Relaxed);
                add(&entry.resources[index], self.amount);
                self.index = index;
            }
        }
    }
}
impl Drop for Resource {
    /// 业务作用：真实 owner 结束时归还详细资源量，取消和失败共用相同出口。
    /// 参数说明: 无。
    ///
    /// 返回：准确归还本责任的数量，不修改任何业务资格。
    fn drop(&mut self) {
        if let Some(entry) = &self.entry {
            entry.resources[self.index].fetch_sub(self.amount, Ordering::Relaxed);
        }
    }
}
impl Event {
    /// 业务作用：记录一次完成结局和单调耗时，未知结局不写入错误原文标签。
    /// 参数说明：
    /// - `outcome`：预登记结局。
    ///
    /// 返回：最多计数一次；不能由监控观察改变调用结果。
    pub fn finish(&mut self, outcome: &str) {
        if self.completed {
            return;
        }
        self.completed = true;
        if let (Some(entry), Some(outcome)) = (
            &self.entry,
            OUTCOMES.iter().position(|value| *value == outcome),
        ) {
            add(
                &entry.counters[self.operation * OUTCOMES.len() + outcome],
                1,
            );
            entry.durations[self.operation].observe(self.started.elapsed());
        }
    }
}
impl Drop for Event {
    /// 业务作用：异步等待取消也保留结局，不将没有完成的远端请求计为成功。
    /// 参数说明: 无。
    ///
    /// 返回：未完成时只计一次 canceled，实际副作用语义继续由业务合同决定。
    fn drop(&mut self) {
        self.finish("canceled");
    }
}

impl LegacyMetricsSource for Scopes {
    /// 业务作用：把详细领域指标登记到既有 nafana 目录，不另建出口或服务。
    /// 参数说明: 无。
    ///
    /// 返回：启动固定的三个描述符。
    fn descriptors(&self) -> &'static [&'static MetricDescriptor] {
        DESCRIPTORS
    }

    /// 业务作用：仅从预分配原子数据生成有限快照，抓取不访问会话、磁盘或 broker。
    /// 参数说明: 无。
    ///
    /// 返回：受启动基数预算约束的结构化样本。
    fn snapshot(&self) -> Option<Vec<MetricSample>> {
        let mut samples = Vec::new();
        for entry in self.entries.iter().flat_map(|entries| entries.values()) {
            for (operation_index, operation) in OPERATIONS[entry.kind].iter().enumerate() {
                for (outcome_index, outcome) in OUTCOMES.iter().enumerate() {
                    samples.push(MetricSample {
                        name: EVENTS.name,
                        labels: vec![
                            ("kind", KINDS[entry.kind].into()),
                            ("name", entry.name.clone()),
                            ("operation", (*operation).into()),
                            ("outcome", (*outcome).into()),
                        ],
                        value: MetricValue::Counter(
                            entry.counters[operation_index * OUTCOMES.len() + outcome_index]
                                .load(Ordering::Relaxed),
                        ),
                    });
                }
                samples.push(MetricSample {
                    name: LATENCY.name,
                    labels: vec![
                        ("kind", KINDS[entry.kind].into()),
                        ("name", entry.name.clone()),
                        ("operation", (*operation).into()),
                    ],
                    value: entry.durations[operation_index].snapshot(),
                });
            }
            for (index, resource) in RESOURCES[entry.kind].iter().enumerate() {
                samples.push(MetricSample {
                    name: LIVE.name,
                    labels: vec![
                        ("kind", KINDS[entry.kind].into()),
                        ("name", entry.name.clone()),
                        ("resource", (*resource).into()),
                    ],
                    value: MetricValue::Gauge(entry.resources[index].load(Ordering::Relaxed) as f64),
                });
            }
        }
        Some(samples)
    }

    /// 业务作用：使用统一快照编码器提供文本表示，保持结构化与文本出口一致。
    /// 参数说明：
    /// - `output`：接收当前有限快照的目标。
    ///
    /// 返回：追加当前来源文本，不进行外部采样。
    fn render_prometheus(&self, output: &mut String) {
        render_snapshot(DESCRIPTORS, &self.snapshot().unwrap_or_default(), output);
    }
}
