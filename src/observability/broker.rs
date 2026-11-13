//! 固定 MQ 入口的有界只读观察；采样失败不会改变路由和消费权限。

use crate::config::{messaging::Backend, Deployment};
use nasa::application::{
    LegacyMetricsSource, MetricDescriptor, MetricKind, MetricSample, MetricValue,
};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

const OUTCOMES: &[&str] = &["ok", "unavailable", "capacity", "canceled"];
static FRESH: MetricDescriptor = MetricDescriptor {
    name: "nacket_broker_observation_fresh",
    help: "固定入口聚合桶内全部必要来源是否具有新鲜且完整的位置证据。",
    unit: "",
    kind: MetricKind::Gauge,
    label_names: &["backend", "ingress"],
    histogram_bounds: &[],
};
static AGE: MetricDescriptor = MetricDescriptor {
    name: "nacket_broker_observation_age_seconds",
    help: "距聚合桶内最旧成功采样起点的时间；从未取得完整证据时省略。",
    unit: "seconds",
    kind: MetricKind::Gauge,
    label_names: &["backend", "ingress"],
    histogram_bounds: &[],
};
static VALUES: &[MetricDescriptor] = &[
    MetricDescriptor {
        name: "nacket_broker_unread_positions",
        help: "Redis 新增位置或 Kafka 已提交读取末尾与组提交位置的距离，不能当作业务消息数。",
        unit: "",
        kind: MetricKind::Gauge,
        label_names: &["backend", "ingress"],
        histogram_bounds: &[],
    },
    MetricDescriptor {
        name: "nacket_broker_pending_records",
        help: "Redis 稳定组已交付但未确认的实际 PEL 记录数，不能跨节点当作唯一消息数。",
        unit: "",
        kind: MetricKind::Gauge,
        label_names: &["backend", "ingress"],
        histogram_bounds: &[],
    },
    MetricDescriptor {
        name: "nacket_broker_pending_max_idle_seconds",
        help: "Redis PEL 内最大的认领后空闲时间；重新认领会重置，不代表业务创建年龄。",
        unit: "seconds",
        kind: MetricKind::Gauge,
        label_names: &["backend", "ingress"],
        histogram_bounds: &[],
    },
    MetricDescriptor {
        name: "nacket_broker_observed_partitions",
        help: "Kafka 本轮完整取得位置证据的固定分区数量，缺项时不显示为完整零积压。",
        unit: "",
        kind: MetricKind::Gauge,
        label_names: &["backend", "ingress"],
        histogram_bounds: &[],
    },
];
static EVENTS: MetricDescriptor = MetricDescriptor {
    name: "nacket_broker_observations_total",
    help: "独立 broker 采样的完成、失败、容量拒绝与关闭次数，不计入业务调用。",
    unit: "",
    kind: MetricKind::Counter,
    label_names: &["backend", "ingress", "outcome"],
    histogram_bounds: &[],
};

/// 一次只读位置观察，各可选维度必须与固定后端能力匹配。
pub struct Reading {
    /// 可选来源拓扑与分配的连续证据，仅用于撤回观察；失效时数量立即不可展示。
    pub dependency: Option<crate::domain::route::dependency::DependencyPermit>,
    /// 尚未读取或尚未提交的位置距离；Kafka 控制记录和中止事务可能占据位置。
    pub unread_positions: u64,
    /// Redis 可恢复组的实际 PEL 数量，其它接入不提供此维度。
    pub pending_records: Option<u64>,
    /// Redis 完整有限 PEL 中最大的空闲时长，不能推导消息存活时间。
    pub pending_max_idle: Option<Duration>,
    /// Kafka 已完整观测的固定分区数，其它后端不提供此维度。
    pub partitions: Option<u64>,
}

struct State {
    /// 成功采样所依赖的原连续有效区间，后续恢复不能让旧观察重新有效。
    dependency: Option<crate::domain::route::dependency::DependencyPermit>,
    /// 最近成功采样的请求起点，等待也计入样本年龄。
    observed: Option<Instant>,
    /// 最近一轮是否成功；失败立即撤回旧数量，不等待新鲜度耗尽。
    available: bool,
    /// 固定四项观察量，未支持的维度保持 None。
    values: [Option<f64>; 4],
    /// 固定四类采样结局，从进程启动累计，不生成外部原因标签。
    events: [u64; 4],
}

/// 固定入口的观察责任；只读采样任务持有，不持有正文或消费者。
pub struct Source {
    /// 后端及固定聚合名称在启动目录中的索引。
    group: usize,
    /// 该入口承诺提供的维度，聚合时不能用不支持的零值代替。
    required: [bool; 4],
    /// 采样结果与累计结局的短临界区，不在此锁内执行网络操作。
    state: Mutex<State>,
}

/// 所有来源的有限目录，共用采样并发预算，抓取只读取内存快照。
pub struct Broker {
    /// 启动冻结的采样周期、期限和并发开关。
    pub policy: crate::config::broker_metrics::BrokerMetrics,
    /// 节点采样名额；取不到时本轮跳过，不形成无限等待队列。
    pub capacity: tokio::sync::Semaphore,
    /// 实际入口到固定观察责任的映射，最多跟随部署的六十四个来源。
    sources: BTreeMap<String, Arc<Source>>,
    /// 固定后端与公开名称；详细名称未开启时统一汇总为 _all。
    groups: Vec<(&'static str, String)>,
}

impl Broker {
    /// 业务作用：在启动时冻结可观测来源和公开标签，客户端不能创建指标条目。
    /// 参数说明：
    /// - `deployment`：已完整验证的部署配置及详细名称白名单。
    ///
    /// 返回：关闭时空目录；开启时只登记实际启用的入口，不连接 broker。
    pub fn new(deployment: &Deployment) -> Arc<Self> {
        let mut groups = Vec::new();
        let mut sources = BTreeMap::new();
        if deployment.broker_metrics.enabled
            && !deployment
                .control
                .settings()
                .is_some_and(|settings| settings.role == crate::config::control::Role::Controller)
        {
            for (id, ingress) in &deployment.ingresses {
                if !ingress.enabled {
                    continue;
                }
                let (backend, required) = match ingress.transport {
                    Backend::RedisStream {
                        ..
                    } => {
                        let recovery = ingress.delivery
                            == crate::domain::route::policies::DeliveryMode::Recoverable;
                        ("redis", [true, recovery, recovery, false])
                    }
                    Backend::Kafka {
                        ..
                    } => ("kafka", [true, false, false, true]),
                };
                let name = if !deployment.metric_scopes.enabled {
                    "_all"
                } else if deployment.metric_scopes.ingress_refs.contains(id) {
                    id
                } else {
                    "_other"
                };
                let key = (backend, name.to_owned());
                let group = groups
                    .iter()
                    .position(|group| group == &key)
                    .unwrap_or_else(|| {
                        groups.push(key);
                        groups.len() - 1
                    });
                sources.insert(
                    id.clone(),
                    Arc::new(Source {
                        group,
                        required,
                        state: Mutex::new(State {
                            dependency: None,
                            observed: None,
                            available: false,
                            values: [None; 4],
                            events: [0; 4],
                        }),
                    }),
                );
            }
        }
        Arc::new(Self {
            policy: deployment.broker_metrics.clone(),
            capacity: tokio::sync::Semaphore::new(deployment.broker_metrics.max_in_flight),
            sources,
            groups,
        })
    }

    /// 业务作用：为已经登记的实际入口取得采样责任，关闭时不创建后台网络任务。
    /// 参数说明：
    /// - `id`：编译部署中固定的入口引用。
    ///
    /// 返回：共享观察条目；关闭、未启用或未登记入口为 None。
    pub fn source(&self, id: &str) -> Option<Arc<Source>> {
        self.sources.get(id).cloned()
    }

    /// 业务作用：为固定来源的所有有无数据状态预留最大指标容量。
    /// 参数说明: 无。
    ///
    /// 返回：每个聚合桶四项数值、可用性、年龄和四类结局的系列上限。
    pub fn worst_case_series(&self) -> usize {
        self.groups.len() * (VALUES.len() + OUTCOMES.len() + 2)
    }

    /// 业务作用：在独立受管任务中定期读取固定来源，抓取和业务消费者都不等待采样。
    /// 参数说明：
    /// - `application`：提供启动及关闭信号的唯一宿主。
    /// - `broker`：固定目录与节点共享采样容量。
    /// - `id`：已登记入口引用，用于分散采样起点。
    /// - `read`：借用已有受管客户端的只读请求，参数是本轮总预算。
    ///
    /// 返回：关闭或未登记入口时不创建任务；采样超期后保留实际等待责任，禁止叠加新请求。
    pub async fn install<F, Fut>(
        application: &nasa::Application,
        broker: Arc<Self>,
        id: &str,
        read: F,
    ) -> anyhow::Result<()>
    where
        F: Fn(Duration) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = anyhow::Result<Reading>> + Send,
    {
        let Some(source) = broker.source(id) else {
            return Ok(());
        };
        let index = broker
            .sources
            .keys()
            .position(|key| key == id)
            .expect("registered source");
        let delay = Duration::from_millis(
            broker.policy.interval_ms * index as u64 / broker.sources.len() as u64,
        );
        application.serve_when_ready(format!("socket-broker-metrics-{id}"), move |shutdown| async move {
            tokio::select! {
                biased;
                _ = shutdown.cancelled() => return Ok::<(), anyhow::Error>(()),
                _ = tokio::time::sleep(delay) => {},
            }
            loop {
                if let Ok(_permit) = broker.capacity.try_acquire() {
                    let started = Instant::now();
                    let timeout = Duration::from_millis(broker.policy.timeout_ms);
                    let request = read(timeout);
                    tokio::pin!(request);
                    let response = tokio::select! {
                        biased;
                        _ = shutdown.cancelled() => {
                            source.unavailable("canceled");
                            return Ok(());
                        }
                        response = tokio::time::timeout(timeout, &mut request) => response,
                    };
                    match response {
                        Ok(Ok(reading)) => source.record(started, reading),
                        Ok(Err(_)) => source.unavailable("unavailable"),
                        Err(_) => {
                            source.unavailable("unavailable");
                            // 超时只撤回观察；继续持有并发名额直到原请求返回，迟到结果不能刷新样本。
                            tokio::select! {
                                biased;
                                _ = shutdown.cancelled() => return Ok(()),
                                _ = &mut request => {},
                            }
                        }
                    }
                } else {
                    source.unavailable("capacity");
                }
                tokio::select! {
                    biased;
                    _ = shutdown.cancelled() => {
                        source.unavailable("canceled");
                        return Ok(());
                    }
                    _ = tokio::time::sleep(Duration::from_millis(broker.policy.interval_ms)) => {},
                }
            }
        }).await?;
        Ok(())
    }
}

impl Source {
    /// 业务作用：发布一次完整只读证据，发现缺项时撤回旧数量而不是补零。
    /// 参数说明：
    /// - `started`：真正开始采样时的单调起点。
    /// - `reading`：来自固定 broker 资源的观察结果。
    ///
    /// 返回：能力维度完整时替换快照并记成功，否则记不可用；不修改消费资格。
    pub fn record(&self, started: Instant, reading: Reading) {
        let values = [
            Some(reading.unread_positions as f64),
            reading.pending_records.map(|n| n as f64),
            reading.pending_max_idle.map(|d| d.as_secs_f64()),
            reading.partitions.map(|n| n as f64),
        ];
        let complete = self
            .required
            .iter()
            .zip(values)
            .all(|(required, value)| !required || value.is_some());
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if complete {
            state.observed = Some(started);
            state.values = values;
            state.dependency = reading.dependency;
        }
        state.available = complete;
        let outcome = usize::from(!complete);
        state.events[outcome] = state.events[outcome].saturating_add(1);
    }

    /// 业务作用：采样失败或退出时立即停止展示旧积压，不把监控错误变成业务拒绝。
    /// 参数说明：
    /// - `outcome`：unavailable、capacity 或 canceled 中的固定结局。
    ///
    /// 返回：旧样本失效并累计一次对应事件；未知分类统一归入 unavailable。
    pub fn unavailable(&self, outcome: &str) {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        state.available = false;
        let index = OUTCOMES
            .iter()
            .position(|name| *name == outcome)
            .filter(|index| *index != 0)
            .unwrap_or(1);
        state.events[index] = state.events[index].saturating_add(1);
    }
}

impl LegacyMetricsSource for Broker {
    /// 业务作用：向既有 nafana 目录登记固定 broker 观察合同。
    /// 参数说明: 无。
    ///
    /// 返回：不随消息、分区编号或用户身份增长的描述符。
    fn descriptors(&self) -> &'static [&'static MetricDescriptor] {
        static DESCRIPTORS: std::sync::OnceLock<Vec<&'static MetricDescriptor>> =
            std::sync::OnceLock::new();
        DESCRIPTORS.get_or_init(|| {
            [&FRESH, &AGE, &EVENTS]
                .into_iter()
                .chain(VALUES.iter())
                .collect()
        })
    }

    /// 业务作用：聚合启动固定来源的内存观察，缺少任何必要来源就省略该桶数量。
    /// 参数说明: 无。
    ///
    /// 返回：完整且新鲜的数量、失败可用性与累计事件，不发起网络读取。
    fn snapshot(&self) -> Option<Vec<MetricSample>> {
        let mut output = Vec::new();
        for (index, (backend, name)) in self.groups.iter().enumerate() {
            let labels = vec![("backend", (*backend).into()), ("ingress", name.clone())];
            let mut complete = true;
            let mut known_age = true;
            let mut age = Duration::ZERO;
            let mut values = [None; 4];
            let mut events = [0u64; 4];
            for source in self.sources.values().filter(|source| source.group == index) {
                let state = source
                    .state
                    .lock()
                    .unwrap_or_else(|error| error.into_inner());
                let elapsed = state.observed.map(|at| at.elapsed());
                complete &= state.available
                    && state
                        .dependency
                        .as_ref()
                        .is_none_or(|permit| permit.valid())
                    && elapsed
                        .is_some_and(|age| age < Duration::from_millis(self.policy.stale_after_ms));
                known_age &= elapsed.is_some();
                age = age.max(elapsed.unwrap_or_default());
                for (i, value) in state.values.iter().enumerate() {
                    if !source.required[i] {
                        continue;
                    }
                    if let Some(value) = value {
                        let current = values[i].unwrap_or(0.0f64);
                        values[i] = Some(if i == 2 {
                            current.max(*value)
                        } else {
                            current + value
                        });
                    }
                }
                for (sum, event) in events.iter_mut().zip(state.events) {
                    *sum = sum.saturating_add(event);
                }
            }
            output.push(MetricSample {
                name: FRESH.name,
                labels: labels.clone(),
                value: MetricValue::Gauge(if complete {
                    1.0
                } else {
                    0.0
                }),
            });
            if known_age {
                output.push(MetricSample {
                    name: AGE.name,
                    labels: labels.clone(),
                    value: MetricValue::Gauge(age.as_secs_f64()),
                });
            }
            if complete {
                for (descriptor, value) in VALUES.iter().zip(values) {
                    if let Some(value) = value {
                        output.push(MetricSample {
                            name: descriptor.name,
                            labels: labels.clone(),
                            value: MetricValue::Gauge(value),
                        });
                    }
                }
            }
            for (outcome, count) in OUTCOMES.iter().zip(events) {
                let mut labels = labels.clone();
                labels.push(("outcome", (*outcome).into()));
                output.push(MetricSample {
                    name: EVENTS.name,
                    labels,
                    value: MetricValue::Counter(count),
                });
            }
        }
        Some(output)
    }

    /// 业务作用：提供与结构化快照相同的有限文本表示，运行期仍由统一出口编码。
    /// 参数说明：
    /// - `output`：接收 Prometheus 文本的目标。
    ///
    /// 返回：追加当前内存观察，不读取 broker 或业务会话。
    fn render_prometheus(&self, output: &mut String) {
        nametrics_core::atomic::render_snapshot(
            self.descriptors(),
            &self.snapshot().unwrap_or_default(),
            output,
        );
    }
}
