use crate::{
    config::uid_events::UidEvents,
    observability::{Counter, Metrics, Reservation},
};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

/// 租户和主体的完整结构化键，不按路由拆分预算，也不用于指标标签。
pub(super) type Key = (Arc<str>, Arc<str>);

/// 节点共用的有界本地事件目录，只保留仍有真实任务 owner 的主体。
pub struct Budget {
    /// 启动时冻结的条数、字节与控制预留预算。
    config: UidEvents,
    /// 所有计数在同一短锁下接纳和回收，失败不能留下半份预约。
    state: Mutex<State>,
    /// 全节点共用的静态指标目录，不包含任意用户标识。
    pub(super) metrics: Arc<Metrics>,
}

#[derive(Default)]
struct State {
    /// 正在排队及消费的事件总数。
    count: usize,
    /// 普通事件占用数，不能借用控制预留。
    data: usize,
    /// 排队及消费闭包持有的逻辑字节。
    bytes: usize,
    /// 有实际任务责任的主体，最后一个任务释放后删除。
    subjects: BTreeMap<Key, Subject>,
}

#[derive(Default)]
struct Subject {
    /// 当前主体的总事件数。
    count: usize,
    /// 当前主体的普通事件数，至少保留一个控制名额。
    data: usize,
    /// 当前主体闭包持有的逻辑字节。
    bytes: usize,
}

/// 随实际消费闭包持有的责任，观察方取消不提前归还尚未释放的载荷预算。
pub(super) struct Lease {
    /// 原预算目录，负责唯一归还。
    owner: Arc<Budget>,
    /// 原始可信主体，不因会话更替转移。
    key: Key,
    /// 实际预约的逻辑字节数。
    bytes: usize,
    /// 是否属于普通事件，控制预留不改变消费优先级。
    data: bool,
    /// 待决条数及年龄观察，与真实任务责任同时释放。
    _count: Reservation,
    /// 逻辑字节观察，与实际保留责任同时释放。
    _bytes: Reservation,
}

impl Budget {
    /// 业务作用：创建本节点唯一事件预算，全部会话和协议共用同一主体目录。
    /// 参数说明：
    /// - `config`：通过部署校验且启动冻结的事件预算。
    /// - `metrics`：节点统一 nafana 指标来源。
    ///
    /// 返回：尚无主体和事件的有界目录；无效配置拒绝建立。
    pub fn new(config: UidEvents, metrics: Arc<Metrics>) -> anyhow::Result<Arc<Self>> {
        config.validate()?;
        Ok(Arc::new(Self {
            config,
            state: Mutex::new(State::default()),
            metrics,
        }))
    }

    /// 业务作用：同时预约主体与节点容量，普通业务不得挤占完成及关闭所需空间。
    /// 参数说明：
    /// - `key`：已经验证的可信主体。
    /// - `bytes`：本次闭包保留的逻辑字节估计，正文引用计入保留量。
    /// - `control`：仅内部完成、取消和关闭路径允许使用控制预留。
    ///
    /// 返回：完整责任或立即拒绝，不等待名额、不产生半份预算。
    pub(super) fn reserve(
        self: &Arc<Self>,
        key: &Key,
        bytes: usize,
        control: bool,
    ) -> anyhow::Result<Lease> {
        let bytes = bytes.max(65536);
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let config = &self.config;
        // 全节点普通任务数不超过单个原始分区剩余容量；即使全部 key 碰撞，仍保留控制位置。
        let data_limit = config.queue_capacity.min(config.max_inflight) - config.control_reserve;
        let subject = state.subjects.get(key);
        let count = subject.map_or(0, |value| value.count);
        let data = subject.map_or(0, |value| value.data);
        let retained = subject.map_or(0, |value| value.bytes);
        let allowed = state.count < config.max_inflight
            && count < config.max_per_uid
            && bytes <= config.max_bytes.saturating_sub(state.bytes)
            && bytes <= config.max_bytes_per_uid.saturating_sub(retained)
            && (control
                || (state.data < data_limit
                    && data < config.max_per_uid - 1
                    && bytes
                        <= config
                            .max_bytes
                            .saturating_sub(config.control_reserve * 65536)
                            .saturating_sub(state.bytes)
                    && bytes
                        <= config
                            .max_bytes_per_uid
                            .saturating_sub(65536)
                            .saturating_sub(retained)));
        if !allowed {
            self.metrics.add(
                Counter::UidEvents,
                if control {
                    "control"
                } else {
                    "data"
                },
                "rejected",
                1,
            );
            anyhow::bail!("uid_event_capacity");
        }
        state.count += 1;
        state.bytes += bytes;
        state.data += usize::from(!control);
        let subject = state.subjects.entry(key.clone()).or_default();
        subject.count += 1;
        subject.bytes += bytes;
        subject.data += usize::from(!control);
        Ok(Lease {
            owner: self.clone(),
            key: key.clone(),
            bytes,
            data: !control,
            _count: self.metrics.reserve("uid_events", 1),
            _bytes: self.metrics.reserve("uid_event_bytes", bytes as u64),
        })
    }
}

impl Drop for Lease {
    /// 业务作用：实际载荷 owner 退出后归还全部容量，最后责任结束即删除主体条目。
    /// 参数说明: 无。
    ///
    /// 返回：唯一预约已回收；取消等待者不能提前释放仍在运行的闭包责任。
    fn drop(&mut self) {
        let mut state = self
            .owner
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        state.count -= 1;
        state.bytes -= self.bytes;
        state.data -= usize::from(self.data);
        if let Some(subject) = state.subjects.get_mut(&self.key) {
            subject.count -= 1;
            subject.bytes -= self.bytes;
            subject.data -= usize::from(self.data);
            if subject.count == 0 {
                state.subjects.remove(&self.key);
            }
        }
    }
}
