use crate::{
    application::gateway::{dispatch::Deferred, Gateway},
    config::messaging::{Ingress, IngressPurpose},
    observability::Reservation,
};
use std::sync::Arc;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// 拉取前按最坏响应预约容量，原始字段和隔离材料处理结束后才释放。
pub struct ReadPlan {
    /// 在入口条数和节点字节预算共同约束下实际允许读取的批量。
    pub batch: usize,
    /// 本次读取前必须预约的保守响应空间，单位字节。
    bytes: usize,
    /// 按业务、初始化或授权控制用途选择的独立读前预算池。
    pool: Arc<Semaphore>,
    /// 固定观测资源名，不从 stream 或消息字段创建标签。
    resource: &'static str,
}
pub struct ReadReservation {
    /// 覆盖实际读取和响应持有阶段的字节容量许可。
    _permit: OwnedSemaphorePermit,
    /// 与实际读前容量同生命周期的观测责任。
    _observed: Reservation,
}
impl ReadPlan {
    /// 业务作用：为数据、初始化回复和授权控制选择独立保留容量，数据拉取不能挤占撤权来源。
    /// 参数说明：
    /// - `gateway`：全节点预算与指标。
    /// - `ingress`：固定单记录及条数边界。
    ///
    /// 返回：本轮可覆盖的有限批次；单条最坏响应无法容纳时拒绝来源启动。
    pub fn new(gateway: &Gateway, ingress: &Ingress) -> anyhow::Result<Self> {
        let (index, resource, capacity) = match ingress.purpose {
            IngressPurpose::Push => (
                0,
                "ingress_read_bytes",
                gateway.configuration.ingress_processing.max_read_bytes,
            ),
            IngressPurpose::InitializationReply => (
                1,
                "initialization_read_bytes",
                gateway
                    .configuration
                    .ingress_processing
                    .max_control_read_bytes,
            ),
            IngressPurpose::AuthorizationControl => (
                2,
                "authorization_read_bytes",
                gateway
                    .configuration
                    .ingress_processing
                    .max_control_read_bytes,
            ),
        };
        let maximum = ingress.max_body_bytes + ingress.max_metadata_bytes + 4096;
        let cost = maximum * 4;
        anyhow::ensure!(cost <= capacity, "ingress_read_capacity_below_record");
        let batch = ingress
            .batch_size
            .min(
                gateway
                    .configuration
                    .ingress_processing
                    .max_key_pending_records,
            )
            .min(capacity / cost)
            .min((32 * 1024 * 1024 / maximum).max(1));
        Ok(Self {
            batch,
            bytes: cost * batch,
            pool: gateway.ingress_reads[index].clone(),
            resource,
        })
    }
    /// 业务作用：在发送读取命令前预约全部响应责任，容量不足暂缓输入而不推进 broker 水位。
    /// 参数说明：
    /// - `gateway`：提供真实资源观察。
    ///
    /// 返回：覆盖当前读取及原文处理的责任；无容量时只允许退避重试。
    pub fn acquire(&self, gateway: &Gateway) -> anyhow::Result<ReadReservation> {
        let permit = self
            .pool
            .clone()
            .try_acquire_many_owned(self.bytes as u32)
            .map_err(|_| Deferred("ingress_read_capacity"))?;
        Ok(ReadReservation {
            _permit: permit,
            _observed: gateway.metrics.reserve(self.resource, self.bytes as u64),
        })
    }
}
