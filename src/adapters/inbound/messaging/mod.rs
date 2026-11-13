//! 受信业务 MQ 适配；控制命令、客户端上行与节点 relay 不进入这个入口。

mod buffers;
mod kafka;
mod redis;

use crate::{
    application::gateway::{dispatch::TrustedIngress, Gateway},
    config::messaging::Backend,
};
use std::{collections::BTreeMap, sync::Arc};

/// 业务作用：从 napp 命名来源装配业务下行，所有消费任务及关闭责任交给宿主。
/// 参数说明：
/// - `application`：处于 UserHook 的受管应用。
/// - `gateway`：已经建立但尚未接流的产品运行域。
///
/// 返回：来源、合同及消费者登记完成；任一必要依赖不满足时不进入 Ready。
pub async fn install(application: &nasa::Application, gateway: Arc<Gateway>) -> anyhow::Result<()> {
    let inbox =
        crate::application::gateway::push_inbox::Inbox::install(application, gateway.clone())
            .await?;
    gateway
        .push_inbox
        .set(inbox)
        .map_err(|_| anyhow::anyhow!("push_inbox_already_installed"))?;
    let mut kafka_sources: BTreeMap<String, Vec<Arc<TrustedIngress>>> = BTreeMap::new();
    for (id, configuration) in &gateway.configuration.ingresses {
        if !configuration.enabled {
            continue;
        }
        let ingress = TrustedIngress::new(id.clone(), configuration.clone())?;
        match &configuration.transport {
            Backend::RedisStream {
                source_ref,
                stream,
            } => redis::install(application, gateway.clone(), ingress, source_ref, stream).await?,
            Backend::Kafka {
                source_ref,
                ..
            } => kafka_sources
                .entry(source_ref.clone())
                .or_default()
                .push(ingress),
        }
    }
    for (source, ingresses) in kafka_sources {
        kafka::install(application, gateway.clone(), source, ingresses).await?;
    }
    Ok(())
}
