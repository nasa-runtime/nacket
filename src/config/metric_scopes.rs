use serde::Deserialize;

/// 详细指标只使用启动时固定的受权名称，目录不随客户端或动态路由扩张。
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(default, rename_all = "camelCase", deny_unknown_fields)]
pub struct MetricScopes {
    /// 是否登记固定名称的详细指标来源；默认关闭，不影响节点总量及业务权限。
    pub enabled: bool,
    /// 启动时允许细分的路由 ID，最多三十二个；其它路由汇总到 _other。
    pub route_ids: Vec<String>,
    /// 启动时允许细分的远程端点引用，最多三十二个，不使用 URL 作为标签。
    pub endpoint_refs: Vec<String>,
    /// 启动时允许细分的受信 MQ 入口引用，最多三十二个；目录改变需要重启。
    pub ingress_refs: Vec<String>,
}

impl MetricScopes {
    /// 业务作用：限制整个进程生命周期的详细标签集合，不以淘汰和重用名称规避基数预算。
    /// 参数说明: 无。
    ///
    /// 返回：三个目录各最多三十二项且名称唯一；保留聚合名和非法标识拒绝。
    pub fn validate(&self) -> anyhow::Result<()> {
        for names in [&self.route_ids, &self.endpoint_refs, &self.ingress_refs] {
            let unique = names.iter().collect::<std::collections::BTreeSet<_>>();
            anyhow::ensure!(
                names.len() <= 32 && unique.len() == names.len(),
                "metric_scope_capacity"
            );
            for name in names {
                crate::domain::message::envelope::identifier(name)?;
                anyhow::ensure!(name != "_other" && name != "_all", "metric_scope_reserved");
            }
        }
        Ok(())
    }
}
