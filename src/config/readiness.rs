use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

/// 就绪策略与动态路由目录分离，空目录可以继续提供管理能力。
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "policy", rename_all = "snake_case", deny_unknown_fields)]
pub enum Readiness {
    /// 以运行依赖就绪为条件，允许暂时没有在线业务路由。
    Runtime {},
    /// 运行依赖就绪之外，还要求指定路由全部满足服务条件。
    RequiredRoutes {
        #[serde(rename = "routeIds")]
        /// 除了运行环境可用，还必须全部在线且许可有效的固定路由集合。
        route_ids: BTreeSet<String>,
    },
}

impl Default for Readiness {
    /// 业务作用：允许没有在线路由的实例完成运行与管理初始化，路由接流仍受各自门禁限制。
    /// 参数说明: 无。
    ///
    /// 返回：只检查运行依赖的默认策略，不隐式要求任意路由在线。
    fn default() -> Self {
        Self::Runtime {}
    }
}

impl Readiness {
    /// 业务作用：限制必需路由清单，不要求动态路由必须已经出现在启动 YAML 中。
    /// 参数说明: 无。
    ///
    /// 返回：清单有界且每个标识合法；空清单或过量引用拒绝。
    pub fn validate(&self) -> anyhow::Result<()> {
        if let Self::RequiredRoutes {
            route_ids,
        } = self
        {
            anyhow::ensure!(
                !route_ids.is_empty() && route_ids.len() <= 1024,
                "readiness_route_capacity"
            );
            for route in route_ids {
                crate::domain::message::envelope::identifier(route)?;
            }
        }
        Ok(())
    }
}
