//! 握手附加材料是受限的不可信输入，只能转交固定业务端点，不能作为已验证身份。

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// 路由明确允许转交的字段与单值上限，空目录不接受附加认证字段。
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase", deny_unknown_fields)]
pub struct HandshakePolicy {
    /// AUTH authData 中允许的标量属性及其最大 JSON 字节数，最多十六项。
    pub auth_fields: BTreeMap<String, usize>,
    /// 允许转交的小写 HTTP header 名及最大值字节数，不包含身份凭据或代理来源头。
    pub headers: BTreeMap<String, usize>,
}

impl HandshakePolicy {
    /// 业务作用：省略空的转交目录，避免默认路由定义产生额外摘要字段。
    /// 参数说明: 无。
    ///
    /// 返回：不接受附加认证字段或业务 header 时为真。
    pub fn is_empty(&self) -> bool {
        self.auth_fields.is_empty() && self.headers.is_empty()
    }

    /// 业务作用：冻结有限输入目录并禁止将凭据、转发来源和握手控制头当作业务参数。
    /// 参数说明: 无。
    ///
    /// 返回：名称、数量及字节限制合法；保留头和无界配置拒绝。
    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.auth_fields.len() <= 16 && self.headers.len() <= 16,
            "handshake_field_capacity"
        );
        for (name, limit) in &self.auth_fields {
            crate::domain::message::envelope::identifier(name)?;
            anyhow::ensure!((1..=4096).contains(limit), "handshake_value_capacity");
        }
        for (name, limit) in &self.headers {
            anyhow::ensure!(
                !name.is_empty()
                    && name.len() <= 64
                    && name.bytes().all(|value| value.is_ascii_lowercase()
                        || value.is_ascii_digit()
                        || value == b'-')
                    && ![
                        "authorization",
                        "proxy-authorization",
                        "cookie",
                        "set-cookie",
                        "host",
                        "origin",
                        "forwarded",
                        "x-real-ip",
                        "connection",
                        "upgrade"
                    ]
                    .contains(&name.as_str())
                    && !["sec-", "content-", "x-forwarded-", "proxy-"]
                        .iter()
                        .any(|prefix| name.starts_with(prefix))
                    && (1..=1024).contains(limit),
                "handshake_header_policy_invalid"
            );
        }
        Ok(())
    }
}
