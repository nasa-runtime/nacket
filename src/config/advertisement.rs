use serde::{Deserialize, Serialize};

/// 外部网络由部署准备；端口映射只描述已经建立的连接路径，不创建 NAT、代理或 TLS 终止器。
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase", deny_unknown_fields)]
pub struct Advertisement {
    /// 是否把本监听的有限接入声明发布到集群目录，默认不发布。
    pub enabled: bool,
    /// 部署确认的公共主机名或地址，不从本地绑定地址推断。
    pub host: String,
    /// 外部入口是否提供安全传输，用于生成接入提示，不声称本进程终止 TLS。
    pub secure: bool,
    /// 部署对转发及外部网络已经就绪的明确声明，不代表主动探测结果。
    pub network_ready: bool,
    /// 本地端口到公共端口的有限连续映射，必须覆盖使用的自动端口范围。
    pub ports: Vec<PortMapping>,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
/// 部署已建立的连续端口映射，服务只据此公告地址，不创建网络转发。
pub struct PortMapping {
    /// 本地连续端口范围的首个端口，范围长度为 count。
    pub bound_start: u16,
    /// 与 bound_start 对应的外部首端口，后续按相同偏移映射。
    pub public_start: u16,
    /// 连续映射的端口数量，必须非零且本地、外部范围均不溢出。
    pub count: u16,
}

impl Advertisement {
    /// 业务作用：冻结公开地址的部署声明，禁止通配地址、含凭据 URL 和相互歧义的端口范围。
    /// 参数说明: 无。
    ///
    /// 返回：启用时必须有明确外部网络准备声明；关闭时不发布任何地址。
    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(self.ports.len() <= 16, "advertisement_mapping_capacity");
        if self.enabled {
            anyhow::ensure!(
                self.network_ready && !self.ports.is_empty(),
                "advertisement_network_not_ready"
            );
        }
        if self.enabled || !self.host.is_empty() {
            host(&self.host)?;
        }
        for (index, mapping) in self.ports.iter().enumerate() {
            let end = mapping.end()?;
            let public_end = mapping.public_start as u32 + mapping.count as u32 - 1;
            for previous in &self.ports[..index] {
                let previous_end = previous.end()?;
                let previous_public_end = previous.public_start as u32 + previous.count as u32 - 1;
                anyhow::ensure!(
                    (end < previous.bound_start as u32
                        || previous_end < mapping.bound_start as u32)
                        && (public_end < previous.public_start as u32
                            || previous_public_end < mapping.public_start as u32),
                    "advertisement_port_overlap"
                );
            }
        }
        Ok(())
    }

    /// 业务作用：在自动端口分配前验证全部候选端口都有已准备的外部映射，避免节点随机选到不可发布端口。
    /// 参数说明：
    /// - `start`：本地自动范围起点。
    /// - `end`：本地自动范围终点。
    ///
    /// 返回：完整范围均有映射；有任何缺口则拒绝准备。
    pub fn covers(&self, start: u16, end: u16) -> anyhow::Result<()> {
        anyhow::ensure!(start > 0 && start <= end, "advertisement_range_invalid");
        let mut ranges = self.ports.iter().collect::<Vec<_>>();
        ranges.sort_by_key(|range| range.bound_start);
        let mut next = start as u32;
        for range in ranges {
            let last = range.end()?;
            if last < next {
                continue;
            }
            anyhow::ensure!(
                range.bound_start as u32 <= next,
                "advertisement_range_unmapped"
            );
            next = last + 1;
            if next > end as u32 {
                return Ok(());
            }
        }
        anyhow::bail!("advertisement_range_unmapped")
    }

    /// 业务作用：根据真实绑定结果转换外部端口，不把自动分配的第一个节点端口当作集群统一地址。
    /// 参数说明：
    /// - `bound`：实际 listener 已取得的端口。
    ///
    /// 返回：显式准备范围内的外部端口；没有映射时拒绝宣告接入。
    pub fn public_port(&self, bound: u16) -> anyhow::Result<u16> {
        anyhow::ensure!(self.enabled && self.network_ready, "advertisement_disabled");
        self.ports
            .iter()
            .find_map(|mapping| {
                let offset = bound.checked_sub(mapping.bound_start)?;
                (offset < mapping.count)
                    .then(|| mapping.public_start.checked_add(offset))
                    .flatten()
            })
            .ok_or_else(|| anyhow::anyhow!("advertisement_port_unmapped"))
    }
}

impl PortMapping {
    /// 业务作用：在端口分配前核对本地与外部范围均不溢出有效端口空间。
    /// 参数说明: 无。
    ///
    /// 返回：本地范围末尾；空范围、零端口和溢出拒绝。
    fn end(&self) -> anyhow::Result<u32> {
        anyhow::ensure!(
            self.bound_start > 0 && self.public_start > 0 && self.count > 0,
            "advertisement_port_invalid"
        );
        let end = self.bound_start as u32 + self.count as u32 - 1;
        anyhow::ensure!(
            end <= 65535 && self.public_start as u32 + self.count as u32 - 1 <= 65535,
            "advertisement_port_overflow"
        );
        Ok(end)
    }
}

/// 业务作用：验证目录地址只包含明确主机名或 IP，不允许把 URL、凭据或监听通配符传给客户端。
/// 参数说明：
/// - `value`：部署声明的公开主机。
///
/// 返回：保留原主机语义；非法或未指定地址拒绝。
pub fn host(value: &str) -> anyhow::Result<()> {
    anyhow::ensure!(
        !value.is_empty() && value.len() <= 253 && value.is_ascii(),
        "advertisement_host_invalid"
    );
    if let Ok(address) = value.parse::<std::net::IpAddr>() {
        anyhow::ensure!(
            !address.is_unspecified() && !address.is_multicast(),
            "advertisement_host_unspecified"
        );
        return Ok(());
    }
    anyhow::ensure!(
        value.split('.').all(|part| !part.is_empty()
            && part.len() <= 63
            && !part.starts_with('-')
            && !part.ends_with('-')
            && part
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')),
        "advertisement_host_invalid"
    );
    Ok(())
}
