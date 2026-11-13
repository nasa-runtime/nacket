use super::{clock::LeaseTime, Ledger, Permit};
use std::{collections::BTreeMap, time::Duration};

/// 只接受已提交快照的本地失效观察；进程重启丢失观察会延后完成，不能提前解除屏障。
#[derive(Default)]
pub struct Observations {
    /// 已完整观察的许可指纹与本地有限时间证据，第一次观察决定等待起点。
    permits: BTreeMap<String, (String, LeaseTime)>,
}

/// 证据不能从管理 JSON 构造，必须经过连续时钟的完整观察窗口。
pub struct Expired {
    /// 失效证据对应的原控制集群身份，不能跨集群用于屏障完成。
    pub(super) cluster_id: String,
    /// 失效证据对应的原控制作用域代际。
    pub(super) scope_epoch: String,
    /// 已经经过完整寿命及部署安全余量的精确许可指纹，不以节点缺席代替证据。
    pub(super) permits: BTreeMap<String, String>,
}

impl Observations {
    /// 业务作用：从已提交许可建立保守观察窗口，续期内容变化后重新等待完整寿命。
    /// 参数说明：
    /// - `ledger`：后端已确认的当前控制快照。
    /// - `margin`：部署批准的时钟误差、调度与写出终止余量之和。
    ///
    /// 返回：已经完整等待的精确许可证据；时钟读取失败不会产生证据。
    pub fn observe(&mut self, ledger: &Ledger, margin: Duration) -> anyhow::Result<Expired> {
        anyhow::ensure!(margin <= Duration::from_secs(120), "invalid_expiry_margin");
        self.permits.retain(|id, _| ledger.permits.contains_key(id));
        let mut permits = BTreeMap::new();
        for (id, permit) in &ledger.permits {
            let fingerprint = fingerprint(permit)?;
            if self
                .permits
                .get(id)
                .is_none_or(|(old, _)| old != &fingerprint)
            {
                let time = LeaseTime::start(Duration::from_millis(permit.ttl_ms) + margin)?;
                self.permits.insert(id.clone(), (fingerprint.clone(), time));
            }
            if self.permits[id].1.elapsed() {
                permits.insert(id.clone(), fingerprint);
            }
        }
        Ok(Expired {
            cluster_id: ledger.cluster_id.clone(),
            scope_epoch: ledger.scope_epoch.clone(),
            permits,
        })
    }
}

/// 业务作用：绑定失效观察对应的全部许可内容，不能只比较可复用 permitId。
/// 参数说明：
/// - `permit`：来自已提交账本的完整许可。
///
/// 返回：确定性内容摘要；任何续期、代际或作用域变化都会使旧证据失效。
pub(super) fn fingerprint(permit: &Permit) -> anyhow::Result<String> {
    use sha2::{Digest, Sha256};
    Ok(format!("{:x}", Sha256::digest(serde_json::to_vec(permit)?)))
}
