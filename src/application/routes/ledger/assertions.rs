use super::LocalLedger;
use crate::domain::{assertion::Replay, message::UnixMillis};
use serde::{Deserialize, Serialize};
use std::{
    fs::{File, OpenOptions},
    io::{Read, Write},
    path::Path,
    sync::atomic::Ordering,
};

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct State {
    /// 本地断言消费账本绑定的集群身份。
    cluster_id: String,
    /// 断言消费证据所在作用域代际，旧代证据不能在新代复用。
    scope_epoch: String,
    /// 持有本地消费责任的稳定节点身份。
    node_id: String,
    /// 有界单次消费记录和持久时间下界，重启不能恢复已消费资格。
    replay: Replay,
}

impl LocalLedger {
    /// 业务作用：在分配单节点启动代际前恢复一次性断言基线，文件丢失不能解释为未使用。
    /// 参数说明：
    /// - `cluster`：当前集群隔离域。
    /// - `epoch`：部署声明的持久作用域代际。
    /// - `node`：持有本地文件锁的稳定节点。
    /// - `required`：旧启动记录是否已经依赖此持久基线。
    ///
    /// 返回：相同作用域的完整基线已落盘；缺失、异作用域和磁盘失败阻止启动。
    pub(super) fn prepare_assertions(
        &self,
        cluster: &str,
        epoch: &str,
        node: &str,
        required: bool,
    ) -> anyhow::Result<()> {
        let mut state = self
            .assertions
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if let Some(state) = state.as_ref() {
            anyhow::ensure!(
                state.cluster_id == cluster && state.scope_epoch == epoch && state.node_id == node,
                "assertion_baseline_scope"
            );
            return state.replay.validate();
        }
        // 曾经启用的基线缺失必须人工恢复，不能为重启创建新的空消费目录。
        anyhow::ensure!(!required, "assertion_baseline_missing");
        let initial = State {
            cluster_id: cluster.into(),
            scope_epoch: epoch.into(),
            node_id: node.into(),
            replay: Replay::default(),
        };
        persist(&self.path, &initial)?;
        *state = Some(initial);
        Ok(())
    }

    /// 业务作用：在独占数据卷中持久消费代理断言，收到确认之前不允许发布会话身份。
    /// 参数说明：
    /// - `key`：已验签断言的作用域摘要。
    /// - `owner`：精确会话认证尝试的摘要。
    /// - `expires_at`：原始断言到期毫秒时间。
    /// - `now`：本次消费的本地时间观察。
    ///
    /// 返回：文件和目录同步后成功；不确定持久结果封闭后续消费直至重新恢复基线。
    pub fn consume_assertion(
        &self,
        key: &str,
        owner: &str,
        expires_at: UnixMillis,
        now: UnixMillis,
    ) -> anyhow::Result<()> {
        let mut state = self
            .assertions
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        anyhow::ensure!(
            !self.assertion_failed.load(Ordering::Acquire),
            "assertion_store_unavailable"
        );
        let current = state
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("assertion_baseline_missing"))?;
        let mut replay = current.replay.clone();
        replay.consume(key, owner, expires_at, now)?;
        let candidate = State {
            cluster_id: current.cluster_id.clone(),
            scope_epoch: current.scope_epoch.clone(),
            node_id: current.node_id.clone(),
            replay,
        };
        if let Err(error) = persist(&self.path, &candidate) {
            // rename 或目录同步失败可能已经改变磁盘；保持关闭，不能用旧内存状态再接受同一断言。
            self.assertion_failed.store(true, Ordering::Release);
            return Err(error);
        }
        *current = candidate;
        Ok(())
    }
}

/// 业务作用：在持有节点文件锁后恢复有界消费文件，不读取未提交候选。
/// 参数说明：
/// - `path`：部署冻结的路由账本位置，用独立扩展名隔离消费状态。
///
/// 返回：完整基线或首次启动的空状态；损坏文件不能自动重建。
pub(super) fn load(path: &Path) -> anyhow::Result<Option<State>> {
    let file = match File::open(path.with_extension("assertions")) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let mut bytes = Vec::new();
    file.take(4 * 1024 * 1024 + 1).read_to_end(&mut bytes)?;
    let state: State = crate::domain::message::json::decode_bounded(
        &bytes,
        4 * 1024 * 1024,
        8,
        crate::domain::assertion::CAPACITY,
    )?;
    state.replay.validate()?;
    Ok(Some(state))
}

/// 业务作用：先同步消费候选，再替换正式文件并同步目录，确保接入许可不早于持久提交。
/// 参数说明：
/// - `path`：唯一节点账本的位置。
/// - `state`：已校验的完整消费候选。
///
/// 返回：消费基线稳定落盘；失败不能被解释为消费一定没有发生。
fn persist(path: &Path, state: &State) -> anyhow::Result<()> {
    let bytes = serde_json::to_vec(state)?;
    anyhow::ensure!(
        bytes.len() <= 4 * 1024 * 1024,
        "assertion_snapshot_capacity"
    );
    let pending = path.with_extension("assertions-pending");
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&pending)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    std::fs::rename(pending, path.with_extension("assertions"))?;
    File::open(
        path.parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new(".")),
    )?
    .sync_all()?;
    Ok(())
}
