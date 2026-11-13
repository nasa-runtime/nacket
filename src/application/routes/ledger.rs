mod assertions;

use crate::{config::RemoteEndpoint, domain::route::RouteDefinition};
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    fs::{File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::Mutex,
};

/// 节点已经观察到的持久控制高水位；随数据卷保留，不能随进程重启归零。
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ControlMark {
    /// 当前数据卷已经观察的控制集群身份，不能静默切换集群。
    cluster_id: String,
    /// 控制作用域代际，防止旧数据卷接入重建后的不同权威。
    scope_epoch: String,
    /// 持久高水位所属节点身份，不能复制为其它节点的有效证据。
    node_id: String,
    /// 最近已确认控制快照的十进制序号，拒绝观察回退。
    sequence: String,
    /// 该序号完整快照的摘要，同序号内容变化视为权威不一致。
    digest: String,
}

/// 持久记录同时冻结路由与身份端点，引用资源变化必须提交新的路由定义。
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RouteRecord {
    /// 已经持久接受的完整路由定义，重启不从空配置推断在线状态。
    pub definition: RouteDefinition,
    #[serde(default)]
    /// 随该定义冻结的远程端点配置，外部目录变更不能悄然改变旧 revision。
    pub resolved_endpoints: BTreeMap<String, RemoteEndpoint>,
    #[serde(default)]
    /// 随该定义冻结的身份验证策略，重启使用同一授权边界。
    pub resolved_verifiers: BTreeMap<String, crate::config::identity::Verifier>,
}

/// 单节点控制账本的唯一进程 owner；文件锁防止两个进程竞争同一目录。
pub struct LocalLedger {
    /// 本节点路由账本的正式文件路径，替换必须完成持久写入顺序。
    path: PathBuf,
    /// 数据卷唯一进程锁的实际文件 owner，整个账本存活期间保留。
    _lock: File,
    /// 本节点已持久观察的控制身份和高水位，空值只表示尚未建立标记。
    control: Mutex<Option<ControlMark>>,
    /// 本地单次断言消费状态，序列化消费与时间下界更新。
    assertions: Mutex<Option<assertions::State>>,
    /// 断言持久结果不确定后的永久保护标志，禁止继续凭内存放行。
    assertion_failed: std::sync::atomic::AtomicBool,
}

impl LocalLedger {
    /// 业务作用：确定节点是否已有持久控制观察，已运行的数据卷不能再次批准建立空权威。
    /// 参数说明: 无。
    ///
    /// 返回：存在已落盘控制基线时为 true。
    pub fn has_control_mark(&self) -> bool {
        match self.control.try_lock() {
            Ok(mark) => mark.is_some(),
            // 存在未结束的高水位提交时不能把目录视为全新，读取也不等待磁盘锁。
            Err(_) => true,
        }
    }

    /// 业务作用：在使用控制快照前持久推进本节点高水位，检测已观察序号回退与同序号异内容。
    /// 参数说明：
    /// - `cluster`：固定集群作用域。
    /// - `epoch`：受信恢复代际。
    /// - `node`：稳定节点身份。
    /// - `sequence`：后端已经确认的控制提交序号。
    /// - `digest`：与已提交完整快照相符的后端内容摘要。
    ///
    /// 返回：快照被纳入持久观察后成功；回退、内容冲突或磁盘失败阻止使用该快照。
    pub fn observe_control(
        &self,
        cluster: &str,
        epoch: &str,
        node: &str,
        sequence: u64,
        digest: &str,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(
            (40..=64).contains(&digest.len()) && digest.bytes().all(|b| b.is_ascii_hexdigit()),
            "control_checkpoint_digest_invalid"
        );
        let mut previous = self
            .control
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if let Some(old) = previous.as_ref() {
            anyhow::ensure!(
                old.cluster_id == cluster && old.scope_epoch == epoch && old.node_id == node,
                "control_checkpoint_scope_mismatch"
            );
            let old_sequence: u64 = old.sequence.parse()?;
            anyhow::ensure!(sequence >= old_sequence, "control_baseline_regression");
            if sequence == old_sequence {
                anyhow::ensure!(old.digest == digest, "control_baseline_conflict");
                return Ok(());
            }
        }
        let mark = ControlMark {
            cluster_id: cluster.into(),
            scope_epoch: epoch.into(),
            node_id: node.into(),
            sequence: sequence.to_string(),
            digest: digest.into(),
        };
        let path = self.path.with_extension("control");
        let pending = self.path.with_extension("control-pending");
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&pending)?;
        file.write_all(&serde_json::to_vec(&mark)?)?;
        file.sync_all()?;
        // 先完成数据卷落盘，再允许依赖该快照授予控制权或本地业务资格。
        std::fs::rename(pending, &path)?;
        File::open(
            path.parent()
                .filter(|p| !p.as_os_str().is_empty())
                .unwrap_or_else(|| Path::new(".")),
        )?
        .sync_all()?;
        *previous = Some(mark);
        Ok(())
    }
    /// 业务作用：在独占文件锁下递增单节点启动代际，随机连接标识不能替代持久围栏。
    /// 参数说明：
    /// - `cluster`：部署集群标识。
    /// - `epoch`：当前控制基线代际。
    /// - `node`：稳定节点标识。
    ///
    /// 返回：已同步落盘的新代际；作用域变化或计数损坏阻止启动。
    pub fn incarnation(&self, cluster: &str, epoch: &str, node: &str) -> anyhow::Result<String> {
        let path = self.path.with_extension("incarnation");
        let previous: Option<serde_json::Value> = match File::open(&path) {
            Ok(file) => {
                let mut bytes = Vec::new();
                file.take(4097).read_to_end(&mut bytes)?;
                Some(crate::domain::message::json::decode(&bytes, 4096)?)
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => return Err(error.into()),
        };
        let counter = if let Some(previous) = previous {
            anyhow::ensure!(
                previous["clusterId"] == cluster
                    && previous["scopeEpoch"] == epoch
                    && previous["nodeId"] == node,
                "local_incarnation_scope_mismatch"
            );
            self.prepare_assertions(
                cluster,
                epoch,
                node,
                previous["assertions"].as_bool() == Some(true),
            )?;
            let counter = previous["incarnation"]
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("local_incarnation_invalid"))?;
            crate::domain::message::envelope::revision(counter)?
                .checked_add(1)
                .ok_or_else(|| anyhow::anyhow!("local_incarnation_exhausted"))?
        } else {
            self.prepare_assertions(cluster, epoch, node, false)?;
            1
        };
        let incarnation = counter.to_string();
        let bytes = serde_json::to_vec(&serde_json::json!({
            "clusterId": cluster,
            "scopeEpoch": epoch,
            "nodeId": node,
            "incarnation": incarnation,
            "assertions": true
        }))?;
        let pending = self.path.with_extension("incarnation-pending");
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&pending)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        // 新代际必须先稳定落盘；此后启动失败也不能复用已经分配的序号。
        std::fs::rename(pending, &path)?;
        File::open(
            path.parent()
                .filter(|p| !p.as_os_str().is_empty())
                .unwrap_or_else(|| Path::new(".")),
        )?
        .sync_all()?;
        Ok(incarnation)
    }

    /// 业务作用：取得单节点账本所有权并恢复最后完整提交的路由定义。
    /// 参数说明：
    ///
    /// - `path`：部署提供的持久路由文件路径。
    ///
    /// 返回：独占 owner 与完整记录；锁冲突、容量超限或损坏账本阻止启动。
    pub fn open(path: &Path) -> anyhow::Result<(Self, BTreeMap<String, RouteRecord>)> {
        let parent = path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        std::fs::create_dir_all(parent)?;
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path.with_extension("lock"))?;
        // 独占锁必须早于恢复或提交，避免同一路由文件被并发启动覆盖。
        lock.try_lock_exclusive()?;
        let records = match File::open(path) {
            Ok(file) => {
                let mut bytes = Vec::new();
                file.take(2 * 1024 * 1024 + 1).read_to_end(&mut bytes)?;
                anyhow::ensure!(
                    bytes.len() <= 2 * 1024 * 1024,
                    "route ledger capacity exceeded"
                );
                serde_json::from_slice(&bytes)?
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => BTreeMap::new(),
            Err(error) => return Err(error.into()),
        };
        let control: Option<ControlMark> = match File::open(path.with_extension("control")) {
            Ok(file) => {
                let mut bytes = Vec::new();
                file.take(4097).read_to_end(&mut bytes)?;
                let mark: ControlMark = crate::domain::message::json::decode(&bytes, 4096)?;
                let sequence = mark.sequence.parse::<u64>()?;
                anyhow::ensure!(
                    sequence.to_string() == mark.sequence,
                    "control_checkpoint_sequence_invalid"
                );
                Some(mark)
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => return Err(error.into()),
        };
        Ok((
            Self {
                path: path.to_owned(),
                _lock: lock,
                control: Mutex::new(control),
                assertions: Mutex::new(assertions::load(path)?),
                assertion_failed: std::sync::atomic::AtomicBool::new(false),
            },
            records,
        ))
    }

    /// 业务作用：持久提交完整路由目录，使重启不会被旧 YAML 重新开放已下线路由。
    /// 参数说明：
    ///
    /// - `records`：已经完成结构、引用和单调代次检查的完整候选。
    ///
    /// 返回：文件及目录均同步后成功；失败时调用方必须保持路由保护态。
    pub fn commit(&self, records: &BTreeMap<String, RouteRecord>) -> anyhow::Result<()> {
        let bytes = serde_json::to_vec(records)?;
        anyhow::ensure!(
            bytes.len() <= 2 * 1024 * 1024,
            "route ledger capacity exceeded"
        );
        let temporary = self.path.with_extension("pending");
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&temporary)?;
        // 正式账本只能由普通文件替换，特殊文件不能凭 write 或 sync 返回成功进入恢复路径。
        anyhow::ensure!(
            file.metadata()?.is_file(),
            "route ledger requires a regular file"
        );
        file.write_all(&bytes)?;
        file.sync_all()?;
        // 内容先落盘再原子替换；重启只读取正式文件，不把半写入的候选当作权威。
        std::fs::rename(&temporary, &self.path)?;
        let parent = self
            .path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        File::open(parent)?.sync_all()?;
        Ok(())
    }
}
