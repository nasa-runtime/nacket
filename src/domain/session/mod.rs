//! 每个业务连接的权限、订阅意图及真实写出屏障，不持有远程客户端。

mod closure;
mod maintenance;
pub use closure::Closure;
pub mod notices;
pub mod receipts;

use crate::domain::{
    message::envelope::{identifier, user_identifier, SessionKey},
    route::{RouteGate, WritePermit},
};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex, OnceLock, Weak},
};
use tokio::{sync::watch, time::Instant};

/// 身份证据绑定真实主体和期限，心跳不能延长它。
#[derive(Clone)]
pub struct IdentityGrant {
    /// 已验证主体所属租户，订阅、定向接收和撤权必须保持同一隔离域。
    pub tenant: String,
    /// 已验证用户身份，不从客户端目标或正文推导。
    pub uid: String,
    /// 签发当前身份资格的业务证据 ID，用于关联该次授权。
    pub authorization_id: String,
    /// 身份授权的单调版本，与用户撤权下界比较，不替代成员或群版本。
    pub version: u64,
    /// 当前身份证据的本地单调截止点，心跳和业务流量不能延长。
    pub deadline: Instant,
}

/// 群读取授权与群状态版本分开；订阅意图本身不构成成员资格。
#[derive(Clone)]
pub struct GroupGrant {
    /// 业务签发的本次成员读取授权 ID，订阅意图本身不能生成此证据。
    pub authorization_id: String,
    /// 当前用户在该群的成员授权版本，与成员撤权下界独立比较。
    pub version: u64,
    /// 该授权所依赖的群状态版本，删除或重建群后旧证据失效。
    pub group_version: u64,
    /// 本次订阅读取资格的单调截止点，最终写出同时受身份期限限制。
    pub deadline: Instant,
}

/// 跨会话共享的撤权下界，跨路由定义更新保留已知撤权事实。
#[derive(Default)]
pub struct Revocations {
    /// 当前路由共享的单调撤权投影，所有会话在同一事实域复验。
    state: Mutex<RevocationState>,
}
#[derive(Default)]
struct RevocationState {
    /// 本地撤权事实发生实际变化时推进的缓存代际，不参与业务版本比较。
    generation: u64,
    /// 按租户和用户保存的身份撤权版本下界，不因重连清除。
    subjects: BTreeMap<(String, String), u64>,
    /// 按租户、用户和群保存的成员撤权版本下界。
    memberships: BTreeMap<(String, String, String), u64>,
    /// 按租户和群保存的群状态下界，与个别成员授权版本分开。
    groups: BTreeMap<(String, String), u64>,
    /// 已经精确踢出的完整会话键摘要，只影响对应实例与会话。
    sessions: BTreeSet<String>,
    /// 无法继续保留撤权证据时进入的保护事实，不能通过淘汰有效下界继续放行。
    exhausted: bool,
}

/// 撤权命令不能跨越所属路由，tenant 和用户、群均为明确单值。
pub enum Revocation {
    /// 撤销精确连接，不传播到重新认证得到的新会话。
    Session {
        /// 需要踢出的精确会话完整键编码，不能退化为同用户的所有新连接。
        key: String,
    },
    /// 推进主体身份版本下界，使旧身份资格失效。
    Subject {
        /// 当前撤权命令明确限定的租户，不能传播到其它租户。
        tenant: String,
        /// 主体或成员撤权指定的单个用户，不从目标列表隐式扩大范围。
        uid: String,
        /// 该命令对应版本域要推进的下界，旧值不回退已有撤权事实。
        version: u64,
    },
    /// 推进单个用户对一个订阅目标的成员资格下界。
    Membership {
        /// 当前撤权命令明确限定的租户，不能传播到其它租户。
        tenant: String,
        /// 主体或成员撤权指定的单个用户，不从目标列表隐式扩大范围。
        uid: String,
        /// 成员或群撤权指定的单个群，必须与该命令租户共同定位。
        group: String,
        /// 该命令对应版本域要推进的下界，旧值不回退已有撤权事实。
        version: u64,
    },
    /// 推进订阅目标自身的版本下界，独立于各成员的授权版本。
    Group {
        /// 当前撤权命令明确限定的租户，不能传播到其它租户。
        tenant: String,
        /// 成员或群撤权指定的单个群，必须与该命令租户共同定位。
        group: String,
        /// 该命令对应版本域要推进的下界，旧值不回退已有撤权事实。
        version: u64,
    },
}

impl Revocations {
    /// 业务作用：记录单调撤权事实，容量耗尽时保护整个权限域而不是淘汰仍有效的撤权证据。
    /// 参数说明：
    /// - `command`：已验证来源及路由的撤权命令。
    /// - `maximum`：当前路由可保留的撤权条目上限。
    ///
    /// 返回：撤权下界已经推进或幂等存在；容量不足后该路由所有证据均失效。
    pub fn apply(&self, command: Revocation, maximum: usize) -> anyhow::Result<()> {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let count = state.subjects.len()
            + state.memberships.len()
            + state.groups.len()
            + state.sessions.len();
        let (exists, version) = match &command {
            Revocation::Session {
                key,
            } => {
                anyhow::ensure!(
                    key.len() == 64 && key.bytes().all(|byte| byte.is_ascii_hexdigit()),
                    "invalid_session_revocation"
                );
                (state.sessions.contains(key), 1)
            }
            Revocation::Subject {
                tenant,
                uid,
                version,
            } => (
                state.subjects.contains_key(&(tenant.clone(), uid.clone())),
                *version,
            ),
            Revocation::Membership {
                tenant,
                uid,
                group,
                version,
            } => (
                state
                    .memberships
                    .contains_key(&(tenant.clone(), uid.clone(), group.clone())),
                *version,
            ),
            Revocation::Group {
                tenant,
                group,
                version,
            } => (
                state.groups.contains_key(&(tenant.clone(), group.clone())),
                *version,
            ),
        };
        anyhow::ensure!(version > 0, "invalid_authorization_version");
        if !exists && count >= maximum {
            // 撤权证据不能以缓存淘汰恢复旧资格，容量不足必须停止授权域投递。
            state.exhausted = true;
            state.generation = state.generation.saturating_add(1);
            anyhow::bail!("revocation_capacity");
        }
        let current = match command {
            Revocation::Session {
                key,
            } => {
                if state.sessions.insert(key) {
                    state.generation = state.generation.saturating_add(1);
                    state.exhausted |= state.generation == u64::MAX;
                }
                return Ok(());
            }
            Revocation::Subject {
                tenant,
                uid,
                ..
            } => state.subjects.entry((tenant, uid)).or_default(),
            Revocation::Membership {
                tenant,
                uid,
                group,
                ..
            } => state.memberships.entry((tenant, uid, group)).or_default(),
            Revocation::Group {
                tenant,
                group,
                ..
            } => state.groups.entry((tenant, group)).or_default(),
        };
        if version > *current {
            *current = version;
            state.generation = state.generation.saturating_add(1);
            state.exhausted |= state.generation == u64::MAX;
        }
        Ok(())
    }

    /// 业务作用：在接纳证据和开始实际写出时同时检查身份与群撤权下界。
    /// 参数说明：
    /// - `identity`：当前会话身份。
    /// - `group`：群投递时的群 ID 与授权证据；用户定向传 None。
    ///
    /// 返回：证据严格晚于全部适用撤权事实且未过期时允许。
    pub fn allows(&self, identity: &IdentityGrant, group: Option<(&str, &GroupGrant)>) -> bool {
        let now = Instant::now();
        identity.deadline > now
            && group.is_none_or(|(_, grant)| grant.deadline > now)
            && self.allows_versions(
                &identity.tenant,
                &identity.uid,
                identity.version,
                group.map(|(group, grant)| (group, grant.version, grant.group_version)),
            )
    }

    /// 业务作用：复验已经取得写出责任时冻结的授权版本，后续身份刷新不能替旧写出覆盖失权事实。
    /// 参数说明：
    /// - `tenant`：会话固定租户。
    /// - `uid`：会话固定主体。
    /// - `subject_version`：取得写出许可时的用户授权版本。
    /// - `group`：可选群、成员版本与群状态版本。
    ///
    /// 返回：所有适用版本严格晚于撤权下界时允许；期限由原许可独立限制。
    fn allows_versions(
        &self,
        tenant: &str,
        uid: &str,
        subject_version: u64,
        group: Option<(&str, u64, u64)>,
    ) -> bool {
        let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if state.exhausted
            || subject_version
                <= *state
                    .subjects
                    .get(&(tenant.into(), uid.into()))
                    .unwrap_or(&0)
        {
            return false;
        }
        if let Some((group, membership_version, group_version)) = group {
            if membership_version
                <= *state
                    .memberships
                    .get(&(tenant.into(), uid.into(), group.into()))
                    .unwrap_or(&0)
                || group_version
                    <= *state
                        .groups
                        .get(&(tenant.into(), group.into()))
                        .unwrap_or(&0)
            {
                return false;
            }
        }
        true
    }

    /// 业务作用：检查精确会话的永久关闭事实，不影响同用户重新认证得到的新会话。
    /// 参数说明：
    /// - `key`：会话创建时冻结的完整主体及 SessionKey 摘要。
    ///
    /// 返回：没有已知踢出事实且撤权域容量正常时为 true。
    fn session_allowed(&self, key: &str) -> bool {
        let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        !state.exhausted && !state.sessions.contains(key)
    }
}

/// 新授权结果必须携带同一意图序号，迟到结果不能覆盖较新的取消。
pub struct SubscriptionIntent {
    /// 本次订阅意图序号，迟到授权必须仍匹配它才可生效。
    pub sequence: u64,
    /// 本次申请的有限群集合，持有意图不代表已经得到读取资格。
    pub groups: Vec<String>,
    /// 原会话弱引用，用于取消时仅回收属于本意图的预约，不延长会话寿命。
    session: Weak<Session>,
}

impl Drop for SubscriptionIntent {
    /// 业务作用：操作未执行、等待取消或远程拒绝时回收仍属于本意图的未授权预约。
    /// 参数说明: 无。
    ///
    /// 返回：已有授权和更新意图不受影响；会话已经销毁时无需再次清理。
    fn drop(&mut self) {
        if let Some(session) = self.session.upgrade() {
            session.fail_subscribe(self);
        }
    }
}

struct Subscription {
    /// 当前会话是否仍希望订阅该群，取消先清除此意图再等待旧写出。
    desired: bool,
    /// 该群当前意图的序号，隔离重复 sub、unsub 和迟到授权结果。
    sequence: u64,
    /// 当前已取得且仍需逐次复验的成员读取证据；待授权时为 None。
    grant: Option<GroupGrant>,
    /// 跟随实际待决或有效订阅 owner 的观察责任，状态迁移时同步转移。
    observed: Option<crate::observability::Reservation>,
}
struct State {
    /// 最近成功业务动作或完整业务写出的单调时刻，心跳与资格刷新不更新。
    last_activity: Option<Instant>,
    /// 首次关闭的原因、时刻和取消数量，与权限撤销在同一临界区发布。
    closure: Option<Closure>,
    /// 会话是否仍允许产生新的权限；一旦关闭不能由迟到结果重开。
    open: bool,
    /// 确认、初始化与必要准入是否已完成，未就绪不能接收业务推送。
    ready: bool,
    /// 当前已核验的主体及其资格期限，刷新仍不能改变会话租户或用户。
    identity: IdentityGrant,
    /// 可选用户定向接收资格的单调截止点；没有有效资格不能仅凭登录接收定向消息。
    direct: Option<Instant>,
    /// 会话订阅意图的单调序号来源，新的取消和订阅不能复用旧序号。
    sequence: u64,
    /// 按群保存的当前意图及授权，条数受会话容量限制。
    subscriptions: BTreeMap<String, Subscription>,
    /// 已经取得最终写出资格的实际 writer 目录，直到许可 owner 释放才移除。
    writing: BTreeMap<u64, Writing>,
    /// 下一次实际写出责任的唯一序号，用于精确等待旧 writer。
    next_write: u64,
    /// 已发出临近到期通知的身份截止点，同一资格期限最多提醒一次。
    warned_deadline: Option<Instant>,
    /// 当前最早订阅截止点的缓存，只有到期时才需要扫描订阅目录。
    next_subscription_expiry: Option<Instant>,
}

/// 实际写出的授权版本固定到取得许可的时刻，不跟随后续刷新或重新订阅变化。
struct Writing {
    /// 本次实际写出所属的单个群；无群时不参与订阅取消屏障。
    group: Option<String>,
    /// 取得写出许可时冻结的身份版本，后续刷新不能替旧 writer 升级。
    subject_version: u64,
    /// 取得写出许可时冻结的成员版本，重新订阅不能复活旧资格。
    membership_version: u64,
    /// 取得写出许可时冻结的群状态版本，群更新不能转移旧写出责任。
    group_version: u64,
}

/// 会话的权限权威；候选索引和排队消息必须在写出前重新访问它。
pub struct Session {
    /// 已验证身份创建本次会话的单调起点，用于存续时长观测。
    created_at: Instant,
    /// 当前精确会话完整键，包含集群、实例、路由定义和会话唯一标识。
    pub key: SessionKey,
    /// 当前冻结定义的路由门禁，最终发送须同时持有路由与会话资格。
    route: Arc<RouteGate>,
    /// 当前会话绑定的数值 revision，不随目录中的新定义变化。
    route_revision: u64,
    /// 同一路由共享的单调撤权投影，不能由单会话刷新清除。
    revocations: Arc<Revocations>,
    /// 当前完整会话键的稳定编码，用于精确踢出检查。
    revocation_key: String,
    /// 会话资格、订阅意图和实际 writer 的统一短临界区，不跨网络等待持有。
    state: Mutex<State>,
    /// 向正在等待的取消和下线屏障发布状态变动，不把通知当成权限证明。
    changes: watch::Sender<u64>,
    /// 当前冻结路由允许本会话保留的最大订阅意图数量。
    maximum_subscriptions: usize,
    /// 首次接线后固定的节点观察来源，既有订阅责任不能迁移到其它来源。
    metrics: OnceLock<Arc<crate::observability::Metrics>>,
    /// 当前精确会话的独立状态通知 owner，权限变化先行，通知到达不构成授权。
    pub notices: OnceLock<Arc<notices::Notices>>,
}

/// 本次写出持有的会话与路由双重许可，释放后才可完成下线或取消订阅 ACK。
pub struct DeliveryPermit {
    /// 本次真实写出所占用的会话 owner，保持目录直到实际写出结束。
    session: Arc<Session>,
    /// 本次写出在会话目录中的唯一责任序号，释放时只移除自身。
    entry: u64,
    /// 消息、身份、订阅及其它限制共同收窄的单调写出截止点。
    pub deadline: Instant,
    /// 同时持有的冻结路由写出许可，旧路由屏障必须等其真实释放。
    _route: WritePermit,
}

/// 一次取消只等待它撤销的旧意图写出，不等待之后重新订阅产生的新消息。
pub struct SubscriptionBarrier {
    /// 需要等待旧写出结束的原会话，不能改查重连后的新会话。
    session: Arc<Session>,
    /// 取消时捕获的旧 writer 序号集合，不等待后来重新授权产生的新写出。
    entries: Vec<u64>,
}

impl Session {
    /// 业务作用：将只读授权缓存绑定到当前撤权投影，重放相同撤权不使代际继续增长。
    /// 参数说明: 无。
    ///
    /// 返回：路由共享的本地缓存代际，不可用于授予权限或代替业务版本。
    pub fn authorization_generation(&self) -> u64 {
        self.revocations
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .generation
    }

    /// 业务作用：缓存复用前检查所有资格仍满足身份、路由和持久撤权门禁。
    /// 参数说明：
    /// - `grants`：原始截止点保持不变的目标资格集合。
    ///
    /// 返回：全部资格仍有效时允许返回；实际订阅应用还需复验当前意图。
    pub fn grants_allowed(&self, grants: &BTreeMap<String, GroupGrant>) -> bool {
        let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        self.eligible(&state)
            && grants.iter().all(|(group, grant)| {
                self.revocations
                    .allows(&state.identity, Some((group, grant)))
            })
    }
    /// 业务作用：报告会话资格结束的封闭原因，日志不读取主体、令牌或业务正文。
    /// 参数说明: 无。
    ///
    /// 返回：关闭、授权失效、精确踢出或路由依赖原因；仍有资格时返回 None。
    pub fn unavailable_reason(&self) -> Option<&'static str> {
        let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if !state.open {
            return Some("session_closed");
        }
        if !self.revocations.allows(&state.identity, None) {
            return Some("authorization_unavailable");
        }
        if !self.revocations.session_allowed(&self.revocation_key) {
            return Some("session_revoked");
        }
        self.route.unavailable_reason()
    }
    /// 业务作用：在确认会话且尚未登记订阅时绑定节点事实源，订阅责任随条目共同释放。
    /// 参数说明：
    /// - `metrics`：节点的静态指标目录。
    ///
    /// 返回：首次绑定成功；重复绑定不迁移已存在的责任。
    pub fn observe(&self, metrics: Arc<crate::observability::Metrics>) {
        let _ = self.metrics.set(metrics);
    }

    /// 业务作用：为一个完整会话身份创建尚未 Ready 的权限状态。
    /// 参数说明：
    /// - `key`：包含节点代际和路由定义的精确会话键。
    /// - `identity`：已验证身份的有界证据。
    /// - `route`：当前权威定义对应的门禁。
    /// - `revocations`：同一路由共享的撤权事实。
    /// - `maximum_subscriptions`：包含待决意图的单连接订阅上限。
    ///
    /// 返回：待确认会话；身份、代次或容量非法时拒绝创建。
    pub fn new(
        key: SessionKey,
        identity: IdentityGrant,
        route: Arc<RouteGate>,
        revocations: Arc<Revocations>,
        maximum_subscriptions: usize,
    ) -> anyhow::Result<Arc<Self>> {
        key.validate()?;
        identifier(&identity.tenant)?;
        // 会话构造保留同一准入门禁，新增接入适配器也不能登记空白用户主体。
        user_identifier(&identity.uid)?;
        identifier(&identity.authorization_id)?;
        let revocation_key = crate::domain::authorization::Action::Kick {
            tenant_id: identity.tenant.clone(),
            uid: identity.uid.clone(),
            session_key: key.clone(),
        }
        .key()?;
        anyhow::ensure!(
            maximum_subscriptions <= 4096
                && identity.version > 0
                && revocations.allows(&identity, None)
                && revocations.session_allowed(&revocation_key),
            "invalid_identity_or_capacity"
        );
        let route_revision = key.route_revision.parse()?;
        let (changes, _) = watch::channel(0);
        Ok(Arc::new(Self {
            created_at: Instant::now(),
            key,
            route,
            route_revision,
            revocations,
            revocation_key,
            state: Mutex::new(State {
                closure: None,
                open: true,
                ready: false,
                direct: Some(identity.deadline),
                identity,
                sequence: 0,
                subscriptions: BTreeMap::new(),
                last_activity: None,
                writing: BTreeMap::new(),
                next_write: 0,
                warned_deadline: None,
                next_subscription_expiry: None,
            }),
            changes,
            maximum_subscriptions,
            metrics: OnceLock::new(),
            notices: OnceLock::new(),
        }))
    }

    /// 业务作用：在确认和必要初始化完成后开放会话，最后复验身份及路由仍然有效。
    /// 参数说明: 无。
    ///
    /// 返回：会话首次进入 Ready；已经关闭、过期或重复确认时拒绝。
    pub fn ready(&self) -> anyhow::Result<()> {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        anyhow::ensure!(
            state.open
                && !state.ready
                && self.revocations.allows(&state.identity, None)
                && self.revocations.session_allowed(&self.revocation_key)
                && self.route.acquire(self.route_revision).is_some(),
            "session_not_eligible"
        );
        // 所有门禁通过后才发布 Ready；握手成功本身不能授权业务收发。
        state.ready = true;
        Ok(())
    }

    /// 业务作用：复制当前可信主体供有界远程请求使用，不把锁带到网络等待中。
    /// 参数说明: 无。
    ///
    /// 返回：当前身份快照；使用结果前仍须复验会话门禁。
    pub fn identity(&self) -> IdentityGrant {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .identity
            .clone()
    }

    /// 业务作用：更新用户定向下行资格，控制回复不借用这项业务权限。
    /// 参数说明：
    /// - `deadline`：允许接收时的固定期限，None 表示立即关闭定向下行。
    ///
    /// 返回：后续 writer 必须按新资格复验，已关闭会话不会重新开放。
    pub fn direct_permission(&self, deadline: Option<Instant>) {
        self.state.lock().unwrap_or_else(|e| e.into_inner()).direct = deadline;
    }

    /// 业务作用：在原身份仍有效时接纳同一主体的新授权，不能恢复已关闭会话或降低版本。
    /// 参数说明：
    /// - `authorization_id`：远程批准的新授权标识。
    /// - `version`：同一主体的单调授权版本。
    /// - `deadline`：经过远程预算限制的截止点。
    ///
    /// 返回：新身份授权生效，群授权仍保留各自原始期限；失效或旧证据拒绝。
    pub fn refresh(
        &self,
        authorization_id: String,
        version: u64,
        deadline: Instant,
    ) -> anyhow::Result<()> {
        identifier(&authorization_id)?;
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        anyhow::ensure!(
            self.eligible(&state) && version >= state.identity.version && deadline > Instant::now(),
            "refresh_denied"
        );
        let identity = IdentityGrant {
            authorization_id,
            version,
            deadline,
            ..state.identity.clone()
        };
        anyhow::ensure!(self.revocations.allows(&identity, None), "refresh_revoked");
        let wall = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis();
        let expires = crate::domain::message::UnixMillis::new(u64::try_from(
            wall + deadline
                .saturating_duration_since(Instant::now())
                .as_millis(),
        )?)
        .map_err(anyhow::Error::msg)?;
        state.identity = identity;
        if let Some(notices) = self.notices.get() {
            // 客户端只通过绑定本会话且严格递增的状态通知接收新期限；通知失败不能留下双方不一致的可用连接。
            if let Err(error) = notices.push(
                notices::Reason::AuthorizationRefreshed,
                None,
                Some((version, None)),
                Some(expires),
                false,
            ) {
                state.close("authorization_notice_unavailable");
                return Err(error);
            }
        }
        Ok(())
    }

    /// 业务作用：登记一批订阅意图并预约容量，远程允许之前不开放投递。
    /// 参数说明：
    /// - `groups`：已经按路由 schema 解码的群 ID 列表。
    ///
    /// 返回：关联意图；列表非法、容量不足或会话失效时不改变已有订阅。
    pub fn begin_subscribe(
        self: &Arc<Self>,
        mut groups: Vec<String>,
    ) -> anyhow::Result<SubscriptionIntent> {
        anyhow::ensure!(
            !groups.is_empty() && groups.len() <= 128,
            "subscription_batch_capacity"
        );
        for group in &groups {
            identifier(group)?;
        }
        groups.sort();
        groups.dedup();
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        anyhow::ensure!(self.eligible(&state), "session_not_ready");
        let added = groups
            .iter()
            .filter(|g| !state.subscriptions.contains_key(*g))
            .count();
        anyhow::ensure!(
            state.subscriptions.len() + added <= self.maximum_subscriptions,
            "subscription_capacity"
        );
        let sequence = state
            .sequence
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("intent_exhausted"))?;
        state.sequence = sequence;
        for group in &groups {
            let entry = state
                .subscriptions
                .entry(group.clone())
                .or_insert_with(|| Subscription {
                    desired: false,
                    sequence,
                    grant: None,
                    observed: self.metrics.get().map(|metrics| {
                        metrics.reserve_route(&self.key.route_id, "subscriptions_pending", 1)
                    }),
                });
            entry.desired = true;
            entry.sequence = sequence;
        }
        Ok(SubscriptionIntent {
            sequence,
            groups,
            session: Arc::downgrade(self),
        })
    }

    /// 业务作用：原子接纳完整批次授权，较新取消、撤权或身份变化会使整个旧批次失效。
    /// 参数说明：
    /// - `intent`：发起请求时登记的会话意图。
    /// - `grants`：按群精确关联的可信授权结果，不允许缺项或额外项。
    ///
    /// 返回：全部群的授权一次性生效；失败保留已有合法授权并释放本次未授权预约。
    pub fn complete_subscribe(
        &self,
        intent: &SubscriptionIntent,
        grants: BTreeMap<String, GroupGrant>,
    ) -> anyhow::Result<()> {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let valid = self.eligible(&state)
            && grants.len() == intent.groups.len()
            && intent.groups.iter().all(|group| {
                state
                    .subscriptions
                    .get(group)
                    .is_some_and(|entry| entry.desired && entry.sequence == intent.sequence)
                    && grants.get(group).is_some_and(|grant| {
                        identifier(&grant.authorization_id).is_ok()
                            && self
                                .revocations
                                .allows(&state.identity, Some((group, grant)))
                    })
            });
        if !valid {
            // 迟到 allow 不得重建已取消的订阅，未得到合法证据的预约也不能永久占位。
            Self::release_intent(&mut state, intent);
            anyhow::bail!("subscription_denied_or_superseded");
        }
        for (group, grant) in grants {
            state.next_subscription_expiry = Some(
                state
                    .next_subscription_expiry
                    .map_or(grant.deadline, |current| current.min(grant.deadline)),
            );
            let entry = state
                .subscriptions
                .get_mut(&group)
                .expect("validated subscription");
            entry.grant = Some(grant);
            if let Some(observation) = &mut entry.observed {
                observation.transfer("subscriptions");
            }
        }
        Ok(())
    }

    /// 业务作用：远程拒绝或取消时释放本次没有有效证据的订阅预约。
    /// 参数说明：
    /// - `intent`：需要终止的原始操作。
    ///
    /// 返回：只清理仍属于此意图的条目，不影响后续订阅。
    pub fn fail_subscribe(&self, intent: &SubscriptionIntent) {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        Self::release_intent(&mut state, intent);
    }

    /// 业务作用：保持原有有效订阅，同时收回没有授权证据的待决意图。
    /// 参数说明：
    /// - `state`：已独占的会话状态。
    /// - `intent`：失败的操作关联信息。
    ///
    /// 返回：未授权条目被移除；其它操作的条目不变。
    fn release_intent(state: &mut State, intent: &SubscriptionIntent) {
        for group in &intent.groups {
            if state
                .subscriptions
                .get(group)
                .is_some_and(|entry| entry.sequence == intent.sequence && entry.grant.is_none())
            {
                state.subscriptions.remove(group);
            }
        }
    }

    /// 业务作用：立即取消本连接群投递资格，返回只覆盖旧写出的确认屏障。
    /// 参数说明：
    /// - `groups`：本连接希望停止接收的群；不操作业务成员关系。
    ///
    /// 返回：旧群写出屏障；重复取消有效，不依赖重新取得成员授权。
    pub fn unsubscribe(self: &Arc<Self>, groups: &[String]) -> anyhow::Result<SubscriptionBarrier> {
        anyhow::ensure!(
            !groups.is_empty() && groups.len() <= 128,
            "subscription_batch_capacity"
        );
        for group in groups {
            identifier(group)?;
        }
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        anyhow::ensure!(state.open, "session_closed");
        let mut entries = Vec::new();
        for group in groups {
            // 先撤销未来许可，再收集真实在途责任；取消 ACK 不越过正在写出的旧群消息。
            state.subscriptions.remove(group);
            entries.extend(
                state
                    .writing
                    .iter()
                    .filter(|(_, record)| record.group.as_ref() == Some(group))
                    .map(|(id, _)| *id),
            );
        }
        Ok(SubscriptionBarrier {
            session: self.clone(),
            entries,
        })
    }

    /// 业务作用：取得入队候选的订阅代次，排队期间变化仍由最终写出检查拦截。
    /// 参数说明：
    /// - `group`：群投递时为群 ID，用户定向为 None。
    ///
    /// 返回：有效候选的代次；无授权或已取消的群没有候选。
    pub fn candidate(&self, group: Option<&str>) -> Option<u64> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if !self.eligible(&state) {
            return None;
        }
        match group {
            None => Some(0),
            Some(group) => {
                let entry = state.subscriptions.get(group)?;
                if entry.desired
                    && self
                        .revocations
                        .allows(&state.identity, Some((group, entry.grant.as_ref()?)))
                {
                    Some(entry.sequence)
                } else {
                    None
                }
            }
        }
    }

    /// 业务作用：在主体消费域记录成功的业务活动，供空闲连接观察使用。
    /// 参数说明: 无。
    ///
    /// 返回：有效会话更新活动时刻，已关闭或失权会话保持原值。
    pub fn business_activity(&self) {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if self.eligible(&state) {
            state.last_activity = Some(Instant::now());
        }
    }

    /// 业务作用：读取当前可接流会话及有效订阅目标，观察不授予新的资格。
    /// 参数说明：
    /// - `window`：最近业务活动的判断窗口。
    /// - `visit`：在短临界区内接收目标引用的有界观察器，不得发起网络或回入会话。
    ///
    /// 返回：有效会话的活跃标志；初始化、关闭、过期或受保护状态返回 None。
    pub fn observe_activity(
        &self,
        window: std::time::Duration,
        mut visit: impl FnMut(&str, &str, &str),
    ) -> Option<bool> {
        let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if !self.eligible(&state) {
            return None;
        }
        for (target, subscription) in &state.subscriptions {
            if subscription.desired
                && subscription.grant.as_ref().is_some_and(|grant| {
                    self.revocations
                        .allows(&state.identity, Some((target, grant)))
                })
            {
                visit(&self.key.route_id, &state.identity.tenant, target);
            }
        }
        Some(
            state
                .last_activity
                .is_some_and(|last| last.elapsed() < window),
        )
    }

    /// 业务作用：在 writer 即将发送前原子复验身份、订阅代次与路由许可，并登记真实在途责任。
    /// 参数说明：
    /// - `group`：群目标或用户定向标志。
    /// - `generation`：入队时记录的订阅代次。
    /// - `message_deadline`：消息入队时固定的最晚投递点。
    ///
    /// 返回：覆盖最短授权期限的写出许可；任一条件失效均不允许写出。
    pub fn acquire(
        self: &Arc<Self>,
        group: Option<&str>,
        generation: u64,
        message_deadline: Instant,
    ) -> Option<DeliveryPermit> {
        self.acquire_checked(group, generation, message_deadline, false)
    }

    /// 业务作用：在实际写出用户消息前复验定向接收开关和授权有效期。
    /// 参数说明：
    /// - `message_deadline`：消息自身的最晚投递点。
    ///
    /// 返回：同时覆盖身份、接收权限与路由的许可；控制回复使用普通 acquire。
    pub fn acquire_direct(self: &Arc<Self>, message_deadline: Instant) -> Option<DeliveryPermit> {
        self.acquire_checked(None, 0, message_deadline, true)
    }

    /// 业务作用：在同一会话锁内校验写出权限并登记责任，避免读取权限和记录在途之间出现空档。
    /// 参数说明：
    /// - `group`：群目标或用户定向标志。
    /// - `generation`：入队时的订阅代次。
    /// - `message_deadline`：消息原始截止时间。
    /// - `require_direct`：是否额外要求用户定向接收资格。
    ///
    /// 返回：最短有效期限的真实写出许可；任何门禁失败均不登记责任。
    fn acquire_checked(
        self: &Arc<Self>,
        group: Option<&str>,
        generation: u64,
        message_deadline: Instant,
        require_direct: bool,
    ) -> Option<DeliveryPermit> {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if !self.eligible(&state) || message_deadline <= Instant::now() {
            return None;
        }
        let mut deadline = message_deadline.min(state.identity.deadline);
        if require_direct {
            deadline = deadline.min(state.direct?);
            if deadline <= Instant::now() {
                return None;
            }
        }
        let mut membership_version = 0;
        let mut group_version = 0;
        if let Some(group) = group {
            let entry = state.subscriptions.get(group)?;
            let grant = entry.grant.as_ref()?;
            if !entry.desired
                || entry.sequence != generation
                || !self
                    .revocations
                    .allows(&state.identity, Some((group, grant)))
            {
                return None;
            }
            deadline = deadline.min(grant.deadline);
            membership_version = grant.version;
            group_version = grant.group_version;
        } else if generation != 0 {
            return None;
        }
        let permit = self.route.acquire(self.route_revision)?;
        if let Some(control_deadline) = permit.deadline {
            deadline = deadline.min(control_deadline);
        }
        let entry = state.next_write.checked_add(1)?;
        state.next_write = entry;
        let subject_version = state.identity.version;
        state.writing.insert(
            entry,
            Writing {
                group: group.map(str::to_owned),
                subject_version,
                membership_version,
                group_version,
            },
        );
        Some(DeliveryPermit {
            session: self.clone(),
            entry,
            deadline,
            _route: permit,
        })
    }

    /// 业务作用：在业务入口统一检查 Ready、身份与路由准入，禁止认证结果迟到恢复权限。
    /// 参数说明：
    /// - `state`：同一会话锁下的当前状态。
    ///
    /// 返回：全部准入条件同时满足时为 true。
    fn eligible(&self, state: &State) -> bool {
        state.open
            && state.ready
            && self.revocations.allows(&state.identity, None)
            && self.revocations.session_allowed(&self.revocation_key)
            && self.route.acquire(self.route_revision).is_some()
    }

    /// 业务作用：在初始化和已就绪阶段共同检查身份与路由寿命，维护任务不把未 Ready 当成失权。
    /// 参数说明: 无。
    ///
    /// 返回：会话尚未关闭且原身份、路由许可仍有效。
    pub fn live(&self) -> bool {
        let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        state.open
            && self.revocations.allows(&state.identity, None)
            && self.revocations.session_allowed(&self.revocation_key)
            && self.route.acquire(self.route_revision).is_some()
    }

    /// 业务作用：初始化请求提交前复验尚未 Ready 的身份和路由，并持有实际外部写入责任。
    /// 参数说明：
    /// - `deadline`：初始化首次确定的截止点。
    ///
    /// 返回：覆盖身份与路由的有限许可；已 Ready、关闭或过期的会话不能重新发起初始化。
    pub fn acquire_initialization(self: &Arc<Self>, deadline: Instant) -> Option<DeliveryPermit> {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if !state.open
            || state.ready
            || !self.revocations.allows(&state.identity, None)
            || !self.revocations.session_allowed(&self.revocation_key)
        {
            return None;
        }
        let permit = self.route.acquire(self.route_revision)?;
        let deadline = deadline.min(state.identity.deadline);
        let deadline = permit
            .deadline
            .map_or(deadline, |limit| deadline.min(limit));
        if deadline <= Instant::now() {
            return None;
        }
        let entry = state.next_write.checked_add(1)?;
        state.next_write = entry;
        let subject_version = state.identity.version;
        state.writing.insert(
            entry,
            Writing {
                group: None,
                subject_version,
                membership_version: 0,
                group_version: 0,
            },
        );
        Some(DeliveryPermit {
            session: self.clone(),
            entry,
            deadline,
            _route: permit,
        })
    }

    /// 业务作用：撤销当前连接全部资格并释放订阅索引，正在写出的责任仍由许可 owner 释放。
    /// 参数说明: 无。
    ///
    /// 返回：会话永久关闭，迟到远程授权或确认不能重新开放。
    pub fn close(&self) {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let notify = state.open && state.ready;
        let reason = if !self.revocations.session_allowed(&self.revocation_key) {
            notices::Reason::SessionRevoked
        } else if state.identity.deadline <= Instant::now() {
            notices::Reason::AuthorizationExpired
        } else if !self.revocations.allows(&state.identity, None) {
            notices::Reason::AuthorizationRevoked
        } else if self.route.unavailable_reason() == Some("route_permit_expired") {
            // 许可过期立即撤销旧会话，但终端可重新认证；主动关闭的路由不会进入这个分支。
            notices::Reason::RouteUnavailable
        } else if self.route.unavailable_reason().is_some() {
            notices::Reason::RouteDraining
        } else {
            notices::Reason::SessionClosed
        };
        // 先禁止新资格，再通知 I/O owner 结束；通知调度延迟不能延长权限。
        let close_reason = match reason {
            notices::Reason::SessionRevoked => "session_revoked",
            notices::Reason::AuthorizationExpired => "authorization_expired",
            notices::Reason::AuthorizationRevoked => "authorization_revoked",
            notices::Reason::RouteUnavailable => "route_permit_expired",
            notices::Reason::RouteDraining => self
                .route
                .unavailable_reason()
                .unwrap_or("route_unavailable"),
            _ => "connection_closed",
        };
        state.close(close_reason);
        if notify {
            if let Some(notices) = self.notices.get() {
                let _ = notices.push(reason, None, None, None, true);
            }
        }
        self.changes.send_modify(|v| *v = v.wrapping_add(1));
    }

    /// 业务作用：确认关闭会话已经归还全部实际写出责任，供集群名额和替换屏障使用。
    /// 参数说明: 无。
    ///
    /// 返回：必须同时永久关闭且没有存活 writer；仅从连接目录移除并不足以归还名额。
    pub fn drained(&self) -> bool {
        let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        !state.open && state.writing.is_empty()
    }

    /// 业务作用：提供生命周期通知供连接 owner 等待，状态事实仍以当前门禁为准。
    /// 参数说明: 无。
    ///
    /// 返回：后续关闭及写出释放通知，不包含凭据或业务正文。
    pub fn changes(&self) -> watch::Receiver<u64> {
        self.changes.subscribe()
    }

    /// 业务作用：已知撤权下界生效后撤销失效订阅或关闭主体，并捕获受影响的实际写出责任。
    /// 参数说明: 无。
    ///
    /// 返回：只覆盖既有失效写出的屏障；新版本授权形成的新写出不延长该屏障。
    pub fn apply_revocations(self: &Arc<Self>) -> SubscriptionBarrier {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let previous_open = state.open;
        let previous_subscriptions = state.subscriptions.len();
        let closed = !self.revocations.allows(&state.identity, None)
            || !self.revocations.session_allowed(&self.revocation_key);
        let identity = state.identity.clone();
        let notify = state.open && state.ready;
        let mut notification_failed = false;
        if closed {
            // 先关闭本地资格再通知传输任务；迟到初始化、刷新和订阅回复不能重新建立权限。
            state.close(if !self.revocations.session_allowed(&self.revocation_key) {
                "session_revoked"
            } else if identity.deadline <= Instant::now() {
                "authorization_expired"
            } else {
                "authorization_revoked"
            });
            if notify {
                let reason = if !self.revocations.session_allowed(&self.revocation_key) {
                    notices::Reason::SessionRevoked
                } else if identity.deadline <= Instant::now() {
                    notices::Reason::AuthorizationExpired
                } else {
                    notices::Reason::AuthorizationRevoked
                };
                if let Some(notices) = self.notices.get() {
                    notification_failed = notices
                        .push(reason, None, Some((identity.version, None)), None, true)
                        .is_err();
                }
            }
        } else {
            state.subscriptions.retain(|group, entry| {
                let Some(grant) = entry.grant.as_ref() else {
                    return true;
                };
                if self.revocations.allows(&identity, Some((group, grant))) {
                    return true;
                }
                // 共享撤权下界或期限已经禁止写出；通知只解释当前连接被移除的资格。
                if notify {
                    if let Some(notices) = self.notices.get() {
                        let reason = self
                            .revocations
                            .subscription_reason(&identity, group, grant);
                        notification_failed |= notices
                            .push(
                                reason,
                                Some(group),
                                Some((grant.version, Some(grant.group_version))),
                                None,
                                false,
                            )
                            .is_err();
                    }
                }
                false
            });
        }
        if notification_failed {
            // 不能保持连续状态序列时关闭整个会话，不允许终端沿用残缺的订阅基线。
            state.close("state_notification_unavailable");
        }
        state.next_subscription_expiry = state
            .subscriptions
            .values()
            .filter_map(|entry| entry.grant.as_ref().map(|grant| grant.deadline))
            .min();
        let entries: Vec<u64> = state
            .writing
            .iter()
            .filter(|(_, record)| {
                closed || notification_failed || !self.write_allowed(&identity, record)
            })
            .map(|(id, _)| *id)
            .collect();
        if previous_open != state.open
            || previous_subscriptions != state.subscriptions.len()
            || !entries.is_empty()
        {
            self.changes
                .send_modify(|value| *value = value.wrapping_add(1));
        }
        SubscriptionBarrier {
            session: self.clone(),
            entries,
        }
    }

    /// 业务作用：使用写出开始时保存的三个版本域复验撤权，不受之后身份刷新影响。
    /// 参数说明：
    /// - `identity`：只用于取得会话不可变的租户与主体。
    /// - `record`：实际写出冻结的版本与群目标。
    ///
    /// 返回：该次写出尚未被已知授权下界撤销时为 true。
    fn write_allowed(&self, identity: &IdentityGrant, record: &Writing) -> bool {
        self.revocations.allows_versions(
            &identity.tenant,
            &identity.uid,
            record.subject_version,
            record
                .group
                .as_deref()
                .map(|group| (group, record.membership_version, record.group_version)),
        )
    }
}

impl SubscriptionBarrier {
    /// 业务作用：在节点控制循环中检查旧写出是否真实结束，避免等待一个慢会话阻塞其它路由续期。
    /// 参数说明: 无。
    ///
    /// 返回：捕获的全部写出 owner 已释放时为 true，不查看后续新写出。
    pub fn is_complete(&self) -> bool {
        let state = self
            .session
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        self.entries
            .iter()
            .all(|entry| !state.writing.contains_key(entry))
    }
    /// 业务作用：等待取消操作涉及的真实旧写出退出，再允许发送 unsub 完成确认。
    /// 参数说明: 无。
    ///
    /// 返回：旧写出全部结束；等待取消不会恢复订阅状态。
    pub async fn wait(&self) {
        let mut changes = self.session.changes.subscribe();
        loop {
            {
                let state = self.session.state.lock().unwrap_or_else(|e| e.into_inner());
                if self
                    .entries
                    .iter()
                    .all(|entry| !state.writing.contains_key(entry))
                {
                    return;
                }
            }
            if changes.changed().await.is_err() {
                return;
            }
        }
    }
}

impl Drop for DeliveryPermit {
    /// 业务作用：在写出完成、超时或任务取消时归还真实责任并推进取消订阅屏障。
    /// 参数说明: 无。
    ///
    /// 返回：准确减少本意图在途数量，不改变授权状态。
    fn drop(&mut self) {
        let mut state = self.session.state.lock().unwrap_or_else(|e| e.into_inner());
        state.writing.remove(&self.entry);
        self.session.changes.send_modify(|v| *v = v.wrapping_add(1));
    }
}

impl DeliveryPermit {
    /// 业务作用：在传输任务恢复执行时再次核对原写出期限，休眠或长调度停顿不能延长许可。
    /// 参数说明: 无。
    ///
    /// 返回：原身份、消息和控制许可的最短写出期限仍然有效。
    pub fn valid(&self) -> bool {
        if Instant::now() >= self.deadline
            || !self._route.valid()
            || !self
                .session
                .revocations
                .session_allowed(&self.session.revocation_key)
        {
            return false;
        }
        let state = self
            .session
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        state.open
            && state
                .writing
                .get(&self.entry)
                .is_some_and(|record| self.session.write_allowed(&state.identity, record))
    }
}
