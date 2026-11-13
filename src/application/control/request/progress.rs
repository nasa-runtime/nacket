use crate::domain::control::Command;

/// 单次控制处理的有限诊断上下文，不保存凭据、主体或业务正文。
pub(super) struct Progress {
    /// 最后进入的阶段；超时取消后仍可定位等待边界。
    pub stage: &'static str,
    /// 快照读取中最后进入的固定子步骤；成功后清空，不保存后端错误正文。
    pub read_step: Option<&'static str>,
    /// 当前 owner 的 fencing 序号，不包含完整租约令牌。
    pub term: Option<u64>,
    /// 本轮实际处理的持久输入位置。
    pub cursor: String,
    /// 完整签名请求的关联摘要，仅用于日志。
    pub request_id: String,
    /// 已认证业务撤权的固定关联 ID。
    pub command_id: Option<String>,
    /// 已认证命令对应的单一路由。
    pub route_id: Option<String>,
}

impl Progress {
    /// 业务作用：从没有持有 owner 的状态开始跟踪一次有限控制处理。
    /// 参数说明: 无。
    ///
    /// 返回：只包含空关联和固定阶段名的诊断上下文。
    pub fn new() -> Self {
        Self {
            stage: "owner_lock",
            read_step: None,
            term: None,
            cursor: String::new(),
            request_id: String::new(),
            command_id: None,
            route_id: None,
        }
    }

    /// 业务作用：仅从已经验签的撤权请求摘取关联信息，不输出撤权目标和签名材料。
    /// 参数说明：
    /// - `command`：已确认来源权限的控制请求。
    ///
    /// 返回：清除上条请求关联；当前命令是撤权时记录其稳定 ID 和路由。
    pub fn correlate(&mut self, command: &Command) {
        self.command_id = None;
        self.route_id = None;
        if let Command::ApplyAuthorization {
            command,
            ..
        } = command
        {
            self.command_id = Some(command.command_id.clone());
            self.route_id = Some(command.route_id.clone());
        }
    }
}
