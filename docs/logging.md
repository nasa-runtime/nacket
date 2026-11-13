# 日志配置

[English](logging.en.md)

nacket 通过 `#[nasa::application("log", ...)]` 使用 napp 管理的 nalog。产品不另建日志运行时；源码中的 `tracing` 事件进入同一日志组件。

产品 YAML 默认 `log.level` 为 `info`，`log.path` 为 `/usr/local/logs/${application.name}`；应用名为 `nacket` 时，实际目录为 `/usr/local/logs/nacket`。日志目录随 naml 的嵌套配置引用解析。

`NACKET_LOG_LEVEL` 覆盖级别，`NACKET_LOG_PATH` 覆盖目录。该变量未设置时采用默认目录，显式设为空时只输出控制台，适合由容器平台采集。自定义 YAML 省略或清空 `log.path` 也仅输出控制台；这是 nalog 的缺省行为，与产品 YAML 提供的默认目录分别生效。

以下配置保留产品路径规则，并显式设置滚动与保留策略。将非空日志路径挂载为容器进程可写的持久目录：

```yaml
log:
  level: ${NACKET_LOG_LEVEL:info}
  path: ${NACKET_LOG_PATH:/usr/local/logs/${application.name}}
  max_file_size: 100MB
  total_size_cap: 2GB
  max_history_days: 7
  clean_history_on_start: true
  split_error_file: true
  color: false
```

| 配置项 | 行为 |
| --- | --- |
| `level` | nalog 过滤表达式，默认 `info`；生产环境按需要配置固定模块级别 |
| `path` | 文件日志目录，产生 `info.log` 和可选 `error.log`；缺省或空值关闭文件输出，保留控制台 |
| `max_file_size` | 单文件滚动大小；上例为 100 MiB，省略时为 nalog 默认 500 MiB |
| `total_size_cap` | 归档总量限制；上例为 2 GiB，省略时为 30 GiB；不是整个文件系统的磁盘配额 |
| `max_history_days` | 归档保留天数；上例为 7，省略时为 30 |
| `clean_history_on_start` | 是否在启动时按保留策略清理历史，默认开启 |
| `split_error_file` | 是否额外写独立 `error.log`，默认开启 |
| `color` | 文件中的 ANSI 颜色，默认关闭；控制台独立按终端能力决定 |

日志配置由同一 naml 配置快照加载；启用配置中心时可通过受管配置更新统一提供。napp 先准备有效的新过滤器与文件资源，再发布变更。初始文件创建失败会使日志组件启动失败；热更失败保留既有生效配置，不报告新路径已可用。

## 连接与断连日志

两个会话日志开关独立控制，省略整个配置段或某个属性时，该属性默认为 `true`：

```yaml
nacket:
  connectionLogging:
    connected: true
    disconnected: true
```

`connected: false` 关闭 `client_connected`，`disconnected: false` 关闭 `client_disconnected`。这两个事件为 INFO，开启后仍受 `log.level` 过滤。开关不影响认证、心跳、指标、业务 online/offline 回调或资源清理。断连始终先撤销该连接的全部订阅和待决授权，其他连接的订阅保持独立。

开关随 naml 的产品配置热更；启用配置中心时可以统一下发。使用事件发生时已接受的配置，既有连接无需重连；被拒绝的产品配置保留原开关。关闭期间的事件不会在重新开启后补打。

成功记录在认证、定义确认和初始化通过并发布 Ready 后产生，单纯接受 TCP 或 WebSocket 升级不表示连接成功。断连记录在本地权限和订阅清理之后产生，每个已确认的逻辑会话最多记录一次。已确认但初始化失败的会话只产生断连事件，`was_ready=false`。Socket.IO 的每个 namespace 会话分别记录；未经确认的握手失败不产生这两个事件。

| 字段 | 含义 |
| --- | --- |
| `event` | `client_connected` 或 `client_disconnected` |
| `cluster_id`、`scope_epoch` | 会话所属集群及控制代际 |
| `node_id`、`incarnation` | 承载会话的节点与节点实例代际 |
| `route_id`、`route_revision` | 当前会话冻结的路由及其定义版本 |
| `session_id` | 服务端生成的精确会话 ID，重连后变化 |
| `tenant_id`、`uid` | 服务端验证的租户和用户主体 |
| `transport`、`peer_ip` | `tcp`、`websocket` 或 `socketio`，以及真实网络对端 IP；经过代理时为代理 IP，不信任客户端转发头 |
| `serialization` | 本次会话路由唯一的 ProtocolBytes 模式 |
| `ready_after_ms` | 成功日志：身份会话创建到 Ready 的单调耗时，包含确认和初始化，不含此前的网络握手及身份验证 |
| `was_ready` | 断连日志：该逻辑会话是否曾发布 Ready，不代表用户在整个集群的在线状态 |
| `cleanup_path` | 断连日志：`uid_partition` 表示分区消费完成；`local_fallback` 表示分区未消费，由本地回收路径完成清理，此路径不发布业务下线通知 |
| `session_duration_ms` | 断连日志：身份会话创建到首次关闭的单调耗时，不包含关闭后清理队列的等待 |
| `cancelled_subscriptions` | 首次关闭时取消的订阅意图数，包含待授权意图，不包含此前已取消或移除的订阅 |
| `pending_subscriptions` | 上述数量中尚未取得授权的意图数 |
| `reason` | 首次关闭时已确认的权限或路由原因；没有更具体证据时为 `connection_closed`，不据此推断主动退出或网络故障 |

关闭事实在清空订阅前冻结，重复关闭不会覆盖原数量或时刻。固定原因包括 `authorization_expired`、`authorization_revoked`、`session_revoked`、`state_notification_unavailable`，以及路由门禁当前的有限原因，例如 `route_closed`、`route_permit_expired`。取消数量表示本地订阅清理，不代表远程业务数据被删除或下线回调已送达。

停机、分区不可用或控制队列拒绝时可能采用 `local_fallback`，此时仍先撤销本地权限并记录断连。启用了业务 offline 回调时，该路径将其计为拒绝；成功日志和断连日志都不构成远程通知送达凭据。

日志不输出 token、认证正文、订阅目标列表或消息正文。主体及路由文本转义后写出，避免输入换行破坏记录。非正常进程终止或存储故障仍可能丢失记录，不能用日志代替权限账本。

## 就绪状态日志

本地就绪任务首次观察或观察到生命周期、聚合结果、关键阻断项变化时，输出 `readiness_transition`。满足接流条件为 INFO，否则为 WARN；`application_state` 和 `ready` 表示本次观察。每个阻断项另有 WARN 事件 `readiness_dependency_unavailable`，包含 `component`、`dependency`、`state` 和固定 `reason`。名称经过转义，日志不包含配置凭据或远端错误正文。

任务约每 100 毫秒观察一次，稳定状态不重复输出；处于不可用状态时原因变化也会记录。非关键依赖和允许继续服务的 Degraded 不列为阻断项。观察读取包含框架的新鲜度与租约期限裁决，日志及指标均不改变接流门禁。各项分别读取，短于观察间隔的变化可能遗漏；任务未获得调度或进程异常退出时也不保证留下记录。日志反映采样证据，不能替代每次请求的实时就绪判断或据此单独推断远端故障原因。

## Redis 控制处理日志

Redis 控制器处理失败或达到本轮期限时，产生 WARN 事件 `control_unavailable`，同一控制器的此类日志至少间隔 30 秒。`stage` 表示最后进入的控制阶段，`timed_out` 表示本轮外层期限是否到达。快照读取期间，`read_step` 可为 `serial_lock`（等待读取串行权）、`backend_snapshot`（等待后端快照）或 `persist_checkpoint`（等待本地高水位确认）；成功读取后清空，不把旧读取位置带入后续命令阶段。

读取子步骤只表示取消或错误前最后进入的位置，不证明远端或磁盘故障；调度延迟也可能消耗期限。日志不包含原始错误正文，不延长控制请求或路由许可。检查点等待者取消后，已经开始的落盘任务仍持有读取串行权直到结束。

## 握手拒绝日志

进入认证流程后，同一逻辑握手首次失败产生 WARN 事件 `handshake_rejected`。原生 TCP/WebSocket 与 Socket.IO 共用该记录；它独立于上述两个 INFO 开关，仍遵循 nalog 的级别过滤。任务取消或对端在确认前退出也属于未完成握手，不推断为业务认证拒绝。格式错误导致尚未进入认证流程的报文不保证带有此事件。

`stage` 指出身份校验、`identity_handoff`、`confirm_admission`、`quota_wait`、`initialization_hook`、`session_ready`、初始订阅或 Ready 写出/发布的位置。`reason` 只输出固定本地原因，例如 `authorization_response_mismatch`、`authorization_ttl`、`invalid_uid`、`user_connection_capacity`、`uid_event_incomplete`、`session_not_ready`；未知错误归入有限结局类别，原始错误和远程正文不进入日志。没有确定失败结果而 owner 已退出时使用 `handshake_abandoned`；达到原始期限时使用 `authentication_deadline`。

记录按已取得的证据提供 `attempt_id`、`node_id`、`incarnation`、`route_id`、`route_revision`、`session_id`、`tenant_id` 和 `uid`。尝试 ID 仅在请求校验后记录，主体仅在身份校验通过后记录。`same_uid_sessions_after_rejection` 仅在确认失败后统计当前节点目录中的同主体连接，包含正在回收的连接；它不是集群配额，也不是拒绝发生瞬间的原子快照。`elapsed_ms` 从认证入口开始计时，`deadline_elapsed` 使用原始握手期限。

`runtime_ready` 是宿主状态；`route_unavailable` 与 `route_permit_remaining_ms` 观察原冻结门禁。三个字段分别读取，不能当作原子状态或授权凭据。未启用集群租约时，剩余许可时间为零也不表示路由失效，应同时查看 `route_unavailable`。readiness 成功只说明所配置的运行与路由条件在探测时成立，不保证任意身份、配额和后续时刻的握手都获准；单个用户拒绝不应使其它用户整体失去 readiness。未知握手失败不会因此获得自动重试权限。

相对路径基于进程工作目录。容器持久化建议使用绝对路径，并保持目录写权限与磁盘容量。日志组件在应用关闭时负责刷盘和回收；正常退出期限并不保证在磁盘故障时仍能写完全部记录。日志不承担业务消息存储、授权账本或重放游标的持久职责。
