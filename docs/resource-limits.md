# 接入与字节预算

[English](resource-limits.en.md)

共享端口与独立路由端口使用同一个节点准入控制器。`nacket.transport.admission` 是启动时固定的硬上限，不能由路由的 `limitEnabled` 开关取消；listener 自己的容量约束继续同时生效。

| 配置 | 默认值 | 行为 |
| --- | ---: | --- |
| `maxPhysicalConnections` | 100000 | 所有业务端口的物理连接总数；Socket.IO 的多个 namespace 只计一条物理连接 |
| `maxUnauthenticated` | 256 | 尚未完成首次路由接流准备的连接数；原始认证截止时间仍独立生效 |
| `maxConnectionsPerPeer` | 10000 | 同一真实 TCP 对端 IP 的物理连接数 |
| `connectionsPerSecondPerPeer` | 1024 | 每个真实来源在固定一秒窗口内最多接纳的连接数 |
| `maxTrackedPeers` | 16384 | 来源计数目录上限；目录满且不能安全回收时拒绝新来源 |
| `maxInboundBytes` | 268435456 | 跨端口共享的传输入站字节预算，位于 `transport` 下 |

容量不足时关闭新连接，不排队保留无限握手。名额随实际 reader、writer 退出归还；接入处理函数返回并不代表底层 I/O 已结束。完成首次准备只归还未认证名额，仍占用物理连接及来源容量。快速断开不能清空当前窗口的来源计数；只有没有连接且窗口已结束的记录才可回收。

来源始终取自 TCP 对端，不采用 `Forwarded`、`X-Forwarded-For` 或客户端声明。多用户经同一网关或 NAT 接入会共享来源限额，需要根据部署设置容量。路由、用户及租户配额独立执行，见[连接配额](connection-quotas.md)。

## 入站字节的责任

产品使用 naws 的可选共享传输预算。TCP 先检查长度，再取得字节预约，最后分配和接收；长度合法但共享预算不足也拒绝连接。业务正文与 metadata 切片共用原始 `Bytes`，最后一个视图销毁后才归还原分配容量。

WebSocket 保留标准协议后端。读取前预留后端输入缓冲、分片组装和临时复制的保守容量；后端可能保留已经扩展的缓冲，因此该部分按连接高水位计费，直到实际 I/O owner 全部结束。空闲连接不预约最大业务帧。完整文本、二进制和控制载荷移交后有独立的字节责任；Socket.IO 待附件 metadata 的独立分配也计入同一个预算。大帧后保留的缓冲可能让节点提前达到容量上限，这是对实际保留状态的约束。

WebSocket 握手输入上限为 64 KiB，每个完整业务消息窗口的线长不超过 `2 × maxFrameBytes + 64 KiB`，包含分片头和插入的控制帧。控制帧不会刷新当前接收窗口的组装起点；持续发送控制帧不能无限保留半条业务消息。预算耗尽、组装超时或读取中途取消均结束连接，不继续解析半条输入。

此预算描述传输拥有的字节与保守缓冲容量，不等同于 RSS。内核 socket 缓冲、线程栈、分配器开销、HTTP/MQ 客户端内部缓存和出站缓冲不计入 `maxInboundBytes`；部署仍需容器内存限制及相应组件的容量设置。出站队列、业务操作、请求结果、初始化、发布及授权控制有独立的数量或字节限制。

## 观测

`nacket_admissions_total{scope,outcome}` 区分节点连接、未认证、来源目录、来源连接与来源速率拒绝，不使用 IP 标签。`nacket_live_resources{resource}` 中的 `physical_connections`、`unauthenticated_connections` 跟随实际 owner。

`nacket_runtime_state{state}` 提供 `inbound_bytes`、`inbound_byte_capacity`、`physical_connection_capacity` 和 `unauthenticated_connection_capacity`。指标经 nafana 的独立监控端口导出；采样值不参与权限判断。

## MQ 分发预算

`nacket.ingressProcessing` 是节点固定资源配置：

| 配置 | 默认值 | 责任 |
| --- | ---: | --- |
| `maxPendingRecords` | 256 | Kafka 异步交接与尚待消费回调取得结果的记录数 |
| `maxRetainedBytes` | 67108864 | Kafka 与 Redis 共享原文；所有出站引用释放后归还 |
| `maxReadBytes` | 134217728 | Redis 普通数据读取的总响应预约 |
| `maxControlReadBytes` | 8388608 | Redis 初始化回复、授权控制分别使用一份独立预约池 |
| `maxIsolationBytes` | 67108864 | Redis 实时隔离命令及原文复制的独立预约 |
| `maxParallelFanouts` | 4 | Redis 与 Kafka 合计正在分发的消息数 |
| `maxCandidateReferences` | 1000000 | 所有正在分发的本地会话快照引用总数 |
| `fanoutBatchSize` | 256 | 每批处理的候选数；之后让出执行权并复验原期限及路由门禁 |

Kafka 使用 nafka 的 `SingleConsumer<RawPayload>`，在异步消费调用内等待实际分发结果，正常等待不触发 broker 退避重取。按入口、topic、partition、offset 关联同一次工作；消费等待被取消后，原受管任务仍持有执行责任，重复输入复用原任务或其完成结果。框架从原生消息物化一次正文，产品通过共享 owner 交接，不解析业务对象，也不增加正文副本；保留正文同时保留框架输入预算和产品字节预算。

进程退出前没有完成的工作保持未确认。所有此类来源必须配置 `behavior.max_consume_attempts: 0`，防止依赖不可用时普通重试次数耗尽，将合法记录提前写入 DLT；无效记录的隔离策略继续独立生效。回调结束仍须通过框架 assignment 和连续提交水位门禁。成功决定仅表示本地候选已处理，不代表终端收到或业务完成。

Redis 读前按单条原文上限的四倍预约完整批次，覆盖响应、字段结构及本地转换的保守成本，并按预算自动缩小批次。数据、初始化回复与授权控制分别使用容量池。Lua 在响应离开 Redis 前检查单条总字段成本与最多 128 个字段元素；新记录超限时不推进组游标或建立 PEL，旧 PEL 超限时不重新认领。此类不可容纳记录保持原处，不得跳过并声称已经消费。可恢复入口保留组与 PEL 责任；读取返回 `SC_RECOVERY_*`（包括记录超限），或实时读取及基线检查返回 `SC_REALTIME_*` 时会关闭本节点全部路由门禁、终止关键任务并进入受管停机，需要运维恢复原资源合同后重启。重启的实时入口从新的实际末尾开始，历史与未完成区间由业务重建，不能依赖旧游标补发。

Redis 记录字段保留原始顺序、重复项与任意二进制名字。正常分发共享原正文分配；失败仍可保留原文。实时 DLT 先预约独立容量再构造命令，只有实际写入确认后才推进游标；可恢复 DLT 在 Redis 同槽脚本中完成原文隔离与确认。

Kafka 原生队列、框架进程/来源/组/分区预算与 DLT 流水线继续由 nafka 管理，产品不另建消费者或队列替代它。`behavior.max_process_pending_bytes`、`max_source_pending_bytes`、`max_group_pending_bytes`、`max_partition_pending_bytes`、`dlt_queue_max_bytes` 及 producer lane 容量须与部署内存匹配；librdkafka 自身缓存不属于产品正文预算。

## 远程调用预算

`nacket.remoteCalls` 默认 `maxInFlight: 256`、`maxBytes: 268435456`、`maxRequestBytes: 1048576`。所有远程身份、授权、业务动作、生命周期回调与公钥读取共享它；每个冻结端点的 `call.maxInFlight` 继续限制该端点。

请求先进行无正文分配的 JSON 长度检查，再以请求、映射预留及最大响应编码长度的 32 倍，加 4096 字节固定余量预约构造容量。该保守值覆盖同时存在的控制对象、映射、编码和解析表示，不等同于 RSS。公钥文档使用独立固定的 64 KiB 单文档上限，但并发和缓冲仍进入同一个节点预算。原始业务正文继续复用已持有预算的 `Bytes`。

等待节点并发、端点并发或字节容量使用调用原始截止时间。请求尚未移交 HTTP 执行器时失败，副作用结果为 `none`；移交后响应丢失或超时为 `unknown`。预算不会重置截止点、允许重定向、扩大授权或自动重复业务动作。`remote_calls`、`remote_bytes` 和各 MQ 资源通过固定 `nacket_live_resources` 标签观察。
