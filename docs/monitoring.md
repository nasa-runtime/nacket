# 监控与指标语义

[English](monitoring.en.md)

nacket 通过 napp 装配 nafana。进程资源、框架组件与产品指标汇入同一个 MetricHub；监控必须使用独立端口，不与业务连接或管理 HTTP 共用监听。出厂配置关闭网络出口，业务计数仍在内部维护。

## 启用独立出口

以下片段合入应用 YAML。实例名在本次进程生命周期内保持稳定，副本之间必须不同；读取凭据与业务凭据、管理凭据分开。

```yaml
grafana:
  observability:
    enabled: true
    identity:
      service_name: nacket
      environment: production
      cluster: cluster-a
      instance_id: ${NACKET_INSTANCE_ID}
    collectors:
      enabled: true
      interval_ms: 5000
      filesystem_interval_ms: 30000
      timeout_ms: 1000
      stale_after_ms: 15000
      process: true
      cgroup: true
      filesystems:
        data: /app/data
        logs: /usr/local/logs/nacket
      network_interfaces:
        primary: eth0
      io_devices: {}
    prometheus:
      export_mode: scrape
      scrape:
        listener: dedicated
        bind: 0.0.0.0:9464
        path: /metrics
        max_concurrent_requests: 4
        request_timeout_ms: 2000
        auth:
          mode: bearer
          token: secret://metrics_read
    provisioning:
      mode: disabled
secrets:
  metrics_read:
    encoding: raw
    max_bytes: 512
    fragments:
      - env: NACKET_METRICS_TOKEN
nacket:
  metricScopes:
    enabled: true
    routeIds: [route-a]
    endpointRefs: [identity]
    ingressRefs: [source-a]
```

挂载路径、网卡与设备必须来自部署实际可见资源；别名只用于固定标签。两个路径位于同一文件系统时不能把它们的容量相加。cgroup 反映进程可见层级，本级 CPU 配额不代表所有祖先约束的最小值；没有有限限制时不产生限制样本。资源缺失、不支持、失败和过期均通过采集状态表示，不用零表示健康。

生产环境应限制监控网络来源，并由反向代理或部署网络承担所需 TLS。启动时端口冲突、认证引用无效或容量不足会失败，不退回业务端口。观测身份、出口、采集目录和详细名称目录按启动配置处理，变更需要重启。

## 固定名称与容量

`metricScopes.enabled` 默认 `false`。启用后，`routeIds`、`endpointRefs`、`ingressRefs` 各最多 32 个不同名称，每类另有 `_other` 聚合项。动态路由可以正常上线；未列出的内部名称汇总到 `_other`，不会产生新时间序列。重复名称和显式使用保留名被拒绝。该开关不改变连接权限。

详细指标的 `kind` 只能为 `route`、`endpoint`、`ingress`，各类使用自己的动作和资源目录，不把全部标签做组合展开。启动时按完整计数目录和 Histogram 的 bucket、sum、count 预留最坏系列数；容量不足时显式拒绝装配。启用 remote write 时应同步配置其 `max_batch_samples`，默认 5000 不一定容纳全部详细目录。

`uid`、群 ID、会话 ID、消息 ID、token、任意 URL 和错误原文不能成为标签。路由 revision 与 digest 通过受权管理查询提供，不生成新标签。业务热路径不为观测复制正文；抓取仅读取固定大小的观测快照。路由状态与来源可用性每秒投影，不能用监控延迟作为继续发送的权限。

## 产品指标目录

| 指标 | 口径 |
| --- | --- |
| `nacket_connection_events_total` | 按实际物理适配器统计接纳、握手前拒绝与结束；Socket.IO namespace 不重复计物理连接 |
| `nacket_admissions_total` | 全业务端口共享的物理、未认证、真实来源目录、连接数和速率门禁 |
| `nacket_live_resources` | 真实 owner 持有的连接、会话、订阅、调用、队列、字节、回执和控制责任；取消与异常释放责任时同步归还 |
| `nacket_oldest_pending_age_seconds` / `nacket_pending_age_complete` | 按固定资源记录真实待决年龄；完整性为零时省略年龄样本，不能视为无积压 |
| `nacket_control_snapshot` / `nacket_control_snapshot_fresh` | 最近有效持久控制快照的节点、路由、许可保留数、操作阶段及待决屏障；读取失败或超期时省略数量样本 |
| `nacket_control_snapshot_age_seconds` / `nacket_control_oldest_operation_age_seconds` | 本地距最近验证的时间，以及持久操作创建时间推导的诊断年龄；后者涉及跨节点墙钟，不参与屏障完成判断 |
| `nacket_control_authority_snapshot` / `nacket_control_authority_fresh` | 仅有效控制 owner 输出持久目录数量；备用控制器和纯数据节点的新鲜度为零，不能充当权威 |
| `nacket_control_authority_age_seconds` / `nacket_control_authority_remaining_seconds` / `nacket_control_authority_oldest_operation_age_seconds` | 权威观察年龄、有限证据余量和最旧待决操作年龄；失权、失败或到期时全部省略 |
| `nacket_control_local_routes` / `nacket_control_permit_min_remaining_seconds` | 本节点需要集群许可的期望在线路由数及其中最小剩余期限；未授权或关闭门禁为零，无对应路由时省略余量 |
| `nacket_runtime_state` | 路由在线/保护/下线、实际 writer、必要依赖、监听、有限容量、就绪、待重启和持久任务 |
| `nacket_operations_total` | 首次实际执行的客户端动作结局；结果缓存读取不当作新执行 |
| `nacket_identity_checks_total` / `nacket_authorization_checks_total` | 身份策略及业务授权结局；业务 `deny` 与网络不可用分开 |
| `nacket_request_records_total` | 首次请求、重复关联、身份冲突及结果正文缓存容量 |
| `nacket_mq_consume_total` | 实际消费处理尝试，包含重试；不能跨节点相加当成唯一业务消息数 |
| `nacket_mq_publish_total` | 按用途区分提交、拒绝和未知；提交不表示终端收到 |
| `nacket_deliveries_total` | `admitted`、`queued`、`written`、`received`、`processed` 分开；完整写出只证明本地传输完成 |
| `nacket_receipt_events_total` | 服务器票据对应的有效确认、拒绝、取消和到期；不代表用户阅读 |
| `nacket_receipt_inputs_total` | 每次终端回执的关联校验：accepted、rejected、invalid、unknown；unknown 表示当前会话没有有效票据，包含迟到、重复或其它会话的票据，不新增投递终态 |
| `nacket_queue_events_total` | 队列拒绝、替换、丢弃与取消，逻辑字节量见资源指标 |
| `nacket_lifecycle_events_total` | 精确连接的上下线事实接纳与提交，不能直接推算唯一在线用户 |
| `nacket_control_events_total` / `nacket_configuration_events_total` | 持久控制请求、配置接纳和重启要求；操作受理不是全局屏障完成 |
| `nacket_transport_bytes_total` / `nacket_transport_frames_total` | 应用可见完整协议消息；发送失败单独计帧尝试，不计完整发送字节；不含内核、TLS 与半帧字节 |
| `nacket_heartbeat_events_total` | native 与 Socket.IO 的有效心跳回复和到期；底层 WS 控制帧不混入 |
| `nacket_payloads_total` / `nacket_payload_bytes_total` | 进入业务处理入口的正文模式与大小；不代表 schema 或业务权限已通过，重试按实际尝试计数 |
| `nacket_payload_copies_total` / `nacket_payload_copy_bytes_total` | 产品显式编码分配或复制的次数与字节；共享 Bytes 克隆不算正文复制，框架原生输入物化和网络库内部复制不在此口径内 |
| `nacket_payload_deserializations_total` / `nacket_payload_deserialization_bytes_total` | 订阅、操作转换及显式正文检查的物化尝试；失败也计输入字节，不含固定控制 JSON |
| `nacket_duration_seconds` | 本进程单调时间：握手、初始化、授权、操作、排队、写出、回执、MQ 提交、控制、路由、配置、关闭、心跳和生命周期 |
| `nacket_scope_events_total` / `nacket_scope_duration_seconds` | 固定名称目录内的操作结局和耗时，HTTP 包含容量等待；传输和结构成功不等于业务允许 |
| `nacket_scope_resources` | 路由会话/订阅/执行/状态/writer，端点在途请求，入口处理尝试及可用性；授权控制入口还要求执行进度有效 |

所有 Counter 从进程开始累计，应使用 `rate` 或 `increase`；Histogram 使用秒，边界为 1/5/10/25/50/100/250/500 毫秒及 1/2.5/5/10/30 秒。`queue_wait` 是已经取得 writer 的排队耗时分布。`receipt` 从实际取得写出资格开始计时。`heartbeat_rtt` 包含心跳排队、网络和终端响应，不能当成纯网络 RTT。`route_transition` 只测本节点定义与监听准备，不表示全局收敛；`shutdown` 测持久责任排空等待，不表示整个进程退出耗时。

待决年龄覆盖操作、订阅申请、outbox、writer、回执票据、HTTP、控制请求、授权交接、Kafka 异步分发交接、发布、连接事实和初始化等待。年龄从实际责任取得开始，队列转入 writer 时开始该阶段的新年龄。回执票据年龄包括写出前排队；完成回执的耗时 Histogram 仍从实际写出开始。完成缓存继续占用资源容量，但停止累计待决年龄；授权重试保持最初年龄。

每类资源固定 256 个原子时间槽，以一秒量化记录起点，输出最旧年龄的保守上界，误差小于一秒。槽位或有限原子重试耗尽时保留未知份额，完整性为零并省略年龄，直到全部未知责任结束。不为每条消息分配时间索引，也不因观测容量不足拒绝业务。这些值只描述本节点已经接纳的责任，不代表 Redis PEL、Kafka 未消费日志或业务端 outbox 的总积压。

集群控制数量来自已经验证的账本，各节点观察同一个权威，不能跨副本求和。`barrier_nodes` 对当前准备、应用及旧许可屏障中仍需回应的稳定节点去重，不等于未在线节点数；`permits_retained` 包含尚未合法释放的旧许可，不能视为有效接流节点数。控制观察的有效期采用配置的 `permitTtlMs`，读取失败立即失效；过期数量保持 NoData，最近观察年龄继续可见。本地模式不生成集群控制样本，纯控制器不生成本地接流许可余量。

`control_authority_*` 与上述节点快照分别输出。Redis 从实际 owner 取得或续期请求的起点计算五秒期限并扣除时钟余量；Kafka 必须读到本次启动提交的已确认事务，并在导出时复验同一分配代次。Kafka 证据最长五秒且不超过控制请求期限。准备中的候选、备用实例读到的账本和失败后保留的缓存均不能生成权威数量；指标不会续发或代替实际控制权限。

采集平台可能在接管时同时保留新旧采样点，所以不能直接把权威样本求和。集群目录面板只在同一监控集群内恰好一份新鲜权威观察时展示，零份或多份保持 NoData 并告警；每实例观察仍可诊断。监控的 `cluster` 必须对应一个独立控制作用域，不能将多个互不相关的权威部署合并到同一聚合身份。

CPU、内存、FD、线程、缺页、进程存储 IO、cgroup 限制/压力/OOM、文件系统空间/inode 与白名单网卡由 `napp_process_*`、`napp_cgroup_*`、`napp_filesystem_*`、`napp_network_*` 提供。CPU 累计量使用微秒，除以 1,000,000 才得到秒。采集可用性、状态、错误和最近成功时间见 `napp_resource_collection_*`。框架 HTTP、连接池、Kafka、Redis、注册发现与观测出口指标保持原名与各组件统计边界。

## MQ 位置与待确认责任

主动 broker 观测默认关闭。开启后由独立受管任务复用已有客户端；`/metrics` 只读内存，消费任务不等待采样。配置示例：

```yaml
nacket:
  brokerMetrics:
    enabled: true
    intervalMs: 5000
    timeoutMs: 1000
    staleAfterMs: 15000
    maxInFlight: 4
```

每个已启用入口按固定周期采样，起点分散。并发耗尽时跳过本轮并报告未知；超时立即撤回样本，原请求结束前继续占用采样名额，迟到结果不会刷新证据。纯 controller 不建立数据入口观察。此配置变更需要重启，采样错误不改变业务权限。

| 指标 | 语义 |
| --- | --- |
| `nacket_node_info` | 持久确认后的 node_id、incarnation、role、control_backend，每进程固定一组；与平台 service_instance_id 关联，不使用路由 scopeEpoch 替代进程身份 |
| `nacket_broker_observation_fresh` | 聚合桶内所有必要来源都有完整且未过期证据时为 1，否则为 0 |
| `nacket_broker_observation_age_seconds` | 距桶内最旧成功请求起点的时间，尚无完整成功历史时省略 |
| `nacket_broker_unread_positions` | Redis 新增位置与已读位置的差值，或 Kafka read_committed 末尾与消费组提交 next offset 的距离 |
| `nacket_broker_pending_records` | Redis 可恢复组的实际 PEL 记录数；不包含尚未读取的位置 |
| `nacket_broker_pending_max_idle_seconds` | 完整 PEL 中最大的认领后空闲时间，重新认领会重置，不能当作消息创建年龄 |
| `nacket_broker_observed_partitions` | Kafka 完整观测的固定分区数量；未知组提交位置或任一缺项均不能生成零积压 |
| `nacket_broker_observations_total` | 独立采样的成功、失败、容量拒绝和关闭结局 |
| `nacket_recovery_events_total` | 实际读取、已确认 PEL 认领、broker 确认/隔离及 Redis/Kafka 临时重试；不代表终端收到 |

位置距离不能作为唯一业务消息数：Kafka 事务及控制记录占据 offset，Redis/Kafka 每节点独立消费组也会重复观察同一消息。Kafka 样本依附原拓扑及 assignment 的连续有效区间，拓扑失效或换代后旧值立即不可展示。Redis 只读脚本核对原游标或精确围栏；PEL 超过 128 条时不进行无限扫描，也不把部分结果当完整结果。查询需要相应只读权限；权限不足显示未知，不使用空集合伪造健康。

`backend` 只有 redis/kafka。`ingress` 在详细名称关闭时为 `_all`；开启 `metricScopes` 后只使用允许的入口引用和 `_other`。同一桶中任一来源失败会撤回该桶数量，其他独立命名桶不受影响。当前数量只在完整新鲜时输出，已知零保持为零，未知保持 NoData。

## 面板和告警

导入 [中文面板](../monitoring/dashboard.zh.json) 或 [英文面板](../monitoring/dashboard.en.json)，选择 Prometheus datasource。身份筛选使用 `deployment_environment`、`cluster`、`service_name`、`service_instance_id`；模板默认服务名为 `nacket`，改名部署需同步查询。业务汇总排除重复身份和已知抓取失败的实例，不把查询空集补成零。

抓取可用性面板按 `job`、`instance` 将 `up` 关联到最近五分钟内唯一的已知实例身份，支持同一组身份筛选，并在短暂失联后保留失败值。同一抓取坐标出现多个历史身份或五分钟内没有身份样本时，该面板省略该目标，避免错误归属；可从 Prometheus Targets 和原始 `up` 查询诊断，不能把空值理解为健康。

自动创建 datasource 后，健康 API 成功只说明后端可访问 Prometheus。打开面板前还需确认浏览器加载的数据源目录能按 UID 与名称解析该来源，并通过浏览器查询路径取得结果。精确的 datasource-not-found 可在有限期限内重新加载目录；认证失败、其它查询错误或持续缺失必须作为部署失败报告，不能通过忽略面板错误继续开放。

Grafana 插件必须纳入部署版本管理。启动后的异步插件更新可能终止正在服务查询的进程，使既有 datasource 暂时不可用。使用 Grafana 13.2.1 镜像内置插件时，可配置 `GF_PLUGINS_PREINSTALL_DISABLED=true` 和 `GF_PLUGINS_PREINSTALL_AUTO_UPDATE=false`；需要其它插件时明确固定版本并在开放面板前完成安装。分析上报中的更新检查开关不等于插件自动安装开关。

[告警规则](../monitoring/alerts.json) 使用 Prometheus rule-file 格式的 JSON，Prometheus 的 YAML 解析器可直接读取。将文件路径加入平台 `rule_files`，由平台校验后加载。文件包含路由保护、MQ 不可用、连接/字节容量、CPU/节流、内存/FD、磁盘/inode、OOM、重启要求、采集盲区、身份重复、远程失败、写出失败、回执过期和实例丢失规则。比例规则要求最低样本数；阈值、持续时间、静默与通知渠道由部署方调整。

期望实例规则依赖平台独立持续提供 `platform_expected_instance_info{deployment_environment,cluster,service_name,service_instance_id}=1`，退役后移除。此来源不能从应用心跳或 Nacos 当前在线列表反推。没有期望来源时维持 NoData，并提示库存缺失；规则不能凭空识别从未登记的集群。`up` 仅表示已发现抓取目标的网络结果，不证明业务 Ready。

应用副本不需要 Grafana 管理凭据。面板、规则可由部署平台导入；独立 nafana-controller 负责其通用资源，不将 nacket 专用面板代码写入基础框架。采集周期建议 15 秒，面板刷新 30 秒。开启 scrape 与 remote write 时，平台只选择一条主采集路径，避免同一实例双计数。

## 性能边界

同格式纯转发共享正文；Kafka 的借用记录跨异步边界需要一次拥有型复制，JSON 参数映射和结果编码会物化对象。复制指标不覆盖框架客户端内部缓冲、JSON 对象分配、TLS、WS 掩码与内核；某一边界为零不能据此宣称端到端零拷贝。逻辑队列字节也不能与 RSS 相等比较。

集群业务消息数、唯一在线用户数、业务成员数、未读数及客户端断线期间的端到端延迟需要业务权威，服务不从本地 fan-out、连接或订阅计数猜测这些值。

## 业务活动与订阅目标采样

`nacket.sessionMetrics` 默认开启，按每批 128 个会话、每 100 毫秒一批扫描；业务活跃窗口默认 60000 毫秒。成功的业务操作和完整业务下行写出更新活动时刻；心跳、建连及授权刷新不算业务活动。只统计当前具备接收资格的 Ready 会话。不同订阅目标按 `(routeId, tenantId, groupId)` 在本节点去重，不等于订阅关系数，也不等于跨节点去重后的全局数量。

以下值通过 `nacket_runtime_state{state="..."}` 导出，无 UID 或目标 ID 标签：

| state | 含义 |
| --- | --- |
| `sessions_active` / `sessions_idle` | 完整采样周期中观察到的活跃与空闲会话 |
| `subscription_targets` | 本节点有效订阅目标去重数 |
| `session_sample_available` | 会话采样是否完整可用，1 才能使用上述会话值 |
| `subscription_target_sample_available` | 目标目录未超量且采样完整，1 才能使用目标数 |
| `session_sample_span_seconds` | 本轮扫描跨度，单位秒 |
| `session_sample_timestamp_seconds` | 本轮完成时的 Unix 秒，仅为 Prometheus 时间惯例 |

这些是有界分批观察，不能当成全节点同一瞬间的事务快照。超过 `maxCycleMs`、`maxTargets` 或 `maxTargetBytes` 时相应 availability 为 0，保留的旧数值不能当作当前精确总量。关闭采样不执行目录扫描。所有采样容量变更需要重启，抓取接口不执行扫描。

资源 gauge `nacket_live_resources{resource="..."}` 包含 `authorization_refresh_pending`、`authorization_refresh_inflight`、`authorization_cache_entries`、`authorization_cache_bytes`、`handshake_context_bytes` 和 `redis_pending_records`。`nacket_authorization_cache_total{operation,outcome}` 的 operation 为 hit、miss、wait、bypass、fetched；容量旁路与负缓存不应解释成授权允许。

容量与资源比例只接受大于零的有效分母。分母缺失、为零或为负时不计算比例、不触发容量阈值告警，由采集可用性和缺失实例规则表达观测缺口。远端调用、写出及回执失败率另要求五分钟内至少 100 次相应尝试；零请求不算失败。

## 图例与序列身份

中英文 dashboard 的图例保留各自 PromQL 输出的区分维度。除环境、集群、服务与实例身份外，
同一实例的 state、direction、interface、backend、ingress、collector 等维度在相应面板分别显示；
因此不同后端、入口、网卡或状态不会仅因实例名相同而显示成同名曲线。
直方图分位查询已经消除的 le 不再作为曲线身份；目标存活查询保留 job 与 instance 的区分。

图例只解释已有查询结果，不把不同序列合并，也不改变指标的授权或时间语义。多选实例时仍需按面板
查询的实际分组读取结果；无样本、过期和真实零值按各指标定义区分。
