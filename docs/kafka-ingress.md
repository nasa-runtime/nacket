# Kafka 下行与恢复基线

[English](kafka-ingress.en.md)

每个 socket 节点使用独立 consumer group，在本节点按路由、租户、用户或订阅筛选目标。多个节点共用一个工作组会竞争消息，不符合本服务的节点广播合同。控制权威与业务数据必须使用分别配置的 topic、权限和资源；服务不会自动创建业务 topic 或重置消费位置。

## 来源配置

业务来源使用 nafka 的受管客户端。必须启用严格 header、必需 DLT、invalid-record 的 `dead_letter` 策略和 `isolation.level=read_committed`。DLT producer 的有效配置必须为 `acks=all` 且开启幂等。配置的 topic 分区数必须与 broker 完全一致，DLT 名称必须等于源 topic 加 `behavior.dead_letter_topic_suffix`。

所有产品数据来源都设置 `behavior.max_consume_attempts: 0`。容量或授权依赖不可用时保留合法输入未确认；格式、来源、分区键等明确错误进入 DLT。配置 `behavior.tombstone_policy: dead_letter`，防止 null 正文被静默跳过；也接受 `deliver`，由 RawPayload 拒绝 tombstone 并进入必需的 invalid-record 隔离流程。非 null 的空正文仍与 tombstone 区分。

业务推送使用受管 `SingleConsumer<RawPayload>`，消费调用直接等待原分发完成，正常等待不要求 broker 回退重取。nafka 从原生消息物化一次正文，产品接纳时只保留共享所有权并预约有限容量。消费等待被取消后，重复输入按原始位置、内容指纹和保留结果关联同一次责任，不重新执行已完成部分。只有依赖失败或消费中断才进入重试退避；所有权与确认边界见[接入与字节预算](resource-limits.md)。

入口的 `maxBodyBytes + maxMetadataBytes + 4096` 必须能放入 `behavior.max_record_bytes`。源 topic 和 DLT 使用 `cleanup.policy=delete`，禁用不干净领导选举，`max.message.bytes` 覆盖该记录上限。可恢复来源还要求不按时间或容量删除保留记录：`retention.ms=-1`、`retention.bytes=-1`。Kafka 集群 ID 与运行拓扑持续复验；证据过期、topic 缺失或分区变化使相关路由受保护。

运行期的有限输入背压、进度背压、处理中等待和依赖重试不等于失去消费分配。完整 assignment、有效分配代次、新鲜 poll 和实际 topic 合同同时成立时，服务允许已有责任继续推进。提交受阻、DLT 保护、人工暂停、失权、重建、停止或协调证据超期仍撤销入口可用性；排队期间不会延长消息 TTL、身份或控制许可。

## 业务实时消费与安全状态恢复

业务推送和初始化回复只允许 `realtime`，使用包含本次进程随机启动标识的消费组及 `auto_offset_reset=latest`。重启不继承旧组 offset，不读取停机期间的业务积压。在线短暂故障保留当前进程的处理责任，并受原 TTL 和有限批次、字节预算限制。见[实时消费边界](mq-delivery.md)。

仅专用授权控制入口允许 `recoverable`，使用稳定组及 `auto_offset_reset=none`。`ingresses.<id>.recovery.initialize` 必须为 false。启动前逐个核对全部目标分区的持久提交位置；位置缺失、低于实际 low watermark 或高于 high watermark 都拒绝启动，不默默选择 earliest 或 latest。该入口恢复安全状态，不补发业务历史。

## 组名

组名为 `nacket.data.` 加 SHA-256 小写十六进制摘要。摘要按以下顺序处理四个 UTF-8 字段，每个字段先写四字节大端字节长度，再写原始字节：

1. `nacket.clusterId`
2. `nacket.scopeEpoch`
3. `nacket.nodeId`
4. ingress 的 Kafka `sourceRef`

实时入口在四个字段之后按相同长度前缀格式加入本次 startupId；稳定授权控制入口不加入。两类入口不能共用同一受管来源。同一来源的多个 topic 属于同一组；topic 名不进入组名。实际使用 UTF-8 字节长度，不使用字符个数，不加分隔符。不同逻辑节点使用不同 nodeId，重启同一逻辑节点保留其身份、数据卷与控制基线，但使用新的实时数据组。

## 授权控制入口的首次建立和恢复

以下步骤仅用于授权控制入口。实时业务入口不预置旧组提交位置。

为每个逻辑节点、每个数据来源计算组名，先创建并配置全部源 topic 和 DLT，再在该组没有运行实例时，用受信运维工具提交所有目标分区的明确初始 offset。选择 earliest 表示需要处理全部保留输入；选择 latest 仅适用于业务已经完成相应状态快照、并明确接受忽略此前输入的首次接入。不能把故障恢复当作新的首次接入。

使用 Kafka 自带管理工具时，先 `--describe` 核对组和分区，再对准确组名及 topic 使用 `--reset-offsets`，查看候选后才执行 `--execute`。一个来源包含多个 topic 时，每个目标分区都必须有基线。服务只验证已有事实，不在启动时执行这些运维写入。

丢失提交位置、组被删除、保留数据被裁剪或 topic 被重建时，保持相关入口关闭。恢复需要先围栏旧实例，确认业务状态及可重放范围，再由受信流程恢复完整基线。新 scopeEpoch 表示新作用域，需要与控制账本、生产者、节点配置及业务重建一起处理，不能仅修改 YAML 绕开已有恢复责任。

业务推送采用原始 ProtocolBytes 正文与严格 `nacket-meta` header。签名、分区键、唯一序列化和终端回执的含义见[业务发布](publishing.md)。资源责任见[接入与字节预算](resource-limits.md)。

## Topic 部署合同

`ingresses.<id>.transport.topicContract` 与 `publishers.<id>.transport.topicContract` 可声明 `replicationFactor`、`minInSyncReplicas`、`retentionMs`、`retentionBytes`；`ingresses.<id>.deadLetterContract` 独立约束 DLT。下面是合同片段，topic 必须由部署预先配置成对应值：

```yaml
topicContract:
  replicationFactor: 3
  minInSyncReplicas: 2
  retentionMs: 3600000
  retentionBytes: 1073741824
```

配置的字段要求与 broker 实际值完全相同，不采用“至少”推断。副本数要求所有分区一致；ISR 下限必须为正且不超过副本数；retention 只允许正值或 -1。省略字段仍核对实际值合法，但不替部署选择容错级别或磁盘预算。生产部署应显式设置自己的预期值。消费来源启动和运行期间持续复验；发布器在装配时验证。源和 DLT 分别检查，DLT 的合法 retention 不能替代源 topic 的合同。可恢复授权入口要求源及 DLT 的两项 retention 均为 -1；有限时间/容量保留配置被拒绝。

`ingressProcessing.maxKeyPendingRecords` 控制 Redis 预取；Kafka 使用 `kafkas.<source>.behavior.max_key_pending_records`、`max_key_pending_bytes` 等框架预算，并通过 `max_in_flight_handlers` 限制并行处理。`consumer.concurrency` 是 owner 槽数，不等同于同分区业务键的执行并发。
