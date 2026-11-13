# 业务授权撤销

[English](authorization-control.en.md)

业务服务完成权限校验后，可以通过专用 Redis Stream 或 Kafka 入口提交撤权。普通 Push 生产权限不能用于撤权，客户端不能提交这些命令。nacket 把已受理动作、授权版本下界和完成屏障保存在集群控制权威中；节点重启、路由下线和重新上线不清除这些事实。

## 开关与来源范围

路由默认关闭 `authorization.control.enabled`。开启时必须使用 Redis 或 Kafka 控制权威，并引用 `purpose: authorization_control`、`delivery: recoverable` 的启用入口。业务下行、初始化回复和授权控制各自使用明确的入口目的。

```yaml
authorization:
  control:
    enabled: true
    ingressRef: authorization
    producers:
      service-a:
        tenantIds: [tenant-a]
        actions: [revoke_subject, revoke_subscription, remove_group, kick]
```

以上段落属于路由定义。部署中的 `nacket.ingresses.authorization` 例如：

```yaml
enabled: true
purpose: authorization_control
transport:
  backend: redis_stream
  sourceRef: primary
  stream: socket:{authorization}:commands
identity:
  mode: acl
  producer: service-a
delivery: recoverable
recovery:
  initialize: false
maxMetadataBytes: 16384
maxBodyBytes: 65536
batchSize: 16
dependencyStaleMs: 5000
deadLetter: socket:{authorization}:commands.DLT
```

`identity.mode: acl` 要求 broker ACL 保证该资源只有声明的服务身份能生产；元数据中的 `producer` 不能自行证明身份。共享入口可以使用产品已有的 HMAC 来源验证，每个密钥仍绑定生产者范围。Redis 全新入口首次使用 `recovery.initialize: true`；需先创建 Stream，并保证稳定组和 owner 围栏都尚不存在。初始化脚本在执行时从当前末尾创建组和无 TTL 的 owner Hash，之后保留两者并使用 `initialize: false`。它不回放初始化前的撤权历史；业务应先建立正确的当前授权基线再接流。仅手工创建组不能建立合法恢复基线。Stream、稳定组、owner 围栏及同槽 DLT 资源须共同保留；已有任一基线丢失时不能用 true 重建来继续旧作用域，应围栏旧实例并恢复持久事实。Kafka 使用单独的受管来源、`read_committed` 和已经建立的节点稳定组位点；其入口配置将 `transport` 换为 `backend: kafka`、`sourceRef`、`topic` 和 `partitionCount`。

Kafka 来源必须设置 `behavior.max_consume_attempts: 0`。可恢复入口的暂时依赖等待不能耗尽普通重试次数后跳过输入；重试始终受本地队列、退避、原始命令期限和路由执行进度门禁约束。明确无效的记录仍按 invalid record 策略进入 DLT，不使用无限重试掩盖格式或权限拒绝。

关闭授权控制后拒绝新命令；此前已经持久受理的撤权仍然有效。关闭入口、删除引用或改变目的不能让授权配置不完整的路由进入接流状态。

## 命令结构

Redis 字段沿用 `contract`、`meta`、`message`，外层 `contract` 是 `nacket.split-envelope`；Kafka 对应 `nacket-contract`、`nacket-meta` 头和消息正文。授权命令全部位于 `meta`，正文必须为空。HMAC 仍签署实际元数据字节和空正文的摘要。

```json
{
  "contract": "nacket.authorization/1",
  "commandId": "authorization-7",
  "clusterId": "cluster-a",
  "scopeEpoch": "1",
  "routeId": "route-a",
  "routeRevision": "12",
  "definitionDigest": "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
  "producer": "service-a",
  "createdAt": 1792324800000,
  "expiresAt": 1792324860000,
  "action": {
    "kind": "revoke_subscription",
    "tenantId": "tenant-a",
    "uid": "user-a",
    "groupId": "group-a",
    "throughVersion": "17"
  }
}
```

| 字段 | 作用 |
| --- | --- |
| `contract` | 固定授权控制合同，不能使用普通 Push 结构替代 |
| `commandId` | 路由内稳定操作 ID；多节点消费和重试必须保留原 ID、目标及期限 |
| `clusterId`、`scopeEpoch`、`routeId` | 持久授权域；不会因路由定义更新而清空 |
| `routeRevision`、`definitionDigest` | 新命令必须匹配当前权威定义，防止旧来源策略继续产生副作用 |
| `producer` | 必须等于 ACL 或 HMAC 取得的实际身份，并在路由生产者目录中获准 |
| `createdAt`、`expiresAt` | 整数 Unix 毫秒；新命令的受理窗口最长五分钟，重试不能延长 |
| `action` | 单值租户与目标、固定动作类型和对应版本域 |

同 ID 同内容返回原受理结果；同 ID 异内容拒绝。精确重试查询已受理动作时不重新检查原受理期限，但仍检查当前来源权限。授权版本采用规范十进制字符串并按受检 `u64` 数值比较，不按字符串排序。命令期限届满不撤回已经提交的撤权。

首次受理要求 `createdAt <= 权威当前时间 < expiresAt`。生产者时钟稍快时返回 `authorization_command_not_yet_valid`，节点保留源记录并以原 ID、原内容重试；到达原 `expiresAt` 后拒绝。等待不刷新命令期限或 `dependencyStaleMs` 进度期限，过大的时钟偏移会触发入口保护。

| `action.kind` | 目标字段 | 作用 |
| --- | --- | --- |
| `revoke_subject` | `tenantId`、`uid`、`throughVersion` | 拒绝不高于该用户授权版本的身份和写出；旧身份不能通过延长时间继续使用 |
| `revoke_subscription` | 再加 `groupId` | 拒绝该用户在该群中不高于指定成员版本的证据，保留其它群及其它用户资格 |
| `remove_group` | `tenantId`、`groupId`、`throughVersion` | 拒绝不高于该群状态版本的群证据；同名群重新建立必须给出更高版本 |
| `kick` | `tenantId`、`uid`、`sessionKey` | 永久关闭匹配的完整会话，不含 `throughVersion`，不关闭同用户的新会话 |

`sessionKey` 包含 `clusterId/scopeEpoch/nodeId/incarnation/routeId/routeRevision/definitionDigest/routeSessionId`。业务服务从身份、初始化或生命周期上下文取得该键，不能仅根据客户端自报的短 session ID 踢出连接。三个业务版本域独立：用户版本较高不能覆盖成员或群撤权。新 grant 必须严格高于全部适用的下界。

Kafka key 是 `authorization.` 加上以下规范 JSON 数组的 SHA-256 小写十六进制文本：`[clusterId,scopeEpoch,routeId,action.tenantId]`。对完整 key 字节再计算 SHA-256，取前四字节的大端 `u32` 对固定 `partitionCount` 取模得到分区。该规则使同一路由租户的用户、成员与群撤权处于同一顺序域。

## 持久受理与完成屏障

处理顺序为：验证来源与当前定义 → 权威原子保存撤权下界及操作 → 节点恢复完整授权状态 → 撤销本地资格并等待相关旧写出 owner 结束 → 确认该节点应用完成。没有应用最新撤权状态的节点不能取得或续期路由许可。

MQ 消费位点只在权威确认持久受理后推进。该确认不代表所有节点已经完成撤权。要求撤权先于后续推送的业务流程，应通过受权管理接口查询：

```text
GET /management/routes/{routeId}/authorizations/{commandId}
```

响应包含 `routeId`、`operation` 和 `effect`。`effect=accepted` 表示仍存在旧许可责任；`effect=completed` 表示受理时的旧许可集合已经逐项取得本地应用回执、精确排空回执或合法失效证明。操作中的 `acceptedAt/completedAt` 均为整数 Unix 毫秒，`barrier` 保存尚未完成的精确许可 ID；它不是在线用户列表。

权限检查和最终 writer 共用同一撤权下界。写出许可保存取得许可时的用户、成员和群版本，后续刷新身份不能让旧写出恢复权限。完成屏障只等待已经捕获的旧写出，不等待后来通过新版本授权建立的写出。已到达客户端的数据无法撤回；普通异步模式从业务状态更新到网关收到撤权之间仍有传播窗口。

节点不能用墙钟推测失联实例已经停止写出。许可期限完成依赖部署明确开启的 `expiryCompletion` 及其调度、时钟和写出预算合同；未开启或缺少证明时，操作保持受理中，直至取得精确排空证据。

## 容量与失败

节点提交缓存最多 128 项、2 MiB 逻辑字节，同时最多八个权威请求；缓存中的完成结果保留三十秒，持久操作身份不随缓存回收。等待者取消、超时和未知提交不会把输入视为完成。明确的来源、目标定义、期限和同 ID 内容错误进入入口原有隔离流程。

Kafka 异步消费在原调用内等待同一交接工作的持久结果；正常等待不触发整组重投，也不提前确认位点。未知提交沿用原 ID、内容和进度期限退避；入口停止、容量不足或进度到期时仍保留未确认输入，按既有恢复与保护规则处理。

撤权执行和 MQ 连通性分别提供必要依赖证据。每条待决命令从首次交接开始使用入口的 `dependencyStaleMs` 作为固定进度期限，重试不能延长；超过期限或提交缓存饱和时，所有引用该入口的路由停止新接入和投递。执行任务每秒核对一次，健康证据最多保留三秒且不超过待决工作的原始期限；任务停止不能无限保留旧资格。完成原工作并重新确认执行能力后才恢复，故障前捕获的写出许可不能复用。

同一集群控制作用域最多保留 16384 条授权操作，完整控制快照还受总字节上限约束。撤权记录不按在线人数淘汰、不因普通 TTL 自动删除；达到容量时拒绝新增，不以删除旧事实恢复权限。业务授权计数需要重置时，应建立新的隔离代际并完成旧作用域退出，不能把 HTTP 返回版本直接归零。

`nacket_control_events_total` 区分 `authorization` 与 `authorization_applied`；资源指标中的 `authorization_commands`、`authorization_bytes` 表示节点真实保留的提交责任。依赖状态中的 `authorization:<ingressId>` 单独报告执行能力；它与同名 MQ 入口各自影响必要依赖就绪。操作查询用于判断全局完成，指标不能替代权限或屏障证据。

授权交接日志通过 `node_id`、`ingress`、`route_id`、`command_id` 关联 received、deferred、persisted、rejected、protected 阶段，记录提交次数与累计等待毫秒。persisted 只表示权威受理，仍须查询 `effect=completed` 判断全局完成。重复等待日志限频，命令 ID 不进入指标标签。Redis 控制器故障日志额外提供 owner 序号、输入位置、请求摘要、最后进入的阶段及是否超时；阶段涵盖租约获取、快照与高水位、读取请求、提交及提交后复验。不输出完整租约令牌、签名密钥或撤权目标。DEBUG 日志可关联提交确认及节点投影 revision。

## 主动授权刷新

路由可声明以下片段；`endpointRef` 引用部署已批准的只读授权接口。

```yaml
authorization:
  refresh:
    enabled: true
    endpointRef: authority-refresh
    schedule:
      advanceMs: 5000
      jitterMs: 1000
      retryMs: 1000
      timeoutMs: 1500
  cache:
    enabled: false
    positiveTtlMs: 1000
    negativeTtlMs: 200
    maxEntries: 128
    maxBytes: 1048576
    coalesce: true
```

开启刷新后，Ready 会话在原授权到期前进入统一有界队列。提前量加上按会话稳定分散的抖动；每会话至多一个刷新在途，显式 RefreshAuthorization 与自动刷新共用串行门禁。关闭后不自动请求，也不允许显式刷新动作延长期限。请求使用固定端点，包含 `operation: refresh`、`operationName: authorization.refresh`、当前可信 principal、authorizationId/version、精确 sessionKey 与路由允许的握手事实；响应遵循已有请求关联、allow/deny、授权版本、期限和 grants 合同。

成功必须复验同一会话、路由、撤权下界、原授权仍有效及主体不变，再经 UID 分区提交。JWT、可信网关等有独立凭据期限的身份，新的业务授权仍不得超过该期限。远程身份模式依据当前可信业务授权更新，但不能切换 UID 或租户。成功发布连续序号的 `authorization_refreshed` 通知，携带整数 `expiresAt` 和 `authorizationVersion`；SDK 只更新当前身份期限，不延长原消息、回执或旧订阅证据。

拒绝、异常、排队容量不足和超时均保留原期限，按 retryMs 退避；没有到期宽限。到期先撤销权限并清理全部订阅，迟到响应不得复活会话。节点队列和执行名额分别受 `maxRefreshPending`、`maxRefreshInFlight` 约束，同时继续占用 HTTP 节点和端点预算。停止时取消队列与在途责任。

## 可选只读订阅授权缓存

`authorization.cache.enabled` 默认 false，关闭时每次订阅授权调用业务接口。开启后只缓存订阅读取判定；不缓存 msg 发送许可、一次性操作权限或身份认证。精确键包含完整会话、路由定义、租户、UID、身份授权 ID/版本、撤权代际、端点、规范订阅集合及允许的握手上下文，不能跨会话借用授权。

正缓存期限取 positiveTtlMs、原身份期限和远端 grant 期限的最早者。只有明确 deny 可以进入 negativeTtlMs 短负缓存；网络故障、取消、超时及非法响应不形成拒绝缓存。命中及最终应用仍复验撤权、会话和订阅意图。撤权代际变化使已有条目失效；刷新后不同身份授权键不会命中旧条目。

`coalesce: true` 合并完全相同的在途查询，每个等待者仍可独立结束。单个等待者取消不取消其它等待者的共享查询；连接关闭、总期限或服务停止取消实际工作。条目和保留字节同时受每会话与节点上限约束；满容量直接走原授权调用，不淘汰未完成责任，也不把容量不足解释为允许。

刷新响应的 `principal` 必须省略或为 null；任何非 null 主体对象均拒绝，即使内容与原主体相同。刷新保留原会话主体，不能替换身份。
