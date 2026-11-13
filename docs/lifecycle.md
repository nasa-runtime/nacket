# 连接生命周期

[English](lifecycle.en.md)

生命周期以精确 `SessionKey` 为单位。一个主体的不同设备、重连和不同路由分别形成连接记录。事件不会授予群读取权限，也不会向其他客户端转发消息。

## 初始化与连接事实

`lifecycle.initialization` 在认证和路由确认之后、Ready 之前执行。HTTP 或 MQ 初始化需要明确允许的关联响应；开启 `required` 时，超时、拒绝或无效证据均关闭连接。初始化可以提供有期限的授权快照，但不能更换认证主体。未完成的授权不会通过连接事实回调补发。

`lifecycle.online` 在建立 Ready 回复顺序后记录一次上线；`lifecycle.offline` 在撤销本地权限、取消待决操作并移除会话后记录一次下线。初始化失败的已确认会话也可以产生 `wasReady: false` 的下线事件。两个事实开关相互独立，关闭其中任何一个都不影响本地状态清理。

上下线回调支持 HTTP 和 MQ，不接受 `required` 字段。它们描述已经发生的状态变化，不能阻止断连或重新授权。初始化支持 `required`；MQ 初始化请求与授权回复使用文末的独立合同。

## 配置

以下内容分别放入部署配置和某一条路由定义。示例端点、发布器必须在部署目录中存在；产品不创建内置路由。

```yaml
nacket:
  lifecycleDelivery:
    workers: 16
    maxPending: 1024
    maxBytes: 8388608
    maxEventBytes: 16384
    maxAttempts: 1
    retryBackoffMs: 100
```

```yaml
lifecycle:
  initialization:
    enabled: false
    required: true
    timeoutMs: 5000
  online:
    enabled: true
    timeoutMs: 5000
    target:
      mode: http
      endpointRef: lifecycle-endpoint
  offline:
    enabled: true
    timeoutMs: 5000
    target:
      mode: mq
      publisherRef: lifecycle-publisher
```

HTTP 端点必须使用 POST，服务认证和直接地址或服务发现由部署端点配置决定。MQ 发布器必须是 `purpose: lifecycle`，且 `routeIds` 允许当前路由。生命周期出口与业务下行、隔离记录和控制资源分离，不能写入客户端消息入口。Redis Stream 与 Kafka 的发布器设置见[发布通道](publishing.md)。

`workers` 为固定有序分片数量，范围 1–64；同一连接的事实进入同一分片。`maxPending` 覆盖排队、远程等待及退避，范围为 workers–65536。`maxBytes` 为节点事实预算，最高 64 MiB；必须容纳两个 `maxEventBytes` 加 4096 字节。`maxEventBytes` 范围为 1024–65536 字节。`maxAttempts` 为 1–5，默认不重试；`retryBackoffMs` 为 10–5000 毫秒。队列满时记录拒绝，不无限等待、不保留连接权限。

这些节点预算修改后需要重启；路由回调设置随路由新 revision 应用。`timeoutMs` 是事件自创建开始的总期限，包含排队、网络和重试；每次尝试不会重新计算 TTL。

## 事件合同

HTTP 请求体是下面的 JSON。MQ 使用 `nacket.business/1`，metadata 为同一 JSON，message 为空字节；不需要反序列化业务正文。

```json
{
  "contract": "nacket.business/1",
  "kind": "lifecycle",
  "eventId": "event-id-a",
  "requestId": "event-id-a",
  "operation": "offline",
  "sequence": 2,
  "context": {
    "clusterId": "cluster-a",
    "scopeEpoch": "1",
    "nodeId": "node-a",
    "incarnation": "1",
    "routeId": "route-a",
    "routeRevision": "1",
    "definitionDigest": "sha256:0000000000000000000000000000000000000000000000000000000000000000",
    "routeSessionId": "session-a",
    "attemptId": "attempt-a"
  },
  "principal": {
    "uid": "subject-a",
    "tenantId": "tenant-a"
  },
  "wasReady": true,
  "createdAt": 1792324800000,
  "expiresAt": 1792324805000,
  "bodyPresent": false
}
```

| 属性 | 作用 |
| --- | --- |
| contract / kind | 固定合同与生命周期事件类别，不是下行 push |
| eventId | 服务器生成的幂等键，同一事件的重试保持不变 |
| requestId | 与 eventId 相同，供固定远程参数映射关联请求 |
| operation | online 或 offline，不由客户端指定 |
| sequence | online 为 1，offline 为 2；仅在同一精确会话内比较 |
| context.clusterId / scopeEpoch | 部署作用域与持久控制代际 |
| context.nodeId / incarnation | 原始节点及该节点的当前运行代际 |
| context.routeId / routeRevision / definitionDigest | 事件产生时冻结的路由身份与定义，不随热更重写 |
| context.routeSessionId | 本次路由连接的唯一身份，重连生成新值 |
| context.attemptId | 建连尝试关联标识，不是认证凭据 |
| principal.uid / tenantId | 从已验证身份取得的主体及租户 |
| wasReady | 此连接是否曾进入 Ready，不能表示整个主体在线或离线 |
| createdAt / expiresAt | 首次创建时确定的 Unix 毫秒整数，重试不变 |
| bodyPresent | 固定为 false，事件没有独立业务正文 |

HTTP 成功响应必须为 `{"eventId":"event-id-a","status":"accepted"}` 或对应 `duplicate`。仅 HTTP 200、错误关联、无效 JSON 或其他字段不构成确认。响应不能携带授权或投递动作。业务端按 eventId 幂等受理；启用重试时，已提交但响应丢失的请求可能再次到达。

本节点按会话顺序发起回调；发生未知提交结果时，网络和业务处理仍可能造成迟到。消费方应按完整 SessionKey 保留最高 sequence，拒绝迟到的低序号状态，不能仅按 uid 覆盖所有连接。MQ 确认只证明 broker 接受，不证明业务已处理。

## 关闭与观测

正常停机先封闭会话准入、撤销权限并登记下线，再关闭事实入口。已接纳项在原事件期限和应用总退出期限内排空；生命周期 MQ worker 在事实排空之后停止，底层 HTTP/MQ 连接由 napp 回收。UID 执行域已关闭时，本地断连记录 `cleanup_path=local_fallback` 并记录下线事实拒绝，不能改由网络线程绕过分区顺序发送。退出期限耗尽、进程崩溃或容量拒绝仍可能丢失通知；业务端需要基于会话租期或对账维护最终状态，不能把此队列当作持久 outbox。

`nacket_lifecycle_events_total{event,outcome}` 区分接纳、拒绝、确认、过期、未知与取消。`nacket_live_resources{resource="lifecycle_items"}` 和 `lifecycle_bytes` 表示仍持有的责任；`nacket_duration_seconds{stage="lifecycle_delivery"}` 记录发送与退避耗时。所有标签均来自有限目录，不放入主体、会话或事件 ID。

## MQ 初始化授权

路由可改用 MQ 初始化。请求使用生命周期发布器；回复使用单独的 `purpose: initialization_reply` 入口。此入口不能作为 `servicePush.ingressRef`，普通 push 入口也不解析初始化授权回复。

```yaml
lifecycle:
  initialization:
    enabled: true
    required: true
    timeoutMs: 5000
    target:
      mode: mq
      publisherRef: initialization-publisher
    reply:
      ingressRef: initialization-replies
      allowedProducers: [service-a]
      maxGrantTtlMs: 60000
```

```yaml
nacket:
  lifecycleDelivery:
    maxPendingInitializations: 128
    maxInitializationBytes: 8388608
  ingresses:
    initialization-replies:
      enabled: true
      purpose: initialization_reply
      transport:
        backend: redis_stream
        sourceRef: primary
        stream: socket:initialization-replies
      identity:
        mode: acl
        producer: service-a
      maxMetadataBytes: 16384
      maxBodyBytes: 1
      deadLetter: socket:initialization-replies:dead
```

Kafka 使用相同的入口目的与来源规则，transport 改为 `backend: kafka`、固定 `sourceRef`、`topic` 和 `partitionCount`，同时配置独立 DLT。两类 broker 都要求回复正文为空。生产者身份来自固定 ACL 或 HMAC 验签；metadata 中自报身份不构成来源许可。

服务先预约待决数量及完整回复容量，再登记请求 ID，最后发布请求，避免快速回复早于等待者登记。`maxPendingInitializations` 范围 1–65536，`maxInitializationBytes` 范围 4096–67108864；每个待决请求预约 `2 × maxMetadataBytes + 4096` 字节的逻辑预算，与上下线事实队列预算分别计费。回复来源尚未取得新鲜消费基线时，路由保持保护状态。

请求沿用上文 `context` 和 `principal`，`kind` 为 `initialization_request`，包含服务器生成的 `requestId`、固定 `operation: initialize`、整数 `createdAt/expiresAt` 及 `bodyPresent: false`。业务服务保留关联字段，向预先约定的回复入口发布：

```json
{
  "kind": "initialization_reply",
  "requestId": "request-a",
  "context": {
    "clusterId": "cluster-a",
    "scopeEpoch": "1",
    "nodeId": "node-a",
    "incarnation": "1",
    "routeId": "route-a",
    "routeRevision": "1",
    "definitionDigest": "sha256:0000000000000000000000000000000000000000000000000000000000000000",
    "routeSessionId": "session-a"
  },
  "principal": {
    "uid": "subject-a",
    "tenantId": "tenant-a"
  },
  "response": {
    "requestId": "request-a",
    "decision": "allow",
    "authorizationId": "grant-a",
    "authorizationVersion": "1",
    "expiresAt": 1792324860000,
    "grants": {
      "canReceiveDirect": true,
      "allowedOperations": [],
      "groups": []
    }
  }
}
```

| 回复属性 | 作用 |
| --- | --- |
| kind | 固定初始化回复类别，不能替换成 push 或管理命令 |
| requestId | 原始请求的唯一关联标识 |
| context | 完整 SessionKey，属性含义同上；回复不包含 attemptId，不接受跨会话、跨定义或跨节点代际覆盖 |
| principal | 必须与该等待会话已认证的 uid/tenantId 完全一致 |
| response.requestId | 必须再次匹配外层 requestId |
| response.decision | allow 或 deny；拒绝不授予权限 |
| response.authorizationId / authorizationVersion | 授权服务提供的标识与规范十进制单调版本，允许时必填 |
| response.expiresAt | 授权到期的整数 Unix 毫秒，不能超出 maxGrantTtlMs 和当前身份期限 |
| response.grants.canReceiveDirect | 对本会话用户定向接收的授权上限 |
| response.grants.allowedOperations | 本会话允许的通知操作名集合，不创建未在路由定义中的操作 |
| response.grants.groups | 有界群读取证据；实际接收仍需订阅意图及群状态/授权代际检查 |

回复外层 broker 合同为 `nacket.split-envelope`，Redis 字段是 `contract/meta/message`，Kafka header 是 `nacket-contract/nacket-meta`，message/payload 为空。Kafka 排序键为 ASCII `nacket.initialization` 加一个零字节，再依次拼接 clusterId、scopeEpoch、nodeId、incarnation、routeId、routeRevision、definitionDigest、routeSessionId、requestId；每个 UTF-8 字段前加四字节大端字节长度。分区为排序键 SHA-256 前四字节按大端无符号数解释后对 partitionCount 取模。

只有原等待者仍存活、尚未 Ready、来源被允许且全部关联一致，回复才进入授权校验。错误主体或错误关联不能抢占正确回复；重复、超时、断连及旧实例回复不影响新连接。broker 接受或发布结果未知时仍只等待原请求的合法回复，不重新生成请求。截止点不会续期；必要初始化失败关闭会话，可选初始化失败也不会开放缺失的权限。
