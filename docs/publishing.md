# 业务发送与 MQ 发布

[English](publishing.en.md)

nacket 默认禁止客户端向其它客户端发送业务消息。常规下行由业务微服务的 REST 接口完成权限与业务校验，再向受信 Redis Stream 或 Kafka topic 发布。连接身份、群订阅和发送权限互相独立；成功 `sub` 不授予群发送权限。

## 客户端发送开关

路由必须显式开启 `clientMessages.enabled`，并选择一种执行方式：

| execution | 执行者与结果 |
| --- | --- |
| `business_command` | nacket 调用固定业务 POST 接口；业务服务负责持久幂等、事务与 MQ 发布。nacket 不再发布同一消息。 |
| `authorize_then_publish` | nacket 调用固定授权 POST 接口，验证返回的 `AuthorizedPublication`，再使用专用发布器提交 MQ。授权接口不得同时发布这条消息。 |

两种方式都要求有效身份、已开启的服务下行、明确的事件/schema、目标范围和 `operationId`。匿名路由不能开启客户端发送。业务处理失败不会退回本地广播。

`payloadAccess` 固定 HTTP 输入布局：

| 值 | 业务正文 |
| --- | --- |
| `raw_body`，默认 | HTTP body 为路由编码后的原始字节；控制 JSON 经 Base64URL 编码进入 `nacket-context` header。编码前控制 JSON 最多 6 KiB。 |
| `json` | 按路由唯一 schema 解码，放入普通 JSON 请求的 `arguments`；产生显式反序列化和重新编码成本。 |
| `metadata_only` | 只发送正文长度与摘要，不提供内容；仅限 `authorize_then_publish`，业务必须明确接受无需审核正文的授权合同。 |

原始正文使用 `Content-Type: application/octet-stream`。控制对象包含服务端建立的 `requestId`、`operationId`、`context`、`principal`、`target`、`event`、`schema` 和 `payload.bytes/digest`。客户端不能指定业务地址、服务名、broker 资源或可信主体。授权输入的正文与最终发布的正文来自同一个不可变 `Bytes`。

## 授权响应

授权服务返回严格 JSON。下面展示 `allow` 结构；摘要和标识符需由实际请求及持久业务状态产生。

```json
{
  "requestId": "本次服务端请求关联值",
  "operationId": "客户端持久操作标识",
  "decision": "allow",
  "publication": {
    "publicationId": "业务服务固定的消息标识",
    "grantId": "业务服务固定的授权标识",
    "expiresAt": 1792324860000,
    "bodyDigest": "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
    "push": {
      "kind": "push",
      "id": "业务服务固定的消息标识",
      "operationId": "客户端持久操作标识",
      "context": {
        "clusterId": "cluster-a",
        "scopeEpoch": "1",
        "tenantId": "tenant-a",
        "routeId": "route-a",
        "routeRevision": "4",
        "definitionDigest": "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
      },
      "event": "event-a",
      "schema": "schema-a",
      "target": {
        "kind": "group",
        "groupId": "group-a"
      },
      "sender": {
        "uid": "user-a"
      },
      "delivery": {
        "createdAt": 1792324800000,
        "expiresAt": 1792324860000
      }
    }
  }
}
```

`expiresAt` 是本次授权的截止时间；`push.delivery` 是固定消息存活窗口。所有时间均为 Unix 毫秒整数。`bodyDigest` 是实际正文的 SHA-256。`push.id` 必须等于 `publicationId`；主体、路由绑定、事件、schema 与操作身份必须匹配当前请求。群目标只能原样批准；用户列表只能缩小。拒绝响应为 `decision: "deny"`，省略 `publication`。

授权服务必须按主体、路由和 `operationId` 持久保存请求指纹及完整批准记录。相同操作重试必须返回同一个 `publicationId`、同一份 Push 元数据和原始时间窗口。不同正文或目标使用相同操作标识必须冲突拒绝。nacket 的短期请求缓存与下行去重都不能替代这份业务记录。断连后跨节点重试可能再次写入 broker，但沿用同一消息身份；这不是端到端 exactly-once。

## 固定发布器

部署的 `nacket.publishers` 是固定目录，修改需要重启。每个发布器限定用途、路由、来源、目的地和容量。下面是 Redis 配置片段：

```yaml
nacket:
  publishers:
    publisher-a:
      enabled: true
      purpose: push
      producer: socket-dispatcher
      routeIds: [route-a]
      transport:
        backend: redis_stream
        sourceRef: primary
        stream: socket:business
      maxPending: 128
      maxBytes: 8388608
      maxMetadataBytes: 16384
      maxBodyBytes: 262144
      timeoutMs: 3000
```

`authorize_then_publish` 的 `publisherRef` 必须指向 `purpose: push`。发布器的 transport 必须与路由 `servicePush.ingressRef` 指向的入口完全一致；入口使用独立 ACL 身份，固定 producer 必须与发布器一致且被路由允许。此发布路径不接受共享 HMAC 入口配置。不同权限的业务服务不得共用同一可写来源；broker ACL 必须承担真实生产者隔离，配置中的名称不能替代 ACL。

Kafka transport 使用 `backend: kafka`、`sourceRef`、`topic` 和明确的 `partitionCount`。命名 Kafka 来源必须提供 `producer.lanes.socket-publish`，有效 `acks` 为 `all` 且开启 `enable_idempotence`。启动时核对实际分区数；消息按固定作用域和目标生成排序 key，并据 SHA-256 选择分区。原始正文作为 record value，固定头为 `nacket-contract` 和 `nacket-meta`。

队列同时限制条数与逻辑字节，最多并行处理 16 条记录。正文、控制头及每项固定开销计入字节预算。等待队列与 broker 共用同一截止点，不自动重试未知结果。工作任务持有实际等待责任；客户端取消观察不会提前释放仍在使用的额度。网络栈、Redis 编码、HTTP builder 和 Kafka 原生库可能需要复制，不能把共享正文引用理解为全链路零拷贝。

## 普通业务通知

`notifications` 中的 `business_signal` 必须引用 `purpose: business_signal` 的独立发布器，设置 `requestSchema` 后使用路由唯一 codec。该操作要求 `operationId`，不能配置 `responseSchema`。通知仍需满足其独立 `authorization` 策略。

业务通知使用 `nacket.business/1` 合同，控制对象包含 `kind: business_signal`、`operationId`、`requestId`、可信 `context/principal`、`event`、`schema`、`bodyPresent` 和整数时间 `createdAt/expiresAt`；正文单独承载。通知不能设置下行目标。其目的地不得与下行、隔离或控制资源重合；部署还须用 ACL 防止不同 sourceRef 指向同一受保护资源。

## 结果与可观测性

| code / phase | effect | 含义 |
| --- | --- | --- |
| `broker_accepted` / `broker_accepted` | `accepted` | 收到真实 broker 回执；不代表终端接收或业务处理完成。 |
| `publish_not_dispatched` / `publish` | `none` | 发布前权限、期限、容量或队列准入未通过。 |
| `publish_unknown` / `publish` | `unknown` | 已交给发送器但没有确定成功证据；保持原操作身份查询业务状态。 |

`nacket_mq_publish_total{channel="business",outcome="committed|unknown"}` 记录实际发布等待结局；`nacket_live_resources{resource="publish_items|publish_bytes"}` 记录队列和在途任务持有的逻辑资源。`committed` 对 Redis 只表示收到 XADD 响应，Redis 的磁盘持久化与副本保障由部署决定。断开连接或路由下线不能撤销已经被 broker 接受的消息；消费者继续按原路由绑定和当前权限验证下行。
