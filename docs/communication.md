# 通信合同

[English](communication.en.md)

nacket 使用控制 metadata 与原始业务正文分区的 `nacket.split-envelope` 合同。metadata 是有界 UTF-8 JSON；正文使用路由唯一的 ProtocolBytes 模式。两者不能合并成需要每个节点反序列化的业务对象，也不能把业务字段当作权限字段。

镜像不接收旧 Java ObjectMessage，也不自动补消息 ID、租户、路由版本或有效期。旧生产者须在业务权限校验后转换为完整的新 Push，并在自己的持久 outbox 中固定 ID、创建时间、到期时间与正文，再提交受信 MQ。重投不能重新生成这些字段。

[通信模型目录](../contracts/communication.json) 的 `schemas` 中每个条目都是独立 JSON Schema，来自服务实际使用的类型。`$defs` 引用在该条目内解析。它描述字段、必填项和封闭结构；方向限制、请求关联、字节上限、合法版本、期限和当前权限仍由服务检查。重复 JSON 属性在解析前拒绝；结构 Schema 不能检测已被调用方 JSON parser 覆盖的重复属性。

## 共有字段与数值

| 字段 | 含义 |
| --- | --- |
| kind | 由入口限定的方向类别。客户端只能请求 sub、unsub、notify、msg，不能自报 push/control 获得权限 |
| id | 消息或当前会话请求的稳定标识；同 ID 内容冲突拒绝 |
| operationId | 业务副作用的持久幂等身份，由业务服务保存；与会话请求 id 分工不同 |
| correlationId | 原请求关联信息，不授予权限 |
| context | 按方向收窄的绑定；客户端只有 routeRevision，终端下行只有 routeId/routeRevision/definitionDigest，受信生产者使用完整 Context |
| event / schema | 当前路由声明的事件名与正文 schema 引用，不能逐消息选择另一种序列化 |
| target | 单选对象；只允许该 kind 的字段，不能组合多个目标类型 |
| sender | 仅受信下行接受的主体；终端仅看见公开 uid，不能上行伪造 |
| delivery | 生产者固定的 createdAt/expiresAt，以及只收窄目标的排除项；终端只见期限、messageKey 与可选回执 |
| traceId | 有限链路关联值，不用于鉴权、URL 选择或监控标签 |

所有绝对时间都是 `0..9007199254740991` 的 Unix 毫秒整数。日期字符串、数字字符串、浮点时间和越界整数拒绝。scopeEpoch、routeRevision、incarnation 和授权版本等通信代际使用规范正十进制字符串；配置中的 RouteDefinition.revision 及管理账本投影中的数值字段以具体 Schema 为准，调用方应采用能保留整数精度的 parser。

## 客户端建立与使用连接

1. 发送 `Authenticate`，明确 contract、endpoint、attemptId、token（按身份策略需要）和 capabilities。expectedBinding 可收紧目录定位，不能替代凭据。
2. 接收 `Hello`，检查本次 attemptId、sessionId、唯一 serialization、schemas 与定义摘要。Hello 尚不表示可以执行业务。
3. 回传 `Confirm` 的 attemptId、definitionDigest、confirmNonce。
4. 等待 `Ready`；服务完成连接授权、必要初始化及集群许可复验后才发送。确认、心跳和重试不能延长原身份期限。
5. 使用 `ClientRequest`；断开或权限变化后丢弃旧会话的票据、订阅资格和未执行缓冲。重连使用新 attemptId 与凭据。

所有 SDK 和直接客户端均须通过服务端的有效 uid 门禁：可信身份结果必须是非空、非全空白字符串，且满足 128 个 UTF-8 字节上限和无控制字符约束；缺失、null 和其它类型拒绝。校验发生在 Hello 之前，会话构造再次检查；失败不进入 Ready，不建立在线会话或执行订阅恢复。传输连接或 HTTP Upgrade 成功不代表认证成功。Socket.IO 各 namespace 独立受检，关闭额外连接授权不取消此规则，具体身份来源见[身份 HTTP 合同](http-api.md)。

| 操作 | 固定 metadata 与正文 | 生效边界 |
| --- | --- | --- |
| sub / unsub | kind 与 event 均为 sub 或 unsub；target 为 server；schema 指向路由 subscriptions.schemaRef；独立正文携带声明的群 ID 字段 | sub 申请读取权限，unsub 先撤资格再等待旧写出，不改变业务成员关系 |
| notify | kind=notify，event 为 notifications 白名单名称，target=server；按该动作决定是否存在正文 | 只能执行已配置动作；不能取得通用下行发送器 |
| msg | kind=msg，稳定 operationId、正文 schema、group 或 uids 目标 | 默认关闭；打开后仍经业务 REST 执行或授权，再向批准的 MQ 提交 |
| Receipt | token、messageKey、level、accepted | 只确认本会话已写出的对应票据；sdk_received 与 application_processed 不互换 |
| Heartbeat | 原样回传本轮 nonce | 证明连接活性；不刷新业务授权、订阅或消息 TTL |

`Downstream.result` 的 code/phase/effect/retryable/retryAfterMs 表达操作结果。`effect=none` 表示没有取得执行事实，accepted 表示已受理，completed 表示该层合同已完成，unknown 表示副作用结果未确定。unknown 不能换 operationId 重复执行；超时或本地取消不证明远端回滚。业务推送的 broker 接受、socket 完整写出、SDK 接收和应用处理必须分别统计。

`SessionStateNotice` 使用独立控制输出预算，绑定 attemptId/sessionId/context/stateSeq。状态通知可能因断连未送达；权限先行失效。见[会话状态](session-state.md)。

节点未取得本地就绪资格或正在关闭时，WebSocket HTTP Upgrade 返回 503，客户端尚未进入 AUTH。升级成功后，认证、定义和权限仍分别复验；HTTP 101 不能代替 Ready。已建立会话的 SDK 恢复可以在原次数及时间预算内重新建立传输，不应自动重放已经发送但结果未知的认证或业务操作。

## 原生 TCP / WebSocket 帧

TCP 逐帧读取以下布局；原生 WebSocket 每条二进制消息包含一条完整布局。WebSocket 文本消息不能代替二进制帧。原生 WebSocket 的 HTTP path 为部署 transport.nativePath，逻辑 endpoint 在 Authenticate 中选择。

| 字节偏移 | 长度 | 内容 |
| --- | --- | --- |
| 0 | 4 | 大端 u32，后续帧内容长度，不含本字段 |
| 4 | 1 | type：1 AUTH、2 AUTH_RESPONSE、3 EVENT、4 EVENT_ACK、5 PING、6 PONG、7 BROADCAST、8 CLOSE、9 CONFIRM、10 READY、11 RECEIPT、12 STATE |
| 5 | 1 | 有正文时为 ProtocolBytes ordinal；无正文固定 255 |
| 6 | 1 | bodyPresent：0 无正文，1 有正文；其它位值拒绝 |
| 7 | 4 | 大端 u32，非空 metadata UTF-8 JSON 的字节数 |
| 11 | metadataLength | 固定控制 JSON |
| 11 + metadataLength | 剩余长度 | 可选原始业务正文；零长度正文与不存在正文不同 |

完整帧至少 11 字节，metadata 不得为空，全部区域受协商及部署容量约束。ordinal 为 0 JSON_BYTES、1 VARINT_TLV、2 BITPACK_TLV、3 FAST_FIXED；只允许当前路由配置的一种。控制帧无正文且 mode=255；事件有正文时 schema 与存在标志必须一致。传输帧不是另一份业务序列化；正文使用共享 ProtocolBytes schema，无第二层产品自定义正文编码。

Socket.IO 使用同一控制模型与唯一正文编码，以标准单个二进制附件承载业务正文。`nacket:sub`、`nacket:unsub`、`nacket:notify`、`nacket:msg` 使用对应 ClientRequest；Hello/Confirm/Ready 为 `nacket:hello`、`nacket:confirm`、`nacket:ready`，推送为 `nacket:push`，回执为 `nacket:receipt`。操作 ACK 与终端投递回执独立，namespace 之间不共享权限。连接要求与旧路径别名见[Socket.IO 入口](socketio-compatibility.md)。

`sub`、`unsub`、`msg` 短事件名作为同一操作的别名接受；其 kind、目标、正文 schema、路由开关与权限检查完全相同。SDK 使用带 `nacket:` 前缀的名称；`join/leave` 仍只在显式兼容开关允许时转换。

## 受信 MQ 下行

业务微服务先通过 REST 完成业务身份与目标授权，再编码正文和 Push；服务端不接受浏览器持有 broker 或签名凭据。完整 Push 字段及注释在模型目录中，初始化回复和撤权使用独立入口、独立目的及来源权限。

| 内容 | Redis Stream field | Kafka |
| --- | --- | --- |
| 固定合同 | contract | 单值 header nacket-contract |
| Push / 专用控制 metadata 原字节 | meta | 单值 header nacket-meta |
| 原始正文 | message | record payload；禁止 null，允许专用控制记录的空字节 |
| HMAC 密钥索引 | keyId | 单值 header nacket-key-id |
| HMAC 认证码 | signature | 单值 header nacket-signature |

ACL 模式固定每个入口的真实 producer，不接受 keyId/signature；必须用 broker ACL 隔离写入权限，不能在同一可写入口相信自报 service 名。HMAC 模式必须提供成对密钥索引和认证码。重复、未知、null 或过大字段拒绝；Kafka 可额外包含共享框架固定 `X-Nasa-Event=DEFAULT`、`X-Nasa-Payload-Mode=raw`，不接受其它值。

HMAC 材料为 ASCII `nacket.ingress.signature` 加一个零字节，随后依次拼接 contract、ingress ID、真实 producer、key ID、实际 metadata 字节、正文 SHA-256 的 32 个原始字节；每项前加八字节大端无符号长度。先取整体 SHA-256 的小写十六进制文本，再对该 UTF-8 文本用密钥计算 HMAC-SHA256，输出 64 位小写十六进制。签名绑定实际 JSON 字节，不先解析再序列化；重排 metadata 后必须重新签名。

Kafka Push key 为 ASCII `nacket.ordering` 加零字节，依次拼接 clusterId、scopeEpoch、tenantId、routeId、target.kind 及目标身份。group 追加 groupId；uids 按 UTF-8 字节字典序排序后逐个追加；tenant_route_broadcast 不追加身份。每个字段前加四字节大端 UTF-8 长度。对 key 取 SHA-256，前四字节作为大端 u32，对固定 partitionCount 取模得到分区。revision 不进入 key，路由更新不改变同目标的排序分区。分区数量变更必须按部署合同重新准备，不能静默继续旧消费基线。

broker 确认只表示输入已提交。消费可重试，生产者必须保持原 id/operationId/createdAt/expiresAt 和内容；相同消息键的不同正文或目标拒绝。合法重投可能在节点崩溃边界再次到达客户端，应用按 messageKey 幂等。服务不承诺断线历史补发或端到端恰好一次。

[发布授权](publishing.md)、[初始化和生命周期](lifecycle.md)、[撤权命令](authorization-control.md)、[Kafka 恢复](kafka-ingress.md)。

## 操作拒绝与会话关闭

原生协议中，已正确解析的 EVENT 若违反业务方向或目标限制，返回 `operation_rejected`，`effect=none`，当前有效连接继续保留。未知字段、损坏帧、认证/确认不合法、会话或 UID 分区权威失效仍关闭连接。Socket.IO namespace 的非法事件方向可直接关闭该 namespace。SDK 不自动重放被拒绝或效果未知的业务操作。认证前服务明确报告 `route_unavailable` 时，SDK 可以在原预算内重试；无说明的 AUTH 后断流仍停止重连。

## 请求身份、重放与拒绝

同一会话的有限请求目录按请求 ID 与请求身份关联动作。保留期内，同身份的待决重试共用同一执行责任，
已完成重试取得保留结果；相同 ID 携带不同身份返回 `request_identity_conflict`，不执行新的副作用。
目录的条数与字节限制独立于操作速率；相应拒绝不把仍有效的会话误判为失权。
该目录不是跨重连的持久幂等存储，业务仍须维护稳定 operationId 和外部幂等合同。

Socket.IO 对已通过结构校验的请求，在动作准备阶段发生身份冲突、请求目录容量不足等拒绝时，
返回关联原请求的 ACK，`effect=none`，有效 namespace 可继续使用新的请求 ID。
这里的请求 ID 属于业务信封；Socket.IO ACK 编号仍须在途唯一。重复在途 ACK、请求速率超限或非法事件方向
不属于此类拒绝，按协议关闭受影响 namespace。
主体执行域失权、实际执行结果不能安全完成或 ACK 排队/写出失败时终止受影响会话；
物理传输半包失败仍关闭整个传输，不能为了维持连接而冒险错配附件。其它有效 namespace 的权限独立。
