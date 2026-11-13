# 业务 HTTP 与管理 API

[English](http-api.en.md)

业务服务通过部署批准的 HTTP 端点提供身份、授权和业务执行；nacket 的管理端口只管理路由与控制状态。两类接口有独立凭据和权限，管理 API 没有通用客户端消息推送接口。

## 业务回调

端点配置包含固定 direct URL 或 discovery connection/service/path、GET/POST、超时、并发、响应字节上限、服务凭据环境引用、请求参数映射和响应字段映射。客户端只能填写已声明业务参数，不能指定地址、方法或服务凭据。Nacos 发现的实例选择由共享受管 HTTP 客户端执行。

普通 POST 为 application/json。raw_body 操作使用 application/octet-stream 原始正文，标准上下文先编码成 JSON，再以 Base64URL 放入 nacket-context header；上下文 JSON 最多 6144 字节。GET 只允许无 credentials 的 connect/subscribe/status/authorize，字符串为原值，其它 query 参数为 JSON 文本。认证 token 不进入 URL。受理后的副作用调用不能因响应丢失自动换实例重新执行。

| 请求字段 | 来源与含义 |
| --- | --- |
| requestId | nacket 为本次远程调用生成的关联 ID，响应必须回显 |
| operation | authenticate/connect/initialize/subscribe/authorize/refresh/notify/message/authorize_publication 等实际固定调用职责 |
| operationName | notify/msg 的路由白名单动作或事件，不是动态远程路径 |
| operationId | 客户端副作用请求的稳定业务身份，业务服务必须持久幂等处理 |
| context | 完整 SessionKey 加 attemptId，由当前节点产生，不接受客户端覆盖 |
| credentials.token | 仅认证调用所需原始材料，不能作为已验证 uid |
| handshake.peer / origin | 实际传输事实；Origin 不是身份凭据 |
| principal.uid / tenantId | 认证后服务端确定的主体，后续授权不能切换用户或租户 |
| subscriptionRequestId | 当前订阅意图的精确序号，响应必须回显；取消后迟到允许无效 |
| groupIds | 本次有限订阅申请，允许响应不能扩大集合 |
| authorizationVersion | 当前身份的十进制授权版本；与成员和群版本分别比较 |
| arguments | 按已声明 schema 解析的业务参数；没有配置 JSON 映射时原文仍可作为独立正文 |
| target / schema / payload | 消息执行或发布授权的目标、schema、原始正文长度及 SHA-256，不授予额外权限 |
| parameters | 配置增加的有限标量参数；映射只能读取批准字段，不能覆盖 context/principal |

当前字段按操作选取，不能假定每次请求都携带表中全部字段。结构目录中的 CallContext、Principal、AuthorizationResponse、SubscriptionResponse、RemoteResult、PublicationAuthorization、InitializationReply、LifecycleAcknowledgement 对应实际合同。

身份服务最小允许响应：

```json
{
  "requestId": "request-a",
  "decision": "allow",
  "principal": {
    "uid": "subject-a",
    "tenantId": "tenant-a"
  },
  "authorizationId": "authorization-a",
  "authorizationVersion": "1",
  "expiresAt": 1792324860000,
  "grants": {
    "canReceiveDirect": false,
    "allowedOperations": [],
    "groups": []
  }
}
```

expiresAt 必须是本次调用时仍有效且不超过部署 TTL 的整数毫秒。示例时刻只是格式示意，业务服务应从真实授权生成固定期限。允许结果不隐式允许所有动作或群。deny 不提供权限；HTTP 非 2xx、超时、未知 decision、错误 requestId、缺失证据和过期响应均保持关闭。只返回 HTTP 200 或一个 uid 不构成授权。

`principal.uid` 必须是非空、非全空白的字符串，最多 128 个 UTF-8 字节且不含控制字符。缺失、null、数字或其它类型都拒绝，即使 HTTP 200 且 decision=allow 也不建立会话。不自动 trim 或转换类型：字符串 `"0"` 合法，数字 `0` 不合法。该门禁同时适用于本地 JWT 和受信代理身份、所有 SDK 及接入协议，不能通过关闭额外连接授权跳过。显式匿名模式由服务端生成有效临时 uid；其它认证失败不能回退为匿名身份。

订阅允许响应额外回显 subscriptionRequestId，groups 中每项必须包含 groupId、authorizationId、authorizationVersion、groupVersion、expiresAt、canReceive；canReceive 必须为 true 且关联当前申请。成员版本、群版本和主体版本相互独立。版本过旧、重复群或越界目标拒绝整个本次操作。

RemoteResult 使用 requestId、operationId 和 status（accepted/completed/denied/unknown）报告业务处理；结果字段按操作声明的 responseSchema 编码。超时发生在移交 HTTP 之后时，effect 为 unknown，不能推断未执行。发布授权的 publicationId/grantId/bodyDigest/Push 及固定重试身份见[发布合同](publishing.md)。

响应字段映射只把明确批准的 JSON Pointer 投影到标准响应；allowValue/denyValue 必须明确对应 allow/deny，未知值拒绝。映射不能省略必要关联、主体或期限校验。接口关闭时不会执行对应调用；远程身份关闭并不自动成为匿名路由，匿名必须显式配置且受公共能力限制。

## 管理 API

[OpenAPI](../contracts/management.openapi.json) 枚举全部管理路径。启用 nacket.management 和独立 server listener 时，必须配置 allowedPeers、身份、独立控制密钥和有限请求预算；真实网络对端与 Bearer 身份必须同时匹配。凭据格式是 `Bearer <identityId>.<secret>`。readOnly=true 只允许 GET，在读取变更正文前拒绝写入。

| 方法与路径 | 作用 |
| --- | --- |
| POST /management/routes/validate | 编译候选定义并返回规范化定义、分配后的 revision 与摘要，不提交状态 |
| PUT /management/routes/{routeId} | 用 operationId/expectedRevision/definition 提交路由，首次创建 expectedRevision="0" |
| POST /management/routes/{routeId}/online | 从当前定义派生新 revision 并请求上线，不能绕过持久暂停 |
| POST /management/routes/{routeId}/offline | 持久暂停并下线，等待旧许可与写出屏障 |
| POST /management/routes/{routeId}/resume | 使用 expectedRevision/expectedOwnerRevision 解除暂停，不自动恢复旧连接 |
| POST /management/routes/{routeId}/transfer | 离线条件下转移 owner/ownerKind，两个 CAS 均必须匹配 |
| GET /management/routes/{routeId}/status | 期望状态、本节点状态、归属、暂停及持久操作进度 |
| GET /management/routes/{routeId}/describe | 面向客户端的当前编码、schema 与能力 |
| GET /management/routes/{routeId}/bindings | 本节点真实监听绑定与部署声明的外部映射 |
| GET /management/routes/{routeId}/directory | 有限有效、已经应用的接入地址；不代替客户端认证 |
| GET /management/routes/{routeId}/authorizations/{operationId} | 受信撤权命令的接受与全局完成状态 |
| GET /management/operations/{operationId} | 原路由、归属或退役操作的持久结果 |
| GET /management/manifests/{sourceId}/{generation} | 指定配置清单的逐条结果，任一路由超出身份范围则整体拒绝 |
| POST /management/nodes/{nodeId}/retire | 以 expectedIncarnation 精确退役实例，缺乏排空或有效失效证据时拒绝 |

管理路径不接受 query；GET 不接受正文。definition.revision 可省略，服务端始终用 expectedRevision+1 替换它；调用方不能自行选一个很大的 revision 越过 CAS。可选 definitionDigest 必须匹配规范化定义。validate 不证明资源已经绑定或全局生效。

```json
{
  "operationId": "offline-a",
  "expectedRevision": "1"
}
```

提交响应的 kind=operation 携带持久 operation。phase 可能是 quiescing、preparing、activating、effective 或 superseded；只有对应完成事实与当前状态满足时才可称已生效。继续按同一个 operationId 查询；不要把 HTTP 200、受理或本节点 localOnline 当作全局屏障完成。业务上线后仍可能因租约、授权或依赖失效进入保护。

路由变更失败和提交结果未知不同。400 为结构/语义拒绝，401 为对端或凭据拒绝，403 为只读/范围拒绝，404 为范围内资源不存在，409 为 revision/归属/屏障冲突，503 control_operation_unknown 表示应查询原操作。返回的 code 有限且不包含秘密。宿主请求字节、并发和期限治理可能在业务处理前产生其自身响应；这些响应同样不能作为未发生远端副作用的证据。

删除配置条目、关闭 provisioning 或重启不能擦除已持久的下线和归属事实。业务与管理调用都使用[配置结构](configuration.md)约束，关闭与未知责任见[关闭合同](shutdown.md)。

## 受限握手事实

AUTH 可省略 `authData`；提供时只能是最多 16 个标量属性的对象，完整 JSON 不超过 8192 字节。路由 `handshake.authFields` 按属性配置最大 JSON 字节数，单属性最多 4096。未声明属性拒绝连接，已声明的 null 属性按省略处理；不能通过这些字段覆盖可信 tenant、UID、sessionKey 或 authorizationVersion。

```yaml
handshake:
  authFields:
    deviceId: 256
    locale: 64
  headers:
    x-client-zone: 128
```

`headers` 最多 16 个小写名称，单值最大 1024 字节。只有白名单内的真实 WebSocket 升级头会保留；重复值和超量拒绝。Authorization、Cookie、Host、Origin、转发头、代理头、WebSocket 握手头及可信网关断言头禁止被转交。原生 TCP 无升级 header。部署所有路由的待认证头提取另受总容量限制。

HTTP 参数映射从 `/handshake/auth/deviceId`、`/handshake/peer`、`/handshake/origin`、`/headers/x-client-zone` 取值。它们是来源事实，不是业务权限。保留事实可用于身份、连接/初始化、订阅、通知、客户端发布和授权刷新调用；脱离连接的生命周期事实发布不自动附带握手材料。节点 `remoteCalls.maxContextBytes` 限制全部待确认和已连接会话保留的保守上下文字节；关闭连接归还预算，容量不足拒绝新接入。

## 标准回调请求模型

`contracts/communication.json` 的 `CallbackRequest` 定义九个标准 JSON 操作：`authenticate`、`connect`、`initialize`、`subscribe`、`authorize`、`refresh`、`notify`、`message`、`authorize_publication`。每个操作均包含 `requestId`、`context`、`handshake` 和 `headers`。`handshake.auth` 是路由允许的客户端自报标量映射，`headers` 是允许转交的请求头映射；无值时发送空对象。它们不构成身份或授权。独立生命周期事实不使用此请求联合体。

Rust SDK 使用 `CallbackRequest` 联合类型，Java SDK 使用 `Callbacks.read`；未知顶层属性和未知握手属性均拒绝。为接收未提供附加事实的请求，SDK 的空 `auth`、`headers` 有默认值，非认证操作可省略 `handshake`；这些兼容默认值不会生成权限。自定义字段映射的端点必须按自己的映射合同读取。

`refresh` 使用 `RefreshRequest`，`operationName` 固定为 `authorization.refresh`，包含原 `principal`、`authorizationId`、`authorizationVersion`。刷新响应的 `principal` 必须省略或为 null；非 null 主体对象即使与原主体相同也会被拒绝。主体由原会话保留，不能将普通操作授权模型代替身份续期。`parameters` 是部署映射批准的附加参数，缺省为空对象。
