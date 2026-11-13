# 会话状态通知

[English](session-state.en.md)

原生接入使用无业务体的 `STATE` 帧（`0x0C`），Socket.IO 使用当前 namespace 内的 `nacket:state` 事件。通知说明当前连接已经发生或即将发生的权限变化，不授予权限，不改变路由编码，也不能承载业务正文。

```json
{
  "context": {
    "routeId": "route-a",
    "routeRevision": "1",
    "definitionDigest": "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
  },
  "attemptId": "attempt-a",
  "sessionId": "session-a",
  "stateSeq": "1",
  "reason": "subscription_revoked",
  "issuedAt": 1790000000000,
  "groupId": "group-a",
  "authorizationVersion": "3",
  "groupVersion": "4"
}
```

| 字段 | 含义 |
| --- | --- |
| `context` | 当前公开路由绑定；仅包含路由 ID、定义代次及摘要 |
| `attemptId`、`sessionId` | 本次握手和精确会话；重连后不能沿用旧通知 |
| `stateSeq` | 从 `"1"` 开始、每个已保留通知递增的十进制字符串；跨语言无整数精度损失 |
| `reason` | 下表中的封闭原因 |
| `issuedAt` | 服务端生成通知时的 Unix 毫秒整数 |
| `groupId` | 仅订阅变化存在，每条最多一个当前连接曾获授权的订阅 |
| `authorizationVersion` | 受影响的旧身份或成员授权版本，使用十进制字符串；不是可重新提交的授权凭据 |
| `groupVersion` | 订阅旧授权中的群状态版本，不与成员授权版本混用 |
| `expiresAt` | 到期提示或身份续期存在，Unix 毫秒整数；服务端始终以实际单调期限决定失效 |

| 原因 | 服务端事实与终端处理 |
| --- | --- |
| `subscription_revoked` | 成员授权已失效，清除该连接的确认订阅与期望订阅 |
| `subscription_expired` | 订阅授权已到期；重新订阅仍须取得新授权 |
| `group_removed` | 已知群状态下界使旧订阅失效，清除该组订阅 |
| `authorization_expiring` | 当前身份剩余有效期不超过五秒；每个身份期限最多提示一次，通知本身不执行刷新 |
| `authorization_expired` | 身份到期，连接资格已经关闭 |
| `authorization_revoked` | 身份被撤销，连接资格已经关闭 |
| `session_revoked` | 精确会话被踢出或权限域不可用，不能迁移旧会话资格 |
| `route_unavailable` | 有限控制许可过期，旧会话及全部订阅已撤销；SDK 可按重连开关和原预算重新认证、重新申请订阅 |
| `route_draining` | 路由被主动关闭或定义失效，旧会话终止；SDK 停止自动重连 |
| `session_closed` | 本会话关闭，清除其业务状态 |

撤权事实和订阅移除先于通知生效。旧业务写出仍按会话、路由、授权版本和原始期限复验；通知排队或发送成功不代表旧写出屏障已经完成。Socket.IO 的 Ready 与状态通知共用独立控制 FIFO，Ready 写出前再次复验会话。单个 namespace 的正常关闭保留其它有效 namespace；发生半包写出错误时整个物理传输关闭，防止附件错配。

客户端必须核对 attempt、session、绑定和严格连续的 `stateSeq`。旧连接通知应忽略；当前连接序号出现缺口或绑定不符时重新连接并取得基线，不推测订阅资格。状态通知无需 ACK，也没有重放保证。网络中断、进程退出或控制容量不足时可能只观察到断线；断线必须清除全部确认状态。不得把未收到通知解释为权限仍有效。

每会话最多保留 32 条状态通知，每条预约 16 KiB，覆盖有限对象及控制编码临时空间。`nacket.transport.maxControlOutboundBytes` 默认 8 MiB，允许 64 KiB 至 64 MiB，跨所有业务端口共享；业务正文不能占用该池。Socket.IO 另有最多 256 项的物理控制队列，与业务队列分开；状态移交保留原预约，不重复计费。原生 writer 按序排空状态，正常关闭时最多尝试一秒；发生部分写出错误后不再发送其它帧。

任一状态队列或控制字节容量不足会关闭会话或物理连接，不静默丢弃某个序号后继续工作。`nacket_live_resources{resource="control_output_items"}` 和 `control_output_bytes` 跟随真实控制 owner 释放。握手、心跳和状态的完整发送计入传输统计；自动 WebSocket 控制刷新及未完成的半帧不计为完整应用消息。`nacket_duration_seconds{stage="heartbeat_rtt"}` 使用单调时间，包含本地排队、网络往返和终端处理时间，不代表纯网络延迟。

## 身份续期

`authorization_refreshed` 携带当前精确会话绑定、下一个连续 `stateSeq`、新 `authorizationVersion`、`issuedAt` 和 `expiresAt`，不携带 `groupId` 或 `groupVersion`。客户端只在旧身份期限仍有效时接纳，更新连接身份截止点；已有订阅证据、消息票据和操作截止点不延长，也不授予消息重放资格。

## 服务端与客户端的观察边界

授权命令的 `completed` 证明其目标节点已经应用撤权及完成服务端写出屏障，不证明客户端 reader 已经收到或处理 STATE。控制查询和长连接使用不同通信通道，没有客户端确认时不能建立两者的到达顺序。STATE 不需要 ACK，`stateSeq` 只排序当前会话的状态事实，不是业务帧的远程回收序号。普通 notify ACK 也不是撤权通知的接收确认。

SDK 处理有效状态时先失效相应会话或订阅，再公开该事实；应用从此不能取得仍在 SDK 队列中的旧正文。已写入网络、尚未收到本地失效事实的消息，以及已经交给应用的正文，不具备远程撤回保证。需要在一次业务权限变更后继续处理敏感内容的应用，应等待当前会话的相应状态事实，或停止该会话的消费并重新取得授权；控制查询成功本身不解除这一要求。
