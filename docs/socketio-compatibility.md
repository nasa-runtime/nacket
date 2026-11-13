# Socket.IO 兼容入口

[English](socketio-compatibility.en.md)

Socket.IO 是 nacket 的外部接入协议。身份、路由、订阅、发送权限和唯一 ProtocolBytes 编码由同一服务内核管理。启用部署的 `transport.socketio` 后，每条路由仍需显式打开 `access.socketio.enabled`。

标准入口使用 `transport.socketioPath`，默认 `/socket.io/`，以及路由登记的 namespace。只接受 Engine.IO 4 的 WebSocket 传输；query 必须恰好包含 `EIO=4` 和 `transport=websocket`。认证凭据放在 CONNECT 的 `auth` 正文，不能放入 URL。当前入口不提供 polling 或 polling 升级流程。

业务正文作为单个二进制附件发送，使用该路由选择的 JSON_BYTES、VARINT_TLV、BITPACK_TLV 或 FAST_FIXED。操作 ACK 只说明操作结果；终端接收回执使用单独合同。CONNECT 成功后仍需完成 `nacket:hello`、`nacket:confirm` 和 `nacket:ready`，之后才能处理业务。

## 旧路径映射

`compatibility.legacyPath` 默认关闭。开启时，将当前路由的标准 namespace 同时用作旧 HTTP 升级路径，允许有一个末尾 `/`。例如：

```yaml
access:
  listenerRef: shared
  endpoint: /route-a
  socketio:
    enabled: true
    namespace: /route-a
compatibility:
  legacyPath: true
```

客户端可使用 HTTP path `/route-a/`，在该物理连接上连接根 namespace `/`，CONNECT 中的 `endpoint` 也必须为 `/`。服务端把它映射到已经登记的 `/route-a`，返回的线协议 namespace 保持 `/`，公开 Hello 中的 routeId、routeRevision 和 definitionDigest 对应实际业务定义。标准 `/socket.io/` 加 `/route-a` namespace 的入口继续有效。

旧路径只提供定位别名。它不接纳旧 query 身份声明，也不改变身份认证、Origin 检查、确认、初始化、订阅授权或消息格式。客户端仍须使用当前公开握手和二进制正文合同。`compatibility.joinLeaveAliases` 是单独开关；路径开关不会自动启用旧事件名。

映射有以下边界：

- 只允许非根、无末尾 `/` 的已登记 namespace；不得与固定原生或 Socket.IO 传输路径冲突。不执行 URL 解码、前缀匹配或默认路由回退。
- HTTP 升级时冻结 routeId、revision、摘要和实际监听作用域。后续认证前复验，定义变化后旧物理连接必须重建。
- 旧路径物理连接仅允许根 namespace，不能借同一连接切换其它路由。独立端口还必须匹配它绑定的路由。
- 关闭路由或撤销兼容开关后，新旧连接均继续受当前门禁约束。重新开启也不会让旧握手自动绑定新定义。
- 失败只返回有限错误类别，不回显凭据或内部控制材料。任何认证失败都不建立订阅或接收资格。

## 操作结果与连接资格

请求身份冲突、请求目录容量拒绝等正常业务拒绝，通过原请求 ACK 返回 `effect=none`；
只要身份与路由许可仍有效，该 namespace 可以继续发起新请求，同一物理连接的其它 namespace 不因此关闭。
同一请求身份的重试在有限保留期内共用执行与结果，不重复执行外部操作。

这不适用于主体失权、身份到期、路由撤销或实际输出失败。服务无法安全完成结果或写出 ACK 时关闭相应会话，
发生半包写出时关闭整个物理连接。重放范围和请求身份见[通信模型](communication.md#请求身份重放与拒绝)，
路由不可用及关闭事实见[状态通知](session-state.md)。
