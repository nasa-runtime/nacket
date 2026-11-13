# 外部地址与接入目录

[English](access-directory.en.md)

nacket 的自动独立端口由每个节点实际绑定。同一路由可以在不同节点取得不同端口。管理目录按完整路由定义、节点代际和真实 `bindingId` 发布各节点的地址，不把一个节点的端口覆盖到整个集群。

自动分配跳过共享、管理、监控端口，以及同一权威快照中所有启用路由指定的独立端口；尚未完成本地准备的路由也保留该边界。指定端口必须实际绑定成功，被其它进程或已有监听占用时该路由保持关闭，不改用其它端口，也不迁移已经工作的自动监听。端口预留本身不表示路由已经上线。

默认关闭地址发布。绑定 `0.0.0.0:3032` 只说明本机持有端口，不说明浏览器可访问。Docker 端口发布、NAT、反向代理、防火墙和外部 TLS 终止由部署准备，再通过启动固定的监听模板声明：

```yaml
nacket:
  listeners:
    shared:
      bindAddress: 0.0.0.0
      defaultPort: 3030
      protocols: [native_tcp, native_ws, socketio]
      dedicatedPorts:
        allowed: true
        autoStart: 3032
        autoEnd: 3062
      advertise:
        enabled: true
        host: socket.example.com
        secure: true
        networkReady: true
        ports:
          - boundStart: 3030
            publicStart: 13030
            count: 33
```

这里本地 `3032` 映射到外部 `13032`。`networkReady: true` 是部署对已经建立的网络路径的声明，不是服务主动探测成功；`secure: true` 声明外部使用 TLS，不会在原生 listener 上自动启用 TLS。范围必须与实际网络配置一致，不应仅为让配置通过而填写。关闭 `enabled` 可以保留映射材料，但不会发布地址。

每个模板最多 16 组映射，本地和外部范围分别不得重叠、为零或溢出。共享默认端口须有映射。启用自动独立端口的路由，其全部候选范围须被映射覆盖；显式独立端口须有精确映射。缺失声明时该路由准备失败并保持保护，不能退回共享端口。部署应把自动范围收紧到实际开放的有限端口。

## 查询合同

`GET /management/routes/{routeId}/directory` 使用独立管理 listener、真实 TCP 来源白名单、Bearer 身份和路由范围限制。默认没有匿名公共目录。业务微服务可以使用只读身份查询，再通过自己的授权 REST 接口向客户端提供允许的入口；不要将管理写令牌放进浏览器。

```yaml
nacket:
  management:
    enabled: true
    allowedPeers: [127.0.0.1]
    identities:
      directory-reader:
        tokenSecretRef: NACKET_DIRECTORY_TOKEN
        controlKeyId: directory-scope
        readOnly: true
```

`controlKeyId` 必须引用已经配置的管理员控制身份，其 `routes` 约束可读范围。`readOnly: true` 在读取请求正文和提交命令前拒绝全部非 GET 方法。Bearer 格式为 `directory-reader.<secret>`，秘密来自 `tokenSecretRef` 指定的环境材料。管理入口其余部署要求仍适用。

响应包含 `clusterId`、`scopeEpoch`、`routeId`、字符串 `routeRevision`、`definitionDigest`、整数毫秒 `observedAt` 和 `entries`。每项包含：

| 属性 | 作用 |
| --- | --- |
| `binding` | 精确路由 ID、定义 revision 和摘要。 |
| `nodeId`、`incarnation` | 发布节点与持久进程代际，不能仅按 nodeId 缓存。 |
| `permitId`、`requestNonce` | 本次地址声明绑定的路由许可与续约请求，不构成客户端授权。 |
| `issuedAt`、`expiresAt` | 整数 Unix 毫秒；沿用本次许可的原始寿命，同 nonce 重试不刷新。 |
| `serving` | 节点已提交本地激活回执；目录只返回 true 的记录。 |
| `listener.bindingId` | 真实监听 owner 的身份；端口关闭重用、进程重启不能沿用旧身份。 |
| `listener.boundAddress` | 管理诊断用的本机地址，不能直接当作公网地址。 |
| `listener.host`、`port`、`secure` | 部署声明的外部主机、映射端口及 TLS 属性。 |
| `listener.endpoints` | 本节点实际允许的适配器、scheme、transport path 和业务 endpoint。 |

`native_tcp` 使用 `tcp` 或外部 `tls`，没有 transport path；`native_ws` 使用 `ws/wss` 和独立 WebSocket path；`socketio` 使用 `http/https`、Engine.IO path，`endpoint` 表示 namespace。原生接入的 `endpoint` 放入 AUTH，而不是替换 WebSocket path。IPv6 主机在构造 URL 时须使用方括号。

地址声明与路由许可同次持久提交，再由节点实际激活回执允许查询。下线、旧定义、旧节点代际、许可变化、缺失许可和期限结束都会排除对应项。控制来源不可读取时查询失败，不用过期本地地址冒充新结果。目录是有限发现提示，不能保证查询之后网络或权限仍然有效；客户端必须完成正常握手。

`GET /management/routes/{routeId}/bindings` 单独报告当前节点的 `boundAddress`、`bindingId`、`incarnation`、本地 `state`、`advertised` 和 `publicAccess`。`reachability: deployment_declared` 表示有当前目录项及本地资格；`unpublished` 表示没有当前公开声明。它不声称执行过外网连通性探测。

## 固定握手目标

从目录选址后，原生 AUTH 或 Socket.IO CONNECT auth 可附加 `expectedBinding`：

```json
{
  "clusterId": "cluster-a",
  "scopeEpoch": "1",
  "nodeId": "node-a",
  "incarnation": "3",
  "routeId": "route-a",
  "routeRevision": "12",
  "definitionDigest": "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
  "bindingId": "listener-a"
}
```

这八个字段必须完整。服务器在调用远程身份服务前复验全部目标；旧端口复用、旧节点和旧定义明确拒绝，客户端刷新目录后建立新的接入尝试。该字段只能收紧目标，不能授予身份或订阅权限。直接配置固定入口的客户端可以省略，仍必须核对 hello 中的路由定义并完成 confirm。

Nacos 继续注册固定引导入口，不为动态端口热改注册配置。naway 或其它网关可以通过只读目录取得节点上游，但必须显式实现目录消费、期限及绑定校验；仅配置一个固定 upstream 不会自动获得动态选址能力。没有这种适配时使用已准备的静态端口映射。
## 客户端连接投影

业务 REST 在授权后选择一个目录项，返回 SDK 共用的有限结构：

| 字段 | 作用 |
| --- | --- |
| `adapter` | `native_websocket` 或 `socketio`；Rust 还支持 `native_tcp` |
| `url` | 已批准的公开地址，无凭据、query 或片段 |
| `endpoint` | AUTH 逻辑入口；Socket.IO 对应 namespace |
| `path`、`namespace` | 可选 Socket.IO Engine 路径与 namespace |
| `expectedBinding` | 上述八个字段的完整精确绑定 |
| `expiresAt` | 继承选中目录项的 Unix 毫秒期限，不能刷新或延长 |

投影的 `native_websocket` 对应管理目录 endpoint 的 `native_ws`。REST 返回的 URL origin 必须同时位于客户端本地可信集合；目录权限不能替代消息权限。各端 SocketClient 在每次自动重连前重新读取投影，定义改变时清除旧订阅意图；未知 origin 或非法投影直接终止，不扫描端口或回退旧地址。静态 expectedBinding 只是一份固定断言，不包含目录读取能力。
