# 配置结构与校验

[English](configuration.en.md)

nacket 通过 naml 加载同一棵 YAML 配置树。`nacket` 是产品配置；`application`、`log`、`ws`、`server`、`redis`、`kafka`、`nacos`、`discovery` 和 `grafana` 等段由 NASA Runtime 的对应组件管理。配置中心与注册发现分别启停，不相互隐式开启。

[configuration.schema.json](../contracts/configuration.schema.json) 是 `nacket` 正文的 JSON Schema，采用 JSON Schema 2020-12。将该段转为 JSON 后即可使用支持该标准的编辑器或校验器检查字段、类型、枚举、互斥分支和未知属性。不要把整个 application YAML 直接交给这个正文 Schema。

Schema 与部署类型一同维护，支持通过 `cargo run --features schema --bin nacket-schema` 输出。此命令只写标准输出，不加载配置、不连接外部服务、不开放端口。默认 Cargo 构建不启用 `schema` feature；产品镜像显式启用它并同时携带 Schema 输出命令。

## 主配置路径与文件观察

`APP_CONFIG_FILE` 在启动时选择主文件，未设置或为空时使用 `zcf/application.yml`。相对路径以进程启动工作目录为基准，绝对路径直接使用；目录名、中文和空格不改变选择规则。指定文件缺失、不可读、格式非法或内容不满足配置合同都会阻止启动，不会尝试默认文件。容器默认工作目录为 `/app`，可挂载整个配置目录并设置 `APP_CONFIG_FILE=/config/application.yml`。

相对文件导入以选定主文件的父目录为基准。`APP_PROFILE` 仍选择启动工作目录下的 `zcf/application-{profile}`，按支持的扩展名寻找覆盖文档，不随主文件迁移目录。环境快照和选定路径在加载器建立时固定；改变环境变量需要重启进程。

`config_watch.enabled: true` 在服务模式中观察选定文件及其依赖目录，支持文件原地写入和原子替换，并按 `config_watch.poll_interval_ms` 定期补读，默认 15000 毫秒。容器中需要原子替换时应挂载父目录，单文件 bind mount 可能继续指向旧 inode。读取或校验失败的候选不替换已生效配置；文件观察成功也不代表路由已完成控制屏障。`NACKET_LOG_PATH` 仍通过配置中的占位符设置日志目录，与主配置路径独立。

## 配置分组

| 配置 | 职责与边界 |
| --- | --- |
| `nodeId`、`clusterId`、`scopeEpoch`、`routeLedger` | 稳定节点、集群隔离、持久作用域和本地恢复基线；已有数据卷不能被当作新作用域自动初始化 |
| `control`、`routeProvisioning` | 本地或 Redis/Kafka 权威、节点角色及受信清单提案；观察到候选不等于全局应用 |
| `routes` | 路由声明、唯一编码、接入、身份、订阅、动作、下行与资源策略；默认权限不因能力开关自动扩大 |
| `endpoints`、`verifiers` | 固定远程 HTTP/发现目标及 JWT/网关验证器；客户端不能指定目标或凭据 |
| `ingresses`、`publishers` | 固定 MQ 来源、生产者权限、用途和恢复合同；业务下行与初始化、事实、撤权用途分别限制 |
| `listeners`、`primaryListener`、`transport` | 共享与独立监听、适配器、握手心跳、节点准入和缓冲硬上限 |
| `remoteCalls`、`ingressProcessing`、`lifecycleDelivery` | 节点级调用、读取、异步处理和生命周期责任预算 |
| `management`、`readiness`、`metricScopes` | 管理身份与网络范围、就绪策略以及启动固定的有限指标名称目录 |
| `brokerMetrics` | 独立只读积压采样开关、周期、期限和并发；默认关闭，运行期改变需要重启 |

Schema 中的 `default` 是缺省解析说明，校验器不应自动写回配置或替业务选择权限。只声明 `nodeId` 与 `routeLedger` 可以形成空路由部署；没有路由就没有可认证的业务入口。默认产品配置见 [application.yml](../zcf/application.yml)。

## 结构有效与实际可用

结构校验不能代替服务端完整校验。端点和来源引用、端口冲突、预算组合、匿名策略、协议类型与所选模式是否兼容、密钥可用性、broker 拓扑、持久版本和全局旧许可屏障都需要后续检查。例如，一个结构合法的远程身份路由仍会因为引用不存在的 `endpointRef` 而被拒绝。

路由的 `schemas` 使用 ProtocolBytes 动态描述符：`schemaId`、`fields`、`tag`、`name`、`type`、`nullable`、`maxBytes` 和 `maxElements`。`type.kind` 只接受固定基础类型、`enum`、`message`、`list`；`message.schemaRef` 只能引用当前路由的 schema，不能读取文件或下载远端类型。共享 SchemaPlan 负责类型编译和编码边界，Schema 导出不提供第二套编解码器。

路由定义通过新的 `revision` 更新，动态上线仍须先完成持久提交和旧写出屏障。端点参数、验签策略与 schema 在编译时冻结进引用路由的摘要；更新目录不会原地改变旧会话，引用新材料的路由必须提交新定义。监听模板、MQ 来源与发布器、节点预算、管理信任根和指标名称目录等启动资源变更需要重启；共享组件的发现连接、配置来源和凭据仍遵守各自生命周期。配置观察状态与有效运行状态分开展示。无效候选不得取代当前有效状态，旧快照不能撤销持久下线或暂停事实。

## 路由与节点策略

路由 `activation.minReadyNodes` 可独立配置 `1..256` 的上线门槛；省略时继承 `control.settings.minimumReadyNodes`。该值属于路由定义摘要，修改需提高 revision 并经过控制提交。准备节点不足时维持 Preparing，不能以一个节点的监听成功代替集群屏障；路由下线只要求一个节点接管控制责任，不被上线门槛阻止。

启用路由的上线操作从 `activating` 进入全局 `effective` 还要求当前全部 `targets` 都已上报 Applied，且 Applied 数达到 `minReadyNodes`；只达到数量门槛不足以完成操作。单节点先完成本地门禁复验并开放，再上报 Applied；因此 `activating` 期间已具备有效许可的部分节点可以接流并发布接入目录，不能把一个节点已服务或目录可见当作全局完成。下线操作在旧许可屏障清空后直接进入 `effective`，不要求全部节点 Applied。

`authorization.refresh`、`authorization.cache` 和 `handshake` 随路由定义冻结。缺省刷新关闭、缓存关闭、附加字段与请求头允许目录为空。关闭这些功能不会关闭原身份校验、授权到期清理或撤权。节点 `remoteCalls`、`ingressProcessing` 与 `sessionMetrics` 的容量和扫描参数为启动资源，改变需重启；路由策略通过新定义发布，不原地修改旧会话。

```yaml
nacket:
  remoteCalls:
    maxRefreshInFlight: 32
    maxRefreshPending: 256
    maxContextBytes: 67108864
    maxAuthorizationCacheEntries: 4096
    maxAuthorizationCacheBytes: 16777216
  ingressProcessing:
    maxPendingRecords: 256
    maxKeyPendingRecords: 64
    maxRetryAgeMs: 60000
    maxParallelFanouts: 4
  sessionMetrics:
    enabled: true
    activityWindowMs: 60000
    intervalMs: 100
    batchSize: 128
    maxTargets: 65536
    maxTargetBytes: 16777216
    maxCycleMs: 120000
```

这些节点配置不自动开启任何业务路由。路由级刷新、缓存与附加字段的交互合同见[授权控制](authorization-control.md)和 [HTTP 交互](http-api.md)。

## 路由热更的生效边界

本地路由替换依次关闭旧准入、持久提交新 revision、发布完整的新目录，再等待旧 writer 全部结束。账本中的 revision 只证明已提交，不证明新路由已经接流；`/readyz` 表示节点运行条件，允许部署中存在下线路由。注册中心的服务实例资格也不替代某条路由的生效证据。

INFO 日志 `event="route_configuration"` 的 `stage` 区分 `protected`、`committed`、`published` 与延迟开放时的 `activated`；`published` 携带 `local_online` 和拒绝原因。启用 `metricScopes` 并配置固定 `routeIds` 后，可通过 `nacket_scope_resources{kind="route",name="route-a",resource="online"}` 观察当前路由状态。此指标不授予权限，调用方仍须完成指定 revision 的身份校验、定义确认和 Ready 握手；网络、远程授权或注册资格变化仍可能拒绝连接。不得仅等待账本文件变化便认为路由可用。
