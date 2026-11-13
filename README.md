# nacket

[English](README.en.md)

nacket 是可独立部署的通用长连接接入、动态路由与集群消息分发服务。接入方通过声明式配置和 HTTP/MQ 合同提供身份、业务授权与消息生产；服务负责连接、订阅、协议适配、路由权限、集群投递及资源管理。

服务不预置特定业务路由或业务对象。`route-a`、`service-a`、`event-a` 和 `schema-a` 均为通用示例，实际名称、序列化方式和业务流程由接入方决定。默认禁止客户端互发：订阅群 ID 只能申请读取资格，不能取得向其他用户发送消息的权限。

同一套配置把动态定义、有限路由许可和最终写出门禁连接起来：受理配置不等于已经接流，
客户端连接存活也不等于仍有发送或接收权限。运行架构与各阶段确认见[架构说明](docs/architecture.md)。

## 快速开始

源码使用 Rust 1.95.0，NASA Runtime 与全部其它依赖来自 crates.io，由 `Cargo.lock` 固定版本及校验和；不需要相邻框架源码或本地路径覆盖：

```sh
git clone --branch 1.0.0 https://github.com/nasa-runtime/nacket.git
cd nacket
python3 ci/build-image.py --tag nacket:1.0.0
docker compose --file docker/compose.yml up --detach
curl --fail http://127.0.0.1:3031/readyz
```

该启动方式使用默认空路由目录，不访问 Redis、Kafka 或 Nacos。`readyz` 成功只确认宿主运行时就绪；业务路由需要独立配置和实际应用确认。用 `docker compose --file docker/compose.yml down` 停止，数据卷默认保留。部署所需的持久卷、日志、`APP_CONFIG_FILE`、独立端口与受权来源见[容器指南](docker/README.md)和[完整部署说明](docs/deployment.md)。

发行镜像名称为 `nasaruntime/nacket:1.0.0`，支持 `linux/amd64`、`linux/arm64`。镜像由手动发布流程上传；本地构建无需 Docker Hub 凭据。

## 核心能力

- **运行期动态路由**：创建、上线、下线、定义变更与恢复由受信配置或管理入口驱动。集群通过持久 revision、实例代际、准备状态及旧许可屏障切换定义；旧会话和旧消息不能进入新路由定义。
- **每路由唯一编码**：完整支持 ProtocolBytes 的 `JSON_BYTES`、`VARINT_TLV`、`BITPACK_TLV`、`FAST_FIXED`。路由管理方选择一种模式及合法 schema，同一路由的各接入方式使用相同编码。客户端不能逐条覆盖模式。
- **接入协议适配**：原生 TCP、WebSocket 和可独立启用的 Socket.IO。Socket.IO 是外部兼容入口，核心路由、权限和消息模型不依赖其 namespace 或 ACK 实现。
- **受信下行**：Redis Stream 或 Kafka 消费独立 metadata 和原始业务正文；同格式投递共享正文 bytes。服务检查来源、路由定义、目标、有效期、订阅及最终写出权限。
- **业务接入开关**：身份校验、额外连接授权、`sub/unsub`、白名单 `notify`、显式启用的 `msg`、初始化、上下线事实及接收策略分别配置。能力开启不会隐式获得业务权限。
- **主体事件保序**：同节点内相同可信 `(tenantId, uid)` 的跨连接、跨路由事件由 napart 严格消费；远程等待不阻塞分区，撤权立即收紧，完成回到原分区复验。见[顺序与容量](docs/uid-ordering.md)。
- **受管基础设施**：复用 NASA Runtime 的 napp、naml、HTTP、Redis、Kafka、Nacos 与 nafana；应用入口使用 `#[nasa::application]`，配置、网络资源和关闭流程由同一宿主管理。

## 消息与权限

默认发送流程为：业务微服务 REST 校验身份与业务权限 → 发布 Redis Stream/Kafka → nacket 集群校验并投递 → 客户端接收。

`sub/unsub` 管理当前连接的订阅意图，不改变业务成员关系。私有订阅必须有合法、未过期的读取授权。取消订阅先关闭新投递资格，再等待已经开始的相关写出结束。身份、群授权、路由许可和消息期限均不能因重试或心跳延长。

`notify` 只执行配置白名单中的当前连接操作、固定 HTTP 调用或独立业务通知发布。它不能取得通用下行发送器。`msg` 默认关闭；显式开启后，使用 `business_command` 交给固定业务 REST 执行，或使用 `authorize_then_publish` 取得绑定原始正文摘要、路由和目标的发布许可，再向 MQ 提交。两条路径均不直接本地广播。

正文按路由唯一编码传输。纯转发不要求反序列化业务对象；订阅、JSON 参数映射及需要检查正文的策略会解析相应数据。HTTP 客户端、MQ 客户端、WebSocket 掩码、TLS 和内核边界仍可能复制数据，服务不承诺所有边界零拷贝。

所有通信绝对时间为非负 Unix 毫秒整数，限制在 JavaScript 安全整数范围内。日期字符串、数字字符串和浮点时间拒绝。broker 确认、完整写出、客户端接收与业务处理是不同阶段，不能互相代替。

业务撤权使用独立受信 MQ 入口，用户、成员与群版本分别持久保存；精确会话踢出和撤权完成屏障覆盖整个集群。详见[业务授权撤销](docs/authorization-control.md)、[发布与远程授权合同](docs/publishing.md)、[连接生命周期](docs/lifecycle.md)。

## 路由与部署

默认主端口为 3030，可通过 `ws.addr` 修改。`ws.multiplex: true` 将原生 TCP 与 WebSocket 交给统一受管入口分发。路由可以使用共享端口，或配置独立端口；自动分配通过实际 bind 取得端口，不先探测后抢占。监控和管理端口不参与业务端口分配。

本地控制模式使用 `routeLedger` 持久文件与文件锁维护定义；它不提供多节点一致性。集群模式显式选择 Redis Stream 或 Kafka 控制后端，通过持久控制权威维护定义、节点实例代际和有限许可。数据面节点不能用本地配置绕过控制屏障；控制来源失效超过许可期限后进入保护状态。

配置或管理请求受理不等于集群已应用。更新先停止旧定义产生新的权限，再等待旧写出屏障并开放新定义。准备、持久化或依赖失败时保持关闭或保护状态。相同 revision 的不同内容、旧 revision 和冲突入口被拒绝。

配置清单与管理 API 的路由归属分开；紧急下线保留 suspended 状态，后续配置不能自动解除。解除暂停、归属转移和重建均需要明确的受权操作，不能通过省略配置清除持久下线事实。

## 配置、身份与注册发现

YAML 使用 naml 严格加载、校验与热更。配置中心与注册发现独立启停；Nacos 可分别用于集中配置和业务服务发现。远程调用使用部署批准的端点和受管负载均衡，客户端不能指定 URL、服务名、HTTP 方法或服务凭据。

`APP_CONFIG_FILE` 可指定主配置的相对或绝对路径；未设置或为空时读取工作目录下的 `zcf/application.yml`。指定文件无效会拒绝启动，不回退默认文件；路径、profile 和热更规则见[配置说明](docs/configuration.md)。

身份策略包括远程 HTTP、配置公钥或 JWKS 的本地 JWT、受信网关断言及受限匿名入口。匿名路由只能使用明确开放的公共能力。远程允许结果必须关联当前请求、包含合法主体及有界有效期；拒绝、错误、过期或无效响应不产生权限。连接确认绑定当前路由摘要，初始化完成前不能接收业务 fan-out。

所有 SDK 和直接客户端都必须取得可信的非空、非全空白字符串 uid；缺失、null、非字符串、控制字符和超过 128 个 UTF-8 字节的值拒绝建立已认证会话。此门禁覆盖全部接入协议和重连，不能通过关闭额外连接授权跳过。合法 ID 保留原文；显式匿名入口由服务端生成有效临时 uid，认证失败不能转为匿名放行。

受信网关断言在集群控制权威中持久消费，Redis 与 Kafka 均保持同一断言只属于一个精确接入尝试；本地模式同步保存消费基线。详见[网关断言](docs/gateway-assertions.md)。

路由声明变更通过新的 revision 应用；监听模板、固定发布目录及节点预算等部署资源修改需要重启。无效候选不能替换有效定义，配置观察到候选不代表资源已切换。

默认配置没有在线路由，Redis、Kafka、Nacos 配置及注册发现均显式关闭。启用来源后还必须提供合法资源、来源权限和路由配置；镜像包含组件不等于业务权限已开放。

字段结构、公开 JSON Schema 和完整校验的边界见[配置结构与校验](docs/configuration.md)。

## 观测与运行边界

nafana 使用独立监听地址和端口导出指标。进程与可见容器资源、配置中的文件系统和网卡由共享采集器读取；不可见、不支持、失败和过期数据单独表达，不填成健康零值。产品指标覆盖连接、请求、授权、订阅、出站队列、实际写出、回执、MQ、控制与配置。

日志由 napp 装配 nalog，默认 `info` 级别，文件目录为 `/usr/local/logs/${application.name}`。`NACKET_LOG_PATH` 可覆盖目录，显式空值只输出控制台。非空目录必须允许运行用户写入，见[日志配置](docs/logging.md)。

建连成功与断连日志默认开启，可分别通过 `nacket.connectionLogging.connected`、`disconnected` 热更关闭。断连始终取消该连接的全部订阅；日志包含会话身份、路由版本、来源 IP、连接时长和取消订阅数量。

节点及连接的队列同时限制条数与字节。慢消费者可以拒绝、关闭或按路由显式允许的状态事件保留最新值；逐条消息不能隐式转成覆盖语义。正常关闭先撤销准入和权限，再在原始期限与宿主总退出期限内排空责任。

- 当前 Socket.IO 接入支持 Engine.IO 4 的 WebSocket 传输，不提供 HTTP polling。显式旧路径映射及其权限边界见 [Socket.IO 兼容入口](docs/socketio-compatibility.md)。
- Kafka 来源与稳定消费组的首次准备、缺口保护见[Kafka 下行与恢复基线](docs/kafka-ingress.md)。
- 所有业务端口共用节点准入与传输字节预算，见[接入与字节预算](docs/resource-limits.md)。
- 连接配额支持节点与集群范围；集群名额持久提交，替换等待旧写出结束，见[连接配额](docs/connection-quotas.md)。
- 一次性断言消费需要可用的持久权威；结果未知会拒绝接入，调用方应取得新断言。
- 初始化授权支持 HTTP 或独立 MQ 回复入口；回复绑定原请求、完整会话与主体，不能将 broker 确认当作授权。
- 连接事实队列不是持久 outbox，进程崩溃可能丢失通知。业务在线状态需要租期或对账。
- 服务不存储业务历史，不执行业务流程共识，不保证端到端 exactly-once，也不把 MQ 保留视为客户端断线补发合同。

完整样例、registry 依赖、镜像运行、Nacos 开关与端口映射见[构建与部署](docs/deployment.md)；组件职责及运行顺序见[架构说明](docs/architecture.md)。

## 源码与许可证

`bootstrap` 负责装配，`config` 校验声明，`domain` 保存权限与协议不变量，`application` 协调流程，`adapters` 连接外部系统，`observability` 输出有限状态投影。

构建所解析的 NASA Runtime 组件必须提供受管协议处理、预绑定监听、显式可选来源、指标容量预留及消费者恢复接口。框架能力与产品合同分离：nacket 的路由、信封、权限及业务事件不进入通用框架。

源码、文档及随附配置可在 [MIT](LICENSE-MIT) 或 [Apache-2.0](LICENSE-APACHE) 中任选其一使用。第三方组件遵循各自许可证，详见[依赖说明](THIRD-PARTY.md)。贡献方式见 [CONTRIBUTING.md](CONTRIBUTING.md)，信任边界与私密报告方式见 [SECURITY.md](SECURITY.md)。本服务以源码与容器交付，`publish = false`，不通过 crates.io 分发独立服务包。

## 接口与运行文档

- [通信模型与帧布局](docs/communication.md)、[业务 HTTP 与管理 API](docs/http-api.md)、[管理 OpenAPI](contracts/management.openapi.json)。
- [外部地址与接入目录](docs/access-directory.md)、[会话状态通知](docs/session-state.md)。
- [监控与指标语义](docs/monitoring.md)、[关闭与持久任务责任](docs/shutdown.md)。

握手路由描述的 `restoreSubscriptions` 告知客户端是否自动重新申请期望订阅；关闭后仍可主动 `sub`，重连不会继承旧会话权限。三端配套产品统一命名为 **Nacket SDK**，包名 `nacket-sdk`；当前仓库提供服务端通信合同，不包含 Rust、Java 或 npm SDK 发行包。

默认健康 HTTP 独立监听 3031，提供 `/healthz` 与 `/readyz`；管理动作默认关闭。监控使用另一独立端口及凭据。`server.enabled=false` 会关闭健康监听，部署者需同步修改容器探针。

采用 [MIT](LICENSE-MIT) 或 [Apache-2.0](LICENSE-APACHE) 双许可证，可任选其一。依赖组成见[第三方依赖](THIRD-PARTY.md)。

业务推送和初始化回复在服务重启后跳过离线积压；Redis 从实际消费开始时的末尾读取，Kafka 使用本次启动独立组和 latest。持久路由与撤权状态仍须恢复。详见[实时消费边界](docs/mq-delivery.md)。
