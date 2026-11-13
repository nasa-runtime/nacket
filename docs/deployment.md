# 构建与部署

[English](deployment.en.md)

nacket 的同一镜像包含 ProtocolBytes 全部模式、原生 TCP/WebSocket、Socket.IO 兼容、Redis Stream、Kafka、Nacos、HTTP 和独立监控组件。配置决定启用的资源与业务权限。默认镜像只有空路由目录；就绪表示运行时可用，不表示已经开放业务路由。

## 源码依赖与构建

使用 Rust 1.95.0、Python 3.11 及以上、Clang/libclang、CMake、pkg-config、Perl 和 C/C++ 工具链。NASA Runtime `nasa 2.0.7` 及全部其它依赖来自 crates.io；`Cargo.lock` 固定实际版本、registry 来源和归档校验和。不需要相邻框架源码、Git 依赖、本地 `path` 覆盖或额外 Cargo 配置。

```sh
git clone --branch 1.0.0 https://github.com/nasa-runtime/nacket.git
cd nacket
cargo build --locked --release --features schema --bins
python3 ci/build-image.py --tag nacket:1.0.0
```

`rust-toolchain.toml` 固定工具链，Docker builder 安装 Kafka 与 TLS 所需的 C/C++ 工具。首次构建需要访问基础镜像、Debian 软件源和 crates.io；本地缓存不改变锁文件的来源与校验和。部署端不需要 Rust。

`ci/build-image.py` 只使用当前产品目录作为 Docker 上下文，`.dockerignore` 排除本地运行与生成内容。构建前后比较实际输入，Dockerfile 内在编译前后再次核对；源码、公开资源或锁文件变化会拒绝将产物视为当前输入镜像。构建保持 `--locked`，不能通过隐式更新锁文件接受依赖变化。

镜像包含服务与 schema 两个程序；后者可输出配置或通信合同：

```sh
docker run --rm --entrypoint nacket-schema nacket:1.0.0
docker run --rm --entrypoint nacket-schema nacket:1.0.0 communication
```

`python3 ci/package-source.py` 调用 Cargo 生成实际源码归档，并使用归档自身的 registry 依赖编译服务与 schema 程序。它核对双语 README、许可证、版本和规范化 manifest，逐字节核对解包内容，再从归档源码生成全 feature 严格公开库文档。归档不包含本地质量内容、缓存或运行数据。可用 `--target-dir /path/to/output` 指定产物位置，缓存齐全时使用 `--offline`；不需要路径覆盖。

单独生成库文档使用 `cargo rustdoc --locked --lib --all-features -- -D warnings -D missing-docs`，随后运行 `python3 ci/verify-docs.py target/doc`。缺失公开项说明、文档警告、依赖不可解析或归档内容不同均使命令失败。

CI 在原生 amd64、arm64 runner 分别使用锁定 registry 依赖构建，保留平台镜像归档和配置摘要，不自动上传。产品版本、Git 标签及 Docker Hub 版本均为 `1.0.0`。手动上传的来源绑定、权限、版本不可覆盖和概览回读见[容器发布说明](../docker/README.md)。发行镜像名称为 `nasaruntime/nacket:1.0.0`，本地镜像可命名为 `nacket:1.0.0`。

`docker/compose.yml` 默认使用本地镜像、空路由及独立数据/日志卷。镜像包含中英文公开文档 `/usr/share/doc/nacket`、产品双许可证和第三方许可材料。

## 容器运行

默认业务端口 `3030`、健康 HTTP `3031`、监控建议端口 `9464` 分开。健康 HTTP 提供 `/healthz` 与 `/readyz`；管理动作默认关闭。监控默认关闭，启用后必须使用独立监听和凭据。

```sh
docker volume create nacket-data
docker volume create nacket-logs
docker run -d --name nacket \
  --read-only --cap-drop ALL --security-opt no-new-privileges \
  --stop-timeout 25 \
  --mount type=volume,src=nacket-data,dst=/app/data \
  --mount type=volume,src=nacket-logs,dst=/usr/local/logs/nacket \
  --tmpfs /tmp:rw,noexec,nosuid,size=16m \
  -p 3030:3030 -p 127.0.0.1:3031:3031 \
  nacket:local
```

镜像以 `10001:10001` 运行。挂载已有目录时，配置文件必须可读，数据与日志目录必须可写；不要通过改成 root 运行来绕开目录权限。每个逻辑节点使用独立、持久的数据卷与稳定 `nodeId`，同一卷不能被两个运行实例共用。默认日志目录是 `/usr/local/logs/nacket`；设置 `NACKET_LOG_PATH` 为空只输出控制台，此时可省略日志卷。

配置挂载到 `/app/zcf/application.yml`。需要本地文件热更时挂载整个 `zcf` 目录，使 atomic rename 后的新文件对容器可见；单文件 bind mount 不能可靠观察宿主机替换后的 inode。`config_watch.enabled` 控制本地观察，环境变量在进程启动时固定，改变环境需要重建进程。

镜像 HEALTHCHECK 请求容器内 `127.0.0.1:3031/readyz`。更改 `server.port`、`server.context_path`、关闭 HTTP 或关闭健康路由时，必须同步覆盖探针。容器终止宽限应大于 `application.shutdown_timeout_ms`，给受管退出留出余量。业务准入先关闭，未知远程结果不会被记录成成功。

## 完整配置样例

| 文件 | 资源与能力 |
| --- | --- |
| [local.yml](../deploy/local.yml) | 无外部资源；显式匿名公共路由，仅允许公开订阅和当前连接状态通知 |
| [redis.yml](../deploy/redis.yml) | Redis 控制权威、实时 Stream 下行、远程身份与订阅授权 |
| [kafka.yml](../deploy/kafka.yml) | Kafka 控制权威、独立数据来源及每次启动的新消费组、远程身份与订阅授权 |
| [nacos-discovery.yml](../deploy/nacos-discovery.yml) | 只开启服务发现，业务 HTTP 按服务名负载均衡，不注册自身 |
| [nacos-registration.yml](../deploy/nacos-registration.yml) | 服务发现加固定引导入口自注册 |
| [nacos-configuration.yml](../deploy/nacos-configuration.yml) | 只开启集中配置，业务 HTTP 使用固定 URL |
| [nacos-combined.yml](../deploy/nacos-combined.yml) | 集中配置与服务发现独立同时开启 |
| [nacos-routes.yml](../deploy/nacos-routes.yml) | 上传配置中心的路由片段，必须与相应本地引导配置合并 |

除 local 外的样例需要实际业务 HTTP 合同；替换示例地址、来源和凭据后再启动。`NACKET_BUSINESS_TOKEN` 由业务服务验证；控制样例使用 `NACKET_NODE_A_KEY`、`NACKET_NODE_B_KEY` 和 `NACKET_CONFIG_KEY`，分别注入独立、高熵秘密。不能把控制管理员秘密分发给客户端。业务服务应先校验用户权限，再按[通信合同](communication.md)向获准来源生产消息。

## Redis 与 Kafka 基线

Redis 样例的 `NACKET_REDIS_URL` 指向已配置的 Redis。控制 namespace 和业务 Stream 使用同一 hash tag，但它们是不同资源；ACL 必须限制每个服务可写的资源。`identity.mode=acl` 仅适用于该入口实际只有配置生产者可写的部署，共用写权限时应选择绑定 producer 的签名身份。

`control.initialize: true` 只用于明确首次建立的控制作用域；已有基线丢失时应关闭自动初始化、围栏旧实例并恢复持久事实，不能靠重新启动空资源恢复业务权限。业务 Stream 每次启动取得当前末尾，无需恢复旧 PEL。复制 Redis 配置启动第二节点时，将 `nodeId` 与 `control.settings.nodeKeyId` 改为 `node-b`，使用独立数据卷；保持相同集群、epoch、来源、清单与权威。

Kafka 需要 `NACKET_KAFKA_BOOTSTRAP` 和真实 `NACKET_KAFKA_CLUSTER_ID`。启动前创建控制 command/state topic、业务 topic 和 DLT；控制 command/state 均为单分区，控制状态使用 `cleanup.policy=compact`，控制命令使用 `cleanup.policy=delete`，两者均设置 `retention.ms=-1` 与 `retention.bytes=-1`，并禁用不干净领导选举。实际 topic 策略由服务检查，错误拓扑不会被默默改写。

业务来源与 DLT 使用 `cleanup.policy=delete`，禁用不干净领导选举；生产部署根据耐久目标配置复制因子、最小同步副本数及有限存储保留。业务消费必须为 `realtime`，Kafka 设置 `auto_offset_reset: latest`，每次启动使用新组，无需预置旧组 offset。停机一小时再启动不读取这一小时的业务积压。授权控制入口的持久恢复独立配置，见[实时消费边界](mq-delivery.md)与 [Kafka 入口](kafka-ingress.md)。

业务路由的 `delivery.mode` 与对应 push ingress 的 `delivery` 都必须是 `realtime`，两者不能单独切换。此类 ingress 不设置 `recovery.initialize`；恢复初始化只属于需要持久权威的控制入口。控制来源的恢复能力不能用于恢复离线期间的业务推送。

两份 broker 样例使用 `readiness.policy: required_routes`，并将 `route-a` 列入 `routeIds`；只有控制许可、数据来源和该路由均可用时 `/readyz` 才成功。替换路由标识时同步更新此清单。空目录或只提供管理能力的部署可使用 `runtime`，但其就绪结果不保证任意业务路由已经开放。

两个后端的控制权限与业务生产权限分别配置。`routeProvisioning` 使用稳定 `sourceId`、递增 `generation` 和准确 `expectedRevision` 提案；已有路由更新不能继续用首次创建的 `0`。管理下线后的 suspended 不会被新 YAML 自动解除。

## Nacos 开关与注册

配置中心使用 `nacos.enabled`；服务发现使用 `discovery.enabled`。两个都关闭、仅配置、仅发现、同时开启均有独立行为。后端当前只接受 Nacos，Consul 与 Kubernetes 不作为可用选项。Nacos 服务端以 3.2.3 配置与发现接口为当前兼容基线，部署需要提供 HTTP 与其客户端要求的 gRPC 网络路径；不是只开放 HTTP 就能完成配置订阅。

示例通过 `NACKET_NACOS_USER`、`NACKET_NACOS_PASSWORD` 注入认证。配置中心地址由 `NACKET_NACOS_ADDRESS` 提供，发现 HTTP 地址由 `NACKET_NACOS_HTTP` 提供。namespace ID、group 和服务名必须与服务端一致；配置中心示例 namespace 为空表示默认命名空间，发现示例使用 `public`。

集中配置先把 `nacos-routes.yml` 正文保存为 `NACKET` group 下的 `nacket-routes.yml`。必需首拉失败不会就绪。远端片段只能包含业务配置，不能声明 `yml`、`nacos` 或 `secret_providers` 来修改引导权限。路由数组整体替换，运行期 revision 必须递增；集群部署还需有效清单提案。

注册样例的 `NACKET_ADVERTISE_IP` 必须是调用方可达的节点地址，禁止填 `0.0.0.0`。注册固定共享引导端口，`listener_ref` 与 `eligibility_ref` 都是 `socket`；它根据实际监听及本地就绪资格发布，不以注册成功自身作为资格来源。该示例假设同号可达网络；非同号 Docker NAT 应使用明确的接入目录，不能登记不可达容器地址。动态独立端口不通过修改 Nacos 注册来发布。

## 路由端口与外部网络

共享默认端口通过 `ws.addr` 或显式 listener 模板配置。路由 `access.dedicatedPort.enabled=false` 使用模板默认端口；设为 true 后，指定 `port` 则只绑定该端口，省略则从模板的 `autoStart`（默认 3030）递增到 `autoEnd`。共享、健康、监控及显式预约端口都会跳过；默认健康端口 3031 开启时，自动独立端口通常从 3032 获得。

Docker 只暴露显式发布的端口。自动范围应收紧为部署已经映射的有限范围，例如 3032–3062；这不包含健康 3031。非同号映射通过 `advertise.ports` 描述，绑定成功与外部可达是两种事实。完整的 listener、映射、目录与 `expectedBinding` 合同见[外部地址与接入目录](access-directory.md)。

业务监听接收明文 TCP 与 HTTP WebSocket Upgrade，不内置 TLS 终止或 PROXY protocol。需要 TLS 时由部署网关终止，并保护网关至服务的内部链路；公告中的 `wss` 地址必须指向实际提供 TLS 的入口。来源准入使用真实 socket 对端，转发头不会改变该身份；业务身份仍由路由配置的身份策略验证。

独立监控的认证、采集和中英文面板见[监控文档](monitoring.md)。架构、权限及失败顺序见[架构说明](architecture.md)。镜像内 `/usr/share/licenses/nacket` 保存产品和第三方许可材料；依赖组成与许可文件边界见[第三方依赖](../THIRD-PARTY.md)。

镜像内 `/usr/share/nacket/build-manifest.json` 记录 Docker 构建实际使用的产品文件及 Cargo.lock 的 SHA-256，以及两个可执行文件的摘要。构建前后核对同一输入；读取清单可确认产品源码、锁定 registry 依赖图和二进制。该清单依赖受信构建环境，不是独立签名或对恶意构建者的证明；公开交付还应通过固定提交和构建来源证明确认信任链。
