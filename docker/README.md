# nacket 容器指南

[English](README.en.md) · [项目首页](https://github.com/nasa-runtime/nacket) · [完整部署说明](https://github.com/nasa-runtime/nacket/blob/1.0.0/docs/deployment.md)

nacket 提供通用长连接接入、动态路由与受信 Redis Stream/Kafka 投递。原生 TCP、WebSocket 和可选 Socket.IO 共用 ProtocolBytes、身份、订阅及最终写出权限。路由切换先关闭旧权限和排空旧写出，再开放新定义；配置受理不代表实际接流。

## 镜像与本地构建

发行名称为 `nasaruntime/nacket:1.0.0`，目标平台为 `linux/amd64`、`linux/arm64`。`latest` 是可移动入口；部署应固定版本，严格固定内容时使用镜像清单摘要。发行上传由手动工作流完成，不因推送分支或 Git 标签自动上传。

NASA Runtime 与其它依赖均从 crates.io 获取，`Cargo.lock` 固定版本及校验和，不需要相邻框架源码。在 nacket 根目录构建：

```sh
python3 ci/build-image.py --tag nacket:1.0.0
docker compose --file docker/compose.yml up --detach
curl --fail http://127.0.0.1:3031/readyz
docker compose --file docker/compose.yml logs --tail 100
```

Compose 默认选择本地镜像。发行镜像已上传后，可以显式选择：

```sh
export NACKET_IMAGE=nasaruntime/nacket:1.0.0
docker compose --file docker/compose.yml pull
docker compose --file docker/compose.yml up --detach
```

同一镜像包含全部四种编码、Redis、Kafka、Nacos、Socket.IO 与监控组件，但默认无在线路由，外部来源与监控均关闭。默认 readiness 只确认宿主运行时，不证明业务路由已应用。Compose 的两个宿主端口均只绑定回环，远程接入须显式调整并配置网络权限。

## 端口、卷与环境

| 设置 | 合同 |
| --- | --- |
| `3030` | 默认共享业务端口；TCP/WebSocket 复用。动态独立端口须另行映射。 |
| `3031` | 独立健康 HTTP，提供 `/healthz`、`/readyz`；管理动作默认关闭。 |
| `9464` | 监控建议端口；默认关闭，不应直接映射到公网。 |
| `/app/data` | UID/GID `10001:10001` 可写的独占持久卷；保存路由和授权相关事实。 |
| `/usr/local/logs/nacket` | 默认文件日志目录；需独立可写卷。 |
| `NACKET_LOG_PATH` | 覆盖日志目录，显式空值只输出控制台。 |
| `NACKET_LOG_LEVEL` | 启动日志级别，默认 `info`；重建进程后生效。 |
| `APP_CONFIG_FILE` | 相对或绝对主配置路径；未设置或空值读取 `/app/zcf/application.yml`。指定文件无效时拒绝启动。 |
| `APP_PROFILE` | 使用 naml 独立 profile 模板；改变主文件不会重定位 profile 路径。 |
| `stop_grace_period: 25s` | 覆盖默认应用 15 秒关闭预算；应用预算增大时同步增大容器宽限。 |

每个逻辑节点使用稳定 `nodeId` 与独占卷；不能让两个实例共享同一卷。初始卷从镜像目录继承权限，已有 bind 目录由部署者预先设置读写权限。不要通过 root 身份规避权限错误。

`docker compose --file docker/compose.yml down` 停止并保留命名卷。删除数据卷会丢失持久事实，不可用空初始化继续旧作用域；卷恢复与集群控制恢复见完整部署说明。

## 挂载配置与实际业务

以下无 broker 的公共示例仅开放匿名公共订阅与当前连接状态通知，不开放业务推送。配置目录整体只读挂载，主文件由 `APP_CONFIG_FILE` 指定：

```sh
docker volume create nacket-data
docker volume create nacket-logs
docker run --detach --name nacket \
  --read-only --cap-drop ALL --security-opt no-new-privileges \
  --restart unless-stopped --stop-timeout 25 \
  --env APP_CONFIG_FILE=/etc/nacket/local.yml \
  --env NACKET_LOG_PATH=/usr/local/logs/nacket \
  --mount "type=bind,src=$PWD/deploy,dst=/etc/nacket,readonly" \
  --mount type=volume,src=nacket-data,dst=/app/data \
  --mount type=volume,src=nacket-logs,dst=/usr/local/logs/nacket \
  --tmpfs /tmp:rw,noexec,nosuid,size=16m \
  --publish 127.0.0.1:3030:3030 --publish 127.0.0.1:3031:3031 \
  nacket:1.0.0
```

不要与 Compose 在相同宿主端口同时运行。使用 Redis/Kafka 样例时，提供容器可达的 broker、可信业务 REST、密钥与正确控制作用域，并替换节点身份。`localhost` 在容器中指向容器自身。Nacos 配置、发现和注册独立启用；Nacos 地址、namespace、认证及 gRPC 网络必须实际可达。

文件热更需挂载整个目录，使 atomic rename 后的新文件可见；单文件挂载不能可靠观察宿主机 inode 替换。`config_watch.enabled` 控制观察。环境变量和引导资源修改需要重建进程。配置观察到候选不等于路由实际 Applied；业务 readiness 应采用所需路由策略。

业务入口不内置 TLS 或 PROXY protocol。使用可信 TLS 网关，保护内部链路，显式配置公告地址和端口映射；转发头不会改变实际 socket 对端的来源身份。业务历史、离线补发和端到端 exactly-once 不属于本服务合同。

## 运维与分发内容

镜像以非 root 用户运行，支持只读根目录。默认 HEALTHCHECK 检查 `127.0.0.1:3031/readyz`；更改 HTTP 端口、路径或关闭健康服务时必须覆盖探针。监控需独立端口和凭据；中英文 Grafana 面板与规则位于 `/usr/share/nacket/monitoring`。

两个可执行文件为 `/usr/local/bin/nacket`、`/usr/local/bin/nacket-schema`。后者可输出配置 Schema 或 `communication` 合同。`/usr/share/nacket/build-manifest.json` 记录实际输入与两个程序摘要；`dependencies.json` 和 `/usr/share/licenses/nacket` 提供实际依赖组成及许可材料。中英文公开文档随镜像放在 `/usr/share/doc/nacket`。

```sh
docker stop --time 25 nacket
```

先关闭新准入，再在原期限内排空；强制终止可能留下未知结果。日志正文不是业务历史或投递确认。不要从 broker 确认推断客户端或业务已处理。

## 手动发布

`Build` 工作流分别在原生 amd64、arm64 runner 构建两个平台，保留 `image-amd64`、`image-arm64` 归档及每平台 `image-identity-<arch>.json`。归档内的产品版本、源提交和 Cargo 锁文件均被绑定；上传阶段不重新编译。

在仓库配置 Variable `DOCKERHUB_USERNAME=nasaruntime` 和 Secret `DOCKERHUB_TOKEN`，令牌须具备目标镜像推送与仓库概览更新权限。手动运行 `Publish Docker image`，选择 `release`，提供成功的 release `build_run_id` 与两个 `image-identity-<arch>.json` 中的 `configDigest`。

流程要求 master、release、同名版本 Git 标签指向同一源提交，且对应 Build 已成功；随后回读归档身份并上传 `nasaruntime/nacket:1.0.0`。已有版本只允许内容完全相同的继续操作，不允许覆盖不同内容。上传后回读平台清单、各平台配置摘要和 Docker Hub 英文概览，最后更新 `latest`。产品采用 MIT OR Apache-2.0，第三方材料保留其各自许可。
