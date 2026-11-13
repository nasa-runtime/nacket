# 受信网关断言

[English](gateway-assertions.en.md)

`identity.mode: trusted_gateway` 用于部署批准的代理向 nacket 提供一次性签名身份。客户端不能通过普通请求头取得信任。服务先验证实际对端、签名、签发者、受众、有效期和入口作用域，再取得持久消费确认，最后进行连接确认和初始化。

## 路由配置

```yaml
identity:
  mode: trusted_gateway
  verifierRef: gateway-a
  assertionHeader: x-nasa-assertion
  trustedPeers:
    - 127.0.0.1
```

`verifierRef` 引用部署中的 `verifiers`。验证策略使用 RS256、公钥或受管 JWKS、固定 issuer/audience 和明确的主体字段指针。代理必须覆盖外部同名头，不能透传用户自填的断言。实际对端须匹配 `trustedPeers`；只有来源地址而没有合法签名仍不能认证。此策略用于 WebSocket 接入，原生 TCP 不能通过认证正文冒充代理头。

签名 JWT 使用 `typ: at+jwt`，包含标准整数秒 `iat`、`exp`，以及验证器配置的 uid、tenant、authorization ID 和授权版本。authorization ID 应映射到签发者保证唯一的 `jti`。服务内部和通信信封中的绝对时间均为 Unix 毫秒整数；JWT 标准字段保留其协议单位。

JWT 的 `socketContext` 必须恰好包含以下字段：

```json
{
  "clusterId": "cluster-a",
  "scopeEpoch": "1",
  "routeId": "route-a",
  "listenerRef": "shared",
  "endpoint": "/route-a"
}
```

| 字段 | 作用 |
| --- | --- |
| `clusterId` | 限定接收断言的部署集群 |
| `scopeEpoch` | 限定持久控制作用域，恢复至新作用域后旧断言不可复用 |
| `routeId` | 限定业务方配置的通用路由 |
| `listenerRef` | 限定实际接入监听模板 |
| `endpoint` | 限定当前逻辑入口或 Socket.IO namespace |

同一物理连接的 namespace 分别认证，断言不能扩大成整个连接的通配身份。

## 持久消费

消费键为 issuer、audience、clusterId、scopeEpoch、routeId、authorization ID 的规范编码摘要；记录中保存精确 SessionKey 与 attemptId 的拥有者摘要、原始到期时间，不保存 JWT 或正文。

集群模式将消费纳入 Redis 原子控制提交或 Kafka 控制事务。请求必须来自当前服务节点，并匹配当前可服务路由、准备记录与许可。两个节点同时使用同一断言时仅一个接入尝试能取得消费权。同一精确尝试且到期时间一致可幂等确认，换节点、换会话、换尝试或修改到期时间均拒绝。消费发生后即使连接确认失败，也不释放该断言。

本地模式使用路由账本旁的 `.assertions` 文件，并与节点文件锁共享实际生命周期。先写候选、同步文件、原子替换、同步目录，再确认消费。`.incarnation` 记录依赖该基线后，缺失消费文件会阻止启动。持久结果不确定时封闭后续消费，重新启动必须恢复完整正式基线。不同数据卷的本地模式实例不构成共享消费域；多副本必须使用集群控制。

每个作用域最多保留 16,384 条尚未清理的消费记录。容量耗尽拒绝新断言，不淘汰仍有效的记录。集群完整控制快照还受 16 MiB 上限约束，本地消费文件最多 4 MiB，本地同时持有落盘责任最多 16 项。集群控制通道的共享请求容量和接入原始截止时间同时约束消费。此流程在认证时使用控制提交，不在业务消息投递时访问消费目录。

清理记录时与记录在同一提交中推进单调 Unix 毫秒时间下界。所有后续消费必须晚于该下界，因此已经清理的旧断言不能借节点时钟回退或重启重新使用。时钟超前可能收紧准入，需要恢复时间一致性；不得回退持久下界以恢复可用性。

## 失败与恢复

- 控制权威不可用、容量不足、持久失败或原始认证截止时间到达时拒绝接入，集群模式不回退到本地去重。
- 请求发出后超时可能已经消费成功；不撤回记录，不把未知结果当作可复用凭据。代理应重新签发新的唯一断言。
- 重启保留数据卷和控制基线。备份恢复必须遵循控制高水位和作用域更换合同，不得单独删除消费记录。
- 断言只证明受信身份。连接、订阅和动作仍遵守路由权限、授权期限及业务权限检查。

`nacket_identity_checks_total{strategy="trusted_gateway",outcome}` 统计身份结局；集群消费另外计入 `nacket_control_events_total{operation="consume_assertion",outcome}`。指标不包含 JWT、用户、断言 ID 或会话 ID。
