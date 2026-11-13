# 连接配额与会话替换

[English](connection-quotas.en.md)

`connections.scope` 决定配额在单节点还是整个控制作用域计数。集群范围必须启用 Redis 或 Kafka 持久控制；本地模式拒绝集群配置。两种范围都按稳定 `routeId` 统计路由数量，按 `routeId + tenantId + uid` 隔离主体，身份来自已验证凭据。

```yaml
connections:
  scope: cluster
  limitEnabled: true
  maxConnections: 10000
  maxPerUid: 8
  sameUserPolicy: allow_multiple
  maxPendingOperations: 8
  operationsPerSecond: 32
```

| 配置 | 行为 |
| --- | --- |
| `scope: node` | 当前节点校验数量；其它节点的同主体连接不参与本节点计数。 |
| `scope: cluster` | 在持久控制提交边界串行分配名额，控制不可用或结果未知时不得进入 Ready。 |
| `limitEnabled: true` | 同时限制路由总连接数与 `allow_multiple` 的每主体数量。 |
| `limitEnabled: false` | 取消路由附加数量限制；节点资源、持久账本容量和排他策略仍有效。 |
| `sameUserPolicy: allow_multiple` | 多设备独立会话，共用所选范围的主体数量上限；订阅不自动共享。 |
| `sameUserPolicy: reject_new` | 只要同作用域主体仍有连接责任，拒绝新申请；不影响不同租户中的同名主体。 |
| `sameUserPolicy: replace_old` | 先关闭同主体旧会话，并等待全部实际写出结束，再开放新会话。已有替换正在等待时，集群拒绝并发替换。 |

`reject_new` 和 `replace_old` 按单主体排他解释，不使用 `maxPerUid` 作为同时存活数。替换在路由数量已满但正在替换自己的旧名额时仍可受理；不同主体不能借用这些名额。

## 集群责任与恢复

每个名额绑定完整 `SessionKey`、可信租户与主体、本次节点代际中的递增申请序号，以及精确路由许可 ID。申请先持久提交，然后进入以下状态：

- `active`：可继续必要初始化，最终 Ready 仍复验身份、路由和授权期限。
- `waiting`：替换已受理，等待捕获的全部旧名额结束，不能初始化或进入 Ready。
- `revoking`：旧会话须关闭，已登记 writer 仍由真实 owner 持有。

断连先关闭权限；只有实际 writer 全部结束，节点才提交归还。释放节点内连接目录或取消异步等待不代表写出完成。归还也推进节点申请序号下界，先到的取消能拒绝迟到申请，未知提交不能重新建立已取消会话。

进程崩溃、新节点代际登记和墙钟到期本身都不会释放旧名额。旧路由许可的精确排空回执，或覆盖完整许可寿命与部署余量的有效失效证据，才能解除其全部会话责任。重启后的节点先恢复控制基线，不用本地计数覆盖持久状态。名额没有独立 TTL，也不随心跳重新创建。

替换完成只说明旧会话不再具备写出资格且已登记写出责任结束，不撤回网络中已经写出的字节，也不替业务端实现设备状态或业务事务接管。

## 容量与观察

一个持久控制作用域最多保留 16,384 个会话名额，完整账本另受 16 MiB 上限约束。每节点集群名额目录同样最多 16,384 条；临时保留旧名额与等待者也消耗容量。节点每轮在维护路由许可后处理至多 32 条名额动作，未知提交沿用原会话和序号。所有等待受接入时的固定认证总期限限制。容量耗尽明确拒绝，不删除仍有效的责任。

`GET /management/routes/{routeId}/status` 的 `clusterConnections` 提供持久 `active/waiting/revoking` 数量；节点范围返回 `null`。该接口沿用管理入口来源、凭据和路由范围检查，不公开用户或会话清单。数量不是用户在线状态，也不能替代准入权威。

`nacket_control_events_total` 的 `reserve_connection`、`release_connection` 维度记录明确提交结果；`nacket_live_resources{resource="connection_reservations"}` 表示本节点仍持有的集群名额责任，包括等待和未知结果。排查积压应结合路由许可、控制可用性和 writer 数量，不通过手工删除名额解除屏障。
