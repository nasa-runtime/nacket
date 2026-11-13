# Connection quotas and session replacement

[中文](connection-quotas.md)

`connections.scope` selects node or cluster counting. Cluster scope requires Redis or Kafka durable control; local mode rejects it. Both scopes count route totals by stable `routeId` and isolate principals by `routeId + tenantId + uid`. Identity comes from verified credentials.

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

| Setting | Behavior |
| --- | --- |
| `scope: node` | Counts only this node. Connections on other nodes do not participate. |
| `scope: cluster` | Allocates reservations at a durable control commit boundary. Unavailable or uncertain control cannot produce Ready. |
| `limitEnabled: true` | Enforces route capacity and the per-principal limit for `allow_multiple`. |
| `limitEnabled: false` | Disables additional route quantity limits. Node resources, ledger capacity, and exclusive policies remain enforced. |
| `sameUserPolicy: allow_multiple` | Devices have independent sessions and subscriptions, sharing the selected principal quota. |
| `sameUserPolicy: reject_new` | Rejects admission while the same scoped principal still owns a connection responsibility. Equal user names in different tenants remain isolated. |
| `sameUserPolicy: replace_old` | Closes previous sessions and waits for all actual writes before admitting the replacement. Cluster scope rejects concurrent replacement while an existing replacement is waiting. |

`reject_new` and `replace_old` enforce one principal session independently of `maxPerUid`. A route at capacity can accept a replacement of that principal's own reservation; other principals cannot borrow those slots.

## Ownership and recovery

Each reservation binds the full `SessionKey`, verified tenant and principal, a monotonically increasing admission ordinal within the node incarnation, and an exact route permit ID. Durable admission produces one of these states:

- `active`: required initialization may proceed. Ready still checks identity, route, and authorization deadlines.
- `waiting`: replacement was accepted but captured previous reservations remain. Initialization and Ready are blocked.
- `revoking`: the previous session must close while its actual owners retain outstanding write responsibilities.

Disconnect revokes permissions first. A node submits release only after all actual writers finish. Removing a connection from a local directory or canceling an asynchronous wait does not prove completion. Release also advances the node's admission watermark, fencing late reservation requests and preventing uncertain commits from reopening canceled sessions.

A crash, new node incarnation, or wall-clock expiry does not release previous reservations. Only an exact route-permit drain acknowledgment or valid expiry evidence covering the full permit lifetime and deployment margin releases its session responsibilities. Restart restores the control baseline instead of replacing it with local counts. Reservations have no independent TTL and are not recreated by heartbeats.

Replacement completion means that previous sessions have no write permission and outstanding write ownership has ended. It does not retract bytes already written to the network or implement application device-state or transaction takeover.

## Capacity and observation

A durable control scope holds at most 16,384 reservations and has a separate 16 MiB complete-ledger limit. Each node's cluster reservation directory also holds at most 16,384 entries. Retained previous reservations and waiting replacements consume capacity. Each node processes at most 32 reservation actions per coordination cycle after maintaining route permits. Uncertain retries retain the original session and ordinal. The original overall authentication deadline bounds admission waits. Capacity exhaustion rejects admission without deleting live responsibilities.

`GET /management/routes/{routeId}/status` exposes durable `active/waiting/revoking` counts in `clusterConnections`; node scope returns `null`. Existing management peer, credential, and route-scope checks apply. It exposes no principal or session list. Counts are neither user presence nor admission authority.

The `reserve_connection` and `release_connection` subjects of `nacket_control_events_total` report durable control outcomes. `nacket_live_resources{resource="connection_reservations"}` measures cluster reservation responsibilities still owned locally, including waiting and uncertain work. Diagnose accumulation together with route permits, control availability, and writer counts; manually deleting reservations is not a supported barrier-completion mechanism.
