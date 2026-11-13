# Subject event ordering and capacity

nacket uses the napart default Runner managed by napp. The trusted `(tenantId, uid)` is the partition key and every event uses one strict task type. Connections, routes and transports for the same subject share admission order within one node. This does not establish global ordering across nodes. Identity must be verified first; missing, empty or whitespace-only uid values cannot establish a session.

## Consumption and remote work

Authentication handoff, session confirmation, subscription intents, cancellation, authorization results, operation results, Ready, recipient selection, final write permits, receipts and connection termination enter the same ordering domain. Native application heartbeats advance there too. Physical WebSocket and Engine.IO control frames do not grant or extend identity authorization.

Consumption closures perform bounded synchronous state changes. HTTP, MQ and physical writes wait outside the partition. Their results return to the original subject and revalidate the exact session, original route, deadline and subscription intent. A later cancellation can invalidate an earlier subscription approval. Local state ordering does not guarantee the completion order of remote business effects; SDKs must not replay requests whose effects are unknown.

Global revocation, route shutdown and disconnection tighten permission gates immediately. Subsequent cleanup follows subject order. Writers continuously revalidate their original permits; bytes already sent cannot be recalled. Rejected cleanup may only revoke and release resources. An offline fact that was not consumed is counted as rejected and cannot bypass the partition.

## Configuration

`nacket.transport.uidEvents` is frozen at startup and requires a restart to change. The UserHook configures the sole default Runner; do not register it again through `partition` configuration. Routes cannot disable this execution model.

| Setting | Default | Purpose |
| --- | ---: | --- |
| `partitions` | 32 | Fixed original partitions; must be a power of two |
| `queueCapacity` | 256 | Queue capacity for the shared task type in each original partition |
| `maxInflight` | 8192 | Runner-wide in-flight hard limit |
| `timeoutMs` | 1000 | Local admission-to-completion deadline, bounded by the original business deadline |
| `maxPerUid` | 128 | Local events shared by every connection of the same tenant and uid |
| `maxBytesPerUid` | 8388608 | Logical retained bytes for one subject's event closures |
| `maxBytes` | 268435456 | Node-wide logical retained event bytes |
| `controlReserve` | 16 | Reserved completion, cancellation and shutdown entries; no priority reordering |

Ordinary events are limited node-wide to `min(queueCapacity, maxInflight) - controlReserve`, or 240 by default. This preserves control space even when every ordinary key collides in one original partition. Each subject reserves at least one control entry. Every event accounts for at least 64 KiB of control retention. Payload-bearing entries additionally account for shared payload and control bytes. Accounting does not copy shared Bytes and applies alongside physical transport byte budgets. Subject directory entries disappear when their last actual task owner releases them.

These limits cover queued and executing local closures. Remote operations and physical writes retain their separate node, connection, HTTP/MQ and writer limits. Unsubscribe has separate connection and node operation slots that ordinary business requests cannot consume. Cancellation releases capacity with the actual retained payload; already executing work is never reported as rolled back.

## Failure and observation

Capacity, expiry, Runner health and shutdown gates reject promptly. Work never switches to direct execution, another Runner or unbounded queuing. Downstream follows the route's slow-consumer policy. Closing permanently revokes the old session; reconnection creates a new identity generation. Control exhaustion also rejects and tightens permissions; notification delivery is not guaranteed.

nafana shares napp partition health and backlog sources. Fixed product series include `nacket_uid_events_total{class,outcome}`, `nacket_live_resources{resource="uid_events"}` and `resource="uid_event_bytes"`, plus the `uid_event_wait`, `uid_event_execute` and `uid_event_total` duration stages. Oldest pending age uses the `uid_events` resource. Labels never include uid, sessionId or operationId. Monitoring retains its separate port.
