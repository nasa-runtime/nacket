# Business authorization revocation

[中文](authorization-control.md)

After checking business permissions, a service can submit revocations through a dedicated Redis Stream or Kafka ingress. Push permission does not imply revocation permission, and clients cannot submit these commands. The cluster control authority stores accepted operations, authorization version floors, and completion barriers. Node restarts and route reactivation preserve these records.

## Switch and source permissions

`authorization.control.enabled` is disabled by default. Enabling it requires a Redis or Kafka control authority and an enabled ingress with `purpose: authorization_control` and `delivery: recoverable`. Push, initialization replies, and authorization control have distinct ingress purposes.

```yaml
authorization:
  control:
    enabled: true
    ingressRef: authorization
    producers:
      service-a:
        tenantIds: [tenant-a]
        actions: [revoke_subject, revoke_subscription, remove_group, kick]
```

The preceding section belongs to a route definition. A deployment entry at `nacket.ingresses.authorization` can use:

```yaml
enabled: true
purpose: authorization_control
transport:
  backend: redis_stream
  sourceRef: primary
  stream: socket:{authorization}:commands
identity:
  mode: acl
  producer: service-a
delivery: recoverable
recovery:
  initialize: false
maxMetadataBytes: 16384
maxBodyBytes: 65536
batchSize: 16
dependencyStaleMs: 5000
deadLetter: socket:{authorization}:commands.DLT
```

ACL mode requires broker ACLs that restrict production to the declared service identity. A metadata `producer` field is not proof of identity. Shared resources can use the existing HMAC authentication contract, with each key bound to an approved producer. For a new Redis ingress, first create the Stream and use `recovery.initialize: true` only when both its stable group and owner fence are absent. At execution time the script creates the group at the current tail and creates an owner Hash without a TTL. Retain both and use `initialize: false` subsequently. Initialization does not replay earlier revocations; establish the correct current business authorization baseline before serving. A manually created group alone is not a valid recovery baseline. Preserve the Stream, stable group, owner fence, and same-slot DLT resources together. If an established baseline is lost, do not rebuild it with true to continue the old scope; fence old instances and restore durable facts. Kafka requires a dedicated managed source, `read_committed`, and provisioned offsets for each stable node group. Its transport uses `backend: kafka`, `sourceRef`, `topic`, and `partitionCount`.

Disabling control rejects new commands and preserves previously accepted revocations. An unavailable ingress or invalid purpose prevents an incompletely configured route from admitting traffic.

## Command contract

Kafka sources must set `behavior.max_consume_attempts: 0`. Temporary dependency waits on a recoverable ingress must not exhaust ordinary retry attempts and skip input. Queue limits, backoff, the original command lifetime, and the route execution-progress gate still apply. Explicitly invalid records continue through the invalid-record DLT policy.

Redis uses `contract`, `meta`, and `message`; the outer contract remains `nacket.split-envelope`. Kafka uses `nacket-contract`, `nacket-meta`, and the record body. Place the entire command in metadata and use an empty body. HMAC covers the actual metadata bytes and the empty body's digest.

```json
{
  "contract": "nacket.authorization/1",
  "commandId": "authorization-7",
  "clusterId": "cluster-a",
  "scopeEpoch": "1",
  "routeId": "route-a",
  "routeRevision": "12",
  "definitionDigest": "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
  "producer": "service-a",
  "createdAt": 1792324800000,
  "expiresAt": 1792324860000,
  "action": {
    "kind": "revoke_subscription",
    "tenantId": "tenant-a",
    "uid": "user-a",
    "groupId": "group-a",
    "throughVersion": "17"
  }
}
```

| Field | Meaning |
| --- | --- |
| `contract` | Fixed authorization control contract; a Push envelope is not interchangeable |
| `commandId` | Stable operation ID within the route; retries preserve the ID, target, and original lifetime |
| `clusterId`, `scopeEpoch`, `routeId` | Durable authorization scope, retained across route definition changes |
| `routeRevision`, `definitionDigest` | New commands must match the current authoritative definition |
| `producer` | Must match the identity established by ACL or HMAC and the route's producer permissions |
| `createdAt`, `expiresAt` | Integer Unix milliseconds; the admission window is at most five minutes |
| `action` | One tenant and target, a fixed action, and its independent version domain |

An identical retry returns the existing operation. Reusing an ID with different content is rejected. A retry of an accepted operation may query its result after its original admission window, subject to current source permissions. Business versions use canonical decimal strings and checked `u64` comparison. Command expiry does not undo a committed revocation.

Initial admission requires `createdAt <= authority time < expiresAt`. A producer clock that runs ahead yields `authorization_command_not_yet_valid`; the node retains the source record and retries the same ID and content. The original `expiresAt` remains the deadline. Waiting extends neither this deadline nor the `dependencyStaleMs` progress deadline; excessive clock skew protects the ingress.

| Action | Target fields | Effect |
| --- | --- | --- |
| `revoke_subject` | `tenantId`, `uid`, `throughVersion` | Reject subject grants and writes at or below the subject version floor |
| `revoke_subscription` | Also `groupId` | Reject that user's membership grants at or below the floor; other groups and users retain their permissions |
| `remove_group` | `tenantId`, `groupId`, `throughVersion` | Reject group evidence at or below the group state floor; recreating the same group requires a higher version |
| `kick` | `tenantId`, `uid`, `sessionKey` | Permanently close the exact session; omit `throughVersion`; new sessions remain independent |

`sessionKey` contains `clusterId/scopeEpoch/nodeId/incarnation/routeId/routeRevision/definitionDigest/routeSessionId`. Services obtain it from identity, initialization, or lifecycle context. A client-supplied short session ID is insufficient. Subject, membership, and group state versions are independent; a newer subject version cannot override a revoked group. New evidence must exceed every applicable floor.

The Kafka key is `authorization.` followed by the lowercase SHA-256 hex digest of the canonical JSON array `[clusterId,scopeEpoch,routeId,action.tenantId]`. Hash the complete key bytes with SHA-256, interpret the first four bytes as a big-endian `u32`, and take modulo the fixed `partitionCount`. This places subject, membership, and group commands for a route tenant in the same ordering domain.

## Acceptance and completion

Processing verifies the source and current definition, atomically persists the floor and operation, restores the complete authorization state on each node, removes affected local permissions, and waits for the affected write owners before acknowledging local application. A node that has not applied the latest authorization state cannot acquire or renew its route permit.

The MQ position advances only after durable acceptance. This is separate from global revocation completion. A business flow that requires revocation before subsequent delivery must query the authenticated management endpoint:

```text
GET /management/routes/{routeId}/authorizations/{commandId}
```

The response contains `routeId`, `operation`, and `effect`. `accepted` means old permit responsibilities remain. `completed` means each permit captured at acceptance has an application acknowledgment, exact drain acknowledgment, or valid expiry proof. `acceptedAt/completedAt` are integer Unix milliseconds. `barrier` contains pending permit IDs, not online user identities.

Permission admission and final writers share the same version floors. A write permit keeps the subject, membership, and group versions from its acquisition; a later identity refresh cannot restore that old write. Barriers wait only for captured old writes, not newer writes authorized by newer evidence. Data already received cannot be recalled. Asynchronous revocation has a propagation interval between the business update and gateway receipt.

Wall-clock time alone cannot prove an absent node stopped writing. Expiry completion requires explicitly enabled `expiryCompletion` and its scheduling, clock, and write-budget assumptions. Otherwise an operation remains accepted until exact drain evidence is available.

## Capacity and failures

Kafka asynchronous consumption waits within the original invocation for the durable result of the same handed-off operation. Normal waiting neither triggers a group retry nor acknowledges the offset early. An unknown submission backs off with the original ID, contents, and progress deadline. Shutdown, exhausted capacity, or an expired progress deadline retains unacknowledged input under the existing recovery and protection rules.

Execution progress and MQ connectivity provide separate required dependency evidence. Each pending command uses the ingress `dependencyStaleMs` as a fixed progress deadline starting at its first handoff. Retries cannot extend it. An expired deadline or a saturated submission cache protects every route using that ingress against new admission and delivery. The worker checks progress each second; healthy evidence lasts at most three seconds and never exceeds the original deadline of pending work. A stopped worker cannot retain eligibility indefinitely. Recovery requires completing the original work and confirming execution health again; permits captured before the failure remain invalid.

Each node retains at most 128 submission entries and 2 MiB of logical bytes, with at most eight concurrent authority requests. Completed submission results remain cached for thirty seconds; durable operation identity survives cache eviction. Cancellation, timeout, or unknown submission never confirms the broker input. Definite source, definition, lifetime, and conflicting-ID errors use the ingress's quarantine path.

A cluster control scope retains at most 16384 authorization operations and is also subject to the total control snapshot byte limit. Records are not evicted when users disconnect or ordinary TTLs expire. Capacity exhaustion rejects new operations without discarding previous floors. Resetting business counters requires a new isolated epoch and completion of the old scope's shutdown, not a lower version in an HTTP response.

`nacket_control_events_total` distinguishes `authorization` and `authorization_applied`. Resource values `authorization_commands` and `authorization_bytes` show retained node submission responsibility. The `authorization:<ingressId>` dependency reports execution health separately from the MQ ingress; both contribute to required dependency readiness. Use the operation result to determine global completion; metrics cannot replace authorization or barrier evidence.

Handoff logs correlate received, deferred, persisted, rejected and protected stages through `node_id`, `ingress`, `route_id` and `command_id`, with attempt counts and elapsed milliseconds. Persisted means authority admission; query `effect=completed` for global completion. Repeated waiting logs are rate limited, and command IDs never become metric labels. Redis controller failure logs include the owner term, input cursor, request digest, last entered stage and timeout status. Stages cover lease acquisition, snapshot/checkpoint reads, request reads, commits and verification after commit. Full lease tokens, signing secrets and revocation targets are excluded. DEBUG logs correlate confirmed commits and node projection revisions.

## Proactive authorization refresh

A route may declare this fragment; `endpointRef` names a deployment-approved read-only authorization endpoint.

```yaml
authorization:
  refresh:
    enabled: true
    endpointRef: authority-refresh
    schedule:
      advanceMs: 5000
      jitterMs: 1000
      retryMs: 1000
      timeoutMs: 1500
  cache:
    enabled: false
    positiveTtlMs: 1000
    negativeTtlMs: 200
    maxEntries: 128
    maxBytes: 1048576
    coalesce: true
```

When enabled, Ready sessions enter one bounded queue before their original authorization expiry. Stable per-session jitter spreads the advance window. At most one refresh runs per session; explicit RefreshAuthorization and automatic refresh share the serialization gate. Disabling refresh prevents both automatic requests and explicit extensions. Requests use the fixed endpoint and carry `operation: refresh`, `operationName: authorization.refresh`, trusted principal, authorizationId/version, exact sessionKey and allowed handshake facts. Responses follow existing request correlation, allow/deny, version, expiry and grants contracts.

Success rechecks the exact session, route, revocation lower bounds, still-valid original authority and unchanged principal before UID-partition application. Identities with independent credential deadlines, including JWT and trusted gateway assertions, cannot exceed those deadlines. Remote identity may receive new business authority but cannot change UID or tenant. A strictly sequenced `authorization_refreshed` notice carries integer `expiresAt` and `authorizationVersion`. SDKs update only the current identity deadline, never original messages, receipts or old subscription evidence.

Denial, errors, capacity exhaustion and timeouts retain the original deadline and back off by retryMs. There is no expiry grace period. Expiry removes authority and subscriptions; late responses cannot revive a session. Queue and execution limits are `maxRefreshPending` and `maxRefreshInFlight`, in addition to global and endpoint HTTP budgets. Shutdown cancels queued and active work.

## Optional read-only subscription authorization cache

`authorization.cache.enabled` defaults to false: each subscription request calls the business endpoint. Enabling it caches only subscription read decisions, never msg send permission, one-shot operations or identity authentication. Keys bind the complete session, route definition, tenant, UID, identity authorization ID/version, revocation generation, endpoint, canonical target set and allowed handshake context. Sessions cannot share authority.

Positive expiry is the earliest of positiveTtlMs, original identity expiry and remote grant expiry. Only explicit deny enters the short negativeTtlMs cache. Network failure, cancellation, timeout and invalid responses are never negative-cached. Hits and final application recheck revocation, session and subscription intent. A revocation-generation change invalidates entries; a refreshed identity authorization key cannot hit old entries.

`coalesce: true` combines identical in-flight queries while waiters end independently. One waiter cancelling does not cancel another waiter's shared query. Connection close, the total deadline or shutdown cancels actual work. Entries and retained bytes have both session and node limits. Full capacity uses the original uncached authorization call; it neither evicts active responsibility nor implies permission.

A refresh response must omit `principal` or set it to null. Every non-null principal object is rejected, even if identical to the original. Renewal retains the existing session principal and cannot replace identity.
