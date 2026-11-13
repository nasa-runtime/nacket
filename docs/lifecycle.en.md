# Connection lifecycle

[中文](lifecycle.md)

Lifecycle state belongs to an exact `SessionKey`. Devices, reconnects, and routes create separate connection records. Events grant no subscription permissions and cannot forward messages to other clients.

## Initialization and connection facts

`lifecycle.initialization` runs after authentication and route confirmation, before Ready. HTTP or MQ initialization requires an explicitly allowed, correlated response. When `required` is enabled, a timeout, denial, or invalid grant closes the connection. Initialization may provide a bounded authorization snapshot but cannot replace the authenticated principal. Fact callbacks cannot supply missing permissions later.

`lifecycle.online` records one fact after Ready response ordering is established. `lifecycle.offline` records one fact after local permissions, pending operations, and the session directory entry are revoked. A confirmed session that fails initialization may produce an offline event with `wasReady: false`. Both switches are independent; disabling either one never disables local cleanup.

Online and offline callbacks support HTTP and MQ and do not accept `required`. They describe completed state transitions and cannot prevent disconnection or authorize a session. Initialization supports `required`; MQ initialization requests and grant replies use the separate contract below.

## Configuration

Place the following sections in the deployment and a route definition respectively. Referenced endpoints and publishers must exist in the deployment catalog; the service creates no built-in routes.

```yaml
nacket:
  lifecycleDelivery:
    workers: 16
    maxPending: 1024
    maxBytes: 8388608
    maxEventBytes: 16384
    maxAttempts: 1
    retryBackoffMs: 100
```

```yaml
lifecycle:
  initialization:
    enabled: false
    required: true
    timeoutMs: 5000
  online:
    enabled: true
    timeoutMs: 5000
    target:
      mode: http
      endpointRef: lifecycle-endpoint
  offline:
    enabled: true
    timeoutMs: 5000
    target:
      mode: mq
      publisherRef: lifecycle-publisher
```

HTTP endpoints must use POST. Deployment endpoint settings determine service authentication and direct addressing or service discovery. MQ publishers require `purpose: lifecycle` and a `routeIds` allowlist containing this route. Lifecycle destinations are separated from client delivery, quarantine, and control resources. See [publishing](publishing.en.md) for Redis Stream and Kafka settings.

`workers` selects 1–64 fixed ordered lanes; facts for one session share a lane. `maxPending` covers queued, active, and backing-off work and ranges from workers to 65536. `maxBytes` limits node fact resources to at most 64 MiB and must accommodate twice `maxEventBytes` plus 4096 bytes. `maxEventBytes` ranges from 1024 to 65536. `maxAttempts` ranges from 1 to 5 and defaults to one attempt; `retryBackoffMs` ranges from 10 to 5000 milliseconds. Full queues record rejection without unbounded waiting or retaining session permissions.

Node budget changes require restart. Route callback settings apply with a new route revision. `timeoutMs` covers the entire event lifetime, including queueing, network calls, and retries. Attempts never renew the TTL.

## Event contract

HTTP sends the following JSON body. MQ uses `nacket.business/1`, the same JSON metadata, and an empty message byte field. No business body needs deserialization.

```json
{
  "contract": "nacket.business/1",
  "kind": "lifecycle",
  "eventId": "event-id-a",
  "requestId": "event-id-a",
  "operation": "offline",
  "sequence": 2,
  "context": {
    "clusterId": "cluster-a",
    "scopeEpoch": "1",
    "nodeId": "node-a",
    "incarnation": "1",
    "routeId": "route-a",
    "routeRevision": "1",
    "definitionDigest": "sha256:0000000000000000000000000000000000000000000000000000000000000000",
    "routeSessionId": "session-a",
    "attemptId": "attempt-a"
  },
  "principal": {
    "uid": "subject-a",
    "tenantId": "tenant-a"
  },
  "wasReady": true,
  "createdAt": 1792324800000,
  "expiresAt": 1792324805000,
  "bodyPresent": false
}
```

| Property | Meaning |
| --- | --- |
| contract / kind | Fixed contract and lifecycle category; this is not a delivery push |
| eventId | Server-generated idempotency key, unchanged across retries |
| requestId | Equal to eventId, available to fixed remote parameter mappings |
| operation | Server-selected online or offline |
| sequence | 1 for online, 2 for offline; comparable only within the exact session |
| context.clusterId / scopeEpoch | Deployment scope and durable control epoch |
| context.nodeId / incarnation | Origin node and its running incarnation |
| context.routeId / routeRevision / definitionDigest | Frozen route identity and definition at event creation, unaffected by reloads |
| context.routeSessionId | Unique route connection identity; reconnects get a new value |
| context.attemptId | Connection attempt correlation, not an authentication credential |
| principal.uid / tenantId | Subject and tenant from verified identity |
| wasReady | Whether this connection reached Ready, not whether the entire subject is online |
| createdAt / expiresAt | Integer Unix milliseconds fixed at creation and preserved across retries |
| bodyPresent | Always false; there is no separate business body |

A successful HTTP response must be `{"eventId":"event-id-a","status":"accepted"}` or use `duplicate` with the same eventId. HTTP 200 alone, incorrect correlation, invalid JSON, or additional fields do not confirm acceptance. Responses cannot carry grants or delivery actions. Services must handle eventId idempotently: when retries are enabled, a committed request whose response was lost can arrive again.

The node initiates callbacks in session order. Unknown commit results can still lead to late network or business completion. Consumers should retain the highest sequence for the full SessionKey and reject older state, rather than overwrite every connection by uid. MQ confirmation means broker acceptance, not completed business processing.

## Shutdown and observation

Normal shutdown closes session admission, revokes permissions, and records offline facts before closing fact admission. Accepted work drains within its original event deadline and the application's overall shutdown deadline. Lifecycle MQ workers stop after fact draining; napp releases the underlying HTTP/MQ resources. Deadline exhaustion, crashes, and capacity rejection can lose notifications. Services need session leases or reconciliation for final state; this queue is not a durable outbox.

`nacket_lifecycle_events_total{event,outcome}` distinguishes admission, rejection, confirmation, expiry, unknown results, and cancellation. `nacket_live_resources{resource="lifecycle_items"}` and `lifecycle_bytes` track outstanding responsibility. `nacket_duration_seconds{stage="lifecycle_delivery"}` measures sending and backoff time. Labels come from finite catalogs and contain no subject, session, or event IDs.

## MQ initialization grants

Routes can initialize over MQ. Requests use a lifecycle publisher; replies use a separate ingress with `purpose: initialization_reply`. That ingress cannot be a `servicePush.ingressRef`, and push ingresses do not interpret initialization grants.

```yaml
lifecycle:
  initialization:
    enabled: true
    required: true
    timeoutMs: 5000
    target:
      mode: mq
      publisherRef: initialization-publisher
    reply:
      ingressRef: initialization-replies
      allowedProducers: [service-a]
      maxGrantTtlMs: 60000
```

```yaml
nacket:
  lifecycleDelivery:
    maxPendingInitializations: 128
    maxInitializationBytes: 8388608
  ingresses:
    initialization-replies:
      enabled: true
      purpose: initialization_reply
      transport:
        backend: redis_stream
        sourceRef: primary
        stream: socket:initialization-replies
      identity:
        mode: acl
        producer: service-a
      maxMetadataBytes: 16384
      maxBodyBytes: 1
      deadLetter: socket:initialization-replies:dead
```

Kafka follows the same purpose and source rules, using `backend: kafka`, fixed `sourceRef`, `topic`, and `partitionCount`, plus a separate DLT. Both brokers require an empty reply body. Producer identity comes from fixed ACL configuration or verified HMAC, not a metadata claim.

The service reserves pending count and complete reply capacity, installs the request ID, and only then publishes the request. Fast replies therefore cannot precede waiter registration. `maxPendingInitializations` ranges from 1 to 65536, and `maxInitializationBytes` from 4096 to 67108864. Each waiter reserves a logical `2 × maxMetadataBytes + 4096` bytes, separately from connection fact queues. A route remains protected until its reply consumer has fresh baseline evidence.

Requests use the same `context` and `principal` described above, with `kind: initialization_request`, a server-generated `requestId`, fixed `operation: initialize`, integer `createdAt/expiresAt`, and `bodyPresent: false`. The business service preserves correlation and publishes to the preconfigured reply ingress:

```json
{
  "kind": "initialization_reply",
  "requestId": "request-a",
  "context": {
    "clusterId": "cluster-a",
    "scopeEpoch": "1",
    "nodeId": "node-a",
    "incarnation": "1",
    "routeId": "route-a",
    "routeRevision": "1",
    "definitionDigest": "sha256:0000000000000000000000000000000000000000000000000000000000000000",
    "routeSessionId": "session-a"
  },
  "principal": {
    "uid": "subject-a",
    "tenantId": "tenant-a"
  },
  "response": {
    "requestId": "request-a",
    "decision": "allow",
    "authorizationId": "grant-a",
    "authorizationVersion": "1",
    "expiresAt": 1792324860000,
    "grants": {
      "canReceiveDirect": true,
      "allowedOperations": [],
      "groups": []
    }
  }
}
```

| Reply property | Meaning |
| --- | --- |
| kind | Fixed initialization reply category, never a push or management command |
| requestId | Original request correlation |
| context | Full SessionKey with the meanings above; replies omit attemptId and cannot overwrite another session, definition, or incarnation |
| principal | Must exactly match the waiter's authenticated uid and tenantId |
| response.requestId | Must also match the outer requestId |
| response.decision | allow or deny; denial grants no permissions |
| response.authorizationId / authorizationVersion | Authorization identifier and canonical decimal monotonic version, required for allow |
| response.expiresAt | Integer Unix milliseconds bounded by maxGrantTtlMs and the current identity deadline |
| response.grants.canReceiveDirect | Authorization ceiling for direct receiving by this session |
| response.grants.allowedOperations | Allowed notification names; cannot create operations absent from the route |
| response.grants.groups | Bounded group read evidence, still subject to subscription intent and group/authorization versions |

Replies use the outer broker contract `nacket.split-envelope`, Redis fields `contract/meta/message`, or Kafka headers `nacket-contract/nacket-meta`, with an empty message/payload. The Kafka ordering key starts with ASCII `nacket.initialization` and a zero byte, then clusterId, scopeEpoch, nodeId, incarnation, routeId, routeRevision, definitionDigest, routeSessionId, and requestId in that order. Prefix each UTF-8 field with its four-byte big-endian byte length. The partition is the first four SHA-256 digest bytes interpreted as an unsigned big-endian integer, modulo partitionCount.

A reply reaches grant validation only while the exact waiter remains live and not Ready, with an allowed source and fully matching correlation. Incorrect principals or correlation cannot consume the correct reply's completion slot. Duplicate, expired, disconnected, or old-incarnation replies cannot affect a new connection. After broker acceptance or an unknown publish result, the service waits only for the original request's valid reply, without creating a replacement request. Deadlines never renew. Required initialization failure closes the session; optional initialization failure cannot enable missing permissions.

If the UID execution domain has already closed, local cleanup logs `cleanup_path=local_fallback` and records rejection of the offline fact. A network thread must not bypass partition ordering to publish it. This cleanup does not claim remote delivery.
