# Trusted gateway assertions

[中文](gateway-assertions.md)

`identity.mode: trusted_gateway` lets a deployment-approved proxy provide a signed, single-use identity to nacket. Ordinary client headers grant no trust. The service verifies the actual peer, signature, issuer, audience, expiry, and endpoint scope, obtains durable consumption confirmation, and then proceeds with connection confirmation and initialization.

## Route configuration

```yaml
identity:
  mode: trusted_gateway
  verifierRef: gateway-a
  assertionHeader: x-nasa-assertion
  trustedPeers:
    - 127.0.0.1
```

`verifierRef` selects an entry in deployment `verifiers`. The verifier uses RS256, configured public keys or managed JWKS, fixed issuer/audience, and explicit principal claim pointers. The proxy must replace an external header of the same name rather than forward a user-supplied assertion. The actual peer must match `trustedPeers`; a permitted address without a valid signature cannot authenticate. This strategy accepts WebSocket access. Native TCP authentication bodies cannot impersonate proxy headers.

Signed JWTs use `typ: at+jwt`, standard integer-second `iat` and `exp`, and the configured uid, tenant, authorization ID, and authorization version claims. Map the authorization ID to an issuer-unique `jti`. Internal absolute times and communication envelopes use integer Unix milliseconds; standard JWT fields retain their protocol units.

The JWT `socketContext` must contain exactly these fields:

```json
{
  "clusterId": "cluster-a",
  "scopeEpoch": "1",
  "routeId": "route-a",
  "listenerRef": "shared",
  "endpoint": "/route-a"
}
```

| Field | Purpose |
| --- | --- |
| `clusterId` | Restricts the receiving deployment cluster |
| `scopeEpoch` | Restricts the durable authority scope; assertions from an earlier scope cannot be reused |
| `routeId` | Restricts the application-configured generic route |
| `listenerRef` | Restricts the actual listener template |
| `endpoint` | Restricts the logical endpoint or Socket.IO namespace |

Namespaces on the same physical connection authenticate separately. An assertion does not grant a wildcard identity to the connection.

## Durable consumption

The consumption key hashes the canonical encoding of issuer, audience, clusterId, scopeEpoch, routeId, and authorization ID. Each record stores an owner digest derived from the full SessionKey and attemptId, plus the original expiry. It stores neither JWTs nor payloads.

Cluster mode consumes assertions through a Redis atomic control commit or Kafka control transaction. Requests must come from the current serving node and match the current available route, preparation record, and permit. When two nodes consume the same assertion concurrently, only one admission attempt obtains ownership. An exact retry with the same expiry is idempotent; another node, session, attempt, or expiry is rejected. A failed connection confirmation does not release a consumed assertion.

Local mode stores an `.assertions` file beside the route ledger and shares the node file lock's lifetime. It writes and synchronizes a candidate, atomically replaces the file, synchronizes its directory, and only then confirms consumption. Once `.incarnation` declares dependency on this baseline, a missing consumption file prevents startup. An uncertain persistence result seals further consumption; restarting requires recovery of the complete committed baseline. Local instances on different volumes do not share consumption state. Multiple replicas must use cluster control.

Each authority scope retains at most 16,384 consumption records awaiting collection. Capacity exhaustion rejects new assertions without evicting live records. The complete cluster snapshot also has a 16 MiB limit. Local consumption files have a 4 MiB limit and at most 16 concurrent persistence responsibilities. Shared control-request capacity and the original authentication deadline also bound consumption. This commit occurs during authentication, not during business-message delivery.

Collection advances a monotonic Unix-millisecond time floor in the same durable commit as the records. Subsequent consumption must expire after that floor. A collected assertion therefore cannot become usable again after a node clock rollback or restart. A clock jump forward may restrict admission until time is consistent again; never lower the durable floor to regain availability.

## Failure and recovery

- Unavailable authority, capacity exhaustion, persistence failure, or the original authentication deadline denies admission. Cluster mode never falls back to local deduplication.
- A timeout after submission may have consumed the assertion. Records are not withdrawn, and unknown outcomes do not make credentials reusable. The proxy should issue a new unique assertion.
- Preserve node volumes and authority state across restarts. Backup recovery must respect control high-water marks and scope replacement; never delete consumption records independently.
- Assertions establish trusted identity only. Connections, subscriptions, and actions still obey route permissions, grant expiry, and business authorization.

`nacket_identity_checks_total{strategy="trusted_gateway",outcome}` records identity outcomes. Cluster consumption also uses `nacket_control_events_total{operation="consume_assertion",outcome}`. Metrics contain no JWT, user, assertion ID, or session ID.
