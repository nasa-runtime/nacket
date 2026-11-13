# Session state notices

[简体中文](session-state.md)

Native connections receive a body-free `STATE` frame (`0x0C`). Socket.IO receives `nacket:state` in the affected namespace. Notices describe current or approaching permission changes. They grant no authority, cannot change the route codec, and contain no business payload.

```json
{
  "context": {
    "routeId": "route-a",
    "routeRevision": "1",
    "definitionDigest": "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
  },
  "attemptId": "attempt-a",
  "sessionId": "session-a",
  "stateSeq": "1",
  "reason": "subscription_revoked",
  "issuedAt": 1790000000000,
  "groupId": "group-a",
  "authorizationVersion": "3",
  "groupVersion": "4"
}
```

| Field | Meaning |
| --- | --- |
| `context` | Public route ID, definition revision and digest only |
| `attemptId`, `sessionId` | Current handshake and exact session; reconnecting never inherits old notices |
| `stateSeq` | Canonical decimal string starting at `"1"`, incremented for each retained notice without cross-language precision loss |
| `reason` | One of the reasons below |
| `issuedAt` | Server-generated integer Unix milliseconds |
| `groupId` | Present only for a subscription change; at most one previously authorized subscription per notice |
| `authorizationVersion` | Affected old subject or membership grant version as a decimal string, not reusable authorization evidence |
| `groupVersion` | Group-state version in the old subscription grant, separate from membership authorization |
| `expiresAt` | Expiry warning or identity renewal, in integer Unix milliseconds; the server enforces the effective monotonic deadline |

| Reason | Server fact and client behavior |
| --- | --- |
| `subscription_revoked` | Membership authority ended; remove confirmed and desired subscription state |
| `subscription_expired` | Subscription authority expired; subscribing again requires a new grant |
| `group_removed` | The known group-state floor invalidated the old subscription; remove it |
| `authorization_expiring` | At most five seconds remain on the current identity grant; one warning per deadline; the notice itself does not renew authorization |
| `authorization_expired` | Identity expired and session permission is closed |
| `authorization_revoked` | Identity was revoked and session permission is closed |
| `session_revoked` | Exact-session eviction or an unavailable permission domain; old authority cannot be transferred |
| `route_unavailable` | A finite control permit expired. The old session and subscriptions are revoked; the SDK may reauthenticate and resubscribe within its configured recovery budget. |
| `route_draining` | Explicit route closure or definition invalidation terminates the old session and stops automatic recovery. |
| `session_closed` | The session closed; clear its business state |

Revocation facts and subscription removal take effect before notification. Existing writes still check session, route, frozen grant versions and original deadlines. Enqueuing or sending a notice does not complete the old-write barrier. Socket.IO Ready and state notices share an independent control FIFO; Ready rechecks session permission at write time. Normal namespace closure preserves other valid namespaces. A partial-write failure closes the physical connection to prevent attachment mismatch.

Clients must check the attempt, session, binding and strictly contiguous `stateSeq`. Ignore notices from old connections. A sequence gap or binding mismatch on the current connection requires a fresh session baseline; never infer subscription authority. Notices have no ACK or replay guarantee. Network loss, process exit or exhausted control capacity may produce only a disconnect, which must clear all confirmed state. Missing notices never imply continued permission.

Each session retains at most 32 notices, reserving 16 KiB per notice for the bounded object and temporary control encoding. `nacket.transport.maxControlOutboundBytes` defaults to 8 MiB and accepts 64 KiB through 64 MiB across all business ports. Business payloads cannot consume this pool. Socket.IO has a separate physical control queue of at most 256 entries. Moving a state notice preserves its original reservation. Native writers preserve notice order and attempt a normal closing flush for at most one second. No further frame is sent after a partial-write failure.

Exhausted notice counts or control bytes close the session or physical connection. The service never drops a sequence number and then continues normally. `nacket_live_resources{resource="control_output_items"}` and `control_output_bytes` follow actual owners. Completed handshake, heartbeat and state writes contribute to transport counters; automatic WebSocket flushes and incomplete frames do not count as complete application messages. `nacket_duration_seconds{stage="heartbeat_rtt"}` uses monotonic time and includes local queueing, network round trip and terminal processing rather than measuring network latency alone.

## Authorization renewal

`authorization_refreshed` carries the current exact session binding, next consecutive `stateSeq`, new `authorizationVersion`, `issuedAt` and `expiresAt`, without `groupId` or `groupVersion`. The client accepts it only while its previous identity deadline remains valid. It updates the connection deadline; existing subscription grants, message tickets and operation deadlines retain their original limits. It does not authorize replay.

## Server and client observation boundaries

An authorization command's `completed` result proves that its target nodes applied revocation and completed their server-side write barriers. It does not prove that a client reader received or processed STATE. Management queries and client connections are independent channels; without a client acknowledgment they do not establish an arrival order. STATE has no ACK. Its `stateSeq` orders session facts, not remotely recalled business frames. An ordinary notify ACK is not confirmation of revocation receipt.

Before exposing a valid state fact, SDKs invalidate the affected session or subscription. Old payloads still queued inside the SDK must then be rejected at delivery. There is no remote recall guarantee for previously transmitted data before local invalidation is observed, or for payloads already handed to application code. An application coordinating sensitive processing with a permission change must wait for the matching fact on its current session, or stop consuming and obtain fresh authorization. A successful management query alone does not satisfy that condition.
