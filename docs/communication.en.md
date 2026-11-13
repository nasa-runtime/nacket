# Communication contracts

[中文](communication.md)

nacket uses `nacket.split-envelope`: bounded UTF-8 JSON control metadata and a separate raw business body encoded with the route's single ProtocolBytes mode. Forwarding does not require converting the business body into an object. Business fields cannot become permission fields.

The image does not accept legacy Java ObjectMessage records or invent missing message IDs, tenants, route revisions or expiry. After business authorization, a legacy producer must convert to a complete Push and persist its ID, creation time, expiry and body in its own outbox before submitting it to trusted MQ. Retries must preserve these fields.

Each entry in `schemas` in the [model catalog](../contracts/communication.json) is a standalone JSON Schema generated from the types used by the service. Resolve `$defs` inside that entry. Schemas describe shape; the service also checks direction, correlation, byte limits, canonical versions, expiry and current authority. Duplicate JSON properties are rejected before deserialization. A schema cannot detect duplicates already discarded by a caller's JSON parser.

## Shared fields and numbers

| Field | Meaning |
| --- | --- |
| kind | Direction constrained by the ingress. Clients can request sub, unsub, notify or msg, never acquire authority by declaring push/control |
| id | Stable message or session request identity; conflicting content under the same ID is rejected |
| operationId | Business service's durable idempotency identity, separate from the session request id |
| correlationId | Original request correlation, without authority |
| context | Direction-specific scope: clients send only routeRevision; downstream exposes routeId/routeRevision/definitionDigest; trusted producers use complete Context |
| event / schema | Declared event and body schema; cannot select another encoding per message |
| target | Exactly one tagged target with only that target's fields |
| sender | Trusted downstream identity only; clients see public uid and cannot impersonate it upstream |
| delivery | Producer's fixed createdAt/expiresAt and exclusions that only narrow delivery; clients see deadlines, messageKey and optional receipt |
| traceId | Bounded trace correlation, never credentials, endpoint selection or a metric label |

All absolute times are Unix milliseconds as integers in `0..9007199254740991`. Date strings, numeric strings, fractional values and out-of-range integers are rejected. Wire generations such as scopeEpoch, routeRevision, incarnation and authorization versions use canonical positive decimal strings. Configuration RouteDefinition.revision and numeric management ledger fields follow their individual schemas; use an integer-preserving parser.

## Client connection and operations

1. Send Authenticate with contract, endpoint, attemptId, token when required and capabilities. Optional expectedBinding narrows directory selection without replacing credentials.
2. Validate Hello's attemptId, sessionId, unique serialization, schemas and definition digest. Hello is not readiness.
3. Echo attemptId, definitionDigest and confirmNonce in Confirm.
4. Wait for Ready after connection authorization, required initialization and current cluster permits. Confirmation, heartbeats and retries never extend original authorization.
5. Use ClientRequest. On disconnect or identity change, discard old session tickets, grants and unexecuted buffered actions. Reconnect with a new attemptId and credentials.

| Operation | Metadata and body | Boundary |
| --- | --- | --- |
| sub / unsub | Matching kind and event, server target, schema from subscriptions.schemaRef; separate body contains the declared group ID field | sub requests reading permission; unsub withdraws permission before draining prior writes; neither changes business membership |
| notify | kind=notify, allowlisted event, server target, body only as declared by the operation | Executes configured actions without obtaining a general downstream sender |
| msg | kind=msg, stable operationId, body schema, group or uids target | Disabled by default; enabled requests still pass through business REST execution or authorization and approved MQ publication |
| Receipt | token, messageKey, level, accepted | Confirms only the corresponding written ticket on this session; sdk_received and application_processed are distinct |
| Heartbeat | Echo the current nonce | Connection liveness only, never authorization or message TTL renewal |

Downstream.result contains code/phase/effect/retryable/retryAfterMs. Effect none provides no execution fact, accepted means accepted, completed means completion at the declared layer, and unknown means the side effect is unresolved. Do not change operationId to retry unknown effects. A local timeout or cancellation does not establish remote rollback. Broker acceptance, complete socket write, SDK reception and application processing are separate states.

SessionStateNotice has its own bounded control output and binds attemptId/sessionId/context/stateSeq. Permission expires before notification, and disconnect can prevent notification delivery. See [session state](session-state.en.md).

Every SDK and direct client must pass server-side uid admission: the verified identity must contain a nonempty, non-whitespace-only string of at most 128 UTF-8 bytes without control characters. Missing, null and non-string values are rejected. Validation precedes Hello and is repeated at session construction; failure cannot reach Ready, register an online session or restore subscriptions. Transport connection or HTTP Upgrade alone is not authenticated success. Socket.IO namespaces are checked independently; disabling additional connection authorization does not disable this rule. See the [identity HTTP contract](http-api.en.md).

WebSocket HTTP Upgrade returns 503 while the node lacks local readiness or is closing, before the client enters AUTH. After upgrade, authentication, definition and permissions are independently rechecked; HTTP 101 does not imply Ready. Recovery of an established session may retry transport setup within its original attempt/time budget; already-sent authentication or business operations with unknown outcomes must not be automatically replayed.

## Native TCP / WebSocket frames

TCP reads consecutive frames. Each native WebSocket binary message contains exactly one complete frame; text messages are not accepted as replacements. The HTTP upgrade path is transport.nativePath; Authenticate selects the logical endpoint.

| Offset | Bytes | Content |
| --- | --- | --- |
| 0 | 4 | Big-endian u32 length of the following content, excluding this field |
| 4 | 1 | type: 1 AUTH, 2 AUTH_RESPONSE, 3 EVENT, 4 EVENT_ACK, 5 PING, 6 PONG, 7 BROADCAST, 8 CLOSE, 9 CONFIRM, 10 READY, 11 RECEIPT, 12 STATE |
| 5 | 1 | ProtocolBytes ordinal when body is present, otherwise 255 |
| 6 | 1 | bodyPresent: 0 absent or 1 present; other values rejected |
| 7 | 4 | Big-endian u32 byte length of nonempty UTF-8 JSON metadata |
| 11 | metadataLength | Control JSON |
| 11 + metadataLength | Remaining | Optional raw business body; empty differs from absent |

A frame is at least 11 bytes and metadata must be nonempty. Negotiated and deployment capacities apply to all regions. Ordinals are 0 JSON_BYTES, 1 VARINT_TLV, 2 BITPACK_TLV, 3 FAST_FIXED; the route permits exactly one. Control frames have no body and mode=255. Body presence and schema must match for events. The transport envelope is not another business serialization layer; the body uses shared ProtocolBytes schemas.

Socket.IO uses the same models and encoding with one standard binary attachment. nacket:sub/unsub/notify/msg carry ClientRequest; handshake uses nacket:hello/confirm/ready, push uses nacket:push and delivery receipts use nacket:receipt. Operation ACKs and delivery receipts are distinct. Namespaces do not share authority. See [Socket.IO ingress](socketio-compatibility.en.md).

The short event names `sub`, `unsub` and `msg` are accepted as aliases of the same operations, with identical kind, target, schema, route-policy and authorization checks. SDKs use the `nacket:` names. Converting `join/leave` still requires the explicit compatibility switch.

## Trusted broker input

The business microservice first checks identity and target permission through REST, then encodes the body and Push. Browsers must not hold broker or signing credentials. The model catalog documents all Push fields. Initialization replies and revocation commands require separate ingress purposes and producer permissions.

| Content | Redis Stream field | Kafka |
| --- | --- | --- |
| Fixed contract | contract | Single nacket-contract header |
| Original metadata bytes | meta | Single nacket-meta header |
| Raw body | message | Record payload; never null, empty bytes allowed for dedicated control records |
| HMAC key selector | keyId | Single nacket-key-id header |
| HMAC authentication code | signature | Single nacket-signature header |

ACL mode fixes the producer for an ingress and rejects signing fields. Isolate write access with actual broker ACLs; a self-reported service name on a shared writable stream is not authentication. HMAC mode requires both selector and signature. Duplicate, unknown, null or oversized fields are rejected. Kafka additionally permits the framework's fixed X-Nasa-Event=DEFAULT and X-Nasa-Payload-Mode=raw headers.

HMAC input starts with ASCII `nacket.ingress.signature` followed by a zero byte. Append contract, ingress ID, trusted producer, key ID, original metadata bytes and the raw 32-byte SHA-256 of the body, each prefixed with an eight-byte big-endian length. Hash the entire sequence with SHA-256, express it as lowercase hexadecimal, then compute HMAC-SHA256 over that UTF-8 text using the secret. Output 64 lowercase hex characters. Sign actual metadata bytes; reordering JSON requires signing again.

Kafka Push keys start with ASCII `nacket.ordering` and a zero byte. Append clusterId, scopeEpoch, tenantId, routeId, target.kind and target identities, each with a four-byte big-endian UTF-8 byte length. Group adds groupId; uids adds IDs sorted by UTF-8 byte order; tenant_route_broadcast adds no identity. Interpret the first four SHA-256 bytes of the key as big-endian u32 and take modulo fixed partitionCount. Revision is excluded so route updates do not move a target's ordering partition. Partition-count changes require explicit deployment preparation.

Broker acknowledgement means input commitment only. Retry with unchanged id/operationId/createdAt/expiresAt and content. Conflicting bodies or targets under one message key are rejected. A crash boundary can deliver a valid message again; consumers use messageKey for idempotency. This service does not promise disconnected history replay or end-to-end exactly-once delivery.

See [publication authorization](publishing.en.md), [initialization and lifecycle](lifecycle.en.md), [revocation](authorization-control.en.md) and [Kafka recovery](kafka-ingress.en.md).

## Operation rejection and session closure

For native EVENT frames that parse correctly but violate the allowed business direction or target, the service returns `operation_rejected` with `effect=none` and keeps the valid connection. Unknown fields, malformed frames, invalid authentication/confirmation, and lost session or UID partition authority close the connection. Socket.IO may close a namespace for an invalid event direction. SDKs do not replay rejected operations or operations with unknown effects. An explicit pre-authentication `route_unavailable` permits bounded recovery; unexplained closure after AUTH remains terminal.

## Request identity, replay and rejection

A finite session request directory binds each request ID to its request identity. Within retention, an identical
pending retry shares its execution responsibility and a completed retry receives the retained result. Reusing
an ID for a different identity returns `request_identity_conflict` without a new side effect. Record and byte
limits are separate from operation rate limits. This directory is not durable idempotency across reconnects;
applications still need stable operation IDs and external idempotency contracts.

For structurally valid Socket.IO requests, identity conflicts and request-directory capacity refusals during
action preparation return a correlated ACK with `effect=none`; the valid namespace can use a fresh request ID.
These IDs belong to business envelopes. Socket.IO ACK numbers must remain unique while in flight; a duplicate
in-flight ACK, request-rate excess or invalid event direction closes the affected namespace under the protocol.
Loss of subject authority, inability to complete the
execution result safely, or failed ACK enqueue/write terminates the affected session. A partial physical write
still closes the transport to prevent attachment mismatches. Other valid namespaces have independent permissions.
