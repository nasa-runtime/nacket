# Business messages and MQ publishing

[中文](publishing.md)

nacket rejects client-to-client business messages by default. For normal delivery, a business microservice validates permissions and business rules through its REST API, then publishes to a trusted Redis Stream or Kafka topic. Identity, group subscriptions, and permission to send messages are separate. A successful `sub` never grants permission to send to that group.

## Enabling client messages

A route must explicitly enable `clientMessages.enabled` and select one execution mode:

| execution | Responsibility and result |
| --- | --- |
| `business_command` | nacket calls a fixed business POST endpoint. The business service owns durable idempotency, transactions, and MQ publishing. nacket does not publish the same message again. |
| `authorize_then_publish` | nacket calls a fixed authorization POST endpoint, validates its `AuthorizedPublication`, and submits the message through a dedicated publisher. The authorization endpoint must not also publish that message. |

Both modes require a valid identity, enabled service delivery, explicit events/schemas, an allowed target scope, and an `operationId`. Anonymous routes cannot enable client messages. Business failures never fall back to local broadcasting.

`payloadAccess` fixes the HTTP input layout:

| Value | Business payload |
| --- | --- |
| `raw_body`, the default | The HTTP body contains the original route-encoded bytes. Control JSON is Base64URL encoded in the `nacket-context` header; the control JSON is limited to 6 KiB before encoding. |
| `json` | The payload is decoded using the route's sole schema and placed in `arguments` in a regular JSON request. This incurs explicit deserialization and encoding costs. |
| `metadata_only` | Only the payload length and digest are provided. This is available only for `authorize_then_publish`, and the business service must explicitly accept authorization without inspecting content. |

Raw requests use `Content-Type: application/octet-stream`. The control object includes server-established `requestId`, `operationId`, `context`, `principal`, `target`, `event`, `schema`, and `payload.bytes/digest`. Clients cannot select business addresses, service names, broker resources, or trusted identities. Authorization and publishing use the same immutable `Bytes` payload.

## Authorization response

The authorization service returns strict JSON. This example shows an `allow` response; actual identifiers and digests must come from the request and durable business state.

```json
{
  "requestId": "current-server-request",
  "operationId": "durable-client-operation",
  "decision": "allow",
  "publication": {
    "publicationId": "stable-business-message",
    "grantId": "stable-business-grant",
    "expiresAt": 1792324860000,
    "bodyDigest": "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
    "push": {
      "kind": "push",
      "id": "stable-business-message",
      "operationId": "durable-client-operation",
      "context": {
        "clusterId": "cluster-a",
        "scopeEpoch": "1",
        "tenantId": "tenant-a",
        "routeId": "route-a",
        "routeRevision": "4",
        "definitionDigest": "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
      },
      "event": "event-a",
      "schema": "schema-a",
      "target": {
        "kind": "group",
        "groupId": "group-a"
      },
      "sender": {
        "uid": "user-a"
      },
      "delivery": {
        "createdAt": 1792324800000,
        "expiresAt": 1792324860000
      }
    }
  }
}
```

`expiresAt` is the authorization deadline; `push.delivery` is the immutable message lifetime. All timestamps are Unix milliseconds as integers. `bodyDigest` is the SHA-256 digest of the actual payload. `push.id` must equal `publicationId`. The principal, route binding, event, schema, and operation identity must match the request. A group target must remain unchanged; a UID list may only be narrowed. Denials use `decision: "deny"` and omit `publication`.

The authorization service must durably store the request fingerprint and complete authorization by principal, route, and `operationId`. Retries must return the same `publicationId`, the same Push metadata, and the original timestamps. Reusing an operation identity with different content or targets must be rejected as a conflict. Neither the short-lived request cache nor delivery deduplication replaces this business record. A retry after reconnecting to another node may produce another broker record with the same message identity. This is not end-to-end exactly-once delivery.

## Fixed publishers

`nacket.publishers` is a deployment catalog whose changes require a restart. Each publisher limits its purpose, routes, source, destination, and capacity. Redis example fragment:

```yaml
nacket:
  publishers:
    publisher-a:
      enabled: true
      purpose: push
      producer: socket-dispatcher
      routeIds: [route-a]
      transport:
        backend: redis_stream
        sourceRef: primary
        stream: socket:business
      maxPending: 128
      maxBytes: 8388608
      maxMetadataBytes: 16384
      maxBodyBytes: 262144
      timeoutMs: 3000
```

The `publisherRef` for `authorize_then_publish` must select `purpose: push`. Its transport must exactly match the ingress selected by `servicePush.ingressRef`. That ingress must use a dedicated ACL identity whose fixed producer matches the publisher and is allowed by the route. This publishing path rejects shared HMAC ingress configurations. Services with different permissions must not share writable resources. Broker ACLs provide actual producer isolation; configuration names cannot replace them.

Kafka transport uses `backend: kafka`, `sourceRef`, `topic`, and an explicit `partitionCount`. The named Kafka source must provide `producer.lanes.socket-publish` with effective `acks: all` and `enable_idempotence: true`. Startup checks the actual partition count. A fixed scope-and-target ordering key is hashed with SHA-256 to choose the partition. The raw payload becomes the record value; control headers are `nacket-contract` and `nacket-meta`.

Queues limit both record count and logical bytes, with at most 16 concurrent sends per publisher. The payload, metadata, and fixed per-record overhead consume the byte budget. Queueing and broker work share one deadline. Unknown results are not automatically retried. Workers retain payload ownership and capacity while work is outstanding, even if a client stops waiting. Network layers, Redis encoding, the HTTP builder, and native Kafka code may copy data; shared payload references do not imply end-to-end zero-copy operation.

## Business notifications

A `business_signal` operation in `notifications` must select a dedicated `purpose: business_signal` publisher. With `requestSchema`, its payload uses the route's sole codec. The operation requires `operationId`, does not support `responseSchema`, and remains subject to its independent `authorization` policy.

Signals use the `nacket.business/1` contract. Metadata contains `kind: business_signal`, `operationId`, `requestId`, trusted `context/principal`, `event`, `schema`, `bodyPresent`, and integer `createdAt/expiresAt` timestamps. The payload is separate. Signals cannot specify delivery targets. Their destinations cannot overlap delivery, dead-letter, or control resources; deployment ACLs must also prevent source aliases from reaching protected resources.

## Results and observability

| code / phase | effect | Meaning |
| --- | --- | --- |
| `broker_accepted` / `broker_accepted` | `accepted` | An actual broker acknowledgment was received. This does not prove client receipt or business completion. |
| `publish_not_dispatched` / `publish` | `none` | Permission, deadline, capacity, or queue admission failed before publishing. |
| `publish_unknown` / `publish` | `unknown` | Work entered the sender without a definite success result. Preserve the original operation identity and query business state. |

`nacket_mq_publish_total{channel="business",outcome="committed|unknown"}` records publishing outcomes. `nacket_live_resources{resource="publish_items|publish_bytes"}` tracks logical resources held by queued and active work. For Redis, `committed` only means an XADD response was received; disk and replica durability depend on deployment settings. Disconnecting a client or disabling a route cannot undo a message already accepted by the broker. Consumers still validate the original route binding and current permissions before delivery.
