# Business HTTP and management API

[中文](http-api.md)

Approved business endpoints provide identity, authorization and business execution. The separate nacket management port manages routes and control state. They use separate credentials and authority. There is no generic client-message push management endpoint.

## Business callbacks

An endpoint fixes a direct URL or discovery connection/service/path, GET/POST, deadline, concurrency, response size, service credential environment reference, parameter mappings and response mappings. Clients cannot select addresses, methods or service credentials. The shared managed HTTP client selects Nacos service instances.

Ordinary POST uses application/json. raw_body operations send application/octet-stream with original bytes; the standard context is JSON encoded and Base64URL encoded in nacket-context, with a maximum context JSON size of 6144 bytes. GET allows only credential-free connect/subscribe/status/authorize: string query values remain strings and other values are JSON text. Tokens never enter URLs. A side effect handed to HTTP is not automatically replayed on another instance after response loss.

| Request field | Source and meaning |
| --- | --- |
| requestId | Server-generated remote call identity, required in the response |
| operation | Fixed responsibility such as authenticate/connect/initialize/subscribe/authorize/refresh/notify/message/authorize_publication |
| operationName | Route allowlisted notification action or event, not a dynamic URL |
| operationId | Stable business side-effect identity; the business service persists idempotency |
| context | Complete SessionKey plus attemptId, established by the server |
| credentials.token | Authentication material only, never an already verified uid |
| handshake.peer / origin | Actual transport facts; Origin is not credentials |
| principal.uid / tenantId | Verified identity; later authorization cannot switch user or tenant |
| subscriptionRequestId | Exact subscription intent sequence; responses echo it and cannot revive canceled intent |
| groupIds | Bounded requested targets; approvals cannot expand the set |
| authorizationVersion | Current subject authorization generation, independent of membership and group generations |
| arguments | Business parameters decoded using the declared schema; raw-body paths can retain original bytes |
| target / schema / payload | Candidate message target, schema, original size and SHA-256; no additional authority |
| parameters | Bounded configured scalar mappings, never replacements for context/principal |

Fields depend on the operation. The model catalog includes the actual CallContext, Principal, AuthorizationResponse, SubscriptionResponse, RemoteResult, PublicationAuthorization, InitializationReply and LifecycleAcknowledgement structures.

Minimal allowed identity response:

```json
{
  "requestId": "request-a",
  "decision": "allow",
  "principal": {
    "uid": "subject-a",
    "tenantId": "tenant-a"
  },
  "authorizationId": "authorization-a",
  "authorizationVersion": "1",
  "expiresAt": 1792324860000,
  "grants": {
    "canReceiveDirect": false,
    "allowedOperations": [],
    "groups": []
  }
}
```

expiresAt must be a still-valid integer millisecond deadline within the deployment TTL. This timestamp illustrates the format; derive actual fixed expiry from real authorization. Allow does not implicitly grant all actions or groups. Deny, non-2xx HTTP, timeout, unknown decisions, mismatched requestId, missing evidence and expiry never grant authority. HTTP 200 or a uid alone is insufficient.

`principal.uid` must be a nonempty, non-whitespace-only string of at most 128 UTF-8 bytes without control characters. Missing, null, numeric and other non-string values are rejected even with HTTP 200 and decision=allow. IDs are neither trimmed nor coerced: string `"0"` is valid, numeric `0` is not. This admission gate also applies to local JWT and trusted gateway identities, every SDK and every transport; disabling additional connection authorization cannot bypass it. Explicit anonymous mode generates a valid temporary uid on the server; other authentication failures never fall back to anonymous identity.

Subscription approvals also echo subscriptionRequestId. Each groups item includes groupId, authorizationId, authorizationVersion, groupVersion, expiresAt and canReceive=true for a requested target. Membership, group and subject generations remain independent. Old evidence, duplicates or expanded targets reject the operation.

RemoteResult correlates requestId/operationId and reports status accepted/completed/denied/unknown; an optional business result uses the declared responseSchema. Timeout after HTTP handoff has unknown effect, not proof of non-execution. See [publication](publishing.en.md) for publicationId/grantId/bodyDigest/Push and stable retry identity.

Response mapping projects only approved JSON Pointers to the standard structure. Explicit allowValue/denyValue map to allow/deny; unknown values fail. Mapping never removes correlation, identity or expiry requirements. Disabled capabilities make no corresponding calls. Disabling remote identity does not implicitly enable anonymous access; anonymous policy must be explicitly configured with public capability restrictions.

## Management API

[OpenAPI](../contracts/management.openapi.json) enumerates all paths. Enable nacket.management and the independent server listener, configuring allowedPeers, identities, separate control keys and bounded requests. Both actual peer and Bearer identity must match. Credentials are `Bearer <identityId>.<secret>`. readOnly=true permits GET only and rejects mutations before reading their body.

| Method and path | Purpose |
| --- | --- |
| POST /management/routes/validate | Compile a candidate and return normalized definition, assigned revision and digest without committing |
| PUT /management/routes/{routeId} | Submit operationId/expectedRevision/definition; first creation uses expectedRevision="0" |
| POST /management/routes/{routeId}/online | Derive a new revision and request online without bypassing suspension |
| POST /management/routes/{routeId}/offline | Persist suspension and wait for old permits and writes |
| POST /management/routes/{routeId}/resume | Clear suspension with expectedRevision and expectedOwnerRevision; old connections stay invalid |
| POST /management/routes/{routeId}/transfer | Transfer owner/ownerKind under offline conditions and both CAS checks |
| GET /management/routes/{routeId}/status | Desired and local state, ownership, suspension and durable operation progress |
| GET /management/routes/{routeId}/describe | Current public encoding, schemas and capabilities |
| GET /management/routes/{routeId}/bindings | Actual local listener and declared external mapping |
| GET /management/routes/{routeId}/directory | Finite-lived, applied access addresses without client authority |
| GET /management/routes/{routeId}/authorizations/{operationId} | Trusted revocation acceptance and global completion |
| GET /management/operations/{operationId} | Durable route, ownership or retirement result |
| GET /management/manifests/{sourceId}/{generation} | Per-entry manifest results; reject the entire query if any route is outside scope |
| POST /management/nodes/{nodeId}/retire | Retire an exact expectedIncarnation; reject without drain or valid expiry evidence |

Queries are not accepted; GET bodies are rejected. definition.revision is optional and always replaced by expectedRevision+1. Clients cannot select an arbitrary large revision to bypass CAS. Optional definitionDigest must match the normalized definition. Validation does not prove resource preparation or global completion.

```json
{
  "operationId": "offline-a",
  "expectedRevision": "1"
}
```

A kind=operation response carries a durable operation. Phases are quiescing, preparing, activating, effective or superseded. Only the corresponding completion facts establish effectiveness. Query the same operationId. HTTP 200, acceptance or one node's localOnline does not establish global barrier completion. Active routes can subsequently enter protection after lease, authorization or dependency loss.

400 means structural/semantic rejection, 401 peer/credential denial, 403 read-only/scope denial, 404 a missing scoped resource, 409 revision/ownership/barrier conflict. A 503 control_operation_unknown requires querying the original operation. Codes are bounded and contain no secrets. Host request size, concurrency and deadline limits may produce their own earlier response; those responses are also not proof of remote side-effect rollback.

Removing configuration entries, disabling provisioning or restarting never erases persisted offline/ownership facts. See [configuration](configuration.en.md) and [shutdown responsibilities](shutdown.en.md).

## Bounded handshake facts

AUTH may omit `authData`. When present it must be an object with at most 16 scalar properties and at most 8192 encoded JSON bytes. Route `handshake.authFields` assigns a maximum JSON size per allowed property, up to 4096 bytes. Unknown properties reject connection; allowed null properties are treated as omitted. These fields cannot overwrite trusted tenant, UID, sessionKey or authorizationVersion.

```yaml
handshake:
  authFields:
    deviceId: 256
    locale: 64
  headers:
    x-client-zone: 128
```

`headers` permits up to 16 lowercase names, each limited to 1024 bytes. Only allowlisted values from the real WebSocket upgrade are retained; duplicates and oversize values are rejected. Authorization, Cookie, Host, Origin, forwarding, proxy, WebSocket negotiation and trusted-gateway assertion headers cannot be forwarded. Native TCP has no upgrade headers. Pre-authentication header extraction across deployment routes has an additional aggregate bound.

HTTP mappings read `/handshake/auth/deviceId`, `/handshake/peer`, `/handshake/origin` and `/headers/x-client-zone`. These are source facts, not authority. Retained facts are available to identity, connection/initialization, subscription, notification, client publishing and refresh calls. Lifecycle fact publication outside the connection does not automatically include them. Node `remoteCalls.maxContextBytes` bounds conservative context retention across pending and established sessions. Connection close releases it; exhaustion rejects new admission.

## Standard callback request models

`CallbackRequest` in `contracts/communication.json` defines nine standard JSON operations: `authenticate`, `connect`, `initialize`, `subscribe`, `authorize`, `refresh`, `notify`, `message` and `authorize_publication`. Each carries `requestId`, `context`, `handshake` and `headers`. `handshake.auth` contains route-approved client-reported scalar values; `headers` contains the approved forwarded request headers. Empty maps are sent when no values are retained. Neither map grants identity or authority. Independent lifecycle facts use a separate contract.

Rust exposes the `CallbackRequest` union; Java exposes `Callbacks.read`. Unknown top-level and handshake properties are rejected. SDK defaults allow absent empty `auth` and `headers`, and absent `handshake` for non-authentication requests, without generating authority. Custom field mappings require the endpoint to read its own mapped contract.

`refresh` uses `RefreshRequest` with the fixed `operationName` of `authorization.refresh`, the original `principal`, `authorizationId` and `authorizationVersion`. The response must omit `principal` or set it to null. Every non-null principal object is rejected, even when identical to the original; the session retains its existing principal. Operation authorization is not an identity refresh. Deployment-approved extra `parameters` default to an empty object.
