# Socket.IO compatibility

[中文](socketio-compatibility.md)

Socket.IO is an external transport adapter for nacket. The same service core owns identity, routes, subscriptions, sending permissions, and each route's single ProtocolBytes encoding. Deployment setting `transport.socketio` and route setting `access.socketio.enabled` must both be enabled.

The standard entry uses `transport.socketioPath`, `/socket.io/` by default, and the registered route namespace. It accepts Engine.IO 4 over WebSocket. The query must contain exactly `EIO=4` and `transport=websocket`. Credentials belong in the CONNECT `auth` body, never the URL. Polling and polling upgrades are not provided.

Business bodies use one binary attachment encoded with the route's JSON_BYTES, VARINT_TLV, BITPACK_TLV, or FAST_FIXED mode. An operation ACK describes an operation result; terminal receipt acknowledgements have a separate contract. After CONNECT, clients must complete `nacket:hello`, `nacket:confirm`, and `nacket:ready` before business operations.

## Legacy path mapping

`compatibility.legacyPath` defaults to false. When enabled, the route's registered namespace also serves as an HTTP upgrade path, optionally followed by one `/`:

```yaml
access:
  listenerRef: shared
  endpoint: /route-a
  socketio:
    enabled: true
    namespace: /route-a
compatibility:
  legacyPath: true
```

A client can use HTTP path `/route-a/` and connect to root namespace `/` on that physical connection. CONNECT `endpoint` must also be `/`. The server maps it to registered namespace `/route-a`. Wire responses retain namespace `/`, while Hello exposes the actual routeId, routeRevision, and definitionDigest. The standard `/socket.io/` entry with namespace `/route-a` remains available.

This alias only locates a route. It does not accept legacy query credentials or change identity verification, Origin checks, confirmation, initialization, subscription authorization, or message formats. Clients must implement the current public handshake and binary body contract. `compatibility.joinLeaveAliases` is a separate switch; enabling a path alias does not enable old event names.

The mapping has these boundaries:

- The registered namespace must be non-root and have no trailing `/`. It must not conflict with either fixed transport path. There is no URL decoding, prefix matching, or default route fallback.
- Upgrade freezes the route ID, revision, digest, and actual listener scope. Authentication checks them again. A definition change requires a new physical connection.
- A legacy path connection only accepts root namespace. It cannot switch to another route. A dedicated port also enforces its bound route.
- Route shutdown and disabling compatibility enforce current gates for new and existing sessions. Re-enabling does not bind an old handshake to a new definition.
- Failures expose bounded error categories without credentials or internal control material. Failed authentication creates no subscription or receiving permission.

## Operation results and connection authority

Normal business rejections, including request identity conflicts or request-directory limits, return the original
request's ACK with `effect=none`. A namespace with valid identity and route authority can continue with new
requests, and other namespaces on the same transport remain independent. Identical retries share execution and
results within finite retention rather than invoking external work again.

Loss of authority, expired identity, route revocation and actual output failure have different consequences.
A result or ACK that cannot complete safely closes the affected session; a partial physical write closes the
entire transport. See [request identity](communication.en.md#request-identity-replay-and-rejection) and
[session state notices](session-state.en.md).
