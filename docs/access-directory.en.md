# External addresses and access directory

[中文](access-directory.md)

Each nacket node binds its own dedicated ports. The same route can use different ports on different nodes. The management directory publishes node-specific addresses bound to the complete route definition, node incarnation, and actual `bindingId`.

Automatic allocation skips shared, management, and monitoring ports, plus every explicit dedicated port of an enabled route in the same authority snapshot, including routes whose local preparation has not completed. An explicit port must bind successfully. If another process or an existing listener owns it, the route stays closed; it neither falls back to another port nor relocates an active automatic listener. Reserving a port does not make a route ready.

Address publication is disabled by default. Binding `0.0.0.0:3032` establishes local ownership, not browser reachability. Deployment prepares Docker publishing, NAT, proxies, firewall rules, and external TLS termination, then declares the path in a startup-fixed listener template:

```yaml
nacket:
  listeners:
    shared:
      bindAddress: 0.0.0.0
      defaultPort: 3030
      protocols: [native_tcp, native_ws, socketio]
      dedicatedPorts:
        allowed: true
        autoStart: 3032
        autoEnd: 3062
      advertise:
        enabled: true
        host: socket.example.com
        secure: true
        networkReady: true
        ports:
          - boundStart: 3030
            publicStart: 13030
            count: 33
```

Local port `3032` maps to external port `13032`. `networkReady: true` is a deployment declaration that the network path exists, not a successful active probe. `secure: true` declares external TLS; it does not enable TLS termination on the native listener. Declare only mappings that deployment has established. Disabling `enabled` retains configuration but publishes no addresses.

A template supports at most 16 mappings. Neither local nor public ranges may overlap, contain port zero, or overflow. The shared default port must be mapped. A route using automatic dedicated ports requires coverage of its entire candidate range; an explicit dedicated port requires an exact mapping. Incomplete mapping leaves the route protected rather than falling back to the shared port. Restrict automatic ranges to ports actually exposed by deployment.

## Query contract

`GET /management/routes/{routeId}/directory` uses the separate management listener, actual TCP peer allowlist, Bearer authentication, and route scope. There is no anonymous public directory by default. An application microservice can query with a read-only identity and return authorized entries through its own REST API. Do not put management write tokens in browsers.

```yaml
nacket:
  management:
    enabled: true
    allowedPeers: [127.0.0.1]
    identities:
      directory-reader:
        tokenSecretRef: NACKET_DIRECTORY_TOKEN
        controlKeyId: directory-scope
        readOnly: true
```

`controlKeyId` must reference a configured administrator control identity whose `routes` limit readable scope. `readOnly: true` rejects all non-GET methods before reading the body or submitting commands. The Bearer value is `directory-reader.<secret>`, with the secret resolved from the referenced environment material. Other management deployment requirements still apply.

The response contains `clusterId`, `scopeEpoch`, `routeId`, string `routeRevision`, `definitionDigest`, integer-millisecond `observedAt`, and `entries`. Each entry contains:

| Property | Meaning |
| --- | --- |
| `binding` | Exact route ID, definition revision, and digest. |
| `nodeId`, `incarnation` | Publishing node and durable process incarnation. Caching by nodeId alone is insufficient. |
| `permitId`, `requestNonce` | Route permit and renewal request associated with the publication. Neither authorizes a client. |
| `issuedAt`, `expiresAt` | Integer Unix milliseconds, retaining the original permit lifetime. Retrying the same nonce does not refresh them. |
| `serving` | The node has acknowledged local activation. Only true entries are returned. |
| `listener.bindingId` | Actual listener-owner identity. Port reuse and process restart cannot retain an old identity. |
| `listener.boundAddress` | Local address for management diagnostics, not an external endpoint. |
| `listener.host`, `port`, `secure` | Declared public host, mapped port, and TLS property. |
| `listener.endpoints` | Enabled adapter, scheme, transport path, and application endpoint. |

`native_tcp` uses `tcp` or external `tls` without a transport path. `native_ws` uses `ws/wss` and a WebSocket path. `socketio` uses `http/https`, an Engine.IO path, and a namespace in `endpoint`. A native application endpoint belongs in AUTH; it does not replace the WebSocket transport path. Bracket IPv6 hosts when constructing URLs.

Address publication commits atomically with its route permit and becomes queryable after the node acknowledges actual activation. Offline routes, obsolete definitions or incarnations, changed or missing permits, and expired entries are excluded. Unavailable control fails the query instead of presenting stale local addresses as current results. The directory is a finite discovery hint; networking and authorization can change after a query, so clients must complete normal authentication.

`GET /management/routes/{routeId}/bindings` separately reports this node's `boundAddress`, `bindingId`, `incarnation`, local `state`, `advertised`, and `publicAccess`. `reachability: deployment_declared` indicates a current entry and local eligibility; `unpublished` indicates no current publication. Neither claims an external connectivity probe.

## Pinning the handshake target

After selecting a directory entry, native AUTH or Socket.IO CONNECT auth can include `expectedBinding`:

```json
{
  "clusterId": "cluster-a",
  "scopeEpoch": "1",
  "nodeId": "node-a",
  "incarnation": "3",
  "routeId": "route-a",
  "routeRevision": "12",
  "definitionDigest": "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
  "bindingId": "listener-a"
}
```

All eight fields are required together. The server checks the exact target before invoking remote identity services. Reused ports and obsolete nodes or definitions are rejected; the client refreshes the directory and starts a new attempt. This field narrows the target without granting identity or subscription permission. Clients using a configured fixed endpoint can omit it, but must still check the route definition in hello and complete confirm.

Nacos continues registering fixed bootstrap endpoints; dynamic ports do not hot-edit registration configuration. naway or another gateway can consume the read-only directory, but must explicitly implement discovery, expiry, and binding checks. A fixed upstream alone does not provide dynamic selection. Use prepared static mappings until that integration exists.
## Client connection projection

After authorizing the caller, business REST selects one entry and returns the bounded structure shared by the SDKs:

| Field | Meaning |
| --- | --- |
| `adapter` | `native_websocket` or `socketio`; Rust also supports `native_tcp` |
| `url` | Approved public address, without credentials, query or fragment |
| `endpoint` | Logical AUTH endpoint; namespace for Socket.IO |
| `path`, `namespace` | Optional Socket.IO Engine path and namespace |
| `expectedBinding` | The complete eight-field binding described above |
| `expiresAt` | Unix-millisecond deadline inherited from the selected entry, never renewed or extended |

Projection adapter `native_websocket` corresponds to directory endpoint adapter `native_ws`. The URL origin must also belong to the client's locally trusted set. Directory access does not grant messaging permission. SocketClient resolves a fresh projection before each automatic reconnect and clears old subscription intents when the definition changes. Unknown origins or invalid projections stop recovery; there is no port scanning or stale-address fallback. Static expectedBinding is only a fixed assertion, not a directory resolver.
