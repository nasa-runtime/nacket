# nacket

[中文](README.md)

nacket is an independently deployable service for persistent connections, dynamic routes, and clustered message delivery. Applications supply identity, authorization, and message production through declarative configuration and HTTP/MQ contracts. The service manages connections, subscriptions, protocol adapters, route permissions, delivery, and resources.

The service provides no built-in application routes or domain objects. `route-a`, `service-a`, `event-a`, and `schema-a` are neutral examples; applications choose their names, serialization, and workflows. Client-to-client forwarding is denied by default. Subscribing to a group ID requests read access, not permission to send to other users.

One configuration joins dynamic definitions, finite route permits and final write gates. Accepting a
configuration does not establish that a route is serving, and a live connection does not establish permission
to send or receive. See [architecture](docs/architecture.en.md) for each stage and its confirmation boundary.

## Quick start

Build with Rust 1.95.0. NASA Runtime and all other dependencies come from crates.io, with versions and checksums pinned by `Cargo.lock`. No adjacent framework checkout or local path override is required:

```sh
git clone --branch 1.0.0 https://github.com/nasa-runtime/nacket.git
cd nacket
python3 ci/build-image.py --tag nacket:1.0.0
docker compose --file docker/compose.yml up --detach
curl --fail http://127.0.0.1:3031/readyz
```

This starts the default empty route catalog without Redis, Kafka, or Nacos. Successful readiness establishes host runtime availability; business routes require their own configuration and applied-state confirmation. Stop with `docker compose --file docker/compose.yml down`; volumes remain by default. See the [container guide](docker/README.en.md) and [deployment reference](docs/deployment.en.md) for persistent storage, logging, `APP_CONFIG_FILE`, dedicated ports, and trusted sources.

The distribution image is named `nasaruntime/nacket:1.0.0` for `linux/amd64` and `linux/arm64`. Image upload uses a manual publishing workflow; local builds require no Docker Hub credentials.

## Core capabilities

- **Dynamic routes at runtime**: trusted configuration or management operations create, activate, deactivate, redefine, and restore routes. Durable revisions, node incarnations, preparation state, and barriers for old permits govern cluster transitions. Old sessions and messages cannot enter a new definition.
- **One codec per route**: ProtocolBytes `JSON_BYTES`, `VARINT_TLV`, `BITPACK_TLV`, and `FAST_FIXED` are supported. Route administrators choose one mode and a valid schema. All adapters for that route use the same encoding; clients cannot override it per message.
- **Protocol adapters**: native TCP, WebSocket, and independently enabled Socket.IO. Socket.IO is an external compatibility protocol; the core route, permission, and message models do not depend on its namespace or ACK implementation.
- **Trusted delivery**: Redis Stream or Kafka carries separate metadata and original business bytes. Deliveries with the same format share body bytes. Source, route definition, target, expiry, subscription, and final write permissions are checked.
- **Independent application policies**: identity checks, additional connection authorization, `sub/unsub`, allowlisted `notify`, explicitly enabled `msg`, initialization, connection facts, and receiving policies are configured separately. Enabling a capability grants no implicit business permission.
- **Subject event ordering**: napart strictly consumes events for the same trusted `(tenantId, uid)` across connections and routes within one node. Remote waits stay outside partitions; revocation is immediate and completions revalidate in the original domain. See [ordering and capacity](docs/uid-ordering.en.md).
- **Managed infrastructure**: NASA Runtime supplies napp, naml, HTTP, Redis, Kafka, Nacos, and nafana. `#[nasa::application]` starts the application; configuration, network resources, and shutdown share one host lifecycle.

## Messages and permissions

The default sending flow is: business REST service validates identity and permissions → publishes to Redis Stream/Kafka → nacket validates and delivers → client receives.

`sub/unsub` manages the current connection's subscription intent without changing business membership. Private subscriptions require valid, unexpired read grants. Unsubscribe first prevents new delivery permits, then waits for affected writes already in progress. Retries and heartbeats cannot extend identity, group grants, route permits, or message deadlines.

`notify` performs allowlisted operations on the current connection, fixed HTTP calls, or isolated business notification publishing. It receives no general delivery sender. `msg` is disabled by default. When enabled, `business_command` delegates to a fixed business REST service, while `authorize_then_publish` obtains a grant bound to the original body digest, route, and narrowed target before submitting to MQ. Neither path broadcasts directly to local clients.

Bodies use the route's single encoding. Forwarding does not require materializing business objects; subscriptions, JSON parameter mapping, and policies that inspect bodies do decode their data. HTTP/MQ clients, WebSocket masking, TLS, and kernel boundaries can still copy bytes. The service does not claim zero copies across every boundary.

All absolute communication times are nonnegative integer Unix milliseconds within JavaScript's safe integer range. Date strings, numeric strings, and floating-point times are rejected. Broker acceptance, completed writes, client receipt, and business processing are distinct stages.

Authorization revocation uses a separate trusted MQ ingress and persists independent subject, membership, and group version floors. Exact-session eviction and completion barriers cover the cluster. See [authorization revocation](docs/authorization-control.en.md), [publishing and remote authorization](docs/publishing.en.md), and [connection lifecycle](docs/lifecycle.en.md).

## Routes and deployment

The primary port defaults to 3030 and can be changed with `ws.addr`. `ws.multiplex: true` dispatches native TCP and WebSocket through one managed listener. Routes can share a port or use a dedicated port. Automatic allocation acquires the actual socket with bind; it does not probe then compete for the port. Monitoring and management ports are excluded from business allocation.

Local control uses the durable `routeLedger` file and a file lock; it does not provide multi-node consistency. Cluster mode explicitly selects Redis Stream or Kafka control authority, maintaining definitions, node incarnations, and finite permits durably. Data nodes cannot bypass control barriers with local configuration. Losing control evidence beyond permit expiry places routes in protection.

Accepting configuration or a management request does not mean the cluster has applied it. Updates prevent new permissions under the old definition, wait for the old write barrier, and then activate the new definition. Preparation, persistence, or dependency failure leaves routes closed or protected. Conflicting content at the same revision, stale revisions, and conflicting entry points are rejected.

Configuration manifests and management API operations have separate route ownership. Emergency deactivation preserves suspended state, which subsequent configuration cannot clear automatically. Resuming, transferring ownership, and recreating routes require explicit authorized operations; omission from configuration cannot erase durable deactivation facts.

## Configuration, identity, and discovery

naml strictly loads, validates, and watches YAML. Configuration center and registration/discovery have independent switches. Nacos can supply centralized configuration and business service discovery separately. Remote calls use deployment-approved endpoints and managed load balancing. Clients cannot select URLs, services, HTTP methods, or service credentials.

`APP_CONFIG_FILE` selects a relative or absolute main configuration path. When unset or empty, it defaults to `zcf/application.yml` under the working directory. An invalid selected file prevents startup without falling back to that default. See [configuration](docs/configuration.en.md) for path, profile, and reload rules.

Identity strategies include remote HTTP, local JWT with configured keys or JWKS, trusted gateway assertions, and restricted anonymous access. Anonymous routes only expose explicitly public capabilities. Remote grants must correlate to the current request and include a valid principal and bounded expiry. Denial, errors, expiry, or malformed responses grant no permissions. Connection confirmation binds the route digest, and business fan-out waits for initialization.

Every SDK and direct client must resolve to a trusted, nonempty, non-whitespace-only string uid. Missing, null, non-string, control-character or over-128-byte UTF-8 values cannot establish an authenticated session. This gate applies to every transport and reconnect and cannot be disabled through additional connection authorization settings. Valid IDs remain unchanged. Explicit anonymous access generates a valid temporary uid on the server; failed authentication never falls back to anonymous admission.

Trusted gateway assertions are durably consumed by the cluster authority. Redis and Kafka both bind each assertion to one exact admission attempt; local mode synchronizes its own durable baseline. See [gateway assertions](docs/gateway-assertions.en.md).

Route changes apply with new revisions. Deployment resources such as listener templates, fixed publisher catalogs, and node budgets require restart when changed. Invalid candidates cannot replace effective definitions; observing a candidate does not mean resources have switched.

The default configuration has no online routes. Redis, Kafka, Nacos configuration, and discovery are explicitly disabled. Enabling a source also requires valid resources, source permissions, and route definitions. A component being included in an image does not enable application permissions.

See [configuration structure and validation](docs/configuration.en.md) for the public JSON Schema and the boundary between structural checks and full server validation.

## Observation and operational boundaries

nafana exposes metrics on a separate listener and port. Shared collectors read process resources, visible container resources, and explicitly configured filesystems and interfaces. Unsupported, unavailable, failed, and stale measurements remain distinct rather than becoming healthy zeros. Product metrics cover connections, requests, authorization, subscriptions, outbound queues, actual writes, receipts, MQ, control, and configuration.

napp manages nalog with an `info` level and file directory `/usr/local/logs/${application.name}` by default. `NACKET_LOG_PATH` overrides the directory; an explicitly empty value selects console-only output. A nonempty directory must be writable by the process user. See [logging configuration](docs/logging.en.md).

Connection success and disconnection logs default to enabled and can be disabled independently through hot-reloaded `nacket.connectionLogging.connected` and `disconnected`. Disconnection always cancels every subscription on that connection. Logs include session identity, route revision, peer IP, session duration, and cancelled subscription counts.

Node and connection queues bound both item counts and bytes. Slow consumers can be rejected, disconnected, or retain the latest value for route-authorized state events. Individual messages never silently become replaceable state. Normal shutdown revokes admission and permissions before draining work within original deadlines and the host's overall shutdown deadline.

- Socket.IO currently supports Engine.IO 4 WebSocket transport, without HTTP polling. See [Socket.IO compatibility](docs/socketio-compatibility.en.md) for explicit legacy path mapping and its permission boundaries.
- See [Kafka delivery and recovery](docs/kafka-ingress.en.md) for source settings, stable group provisioning and gap protection.
- Business listeners share node admission and receive budgets. See [resource limits](docs/resource-limits.en.md).
- Connection quotas support node and cluster scopes. Cluster reservations commit durably; replacement waits for previous writes. See [connection quotas](docs/connection-quotas.en.md).
- Single-use assertion consumption requires an available durable authority. An unknown result denies admission; callers should obtain a new assertion.
- Initialization grants use HTTP or a separate MQ reply ingress, bound to the original request, full session, and principal. Broker acceptance grants no permission.
- Connection fact queues are not durable outboxes. A crash can lose notifications; application presence needs leases or reconciliation.
- The service stores no business history, performs no business workflow consensus, and guarantees no end-to-end exactly-once delivery. MQ retention is not a disconnected-client replay contract.

See [build and deployment](docs/deployment.en.md) for complete examples, source path dependencies, container operation, Nacos switches and port mappings. See [architecture](docs/architecture.en.md) for responsibilities and ordering.

## Source and license

`bootstrap` installs components, `config` validates declarations, `domain` holds permission and protocol invariants, `application` coordinates workflows, `adapters` connects external systems, and `observability` exposes bounded state projections.

Resolved NASA Runtime components must provide managed protocol handlers, prebound listeners, explicitly optional sources, metric capacity reservations, and consumer recovery interfaces. Framework capabilities remain separate from product contracts: nacket routes, envelopes, permissions, and business events do not belong in the generic framework.

The source, documentation, and included configuration may be used under either [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE). Third-party components retain their own licenses; see [dependency notices](THIRD-PARTY.en.md). Read [CONTRIBUTING.en.md](CONTRIBUTING.en.md) for contributions and [SECURITY.en.md](SECURITY.en.md) for trust boundaries and private reporting. Distribution uses source and containers with `publish = false`; this service is not distributed as a standalone crates.io package.

## Interface and operational documentation

- [Communication models and wire layout](docs/communication.en.md), [business HTTP and management API](docs/http-api.en.md), [management OpenAPI](contracts/management.openapi.json).
- [Access directory](docs/access-directory.en.md), [session state notices](docs/session-state.en.md).
- [Monitoring and metric semantics](docs/monitoring.en.md), [shutdown and persistence ownership](docs/shutdown.en.md).

The handshake route descriptor exposes `restoreSubscriptions` for automatic subscription requests. Disabling it still permits explicit `sub`; reconnecting never inherits previous session permissions. The companion product name is **Nacket SDK**, with package name `nacket-sdk` across Rust, Java and JavaScript. This repository provides server communication contracts, not Rust, Java or npm SDK distribution packages.

The default health HTTP listener is port 3031 with `/healthz` and `/readyz`. Management actions remain disabled. Monitoring uses its own port and credentials. `server.enabled=false` closes the health listener; update container probes accordingly.

Dual-licensed under [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE), at your option. See [third-party dependencies](THIRD-PARTY.en.md) for dependency composition.

Business pushes and initialization replies skip offline backlog after restart. Redis captures the tail when consumption starts; Kafka uses a fresh startup group with latest offsets. Durable routing and revocation state still recover. See [realtime consumption boundaries](docs/mq-delivery.en.md).
