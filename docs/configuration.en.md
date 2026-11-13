# Configuration structure and validation

[中文](configuration.md)

nacket uses naml to load one YAML configuration tree. `nacket` contains product settings. NASA Runtime components own sections such as `application`, `log`, `ws`, `server`, `redis`, `kafka`, `nacos`, `discovery`, and `grafana`. Configuration management and service discovery have independent switches.

[configuration.schema.json](../contracts/configuration.schema.json) describes the body of `nacket` using JSON Schema 2020-12. Convert this section to JSON to check fields, types, enums, exclusive branches, and unknown properties with a compatible editor or validator. The schema does not describe the entire application YAML.

The schema follows the deployment types. Run `cargo run --features schema --bin nacket-schema` to write it to standard output. This command does not load configuration, connect to services, or open ports. The default Cargo build does not enable the `schema` feature. The product image enables it explicitly and includes the schema command.

## Main configuration path and file watching

`APP_CONFIG_FILE` selects the main file at startup. When unset or empty, it defaults to `zcf/application.yml`. Relative paths resolve against the process working directory at startup; absolute paths are used directly. Unicode and spaces are supported. A missing, unreadable, malformed, or invalid selected file prevents startup without trying the default file. The container working directory defaults to `/app`; mount a configuration directory and set `APP_CONFIG_FILE=/config/application.yml` to use another location.

Relative file imports resolve against the selected main file's parent directory. `APP_PROFILE` continues to select `zcf/application-{profile}` under the startup working directory, searching supported extensions; its directory does not move with the main file. The environment snapshot and selected path are fixed when the loader is created. Changing environment variables requires a process restart.

In service mode, `config_watch.enabled: true` watches the selected file and dependency directories, including in-place writes and atomic replacement. It also rereads them periodically using `config_watch.poll_interval_ms`, which defaults to 15000 milliseconds. Mount the parent directory when replacing files atomically in a container: a single-file bind mount may continue to reference the previous inode. A candidate that cannot be read or validated does not replace effective configuration. Successful file observation does not establish route barrier completion. `NACKET_LOG_PATH` continues to select the log directory through its configuration placeholder, independently of the main configuration path.

## Configuration groups

| Settings | Responsibility and boundary |
| --- | --- |
| `nodeId`, `clusterId`, `scopeEpoch`, `routeLedger` | Stable node identity, cluster isolation, persistent scope, and local recovery baseline; an existing volume cannot be initialized as a new scope implicitly |
| `control`, `routeProvisioning` | Local or Redis/Kafka authority, node roles, and trusted manifest proposals; observing a candidate does not mean it has been applied globally |
| `routes` | Route definitions, one codec, access, identity, subscriptions, actions, downstream delivery, and resource policies; enabling a capability does not grant permission |
| `endpoints`, `verifiers` | Fixed HTTP/discovery targets and JWT/gateway verifiers; clients cannot choose destinations or service credentials |
| `ingresses`, `publishers` | Fixed MQ sources, producer permissions, purposes, and recovery contracts; downstream messages, initialization, lifecycle facts, and revocation have distinct permissions |
| `listeners`, `primaryListener`, `transport` | Shared and dedicated listeners, adapters, handshake and heartbeat rules, node admission, and buffer limits |
| `remoteCalls`, `ingressProcessing`, `lifecycleDelivery` | Node budgets for remote calls, reads, asynchronous work, and lifecycle obligations |
| `management`, `readiness`, `metricScopes` | Management identities and network restrictions, readiness policy, and bounded metric names fixed at startup |
| `brokerMetrics` | Independent read-only backlog sampling switch, interval, timeout, and concurrency; disabled by default and fixed until restart |

Schema `default` annotations describe deserialization defaults. Validators should not write them back or select permissions for an application. `nodeId` and `routeLedger` alone describe an empty route deployment; it has no authenticated application entry. See the default [application.yml](../zcf/application.yml).

## Structure and operational validity

Structural validation does not replace full server validation. Endpoint and source references, port conflicts, combined budgets, anonymous policies, codec/type compatibility, key availability, broker topology, persistent revisions, and barriers for previous permits require further checks. A structurally valid remote identity route is still rejected if its `endpointRef` does not exist.

Route `schemas` use ProtocolBytes dynamic descriptors: `schemaId`, `fields`, `tag`, `name`, `type`, `nullable`, `maxBytes`, and `maxElements`. `type.kind` accepts only the defined scalar types, `enum`, `message`, and `list`. A `message.schemaRef` references a schema in the same route; it cannot read files or download types. The shared SchemaPlan compiles and encodes these types. Schema export does not introduce another codec implementation.

Route changes require a new `revision`; activation still waits for persistent commit and the previous write barrier. Endpoint parameters, verifier policies and schemas are frozen into the referencing route's digest. Updating their catalogs does not change existing sessions in place: routes using new material must submit new definitions. Listener templates, MQ sources and publishers, node budgets, management trust roots and metric name catalogs are startup resources that require a restart. Discovery connections, configuration sources and credentials retain their shared component lifecycle contracts. Observed configuration and effective runtime state are distinct. An invalid candidate cannot replace effective state, and an old snapshot cannot clear persistent offline or suspended state.

## Route and node policies

Each route may set `activation.minReadyNodes` to `1..256`; omission inherits `control.settings.minimumReadyNodes`. This field participates in the definition digest and requires a higher revision and control commit to change. Insufficient prepared nodes keep an operation Preparing; a listening socket is not a cluster readiness barrier. Taking a route offline requires only one node to take control responsibility, so the online threshold cannot block shutdown.

An enabled route's activation moves from `activating` to global `effective` only with Applied from every current `target`, with the Applied count meeting `minReadyNodes`; a sufficient count alone cannot complete the operation. Each node validates and opens its local gate before reporting Applied. Eligible nodes may therefore serve and publish directory entries during `activating`; a serving node or visible directory entry does not establish global completion. Deactivation enters `effective` directly once the old-permit barrier is empty, without waiting for Applied from every node.

Route `authorization.refresh`, `authorization.cache` and `handshake` are frozen with the definition. Refresh and cache default to disabled; extension and header allowlists default to empty. Disabling these features does not disable identity validation, expiry or revocation. Node `remoteCalls`, `ingressProcessing` and `sessionMetrics` capacities and scan settings require restart. New route definitions do not mutate existing sessions.

```yaml
nacket:
  remoteCalls:
    maxRefreshInFlight: 32
    maxRefreshPending: 256
    maxContextBytes: 67108864
    maxAuthorizationCacheEntries: 4096
    maxAuthorizationCacheBytes: 16777216
  ingressProcessing:
    maxPendingRecords: 256
    maxKeyPendingRecords: 64
    maxRetryAgeMs: 60000
    maxParallelFanouts: 4
  sessionMetrics:
    enabled: true
    activityWindowMs: 60000
    intervalMs: 100
    batchSize: 128
    maxTargets: 65536
    maxTargetBytes: 16777216
    maxCycleMs: 120000
```

These node settings do not enable any business route. See [authorization control](authorization-control.en.md) and [HTTP integration](http-api.en.md) for route policies and remote contracts.

## Route reload activation boundary

A local route replacement closes previous admission, durably commits the new revision, publishes the complete directory, and waits for all previous writers to finish. A ledger revision proves commitment, not live admission. `/readyz` describes node operating conditions and allows offline routes. Registry instance eligibility does not establish activation of an individual route.

INFO logs with `event="route_configuration"` distinguish `protected`, `committed`, `published`, and `activated` for delayed opening through `stage`. The `published` event includes `local_online` and an unavailability reason. Enable `metricScopes` with fixed `routeIds` to observe `nacket_scope_resources{kind="route",name="route-a",resource="online"}`. This metric grants no permission: callers must still authenticate, confirm the expected revision and complete the Ready handshake. Network, remote authorization or registration eligibility changes may reject a connection. Observing a ledger file update alone is insufficient.
