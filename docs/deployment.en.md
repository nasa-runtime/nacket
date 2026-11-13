# Build and deployment

[中文](deployment.md)

One nacket image contains every ProtocolBytes mode, native TCP/WebSocket, Socket.IO compatibility, Redis Stream, Kafka, Nacos, HTTP and dedicated monitoring. Configuration selects resources and permissions. The default image has an empty route catalog: readiness means the runtime is available, not that a business route has been opened.

## Source dependencies and build

Use Rust 1.95.0, Python 3.11 or later, Clang/libclang, CMake, pkg-config, Perl, and a C/C++ toolchain. NASA Runtime `nasa 2.0.7` and all other dependencies come from crates.io. `Cargo.lock` pins resolved versions, registry sources, and archive checksums. No adjacent framework checkout, Git dependency, local `path` override, or extra Cargo configuration is required.

```sh
git clone --branch 1.0.0 https://github.com/nasa-runtime/nacket.git
cd nacket
cargo build --locked --release --features schema --bins
python3 ci/build-image.py --tag nacket:1.0.0
```

`rust-toolchain.toml` pins Rust. The Docker builder installs C/C++ tools for Kafka and TLS. Fresh builds need access to base images, Debian repositories, and crates.io. Local caches do not change lockfile sources or checksums. Runtime deployments need no Rust installation.

`ci/build-image.py` supplies only the product directory as Docker context. `.dockerignore` excludes local runtime and generated content. The helper compares actual inputs before and after building; Docker checks again before and after compilation. Changes to source, public resources, or the lockfile prevent the image from representing current inputs. Builds retain `--locked` rather than implicitly changing dependency resolution.

Images include the service and schema executables. The latter emits configuration or communication contracts:

```sh
docker run --rm --entrypoint nacket-schema nacket:1.0.0
docker run --rm --entrypoint nacket-schema nacket:1.0.0 communication
```

`python3 ci/package-source.py` generates a real Cargo archive and compiles its service and schema executables using the archive's registry dependencies. It checks bilingual READMEs, licenses, version, and normalized manifest, compares extracted bytes, and generates strict public library documentation for all features from the archive. Local quality content, caches, and runtime data are excluded. Use `--target-dir /path/to/output` for output and `--offline` with a complete dependency cache; no dependency override is required.

Generate library documentation separately with `cargo rustdoc --locked --lib --all-features -- -D warnings -D missing-docs`, followed by `python3 ci/verify-docs.py target/doc`. Undocumented public items, documentation warnings, unresolved dependencies, or inconsistent archive contents fail the command.

CI builds with locked registry dependencies on native amd64 and arm64 runners, retaining platform image archives and configuration digests without automatic upload. Product version, Git tag, and Docker Hub version are all `1.0.0`. See [container publication](../docker/README.en.md) for source binding, credentials, immutable versions, and overview readback. The distribution image is `nasaruntime/nacket:1.0.0`; local builds may use `nacket:1.0.0`.

`docker/compose.yml` defaults to the local image with empty routes and separate data/log volumes. Images include English and Chinese public documentation under `/usr/share/doc/nacket`, both product licenses, and third-party notices.

## Run the container

Business port `3030`, health HTTP `3031` and the recommended monitoring port `9464` are separate. Health HTTP provides `/healthz` and `/readyz`; management actions remain disabled by default. Monitoring is disabled by default and requires a dedicated listener and credentials when enabled.

```sh
docker volume create nacket-data
docker volume create nacket-logs
docker run -d --name nacket \
  --read-only --cap-drop ALL --security-opt no-new-privileges \
  --stop-timeout 25 \
  --mount type=volume,src=nacket-data,dst=/app/data \
  --mount type=volume,src=nacket-logs,dst=/usr/local/logs/nacket \
  --tmpfs /tmp:rw,noexec,nosuid,size=16m \
  -p 3030:3030 -p 127.0.0.1:3031:3031 \
  nacket:local
```

The image runs as `10001:10001`. Existing mounted configuration must be readable, and data/log directories must be writable by that identity. Keep the non-root identity and prepare directory permissions. Each logical node needs its own persistent data volume and stable `nodeId`; two running instances cannot share one volume. Logs default to `/usr/local/logs/nacket`. An explicitly empty `NACKET_LOG_PATH` selects console output only, allowing the log volume to be omitted.

Mount configuration at `/app/zcf/application.yml`. For local file reload, mount the entire `zcf` directory so files replaced by atomic rename remain visible; a single-file bind mount does not reliably follow replacement inodes. `config_watch.enabled` controls local observation. Environment values are frozen at startup; changing them requires recreating the process.

The image HEALTHCHECK requests `127.0.0.1:3031/readyz` inside the container. Override it when changing `server.port`, `server.context_path`, or disabling HTTP/health routes. The container termination grace must exceed `application.shutdown_timeout_ms`. Admission closes before draining, and uncertain remote effects are never reported as successful.

## Complete configuration examples

| File | Resources and capabilities |
| --- | --- |
| [local.yml](../deploy/local.yml) | No external resources; explicit anonymous public route with public subscriptions and current-session status notifications |
| [redis.yml](../deploy/redis.yml) | Redis control authority, realtime Stream delivery, remote identity and subscription authorization |
| [kafka.yml](../deploy/kafka.yml) | Kafka control authority, separate data source and fresh consumer groups on every start, remote identity and subscription authorization |
| [nacos-discovery.yml](../deploy/nacos-discovery.yml) | Discovery only, load-balanced business HTTP, no self-registration |
| [nacos-registration.yml](../deploy/nacos-registration.yml) | Discovery plus self-registration of the fixed bootstrap listener |
| [nacos-configuration.yml](../deploy/nacos-configuration.yml) | Central configuration only, fixed business HTTP URL |
| [nacos-combined.yml](../deploy/nacos-combined.yml) | Independently enabled central configuration and discovery |
| [nacos-routes.yml](../deploy/nacos-routes.yml) | Remote route fragment, merged with the corresponding local bootstrap configuration |

Examples other than local require an actual business HTTP contract. Replace example addresses, sources and credentials before starting. The business service validates `NACKET_BUSINESS_TOKEN`. Control examples use independent high-entropy `NACKET_NODE_A_KEY`, `NACKET_NODE_B_KEY` and `NACKET_CONFIG_KEY` values; never distribute administrator secrets to clients. Business services authorize requests before publishing to approved sources using the [communication contract](communication.en.md).

## Redis and Kafka baselines

`NACKET_REDIS_URL` selects the prepared Redis service. Control and data resources share a hash tag but remain distinct resources; ACLs must restrict each producer's writable resources. `identity.mode=acl` is valid only when the configured producer is the actual exclusive writer. Shared write access requires signed identities bound to individual producers.

`control.initialize: true` is for explicitly creating a new control scope. Business Stream readers start at the current tail and do not resume old PEL entries. When existing baselines are missing, disable automatic initialization, fence old instances and recover persistent facts; starting empty resources cannot restore permissions. For node B, change `nodeId` and `control.settings.nodeKeyId` to `node-b`, use a separate volume, and preserve the same cluster, epoch, sources, manifest and authority.

Kafka requires `NACKET_KAFKA_BOOTSTRAP` and the actual `NACKET_KAFKA_CLUSTER_ID`. Prepare control command/state topics, the business topic and DLT before startup. Both control topics use one partition; state uses `cleanup.policy=compact` and commands use `cleanup.policy=delete`. Both require `retention.ms=-1`, `retention.bytes=-1` and disabled unclean leader election. The service validates topic policies and refuses incompatible topology.

Business and DLT topics use `cleanup.policy=delete` with unclean leader election disabled. Configure replication, minimum in-sync replicas and finite storage retention for the deployment. Business consumption requires `realtime`; Kafka uses `auto_offset_reset: latest` with a fresh group on every start, without provisioned old offsets. Restarting after an hour does not consume that hour of business backlog. Authorization-control recovery is configured independently; see [realtime consumption boundaries](mq-delivery.en.md) and [Kafka ingress](kafka-ingress.en.md).

Both the route's `delivery.mode` and its push ingress `delivery` must be `realtime`; changing only one is invalid. These ingresses do not set `recovery.initialize`. Recovery initialization belongs to control inputs that require persistent authority and cannot enable replay of offline business pushes.

Both broker examples use `readiness.policy: required_routes` with `route-a` in `routeIds`. `/readyz` succeeds only when the control permit, data source and required route are available. Update this list when renaming the route. Empty-directory or management-only deployments can use `runtime`, whose readiness result does not guarantee an open business route.

Keep control authority and business production permissions separate. `routeProvisioning` requires a stable `sourceId`, increasing `generation` and accurate `expectedRevision`. Existing routes cannot keep using the initial `0`. A new YAML file does not clear a management-induced suspended state.

## Nacos switches and registration

`nacos.enabled` controls configuration; `discovery.enabled` controls discovery. Both disabled, configuration only, discovery only and both enabled have independent behavior. Nacos is the currently supported backend; Consul and Kubernetes are not available choices. The current compatibility baseline is the Nacos 3.2.3 configuration and discovery interfaces. Provide both HTTP and the gRPC paths required by the configuration client; HTTP access alone does not establish configuration subscriptions.

Examples inject authentication through `NACKET_NACOS_USER` and `NACKET_NACOS_PASSWORD`. `NACKET_NACOS_ADDRESS` is the configuration address; `NACKET_NACOS_HTTP` is the discovery HTTP URL. Match namespace IDs, groups and service names to the server. The configuration example uses an empty namespace for the default namespace; discovery uses `public`.

Upload `nacos-routes.yml` as `nacket-routes.yml` in group `NACKET` before starting centralized configuration. A failed required initial fetch prevents readiness. Remote fragments cannot declare `yml`, `nacos` or `secret_providers` to change bootstrap authority. Route arrays replace the previous array; runtime revisions must increase. Cluster deployments also require a valid manifest proposal.

`NACKET_ADVERTISE_IP` must be reachable by callers, never `0.0.0.0`. Registration uses the fixed shared bootstrap port; both `listener_ref` and `eligibility_ref` are `socket`. Qualification depends on the actual listener and local readiness, independently of registration success. The example assumes reachable equal-numbered ports. For Docker NAT with different public ports, use an explicit access directory rather than registering an unreachable container address. Dynamic dedicated ports are not published by changing Nacos registration.

## Route ports and external networking

Configure the shared port with `ws.addr` or an explicit listener template. `access.dedicatedPort.enabled=false` uses the shared template port. When true, an explicit `port` binds only that port; otherwise allocation walks from `autoStart` (default 3030) to `autoEnd`. Shared, health, monitoring and explicitly reserved ports are skipped. With default health port 3031 enabled, the first automatic dedicated port is normally 3032.

Docker exposes only published ports. Restrict automatic allocation to a prepared range such as 3032–3062, excluding health port 3031. `advertise.ports` describes unequal NAT mappings. Local binding and external reachability are separate facts. See the [access directory](access-directory.en.md) for listener templates, mappings, directory entries and `expectedBinding`.

Business listeners accept plaintext TCP and HTTP WebSocket Upgrade, without built-in TLS termination or PROXY protocol. Terminate TLS at the deployment gateway and protect the internal gateway-to-service connection; an advertised `wss` address must lead to an actual TLS endpoint. Admission uses the real socket peer, which forwarding headers cannot override. The route's identity policy still validates business identity.

See [monitoring](monitoring.en.md) for credentials, collection and bilingual dashboards, and [architecture](architecture.en.md) for responsibilities and failure ordering. Product and dependency license material lives under `/usr/share/licenses/nacket`; see [third-party dependencies](../THIRD-PARTY.en.md) for inventory boundaries.

`/usr/share/nacket/build-manifest.json` records SHA-256 digests of the product files and Cargo.lock consumed by Docker and both executables. The build verifies identical inputs before and after compilation. This binds the product sources, locked registry dependency graph, and binaries within a trusted builder; it is not an independent signature or proof against a malicious builder. Public distribution should establish trust through pinned commits and build provenance.
