# nacket containers

[中文](https://github.com/nasa-runtime/nacket/blob/1.0.0/docker/README.md) · [Source](https://github.com/nasa-runtime/nacket) · [Deployment reference](https://github.com/nasa-runtime/nacket/blob/1.0.0/docs/deployment.en.md)

nacket provides persistent connections, dynamic routes, and trusted Redis Stream/Kafka delivery. Native TCP, WebSocket, and optional Socket.IO share ProtocolBytes, identity, subscriptions, and final write permissions. A route transition closes old permits and drains old writes before opening the new definition. Configuration acceptance does not establish serving state.

## Images and local builds

The distribution image is named `nasaruntime/nacket:1.0.0` for `linux/amd64` and `linux/arm64`. `latest` moves; pin a version or a manifest digest for deployment. Upload is manual and is not triggered automatically by a branch or Git tag push.

NASA Runtime and other dependencies come from crates.io with versions and checksums pinned by `Cargo.lock`; no adjacent framework checkout is required. Run from the nacket repository root:

```sh
python3 ci/build-image.py --tag nacket:1.0.0
docker compose --file docker/compose.yml up --detach
curl --fail http://127.0.0.1:3031/readyz
docker compose --file docker/compose.yml logs --tail 100
```

Compose selects the local image by default. Once the distribution image is uploaded, select it explicitly:

```sh
export NACKET_IMAGE=nasaruntime/nacket:1.0.0
docker compose --file docker/compose.yml pull
docker compose --file docker/compose.yml up --detach
```

One image contains all four codecs, Redis, Kafka, Nacos, Socket.IO, and monitoring. The default route catalog is empty; external sources and monitoring are disabled. Default readiness establishes host runtime availability, not an applied business route. Both Compose host ports bind to loopback only; remote access needs explicit mappings and network authorization.

## Ports, volumes, and environment

| Setting | Contract |
| --- | --- |
| `3030` | Default shared TCP/WebSocket business listener. Map dedicated route ports separately. |
| `3031` | Separate health HTTP with `/healthz` and `/readyz`. Management actions are disabled by default. |
| `9464` | Suggested monitoring port; disabled by default. Keep it off public networks. |
| `/app/data` | Exclusive persistent volume writable by UID/GID `10001:10001` for route and authorization facts. |
| `/usr/local/logs/nacket` | Default file-log directory; requires its own writable volume. |
| `NACKET_LOG_PATH` | Overrides the log directory. An explicit empty value selects console output only. |
| `NACKET_LOG_LEVEL` | Startup filter, default `info`; recreate the process to change it. |
| `APP_CONFIG_FILE` | Relative or absolute main configuration path. Unset or empty selects `/app/zcf/application.yml`. An invalid selected file prevents startup. |
| `APP_PROFILE` | Uses naml's independent profile template; selecting a main file does not relocate profiles. |
| `stop_grace_period: 25s` | Covers the default 15-second application shutdown budget. Increase both together. |

Each logical node needs a stable `nodeId` and an exclusive data volume. Two instances must never share a volume. Fresh named volumes inherit image directory permissions; deployments must prepare existing bind-directory permissions. Do not switch to root to bypass permission errors.

`docker compose --file docker/compose.yml down` stops the service and retains named volumes. Deleting a data volume loses durable facts; empty initialization must not reopen an old scope. See the deployment reference for volume and control recovery.

## Configuration and business traffic

The broker-free public example below enables anonymous public subscriptions and current-connection status notifications only, with business push disabled. Mount the directory read-only and select the main file using `APP_CONFIG_FILE`:

```sh
docker volume create nacket-data
docker volume create nacket-logs
docker run --detach --name nacket \
  --read-only --cap-drop ALL --security-opt no-new-privileges \
  --restart unless-stopped --stop-timeout 25 \
  --env APP_CONFIG_FILE=/etc/nacket/local.yml \
  --env NACKET_LOG_PATH=/usr/local/logs/nacket \
  --mount "type=bind,src=$PWD/deploy,dst=/etc/nacket,readonly" \
  --mount type=volume,src=nacket-data,dst=/app/data \
  --mount type=volume,src=nacket-logs,dst=/usr/local/logs/nacket \
  --tmpfs /tmp:rw,noexec,nosuid,size=16m \
  --publish 127.0.0.1:3030:3030 --publish 127.0.0.1:3031:3031 \
  nacket:1.0.0
```

Do not run this command and Compose on the same host ports simultaneously. Redis/Kafka examples need container-reachable brokers, trusted business REST, keys, and a correct control scope. Set independent node identities. Container `localhost` refers to that container. Nacos configuration, discovery, and registration are independent; provide reachable addresses, namespace, authentication, and required gRPC networking.

Mount entire directories for file watching so atomic replacements become visible. Single-file mounts do not reliably follow host inode replacement. `config_watch.enabled` controls watching. Changes to environment or bootstrap resources require a new process. Observing a candidate does not establish route Applied; business readiness should require the relevant routes.

Business listeners do not terminate TLS or implement PROXY protocol. Use a trusted TLS gateway, protect its internal link, and configure advertised addresses and port mapping explicitly. Forwarded headers do not change the real socket peer's source identity. Business history, offline replay, and end-to-end exactly-once are outside the service contract.

## Operations and included resources

The image runs as a non-root user with read-only-root support. Its default HEALTHCHECK reads `127.0.0.1:3031/readyz`; override the probe when changing the HTTP port/path or disabling health. Monitoring requires a separate listener and credentials. English and Chinese Grafana dashboards and rules are under `/usr/share/nacket/monitoring`.

Executables are `/usr/local/bin/nacket` and `/usr/local/bin/nacket-schema`. The latter emits configuration schema or the `communication` contract. `/usr/share/nacket/build-manifest.json` records actual inputs and executable digests. `dependencies.json` and `/usr/share/licenses/nacket` contain resolved dependency and licensing material. English and Chinese product documentation is under `/usr/share/doc/nacket`.

```sh
docker stop --time 25 nacket
```

Shutdown closes new admission before draining within original deadlines. Forced termination can leave unknown outcomes. Logs are neither business history nor delivery acknowledgement. Broker acceptance does not establish client receipt or business processing.

## Manual publication

The `Build` workflow uses native amd64 and arm64 runners and retains `image-amd64`, `image-arm64`, and a per-platform `image-identity-<arch>.json`. Images bind the product version, source commit, and Cargo lockfile. Upload does not rebuild them.

Configure repository Variable `DOCKERHUB_USERNAME=nasaruntime` and Secret `DOCKERHUB_TOKEN` with push and repository-overview permission. Manually dispatch `Publish Docker image` on `release`, supplying its successful `build_run_id` and the two `configDigest` values from `image-identity-<arch>.json`.

The workflow requires master, release, and the matching version Git tag to identify the same source, with a successful Build. It revalidates archive identity before uploading `nasaruntime/nacket:1.0.0`. An existing version may only be continued with identical content; different content cannot replace it. The workflow reads back platforms, configuration digests, and the Docker Hub English overview before updating `latest`. The product uses MIT OR Apache-2.0; dependency notices retain their own licenses.
