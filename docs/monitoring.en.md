# Monitoring and metric semantics

[中文](monitoring.md)

napp installs nafana for nacket. Process resources, framework components and product metrics share one MetricHub. Monitoring uses a dedicated listener, separate from business connections and management HTTP. The default configuration disables network export while retaining internal counters.

## Enable a dedicated endpoint

Merge this fragment into application YAML. Keep the instance identity stable for the process lifetime and unique across replicas. Use a separate credential for monitoring.

```yaml
grafana:
  observability:
    enabled: true
    identity:
      service_name: nacket
      environment: production
      cluster: cluster-a
      instance_id: ${NACKET_INSTANCE_ID}
    collectors:
      enabled: true
      interval_ms: 5000
      filesystem_interval_ms: 30000
      timeout_ms: 1000
      stale_after_ms: 15000
      process: true
      cgroup: true
      filesystems:
        data: /app/data
        logs: /usr/local/logs/nacket
      network_interfaces:
        primary: eth0
      io_devices: {}
    prometheus:
      export_mode: scrape
      scrape:
        listener: dedicated
        bind: 0.0.0.0:9464
        path: /metrics
        max_concurrent_requests: 4
        request_timeout_ms: 2000
        auth:
          mode: bearer
          token: secret://metrics_read
    provisioning:
      mode: disabled
secrets:
  metrics_read:
    encoding: raw
    max_bytes: 512
    fragments:
      - env: NACKET_METRICS_TOKEN
nacket:
  metricScopes:
    enabled: true
    routeIds: [route-a]
    endpointRefs: [identity]
    ingressRefs: [source-a]
```

Use mount paths, interfaces and devices visible to the deployment. Aliases are fixed labels. Do not sum capacity for two paths on the same filesystem. Cgroup values describe the visible hierarchy; a local CPU quota may exceed an ancestor's effective limit. Unlimited resources have no finite-limit sample. Missing, unsupported, failed and stale collectors report their state instead of healthy zero values.

Restrict access to the monitoring network and provide TLS through the deployment network or proxy when needed. Listener conflicts, invalid credentials and insufficient metric capacity fail startup; there is no fallback to a business port. Monitoring identity, exporters, collector directories and detailed-name directories require a restart to change.

## Fixed names and capacity

`metricScopes.enabled` defaults to `false`. When enabled, `routeIds`, `endpointRefs` and `ingressRefs` each accept at most 32 distinct names and receive one additional `_other` aggregate. Dynamic routes can still become available; names outside the directory enter `_other` without creating new series. Duplicate names and explicit use of the reserved name are rejected. This switch changes no connection permissions.

Detailed `kind` values are `route`, `endpoint` and `ingress`. Each has its own finite operations and resources. Startup reserves the complete counter directory and all Histogram buckets, sum and count. Insufficient capacity rejects installation. If using remote write, adjust `max_batch_samples` accordingly; its default of 5000 may be insufficient for a full detailed directory.

Labels never contain user, group, session or message identifiers, credentials, arbitrary URLs or error text. Route revision and digest belong in authorized management queries. Observation does not copy payloads. Scraping reads bounded observation snapshots; route and source state are projected every second. Monitoring lag cannot extend a business permission.

## Product metric catalog

| Metric | Meaning |
| --- | --- |
| `nacket_node_info` | Persistently confirmed node_id, incarnation, role, and control_backend; one fixed set per process, associated with platform service_instance_id rather than using route scopeEpoch as process identity |
| `nacket_connection_events_total` | Physical admission, rejection before adapter identification, and closure; Socket.IO namespaces do not multiply physical connections |
| `nacket_admissions_total` | Node-wide physical, unauthenticated, peer-directory, peer-connection and peer-rate limits across all business ports |
| `nacket_live_resources` | Connections, sessions, subscriptions, calls, queues, bytes, receipts and control work held by actual owners; cancellation releases the same responsibility |
| `nacket_oldest_pending_age_seconds` / `nacket_pending_age_complete` | Age of actual pending owners by fixed resource; incomplete coverage omits age samples instead of reporting zero backlog |
| `nacket_control_snapshot` / `nacket_control_snapshot_fresh` | Nodes, routes, retained permits, operation phases and barriers from a verified durable snapshot; failed reads or expiry omit count samples |
| `nacket_control_snapshot_age_seconds` / `nacket_control_oldest_operation_age_seconds` | Local time since verification and diagnostic age derived from durable operation creation; cross-node wall time never completes a barrier |
| `nacket_control_authority_snapshot` / `nacket_control_authority_fresh` | Only the effective control owner emits durable directory counts; standby controllers and data-only nodes report zero freshness |
| `nacket_control_authority_age_seconds` / `nacket_control_authority_remaining_seconds` / `nacket_control_authority_oldest_operation_age_seconds` | Authority observation age, finite evidence remaining, and oldest pending operation age; omitted after ownership loss, failure, or expiry |
| `nacket_control_local_routes` / `nacket_control_permit_min_remaining_seconds` | Locally desired online routes requiring cluster permits and their minimum remaining permit lifetime; closed or unauthorized gates report zero, no matching routes omit the minimum |
| `nacket_runtime_state` | Online/protected/offline routes, actual writers, required dependencies, listeners, capacities, readiness, pending restart and persistence jobs |
| `nacket_operations_total` | Outcomes of first executions; cached result reads do not count as new execution |
| `nacket_identity_checks_total` / `nacket_authorization_checks_total` | Identity and authorization outcomes; business denial is separate from transport unavailability |
| `nacket_request_records_total` | Initial requests, duplicate association, identity conflicts and result-body cache capacity |
| `nacket_mq_consume_total` | Actual consumption attempts, including retries; summing across nodes does not count unique business messages |
| `nacket_mq_publish_total` | Publication outcomes by purpose; a broker commit does not prove terminal receipt |
| `nacket_deliveries_total` | Separate `admitted`, `queued`, `written`, `received` and `processed` stages; a complete write proves only local transport completion |
| `nacket_receipt_events_total` | Valid acknowledgements, rejection, cancellation and expiry of server-issued tickets; not evidence that a person read a message |
| `nacket_receipt_inputs_total` | Correlation checks for each terminal receipt: accepted, rejected, invalid or unknown. Unknown means no current valid ticket, including late, repeated or another session's tickets; it does not create another delivery outcome |
| `nacket_queue_events_total` | Queue rejection, replacement, dropping and cancellation; logical byte occupancy is in resource metrics |
| `nacket_lifecycle_events_total` | Admission and publication of exact connection facts; not a unique online-user count |
| `nacket_control_events_total` / `nacket_configuration_events_total` | Durable control requests, configuration admission and restart requirements; admission does not complete a global barrier |
| `nacket_transport_bytes_total` / `nacket_transport_frames_total` | Complete application-visible protocol messages; failed writes count attempts without completed bytes; kernel, TLS and partial-frame bytes are excluded |
| `nacket_heartbeat_events_total` | Valid native and Socket.IO heartbeat replies and deadlines; excludes lower-level WS control frames |
| `nacket_payloads_total` / `nacket_payload_bytes_total` | Encoded bodies entering business processing; not evidence of schema or authorization acceptance; retries count actual attempts |
| `nacket_payload_copies_total` / `nacket_payload_copy_bytes_total` | Explicit product encoding allocations and copies; excludes shared Bytes clones, framework native input materialization and copies inside network libraries |
| `nacket_payload_deserializations_total` / `nacket_payload_deserialization_bytes_total` | Subscription decoding, operation conversion and explicit body validation, including failed attempts; fixed control JSON is excluded |
| `nacket_duration_seconds` | Local monotonic durations for handshake, initialization, authorization, operations, queueing, writing, receipts, broker commits, control, local route preparation, configuration, persistence drain, heartbeat and lifecycle |
| `nacket_scope_events_total` / `nacket_scope_duration_seconds` | Fixed-name operation outcomes and latency; HTTP includes capacity waits; transport and structural success do not imply business permission |
| `nacket_scope_resources` | Route sessions/subscriptions/inflight/state/writers, endpoint inflight requests, ingress processing attempts and availability; authorization-control ingress also requires valid execution progress |

Counters accumulate from process start; use `rate` or `increase`. Histogram boundaries are 1/5/10/25/50/100/250/500 ms and 1/2.5/5/10/30 s. `queue_wait` covers batches that reached the writer. `receipt` starts when the actual write becomes eligible. `heartbeat_rtt` includes local queueing, network and terminal response. `route_transition` measures local preparation rather than global convergence; `shutdown` measures waiting for persistence owners rather than the entire process exit.

Pending age covers operations, subscription requests, outbox, writers, receipt tickets, HTTP, control requests, authorization handoff, asynchronous Kafka dispatch handoff, publication, lifecycle facts and initialization. Age starts when the actual responsibility is acquired. Transfer from queue to writer starts the new stage's age. Ticket age includes queueing; the completed receipt Histogram still starts at the actual write. Completed caches retain capacity without accumulating pending age. Authorization retries retain the first age.

Each resource has 256 fixed atomic time slots with one-second quantization. The reported oldest age is a conservative upper bound with less than one second of error. Slot exhaustion or bounded atomic contention retains an unknown share: completeness becomes zero and age is omitted until all unknown owners finish. There is no per-message time index, and observation capacity never rejects business work. These values describe locally admitted responsibility, not Redis PEL, unconsumed Kafka records or a business service's outbox backlog.

Cluster control counts come from the verified ledger. Replicas observe the same authority and must not be summed. `barrier_nodes` deduplicates stable nodes still needed for preparation, application or old-permit barriers; it is not a count of offline nodes. `permits_retained` includes old permits awaiting lawful release, not just valid serving permits. Snapshot freshness uses configured `permitTtlMs`; a failed read invalidates it immediately. Stale counts remain NoData while snapshot age remains visible. Local mode emits no cluster control samples; a controller-only node emits no local serving-permit minimum.

`control_authority_*` is separate from node snapshot observations. Redis evidence lasts at most five seconds from the start of an actual owner acquisition or renewal request, minus the clock margin. Kafka must read a committed transaction written by this process startup and recheck the same assignment epoch during export. Kafka evidence lasts at most five seconds and never exceeds the control request deadline. Uncommitted candidates, standby reads, and caches retained after failure cannot emit authority counts. Metrics neither renew nor replace actual control permissions.

During takeover, a monitoring system can retain samples from both the previous and current owner. Do not sum these samples. The cluster directory panel displays values only when exactly one fresh authority observation exists in that monitoring cluster; zero or multiple observations remain NoData and trigger separate alerts. Per-instance observations remain available for diagnosis. The monitoring `cluster` must identify one independent control scope, rather than combining unrelated authorities.

`napp_process_*`, `napp_cgroup_*`, `napp_filesystem_*` and `napp_network_*` cover CPU, memory, descriptors, threads, faults, storage IO, cgroup limits/pressure/OOM, filesystem capacity/inodes and allowed interfaces. CPU counters use microseconds; divide by 1,000,000 to obtain seconds. `napp_resource_collection_*` exposes availability, state, errors and last-success time. Framework HTTP, pools, Kafka, Redis, discovery and exporter metrics retain their original names and measurement boundaries.

## MQ positions and pending responsibility

Active broker observation is disabled by default. When enabled, independent managed tasks reuse existing clients. `/metrics` reads memory only, and consumers do not wait for sampling:

```yaml
nacket:
  brokerMetrics:
    enabled: true
    intervalMs: 5000
    timeoutMs: 1000
    staleAfterMs: 15000
    maxInFlight: 4
```

Enabled ingresses have fixed sampling intervals with staggered starts. Capacity exhaustion skips the attempt and reports unknown. Timeout withdraws the sample immediately while retaining its sampling permit until the original request returns; late results cannot refresh evidence. A pure controller does not create data-ingress observations. These settings require a restart, and observation failures do not change business permissions.

| Metric | Meaning |
| --- | --- |
| `nacket_broker_observation_fresh` | 1 only when all required sources in the bucket have complete, unexpired evidence; otherwise 0 |
| `nacket_broker_observation_age_seconds` | Time since the oldest successful request start in the bucket; omitted without complete successful history |
| `nacket_broker_unread_positions` | Redis added-position distance from the consumed position, or Kafka read_committed end minus the group's committed next offset |
| `nacket_broker_pending_records` | Actual Redis recoverable-group PEL records; excludes positions not yet read |
| `nacket_broker_pending_max_idle_seconds` | Largest idle time since claim across the complete PEL; claiming again resets it, so it is not message creation age |
| `nacket_broker_observed_partitions` | Fully observed fixed Kafka partitions; missing group commits or any missing evidence cannot produce zero backlog |
| `nacket_broker_observations_total` | Independent sampling success, failure, capacity rejection, and cancellation |
| `nacket_recovery_events_total` | Actual reads, acknowledged PEL claims, broker confirmation/isolation, and Redis/Kafka temporary retries; not terminal receipt |

Position distance is not a count of unique business messages. Kafka transactions and control records occupy offsets, and independent node groups observe the same input repeatedly. Kafka samples retain the original continuous topology and assignment evidence; invalidation or a new generation immediately hides old values. Redis read-only scripts verify the original cursor or exact fence. A PEL above 128 records is not scanned without bounds or presented as complete. Missing read permissions produce unknown observations rather than a healthy empty collection.

`backend` is redis or kafka. `ingress` is `_all` when detailed names are disabled. With `metricScopes`, only approved ingress references and `_other` are used. Any missing source withdraws quantities for its entire aggregate bucket; separate named buckets remain independent. Complete fresh zero stays zero, and unknown stays NoData.

## Dashboards and alerts

Import the [English dashboard](../monitoring/dashboard.en.json) or [Chinese dashboard](../monitoring/dashboard.zh.json) and select a Prometheus datasource. Filters use `deployment_environment`, `cluster`, `service_name` and `service_instance_id`. Templates select service name `nacket`; adjust queries if renaming it. Business aggregation excludes duplicate identities and known scrape failures. Empty results remain NoData.

The scrape availability panel joins `up` by `job` and `instance` to the unique identity observed within the last five minutes. It uses the same identity filters and retains failure values during a brief outage. If a scrape coordinate has multiple recent identities, or no identity sample within that window, the panel omits it to avoid attribution to the wrong instance. Use Prometheus Targets and the raw `up` query to diagnose such targets; missing values do not indicate health.

After creating a datasource, a successful health API response only establishes backend access to Prometheus. Before opening dashboards, also confirm that the browser's loaded datasource catalog resolves its UID and name and that the browser query path returns data. Retry only an exact datasource-not-found condition within a bounded deadline by reloading the catalog. Authentication failures, other query errors and persistent absence must fail deployment instead of being ignored.

Manage Grafana plugin versions as deployment inputs. Asynchronous startup updates can terminate a plugin process serving queries and temporarily make an existing datasource unavailable. When using the plugins bundled with Grafana 13.2.1, set `GF_PLUGINS_PREINSTALL_DISABLED=true` and `GF_PLUGINS_PREINSTALL_AUTO_UPDATE=false`. Pin any additional plugins and finish installation before opening dashboards. Analytics update checks and automatic plugin installation are separate settings.

The [alert file](../monitoring/alerts.json) uses Prometheus rule-file JSON, accepted by its YAML parser. Add it to the platform's `rule_files`, validate it and reload through the platform. Rules cover protected routes, required sources, connection/byte capacity, CPU/throttling, memory/descriptors, disk/inodes, OOM, pending restart, collector gaps, identity collisions, remote failures, write failures, expired receipts and missing instances. Capacity and resource ratios require a positive denominator; absent, zero or negative capacity yields no ratio alert. Collector and inventory rules describe observation gaps. Remote-call, write and receipt failure ratios additionally require at least 100 corresponding attempts over five minutes; zero requests do not indicate failure. Customize thresholds, durations, silencing and notification routing for the deployment.

Expected-instance rules require an independent platform source that continuously exposes `platform_expected_instance_info{deployment_environment,cluster,service_name,service_instance_id}=1` and removes retired instances. Do not derive this inventory from application heartbeats or currently online Nacos instances. Missing inventory remains NoData and raises an inventory warning; these rules cannot identify a cluster that was never registered. `up` only describes the scrape target, not business readiness.

Application replicas need no Grafana management credentials. Deploy these assets through the monitoring platform. A separate nafana-controller owns its generic resources; product-specific dashboards do not belong in the framework. Suggested scrape interval is 15 s and dashboard refresh is 30 s. If both scrape and remote write are enabled, select one ingestion path to avoid counting the same instance twice.

## Performance boundaries

Forwarding within one encoding shares the body. Borrowed Kafka records require an owned copy across the asynchronous boundary; JSON mapping and result encoding materialize objects. Copy counters exclude client-library buffers, JSON object allocations, TLS, WS masking and kernel work. A zero counter at one boundary is not proof of end-to-end zero-copy behavior. Logical queue bytes are not RSS.

Unique business-message counts, unique online users, business membership, unread counts and latency across offline intervals require business authority. The service does not infer them from local fan-out, connections or subscriptions.

## Business activity and subscription-target sampling

`nacket.sessionMetrics` defaults to enabled, scanning 128 sessions per batch every 100 milliseconds with a 60000-millisecond activity window. Successful business operations and complete business downlink writes update activity. Heartbeats, connection setup and authorization refresh do not. Only currently eligible Ready sessions are counted. Distinct targets are deduplicated by `(routeId, tenantId, groupId)` within a node; this is neither subscription-relation count nor a globally deduplicated total.

Values use `nacket_runtime_state{state="..."}`, without UID or target-ID labels:

| state | Meaning |
| --- | --- |
| `sessions_active` / `sessions_idle` | Active and idle sessions observed over a complete cycle |
| `subscription_targets` | Distinct eligible targets on this node |
| `session_sample_available` | Session observation is complete; require 1 before using session totals |
| `subscription_target_sample_available` | Target budget and complete-cycle requirements hold; require 1 before using target totals |
| `session_sample_span_seconds` | Scan span in seconds |
| `session_sample_timestamp_seconds` | Completion time in Unix seconds, following Prometheus conventions |

These bounded batch observations are not an atomic node-wide snapshot. Exceeding `maxCycleMs`, `maxTargets` or `maxTargetBytes` sets the corresponding availability to 0. Retained old values are not current exact totals. Disabled sampling performs no directory scan. Capacity changes require restart; scraping never performs the scan.

Resource gauge `nacket_live_resources{resource="..."}` includes `authorization_refresh_pending`, `authorization_refresh_inflight`, `authorization_cache_entries`, `authorization_cache_bytes`, `handshake_context_bytes` and `redis_pending_records`. `nacket_authorization_cache_total{operation,outcome}` uses operations hit, miss, wait, bypass and fetched. Bypass and negative cache outcomes do not imply authorization.

## Legends and series identity

Chinese and English dashboard legends retain the dimensions returned by each PromQL query. Alongside environment,
cluster, service and instance identity, applicable panels show state, direction, interface, backend, ingress and
collector. Different backends, inputs, devices or states on one instance therefore remain distinguishable.
Quantile queries omit the already-consumed le dimension; target availability retains job and instance.

Legends describe existing results without merging distinct series or changing authority or time semantics.
When selecting several instances, use the query's actual grouping. Missing samples, stale evidence and real
zero values remain distinct according to each metric contract.
