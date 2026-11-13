# Kafka delivery and recovery baseline

[简体中文](kafka-ingress.md)

Each socket node has an independent consumer group and selects recipients locally by route, tenant, user or subscription. Sharing one worker group across nodes would distribute records competitively. Control authority and business data use separately configured topics, permissions and resources. The service does not create business topics or reset offsets automatically.

## Source contract

Use the managed nafka client with strict headers, required DLT, the `dead_letter` invalid-record policy and `isolation.level=read_committed`. Effective DLT producer settings require `acks=all` and idempotence. Actual topic partitions must exactly match the configured count. The DLT name is the source topic plus `behavior.dead_letter_topic_suffix`.

All product data sources require `behavior.max_consume_attempts: 0`. Capacity or authorization dependency failures keep valid input unacknowledged; explicit format, source and partition-key violations enter DLT. Configure `behavior.tombstone_policy: dead_letter` so null values cannot silently advance the input position. `deliver` is also accepted because RawPayload rejects tombstones through the required invalid-record policy. Empty non-null bodies remain distinct from tombstones.

Business pushes use the managed `SingleConsumer<RawPayload>` interface. The consume call awaits the original dispatch operation without broker refetch during ordinary waiting. Native input is materialized once by nafka; product admission retains shared ownership and bounded capacity. A cancelled waiter does not restart a partially completed dispatch: repeated input uses the original position, fingerprint and retained result. Only dependency failures or consumption interruption invoke retry backoff. See [resource limits](resource-limits.en.md) for ownership and acknowledgement boundaries.

`behavior.max_record_bytes` must cover `maxBodyBytes + maxMetadataBytes + 4096`. Source and DLT topics require `cleanup.policy=delete`, disabled unclean leader election and a sufficient `max.message.bytes`. Recoverable sources also require `retention.ms=-1` and `retention.bytes=-1`. Cluster identity and topology are checked continuously. Stale evidence, missing topics or changed partition counts protect affected routes.

Bounded input pressure, progress pressure, an in-flight handler and dependency retry do not by themselves revoke the assignment. Existing work may continue only while the full assignment, its ready epoch, fresh polling and actual topic contracts remain valid. Blocked commits, DLT protection, operator pauses, lost ownership, recovery, shutdown or stale coordination evidence withdraw ingress availability. Queuing does not extend message TTLs, identity grants or control permits.

## Realtime data and durable authorization state

Push and initialization replies require `realtime`, with the current process startup identifier in the group and `auto_offset_reset=latest`. Restarting does not resume old offsets or read offline business backlog. Transient failures retain the current process's work under its original TTL and finite batch/byte budgets. See [realtime consumption boundaries](mq-delivery.en.md).

Only the separate authorization-control ingress may use `recoverable`, with a stable group and `auto_offset_reset=none`. `ingresses.<id>.recovery.initialize` must be false. Startup verifies a persistent committed position for every required partition. Missing positions, positions below the low watermark and positions beyond the high watermark reject startup. The service never silently resets to earliest or latest. This ingress restores security state, not business history.

## Group identity

The group is `nacket.data.` followed by a lowercase hexadecimal SHA-256 digest. Hash these UTF-8 fields in order, each preceded by its four-byte big-endian byte length:

1. `nacket.clusterId`
2. `nacket.scopeEpoch`
3. `nacket.nodeId`
4. The ingress Kafka `sourceRef`

Realtime ingresses append startupId after these four fields with the same length prefix. Stable authorization-control ingresses omit it. The two modes cannot share a managed source. All topics in one source share this group; topic names are excluded from the digest. Use byte lengths, not character counts, with no separator. Logical nodes have distinct nodeIds. Restarting preserves node identity, data volume and control baseline, but creates a new realtime data group.

## Authorization-control provisioning and recovery

The following steps apply only to authorization-control ingresses. Realtime business ingresses do not provision old committed offsets.

Compute the group for each logical node and source. Create and configure all source and DLT topics. While the group has no running instance, use trusted administration tools to commit explicit initial offsets for every required partition. Earliest selects all retained input. Latest is appropriate only for initial admission after the business state snapshot is complete and ignoring earlier input is intentional. Failure recovery is not fresh admission.

With Kafka administration tools, inspect the exact group and partitions using `--describe`, then review the candidate `--reset-offsets` operation before `--execute`. Every topic and partition in the source requires a baseline. The service validates these facts and does not perform the administrative writes during startup.

Missing commits, deleted groups, retention gaps or recreated topics keep the source closed. Recovery first fences old instances and establishes business state and the replay range, then restores a complete trusted baseline. A new scopeEpoch requires coordinated control, producer, node and business reconstruction. Changing YAML alone cannot erase existing recovery responsibility.

Push records carry raw ProtocolBytes payloads and strict `nacket-meta` headers. See [publication](publishing.en.md) for signatures, partition keys, serialization and receipts, and [resource limits](resource-limits.en.md) for ownership budgets.

## Topic deployment contract

`ingresses.<id>.transport.topicContract` and `publishers.<id>.transport.topicContract` may declare `replicationFactor`, `minInSyncReplicas`, `retentionMs` and `retentionBytes`. `ingresses.<id>.deadLetterContract` independently constrains the DLT. This is a contract fragment; provision topics with these values before starting the service:

```yaml
topicContract:
  replicationFactor: 3
  minInSyncReplicas: 2
  retentionMs: 3600000
  retentionBytes: 1073741824
```

Configured values must exactly match broker facts, not an inferred minimum. All partitions must have the same replica count. Minimum ISR must be positive and no greater than replicas; retention accepts positive values or -1. Omitted fields still validate broker values but do not choose resilience or disk budgets for the deployment. Production deployments should declare explicit expectations. Consumers verify at startup and continuously; publishers verify during assembly. Source and DLT are independent contracts. Recoverable authorization sources require both retention values to be -1 on both source and DLT; finite retention is rejected.

`ingressProcessing.maxKeyPendingRecords` controls Redis prefetch. Kafka uses framework `kafkas.<source>.behavior.max_key_pending_records`, `max_key_pending_bytes` and related limits, with `max_in_flight_handlers` bounding concurrent execution. `consumer.concurrency` counts owner slots, not concurrent business keys within a partition.
