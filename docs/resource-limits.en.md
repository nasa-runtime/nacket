# Admission and byte budgets

[简体中文](resource-limits.md)

Shared and dedicated route listeners use one node admission controller. `nacket.transport.admission` contains startup limits that route `limitEnabled` switches cannot disable. Listener limits apply in addition.

| Setting | Default | Behavior |
| --- | ---: | --- |
| `maxPhysicalConnections` | 100000 | Physical connections across all business ports; Socket.IO namespaces share one connection |
| `maxUnauthenticated` | 256 | Connections that have not completed their first route admission; the original authentication deadline still applies |
| `maxConnectionsPerPeer` | 10000 | Physical connections from one actual TCP peer IP |
| `connectionsPerSecondPerPeer` | 1024 | Accepted connections per peer in a fixed one-second window |
| `maxTrackedPeers` | 16384 | Bounded peer directory; new peers are rejected when no entry can be reclaimed safely |
| `maxInboundBytes` | 268435456 | Shared transport receive budget, configured directly under `transport` |

Exhaustion closes the new connection. Capacity follows the actual reader and writer owners, even when the main connection handler has returned. First route admission releases only the unauthenticated slot. Fast disconnects retain the current rate window. Entries can be reclaimed only after all connections end and the window expires.

Peer identity comes from TCP, never forwarding headers or client input. Users behind one gateway or NAT share its peer limit. Configure this limit for the deployment. Route, user and tenant quotas remain independent; see [connection quotas](connection-quotas.en.md).

## Receive ownership

The product uses the optional naws shared transport budget. TCP validates the length and acquires capacity before allocating the message. A valid frame length can still be rejected when shared capacity is exhausted. Metadata and body slices share the original `Bytes`; capacity remains charged until its final view is dropped.

WebSocket retains the standard protocol implementation. Before reading, it reserves a conservative allowance for input buffers, fragment assembly and temporary copies. Backend buffers may retain their largest allocations, so this allowance follows the connection high-water mark until all actual I/O owners exit. Idle connections do not reserve a maximum-size business frame. Exported text, binary and control payloads have separate ownership charges. Socket.IO metadata retained while waiting for an attachment is also charged to this budget. Retained buffers after a large message can cause subsequent admission to reach capacity earlier.

WebSocket handshake input is limited to 64 KiB. A complete business-message window permits at most `2 × maxFrameBytes + 64 KiB` wire bytes, including fragment headers and interleaved control frames. Control frames do not reset the active assembly window. Exhaustion, assembly timeout or cancellation during a receive ends the connection; partial input is never retried.

This is a transport ownership budget, not an RSS limit. Kernel buffers, thread stacks, allocator overhead, HTTP/MQ client caches and outbound buffers are outside `maxInboundBytes`. Container memory limits and component capacities remain necessary. Outbound queues, operations, request results, initialization, publication and authorization control have separate count or byte limits.

## Observation

`nacket_admissions_total{scope,outcome}` distinguishes physical, unauthenticated, peer-directory, peer-connection and peer-rate rejection without IP labels. The `physical_connections` and `unauthenticated_connections` resources in `nacket_live_resources` follow actual owners.

`nacket_runtime_state{state}` includes `inbound_bytes`, `inbound_byte_capacity`, `physical_connection_capacity` and `unauthenticated_connection_capacity`. nafana exports these through the independent monitoring port. Sampled values never grant permission.

## MQ dispatch budgets

`nacket.ingressProcessing` contains fixed node capacities:

| Setting | Default | Responsibility |
| --- | ---: | --- |
| `maxPendingRecords` | 256 | Kafka handoffs and results still awaiting their consume callback |
| `maxRetainedBytes` | 67108864 | Shared Kafka and Redis payloads, retained until all outbound references end |
| `maxReadBytes` | 134217728 | Total Redis data-response reservations |
| `maxControlReadBytes` | 8388608 | Separate Redis pools for initialization replies and authorization control |
| `maxIsolationBytes` | 67108864 | Independent Redis realtime isolation command and payload reservations |
| `maxParallelFanouts` | 4 | Active dispatch operations across Redis and Kafka |
| `maxCandidateReferences` | 1000000 | Total references in active session snapshots |
| `fanoutBatchSize` | 256 | Candidates per batch before yielding and checking the original deadline and route gate |

Kafka uses nafka's `SingleConsumer<RawPayload>` to await dispatch inside the asynchronous consume call. Ordinary waiting does not trigger broker backoff or refetch. Work is associated with ingress, topic, partition and offset. If the consume waiter is cancelled, the original managed task retains execution responsibility; repeated input reuses that task or its result. The framework materializes the native body once. The product shares its owner without decoding business objects or creating another body copy. Retained bodies keep both framework input permits and product byte reservations alive.

Unfinished work remains unacknowledged at shutdown. All such sources require `behavior.max_consume_attempts: 0`, so dependency failures cannot exhaust ordinary retry attempts and prematurely isolate a valid record. Explicit invalid-record isolation remains active. Callback completion still passes the framework's assignment and contiguous commit gates. A completed local decision does not confirm terminal receipt or application processing.

Redis reserves a complete batch before reading, at four times its maximum raw-record cost, and reduces batch size to fit the pool. Data, initialization replies and authorization control have separate pools. Lua checks record size and at most 128 field elements before returning payloads. Oversized new records do not advance group progress or create PEL entries; oversized existing PEL records are not reclaimed. Such records remain in place and cannot be silently skipped. Recoverable ingress retains group and PEL responsibility. A read returning `SC_RECOVERY_*`, including oversized records, or a realtime read or baseline check returning `SC_REALTIME_*`, closes every local route gate, fails the critical task, and initiates managed shutdown. Restore the original resource contract before restarting. The restarted realtime ingress begins at its new actual tail; business history or reconstruction must cover unfinished intervals rather than relying on the previous cursor for replay.

Redis fields preserve order, duplicates and binary names. Dispatch shares the original payload allocation while failure retains raw evidence. Realtime DLT commands acquire separate capacity before construction and must commit before cursor progress. Recoverable isolation and acknowledgment execute in one Redis hash-slot script.

nafka continues to own Kafka native queues, process/source/group/partition limits and the DLT pipeline. Configure `behavior.max_process_pending_bytes`, `max_source_pending_bytes`, `max_group_pending_bytes`, `max_partition_pending_bytes`, `dlt_queue_max_bytes` and producer lanes for the deployment. librdkafka caches are outside the product payload budget.

## Remote-call budgets

`nacket.remoteCalls` defaults to `maxInFlight: 256`, `maxBytes: 268435456` and `maxRequestBytes: 1048576`. Identity, authorization, actions, lifecycle callbacks and public-key reads share these limits. Each frozen endpoint also retains its own `call.maxInFlight` limit.

Requests first undergo a JSON length check without allocating the encoded body. Construction then reserves 32 times the request, mapping allowance and maximum response length, plus 4096 bytes. This conservative allowance covers overlapping control objects, mappings, encoded buffers and parsed representations; it is not RSS. Public-key documents have a fixed 64 KiB limit while sharing the node call and buffer pools. Raw business bodies retain their existing `Bytes` ownership.

Waiting for node, endpoint or byte capacity consumes the original call deadline. Failure before handoff to the HTTP executor has effect `none`; lost responses or timeout after handoff have effect `unknown`. Capacity never resets deadlines, grants authority or retries business side effects automatically. Fixed resources in `nacket_live_resources` expose remote calls, remote bytes and MQ capacities in use.
