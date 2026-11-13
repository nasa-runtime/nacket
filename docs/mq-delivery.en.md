# Realtime consumption boundaries

[简体中文](mq-delivery.md)

Business push and initialization replies consume only records after the current process establishes its consumption baseline. Restarting after an hour does not read, decode or fan out that hour of business backlog. Business REST APIs own history and gap recovery. Clients obtain that history using business cursors after authentication and subscription succeed.

## Redis Stream

`purpose: push` and `purpose: initialization_reply` require `delivery: realtime` and `recovery.initialize: false`. When consumption actually starts, the service captures the Stream's current `last-generated-id` and logical position, then reads bounded batches using concrete IDs. Records accumulated during startup preparation are skipped too.

The service establishes a baseline at the tail when consumption actually begins, without proactively replaying existing business history, resuming old groups, or claiming old PEL entries. If the Stream does not exist yet, its valid initial tail can be `0-0`. During normal operation it retains a concrete cursor: using `$` on every read would lose records between reads. Ordinary connection errors withdraw that source's dependency eligibility while retaining the cursor and process responsibility, with bounded backoff. Any `SC_REALTIME_*` read or baseline-check error, including trimming gaps, baseline regression, or records exceeding script boundaries, closes all local route gates and fails the critical consumer task, initiating managed shutdown. Its impact is not limited to that source. Never advance the cursor to conceal a gap. Restore the broker and original resource contract, retain nodeId and the data volume, then restart and await durable control recovery. The new process establishes its realtime baseline at the tail when consumption begins; it does not replay downtime or unfinished intervals. Business history or reconciliation must recover needed data.

## Kafka

Business sources require `auto_offset_reset: latest`. The group digest includes clusterId, scopeEpoch, nodeId, sourceRef and a new startupId on every process start. Reusing a nodeId does not resume the previous process's committed offsets. Setting latest on an old group alone would not provide this guarantee.

Every node consumes independently and filters eligible local recipients. Nodes do not compete in a shared worker group. Reassignment and transient failures retain the current process's progress; old assignments cannot commit. Broker group retention cleans up inactive realtime groups; the service does not scan or delete other groups. See [Kafka ingress](kafka-ingress.en.md) for source requirements.

## Capacity and time

Realtime consumption does not imply unlimited throughput. Read reservations, batches, concurrent fanout, candidate references, session queues and slow-consumer policies remain bounded. Messages retain their original integer `createdAt` and `expiresAt`; retries, queuing and reconnection never extend TTL. An outbox publishing an older, still-valid business command after the node restarts creates a new broker input; message identity, original TTL and current authorization still apply.

## Security control state

Route definitions, offline state, incarnation fences and authorization floors are not business history. They must recover from control authority before routes open. The separate `purpose: authorization_control` ingress still requires `delivery: recoverable`, stable groups and explicit baselines. Skipping business backlog must not erase revocations. This ingress has independent capacity, permissions and original command deadlines; it does not replay historical payloads to clients.

Configuring business push or initialization replies as `recoverable` rejects startup or the configuration candidate. Authorization control and realtime data use separate sources. Hot configuration cannot switch group semantics, and old data progress does not become a new node's history recovery cursor.

Kafka DltPending retains the valid assignment and original-record responsibility while isolation is in flight. One rejected record in this state does not close other valid sessions. Persistent DLT failures, DLT capacity protection, commit failures, lost ownership and hard pauses still revoke dependency eligibility; commits cannot pass the original record before successful isolation.

After a Kafka authorization command is handed to durable control authority, its original record remains unacknowledged until the submission outcome is known. The managed consumer group may enter Retrying during that wait. The strict napp readiness policy then makes `/readyz` return 503 temporarily and rejects new client authentication. This neither confirms the record nor requires every existing session to close: existing sessions remain subject to their route permits, authorization and write gates. Clients should start a new connection after the node becomes Ready. Load balancing and monitoring should distinguish temporary traffic withdrawal from process termination; refreshing message deadlines or ignoring revocation progress cannot preserve readiness.

When a Kafka control transaction reports a terminal failure, or the control topic contract cannot be verified, its critical managed task triggers process shutdown. The orchestrator should restart the instance after the broker recovers, preserving its nodeId and data volume, and wait for control baseline recovery and a successful `/readyz`. A process that remains alive still cannot bypass protection gates. Clients must authenticate and subscribe again. Expired outbox records must be rejected; their timestamps cannot be refreshed for retry.

## Per-key processing and continuous completion

Redis business pushes use bounded queues keyed by canonical `ordering_key`. At most one task runs per key; unrelated keys share `maxParallelFanouts` workers. Parsed metadata and the original body digest stay with the responsibility, while the body retains shared Bytes ownership. Retries do not decode the business body again. `maxKeyPendingRecords` bounds per-key prefetch, and `maxPendingRecords` bounds unreleased node responsibility, including completed records after an unresolved gap.

Prefetch and continuous-completion positions are distinct. Completion advances only after every earlier record has a local decision or confirmed isolation. Other keys can finish while one key is slow, without acknowledging across the unresolved source position. Conservative read reservations remain until the batch's raw input is released. Key, record or byte exhaustion pauses reads. A physical stream cannot skip unread hot-key records, so exhausted capacity backpressures the source as a whole.

Kafka realtime pushes and initialization replies select nafka `KeyOrdered`: different keys in a partition may run concurrently, while the same key remains ordered. Initialization replies still use their correlation keys. Dedicated authorization-control sources remain PartitionOrdered. Framework worker, per-key record/byte, partition, source, process and contiguous-commit limits remain effective. Full key capacity may backpressure the source; no unlimited bypass of old commit gaps is allowed.

Business delivery ends at the earlier of original expiresAt and `createdAt + maxRetryAgeMs`; the default retry age is 60000 milliseconds. Checks run before body handoff reservations and during actual dispatch, so capacity shortage cannot retain expired business responsibility indefinitely. Redis also bounds local ownership time. Timeout and invalid records require confirmed isolation; isolation failure withdraws source availability. Security-control commands retain separate recovery and expiry semantics and are not discarded by the business-age policy.
