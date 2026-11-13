# Logging configuration

[中文](logging.md)

nacket uses napp-managed nalog through `#[nasa::application("log", ...)]`. The product creates no separate logging runtime. Source `tracing` events enter the same logging component.

The product YAML defaults `log.level` to `info` and `log.path` to `/usr/local/logs/${application.name}`. With application name `nacket`, the directory resolves to `/usr/local/logs/nacket`. naml resolves the nested configuration reference.

`NACKET_LOG_LEVEL` overrides the level and `NACKET_LOG_PATH` overrides the directory. An unset path variable uses the default directory; an explicitly empty value selects console-only output for platform collection. A custom YAML document that omits or empties `log.path` also selects console-only output. That nalog fallback is distinct from the directory supplied by the product YAML.

The following configuration keeps the product path rule and supplies explicit rotation and retention limits. Mount a nonempty log path as a persistent directory writable by the container process:

```yaml
log:
  level: ${NACKET_LOG_LEVEL:info}
  path: ${NACKET_LOG_PATH:/usr/local/logs/${application.name}}
  max_file_size: 100MB
  total_size_cap: 2GB
  max_history_days: 7
  clean_history_on_start: true
  split_error_file: true
  color: false
```

| Setting | Behavior |
| --- | --- |
| `level` | nalog filter expression, default `info`; configure fixed module levels as needed |
| `path` | Directory for `info.log` and optional `error.log`; missing or empty disables file output while retaining the console |
| `max_file_size` | File rotation size; 100 MiB above, nalog default 500 MiB when omitted |
| `total_size_cap` | Archive size limit; 2 GiB above, default 30 GiB; this is not a filesystem-wide disk quota |
| `max_history_days` | Archive retention days; 7 above, default 30 |
| `clean_history_on_start` | Apply history cleanup at startup; enabled by default |
| `split_error_file` | Also write a separate `error.log`; enabled by default |
| `color` | ANSI colors in files; disabled by default. Console colors follow terminal capabilities independently |

The same naml configuration snapshot supplies logging settings. When a configuration center is enabled, managed updates can supply them centrally. napp prepares valid filters and file resources before publishing a change. Initial file creation failure prevents successful logging startup. A failed reload preserves the effective configuration and does not report the new path as available.

## Connection and disconnection logs

The two session switches are independent. Omitting the section or either property defaults that property to `true`:

```yaml
nacket:
  connectionLogging:
    connected: true
    disconnected: true
```

`connected: false` suppresses `client_connected`; `disconnected: false` suppresses `client_disconnected`. Both are INFO events and remain subject to `log.level`. These switches do not affect authentication, heartbeats, metrics, business online/offline callbacks, or cleanup. Disconnection always revokes every subscription and pending authorization on that connection; other connections remain independent.

naml reloads these product settings, including updates supplied by an enabled configuration center. Each event uses the accepted settings at that time, including for existing connections. Rejected product configuration retains the previous switches. Re-enabling logging does not replay events suppressed while disabled.

A success event follows authentication, definition confirmation, initialization, and Ready publication. Accepting TCP or upgrading WebSocket alone is not success. A disconnection event follows local permission and subscription cleanup, at most once per confirmed logical session. A confirmed session that fails initialization has only a disconnection event with `was_ready=false`. Each Socket.IO namespace session is logged separately. Handshake failures before confirmation produce neither event.

| Field | Meaning |
| --- | --- |
| `event` | `client_connected` or `client_disconnected` |
| `cluster_id`, `scope_epoch` | Session cluster and control epoch |
| `node_id`, `incarnation` | Hosting node and node instance epoch |
| `route_id`, `route_revision` | Route and definition revision frozen for this session |
| `session_id` | Server-generated exact session ID; changes on reconnect |
| `tenant_id`, `uid` | Server-verified tenant and user identity |
| `transport`, `peer_ip` | `tcp`, `websocket`, or `socketio`, and actual network peer IP; a proxy appears as the peer, and client forwarding headers are not trusted |
| `serialization` | The route's single ProtocolBytes mode for this session |
| `ready_after_ms` | Success event: monotonic time from identity session creation to Ready, including confirmation and initialization, excluding earlier network handshake and identity verification |
| `was_ready` | Disconnection event: whether this logical session published Ready; not cluster-wide user presence |
| `cleanup_path` | Disconnection event: `uid_partition` means partition consumption completed; `local_fallback` means local cleanup ran without partition consumption and does not publish a business offline notification |
| `session_duration_ms` | Disconnection event: monotonic time from identity session creation to first closure, excluding subsequent cleanup queue delays |
| `cancelled_subscriptions` | Subscription intents cancelled at first closure, including pending authorization; excludes subscriptions already cancelled or removed |
| `pending_subscriptions` | Cancelled intents that had not acquired authorization |
| `reason` | Known permission or route condition at first closure; `connection_closed` when no more specific evidence exists, without inferring voluntary exit or network failure |

Closure facts are frozen before clearing subscriptions. Repeated cleanup cannot overwrite the original count or time. Fixed reasons include `authorization_expired`, `authorization_revoked`, `session_revoked`, `state_notification_unavailable`, and the route gate's bounded reasons such as `route_closed` and `route_permit_expired`. Counts describe local subscription cleanup, not remote business deletion or delivery of an offline callback.

Shutdown, partition unavailability, or control queue rejection can select `local_fallback`. Local permissions are still revoked before logging. If a business offline callback is enabled, this path counts it as rejected. Neither connection nor disconnection logs prove remote notification delivery.

Logs exclude tokens, authentication payloads, subscription target lists, and message bodies. Identity and route text is escaped to prevent embedded newlines from splitting records. Abrupt process termination or storage failure can still lose records; logs do not replace the authorization ledger.

## Readiness state logs

The local readiness task emits `readiness_transition` on its first observation and whenever the lifecycle, aggregate result, or critical blockers change. The event is INFO when admission conditions are satisfied and WARN otherwise; `application_state` and `ready` describe that observation. Each blocker also produces the WARN event `readiness_dependency_unavailable` with `component`, `dependency`, `state`, and a fixed `reason`. Names are escaped; configuration credentials and remote error bodies are excluded.

The task observes approximately every 100 milliseconds and suppresses repeated stable states. A changed cause while unavailable is also recorded. Noncritical dependencies and Degraded states that permit service are not blockers. Observations include the framework's freshness and lease-deadline decisions; logs and metrics do not change admission gates. Contributors are read separately, and changes shorter than the observation interval may be missed. Scheduling delays or abrupt termination can also prevent records. These sampled observations do not replace request-time readiness checks or independently establish the cause of a remote failure.

## Redis control processing logs

When a Redis controller fails a processing round or reaches its deadline, it emits the WARN event `control_unavailable`, with at least 30 seconds between these records for the same controller. `stage` identifies the last control phase entered, and `timed_out` indicates whether the outer round deadline expired. During snapshot reads, `read_step` can be `serial_lock` (waiting for read serialization), `backend_snapshot` (waiting for the backend snapshot), or `persist_checkpoint` (waiting for local high-water confirmation). A successful read clears this field so later command phases do not inherit a stale read position.

The read step identifies the last position entered before cancellation or error; it does not establish a remote or disk failure. Scheduling delays can also consume the deadline. Logs exclude raw error bodies and do not extend control requests or route permits. If a checkpoint waiter is cancelled, persistence work that has already started retains read serialization until it finishes.

## Handshake rejection logs

After authentication processing begins, the first failure of a logical handshake produces the WARN event `handshake_rejected`. Native TCP/WebSocket and Socket.IO share this record. It is independent of the two INFO switches above and remains subject to nalog level filtering. Cancellation or a peer leaving before confirmation is an incomplete handshake, not proof of an authorization denial. Malformed frames rejected before authentication processing need not produce this event.

`stage` identifies identity verification, `identity_handoff`, `confirm_admission`, `quota_wait`, `initialization_hook`, `session_ready`, initial subscriptions, or Ready output/publication. `reason` contains a fixed local reason such as `authorization_response_mismatch`, `authorization_ttl`, `invalid_uid`, `user_connection_capacity`, `uid_event_incomplete`, or `session_not_ready`. Unknown errors map to bounded outcome categories; raw errors and remote response bodies are excluded. An owner disappearing without a known failure uses `handshake_abandoned`; reaching the original deadline uses `authentication_deadline`.

Available evidence supplies `attempt_id`, `node_id`, `incarnation`, `route_id`, `route_revision`, `session_id`, `tenant_id`, and `uid`. Attempt IDs appear only after request validation; principals appear only after identity verification. `same_uid_sessions_after_rejection` counts the same principal's connections in the local directory after confirmation fails, including connections being reclaimed. It is neither a cluster quota nor an atomic snapshot at the instant of rejection. `elapsed_ms` starts at authentication entry, and `deadline_elapsed` uses the original handshake deadline.

`runtime_ready` observes the application. `route_unavailable` and `route_permit_remaining_ms` observe the original frozen gate. These are separate reads, not an atomic state or authorization evidence. Without cluster leases, zero remaining permit time does not imply an unavailable route; also inspect `route_unavailable`. Successful readiness reports the configured runtime and route conditions at probe time; it cannot guarantee admission for every identity, quota, or later instant. A single user's denial must not make unrelated users globally unready. An unknown handshake failure does not gain automatic retry permission from these diagnostics.

Relative paths resolve from the process working directory. Use an absolute path for container persistence and provide directory permissions and disk space. The logging component owns flushing and resource retirement during shutdown. A normal shutdown deadline cannot guarantee complete output during a disk failure. Logs are not durable business-message storage, authorization ledgers, or replay checkpoints.
