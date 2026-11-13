# Shutdown and persistence ownership

[简体中文](shutdown.md)

nacket uses the napp shutdown sequence and `application.shutdown_timeout_ms`. Stopping admission, closing sessions, revoking routes, stopping broker consumption, deregistering discovery records and releasing actual resources are separate steps. An unconfirmed step cannot be reported as successful.

Every product route gate shares a permanent node stop flag. Stopping the node publishes this flag before waiting for configuration, directory or data-volume locks. Existing write permits also check the flag. A packet already in progress may need to finish or close its transport; setting the flag does not release actual writer ownership. Late configuration or control responses cannot reopen the stopped node even if their persistence completes.

A bounded persistence registry retains at most 32 actual blocking tasks. Local gateway-assertion commits also have a separate limit of 16. Route commits, clustered definition preparation, control checkpoints and assertion consumption transfer capacity, data-volume references and required serialization locks to their blocking owners. HTTP timeout, session closure or cancellation of an asynchronous waiter cannot release these responsibilities early.

Route edits use a separate serialization lock. They protect affected old gates, persist the change, then publish a complete frozen snapshot. Persistent I/O does not hold the directory write lock or configuration lock used by admission and observation. Control checkpoints retain their original read order even when an awaiting future is canceled.

The data-volume owner synchronously rejects new work at shutdown and waits for actual tasks within the original application deadline. Expiry reports `socket persistence drain incomplete`. Unfinished tasks retain the volume reference and exclusive file lock; the service does not claim the commit was canceled or let another process reuse the volume early. The napp entry point uses its existing runtime shutdown behavior. Container termination grace should exceed the application budget. Recovery after forced termination reads confirmed final records and revalidates control authority and checkpoint floors.

A commit that entered the actual file write can complete after its waiter is cancelled. Restart reads the
formal ledger and uses its effective revision; stale configuration cannot overwrite a newer durable definition.
A pending temporary file is not a committed fact. Regular-file validation, atomic replacement and exclusive
volume ownership remain in force, and late persistence cannot reopen a permanently stopped node.

`nacket_runtime_state{state="persistence_jobs"}` reports actual unfinished persistence tasks, including those whose waiters were canceled. Final metrics are not guaranteed to reach an exporter after exit; nalog and the process exit result also report shutdown failures. State notices, lifecycle callbacks and broker commits retain their individual delivery boundaries. A disconnect does not prove receipt of a closing notice.
