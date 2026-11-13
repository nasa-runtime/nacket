# Security

[中文](SECURITY.md)

## Trust boundaries

nacket manages persistent connections, route permissions, and trusted delivery. It does not replace business identity, membership storage, or history services. There are no active routes by default; client forwarding and management actions are disabled. Deployments must explicitly define identity, subscriptions, trusted producers, and control authority.

Files, Nacos configuration, management identities, and control MQ are privileged inputs. Restrict file writes, configuration accounts, and broker ACLs. Never distribute control administrator secrets, node keys, or producer signing keys to clients. Redis/Kafka ACL identity requires a genuinely isolated producer ingress; shared write access requires constrained signed producer identities.

Remote identity and authorization use fixed deployment-approved endpoints. Clients cannot choose URLs, service credentials, or publication destinations. Timeouts, unknown outcomes, mismatched correlation, invalid deadlines, and unavailable durable authority grant no permission. A group ID does not establish membership, and successful readiness grants no business authorization.

## Networks and credentials

- Business listeners accept plaintext TCP and WebSocket Upgrade. They do not terminate TLS or implement PROXY protocol. Use a trusted TLS gateway and protect its connection to the service; source admission uses the real socket peer.
- Health HTTP listens separately on port 3031 by default. Management actions require explicit enablement, identities, and real-peer restrictions. Keep management off public networks.
- Monitoring is disabled by default. Enable a separate listener, authentication, and network policy. Metrics and logs can contain deployment, route, session, and subject identifiers; restrict access and retention.
- Keep JWT keys, Bearer credentials, gateway assertions, broker passwords, and full business bodies out of public configuration, Issues, image layers, build logs, and downloadable public artifacts. Example environment variables designate credential inputs only.
- Containers run as UID/GID `10001:10001`. Mount configuration read-only and give data and file logs separate writable volumes. A read-only container filesystem does not prevent a host administrator from changing mounted inputs.

## Durable state and revocation

Use a stable `nodeId` and an exclusive data volume for each logical node. Two instances must not share one volume. Local control supplies no cross-node consistency; clusters require a durable control backend, node identities, and the correct scope.

When control state, revocation floors, or a volume is lost, do not initialize an empty baseline to reopen the old scope. Fence old instances, restore durable facts, and confirm fresh permits. Configuration acceptance, route Applied, broker acceptance, completed writes, and business processing are distinct stages.

Revocation closes future permissions and waits for applicable write barriers; it does not retract completed network writes. Connection facts are not a durable outbox, so a crash can lose notifications; business presence requires leases or reconciliation. Realtime business ingress does not replay offline backlog. Applications own history and replay.

## Private reporting

Prefer [GitHub Private Vulnerability Reporting](https://github.com/nasa-runtime/nacket/security/advisories/new). If unavailable, establish contact through a private channel supplied by the repository owner. Public Issues should contain only redacted stable error categories, never credentials, full configuration, exploit materials, business bodies, or unredacted captures.

Include the affected commit or product version, entry point, trust assumptions, observable consequences, and minimal reproduction steps without sensitive data. No fixed response time or maintenance period is promised.
