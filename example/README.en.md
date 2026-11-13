# nacket browser example

[简体中文](README.md)

This consumer application uses the `nacket-sdk` package for authentication, sub/unsub, advertised notifications, readonly connection status and business REST publishing. The SDK project is `nacket-sdk-js`; this example maintains no protocol or client implementation.

Run `npm ci` for pinned build tools. Install the selected SDK archive, for example `npm install --no-save --package-lock=false /absolute/path/nacket-sdk-0.1.0.tgz socket.io-client@4.8.4`. Run `npm run build` and serve dist with a static HTTP server. Public manifests/lockfiles contain no local archive paths. A deployment using an existing registry release can pin it in its own dependencies and lockfile.

Native access uses public `/nacket`; logical endpoint and route ID are separate. For Socket.IO, enter only the origin, `/socket.io/` path and logical endpoint. The route must allow the page origin. HTTPS/WSS is the default; local plaintext requires explicit selection.

The business microservice defines the REST URL, authentication cookies, payload and operationId. After authorization it publishes with the Rust/Java server SDK through Redis Stream/Kafka for nacket delivery. The page holds no MQ or management credentials and cannot bypass membership or tenant checks.

Tokens remain in memory. At most 100 metadata rows are rendered with textContent. This page acknowledges application_processed after recording metadata; real applications should acknowledge after their own business action. The server image builds and runs independently of this example or adjacent Web projects.

The page uses SocketClient token, onMessage, sub/unsub, notify and close APIs. Operation results are directly accessible without constructing wire envelopes.
