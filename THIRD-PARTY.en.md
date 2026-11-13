# Third-party components

[中文](THIRD-PARTY.md)

nacket uses general NASA Runtime components and the Rust and native dependencies pinned in Cargo.lock.
The product is licensed under MIT OR Apache-2.0; dependencies retain their respective licenses.

NASA Runtime and other Rust dependencies come from crates.io. Cargo.lock pins their versions, sources, and archive checksums. Building requires no adjacent checkout, Git dependency, or local path override. Product source does not embed framework implementations.

The image's `/usr/share/nacket/dependencies.json` records the resolved packages, versions, sources,
declared licenses and dependency graph. `/usr/share/licenses/nacket/dependencies/` contains packaged
license texts and a content digest inventory. License texts come from the actual registry archives. Dependencies without a packaged text are explicitly marked declared_only; no additional permission
is inferred. Operating-system copyright notices remain under Debian's `/usr/share/doc`.

Kafka uses statically built librdkafka, OpenSSL, zlib and Zstandard. The target system still supplies libc,
libgcc and libstdc++. Kafka TLS and SASL PLAIN/SCRAM are included; Kerberos/GSSAPI is outside this image's
capabilities. Trust roots use the runtime certificate directory. Deployment credentials are supplied separately
and are not embedded in image layers.
