# Contributing

[中文](CONTRIBUTING.md)

## Build environment

Use Rust 1.95.0 from `rust-toolchain.toml`, Python 3.11 or later, Clang/libclang, CMake, pkg-config, Perl, and a C/C++ toolchain. NASA Runtime and other dependencies come from crates.io, with versions and checksums pinned by `Cargo.lock`. No adjacent checkout or path override is required. See [deployment](docs/deployment.en.md) for container builds.

```sh
cargo fmt --package nacket --check
cargo clippy --locked --all-features --all-targets -- -D warnings
cargo build --locked --features schema --bins
cargo rustdoc --locked --lib --all-features -- -D warnings -D missing-docs
python3 ci/verify-docs.py target/doc
python3 ci/package-source.py
```

The service is distributed as source and containers with `publish = false`. Its Cargo archive resolves locked registry dependencies without source outside the checkout. The archive command compiles both executables and generates public library documentation from the packaged contents.

## Implementation contracts

Route definitions, subject authorization, session permits, and final writes must refer to the same valid state. Definition changes close old admission, wait for old responsibility, obtain fresh authority, and open the new definition. Retries must not extend finite permits, message expiry, identity expiry, or idempotency windows. Unknown external outcomes must not be recorded as success.

Keep business interfaces generic: do not embed domain objects or routes, or move application contracts into the general framework. HTTP/MQ inputs receive no unconfigured destinations or general sender. Configuration observation, management acceptance, and actual application remain separate facts.

Use napp-managed assembly, resource registration, Ready, and reverse shutdown. Do not add a second application runtime, global logger initialization, or independent signal host. Persistence and cancellation must retain responsibility; a cancelled waiter cannot release a write that is still executing.

Source comments use Chinese to explain business responsibilities, individual parameters, and return semantics. Explain the reason and failure consequences of important gates and transitions. Use standard formatting with expanded nonempty blocks and multi-field initializers. Public documentation describes current contracts. Update both languages and relevant references when behavior, configuration, metrics, or failure semantics change.

## Changes and licensing

Include only relevant source, necessary configuration, and public documentation. Exclude local credentials, logs, build caches, runtime data, and editor state. Quality cases, fixtures, and fault-injection tools are for local use only and do not enter public repositories or product archives.

Contributions use the project's dual [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE) license. Contributors must have the right to provide their changes. Follow [SECURITY.en.md](SECURITY.en.md) for private security reporting.
