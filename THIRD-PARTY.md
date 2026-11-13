# 第三方组件

[English](THIRD-PARTY.en.md)

nacket 使用 NASA Runtime 的通用组件，以及 Cargo.lock 锁定的 Rust 与本地编译依赖。
产品自身采用 MIT OR Apache-2.0；各依赖继续遵守其各自许可证。

NASA Runtime 及其它 Rust 依赖均从 crates.io 获取，版本、来源与归档校验和由 Cargo.lock 固定。构建不需要相邻源码、Git 依赖或本地路径覆盖；产品源码不包含框架实现。

镜像内 `/usr/share/nacket/dependencies.json` 记录实际解析的包、版本、来源、许可证声明及依赖图。
`/usr/share/licenses/nacket/dependencies/` 保存依赖包附带的许可文件与内容摘要清单。
许可文本来自实际 registry 归档；未附带许可文件的依赖在清单中明确标为 declared_only，不推断额外许可。
操作系统组件的版权文本保留在 Debian 的 `/usr/share/doc` 中。

Kafka 使用静态构建的 librdkafka、OpenSSL、zlib 和 Zstandard；目标系统仍提供 libc、libgcc 与 libstdc++。
Kafka TLS 与 SASL PLAIN/SCRAM 已编入，Kerberos/GSSAPI 不在此镜像的能力范围内。
CA 信任材料使用运行镜像提供的证书目录，业务凭据由部署者另行提供，不写入镜像层。
