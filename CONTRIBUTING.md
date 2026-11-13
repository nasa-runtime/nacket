# 贡献说明

[English](CONTRIBUTING.en.md)

## 构建环境

使用 `rust-toolchain.toml` 指定的 Rust 1.95.0、Python 3.11 及以上、Clang/libclang、CMake、pkg-config、Perl 和 C/C++ 工具链。NASA Runtime 与其它依赖均来自 crates.io，由 `Cargo.lock` 固定版本和校验和，无需相邻源码或路径覆盖。容器构建见[部署说明](docs/deployment.md)。

```sh
cargo fmt --package nacket --check
cargo clippy --locked --all-features --all-targets -- -D warnings
cargo build --locked --features schema --bins
cargo rustdoc --locked --lib --all-features -- -D warnings -D missing-docs
python3 ci/verify-docs.py target/doc
python3 ci/package-source.py
```

服务以源码和容器交付，`publish = false`。Cargo 归档从 registry 获取锁定依赖，不依赖工作树之外的源码。归档命令会实际编译两个可执行文件，并从归档内容生成公开库文档。

## 实现合同

路由定义、主体权限、会话许可与最终写出必须关联同一有效状态。定义变化先关闭旧准入，再等待旧责任，取得新权威并开放新定义。有限许可、消息期限、身份期限与幂等窗口不能因重试而延长。外部结果未知时不得记为成功。

业务入口保持通用：不内置特定领域对象或路由，不把业务合同移入通用框架。HTTP/MQ 输入不取得未配置的目的地或通用发送器。配置观察、管理受理与实际应用分别表达。

沿用 napp 托管的装配、资源登记、Ready 和反向关闭链，不另建应用 runtime、全局日志初始化或独立信号宿主。数据持久化与取消必须保持责任归属，不能因等待者退出释放仍在执行的写入。

源码注释用中文说明业务作用、逐项参数与返回语义；关键门禁和状态迁移说明原因及失败后果。使用标准格式，不将非空代码块或多字段初始化堆在一行。公开文档只描述当前合同，行为、配置、指标或失败语义变化时同步中英文入口与相关参考文档。

## 提交内容与许可

仅提交目标功能涉及的源码、必要配置和公开文档，不包含本地凭据、日志、构建缓存、运行数据或编辑器状态。质量用例、fixture 和故障注入工具仅本地使用、不进入公开仓库或产品归档。

贡献按项目的 [MIT](LICENSE-MIT) 或 [Apache-2.0](LICENSE-APACHE) 双许可证提供，贡献者应确认有权提交相关内容。安全问题遵循 [SECURITY.md](SECURITY.md) 的私密报告方式。
