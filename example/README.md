# nacket 浏览器接入示例

[English](README.en.md)

本应用通过 `nacket-sdk` 包连接通用路由，演示认证、sub/unsub、白名单通知、只读连接状态和业务 REST 发送。SDK 产品工程是 `nacket-sdk-js`，示例中不维护协议或客户端源码。

执行 `npm ci` 安装固定构建工具，将选定的 SDK 归档安装到本应用，例如 `npm install --no-save --package-lock=false /absolute/path/nacket-sdk-0.1.0.tgz socket.io-client@4.8.4`。执行 `npm run build` 后由静态 HTTP 服务提供 dist。归档路径不写入公开 manifest 或锁文件；使用 registry 已发布版本的部署可以在自己的依赖和锁文件中固定版本。

原生地址为公开的 `/nacket`，逻辑 endpoint 与路由 ID 分开指定。Socket.IO 地址只填写 origin，并提供 `/socket.io/` path 和逻辑 endpoint。页面 origin 必须在该路由的允许集合中。默认 HTTPS/WSS，本机明文需要显式选择。

业务 REST 地址、认证 cookie、正文和 operationId 遵循业务微服务合同。REST 完成权限校验后通过 Rust/Java 服务端 SDK 向 Redis Stream/Kafka 发布，再由 nacket 推送。页面不持有 MQ 或管理凭据，不绕过群成员和租户校验。

token 仅保留内存；消息列表最多 100 行元信息，通过 textContent 展示。页面在成功记录元信息后确认 application_processed，实际应用应在自身业务动作完成后确认。服务端镜像构建与运行不依赖本示例或相邻 Web 工程。

页面使用 `SocketClient` 的 token、onMessage、sub/unsub、notify 与 close 入口；操作结果直接读取，无需构造线路信封。
