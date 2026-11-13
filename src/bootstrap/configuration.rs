/// 业务作用：在启动前固定严格 YAML 来源与观察策略。
/// 参数说明: 无。
///
/// 返回：naml 加载器；非法路径、环境或来源声明阻止启动。
pub fn configuration() -> nasa::yml::strict::Result<nasa::yml::strict::ConfigLoader> {
    nasa::yml::strict::ConfigLoader::standard()
}
