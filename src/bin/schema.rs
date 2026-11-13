/// 业务作用：输出当前配置或通信结构合同，供部署编辑器、SDK 和业务服务核对字段与分支。
/// 参数说明: 无。
///
/// 返回：向标准输出写入完整文档；编码或输出失败以非零状态结束，不启动服务和网络组件。
fn main() -> anyhow::Result<()> {
    use std::io::Write;
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    let schema = match args.as_slice() {
        [] => serde_json::to_value(nacket::config::schema::configuration())?,
        [name] if name == "communication" => nacket::contracts::communication(),
        _ => anyhow::bail!("usage: nacket-schema [communication]"),
    };
    let mut output = std::io::stdout().lock();
    serde_json::to_writer_pretty(&mut output, &schema)?;
    output.write_all(b"\n")?;
    Ok(())
}
