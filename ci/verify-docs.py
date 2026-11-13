"""核对公开库文档的入口、模块链接和来源。"""

from pathlib import Path
import re
import sys


def verify(directory):
    """业务作用：拒绝二进制文档覆盖公开库入口或模块页面缺失的产物。
    参数说明：
    - directory：Cargo 生成的文档根目录。
    返回：完整公开库入口通过时正常返回；首页、来源或模块缺失时抛出错误。
    """
    directory = Path(directory)
    library = directory / "nacket"
    index = (library / "index.html").read_text()
    sidebar = (library / "sidebar-items.js").read_text()
    source = Path(__file__).resolve().parents[1] / "src/lib.rs"
    modules = re.findall(r"^pub mod ([a-z_]+);", source.read_text(), re.MULTILINE)
    if not modules or "配置驱动的长连接动态路由" not in index:
        raise ValueError("public library introduction is missing")
    if "src/nacket/lib.rs.html" not in index:
        raise ValueError("documentation source is not the public library")
    for module in modules:
        # 模块须同时出现在首页和侧栏，避免旧页面残留掩盖入口被覆盖。
        if f'"{module}"' not in sidebar or f"{module}/index.html" not in index:
            raise ValueError("public module link is missing: " + module)
        if not (library / module / "index.html").is_file():
            raise ValueError("public module page is missing: " + module)
    print("Public library documentation verified:", ", ".join(modules))


if __name__ == "__main__":
    verify(sys.argv[1])
