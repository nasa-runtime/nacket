"""记录镜像实际构建输入和可执行文件的内容身份。"""

import argparse
import hashlib
import json
from pathlib import Path


PRODUCT_FILES = (
    "Cargo.toml",
    "Cargo.lock",
    "rust-toolchain.toml",
    "Dockerfile",
    ".dockerignore",
    "LICENSE-MIT",
    "LICENSE-APACHE",
    "THIRD-PARTY.md",
    "THIRD-PARTY.en.md",
    "README.md",
    "README.en.md",
    "SECURITY.md",
    "SECURITY.en.md",
    "CONTRIBUTING.md",
    "CONTRIBUTING.en.md",
    "ci/build-image.py",
    "ci/license-inventory.py",
    "ci/source-manifest.py",
)


def digest(path):
    """业务作用：按文件原始字节取得稳定身份，避免把路径或修改时间当作内容。
    参数说明：
    - path：已选定的普通文件。
    返回：SHA-256 十六进制摘要；符号链接拒绝进入构建清单。
    """
    if not path.is_file() or path.is_symlink():
        raise ValueError("ordinary build input required: " + str(path))
    return hashlib.sha256(path.read_bytes()).hexdigest()


def inventory(product):
    """业务作用：枚举 Dockerfile 消费的产品输入，以锁文件绑定 registry 依赖，不包含生成目录。
    参数说明：
    - product：nacket 源码根目录。
    返回：使用固定相对名称排序的摘要表；缺失输入立即拒绝。
    """
    files = {"nacket/" + name: digest(product / name) for name in PRODUCT_FILES}
    for directory in (
        "src",
        "zcf",
        "contracts",
        "monitoring",
        "docs",
        "deploy",
        "docker",
    ):
        for path in sorted((product / directory).rglob("*")):
            if path.is_file():
                files["nacket/" + path.relative_to(product).as_posix()] = digest(
                    path
                )
    return dict(sorted(files.items()))


def identity(files):
    """业务作用：将明确文件集压缩为可比较的构建输入身份。
    参数说明：
    - files：相对文件名与内容摘要表。
    返回：稳定 JSON 表示的 SHA-256，与目录位置无关。
    """
    return hashlib.sha256(
        json.dumps(files, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def main():
    """业务作用：在受信构建过程中记录输入，并在编译完成后核对输入及绑定产物。
    参数说明: 无。
    返回：写入清单；编译期间输入变化、已有清单不匹配或产物缺失时以失败结束。
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--product", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--binary-dir", type=Path)
    args = parser.parse_args()
    files = inventory(args.product)
    record = {
        "format": "nacket.build-inputs/1",
        "sourceHash": identity(files),
        "files": files,
    }
    if args.binary_dir:
        # 编译前后必须来自同一输入，不能在编译完成后把另一工作树的清单贴到旧产物上。
        previous = json.loads(args.output.read_text())
        if previous != record:
            raise ValueError("build inputs changed during compilation")
        record["artifacts"] = {
            name: digest(args.binary_dir / name)
            for name in ("nacket", "nacket-schema")
        }
    args.output.write_text(json.dumps(record, sort_keys=True, indent=2) + "\n")


if __name__ == "__main__":
    main()
