"""绑定产品源码与 registry 锁文件后构建服务镜像。"""

import argparse
import importlib
from pathlib import Path
import subprocess

manifest = importlib.import_module("source-manifest")


def main():
    """业务作用：使用产品目录与锁定的 registry 依赖构建镜像，并拒绝构建期间的输入漂移。
    参数说明: 无。
    返回：构建指定标签；输入变化或 Docker 构建失败时返回非零状态。
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--tag", required=True)
    parser.add_argument("docker_arguments", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    product = Path(__file__).resolve().parents[1]
    before = manifest.inventory(product)
    arguments = args.docker_arguments
    if arguments[:1] == ["--"]:
        arguments = arguments[1:]
    subprocess.run(
        ["docker", "build", "--tag", args.tag, *arguments, "."],
        cwd=product,
        check=True,
    )
    # 输入变化会使标签无法代表当前源码；调用方只能在整份输入保持不变时使用产物。
    if manifest.inventory(product) != before:
        raise ValueError(
            "build inputs changed; produced image is not the current source candidate"
        )


if __name__ == "__main__":
    main()
