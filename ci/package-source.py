"""生成仅依赖 registry 的服务归档，并验证实际源码、许可证和公开文档。"""

import argparse
from pathlib import Path
import subprocess
import sys
import tarfile
import tempfile

try:
    import tomllib
except ModuleNotFoundError:
    import tomli as tomllib


def inspect_archive(product, archive):
    """业务作用：核对实际归档的许可证、双语入口、版本和规范化依赖声明。
    参数说明：
    - product：提供公开资源与元数据的服务源码根目录。
    - archive：Cargo 已完成编译验证的归档文件。
    返回：归档合同一致时正常返回；必要文件缺失、本地内容泄漏或元数据不符时失败。
    """
    with tarfile.open(archive) as package:
        contents = {}
        for member in package.getmembers():
            relative = Path(*Path(member.name).parts[1:])
            if {"tests", "target", ".git", "__pycache__"}.intersection(relative.parts):
                raise ValueError("non-product archive entry: " + member.name)
            if not member.isfile():
                raise ValueError("ordinary archive file required: " + member.name)
            contents[str(relative)] = package.extractfile(member).read()
    source = tomllib.loads((product / "Cargo.toml").read_text())
    normalized = tomllib.loads(contents["Cargo.toml"].decode())
    for name in (
        "name",
        "version",
        "license",
        "readme",
        "publish",
        "description",
        "repository",
        "homepage",
        "keywords",
        "categories",
    ):
        if normalized["package"][name] != source["package"][name]:
            raise ValueError("archive metadata differs: " + name)
    if "patch" in source or "patch" in normalized:
        raise ValueError("registry-only source must not contain dependency patches")
    for name in (
        "README.md",
        "README.en.md",
        "LICENSE-MIT",
        "LICENSE-APACHE",
        "THIRD-PARTY.md",
        "THIRD-PARTY.en.md",
        "SECURITY.md",
        "SECURITY.en.md",
        "CONTRIBUTING.md",
        "CONTRIBUTING.en.md",
        "docker/README.md",
        "docker/README.en.md",
        "docker/compose.yml",
        ".github/workflows/ci.yml",
        ".github/workflows/docker-publish.yml",
    ):
        if contents[name] != (product / name).read_bytes():
            raise ValueError("archive resource differs: " + name)
    print("Registry-source archive verified:", archive, "files:", len(contents))


def document_archive(archive, target, offline):
    """业务作用：从 Cargo 实际验证的归档内容生成严格公开库文档，拒绝缺失说明的分发包。
    参数说明：
    - archive：本次 Cargo 已生成并编译的源码归档。
    - target：当前构建及文档输出目录。
    - offline：是否禁止 Cargo 在线解析依赖。
    返回：归档字节、公开项说明及页面入口全部通过；任一步失败则阻止归档验收。
    """
    extracted = archive.with_suffix("")
    contents = {}
    with tarfile.open(archive) as package:
        for member in package.getmembers():
            relative = Path(*Path(member.name).parts[1:])
            # 只使用与本次归档逐字节相同的解包源码，旧工作树或残留页面不能代替分发内容。
            if not member.isfile() or ".." in relative.parts:
                raise ValueError("ordinary archive file required: " + member.name)
            current = extracted / relative
            content = package.extractfile(member).read()
            if current.is_symlink() or current.read_bytes() != content:
                raise ValueError("verified extraction differs: " + member.name)
            contents[relative] = content
    command = [
        "cargo",
        "rustdoc",
        "--target-dir",
        str(target),
        "--locked",
        "--lib",
        "--all-features",
    ]
    if offline:
        command.append("--offline")
    command += ["--", "-D", "warnings", "-D", "missing-docs"]
    # 规范化 manifest 不含工作区声明；建立外部隔离边界，避免输出目录的父工作区接管归档。
    with tempfile.TemporaryDirectory(prefix="archive-doc-", dir=target) as directory:
        isolated = Path(directory)
        (isolated / "Cargo.toml").write_text(
            '[workspace]\nmembers = ["source"]\nresolver = "2"\n'
        )
        (isolated / "Cargo.lock").write_bytes(contents[Path("Cargo.lock")])
        source = isolated / "source"
        for relative, content in contents.items():
            destination = source / relative
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_bytes(content)
        subprocess.run(command, cwd=source, check=True)
        subprocess.run(
            [sys.executable, str(source / "ci/verify-docs.py"), str(target / "doc")],
            cwd=source,
            check=True,
        )
    print("Strict documentation verified from archive:", archive)


def main():
    """业务作用：使用 registry 依赖完成 Cargo 归档及归档内代码编译，不执行公开上传。
    参数说明: 无。
    返回：编译、公开文档与归档合同均满足后退出；依赖不可解析、缺失说明或内容不符则失败。
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--target-dir", type=Path, default=Path("target"))
    parser.add_argument("--offline", action="store_true")
    args = parser.parse_args()
    product = Path(__file__).resolve().parents[1]
    target = args.target_dir.resolve()
    command = [
        "cargo",
        "package",
        "--target-dir",
        str(target),
        "--locked",
        "--allow-dirty",
        "--features",
        "schema",
    ]
    if args.offline:
        command.append("--offline")
    # 归档使用自身锁定的 registry 依赖实际编译，工作树中的产物不能替代归档内容。
    subprocess.run(command, cwd=product, check=True)
    data = tomllib.loads((product / "Cargo.toml").read_text())["package"]
    archive = target / "package" / (data["name"] + "-" + data["version"] + ".crate")
    inspect_archive(product, archive)
    document_archive(archive, target, args.offline)


if __name__ == "__main__":
    main()
