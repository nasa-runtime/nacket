"""归档实际依赖所附的许可文本和摘要，不从包名推断授权。"""

import hashlib
import json
from pathlib import Path
import re
import shutil
import sys


def collect(package):
    """业务作用：定位解析依赖实际附带的许可文件，保留 registry 归档中的授权材料。
    参数说明：
    - package：Cargo 解析的单个包元数据。
    返回：有限的实际许可文件集合；没有附带文件时返回空集合，不伪造文本。
    """
    root = Path(package["manifest_path"]).parent
    candidates = list(root.iterdir())
    if package["name"] in {
        "openssl-src",
        "rdkafka-sys",
        "zstd-sys",
        "libz-sys",
        "ring",
    }:
        candidates.extend(root.rglob("LICENSE*"))
        candidates.extend(root.rglob("COPYING*"))
        candidates.extend(root.rglob("NOTICE*"))
    explicit = package.get("license_file")
    if explicit:
        explicit = root / explicit
        candidates.append(explicit)
    return sorted(
        {
            path
            for path in candidates
            if path.is_file()
            and (
                path == explicit
                or re.match(
                    r"^(license|licence|copying|notice)([._-]|$)", path.name, re.I
                )
            )
        }
    )


def main():
    """业务作用：将解析依赖的声明与许可原文关联到镜像中的固定目录。
    参数说明: 无。
    返回：生成 package 清单及按摘要命名的许可文本，文件读取或写入失败使构建终止。
    """
    metadata = json.loads(Path(sys.argv[1]).read_text())
    output = Path(sys.argv[2])
    output.mkdir(parents=True, exist_ok=True)
    records = []
    for package in sorted(
        metadata["packages"], key=lambda p: (p["name"], p["version"])
    ):
        texts = []
        for source in collect(package):
            digest = hashlib.sha256(source.read_bytes()).hexdigest()
            target = output / (digest + ".txt")
            if not target.exists():
                shutil.copyfile(source, target)
            texts.append(
                {
                    "name": source.name,
                    "sha256": digest,
                    "file": target.name,
                }
            )
        records.append(
            {
                "name": package["name"],
                "version": package["version"],
                "license": package["license"],
                "source": package["source"] or "local_path",
                "status": "text_included" if texts else "declared_only",
                "texts": texts,
            }
        )
    (output / "inventory.json").write_text(
        json.dumps(records, ensure_ascii=False, indent=2) + "\n"
    )


if __name__ == "__main__":
    main()
