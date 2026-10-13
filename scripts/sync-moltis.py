#!/usr/bin/env python3
"""Synchronize the native adapter while leaving host configuration untouched."""
import argparse
import hashlib
import json
from pathlib import Path
import shutil


def files(root):
    result = [root / "Cargo.toml"]
    for directory in ("src", "migrations"):
        result.extend(p for p in (root / directory).rglob("*") if p.is_file())
    if any(p.is_symlink() for p in result):
        raise ValueError("adapter symlinks are unsupported")
    return {str(p.relative_to(root)): p for p in sorted(result)}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("moltis_checkout", type=Path)
    parser.add_argument("--direction", choices=("check", "into-moltis", "from-moltis"), default="check")
    args = parser.parse_args()
    checkout = args.moltis_checkout.resolve()
    if not (checkout / "crates/channels/Cargo.toml").is_file() or not (checkout / "crates/cli/Cargo.toml").is_file():
        raise SystemExit("Expected an existing Moltis workspace")
    packaged = Path(__file__).resolve().parent.parent / "moltis"
    installed = checkout / "crates/vk"
    if installed.is_symlink() or not installed.is_dir():
        raise SystemExit("Expected the fork's existing crates/vk; see COMPATIBILITY.md for core integration")
    source, target = (installed, packaged) if args.direction == "from-moltis" else (packaged, installed)
    src, dst = files(source), files(target)
    missing = sorted(set(src) - set(dst))
    extras = sorted(set(dst) - set(src))
    changed = sorted(name for name in src.keys() & dst.keys() if src[name].read_bytes() != dst[name].read_bytes())
    if args.direction == "check":
        if missing or extras or changed:
            raise SystemExit(json.dumps({"missing": missing, "extra": extras, "changed": changed}))
    else:
        if extras:
            raise SystemExit("Refusing to delete additional adapter files: " + ", ".join(extras))
        for name, path in src.items():
            destination = target / name
            destination.parent.mkdir(parents=True, exist_ok=True)
            if destination.is_symlink():
                raise SystemExit("Refusing to replace an adapter symlink")
            shutil.copy2(path, destination)
    digest = hashlib.sha256()
    for name, path in sorted(src.items()):
        digest.update(name.encode() + b"\0" + path.read_bytes() + b"\0")
    print(json.dumps({"matched": True, "adapter_sha256": digest.hexdigest(), "files": len(src)}))


if __name__ == "__main__":
    main()
