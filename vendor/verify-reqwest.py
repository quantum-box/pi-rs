#!/usr/bin/env python3
"""Verify pinned upstream archives and exact, reviewable vendor patches."""
import hashlib
import io
import os
from pathlib import Path, PurePosixPath
import subprocess
import tarfile
import tempfile
import tomllib
import urllib.request

ROOT = Path(__file__).resolve().parent
VERSIONS = {
    "0.12.28": "eddd3ca559203180a307f12d114c268abf583f59b03cb906fd0b3ff8646c1147",
    "0.13.5": "16a1cfa75cc186dd73d5818e510e042e40927bccc9c236b061cea97e1eb08029",
}

with (ROOT / "reqwest-audit.lock").open("rb") as file:
    packages = tomllib.load(file)["package"]
expected = {(version, checksum) for version, checksum in VERSIONS.items()}
actual = {(p["version"], p.get("checksum")) for p in packages if p["name"] == "reqwest" and p.get("source") == "registry+https://github.com/rust-lang/crates.io-index"}
if len(packages) != len(expected) or actual != expected:
    raise SystemExit("RustSec audit lock does not match vendor identities")
with (ROOT.parent / "Cargo.lock").open("rb") as file:
    lock_packages = tomllib.load(file)["package"]
locked = [p for p in lock_packages if p["name"] == "reqwest"]
if {p["version"] for p in locked} != set(VERSIONS) or any("source" in p for p in locked):
    raise SystemExit("workspace reqwest versions do not match vendored audit identities")

for version, checksum in VERSIONS.items():
    filename = f"reqwest-{version}.crate"
    cache = Path(os.environ.get("CARGO_HOME", Path.home() / ".cargo")) / "registry" / "cache"
    cached = next(cache.glob(f"*/{filename}"), None)
    if cached:
        archive = cached.read_bytes()
    else:
        with urllib.request.urlopen(f"https://static.crates.io/crates/reqwest/{filename}", timeout=60) as response:
            archive = response.read(10 * 1024 * 1024 + 1)
    if len(archive) > 10 * 1024 * 1024 or hashlib.sha256(archive).hexdigest() != checksum:
        raise SystemExit(f"invalid upstream archive: {filename}")
    with tempfile.TemporaryDirectory() as temp:
        root = Path(temp)
        with tarfile.open(fileobj=io.BytesIO(archive), mode="r:gz") as tar:
            for member in tar:
                path = PurePosixPath(member.name)
                if path.is_absolute() or ".." in path.parts or not path.parts or path.parts[0] != f"reqwest-{version}":
                    raise SystemExit("unsafe archive path")
                if member.isdir():
                    continue
                if not member.isfile():
                    raise SystemExit("unexpected archive entry")
                target = root.joinpath(*path.parts)
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(tar.extractfile(member).read())
        source = root / f"reqwest-{version}"
        subprocess.run(["patch", "--batch", "--silent", "-p1", "-i", str(ROOT / "patches" / f"reqwest-{version}.patch")], cwd=source, check=True)
        actual = ROOT / f"reqwest-{version}"
        expected_files = {p.relative_to(source) for p in source.rglob("*") if p.is_file()}
        actual_files = {p.relative_to(actual) for p in actual.rglob("*") if p.is_file()}
        if expected_files != actual_files:
            raise SystemExit(f"vendor file set differs for {version}")
        for path in expected_files:
            if (source / path).read_bytes() != (actual / path).read_bytes():
                raise SystemExit(f"vendor content differs: {version}/{path}")
    print(f"reqwest {version}: archive checksum and patch verified")
