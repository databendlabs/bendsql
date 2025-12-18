#!/usr/bin/env python3

# Copyright 2021 Datafuse Labs
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import argparse
import json
import pathlib
import re
import subprocess
import sys


ROOT = pathlib.Path(__file__).resolve().parents[1]
CARGO_TOML = ROOT / "Cargo.toml"
NODE_PACKAGE = ROOT / "bindings" / "nodejs" / "package.json"
NODE_NPM_DIR = ROOT / "bindings" / "nodejs" / "npm"


def ensure_clean_git_state() -> None:
    result = subprocess.run(
        ["git", "status", "--porcelain"],
        capture_output=True,
        text=True,
        cwd=ROOT,
        check=True,
    )
    if result.stdout.strip():
        sys.exit(
            "Working tree has uncommitted changes. Please clean up before bumping."
        )


def read_current_version() -> tuple[str, list[str], int]:
    lines = CARGO_TOML.read_text().splitlines()
    in_workspace_pkg = False
    for idx, line in enumerate(lines):
        stripped = line.strip()
        if stripped.startswith("[") and stripped != "[workspace.package]":
            in_workspace_pkg = False
        if stripped == "[workspace.package]":
            in_workspace_pkg = True
            continue
        if in_workspace_pkg and stripped.startswith("version"):
            match = re.search(r'"([^"]+)"', line)
            if not match:
                break
            return match.group(1), lines, idx
    raise SystemExit("Unable to find workspace package version in Cargo.toml")


def compute_new_version(version: str, bump: str) -> str:
    parts = version.split(".")
    if len(parts) != 3:
        raise SystemExit(f"Unsupported version format: {version}")
    major, minor, patch = (int(part) for part in parts)
    if bump == "major":
        major += 1
        minor = 0
        patch = 0
    elif bump == "minor":
        minor += 1
        patch = 0
    elif bump == "patch":
        patch += 1
    else:
        raise SystemExit(f"Unknown bump type: {bump}")
    return f"{major}.{minor}.{patch}"


def update_cargo_toml(lines: list[str], version_index: int, new_version: str) -> None:
    target_line = lines[version_index]
    lines[version_index] = re.sub(r'"[^"]+"', f'"{new_version}"', target_line, count=1)
    for idx, line in enumerate(lines):
        stripped = line.strip()
        if stripped.startswith("databend-") and "version" in line:
            lines[idx] = re.sub(
                r'version\s*=\s*"[^"]+"', f'version = "{new_version}"', line, count=1
            )
    CARGO_TOML.write_text("\n".join(lines) + "\n")


def update_package_json(path: pathlib.Path, new_version: str) -> None:
    data = json.loads(path.read_text())
    data["version"] = new_version
    path.write_text(json.dumps(data, indent=2) + "\n")


def update_node_packages(new_version: str) -> list[pathlib.Path]:
    updated = [NODE_PACKAGE]
    update_package_json(NODE_PACKAGE, new_version)
    for package_json in sorted(NODE_NPM_DIR.glob("*/package.json")):
        update_package_json(package_json, new_version)
        updated.append(package_json)
    return updated


def create_commit(new_version: str, extra_files: list[pathlib.Path]) -> None:
    files = [CARGO_TOML, *extra_files]
    subprocess.run(
        ["git", "add", *[str(p.relative_to(ROOT)) for p in files]], check=True, cwd=ROOT
    )
    subprocess.run(
        ["git", "commit", "-m", f"chore: bump version to {new_version}"],
        check=True,
        cwd=ROOT,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Bump BendSQL workspace version")
    parser.add_argument(
        "bump",
        choices=["major", "minor", "patch"],
        help="Which part of the semver to increment",
    )
    args = parser.parse_args()

    ensure_clean_git_state()
    current_version, lines, version_index = read_current_version()
    new_version = compute_new_version(current_version, args.bump)
    update_cargo_toml(lines, version_index, new_version)
    updated_json = update_node_packages(new_version)
    create_commit(new_version, updated_json)
    print(f"Bumped version: {current_version} -> {new_version}")


if __name__ == "__main__":
    main()
