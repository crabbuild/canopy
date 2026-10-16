"""Require one exact crates.io Cellule release, including transitive crates."""

from pathlib import Path
import re
import tomllib


def workspace_manifests(root):
    """Read the virtual/root manifest and every explicitly declared member."""
    manifest = tomllib.loads((root / "Cargo.toml").read_text())
    manifests = [manifest]
    members = set()
    for pattern in manifest.get("workspace", {}).get("members", []):
        members.update(path / "Cargo.toml" for path in root.glob(pattern))
    for path in sorted(members):
        if path != root / "Cargo.toml":
            manifests.append(tomllib.loads(path.read_text()))
    return manifests


def check(manifest, lock):
    pins = {}

    def dependencies(table):
        for key, value in table.items():
            if key in ("dependencies", "dev-dependencies", "build-dependencies"):
                for alias, dependency in value.items():
                    spec = {"version": dependency} if isinstance(dependency, str) else dependency
                    name = spec.get("package", alias)
                    if not name.startswith("cellule-"):
                        continue
                    version = spec.get("version", "")
                    if (not re.fullmatch(r"=\d+\.\d+\.\d+", version)
                            or any(field in spec for field in ("git", "path", "registry", "workspace"))):
                        raise ValueError(f"{name} must pin an exact stable crates.io release")
                    pins.setdefault(name, set()).add(version[1:])
            elif isinstance(value, dict):
                dependencies(value)

    for item in manifest if isinstance(manifest, list) else [manifest]:
        dependencies(item)
    versions = {version for values in pins.values() for version in values}
    if len(versions) != 1:
        raise ValueError("all direct Cellule dependencies must pin the same release")
    version = versions.pop()
    locked = {}
    for package in lock["package"]:
        name = package["name"]
        if not name.startswith("cellule-"):
            continue
        if (name in locked or package["version"] != version
                or package.get("source") != "registry+https://github.com/rust-lang/crates.io-index"
                or not package.get("checksum")):
            raise ValueError(f"{name} must resolve once to crates.io {version}")
        locked[name] = package
    if missing := pins.keys() - locked.keys():
        raise ValueError(f"Cellule dependencies missing from Cargo.lock: {sorted(missing)}")
    return version, len(locked)


if __name__ == "__main__":
    root = Path(__file__).resolve().parents[1]
    try:
        with (root / "Cargo.lock").open("rb") as lock:
            version, count = check(workspace_manifests(root), tomllib.load(lock))
    except ValueError as error:
        raise SystemExit(f"Cellule dependency policy: {error}") from error
    print(f"Cellule {version}: {count} crates locked to one exact registry release")
