#!/usr/bin/env python3
"""Exercise a real Canopy process through Git, LFS, restart, and lease takeover.

The caller provides an S3-compatible test bucket and provider credentials in the
environment. This script writes only below a unique prefix in that bucket.
"""

import argparse
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import tempfile
import time
import urllib.error
import urllib.request
import uuid


def port():
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return listener.getsockname()[1]


def run(*args, cwd=None):
    result = subprocess.run(
        args,
        cwd=cwd,
        env={**os.environ, "GIT_TERMINAL_PROMPT": "0"},
        capture_output=True,
        check=False,
    )
    if result.returncode:
        raise RuntimeError(f"{args[0]} failed: {result.stderr.decode(errors='replace')}")
    return result.stdout.strip()


def git(*args, cwd=None):
    return run("git", "-c", "credential.helper=", *args, cwd=cwd)


def wait_ready(process, address, log):
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError(f"Canopy exited: {log.read_text(errors='replace')}")
        try:
            with urllib.request.urlopen(f"http://{address}/readyz", timeout=1) as response:
                if response.status == 200:
                    return
        except (OSError, urllib.error.HTTPError):
            pass
        time.sleep(0.2)
    raise RuntimeError(f"Canopy did not become ready: {log.read_text(errors='replace')}")


def start(binary, directory, settings, instance):
    address = f"127.0.0.1:{port()}"
    config = {
        **settings,
        "listen": address,
        "public_url": f"http://{address}",
        "data_dir": str(directory / instance),
    }
    path = directory / f"{instance}.json"
    path.write_text(json.dumps(config))
    log = directory / f"{instance}.log"
    output = log.open("wb")
    process = subprocess.Popen([str(binary), str(path)], stdout=output, stderr=output)
    output.close()
    wait_ready(process, address, log)
    return process, f"http://{address}"


def create_repository(base_url, name):
    request = urllib.request.Request(
        f"{base_url}/api/repositories",
        data=json.dumps({"name": name}).encode(),
        headers={
            "Authorization": "Bearer local-test-token",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        created = json.load(response)
        return created["clone_url"], created["repository_id"]


def rename_repository(base_url, old_name, new_name, repository_id):
    request = urllib.request.Request(
        f"{base_url}/api/repositories/{old_name}",
        data=json.dumps({"name": new_name, "repository_id": repository_id}).encode(),
        headers={
            "Authorization": "Bearer local-test-token",
            "Content-Type": "application/json",
        },
        method="PATCH",
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        renamed = json.load(response)
        assert renamed["repository_id"] == repository_id
        return renamed["clone_url"]


def clone_and_verify(url, directory, expected_oid, expected_readme, expected_lfs=None):
    git(
        "-c",
        "http.extraHeader=Authorization: Bearer local-test-token",
        "clone",
        url,
        str(directory),
    )
    if expected_lfs is not None:
        git("lfs", "install", "--local", cwd=directory)
        git(
            "-c",
            "http.extraHeader=Authorization: Bearer local-test-token",
            "lfs",
            "pull",
            cwd=directory,
        )
    assert git("rev-parse", "HEAD", cwd=directory) == expected_oid
    assert (directory / "README.md").read_bytes() == expected_readme
    if expected_lfs is not None:
        assert (directory / "asset.lfs").read_bytes() == expected_lfs


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--storage-url", required=True, help="S3 bucket URL, e.g. s3://test-bucket")
    parser.add_argument("--work-parent", type=Path, required=True)
    args = parser.parse_args()
    for name in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "CANOPY_NODE_SIGNING_KEY_HEX"):
        if not os.environ.get(name):
            parser.error(f"{name} must be set")
    os.environ["CANOPY_GIT_TOKEN"] = "local-test-token"
    run_id = uuid.uuid4()
    settings = {
        "storage_url": f"{args.storage_url.rstrip('/')}/process-smoke/{run_id}",
        "tenant_id": str(uuid.uuid4()),
        "application_id": str(uuid.uuid4()),
        "node_id": str(uuid.uuid4()),
        "fleet_digest": "11" * 32,
        "image_digest": "22" * 32,
        "owner": "canopy",
        "peer_endpoint": "https://smoke.example.invalid",
        "local_disk_limit_bytes": 1 << 30,
    }
    with tempfile.TemporaryDirectory(prefix="canopy-process-", dir=args.work_parent) as temp:
        directory = Path(temp)
        processes = []
        try:
            first, base_url = start(args.binary, directory, settings, "first")
            processes.append(first)
            url, repository_id = create_repository(base_url, "example")
            local = directory / "local"
            git("init", "-b", "main", str(local))
            git("config", "user.name", "Canopy Test", cwd=local)
            git("config", "user.email", "canopy@example.invalid", cwd=local)
            git("lfs", "install", "--local", cwd=local)
            git("lfs", "track", "*.lfs", cwd=local)
            (local / "README.md").write_bytes(b"Canopy process smoke\n")
            lfs_body = b"canopy-lfs" * 130_000
            (local / "asset.lfs").write_bytes(lfs_body)
            git("add", ".", cwd=local)
            git("commit", "-m", "Initial commit", cwd=local)
            git(
                "-c",
                "http.extraHeader=Authorization: Bearer local-test-token",
                "push",
                url,
                "HEAD:refs/heads/main",
                cwd=local,
            )
            oid = git("rev-parse", "HEAD", cwd=local)
            other_url, _ = create_repository(base_url, "other")
            other = directory / "other"
            git("init", "-b", "main", str(other))
            git("config", "user.name", "Canopy Test", cwd=other)
            git("config", "user.email", "canopy@example.invalid", cwd=other)
            other_readme = b"Another Repository Cell\n"
            (other / "README.md").write_bytes(other_readme)
            git("add", "README.md", cwd=other)
            git("commit", "-m", "Other repository", cwd=other)
            git(
                "-c",
                "http.extraHeader=Authorization: Bearer local-test-token",
                "push",
                other_url,
                "HEAD:refs/heads/main",
                cwd=other,
            )
            other_oid = git("rev-parse", "HEAD", cwd=other)
            url = rename_repository(base_url, "example", "renamed", repository_id)
            clone_and_verify(url, directory / "renamed-live", oid, b"Canopy process smoke\n", lfs_body)
            first.send_signal(signal.SIGTERM)
            first.wait(timeout=30)
            if first.returncode:
                raise RuntimeError("Canopy did not shut down cleanly")
            second, base_url = start(args.binary, directory, settings, "second")
            processes.append(second)
            url = f"{base_url}/canopy/renamed.git"
            clone_and_verify(url, directory / "clean-clone", oid, b"Canopy process smoke\n", lfs_body)
            clone_and_verify(f"{base_url}/canopy/other.git", directory / "clean-other", other_oid, other_readme)
            second.kill()
            second.wait(timeout=10)
            time.sleep(11)  # Wait past the signed node advertisement's 10-second lease.
            third, base_url = start(args.binary, directory, settings, "third")
            processes.append(third)
            url = f"{base_url}/canopy/renamed.git"
            clone_and_verify(url, directory / "takeover-clone", oid, b"Canopy process smoke\n", lfs_body)
            clone_and_verify(f"{base_url}/canopy/other.git", directory / "takeover-other", other_oid, other_readme)
            third.send_signal(signal.SIGTERM)
            third.wait(timeout=30)
            if third.returncode:
                raise RuntimeError("takeover owner did not shut down cleanly")
            print("PASS: two repositories and a rename survived restart, disk loss, and lease takeover")
        finally:
            for process in processes:
                if process.poll() is None:
                    process.kill()
                    process.wait()


if __name__ == "__main__":
    main()
