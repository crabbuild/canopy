#!/usr/bin/env python3
"""Prove incremental Git cache bodies and fresh-owner recovery on real S3 storage."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import time
import uuid

from smoke_s3_process import create_repository, git, start

AUTH = "http.extraHeader=Authorization: Bearer local-test-token"


def cached_objects(data):
    objects = {}
    for cache in (data / "runtime-v1").glob("canopy-git-*/repo.git"):
        if (cache / "objects/info/alternates").exists():
            continue
        for path in (cache / "objects").glob("*/*"):
            if re.fullmatch(r"[0-9a-f]{2}/[0-9a-f]{38}", path.relative_to(cache / "objects").as_posix()):
                stat = path.stat()
                objects[str(path)] = [stat.st_size, stat.st_mtime_ns]
    return objects


def qualify(args):
    args.work_dir.mkdir(parents=True, exist_ok=False)
    settings = {"storage_url": args.storage_url.rstrip("/") + "/" + uuid.uuid4().hex,
                "tenant_id": str(uuid.uuid4()), "application_id": str(uuid.uuid4()),
                "node_id": str(uuid.uuid4()), "fleet_digest": "11" * 32, "image_digest": "22" * 32,
                "owner": "canopy", "peer_endpoint": "https://cache.example.invalid",
                "local_disk_limit_bytes": 512 * 1024**2, "max_active_repositories": 3}
    report = {"binary_sha256": hashlib.sha256(args.binary.read_bytes()).hexdigest(),
              "storage_url": settings["storage_url"], "cache_reuse_passed": False,
              "recovery_passed": False, "shutdown_passed": False}
    processes = []
    try:
        process, base = start(args.binary, args.work_dir, settings, "first")
        processes.append(process)
        url, _ = create_repository(base, "cache-proof")
        source = args.work_dir / "source"
        git("init", "-b", "main", str(source))
        git("config", "user.name", "Canopy Test", cwd=source)
        git("config", "user.email", "canopy@example.invalid", cwd=source)
        body = os.urandom(2 * 1024**2)
        (source / "large.bin").write_bytes(body)
        (source / "README.md").write_text("Durable object cache\n")
        git("add", ".", cwd=source)
        git("commit", "-m", "Initial objects", cwd=source)
        git("-c", AUTH, "push", url, "HEAD:refs/heads/main", cwd=source)
        cold = args.work_dir / "cold-clone"
        git("-c", AUTH, "clone", url, str(cold))
        assert (cold / "large.bin").read_bytes() == body
        before = cached_objects(args.work_dir / "first")
        assert len(before) == 4
        (source / "increment.txt").write_text("Incremental bytes\n")
        git("add", "increment.txt", cwd=source)
        git("commit", "-m", "Small incremental push", cwd=source)
        expected = git("rev-parse", "HEAD", cwd=source)
        started = time.monotonic()
        git("-c", AUTH, "push", url, "HEAD:refs/heads/main", cwd=source)
        report["incremental_push_seconds"] = time.monotonic() - started

        def clone(remote, name, protocol):
            destination = args.work_dir / name
            git("-c", AUTH, "-c", f"protocol.version={protocol}", "clone", remote, str(destination))
            assert git("rev-parse", "HEAD", cwd=destination) == expected
            assert (destination / "large.bin").read_bytes() == body
            assert (destination / "increment.txt").read_text() == "Incremental bytes\n"
            git("fsck", "--strict", "--full", cwd=destination)

        for protocol in (0, 2):
            clone(url, f"warm-v{protocol}", protocol)
        after = cached_objects(args.work_dir / "first")
        assert len(after) == len(before) + 3
        assert all(after.get(path) == metadata for path, metadata in before.items())
        report.update(cache_reuse_passed=True, initial_cached_objects=len(before),
                      final_cached_objects=len(after), reused_compressed_bytes=sum(item[0] for item in before.values()))
        print("PASS: stock pushes/clones reuse existing object files and hydrate three new objects", flush=True)
        process.kill()
        process.wait(timeout=10)
        time.sleep(11)
        process, restored = start(args.binary, args.work_dir, settings, "restored")
        processes.append(process)
        for protocol in (0, 2):
            clone(url.replace(base, restored, 1), f"restored-v{protocol}", protocol)
        report["recovery_passed"] = True
        process.send_signal(signal.SIGTERM)
        process.wait(timeout=60)
        assert process.returncode == 0
        report["shutdown_passed"] = True
        print("PASS: SIGKILL, fresh local state, Git v0/v2 integrity and graceful shutdown", flush=True)
    finally:
        for process in processes:
            if process.poll() is None:
                process.kill()
                process.wait(timeout=10)
        (args.work_dir / "report.json").write_text(json.dumps(report, indent=2) + "\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--storage-url", required=True, help="caller-owned disposable S3 prefix")
    parser.add_argument("--work-dir", type=Path, required=True, help="new directory on the qualification volume")
    args = parser.parse_args()
    if os.environ.get("CANOPY_GIT_TOKEN") != "local-test-token":
        parser.error("CANOPY_GIT_TOKEN must be the fixture value local-test-token")
    qualify(args)


if __name__ == "__main__":
    main()
