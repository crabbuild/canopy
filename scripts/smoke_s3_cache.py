#!/usr/bin/env python3
"""Prove incremental Git cache metadata and bodies and fresh-owner recovery on real S3 storage."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import time
import urllib.request
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
              "recovery_passed": False, "shutdown_passed": False,
              "capability_discovery_passed": False}
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
        for index in range(256):
            (source / f"history-{index:03}.txt").write_text(f"Existing object {index}\n")
        git("add", ".", cwd=source)
        git("commit", "-m", "Initial objects", cwd=source)
        git("-c", AUTH, "push", url, "HEAD:refs/heads/main", cwd=source)
        cold = args.work_dir / "cold-clone"
        git("-c", AUTH, "clone", url, str(cold))
        assert (cold / "large.bin").read_bytes() == body
        before = cached_objects(args.work_dir / "first")
        assert len(before) == 260
        first_log = args.work_dir / "first.log"
        log_offset = len(first_log.read_text())
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
        refreshes = []
        for line in first_log.read_text()[log_offset:].splitlines():
            if "hydrated Git cache" in line:
                refreshes.append({key: int(value) for key, value in re.findall(
                    r"\b(objects|scanned|from_sequence|through_sequence|bytes)=([0-9]+)", line)})
        assert refreshes, "hydration diagnostics required: enable canopy_server::git_gateway=debug"
        assert sum(event["scanned"] for event in refreshes) == 3, refreshes
        assert sum(event["objects"] for event in refreshes) == 3, refreshes
        assert all(event["from_sequence"] > 0 for event in refreshes), refreshes
        report.update(incremental_refreshes=refreshes, incremental_scanned_objects=3,
                      cache_reuse_passed=True, initial_cached_objects=len(before),
                      final_cached_objects=len(after), reused_compressed_bytes=sum(item[0] for item in before.values()))
        print("PASS: stock pushes/clones reuse 260 object files and scan/hydrate only three new objects", flush=True)
        process.kill()
        process.wait(timeout=10)
        time.sleep(11)
        process, restored = start(args.binary, args.work_dir, settings, "restored")
        processes.append(process)
        restored_url = url.replace(base, restored, 1)
        restored_data = args.work_dir / "restored"
        restored_log = args.work_dir / "restored.log"

        def capabilities():
            request = urllib.request.Request(
                restored_url + "/info/refs?service=git-upload-pack",
                headers={"Authorization": "Bearer local-test-token", "Git-Protocol": "version=2"})
            with urllib.request.urlopen(request, timeout=30) as response:
                assert response.headers["Content-Type"] == "application/x-git-upload-pack-advertisement"
                result = response.read()
            assert result.startswith(b"000eversion 2\n")
            return result

        assert not cached_objects(restored_data)
        log_offset = len(restored_log.read_text())
        started = time.monotonic()
        cold_capabilities = capabilities()
        report["cold_capability_discovery_seconds"] = time.monotonic() - started
        report["cold_capability_cached_objects"] = len(cached_objects(restored_data))
        assert report["cold_capability_cached_objects"] == 0
        assert "hydrated Git cache" not in restored_log.read_text()[log_offset:]
        for protocol in (0, 2):
            clone(restored_url, f"restored-v{protocol}", protocol)
        assert len(cached_objects(restored_data)) == 263
        assert capabilities() == cold_capabilities
        report["capability_discovery_passed"] = True
        print("PASS: cold Git v2 capabilities hydrate no objects and match warm discovery", flush=True)
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
