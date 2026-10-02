#!/usr/bin/env python3
"""Qualify stock Git LFS under saturated account admission against real S3 storage."""
import argparse
import hashlib
import json
from native_limits import fixture_native_limits
import os
from pathlib import Path
import signal
import socket
import subprocess
import time
import urllib.request
import uuid

from smoke_s3_process import create_repository, git, start

AUTH = "http.extraHeader=Authorization: Bearer local-test-token"
BODY = b"held transfer admission"


def hold_upload(base, oid):
    address = base.removeprefix("http://")
    host, port = address.split(":")
    stream = socket.create_connection((host, int(port)), timeout=10)
    stream.sendall((f"PUT /canopy/fairness.git/info/lfs/objects/{oid} HTTP/1.1\r\n"
                    f"Host: {address}\r\nAuthorization: Bearer local-test-token\r\n"
                    f"Content-Length: {len(BODY)}\r\nExpect: 100-continue\r\n"
                    "Connection: close\r\n\r\n").encode())
    expected = b"HTTP/1.1 100 Continue\r\n\r\n"
    received = b""
    while len(received) < len(expected):
        part = stream.recv(len(expected) - len(received))
        if not part:
            raise RuntimeError("upload closed before admission")
        received += part
    assert received == expected, received
    return stream


def transfer(source, *arguments):
    started = time.monotonic()
    result = subprocess.run(["git", "-c", "credential.helper=", "-c", AUTH, *arguments],
                            cwd=source, capture_output=True, timeout=100,
                            env={**os.environ, "GIT_TERMINAL_PROMPT": "0",
                                 "GIT_TRACE": "1", "GIT_CURL_VERBOSE": "1"})
    # This fixture uses one fixed test credential. Strip it and authorization
    # header lines before retaining client diagnostics outside the checkout.
    trace = result.stderr.decode(errors="replace").replace("local-test-token", "[REDACTED]")
    trace = "\n".join(line for line in trace.splitlines() if "authorization:" not in line.lower())
    (source.parent / f"lfs-{arguments[1]}.log").write_text(trace + "\n")
    if result.returncode:
        raise RuntimeError(f"stock Git LFS failed with exit {result.returncode}; see sanitized client log")
    rejects = result.stderr.count(b"HTTP: 503")
    return {"seconds": time.monotonic() - started, "http_503_retries": rejects}


def qualify(args):
    os.environ["GIT_CONFIG_GLOBAL"] = os.devnull
    os.environ["GIT_CONFIG_NOSYSTEM"] = "1"
    args.work_dir.mkdir(parents=True, exist_ok=False)
    settings = {"storage_url": args.storage_url.rstrip("/") + "/" + uuid.uuid4().hex,
                "tenant_id": str(uuid.uuid4()), "application_id": str(uuid.uuid4()),
                "node_id": str(uuid.uuid4()), "fleet_digest": "11" * 32, "image_digest": "22" * 32,
                "owner": "canopy", "peer_endpoint": "https://fairness.example.invalid",
                "native_limits": fixture_native_limits(), "local_disk_limit_bytes": 512 * 1024**2, "max_active_repositories": 3}
    report = {"binary_sha256": hashlib.sha256(args.binary.read_bytes()).hexdigest(),
              "storage_url": settings["storage_url"], "objects": 16, "object_bytes": 1024**2,
              "git_lfs_version": git("lfs", "version").decode(), "passed": False}
    process = None
    held = []
    try:
        process, base = start(args.binary, args.work_dir, settings, "node")
        remote, _ = create_repository(base, "fairness")
        source = args.work_dir / "source"
        git("init", "-b", "main", str(source))
        git("config", "user.name", "Canopy Test", cwd=source)
        git("config", "user.email", "canopy@example.invalid", cwd=source)
        git("lfs", "install", "--local", cwd=source)
        git("lfs", "track", "*.bin", cwd=source)
        hashes = {}
        for index in range(report["objects"]):
            data = os.urandom(report["object_bytes"])
            name = f"object-{index:02}.bin"
            hashes[name] = hashlib.sha256(data).hexdigest()
            (source / name).write_bytes(data)
        git("add", ".", cwd=source)
        git("commit", "-m", "Concurrent LFS objects", cwd=source)
        git("remote", "add", "origin", remote, cwd=source)
        assert b"ConcurrentTransfers=8" in git("lfs", "env", cwd=source)
        oid = hashlib.sha256(BODY).hexdigest()
        for _ in range(3):
            held.append(hold_upload(base, oid))
        # Three held uploads leave one account slot. The stock client uses its
        # default concurrency/retry settings and must complete all sixteen OIDs.
        report["upload"] = transfer(source, "lfs", "push", "--all", "origin")
        git("-c", AUTH, "push", "origin", "main", cwd=source)
        destination = args.work_dir / "clone"
        subprocess.run(["git", "-c", "credential.helper=", "-c", AUTH,
                        "clone", remote, str(destination)], check=True, capture_output=True,
                       timeout=60, env={**os.environ, "GIT_LFS_SKIP_SMUDGE": "1",
                                       "GIT_TERMINAL_PROMPT": "0"})
        git("lfs", "install", "--local", cwd=destination)
        report["download"] = transfer(destination, "lfs", "pull")
        for name, expected in hashes.items():
            assert hashlib.sha256((destination / name).read_bytes()).hexdigest() == expected
        git("fsck", "--strict", cwd=destination)
        git("lfs", "fsck", cwd=destination)
        for stream in held:
            stream.sendall(BODY)
            response = b""
            while part := stream.recv(4096):
                response += part
            assert response.startswith(b"HTTP/1.1 200 OK\r\n"), response
            stream.close()
        held.clear()
        request = urllib.request.Request(f"{remote}/info/lfs/objects/{oid}",
                                         headers={"Authorization": "Bearer local-test-token"})
        with urllib.request.urlopen(request, timeout=10) as response:
            assert response.read() == BODY
        process.send_signal(signal.SIGTERM)
        process.wait(timeout=60)
        assert process.returncode == 0
        report["passed"] = True
        print("PASS: stock LFS completes under account saturation, exact push/pull bytes, clean shutdown", flush=True)
    finally:
        for stream in held:
            stream.close()
        if process is not None and process.poll() is None:
            process.kill()
            process.wait(timeout=10)
        (args.work_dir / "report.json").write_text(json.dumps(report, indent=2) + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--storage-url", required=True)
    parser.add_argument("--work-dir", type=Path, required=True)
    args = parser.parse_args()
    if os.environ.get("CANOPY_GIT_TOKEN") != "local-test-token":
        parser.error("CANOPY_GIT_TOKEN must be the fixture value local-test-token")
    qualify(args)
