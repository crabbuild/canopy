#!/usr/bin/env python3
"""Verify HTTP and lease progress while the real process log pipe is stalled.

Requires a disposable S3 prefix, provider credentials, CANOPY_GIT_TOKEN set to
local-test-token and CANOPY_NODE_SIGNING_KEY_HEX. Use a fresh work directory on
the qualification volume. Provider data is retained for caller-owned cleanup.
"""
import argparse
import json
from native_limits import fixture_native_limits
import os
from pathlib import Path
import re
import select
import signal
import subprocess
import threading
import time
import uuid

from benchmark_repositories import Client, percentiles
from smoke_s3_process import host_git_environment, port, wait_ready


def qualify(args):
    args.work_dir.mkdir(parents=True, exist_ok=False)
    address = f"127.0.0.1:{port()}"
    config = {
        "storage_url": args.storage_url.rstrip("/") + "/" + uuid.uuid4().hex,
        "tenant_id": str(uuid.uuid4()), "application_id": str(uuid.uuid4()),
        "node_id": str(uuid.uuid4()), "fleet_digest": "11" * 32, "image_digest": "22" * 32,
        "owner": "canopy", "listen": address, "public_url": "http://" + address,
        "peer_endpoint": "https://logging.example.invalid",
        "data_dir": str(args.work_dir / "node"),
        "native_limits": fixture_native_limits(), "local_disk_limit_bytes": 256 * 1024**2, "max_active_repositories": 3,
    }
    config_path = args.work_dir / "config.json"
    config_path.write_text(json.dumps(config))
    log = args.work_dir / "server.log"
    environment = host_git_environment(args.work_dir)
    environment["RUST_LOG"] = "warn,canopy_server::server=debug,canopy=warn"
    environment["NO_COLOR"] = "1"
    pause, paused = threading.Event(), threading.Event()
    reader_errors = []
    process = subprocess.Popen([str(args.binary), str(config_path)], stdout=subprocess.PIPE,
                               stderr=subprocess.STDOUT, env=environment)

    def drain():
        try:
            with log.open("wb", buffering=0) as output:
                while True:
                    if pause.is_set():
                        paused.set()
                        time.sleep(.01)
                        continue
                    readable, _, _ = select.select([process.stdout], [], [], .1)
                    if not readable:
                        continue
                    data = os.read(process.stdout.fileno(), 65536)
                    if not data:
                        break
                    output.write(data)
        except Exception as error:
            reader_errors.append(error)

    reader = threading.Thread(target=drain, daemon=True)
    reader.start()
    client = Client("http://" + address, "local-test-token", 1)
    latencies = []
    try:
        wait_ready(process, address, log)
        status, body = client.request("/api/repositories", {"name": "logging"})
        assert status == 200
        repository_id = json.loads(body)["repository_id"]
        pause.set()
        assert paused.wait(2), "log reader did not pause"
        started = time.monotonic()
        # Twelve seconds exceeds the ten-second node lease. The event volume
        # fills both the OS pipe and bounded log queue; the drop counter proves it.
        for sequence in range(600):
            delay = started + sequence / 50 - time.monotonic()
            if delay > 0:
                time.sleep(delay)
            path = "/readyz" if sequence % 10 else "/api/repositories/logging"
            requested = time.monotonic()
            status, body = client.request(path)
            latencies.append((time.monotonic() - requested) * 1000)
            assert status == 200, f"HTTP {status} while stderr was stalled"
            if sequence % 10 == 0:
                assert json.loads(body)["repository_id"] == repository_id
        time.sleep(max(0, started + 12 - time.monotonic()))
        status, body = client.request("/canopy/logging.git/info/refs?service=git-upload-pack", git=True)
        assert status == 200 and body.startswith(b"000eversion 2\n")
        held_seconds = time.monotonic() - started
        pause.clear()
        # Allow the resumed destination to consume the finite queue before the
        # best-effort exit summary is emitted.
        time.sleep(.5)
        process.send_signal(signal.SIGTERM)
        process.wait(timeout=30)
        assert process.returncode == 0, "server did not drain cleanly"
    finally:
        pause.clear()
        client.close()
        if process.poll() is None:
            process.kill()
            process.wait(timeout=15)
        reader.join(timeout=5)
        process.stdout.close()
    if reader.is_alive() or reader_errors:
        raise RuntimeError("log reader failed to finish")
    output = log.read_text()
    dropped = re.search(r"dropped_lines=(\d+)", output)
    assert dropped and int(dropped[1]) > 0, "fixture did not prove log queue saturation"
    assert " ERROR " not in output, "server reported an error"
    report = {"requests": len(latencies), "paused_log_seconds": held_seconds,
              "service_ms": percentiles(latencies), "dropped_lines": int(dropped[1]),
              "repository_identity_verified": True, "git_v2_discovery_passed": True,
              "shutdown_passed": True}
    (args.work_dir / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--storage-url", required=True)
    parser.add_argument("--work-dir", type=Path, required=True)
    qualify(parser.parse_args())


if __name__ == "__main__":
    main()
