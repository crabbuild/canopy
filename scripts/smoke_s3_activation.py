#!/usr/bin/env python3
"""Verify concurrent cold repository recovery after a real S3-backed owner crash."""
import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
import http.client
import json
import os
from pathlib import Path
import signal
import time
import uuid

from benchmark_repositories import Client, corpus, percentiles, seed, verify
from smoke_s3_process import start


def qualify(args):
    args.work_dir.mkdir(parents=True, exist_ok=False)
    settings = {"storage_url": args.storage_url.rstrip("/") + "/" + uuid.uuid4().hex,
                "tenant_id": str(uuid.uuid4()), "application_id": str(uuid.uuid4()),
                "node_id": str(uuid.uuid4()), "fleet_digest": "11" * 32, "image_digest": "22" * 32,
                "owner": "canopy", "peer_endpoint": "https://activation.example.invalid",
                "local_disk_limit_bytes": 512 * 1024**2, "max_active_repositories": 64}
    report = {"binary_sha256": hashlib.sha256(args.binary.read_bytes()).hexdigest(),
              "repositories": 64, "concurrency": args.concurrency, "cold_recovery_passed": False,
              "git_recovery_passed": False, "shutdown_passed": False}
    processes, clients = [], []
    token = os.environ["CANOPY_GIT_TOKEN"]
    try:
        report["phase"] = "initial_startup"
        started = time.monotonic()
        process, base = start(args.binary, args.work_dir, settings, "first")
        report["initial_startup_seconds"] = time.monotonic() - started
        processes.append(process)
        client = Client(base, token, 30)
        clients.append(client)
        manifest = args.work_dir / "corpus.json"
        report["phase"] = "seed"
        report["seed"] = seed(argparse.Namespace(base_url=base, manifest=manifest,
            work_dir=args.work_dir / "seed", repositories=64, populated=3, seed=20260927), client, token)
        client.close()
        process.kill()
        process.wait(timeout=10)
        time.sleep(11)
        report["phase"] = "recovery_startup"
        started = time.monotonic()
        process, base = start(args.binary, args.work_dir, settings, "restored")
        report["recovery_startup_seconds"] = time.monotonic() - started
        processes.append(process)
        client = Client(base, token, 30)
        clients.append(client)

        def read(entry):
            started = time.monotonic()
            try:
                status, body = client.request(f"/api/repositories/{entry['name']}")
                valid = status == 200 and json.loads(body).get("repository_id") == entry["repository_id"]
                outcome = "ok" if valid else f"invalid_or_http_{status}"
            except (OSError, ValueError, http.client.HTTPException):
                outcome = "transport_or_decode_error"
            return {"repository_id": entry["repository_id"], "outcome": outcome,
                    "elapsed_ms": (time.monotonic() - started) * 1000}

        report["phase"] = "cold_reads"
        started = time.monotonic()
        with ThreadPoolExecutor(max_workers=args.concurrency) as executor:
            results = list(executor.map(read, corpus(manifest)["repositories"]))
        report["cold_seconds"] = time.monotonic() - started
        report["reads"] = results
        report["cold_latency_ms"] = percentiles([result["elapsed_ms"] for result in results])
        assert len(results) == 64 and all(result["outcome"] == "ok" for result in results)
        report["cold_recovery_passed"] = True
        print(f"PASS: 64 cold repository identities restored with {args.concurrency} clients and no retries", flush=True)
        report["phase"] = "git_verification"
        report["verify"] = verify(argparse.Namespace(base_url=base, manifest=manifest,
            work_dir=args.work_dir / "verify"), client, token)
        report["git_recovery_passed"] = True
        report["phase"] = "shutdown"
        process.send_signal(signal.SIGTERM)
        process.wait(timeout=60)
        assert process.returncode == 0
        report["shutdown_passed"] = True
        report["phase"] = "complete"
        print("PASS: Git v0/v2 clone/fsck samples and graceful shutdown", flush=True)
    finally:
        for client in clients:
            client.close()
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
    parser.add_argument("--concurrency", type=int, choices=range(1, 33), default=8,
                        metavar="1..32", help="simultaneous cold reads (default: 8)")
    args = parser.parse_args()
    if os.environ.get("CANOPY_GIT_TOKEN") != "local-test-token":
        parser.error("CANOPY_GIT_TOKEN must be the fixture value local-test-token")
    qualify(args)


if __name__ == "__main__":
    main()
