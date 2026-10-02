#!/usr/bin/env python3
"""Serve three real Canopy nodes behind one bounded local stream proxy.

Use a new work directory; state, configuration and metrics survive shutdown.
Fresh deployments get a unique S3 prefix. Existing deployments require their
configuration template and compatible binary; release admission is not bypassed.
This shared-host loopback topology is not reference Linux capacity proof.
"""

import argparse
from contextlib import ExitStack
import hashlib
import json
import os
from pathlib import Path
import secrets
import signal
import ssl
import subprocess
import threading
import time
import uuid

from benchmark_repositories import save
from local_tcp_proxy import LocalProxy
from smoke_s3_peers import peer_certificate
from smoke_s3_process import port, start


def serve(args):
    args.work_dir.mkdir(parents=True, exist_ok=False)
    if args.config_template:
        settings = json.loads(args.config_template.read_text())
        for name in ("listen", "public_url", "peer_endpoint", "peer_ca_certificate", "data_dir", "node_id"):
            settings.pop(name, None)
    else:
        settings = {"storage_url": args.storage_url.rstrip("/") + "/" + uuid.uuid4().hex,
                    "tenant_id": str(uuid.uuid4()), "application_id": str(uuid.uuid4()),
                    "fleet_digest": "11" * 32, "image_digest": "22" * 32, "owner": "canopy",
                    "local_disk_limit_bytes": 1536 * 1024**2,
                    "max_active_repositories": 100}
    if args.max_active_repositories is not None:
        settings["max_active_repositories"] = args.max_active_repositories
    save(args.work_dir / "deployment.json", settings)
    ca, certificate, key = peer_certificate(args.work_dir)
    settings["peer_ca_certificate"] = str(ca)
    tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    tls.load_cert_chain(certificate, key)
    stop = threading.Event()
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    processes, peer_proxies = [], []
    front = None
    fixture_id = uuid.uuid4().hex
    outcome = {"ready": False, "nodes": [], "error": None, "shutdown": [],
               "fixture_id": fixture_id, "launcher_pid": os.getpid()}
    script_root = Path(__file__).parent
    outcome["fixture_scripts_sha256"] = {name: hashlib.sha256((script_root / name).read_bytes()).hexdigest()
        for name in ("serve_three_gateways.py", "local_tcp_proxy.py", "smoke_s3_process.py", "smoke_s3_peers.py")}
    try:
        with ExitStack() as stack:
            try:
                addresses = [f"127.0.0.1:{port()}" for _ in range(3)]
                front = stack.enter_context(LocalProxy(addresses))
                for index, address in enumerate(addresses):
                    peer = stack.enter_context(LocalProxy([address], tls_context=tls))
                    peer_proxies.append(peer)
                    node_id = str(uuid.uuid4())
                    process, ingress = start(args.binary.resolve(), args.work_dir,
                        {**settings, "node_id": node_id, "peer_endpoint": peer.url},
                        f"node-{index}", listen_address=address,
                        signing_key=secrets.token_hex(32), ready_timeout=args.startup_timeout,
                        public_url=front.url)
                    processes.append(process)
                    outcome["nodes"].append({"index": index, "pid": process.pid,
                        "node_id": node_id, "ingress": ingress, "peer_endpoint": peer.url})
                outcome.update(ready=True, proxy_url=front.url,
                    binary_path=str(args.binary.resolve()),
                    binary_sha256=hashlib.sha256(args.binary.read_bytes()).hexdigest(),
                    public_url=front.url, work_dir=str(args.work_dir),
                    max_active_repositories_per_node=settings["max_active_repositories"],
                    topology="three host processes, loopback TCP proxy, TLS peer proxies; external S3 fixture",
                    balance="round-robin TCP connections; keep-alive retains its backend",
                    proxy_limits={"connections": front.max_connections, "buffer_bytes": front.buffer_bytes},
                    started_at_utc=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()))
                save(args.work_dir / "ready.json", outcome)
                print(json.dumps(outcome), flush=True)
                while not stop.wait(0.5):
                    for index, process in enumerate(processes):
                        if process.poll() is not None:
                            raise RuntimeError(f"node-{index} exited with {process.returncode}")
                    for endpoint in (front, *peer_proxies):
                        if endpoint.failure is not None or not endpoint.worker.is_alive():
                            raise RuntimeError("a fixture proxy stopped unexpectedly")
                    metrics = {"fixture_id": fixture_id, "captured_monotonic": time.monotonic(),
                               "front": front.snapshot(),
                               "peers": [peer.snapshot() for peer in peer_proxies]}
                    save(args.work_dir / "proxy-metrics.json", metrics)
            finally:
                for process in processes:
                    if process.poll() is None:
                        process.send_signal(signal.SIGTERM)
                for index, process in enumerate(processes):
                    forced = False
                    try:
                        process.wait(timeout=30)
                    except subprocess.TimeoutExpired:
                        forced = True
                        process.kill()
                        process.wait(timeout=10)
                    outcome["shutdown"].append({"index": index, "exit_code": process.returncode,
                                                "forced": forced})
        if any(node["forced"] or node["exit_code"] != 0 for node in outcome["shutdown"]):
            raise RuntimeError("fleet shutdown did not confirm graceful node exits")
    except BaseException as error:
        outcome["error"] = f"{type(error).__name__}: {error}"
        raise
    finally:
        save(args.work_dir / "outcome.json", outcome)
        if front is not None:
            save(args.work_dir / "proxy-metrics-final.json", {
                "front": front.snapshot(), "peers": [peer.snapshot() for peer in peer_proxies]})


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    deployment = parser.add_mutually_exclusive_group(required=True)
    deployment.add_argument("--storage-url", help="caller-owned disposable S3 prefix")
    deployment.add_argument("--config-template", type=Path, help="existing compatible deployment")
    parser.add_argument("--work-dir", type=Path, required=True)
    parser.add_argument("--startup-timeout", type=int, default=90)
    parser.add_argument("--max-active-repositories", type=int)
    args = parser.parse_args()
    if args.startup_timeout < 1 or (args.max_active_repositories is not None
                                  and not 1 <= args.max_active_repositories <= 9999):
        parser.error("require positive startup timeout and 1..9999 active repositories")
    if not os.environ.get("CANOPY_GIT_TOKEN"):
        parser.error("CANOPY_GIT_TOKEN is required")
    serve(args)


if __name__ == "__main__":
    main()
