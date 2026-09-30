#!/usr/bin/env python3
"""Serve an existing Canopy deployment through two local HTTPS peer nodes.

This is a disposable local qualification fixture. It keeps its workspace and
logs after shutdown, and never removes data from the configured object store.
Use benchmark_repositories.py with both printed HTTP ingress URLs.
"""

import argparse
from contextlib import ExitStack
import json
import os
from pathlib import Path
import secrets
import signal
import subprocess
import threading
import uuid

from benchmark_repositories import corpus
from smoke_s3_peers import peer_certificate, proxy
from smoke_s3_process import port, start


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--config-template", type=Path, required=True,
                        help="existing deployment identity and store URL")
    parser.add_argument("--manifest", type=Path, required=True,
                        help="complete benchmark corpus for that deployment")
    parser.add_argument("--work-dir", type=Path, required=True,
                        help="new directory for local node state, certificates and logs")
    parser.add_argument("--startup-timeout", type=int, default=90,
                        help="seconds to allow each node to restore the existing deployment")
    parser.add_argument("--max-active-repositories", type=int,
                        help="override each node's bounded active-Cell admission limit")
    args = parser.parse_args()
    if args.startup_timeout < 1:
        parser.error("--startup-timeout must be positive")
    if args.max_active_repositories is not None and args.max_active_repositories < 1:
        parser.error("--max-active-repositories must be positive")
    if not os.environ.get("CANOPY_GIT_TOKEN"):
        parser.error("CANOPY_GIT_TOKEN is required")
    manifest = corpus(args.manifest)
    settings = json.loads(args.config_template.read_text())
    if args.max_active_repositories is not None:
        settings["max_active_repositories"] = args.max_active_repositories
    args.work_dir.mkdir(parents=True, exist_ok=False)
    ca, certificate, key = peer_certificate(args.work_dir)
    settings["peer_ca_certificate"] = str(ca)
    processes = []
    stop = threading.Event()
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    with ExitStack() as stack:
        try:
            addresses = [f"127.0.0.1:{port()}" for _ in range(2)]
            peers = [stack.enter_context(proxy(address, certificate, key))
                     for address in addresses]
            ingresses = []
            for index, (address, peer) in enumerate(zip(addresses, peers)):
                instance = f"node-{index}"
                process, ingress = start(
                    args.binary.resolve(), args.work_dir,
                    {**settings, "node_id": str(uuid.uuid4()), "peer_endpoint": peer},
                    instance, listen_address=address,
                    signing_key=secrets.token_hex(32),
                    ready_timeout=args.startup_timeout)
                processes.append(process)
                ingresses.append(ingress)
            print(json.dumps({"ingresses": ingresses,
                              "corpus_repositories": len(manifest["repositories"]),
                              "max_active_repositories_per_node": settings["max_active_repositories"],
                              "work_dir": str(args.work_dir)}), flush=True)
            while not stop.wait(0.5):
                for index, process in enumerate(processes):
                    if process.poll() is not None:
                        raise RuntimeError(f"node-{index} exited; inspect {args.work_dir}")
        finally:
            for process in processes:
                if process.poll() is None:
                    process.send_signal(signal.SIGTERM)
            for process in processes:
                try:
                    process.wait(timeout=30)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()


if __name__ == "__main__":
    main()
