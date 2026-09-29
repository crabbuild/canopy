#!/usr/bin/env python3
"""Seed disposable repository identities and measure scheduled HTTP read load.

Use CANOPY_GIT_TOKEN for authentication. Reports contain no credentials. This
measures metadata or Git v2 discovery; clone, push and LFS throughput need their
own qualification. Seed and verify use stock Git for a declared corpus sample.
"""

import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import hashlib
import http.client
import json
import math
import os
from pathlib import Path
import random
import re
import subprocess
import threading
import time
from urllib.parse import quote, urlsplit
import uuid


class Client:
    def __init__(self, base, token, timeout):
        url = urlsplit(base)
        if url.scheme not in ("http", "https") or not url.hostname or url.username or url.password or url.query or url.fragment:
            raise ValueError("base URL must be HTTP(S), without credentials, query or fragment")
        self.connection_type = http.client.HTTPSConnection if url.scheme == "https" else http.client.HTTPConnection
        self.host, self.port, self.prefix = url.hostname, url.port, url.path.rstrip("/")
        self.token, self.timeout = token, timeout
        self.local = threading.local()
        self.connections = []
        self.lock = threading.Lock()

    def request(self, path, payload=None, *, git=False, request_id=None):
        connection = getattr(self.local, "connection", None)
        if connection is None:
            connection = self.connection_type(self.host, self.port, timeout=self.timeout)
            self.local.connection = connection
            with self.lock:
                self.connections.append(connection)
        headers = {"Authorization": f"Bearer {self.token}", "Content-Type": "application/json"}
        if request_id is not None:
            headers["X-Request-ID"] = request_id
        if git:
            headers["Git-Protocol"] = "version=2"
        try:
            connection.request("GET" if payload is None else "POST", self.prefix + path,
                               body=None if payload is None else json.dumps(payload), headers=headers)
            response = connection.getresponse()
            body = response.read(2 * 1024 * 1024 + 1)
            if len(body) > 2 * 1024 * 1024:
                raise ValueError("benchmark response exceeds 2 MiB")
            return response.status, body
        except BaseException:
            # Reuse this bounded per-thread connection object after reconnecting.
            connection.close()
            raise

    def close(self):
        for connection in self.connections:
            connection.close()


def git(*args, cwd, token):
    environment = {key: value for key, value in os.environ.items()
                   if not key.startswith(("GIT_", "AWS_", "RUSTFS_", "CANOPY_"))}
    environment.update(GIT_TERMINAL_PROMPT="0", GIT_CONFIG_NOSYSTEM="1",
                       GIT_CONFIG_GLOBAL=os.devnull, GIT_CONFIG_COUNT="2",
                       GIT_CONFIG_KEY_0="credential.helper", GIT_CONFIG_VALUE_0="",
                       GIT_CONFIG_KEY_1="http.extraHeader",
                       GIT_CONFIG_VALUE_1=f"Authorization: Bearer {token}")
    result = subprocess.run(["git", *args], cwd=cwd, env=environment,
                            capture_output=True, timeout=120, check=False)
    if result.returncode:
        raise RuntimeError(f"Git {args[0]} failed (exit {result.returncode})")
    return result.stdout.strip().decode()


def save(path, value):
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2) + "\n")
    temporary.replace(path)


def seed(args, client, token):
    if args.manifest.exists():
        raise ValueError("seed requires a new manifest; use verify for an existing corpus")
    args.work_dir.mkdir(parents=True, exist_ok=False)
    generator = random.Random(args.seed)
    selected = set(generator.sample(range(args.repositories), min(args.populated, args.repositories)))
    prefix = f"density-{uuid.uuid4().hex[:12]}"
    manifest = {"version": 1, "complete": False, "prefix": prefix, "seed": args.seed,
                "requested_repositories": args.repositories, "repositories": [],
                "git_version": git("--version", cwd=args.work_dir, token=token)}
    started = time.monotonic()
    try:
        for index in range(args.repositories):
            name = f"{prefix}-{index:05d}"
            creating = time.monotonic()
            status, body = client.request("/api/repositories", {"name": name})
            create_ms = (time.monotonic() - creating) * 1000
            if status != 200:
                raise RuntimeError(f"repository seed failed with HTTP {status}")
            entry = json.loads(body)
            if entry["name"] != name or not re.fullmatch(r"[A-Za-z0-9_-]+", entry["owner"]):
                raise RuntimeError("seed response has an invalid repository identity")
            uuid.UUID(entry["repository_id"])
            record = {key: entry[key] for key in ("name", "owner", "repository_id")}
            record["create_ms"] = round(create_ms, 3)
            record["commit"] = None
            manifest["repositories"].append(record)
            if index in selected:
                local = args.work_dir / name
                git("init", "-b", "main", str(local), cwd=args.work_dir, token=token)
                git("config", "user.name", "Canopy Density", cwd=local, token=token)
                git("config", "user.email", "density@example.invalid", cwd=local, token=token)
                content = f"{name}\nseed={args.seed}\n".encode()
                (local / "README.md").write_bytes(content)
                git("add", "README.md", cwd=local, token=token)
                git("commit", "-m", "Density fixture", cwd=local, token=token)
                url = f"{args.base_url.rstrip('/')}/{entry['owner']}/{name}.git"
                git("push", url, "HEAD:refs/heads/main", cwd=local, token=token)
                record["commit"] = git("rev-parse", "HEAD", cwd=local, token=token)
                record["readme_sha256"] = hashlib.sha256(content).hexdigest()
            if (index + 1) % 25 == 0:
                save(args.manifest, manifest)
                print(f"seeded {index + 1}/{args.repositories} repositories", flush=True)
        manifest["complete"] = True
    finally:
        manifest["seed_seconds"] = round(time.monotonic() - started, 3)
        save(args.manifest, manifest)
    return {"seeded": len(manifest["repositories"]), "populated": len(selected),
            "seconds": manifest["seed_seconds"]}


def corpus(path):
    manifest = json.loads(path.read_text())
    entries = manifest["repositories"]
    if manifest["version"] != 1 or not manifest["complete"] or len(entries) != manifest["requested_repositories"]:
        raise ValueError("manifest must contain a complete version-1 corpus")
    for entry in entries:
        if any(not re.fullmatch(r"[A-Za-z0-9_-]+", entry[key]) for key in ("name", "owner")):
            raise ValueError("manifest contains invalid repository names")
        uuid.UUID(entry["repository_id"])
    return manifest


def verify(args, client, token):
    manifest = corpus(args.manifest)
    args.work_dir.mkdir(parents=True, exist_ok=False)
    populated = 0
    for index, entry in enumerate(manifest["repositories"]):
        status, body = client.request(f"/api/repositories/{quote(entry['name'])}")
        if status != 200 or json.loads(body)["repository_id"] != entry["repository_id"]:
            raise RuntimeError("restored repository identity differs from manifest")
        if (index + 1) % 25 == 0:
            print(f"verified {index + 1}/{len(manifest['repositories'])} identities", flush=True)
        if entry["commit"] is None:
            continue
        for protocol in ("0", "2"):
            clone = args.work_dir / f"{entry['name']}-v{protocol}"
            url = f"{args.base_url.rstrip('/')}/{entry['owner']}/{entry['name']}.git"
            git("-c", f"protocol.version={protocol}", "clone", url, str(clone), cwd=args.work_dir, token=token)
            if git("rev-parse", "HEAD", cwd=clone, token=token) != entry["commit"]:
                raise RuntimeError("restored commit differs from manifest")
            if hashlib.sha256((clone / "README.md").read_bytes()).hexdigest() != entry["readme_sha256"]:
                raise RuntimeError("restored file bytes differ from manifest")
            git("fsck", "--strict", "--full", cwd=clone, token=token)
        populated += 1
    return {"verified_repositories": len(manifest["repositories"]), "git_v0_v2_samples": populated}


def percentiles(values):
    values = sorted(values)
    return {name: None if not values else round(values[min(len(values) - 1, math.ceil(q * len(values)) - 1)], 3)
            for name, q in (("p50", .5), ("p95", .95), ("p99", .99), ("max", 1))}


def measure(args, client, token):
    del token
    clients = client if isinstance(client, list) else [client]
    manifest = corpus(args.manifest)
    if args.active_repositories > len(manifest["repositories"]):
        raise ValueError("active repository count exceeds corpus")
    generator = random.Random(args.seed)
    active = generator.sample(manifest["repositories"], args.active_repositories)
    # Skew has a declared hot tenth, not an implicit warm-cache assumption.
    hot = active[:max(1, len(active) // 10)]
    total = math.ceil(args.duration * args.rate)
    if total > 1_000_000:
        raise ValueError("one run is limited to one million scheduled arrivals")
    counts, latencies, service_times, dispatch_times = Counter(), [], [], []
    ingress_counts = [Counter() for _ in clients]
    ingress_latencies = [[] for _ in clients]
    ingress_service_times = [[] for _ in clients]
    slots = threading.BoundedSemaphore(args.concurrency)
    lock = threading.Lock()
    started = time.monotonic()
    started_at = datetime.now(timezone.utc).isoformat()
    samples_path = args.output.with_suffix(".samples.jsonl")
    if args.output.exists() or samples_path.exists():
        raise ValueError("run requires new output paths")
    with samples_path.open("x") as samples:
        def record(sample):
            with lock:
                counts[sample["result"]] += 1
                ingress_counts[sample["ingress_index"]][sample["result"]] += 1
                if sample["elapsed_ms"] is not None:
                    latencies.append(sample["elapsed_ms"])
                    service_times.append(sample["service_ms"])
                    dispatch_times.append(sample["dispatch_delay_ms"])
                    ingress_latencies[sample["ingress_index"]].append(sample["elapsed_ms"])
                    ingress_service_times[sample["ingress_index"]].append(sample["service_ms"])
                samples.write(json.dumps(sample) + "\n")

        def execute(sequence, entry, scheduled):
            request_id = str(uuid.uuid4())
            ingress_index = sequence % len(clients)
            ingress = clients[ingress_index]
            dispatched = time.monotonic()
            result = "transport_error"
            try:
                if args.operation == "metadata":
                    status, body = ingress.request(f"/api/repositories/{entry['name']}", request_id=request_id)
                    decoded = json.loads(body) if status == 200 else None
                    valid = isinstance(decoded, dict) and decoded.get("repository_id") == entry["repository_id"]
                else:
                    status, body = ingress.request(f"/{entry['owner']}/{entry['name']}.git/info/refs?service=git-upload-pack", git=True, request_id=request_id)
                    valid = status == 200 and body.startswith(b"000eversion 2\n")
                result = "ok" if valid else (f"http_{status}" if status != 200 else "invalid_response")
            except (OSError, ValueError, KeyError, http.client.HTTPException):
                pass
            finally:
                finished = time.monotonic()
                record({"sequence": sequence, "request_id": request_id, "repository_id": entry["repository_id"], "ingress_index": ingress_index, "result": result,
                        "elapsed_ms": (finished - scheduled) * 1000,
                        "service_ms": (finished - dispatched) * 1000,
                        "dispatch_delay_ms": (dispatched - scheduled) * 1000})
                slots.release()

        with ThreadPoolExecutor(max_workers=args.concurrency) as executor:
            for sequence in range(total):
                scheduled = started + sequence / args.rate
                delay = scheduled - time.monotonic()
                if delay > 0:
                    time.sleep(delay)
                population = hot if args.distribution == "skewed" and generator.random() < .9 else active
                entry = generator.choice(population)
                if slots.acquire(blocking=False):
                    executor.submit(execute, sequence, entry, scheduled)
                else:
                    record({"sequence": sequence, "repository_id": entry["repository_id"],
                            "ingress_index": sequence % len(clients),
                            "result": "driver_busy", "elapsed_ms": None})
    elapsed = time.monotonic() - started
    result = {"version": 1, "started_at_utc": started_at,
              "corpus_repositories": len(manifest["repositories"]),
              "active_repositories": len(active), "distribution": args.distribution,
              "operation": args.operation, "seed": args.seed,
              "offered_rps": args.rate, "schedule_seconds": args.duration,
              "elapsed_including_drain_seconds": round(elapsed, 3),
              "concurrency": args.concurrency, "request_timeout_seconds": args.timeout,
              "scheduled": total, "outcomes": dict(counts),
              "ingresses": [{"index": index, "outcomes": dict(outcomes),
                             "scheduled_latency_ms": percentiles(ingress_latencies[index]),
                             "service_ms": percentiles(ingress_service_times[index])}
                            for index, outcomes in enumerate(ingress_counts)],
              "failed_arrivals": total - counts["ok"],
              "scheduled_latency_ms": percentiles(latencies),
              "service_ms": percentiles(service_times), "dispatch_delay_ms": percentiles(dispatch_times),
              "latency_population": "all completed HTTP attempts, including errors; driver_busy arrivals are counted failures without fabricated latency",
              "manifest_sha256": hashlib.sha256(args.manifest.read_bytes()).hexdigest()}
    if sum(counts.values()) != total:
        raise RuntimeError("benchmark lost scheduled outcomes")
    save(args.output, result)
    return result


def positive(value):
    parsed = int(value)
    if parsed < 1:
        raise argparse.ArgumentTypeError("must be positive")
    return parsed


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", required=True)
    parser.add_argument("--additional-base-url", action="append", default=[],
                        help="additional gateway ingress for round-robin run requests")
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--timeout", type=positive, default=10)
    parser.add_argument("--seed", type=int, default=20260926)
    commands = parser.add_subparsers(dest="command", required=True)
    create = commands.add_parser("seed")
    create.add_argument("--repositories", type=positive, default=1000)
    create.add_argument("--populated", type=positive, default=3)
    create.add_argument("--work-dir", type=Path, required=True)
    check = commands.add_parser("verify")
    check.add_argument("--work-dir", type=Path, required=True)
    run = commands.add_parser("run")
    run.add_argument("--active-repositories", type=positive, required=True)
    run.add_argument("--distribution", choices=("uniform", "skewed"), default="uniform")
    run.add_argument("--operation", choices=("metadata", "refs"), default="metadata")
    run.add_argument("--rate", type=positive, default=20)
    run.add_argument("--duration", type=positive, default=30)
    run.add_argument("--concurrency", type=positive, default=32)
    run.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    token = os.environ.get("CANOPY_GIT_TOKEN")
    if not token:
        parser.error("CANOPY_GIT_TOKEN is required")
    if args.command == "run" and args.concurrency > 256:
        parser.error("driver concurrency is limited to 256")
    if args.command != "run" and args.additional_base_url:
        parser.error("additional gateways are supported only for run")
    clients = [Client(base, token, args.timeout)
               for base in [args.base_url, *args.additional_base_url]]
    try:
        client = clients if args.command == "run" else clients[0]
        result = {"seed": seed, "verify": verify, "run": measure}[args.command](args, client, token)
        print(json.dumps(result, indent=2))
    finally:
        for client in clients:
            client.close()
    if args.command == "run" and result["failed_arrivals"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
