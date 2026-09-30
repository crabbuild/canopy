#!/usr/bin/env python3
"""Run an explicit, repeated load plan through a live three-node fixture.

Validate the complete corpus and every populated Git/LFS fixture before load.
Keep failed load windows, arrival ledgers and resource observations. This does
not kill/restart owners, qualify Linux capacity, or silently retry a workload.
"""

import argparse
from collections import Counter
import json
import math
import os
from pathlib import Path
import re
import subprocess
import threading
import time
from types import SimpleNamespace

import benchmark_repositories as benchmark


OPERATIONS = {"create", "metadata", "refs", "ls_remote", "clone", "cold_fetch",
              "incremental_fetch", "incremental_pull", "push_branch", "push_commit", "lfs_upload", "lfs_download"}
GIT_READS = {"ls_remote", "clone", "cold_fetch", "incremental_fetch", "incremental_pull"}


def read_json(path):
    with path.open("rb") as source:
        body = source.read(2 * 1024 * 1024 + 1)
    if len(body) > 2 * 1024 * 1024:
        raise ValueError("campaign input exceeds 2 MiB")
    return json.loads(body)


def integer(value, lower, upper):
    return isinstance(value, int) and not isinstance(value, bool) and lower <= value <= upper


def windows(plan, manifest):
    entries = manifest["repositories"]
    if (plan.get("version") != 1 or not integer(plan.get("corpus_repositories"), 1, 1_000_000)
            or plan["corpus_repositories"] != len(entries)
            or not integer(plan.get("node_active_limit"), 1, 9999)):
        raise ValueError("plan must declare the exact corpus size and node active limit")
    if len({entry["repository_id"] for entry in entries}) != len(entries):
        raise ValueError("campaign corpus identities must be unique")
    rows = plan.get("windows")
    if not isinstance(rows, list) or not 1 <= len(rows) <= 1000:
        raise ValueError("plan requires 1..1000 explicit windows")
    expanded, names = [], set()
    for row in rows:
        if not isinstance(row, dict):
            raise ValueError("window must be an object")
        name, operation = row.get("id"), row.get("operation")
        if not isinstance(name, str) or not re.fullmatch(r"[a-z0-9-]{1,64}", name) or name in names:
            raise ValueError("window IDs must be unique lowercase names")
        names.add(name)
        if not isinstance(operation, str) or operation not in OPERATIONS:
            raise ValueError("unknown campaign operation")
        if row.get("distribution") not in ("uniform", "skewed"):
            raise ValueError("window must declare its distribution")
        rate, duration = row.get("rate"), row.get("duration")
        if not integer(rate, 1, 10000) or not integer(duration, 1, 3600) or rate * duration > 1_000_000:
            raise ValueError("invalid arrival clock or too many arrivals")
        if not integer(row.get("concurrency"), 1, 256) or not integer(row.get("repetitions"), 1, 20):
            raise ValueError("declare bounded concurrency and repetitions")
        active = row.get("active_repositories")
        eligible = (sum(entry.get("lfs_oid") is not None for entry in entries) if operation == "lfs_download"
                    else sum(entry.get("base_commit") is not None for entry in entries)
                    if operation in ("incremental_fetch", "incremental_pull")
                    else sum(entry["commit"] is not None for entry in entries) if operation in GIT_READS or operation == "push_commit"
                    else len(entries))
        if operation == "create":
            if active is not None:
                raise ValueError("creation does not select an existing active set")
        elif not integer(active, 1, eligible):
            raise ValueError("window active set exceeds eligible corpus")
        size = row.get("lfs_bytes", 1024 * 1024)
        if not integer(size, 1, 16 * 1024 * 1024):
            raise ValueError("invalid LFS payload size")
        if operation == "lfs_upload" and size * row["concurrency"] > 256 * 1024 * 1024:
            raise ValueError("LFS in-flight payload exceeds driver bound")
        git_size = row.get("git_payload_bytes", 256 * 1024)
        if (not integer(git_size, 1, 16 * 1024 * 1024)
                or operation == "push_commit" and git_size * row["concurrency"] > 256 * 1024 * 1024):
            raise ValueError("invalid or excessive new Git payload")
        for repetition in range(row["repetitions"]):
            expanded.append({**row, "active_repositories": active, "lfs_bytes": size,
                             "git_payload_bytes": git_size,
                             "repetition": repetition + 1})
    if len(expanded) > 1000:
        raise ValueError("expanded campaign exceeds 1000 windows")
    return expanded


def validate_fleet(directory, plan):
    ready = read_json(directory / "ready.json")
    nodes = ready.get("nodes", [])
    if (ready.get("ready") is not True or ready.get("error") is not None
            or ready.get("shutdown") or len(nodes) != 3
            or [node.get("index") for node in nodes] != [0, 1, 2]
            or len({node["node_id"] for node in nodes}) != 3
            or len({node["pid"] for node in nodes}) != 3
            or ready.get("max_active_repositories_per_node") != plan["node_active_limit"]):
        raise ValueError("require three distinct ready nodes with the declared admission limit")
    if ready.get("public_url") != ready.get("proxy_url") or not ready.get("fixture_id"):
        raise ValueError("fleet must advertise the proxy and bind its live metrics")
    benchmark.Client(ready["proxy_url"], "validation-only", 1).close()
    binary = Path(ready["binary_path"])
    if benchmark.file_sha256(binary) != ready["binary_sha256"]:
        raise ValueError("server binary changed since fleet startup")
    for name, digest in ready["fixture_scripts_sha256"].items():
        if Path(name).name != name or benchmark.file_sha256(Path(__file__).parent / name) != digest:
            raise ValueError("fleet scripts changed since startup; launch a matched fresh fleet")
    expected = {"serve_three_gateways.py", "local_tcp_proxy.py", "smoke_s3_process.py", "smoke_s3_peers.py"}
    if set(ready["fixture_scripts_sha256"]) != expected:
        raise ValueError("fleet omitted a fixture source binding")
    for index, node in enumerate(nodes):
        command = subprocess.check_output(["ps", "-p", str(node["pid"]), "-o", "command="], text=True).strip()
        configuration = str(directory / f"node-{index}.json")
        if command != f"{binary} {configuration}":
            raise ValueError("node PID no longer identifies its declared binary/configuration")
    return ready


def cpu_seconds(value):
    days, _, clock = value.partition("-")
    if not clock:
        clock, days = days, "0"
    fields = [float(field) for field in clock.split(":")]
    if len(fields) not in (2, 3) or any(not math.isfinite(field) or field < 0 for field in fields):
        raise ValueError("invalid ps CPU clock")
    return int(days) * 86400 + sum(field * 60**index for index, field in enumerate(reversed(fields)))


def observe(directory, ready):
    if (directory / "outcome.json").exists():
        raise RuntimeError("fixture is terminal")
    pids = [ready["launcher_pid"], *(node["pid"] for node in ready["nodes"]), os.getpid()]
    output = subprocess.check_output(["ps", "-p", ",".join(map(str, pids)),
                                      "-o", "pid=,pcpu=,rss=,time="], text=True)
    processes = []
    for line in output.splitlines():
        pid, cpu, rss, clock = line.split()
        processes.append({"pid": int(pid), "ps_lifetime_cpu_percent": float(cpu),
                          "rss_kib": int(rss), "cpu_seconds": cpu_seconds(clock)})
    if {item["pid"] for item in processes} != set(pids):
        raise RuntimeError("a campaign or fleet process is missing")
    metrics = read_json(directory / "proxy-metrics.json")
    captured = metrics.get("captured_monotonic")
    now = time.monotonic()
    if (metrics.get("fixture_id") != ready["fixture_id"] or not isinstance(captured, (int, float))
            or not math.isfinite(captured) or not 0 <= now - captured <= 10):
        raise RuntimeError("proxy metrics are stale or belong to another fixture")
    return {"monotonic": now, "processes": processes, "proxy": metrics}


class Monitor:
    def __init__(self, directory, ready, path):
        self.directory, self.ready, self.path = directory, ready, path
        self.stop, self.failure, self.count = threading.Event(), None, 0

    def __enter__(self):
        def collect():
            try:
                with self.path.open("x") as output:
                    while True:
                        output.write(json.dumps(observe(self.directory, self.ready)) + "\n")
                        output.flush()
                        self.count += 1
                        if self.stop.wait(1):
                            break
            except BaseException as error:
                self.failure = type(error).__name__
        self.worker = threading.Thread(target=collect, daemon=True)
        self.worker.start()
        return self

    def __exit__(self, *_):
        self.stop.set()
        self.worker.join(timeout=10)
        if self.worker.is_alive() or self.failure or not self.count:
            raise RuntimeError("resource observation failed; retain the load report but do not qualify it")


def campaign(args, token):
    manifest = benchmark.corpus(args.manifest)
    plan = read_json(args.plan)
    schedule = windows(plan, manifest)
    ready = validate_fleet(args.fleet_dir, plan)
    observe(args.fleet_dir, ready)
    bindings = {"manifest_sha256": benchmark.file_sha256(args.manifest),
                "plan_sha256": benchmark.file_sha256(args.plan),
                "fleet_ready_sha256": benchmark.file_sha256(args.fleet_dir / "ready.json"),
                "campaign_sha256": benchmark.file_sha256(Path(__file__)),
                "driver_sha256": benchmark.file_sha256(Path(benchmark.__file__))}
    args.output_dir.mkdir(parents=True, exist_ok=False)
    result = {"version": 1, "completed": False, "error": None, "bindings": bindings,
              "current_window": None,
              "fleet": ready, "reports": [], "preflight": None,
              "qualification": "load observations only; owner-loss recovery and Linux capacity are not established"}
    client = benchmark.Client(ready["proxy_url"], token, args.timeout)
    try:
        checked = benchmark.verify(SimpleNamespace(manifest=args.manifest,
            base_url=ready["proxy_url"], work_dir=args.output_dir / "preflight-clones",
            concurrency=args.verify_concurrency), client, token)
        result["preflight"] = {**checked, **bindings}
        benchmark.save(args.output_dir / "preflight.json", result["preflight"])
        # Executors use new worker threads in each window. Their thread-local
        # keep-alive sockets must not survive into the next window's budget.
        client.close()
        def check_bindings():
            if (benchmark.file_sha256(args.manifest) != bindings["manifest_sha256"]
                    or benchmark.file_sha256(Path(benchmark.__file__)) != bindings["driver_sha256"]
                    or benchmark.file_sha256(Path(__file__)) != bindings["campaign_sha256"]
                    or benchmark.file_sha256(args.plan) != bindings["plan_sha256"]
                    or benchmark.file_sha256(args.fleet_dir / "ready.json") != bindings["fleet_ready_sha256"]):
                raise RuntimeError("campaign inputs changed; do not mix candidates")
            validate_fleet(args.fleet_dir, plan)
        for index, window in enumerate(schedule):
            check_bindings()
            tag = f"{index:04d}-{window['id']}-r{window['repetition']}"
            output = args.output_dir / f"{tag}.json"
            observation = args.output_dir / f"{tag}.resources.jsonl"
            parameters = SimpleNamespace(**{key: window[key] for key in (
                "operation", "active_repositories", "distribution", "rate", "duration", "concurrency", "lfs_bytes",
                "git_payload_bytes")},
                manifest=args.manifest, seed=args.seed, timeout=args.timeout,
                git_timeout=args.git_timeout, output=output, work_dir=args.output_dir / f"{tag}-clients")
            print(f"window {index + 1}/{len(schedule)}: {tag}", flush=True)
            result["current_window"] = tag
            benchmark.save(args.output_dir / "campaign.json", result)
            client = benchmark.Client(ready["proxy_url"], token, args.timeout)
            try:
                with Monitor(args.fleet_dir, ready, observation):
                    before = observe(args.fleet_dir, ready)
                    report = benchmark.measure(parameters, client, token)
                    client.close()
                    after = observe(args.fleet_dir, ready)
            finally:
                client.close()
            check_bindings()
            if report["manifest_sha256"] != bindings["manifest_sha256"] or report["driver_sha256"] != bindings["driver_sha256"]:
                raise RuntimeError("load report does not match campaign bindings")
            boundary = args.output_dir / f"{tag}.resource-boundary.json"
            benchmark.save(boundary, {"before": before, "after": after,
                "scope": "includes workload preparation and drain; proxy counters include protocol bytes, not only Git/LFS payload"})
            result["reports"].append({"path": output.name, "sha256": benchmark.file_sha256(output),
                "resources_path": observation.name, "resources_sha256": benchmark.file_sha256(observation),
                "resource_boundary_path": boundary.name, "resource_boundary_sha256": benchmark.file_sha256(boundary),
                "operation": report["operation"], "outcomes": report["outcomes"],
                "failed_arrivals": report["failed_arrivals"]})
            result["current_window"] = None
            benchmark.save(args.output_dir / "campaign.json", result)
        result["completed"] = True
        outcomes = Counter()
        for report in result["reports"]:
            outcomes.update(report["outcomes"])
        result["outcomes"] = dict(outcomes)
        result["all_arrivals_succeeded"] = all(row["failed_arrivals"] == 0 for row in result["reports"])
        return result
    except BaseException as error:
        result["error"] = type(error).__name__
        raise
    finally:
        client.close()
        benchmark.save(args.output_dir / "campaign.json", result)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--plan", type=Path, required=True)
    parser.add_argument("--fleet-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--timeout", type=benchmark.positive, default=30)
    parser.add_argument("--git-timeout", type=benchmark.positive, default=120)
    parser.add_argument("--verify-concurrency", type=benchmark.positive, default=16)
    parser.add_argument("--seed", type=int, default=20260926)
    args = parser.parse_args()
    token = os.environ.get("CANOPY_GIT_TOKEN")
    if not token or args.verify_concurrency > 32:
        parser.error("require CANOPY_GIT_TOKEN and verify concurrency at most 32")
    result = campaign(args, token)
    print(json.dumps(result, indent=2))
    if not result["all_arrivals_succeeded"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
