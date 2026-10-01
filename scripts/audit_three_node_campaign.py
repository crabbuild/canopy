#!/usr/bin/env python3
"""Independently audit closed campaign ledgers; never send Git/provider traffic.

Failed arrivals and interrupted/unbound windows remain visible. Ledger integrity
does not establish owner recovery, matched improvement or isolated capacity.
"""
import argparse
from collections import Counter
import hashlib
import json
import math
from pathlib import Path
import random
import re
import uuid

import benchmark_repositories as benchmark
import benchmark_three_node_campaign as campaign


def require(condition, message):
    if not condition:
        raise ValueError(message)


def finite(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value) and value >= 0


def quantiles(values):
    ordered = sorted(values)
    return {name: None if not ordered else round(ordered[math.ceil(q * len(ordered)) - 1], 3)
            for name, q in (("p50", .5), ("p95", .95), ("p99", .99), ("max", 1))}


def selected_repositories(entries, window, seed):
    operation = window["operation"]
    if operation == "create":
        return {sequence: None for sequence in range(window["rate"] * window["duration"])}
    if operation == "lfs_download":
        eligible = [entry for entry in entries if entry.get("lfs_oid") is not None]
    elif operation in ("incremental_fetch", "incremental_pull"):
        eligible = [entry for entry in entries if entry.get("base_commit") is not None]
    elif operation in campaign.GIT_READS | {"push_commit"}:
        eligible = [entry for entry in entries if entry["commit"] is not None]
    else:
        eligible = entries
    generator = random.Random(seed)
    active = generator.sample(eligible, window["active_repositories"])
    hot = active[:max(1, len(active) // 10)]
    result = {}
    for sequence in range(window["rate"] * window["duration"]):
        population = hot if window["distribution"] == "skewed" and generator.random() < .9 else active
        result[sequence] = generator.choice(population)["repository_id"]
    return result


def audit_window(path, window, manifest_digest, driver_digest, repository_ids, expected_selection=None):
    raw = path.read_bytes()
    report = json.loads(raw)
    require(report["version"] == 1 and report["manifest_sha256"] == manifest_digest
            and report["driver_sha256"] == driver_digest, "window provenance differs")
    for key, value in (("operation", window["operation"]), ("distribution", window["distribution"]),
                       ("active_repositories", window["active_repositories"]), ("offered_rps", window["rate"]),
                       ("schedule_seconds", window["duration"]), ("concurrency", window["concurrency"])):
        require(report[key] == value, f"window declaration differs: {key}")
    total = window["rate"] * window["duration"]
    require(isinstance(report["scheduled"], int) and not isinstance(report["scheduled"], bool)
            and report["scheduled"] == total and report["request_timeout_seconds"] == 30
            and report["seed"] == 20260926, "arrival clock/deadline/seed differs")
    if window["operation"] in campaign.GIT_READS | {"push_branch", "push_commit"}:
        require(report["git_timeout_seconds"] == 120, "Git deadline differs")
    if window["operation"] == "push_commit":
        require(report["git_payload_size_bytes"] == window["git_payload_bytes"], "Git payload differs")
    if window["operation"] == "lfs_upload":
        require(report["lfs_size_bytes"] == window["lfs_bytes"], "LFS payload differs")
    if window["operation"] == "create":
        require(isinstance(report["create_run_id"], str) and re.fullmatch(r"[0-9a-f]{32}", report["create_run_id"]), "invalid creation run ID")
    samples_path = path.with_suffix(".samples.jsonl")
    rows, seen, requests, digest = [], set(), set(), hashlib.sha256()
    with samples_path.open("rb") as samples:
        for line in iter(lambda: samples.readline(16 * 1024 + 1), b""):
            require(len(line) <= 16 * 1024, "oversized arrival sample")
            digest.update(line)
            row = json.loads(line)
            sequence = row["sequence"]
            require(isinstance(sequence, int) and not isinstance(sequence, bool)
                    and 0 <= sequence < total and sequence not in seen, "duplicate/out-of-range sequence")
            seen.add(sequence)
            require(type(row["ingress_index"]) is int and row["ingress_index"] == 0
                    and isinstance(row["result"], str), "unexpected ingress/outcome")
            if expected_selection is not None:
                require(row["repository_id"] == expected_selection[sequence], "deterministic active-set/distribution differs")
            if window["operation"] != "create":
                require(row["repository_id"] in repository_ids, "arrival repository outside corpus")
            else:
                require(row["repository_id"] is None, "creation selected an existing repository")
                require(row["created_name"] == f"create-{report['create_run_id']}-{sequence:07d}", "creation name differs")
                if row["result"] == "ok":
                    require(benchmark.canonical_repository_uuid(row.get("created_repository_id")), "creation ACK has invalid UUID")
            if row["result"] == "driver_busy":
                require(row["elapsed_ms"] is None, "busy arrival has fabricated latency")
            else:
                request_id = row["request_id"]
                require(isinstance(request_id, str) and str(uuid.UUID(request_id)) == request_id
                        and request_id not in requests, "invalid/duplicate request ID")
                requests.add(request_id)
                require(all(finite(row[key]) for key in ("elapsed_ms", "service_ms", "dispatch_delay_ms", "completion_offset_seconds")),
                        "invalid arrival timing")
                require(math.isclose(row["elapsed_ms"], row["service_ms"] + row["dispatch_delay_ms"], abs_tol=.001),
                        "elapsed/service/dispatch identity differs")
                expected_elapsed = (row["completion_offset_seconds"] - sequence / window["rate"]) * 1000
                require(math.isclose(row["elapsed_ms"], expected_elapsed, abs_tol=.01), "arrival clock identity differs")
            for key in ("git_push_command_ms", "git_client_preparation_ms"):
                require(row.get(key) is None or finite(row[key]), "invalid push phase timing")
            rows.append(row)
    require(digest.hexdigest() == report["samples_sha256"] == benchmark.file_sha256(samples_path), "arrival digest changed")
    require(len(rows) == len(seen) == total, "missing arrival sequences")
    outcomes = Counter(row["result"] for row in rows)
    require(all(type(value) is int and value >= 0 for value in report["outcomes"].values())
            and type(report["failed_arrivals"]) is int, "outcome counters are not integers")
    require(dict(outcomes) == report["outcomes"] and total - outcomes["ok"] == report["failed_arrivals"], "outcome accounting differs")
    require(report["error_fraction"] == (total - outcomes["ok"]) / total, "error fraction differs")
    if window["operation"] == "create":
        identifiers = [row["created_repository_id"] for row in rows if row["result"] == "ok"]
        require(len(set(identifiers)) == len(identifiers) == report["acknowledged_created_repositories"], "creation ACK count/uniqueness differs")
    dispatched = [row for row in rows if row["result"] != "driver_busy"]
    for field, key in (("elapsed_ms", "scheduled_latency_ms"), ("service_ms", "service_ms"),
                       ("dispatch_delay_ms", "dispatch_delay_ms")):
        require(quantiles([row[field] for row in dispatched]) == report[key], f"percentiles differ: {key}")
    require(report["ingresses"] == [{"index": 0, "outcomes": dict(outcomes),
            "scheduled_latency_ms": report["scheduled_latency_ms"], "service_ms": report["service_ms"]}],
            "per-ingress accounting differs")
    if window["operation"] == "push_commit":
        for key in ("git_push_command_ms", "git_client_preparation_ms"):
            require(quantiles([row[key] for row in rows if row.get(key) is not None]) == report[key], "push percentiles differ")
        require(report["acknowledged_new_git_payload_bytes"] == outcomes["ok"] * window["git_payload_bytes"], "acknowledged payload bytes differ")
    successes = sum(row["result"] == "ok" and row["completion_offset_seconds"] <= window["duration"] for row in rows)
    rate = round(successes / window["duration"], 3)
    require(successes == report["successful_completions_in_schedule_window"]
            and rate == report["successful_rps_in_schedule_window"], "in-window throughput differs")
    require(finite(report["elapsed_including_drain_seconds"]) and report["elapsed_including_drain_seconds"] >= window["duration"],
            "drain duration invalid")
    duration = report["elapsed_including_drain_seconds"]
    require(round(outcomes["ok"] / (duration + .0005), 3) <= report["successful_rps_including_drain"]
            <= round(outcomes["ok"] / (duration - .0005), 3), "drained throughput differs from rounded duration bounds")
    require(benchmark.file_sha256(path) == hashlib.sha256(raw).hexdigest(), "report changed during audit")
    return {"path": path.name, "sha256": hashlib.sha256(raw).hexdigest(), "samples_sha256": digest.hexdigest(),
            "operation": window["operation"], "distribution": window["distribution"],
            "active_repositories": window["active_repositories"], "offered_rps": window["rate"],
            "concurrency": window["concurrency"], "scheduled": total, "outcomes": dict(outcomes),
            "successful_rps_in_schedule_window": rate, "scheduled_latency_ms": report["scheduled_latency_ms"],
            "successful_only_scheduled_latency_ms": quantiles([row["elapsed_ms"] for row in rows if row["result"] == "ok"]),
            "service_ms": report["service_ms"], "dispatch_delay_ms": report["dispatch_delay_ms"]}


def audit_resources(directory, item, ready):
    for key in ("resources", "resource_boundary"):
        path = directory / item[key + "_path"]
        require(path.parent == directory and benchmark.file_sha256(path) == item[key + "_sha256"], "resource binding differs")
    boundary = json.loads((directory / item["resource_boundary_path"]).read_text())
    rows = [json.loads(line) for line in (directory / item["resources_path"]).read_text().splitlines()]
    require(rows and all(b["monotonic"] > a["monotonic"] for a, b in zip(rows, rows[1:])), "resource samples missing/unordered")
    required_pids = {ready["launcher_pid"], *(node["pid"] for node in ready["nodes"])}
    pids = {p["pid"] for p in boundary["before"]["processes"]}
    require(required_pids < pids and len(pids) == 5, "resource process inventory differs")
    for sample in [boundary["before"], *rows, boundary["after"]]:
        require(finite(sample["monotonic"]) and sample["proxy"]["fixture_id"] == ready["fixture_id"], "resource fixture differs")
        captured = sample["proxy"]["captured_monotonic"]
        require(finite(captured) and 0 <= sample["monotonic"] - captured <= 10, "proxy sample stale")
        require(len(sample["processes"]) == 5 and {p["pid"] for p in sample["processes"]} == pids
                and all(finite(p[key]) for p in sample["processes"] for key in ("cpu_seconds", "rss_kib", "ps_lifetime_cpu_percent")),
                "resource PID/counter inventory differs")
    require(boundary["after"]["monotonic"] > boundary["before"]["monotonic"], "resource boundary order differs")
    old = {p["pid"]: p for p in boundary["before"]["processes"]}
    require(all(p["cpu_seconds"] >= old[p["pid"]]["cpu_seconds"] for p in boundary["after"]["processes"]), "self CPU decreased")
    current = {p["pid"]: p for p in boundary["after"]["processes"]}
    node_cpu = {str(n["pid"]): current[n["pid"]]["cpu_seconds"] - old[n["pid"]]["cpu_seconds"] for n in ready["nodes"]}
    rss_peak = {str(n["pid"]): max(p["rss_kib"] for sample in [boundary["before"], *rows, boundary["after"]]
                for p in sample["processes"] if p["pid"] == n["pid"]) for n in ready["nodes"]}
    protocol_bytes = {}
    for key in ("client_bytes", "backend_bytes"):
        first = boundary["before"]["proxy"]["front"][key]
        last = boundary["after"]["proxy"]["front"][key]
        require(type(first) is int and type(last) is int and 0 <= first <= last, "proxy byte counter decreased")
        protocol_bytes[key] = last - first
    return {"samples": len(rows), "resource_boundary_sha256": item["resource_boundary_sha256"],
            "resources_sha256": item["resources_sha256"],
            "boundary_seconds_including_setup_and_drain": boundary["after"]["monotonic"] - boundary["before"]["monotonic"],
            "node_self_cpu_seconds": node_cpu, "node_peak_observed_rss_kib": rss_peak,
            "front_proxy_protocol_bytes": protocol_bytes,
            "scope": "ps self CPU/RSS and proxy protocol counters; includes client setup/drain, not live-child or Git-only CPU"}


def audit(directory, manifest_path, plan_path, fleet_dir):
    index_path = directory / "campaign.json"
    index_digest = benchmark.file_sha256(index_path)
    index = json.loads(index_path.read_text())
    manifest = benchmark.corpus(manifest_path)
    plan = json.loads(plan_path.read_text())
    schedule = campaign.windows(plan, manifest)
    ready_path = fleet_dir / "ready.json"
    ready = json.loads(ready_path.read_text())
    expected = {"manifest_sha256": benchmark.file_sha256(manifest_path), "plan_sha256": benchmark.file_sha256(plan_path),
                "fleet_ready_sha256": benchmark.file_sha256(ready_path), "driver_sha256": benchmark.file_sha256(Path(benchmark.__file__)),
                "campaign_sha256": benchmark.file_sha256(Path(campaign.__file__))}
    require(index["version"] == 1 and index["bindings"] == expected and index["fleet"] == ready, "campaign provenance differs")
    preflight = index["preflight"]
    require(preflight is not None and preflight["verified_repositories"] == len(manifest["repositories"])
            and preflight["git_v0_v2_samples"] == sum(e["commit"] is not None for e in manifest["repositories"])
            and all(preflight[key] == value for key, value in expected.items()), "full corpus preflight differs")
    require(ready["ready"] is True and ready["error"] is None and not ready["shutdown"]
            and len(ready["nodes"]) == 3 and len({n["node_id"] for n in ready["nodes"]}) == 3
            and [n["index"] for n in ready["nodes"]] == [0, 1, 2]
            and len({n["pid"] for n in ready["nodes"]}) == 3
            and ready["max_active_repositories_per_node"] == plan["node_active_limit"]
            and ready["public_url"] == ready["proxy_url"], "fleet topology differs")
    require(benchmark.file_sha256(Path(ready["binary_path"])) == ready["binary_sha256"], "binary changed")
    require(set(ready["fixture_scripts_sha256"]) == {"serve_three_gateways.py", "local_tcp_proxy.py", "smoke_s3_process.py", "smoke_s3_peers.py"}
            and all(benchmark.file_sha256(Path(__file__).parent / name) == digest
                    for name, digest in ready["fixture_scripts_sha256"].items()), "fixture script binding differs")
    bound = {item["path"]: item for item in index["reports"]}
    require(len(bound) == len(index["reports"]), "duplicate bound report")
    windows, paths = [], []
    for ordinal, window in enumerate(schedule):
        name = f"{ordinal:04d}-{window['id']}-r{window['repetition']}.json"
        path = directory / name
        if not path.exists():
            require(name not in bound, "bound report missing")
            continue
        checked = audit_window(path, window, expected["manifest_sha256"], expected["driver_sha256"],
                               {e["repository_id"] for e in manifest["repositories"]},
                               selected_repositories(manifest["repositories"], window, 20260926))
        item = bound.get(name)
        checked["resource_binding_complete"] = item is not None
        if item is not None:
            require(item["sha256"] == checked["sha256"] and item["operation"] == checked["operation"]
                    and item["outcomes"] == checked["outcomes"]
                    and item["failed_arrivals"] == checked["scheduled"] - checked["outcomes"].get("ok", 0), "index/window accounting differs")
            checked["resources"] = audit_resources(directory, item, ready)
        else:
            require(index["completed"] is False and index["current_window"] == path.stem, "unexpected unbound report")
        windows.append(checked)
        paths.append(path)
    require(set(bound) <= {p.name for p in paths}, "index references undeclared report")
    declared_names = {p.stem + ".samples.jsonl" for p in paths}
    orphan_ledgers = [p.name for p in directory.glob("*.samples.jsonl") if p.name not in declared_names]
    require(not orphan_ledgers, "arrival ledgers lack closed reports; retain and audit separately")
    creations = [p for p, row in zip(paths, windows) if row["operation"] == "create"]
    writes = [p for p, row in zip(paths, windows) if row["operation"] in ("push_branch", "push_commit", "lfs_upload")]
    creation_runs = benchmark.creation_runs(creations, manifest_path) if creations and any(
        row["outcomes"].get("ok", 0) for row in windows if row["operation"] == "create") else []
    acknowledged_creations = sum(len(rows) for _, rows, _ in creation_runs)
    write_counts = Counter()
    if writes and any(row["outcomes"].get("ok", 0) for row in windows if row["operation"] in ("push_branch", "push_commit", "lfs_upload")):
        runs = benchmark.write_runs(writes, manifest_path, {e["repository_id"]: e for e in manifest["repositories"]})
        for report, rows, _ in runs:
            write_counts[report["operation"]] += len(rows)
    complete = index["completed"] is True and len(windows) == len(bound) == len(schedule) and index["current_window"] is None
    require(index["completed"] is not True or complete, "completed campaign omitted declared windows")
    if complete:
        total_outcomes = Counter()
        for row in windows:
            total_outcomes.update(row["outcomes"])
        require(dict(total_outcomes) == index["outcomes"] and index["all_arrivals_succeeded"] is
                all(row["outcomes"].get("ok", 0) == row["scheduled"] for row in windows), "campaign outcome totals differ")
    require(benchmark.file_sha256(index_path) == index_digest, "campaign changed during audit")
    return {"version": 1, "integrity_verified": True, "schedule_completed": complete,
            "all_observed_arrivals_succeeded": all(row["outcomes"].get("ok", 0) == row["scheduled"] for row in windows),
            "declared_windows": len(schedule), "observed_windows": len(windows), "resource_bound_windows": len(bound),
            "acknowledged_creations": acknowledged_creations, "acknowledged_writes": dict(write_counts),
            "campaign_sha256": index_digest, "bindings": expected, "auditor_sha256": benchmark.file_sha256(Path(__file__)),
            "windows": windows, "scope": "Closed ledgers/resources only; failures retained. No owner-loss, delivered Git/LFS body, matched improvement or isolated-capacity proof."}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("directory", "manifest", "plan", "fleet-dir", "output"):
        parser.add_argument("--" + name, type=Path, required=True)
    parser.add_argument("--require-complete", action="store_true")
    args = parser.parse_args()
    require(not args.output.exists(), "audit requires new output")
    result = audit(args.directory, args.manifest, args.plan, args.fleet_dir)
    benchmark.save(args.output, result)
    print(json.dumps({key: value for key, value in result.items() if key != "windows"}, indent=2))
    require(not args.require_complete or result["schedule_completed"], "full declared schedule did not complete")


if __name__ == "__main__":
    main()
