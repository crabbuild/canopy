#!/usr/bin/env python3
"""Schedule complete critical Git workflows; preserve every attempted receipt.

Workflow throughput and step wall time include client work and validation. They
are not isolated RPC rates or server-only latency. Recovery requires every
attempted workflow to be complete; partial write ACKs cannot be silently skipped.
Owner loss, expiry, provider boundaries and full-corpus recovery are caller gates.
"""

import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from datetime import datetime, timezone
import json
import math
import os
from pathlib import Path
import threading
import time
from types import SimpleNamespace

import benchmark_repositories as benchmark
import benchmark_three_node_campaign as campaign
import check_proxy_git as critical


STEPS = ["create-source", "create-mirror", "atomic-multiple-ref-publication",
         "mixed-refusal", "atomic-refusal", "force-with-correct-lease", "stale-lease-refusal",
         "shallow-deepen-unshallow", "filtered-lazy-fetch-v0", "filtered-lazy-fetch-v2",
         "incremental-push", "fast-forward-pull", "delete-branch", "fetch-prune",
         "mirror-push", "invalid-credential-refusal", "exact-v0-v2-mirror-fsck"]


def validate_receipt(receipt):
    critical.require(receipt.get("complete") is True and receipt.get("error") is None,
                     "partial critical workflow; reconcile every write ACK before recovery")
    steps = receipt.get("steps", [])
    critical.require([step.get("name") for step in steps] == STEPS
                     and all(step.get("ok") is True for step in steps), "critical operation scope differs")
    entries = receipt.get("repositories", [])
    critical.require(len(entries) == 2 and len({entry["repository_id"] for entry in entries}) == 2
                     and all(benchmark.canonical_repository_uuid(entry["repository_id"]) for entry in entries),
                     "critical repository identities differ")


def summarize(samples, duration, elapsed):
    counts = Counter(sample["result"] for sample in samples)
    attempted = [sample for sample in samples if sample["result"] != "driver_busy"]
    in_window = sum(sample["result"] == "ok" and sample["completion_offset_seconds"] <= duration
                    for sample in attempted)
    steps = {}
    for sample in attempted:
        for step in sample.get("steps", []):
            critical.require(step.get("name") in STEPS and isinstance(step.get("ok"), bool)
                             and isinstance(step.get("wall_seconds"), (int, float))
                             and not isinstance(step["wall_seconds"], bool)
                             and math.isfinite(step["wall_seconds"]) and step["wall_seconds"] >= 0,
                             "invalid critical step measurement")
            times, outcomes = steps.setdefault(step["name"], ([], Counter()))
            times.append(step["wall_seconds"] * 1000)
            outcomes["ok" if step["ok"] else "failed"] += 1
    return {"outcomes": dict(counts), "failed_arrivals": len(samples) - counts["ok"],
            "successful_workflows_in_schedule_window": in_window,
            "successful_workflows_per_second_in_schedule_window": in_window / duration,
            "successful_workflows_per_second_including_drain": counts["ok"] / elapsed,
            "scheduled_workflow_latency_ms": benchmark.percentiles([sample["elapsed_ms"] for sample in attempted]),
            "service_workflow_ms": benchmark.percentiles([sample["service_ms"] for sample in attempted]),
            "step_wall_ms": {name: {"attempts": len(times), "outcomes": dict(outcomes),
                                    "percentiles": benchmark.percentiles(times)}
                             for name, (times, outcomes) in steps.items()},
            "step_timing_scope": "all recorded step attempts including failures; grouped Git commands/client validation, not isolated RPC/server latency",
            "latency_population": "all attempted workflows, including failures; busy arrivals have no fabricated latency"}


def bindings(directory, ready, binary):
    paths = [Path(__file__), Path(critical.__file__), Path(benchmark.__file__), Path(campaign.__file__),
             binary, directory / "ready.json", directory / "deployment.json",
             *(directory / f"node-{index}.json" for index in range(3))]
    return {str(path.resolve()): benchmark.file_sha256(path) for path in paths}


def run(args, token):
    critical.require(args.duration % args.interval == 0 and 1 <= args.concurrency <= 16
                     and args.duration // args.interval <= 1000, "declare bounded complete arrival clock")
    ready = campaign.validate_fleet(args.fleet_dir, {"node_active_limit": args.node_active_limit})
    critical.require(not (args.fleet_dir / "outcome.json").exists(), "fleet is terminal")
    binary = Path(ready["binary_path"])
    bound = bindings(args.fleet_dir, ready, binary)
    args.output_dir.mkdir(parents=True, exist_ok=False)
    total = args.duration // args.interval
    record = {"version": 1, "completed": False, "error": None, "bindings": bound,
              "fleet": ready, "scheduled": total, "interval_seconds": args.interval,
              "schedule_seconds": args.duration, "concurrency": args.concurrency,
              "http_timeout_seconds": args.timeout, "git_timeout_seconds": 120,
              "steps_per_workflow": STEPS, "started_at_utc": datetime.now(timezone.utc).isoformat(),
              "scope": "Complete concurrent critical workflows through three nodes/proxy. Not isolated operation RPS, "
                       "full-corpus recovery, owner-loss proof, provider cost or latest-source artifact qualification."}
    report = args.output_dir / "report.json"
    ledger = args.output_dir / "samples.jsonl"
    benchmark.save(report, record)
    slots = threading.BoundedSemaphore(args.concurrency)
    lock = threading.Lock()
    samples = []
    started = time.monotonic()
    with ledger.open("x") as out:
        def save(sample):
            with lock:
                samples.append(sample)
                out.write(json.dumps(sample) + "\n")
                out.flush()

        def execute(sequence, scheduled):
            dispatched = time.monotonic()
            receipt_path = args.output_dir / f"workflow-{sequence:04d}.json"
            sample = {"sequence": sequence, "result": "workflow_error", "receipt": receipt_path.name}
            try:
                with closing(benchmark.Client(ready["proxy_url"], token, args.timeout)) as client:
                    receipt = critical.seed(SimpleNamespace(receipt=receipt_path,
                        work_dir=args.output_dir / f"workflow-{sequence:04d}", binary=binary,
                        base_url=ready["proxy_url"], timeout=args.timeout), client, token)
                validate_receipt(receipt)
                sample["result"] = "ok"
            except Exception as error:
                sample["error"] = type(error).__name__
            finally:
                finished = time.monotonic()
                if receipt_path.exists():
                    sample["receipt_sha256"] = benchmark.file_sha256(receipt_path)
                    sample["steps"] = campaign.read_json(receipt_path).get("steps", [])
                sample.update(completion_offset_seconds=finished - started,
                              elapsed_ms=(finished - scheduled) * 1000,
                              service_ms=(finished - dispatched) * 1000)
                save(sample)
                slots.release()

        try:
            with ThreadPoolExecutor(max_workers=args.concurrency) as pool:
                for sequence in range(total):
                    scheduled = started + sequence * args.interval
                    delay = scheduled - time.monotonic()
                    if delay > 0:
                        time.sleep(delay)
                    if slots.acquire(blocking=False):
                        pool.submit(execute, sequence, scheduled)
                    else:
                        save({"sequence": sequence, "result": "driver_busy"})
                remaining = started + args.duration - time.monotonic()
                if remaining > 0:
                    time.sleep(remaining)
            critical.require(len(samples) == total and {sample["sequence"] for sample in samples} == set(range(total)),
                             "arrival ledger is incomplete")
            critical.require(all(benchmark.file_sha256(Path(path)) == digest for path, digest in bound.items()),
                             "bound workflow inputs changed")
            record["completed"] = True
        except BaseException as error:
            record["error"] = type(error).__name__
            raise
        finally:
            elapsed = time.monotonic() - started
            out.flush()
            record.update(summarize(samples, args.duration, elapsed),
                          elapsed_including_drain_seconds=elapsed,
                          samples_sha256=benchmark.file_sha256(ledger))
            benchmark.save(report, record)
    return record


def load_receipts(report_path):
    record = campaign.read_json(report_path)
    critical.require(record.get("version") == 1 and record.get("completed") is True
                     and record.get("error") is None and record.get("steps_per_workflow") == STEPS,
                     "require a closed complete critical schedule")
    critical.require(all(benchmark.file_sha256(Path(path)) == digest for path, digest in record["bindings"].items()),
                     "critical source/fleet/binary inputs changed")
    directory = report_path.parent
    ledger = directory / "samples.jsonl"
    critical.require(benchmark.file_sha256(ledger) == record["samples_sha256"], "critical ledger changed")
    samples = [json.loads(line) for line in ledger.read_text().splitlines()]
    critical.require(len(samples) == record["scheduled"]
                     and {sample["sequence"] for sample in samples} == set(range(record["scheduled"])),
                     "critical arrival inventory differs")
    totals = summarize(samples, record["schedule_seconds"], record["elapsed_including_drain_seconds"])
    critical.require(all(record.get(key) == value for key, value in totals.items()),
                     "critical report differs from arrival ledger")
    receipts, identities, expected_paths = [], set(), set()
    for sample in samples:
        if sample["result"] == "driver_busy":
            critical.require("receipt" not in sample, "busy arrival cannot discard a receipt")
            continue
        name = f"workflow-{sample['sequence']:04d}.json"
        critical.require(sample.get("receipt") == name and sample["result"] in ("ok", "workflow_error"),
                         "unexpected receipt path/outcome")
        path = directory / name
        expected_paths.add(name)
        critical.require(benchmark.file_sha256(path) == sample["receipt_sha256"], "critical receipt changed")
        receipt = campaign.read_json(path)
        critical.require(sample.get("steps") == receipt.get("steps"), "step ledger differs from receipt")
        validate_receipt(receipt)
        critical.require(receipt["binary_sha256"] == record["fleet"]["binary_sha256"]
                         and receipt["driver_sha256"] == record["bindings"][str(Path(critical.__file__).resolve())]
                         and receipt["git_driver_sha256"] == record["bindings"][str(Path(benchmark.__file__).resolve())],
                         "receipt source/binary differs")
        for entry in receipt["repositories"]:
            critical.require(entry["repository_id"] not in identities, "duplicate critical workflow identity")
            identities.add(entry["repository_id"])
        receipts.append((path, receipt))
    critical.require({path.name for path in directory.glob("workflow-*.json")} == expected_paths,
                     "orphan critical receipt; reconcile before recovery")
    return record, receipts


def verify(args, token):
    record, receipts = load_receipts(args.report)
    critical.require(not args.output.exists(), "requires new recovery output")
    ready = campaign.validate_fleet(args.fleet_dir, {"node_active_limit": args.node_active_limit})
    old = record["fleet"]["nodes"]
    critical.require(not {node["node_id"] for node in old}.intersection(node["node_id"] for node in ready["nodes"])
                     and not {node["pid"] for node in old}.intersection(node["pid"] for node in ready["nodes"])
                     and ready["binary_sha256"] == record["fleet"]["binary_sha256"],
                     "requires distinct owners with the original binary")
    args.work_dir.mkdir(parents=True, exist_ok=False)
    checks = []
    with closing(benchmark.Client(ready["proxy_url"], token, args.timeout)) as client:
        for index, (path, receipt) in enumerate(receipts):
            try:
                value = {"verified": True, **critical.verify_receipt(receipt, ready["proxy_url"],
                    args.work_dir / f"workflow-{index:04d}", client, token)}
            except Exception as error:
                value = {"verified": False, "error": type(error).__name__}
            checks.append({"receipt": path.name, "receipt_sha256": benchmark.file_sha256(path), **value})
    load_receipts(args.report)
    result = {"version": 1, "completed": True, "report_sha256": benchmark.file_sha256(args.report),
              "attempted_workflow_verifications": len(checks),
              "all_workflows_verified": bool(checks) and all(check["verified"] for check in checks),
              "verified_workflows": sum(check["verified"] for check in checks),
              "verified_repositories": sum(check["verified"] for check in checks) * 2,
              "checks": checks, "failed_load_arrivals": record["failed_arrivals"],
              "owner_recovery": "not established here: caller must bind actual old-process absence, expiry wait, "
                                "fresh local state, unchanged provider and full original corpus"}
    benchmark.save(args.output, result)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fleet-dir", required=True, type=Path)
    parser.add_argument("--node-active-limit", required=True, type=benchmark.positive)
    parser.add_argument("--timeout", type=benchmark.positive, default=30)
    actions = parser.add_subparsers(dest="action", required=True)
    create = actions.add_parser("run")
    create.add_argument("--output-dir", type=Path, required=True)
    create.add_argument("--duration", type=benchmark.positive, default=300)
    create.add_argument("--interval", type=benchmark.positive, default=15)
    create.add_argument("--concurrency", type=benchmark.positive, default=4)
    recover = actions.add_parser("verify")
    recover.add_argument("--report", type=Path, required=True)
    recover.add_argument("--work-dir", type=Path, required=True)
    recover.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    token = os.environ.get("CANOPY_GIT_TOKEN")
    if not token:
        parser.error("CANOPY_GIT_TOKEN is required")
    result = run(args, token) if args.action == "run" else verify(args, token)
    print(json.dumps(result, indent=2))
    if result.get("failed_arrivals", result.get("failed_load_arrivals", 0)) or result.get("all_workflows_verified") is False:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
