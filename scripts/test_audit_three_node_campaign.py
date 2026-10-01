"""Offline audit fixtures only; no server, process signals or remote traffic."""
import copy
import json
from pathlib import Path
import tempfile
import unittest
import uuid

import audit_three_node_campaign as auditor
import benchmark_repositories as benchmark
import benchmark_three_node_campaign as campaign


class AuditTests(unittest.TestCase):
    def fixture(self, root):
        identifier = str(uuid.UUID(int=1, version=4))
        window = {"id": "metadata", "operation": "metadata", "distribution": "uniform", "active_repositories": 1,
                  "rate": 4, "duration": 1, "concurrency": 1, "repetitions": 1}
        rows = []
        for sequence, outcome, elapsed, service, dispatch, completion in (
                (0, "ok", 100, 90, 10, .1), (1, "driver_busy", None, None, None, None),
                (2, "transport_error", 20, 15, 5, .52), (3, "ok", 270, 260, 10, 1.02)):
            row = {"sequence": sequence, "repository_id": identifier, "ingress_index": 0, "result": outcome, "elapsed_ms": elapsed}
            if elapsed is not None:
                row.update(request_id=str(uuid.UUID(int=sequence+10, version=4)), service_ms=service,
                           dispatch_delay_ms=dispatch, completion_offset_seconds=completion)
            rows.append(row)
        report = {"version": 1, "manifest_sha256": "manifest", "driver_sha256": "driver", "seed": 20260926,
                  "operation": "metadata", "distribution": "uniform", "active_repositories": 1, "offered_rps": 4,
                  "schedule_seconds": 1, "concurrency": 1, "scheduled": 4, "request_timeout_seconds": 30,
                  "outcomes": {"ok": 2, "driver_busy": 1, "transport_error": 1}, "failed_arrivals": 2, "error_fraction": .5,
                  "scheduled_latency_ms": {"p50": 100, "p95": 270, "p99": 270, "max": 270},
                  "service_ms": {"p50": 90, "p95": 260, "p99": 260, "max": 260},
                  "dispatch_delay_ms": {"p50": 10, "p95": 10, "p99": 10, "max": 10},
                  "successful_completions_in_schedule_window": 1, "successful_rps_in_schedule_window": 1.,
                  "elapsed_including_drain_seconds": 1.05, "successful_rps_including_drain": 1.905}
        report["ingresses"] = [{"index": 0, "outcomes": report["outcomes"],
                               "scheduled_latency_ms": report["scheduled_latency_ms"], "service_ms": report["service_ms"]}]
        return root / "0000-metadata-r1.json", window, rows, report, identifier

    def write(self, path, rows, report):
        with path.with_suffix(".samples.jsonl").open("w") as out:
            for row in rows:
                out.write(json.dumps(row) + "\n")
        report["samples_sha256"] = benchmark.file_sha256(path.with_suffix(".samples.jsonl"))
        benchmark.save(path, report)

    def test_latency_populations_busy_and_late_success(self):
        with tempfile.TemporaryDirectory() as directory:
            path, window, rows, report, identifier = self.fixture(Path(directory))
            self.write(path, rows, report)
            result = auditor.audit_window(path, window, "manifest", "driver", {identifier}, dict.fromkeys(range(4), identifier))
            self.assertEqual(result["outcomes"]["driver_busy"], 1)
            self.assertEqual(result["successful_rps_in_schedule_window"], 1)
            self.assertEqual(result["scheduled_latency_ms"]["p50"], 100)
            self.assertEqual(result["successful_only_scheduled_latency_ms"]["p50"], 100)
            self.assertEqual(result["successful_only_scheduled_latency_ms"]["p95"], 270)

    def test_corrupt_accounting_timing_and_selection_rejected(self):
        for change in ("missing", "duplicate", "unknown_repo", "wrong_selection", "busy_latency", "negative", "timing_identity",
                       "request_duplicate", "rate", "concurrency", "quantile", "throughput", "drained_rate", "outcomes", "seed", "ingress"):
            with self.subTest(change=change), tempfile.TemporaryDirectory() as directory:
                path, window, rows, report, identifier = self.fixture(Path(directory))
                selection = dict.fromkeys(range(4), identifier)
                if change == "missing":
                    rows.pop()
                elif change == "duplicate":
                    rows[3]["sequence"] = 0
                elif change == "unknown_repo":
                    rows[3]["repository_id"] = "outside"
                elif change == "wrong_selection":
                    selection[3] = "other"
                elif change == "busy_latency":
                    rows[1]["elapsed_ms"] = 0
                elif change == "negative":
                    rows[0]["service_ms"] = -1
                elif change == "timing_identity":
                    rows[0]["completion_offset_seconds"] = .2
                elif change == "request_duplicate":
                    rows[3]["request_id"] = rows[0]["request_id"]
                elif change == "rate":
                    report["offered_rps"] = 5
                elif change == "concurrency":
                    report["concurrency"] = 2
                elif change == "quantile":
                    report["scheduled_latency_ms"]["p50"] = 20
                elif change == "throughput":
                    report["successful_completions_in_schedule_window"] = 2
                elif change == "drained_rate":
                    report["successful_rps_including_drain"] = 2.
                elif change == "outcomes":
                    report["outcomes"]["ok"] = 3
                elif change == "seed":
                    report["seed"] = 1
                else:
                    report["ingresses"][0]["index"] = 1
                self.write(path, rows, report)
                with self.assertRaises(ValueError):
                    auditor.audit_window(path, window, "manifest", "driver", {identifier}, selection)

    def test_changed_samples_digest_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            path, window, rows, report, identifier = self.fixture(Path(directory))
            self.write(path, rows, report)
            with path.with_suffix(".samples.jsonl").open("a") as out:
                out.write("{}\n")
            with self.assertRaises((ValueError, KeyError)):
                auditor.audit_window(path, window, "manifest", "driver", {identifier})

    def test_deterministic_selection_preserves_uniform_skew_and_git_eligibility(self):
        entries = [{"repository_id": str(i), "commit": "tip" if i < 10 else None, "base_commit": "base" if i < 10 else None}
                   for i in range(20)]
        window = {"operation": "clone", "active_repositories": 10, "distribution": "uniform", "rate": 1000, "duration": 1}
        uniform = auditor.selected_repositories(entries, window, 20260926)
        skewed = auditor.selected_repositories(entries, {**window, "distribution": "skewed"}, 20260926)
        self.assertEqual(uniform, auditor.selected_repositories(entries, window, 20260926))
        self.assertEqual(set(uniform.values()), {str(i) for i in range(10)})
        self.assertGreater(max(list(skewed.values()).count(i) for i in set(skewed.values())), 850)
        self.assertEqual(set(auditor.selected_repositories(entries, {**window, "operation": "create"}, 20260926).values()), {None})

    def test_creation_ack_counts_and_duplicate_uuid_rejected(self):
        for corrupt in (False, True):
            with self.subTest(corrupt=corrupt), tempfile.TemporaryDirectory() as directory:
                path, window, rows, report, identifier = self.fixture(Path(directory))
                window.update(operation="create", active_repositories=None)
                report.update(operation="create", active_repositories=None, create_run_id="1"*32, acknowledged_created_repositories=2)
                for row in rows:
                    row.update(repository_id=None, created_name=f"create-{'1'*32}-{row['sequence']:07d}",
                               created_repository_id=str(uuid.UUID(int=row["sequence"]+100, version=4)) if row["result"] == "ok" else None)
                if corrupt:
                    rows[3]["created_repository_id"] = rows[0]["created_repository_id"]
                self.write(path, rows, report)
                if corrupt:
                    with self.assertRaises(ValueError):
                        auditor.audit_window(path, window, "manifest", "driver", {identifier})
                else:
                    self.assertEqual(auditor.audit_window(path, window, "manifest", "driver", {identifier})["outcomes"]["ok"], 2)

    def test_fresh_push_phase_population_and_payload_accounting(self):
        for corrupt in (None, "payload", "phase_quantile", "git_deadline"):
            with self.subTest(corrupt=corrupt), tempfile.TemporaryDirectory() as directory:
                path, window, rows, report, identifier = self.fixture(Path(directory))
                window.update(operation="push_commit", git_payload_bytes=256)
                report.update(operation="push_commit", git_timeout_seconds=120, git_payload_size_bytes=256,
                              acknowledged_new_git_payload_bytes=512)
                for row in rows:
                    if row["result"] == "ok":
                        row.update(git_push_command_ms=10, git_client_preparation_ms=5)
                report["git_push_command_ms"] = dict.fromkeys(("p50", "p95", "p99", "max"), 10)
                report["git_client_preparation_ms"] = dict.fromkeys(("p50", "p95", "p99", "max"), 5)
                if corrupt == "payload":
                    report["acknowledged_new_git_payload_bytes"] = 768
                elif corrupt == "phase_quantile":
                    report["git_push_command_ms"]["p95"] = 11
                elif corrupt == "git_deadline":
                    report["git_timeout_seconds"] = 240
                self.write(path, rows, report)
                if corrupt is not None:
                    with self.assertRaises(ValueError):
                        auditor.audit_window(path, window, "manifest", "driver", {identifier})
                else:
                    self.assertEqual(auditor.audit_window(path, window, "manifest", "driver", {identifier})["operation"], "push_commit")

    def campaign_fixture(self, root):
        path, window, rows, report, identifier = self.fixture(root)
        manifest_path, plan_path, fleet = root / "manifest.json", root / "plan.json", root / "fleet"
        fleet.mkdir()
        benchmark.save(manifest_path, {"version": 1, "complete": True, "requested_repositories": 1,
            "repositories": [{"name": "repo", "owner": "canopy", "repository_id": identifier, "commit": None}]})
        benchmark.save(plan_path, {"version": 1, "corpus_repositories": 1, "node_active_limit": 100, "windows": [window]})
        binary = root / "fake-binary"
        binary.write_bytes(b"offline fixture; never executed")
        ready = {"ready": True, "error": None, "shutdown": [], "fixture_id": "fixture", "launcher_pid": 1,
                 "nodes": [{"index": i, "pid": i+2, "node_id": str(uuid.uuid4())} for i in range(3)],
                 "max_active_repositories_per_node": 100, "public_url": "http://127.0.0.1:1", "proxy_url": "http://127.0.0.1:1",
                 "binary_path": str(binary), "binary_sha256": benchmark.file_sha256(binary),
                 "fixture_scripts_sha256": {name: benchmark.file_sha256(Path(__file__).parent / name) for name in
                    ("serve_three_gateways.py", "local_tcp_proxy.py", "smoke_s3_process.py", "smoke_s3_peers.py")}}
        benchmark.save(fleet / "ready.json", ready)
        bindings = {"manifest_sha256": benchmark.file_sha256(manifest_path), "plan_sha256": benchmark.file_sha256(plan_path),
                    "fleet_ready_sha256": benchmark.file_sha256(fleet / "ready.json"),
                    "driver_sha256": benchmark.file_sha256(Path(benchmark.__file__)),
                    "campaign_sha256": benchmark.file_sha256(Path(campaign.__file__))}
        report.update(manifest_sha256=bindings["manifest_sha256"], driver_sha256=bindings["driver_sha256"])
        self.write(path, rows, report)
        def resource(when, cpu):
            return {"monotonic": when, "processes": [{"pid": pid, "cpu_seconds": cpu, "rss_kib": 1024,
                    "ps_lifetime_cpu_percent": 1.} for pid in range(1, 6)],
                    "proxy": {"fixture_id": "fixture", "captured_monotonic": when-.1,
                              "front": {"client_bytes": int(cpu*100), "backend_bytes": int(cpu*200)}}}
        boundary = path.with_name(path.stem + ".resource-boundary.json")
        resources = path.with_name(path.stem + ".resources.jsonl")
        benchmark.save(boundary, {"before": resource(10, 1), "after": resource(11.1, 2)})
        with resources.open("w") as out:
            out.write(json.dumps(resource(10.25, 1.25)) + "\n")
            out.write(json.dumps(resource(10.75, 1.75)) + "\n")
        item = {"path": path.name, "sha256": benchmark.file_sha256(path), "operation": "metadata",
                "outcomes": report["outcomes"], "failed_arrivals": 2, "resources_path": resources.name,
                "resources_sha256": benchmark.file_sha256(resources), "resource_boundary_path": boundary.name,
                "resource_boundary_sha256": benchmark.file_sha256(boundary)}
        index = {"version": 1, "bindings": bindings, "fleet": ready, "completed": True, "current_window": None,
                 "preflight": {"verified_repositories": 1, "git_v0_v2_samples": 0, **bindings},
                 "reports": [item], "outcomes": report["outcomes"], "all_arrivals_succeeded": False}
        benchmark.save(root / "campaign.json", index)
        return manifest_path, plan_path, fleet, index

    def test_complete_schedule_can_retain_failed_arrivals_without_claiming_recovery(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manifest, plan, fleet, _ = self.campaign_fixture(root)
            result = auditor.audit(root, manifest, plan, fleet)
            self.assertTrue(result["schedule_completed"])
            self.assertFalse(result["all_observed_arrivals_succeeded"])
            self.assertEqual(result["windows"][0]["resources"]["node_self_cpu_seconds"], {"2": 1, "3": 1, "4": 1})
            self.assertEqual(result["windows"][0]["resources"]["front_proxy_protocol_bytes"], {"client_bytes": 100, "backend_bytes": 200})

    def test_partial_window_retained_without_resource_qualification(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manifest, plan, fleet, index = self.campaign_fixture(root)
            index.update(completed=False, current_window="0000-metadata-r1", reports=[])
            benchmark.save(root / "campaign.json", index)
            result = auditor.audit(root, manifest, plan, fleet)
            self.assertFalse(result["schedule_completed"])
            self.assertFalse(result["windows"][0]["resource_binding_complete"])

    def test_campaign_binding_preflight_and_resource_corruption_rejected(self):
        for change in ("preflight", "omitted", "resource_digest", "resource_pid", "resource_clock", "resource_cpu", "binary", "orphan"):
            with self.subTest(change=change), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                manifest, plan, fleet, index = self.campaign_fixture(root)
                if change == "preflight":
                    index["preflight"]["verified_repositories"] = 0
                elif change == "omitted":
                    index["reports"] = []
                elif change == "resource_digest":
                    index["reports"][0]["resources_sha256"] = "wrong"
                elif change.startswith("resource_"):
                    path = root / index["reports"][0]["resource_boundary_path"]
                    boundary = json.loads(path.read_text())
                    if change == "resource_pid":
                        boundary["after"]["processes"][0]["pid"] = 99
                    elif change == "resource_clock":
                        boundary["after"]["proxy"]["captured_monotonic"] = 0
                    else:
                        boundary["after"]["processes"][0]["cpu_seconds"] = .5
                    benchmark.save(path, boundary)
                    index["reports"][0]["resource_boundary_sha256"] = benchmark.file_sha256(path)
                elif change == "binary":
                    (root / "fake-binary").write_bytes(b"changed")
                else:
                    (root / "9999-unbound.samples.jsonl").write_bytes(b"{}\n")
                benchmark.save(root / "campaign.json", index)
                with self.assertRaises(ValueError):
                    auditor.audit(root, manifest, plan, fleet)


if __name__ == "__main__":
    unittest.main()
