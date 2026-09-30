"""Campaign orchestration tests are not server capacity measurements."""

import json
import os
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch
import uuid

import benchmark_repositories as benchmark
import benchmark_three_node_campaign as campaign


def fixture():
    entries = [{"name": f"repo-{index}", "owner": "canopy", "repository_id": str(uuid.uuid4()),
                "commit": "a" * 40 if index == 0 else None,
                "base_commit": "b" * 40 if index == 0 else None,
                "lfs_oid": "c" * 64, "lfs_size": 1} for index in range(3)]
    manifest = {"version": 1, "complete": True, "requested_repositories": 3, "repositories": entries}
    plan = {"version": 1, "corpus_repositories": 3, "node_active_limit": 100,
            "windows": [{"id": "metadata", "operation": "metadata", "distribution": "uniform",
                         "active_repositories": 3, "rate": 2, "duration": 1,
                         "concurrency": 2, "repetitions": 2}]}
    return manifest, plan


class CampaignTests(unittest.TestCase):
    def test_expansion_preserves_explicit_repetitions_and_eligible_sets(self):
        manifest, plan = fixture()
        rows = campaign.windows(plan, manifest)
        self.assertEqual([row["repetition"] for row in rows], [1, 2])
        plan["windows"][0].update(operation="clone", active_repositories=3)
        with self.assertRaisesRegex(ValueError, "eligible corpus"):
            campaign.windows(plan, manifest)
        plan["windows"][0].update(operation="create", active_repositories=None)
        self.assertEqual(len(campaign.windows(plan, manifest)), 2)
        plan["windows"][0]["active_repositories"] = 1
        with self.assertRaisesRegex(ValueError, "creation"):
            campaign.windows(plan, manifest)

    def test_bad_counts_duplicate_ids_and_oversized_driver_are_rejected(self):
        for mutation in (lambda manifest, plan: plan.update(corpus_repositories=2),
                         lambda manifest, plan: plan["windows"][0].update(rate=True),
                         lambda manifest, plan: plan["windows"][0].update(concurrency=257),
                         lambda manifest, plan: plan["windows"].append(plan["windows"][0].copy()),
                         lambda manifest, plan: manifest["repositories"][1].update(
                             repository_id=manifest["repositories"][0]["repository_id"]),
                         lambda manifest, plan: plan["windows"][0].update(
                             operation="lfs_upload", lfs_bytes=16 * 1024 * 1024, concurrency=17)):
            manifest, plan = fixture()
            mutation(manifest, plan)
            with self.assertRaises(ValueError):
                campaign.windows(plan, manifest)

    def test_cpu_clock_is_portable_and_cumulative_not_percent_approximation(self):
        self.assertAlmostEqual(campaign.cpu_seconds("1:49.57"), 109.57)
        self.assertEqual(campaign.cpu_seconds("01:02:03"), 3723)
        self.assertEqual(campaign.cpu_seconds("2-01:02:03"), 176523)
        with self.assertRaises(ValueError):
            campaign.cpu_seconds("0:nan")

    def test_stale_metrics_or_missing_processes_never_qualify_a_window(self):
        ready = {"fixture_id": "fixture", "launcher_pid": 100,
                 "nodes": [{"pid": pid} for pid in (101, 102, 103)]}
        output = "\n".join(f"{pid} 1.0 1024 0:00.01" for pid in (100, 101, 102, 103, os.getpid()))
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for metrics in ({"fixture_id": "other", "captured_monotonic": 99},
                            {"fixture_id": "fixture", "captured_monotonic": 1},
                            {"fixture_id": "fixture", "captured_monotonic": 101}):
                with patch.object(campaign, "read_json", return_value=metrics), \
                     patch.object(campaign.subprocess, "check_output", return_value=output), \
                     patch.object(campaign.time, "monotonic", return_value=100):
                    with self.assertRaisesRegex(RuntimeError, "stale"):
                        campaign.observe(root, ready)
            with patch.object(campaign.subprocess, "check_output", return_value="100 0.0 1 0:00.01"):
                with self.assertRaisesRegex(RuntimeError, "missing"):
                    campaign.observe(root, ready)

    def test_monitor_failure_is_retained_not_silently_ignored(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with patch.object(campaign, "observe", side_effect=RuntimeError("fixture died")):
                monitor = campaign.Monitor(root, {}, root / "resources.jsonl")
                with self.assertRaisesRegex(RuntimeError, "observation failed"):
                    with monitor:
                        monitor.worker.join(timeout=1)
                self.assertEqual(monitor.failure, "RuntimeError")

    def test_fleet_requires_live_commands_and_unchanged_artifact_bindings(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            binary = root / "canopy"
            binary.write_bytes(b"fixture, not a production executable")
            scripts = {name: benchmark.file_sha256(Path(campaign.__file__).parent / name)
                       for name in ("serve_three_gateways.py", "local_tcp_proxy.py",
                                    "smoke_s3_process.py", "smoke_s3_peers.py")}
            ready = {"ready": True, "error": None, "shutdown": [], "fixture_id": "fixture",
                "nodes": [{"index": index, "pid": 100 + index, "node_id": str(uuid.uuid4())}
                          for index in range(3)], "max_active_repositories_per_node": 100,
                "public_url": "http://127.0.0.1:1234", "proxy_url": "http://127.0.0.1:1234",
                "binary_path": str(binary), "binary_sha256": benchmark.file_sha256(binary),
                "fixture_scripts_sha256": scripts}
            benchmark.save(root / "ready.json", ready)
            def command(arguments, **kwargs):
                index = int(arguments[2]) - 100
                return f"{binary} {root / f'node-{index}.json'}\n"
            with patch.object(campaign.subprocess, "check_output", side_effect=command):
                self.assertEqual(campaign.validate_fleet(root, {"node_active_limit": 100}), ready)
            with patch.object(campaign.subprocess, "check_output", return_value="another process"):
                with self.assertRaisesRegex(ValueError, "PID"):
                    campaign.validate_fleet(root, {"node_active_limit": 100})
            scripts["local_tcp_proxy.py"] = "0" * 64
            benchmark.save(root / "ready.json", ready)
            with self.assertRaisesRegex(ValueError, "scripts changed"):
                campaign.validate_fleet(root, {"node_active_limit": 100})
            binary.write_bytes(b"different executable")
            with self.assertRaisesRegex(ValueError, "binary changed"):
                campaign.validate_fleet(root, {"node_active_limit": 100})

    def test_preflight_precedes_load_and_failed_windows_are_kept(self):
        self.run_orchestration()

    def test_failed_preflight_prevents_all_load(self):
        self.run_orchestration(failed_preflight=True)

    def run_orchestration(self, failed_preflight=False):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manifest, plan = fixture()
            manifest_path, plan_path, fleet = root / "manifest.json", root / "plan.json", root / "fleet"
            benchmark.save(manifest_path, manifest)
            benchmark.save(plan_path, plan)
            fleet.mkdir()
            ready = {"proxy_url": "http://127.0.0.1:1234", "fixture_id": "fixture"}
            benchmark.save(fleet / "ready.json", ready)
            args = SimpleNamespace(manifest=manifest_path, plan=plan_path, fleet_dir=fleet,
                output_dir=root / "results", timeout=30, git_timeout=120, seed=42, verify_concurrency=16)
            calls = []
            class FakeClient:
                def __init__(self, url, token, timeout):
                    self.base_url = url
                def close(self):
                    pass
            class FakeMonitor:
                def __init__(self, directory, ready, path):
                    self.path = path
                def __enter__(self):
                    self.path.write_text('{"fixture": "not a measurement"}\n')
                def __exit__(self, *_):
                    pass
            def verify(*args):
                calls.append("verify")
                if failed_preflight:
                    raise RuntimeError("corpus mismatch")
                return {"verified_repositories": 3, "git_v0_v2_samples": 1}
            def measure(parameters, client, token):
                calls.append("measure")
                report = {"operation": parameters.operation, "failed_arrivals": 1,
                    "outcomes": {"ok": 1, "http_503": 1},
                    "manifest_sha256": benchmark.file_sha256(manifest_path),
                    "driver_sha256": benchmark.file_sha256(Path(benchmark.__file__))}
                benchmark.save(parameters.output, report)
                return report
            with patch.object(campaign, "validate_fleet", return_value=ready), \
                 patch.object(campaign, "observe", return_value={}), \
                 patch.object(campaign, "Monitor", FakeMonitor), \
                 patch.object(benchmark, "Client", FakeClient), \
                 patch.object(benchmark, "verify", side_effect=verify), \
                 patch.object(benchmark, "measure", side_effect=measure), patch("builtins.print"):
                if failed_preflight:
                    with self.assertRaisesRegex(RuntimeError, "corpus mismatch"):
                        campaign.campaign(args, "fixture-token")
                else:
                    campaign.campaign(args, "fixture-token")
            result = campaign.read_json(args.output_dir / "campaign.json")
            self.assertEqual(result["completed"], not failed_preflight)
            self.assertNotIn("fixture-token", (args.output_dir / "campaign.json").read_text())
            if failed_preflight:
                self.assertEqual(calls, ["verify"])
                self.assertEqual(result["reports"], [])
                self.assertEqual(result["error"], "RuntimeError")
            else:
                self.assertEqual(calls, ["verify", "measure", "measure"])
                self.assertFalse(result["all_arrivals_succeeded"])
                self.assertEqual(result["outcomes"], {"ok": 2, "http_503": 2})
                self.assertEqual(len(result["reports"]), 2)
                self.assertTrue((args.output_dir / "preflight.json").is_file())


if __name__ == "__main__":
    unittest.main()
