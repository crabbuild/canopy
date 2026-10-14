"""Offline scheduler/recovery guards; no real Git/provider/owner loss proof."""
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch
import uuid

import benchmark_repositories as benchmark
import benchmark_critical_git as driver


def receipt():
    return {"version": 1, "complete": True, "error": None,
            "steps": [{"name": name, "ok": True, "wall_seconds": 0.1} for name in driver.STEPS],
            "repositories": [{"repository_id": str(uuid.uuid4()), "name": name, "owner": "canopy"}
                             for name in ("source", "mirror")], "binary_sha256": "a" * 64,
            "driver_sha256": "b" * 64, "git_driver_sha256": "c" * 64}


class CriticalLoadTests(unittest.TestCase):
    def test_partial_or_shortened_workflow_is_never_skipped(self):
        for mutate in (lambda r: r.update(complete=False), lambda r: r.update(error="TimeoutError"),
                       lambda r: r["steps"].pop(), lambda r: r["steps"][3].update(ok=False),
                       lambda r: r["repositories"][1].update(repository_id=r["repositories"][0]["repository_id"])):
            value = receipt()
            mutate(value)
            with self.assertRaises(RuntimeError):
                driver.validate_receipt(value)

    def test_failed_attempt_latency_is_retained_busy_latency_is_not_fabricated(self):
        samples = [{"result": "ok", "elapsed_ms": 10, "service_ms": 8, "completion_offset_seconds": 0.2},
                   {"result": "workflow_error", "elapsed_ms": 100, "service_ms": 80, "completion_offset_seconds": 1.5},
                   {"result": "driver_busy"},
                   {"result": "ok", "elapsed_ms": 200, "service_ms": 160, "completion_offset_seconds": 2.5}]
        value = driver.summarize(samples, 2, 4)
        self.assertEqual(value["failed_arrivals"], 2)
        self.assertEqual(value["successful_workflows_per_second_in_schedule_window"], 0.5)
        self.assertEqual(value["successful_workflows_per_second_including_drain"], 0.5)
        self.assertEqual(value["scheduled_workflow_latency_ms"], benchmark.percentiles([10, 100, 200]))

    def fixture(self, root):
        sample_path = root / "workflow-0000.json"
        value = receipt()
        benchmark.save(sample_path, value)
        sample = {"sequence": 0, "result": "ok", "receipt": sample_path.name,
                  "receipt_sha256": benchmark.file_sha256(sample_path), "elapsed_ms": 100,
                  "service_ms": 90, "completion_offset_seconds": 0.1, "steps": value["steps"]}
        ledger = root / "samples.jsonl"
        ledger.write_text(json.dumps(sample) + "\n")
        report = {"version": 1, "completed": True, "error": None, "scheduled": 1,
                  "schedule_seconds": 1, "elapsed_including_drain_seconds": 1,
                  "steps_per_workflow": driver.STEPS, "samples_sha256": benchmark.file_sha256(ledger),
                  "bindings": {str(Path(driver.critical.__file__).resolve()): "b" * 64,
                               str(Path(benchmark.__file__).resolve()): "c" * 64},
                  "fleet": {"binary_sha256": "a" * 64}}
        report.update(driver.summarize([sample], 1, 1))
        path = root / "report.json"
        benchmark.save(path, report)
        return path, report, sample, value

    def test_closed_receipt_inventory_and_report_are_audited(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            path, record, sample, value = self.fixture(root)
            hashes = {root / sample["receipt"]: sample["receipt_sha256"],
                      root / "samples.jsonl": record["samples_sha256"],
                      **{Path(p): d for p, d in record["bindings"].items()}}
            with patch.object(benchmark, "file_sha256", side_effect=lambda p: hashes[Path(p)]):
                self.assertEqual(len(driver.load_receipts(path)[1]), 1)
                benchmark.save(root / "workflow-9999.json", value)
                with self.assertRaisesRegex(RuntimeError, "orphan"):
                    driver.load_receipts(path)

    def test_digest_bound_partial_receipt_and_forged_report_are_rejected(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            path, record, sample, value = self.fixture(root)
            hashes = {root / sample["receipt"]: sample["receipt_sha256"],
                      root / "samples.jsonl": record["samples_sha256"],
                      **{Path(p): d for p, d in record["bindings"].items()}}
            with patch.object(benchmark, "file_sha256", side_effect=lambda p: hashes[Path(p)]):
                record["failed_arrivals"] = 1
                benchmark.save(path, record)
                with self.assertRaisesRegex(RuntimeError, "differs from arrival"):
                    driver.load_receipts(path)
                record["failed_arrivals"] = 0
                benchmark.save(path, record)
                value["complete"] = False
                benchmark.save(root / sample["receipt"], value)
                with self.assertRaisesRegex(RuntimeError, "partial"):
                    driver.load_receipts(path)

    def test_step_latency_includes_refusals_and_failed_work(self):
        attempts = [{"result": result, "completion_offset_seconds": 0.2,
                     "elapsed_ms": elapsed, "service_ms": elapsed,
                     "steps": [{"name": "atomic-refusal", "ok": ok, "wall_seconds": elapsed / 1000}]}
                    for result, ok, elapsed in (("ok", True, 10), ("workflow_error", False, 90))]
        value = driver.summarize(attempts, 1, 1)["step_wall_ms"]["atomic-refusal"]
        self.assertEqual(value["outcomes"], {"ok": 1, "failed": 1})
        self.assertEqual(value["percentiles"], benchmark.percentiles([10, 90]))

    def test_ledger_cannot_escape_paths_or_silently_drop_arrivals(self):
        for change in (lambda sample: sample.update(receipt="../escape.json"),
                       lambda sample: sample.update(sequence=3),
                       lambda sample: sample.update(result="ignored")):
            with self.subTest(change=change), tempfile.TemporaryDirectory() as temp:
                root = Path(temp)
                path, record, sample, value = self.fixture(root)
                change(sample)
                (root / "samples.jsonl").write_text(json.dumps(sample) + "\n")
                record["samples_sha256"] = benchmark.file_sha256(root / "samples.jsonl")
                record.update(driver.summarize([sample], 1, 1))
                benchmark.save(path, record)
                hashes = {root / sample["receipt"]: sample["receipt_sha256"],
                          root / "samples.jsonl": record["samples_sha256"],
                          **{Path(p): d for p, d in record["bindings"].items()}}
                with patch.object(benchmark, "file_sha256", side_effect=lambda p: hashes[Path(p)]):
                    with self.assertRaises(RuntimeError):
                        driver.load_receipts(path)

    def test_reused_owner_is_refused_before_network_git(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            nodes = [{"node_id": f"old-{i}", "pid": i + 1} for i in range(3)]
            record = {"fleet": {"nodes": nodes, "binary_sha256": "a" * 64}, "failed_arrivals": 0}
            args = SimpleNamespace(report=root / "report.json", output=root / "result.json",
                work_dir=root / "clones", fleet_dir=root / "fleet", node_active_limit=100, timeout=30)
            with patch.object(driver, "load_receipts", return_value=(record, [])), \
                 patch.object(driver.campaign, "validate_fleet", return_value=record["fleet"]), \
                 patch.object(driver.benchmark, "Client", side_effect=AssertionError("unexpected network")):
                with self.assertRaisesRegex(RuntimeError, "distinct owners"):
                    driver.verify(args, "fixture-token")
            self.assertFalse(args.work_dir.exists())

    def test_real_scheduler_retains_failed_worker_and_closes_client(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            fleet = root / "fleet"
            fleet.mkdir()
            ready = {"binary_path": str(root / "binary"), "proxy_url": "http://127.0.0.1:1"}
            args = SimpleNamespace(duration=1, interval=1, concurrency=1, fleet_dir=fleet,
                                   node_active_limit=100, output_dir=root / "output", timeout=30)
            closed = []
            client = SimpleNamespace(close=lambda: closed.append(True))
            def failed(options, client, token):
                benchmark.save(options.receipt, {"complete": False, "repositories": [{"name": "partial-ack"}]})
                raise RuntimeError("simulated write failure")
            with patch.object(driver.campaign, "validate_fleet", return_value=ready), \
                 patch.object(driver, "bindings", return_value={}), \
                 patch.object(driver.benchmark, "Client", return_value=client), \
                 patch.object(driver.critical, "seed", side_effect=failed):
                result = driver.run(args, "fixture-token")
            self.assertTrue(result["completed"])
            self.assertEqual(result["failed_arrivals"], 1)
            self.assertEqual(result["outcomes"], {"workflow_error": 1})
            self.assertEqual(closed, [True])
            sample = json.loads((args.output_dir / "samples.jsonl").read_text())
            self.assertEqual(sample["receipt_sha256"], benchmark.file_sha256(args.output_dir / sample["receipt"]))

    def test_recovery_continues_other_complete_workflows_after_one_failure(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            old = [{"node_id": f"old-{i}", "pid": i + 1} for i in range(3)]
            fresh = [{"node_id": f"new-{i}", "pid": i + 10} for i in range(3)]
            record = {"fleet": {"nodes": old, "binary_sha256": "a" * 64}, "failed_arrivals": 0}
            entries = [(root / "one.json", receipt()), (root / "two.json", receipt())]
            ready = {"nodes": fresh, "binary_sha256": "a" * 64, "proxy_url": "http://127.0.0.1:1"}
            args = SimpleNamespace(report=root / "report.json", output=root / "result.json",
                work_dir=root / "clones", fleet_dir=root / "fleet", node_active_limit=100, timeout=30)
            client = SimpleNamespace(close=lambda: None)
            with patch.object(driver, "load_receipts", return_value=(record, entries)), \
                 patch.object(driver.campaign, "validate_fleet", return_value=ready), \
                 patch.object(driver.benchmark, "Client", return_value=client), \
                 patch.object(driver.benchmark, "file_sha256", return_value="f" * 64), \
                 patch.object(driver.critical, "verify_receipt", side_effect=[RuntimeError("one failed"),
                     {"verified_repositories": 2, "protocols": [0, 2], "exact_ref_inventories": 4}]) as verify:
                result = driver.verify(args, "fixture-token")
            self.assertEqual(verify.call_count, 2)
            self.assertFalse(result["all_workflows_verified"])
            self.assertEqual(result["verified_workflows"], 1)
            self.assertEqual(result["attempted_workflow_verifications"], 2)


if __name__ == "__main__":
    unittest.main()
