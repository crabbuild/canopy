"""Kernel CPU units, child rollup, identity boundaries and retained failures."""

import ctypes
import json
import os
from pathlib import Path
import select
import subprocess
import sys
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import fleet_cpu as cpu


class CpuTests(unittest.TestCase):
    def snapshot(self, pid=123, capture=1, user=10, child=2):
        return {"pid": pid, "identity": {"start_ticks": 456}, "backend": "fixture",
                "scale": {"clock_ticks_per_second": 100}, "seconds_per_unit": .01,
                "cpu_units": dict(zip(cpu.COUNTERS, (user, 3, child, 1))),
                "rss_bytes": 4096, "captured_monotonic": capture}

    def test_darwin_sdk_structure_layout(self):
        self.assertEqual(ctypes.sizeof(cpu.DarwinUsage), 160)
        self.assertEqual(cpu.DarwinUsage.child_user_time.offset, 96)
        self.assertEqual(cpu.DarwinUsage.proc_start_abstime.offset, 80)

    def linux_stat(self, state="S"):
        fields = [state] + ["0"] * 21
        for index, value in zip((11, 12, 13, 14, 19, 21), (10, 20, 30, 40, 500, 2)):
            fields[index] = str(value)
        return "123 (odd ) name)) " + " ".join(fields)

    def test_linux_ticks_and_comm_parentheses(self):
        sample = cpu.parse_linux_stat(self.linux_stat(), 123, 100, 4096)
        self.assertEqual(sample["cpu_units"], dict(zip(cpu.COUNTERS, (10, 20, 30, 40))))
        self.assertEqual(sample["identity"], {"start_ticks": 500})
        self.assertEqual(sample["seconds_per_unit"], .01)
        self.assertEqual(sample["rss_bytes"], 8192)
        for text, pid, rate in ((self.linux_stat("Z"), 123, 100), (self.linux_stat("X"), 123, 100),
                               (self.linux_stat(), 124, 100), ("123 (truncated) S", 123, 100),
                               (self.linux_stat(), 123, 0)):
            with self.assertRaises(ValueError):
                cpu.parse_linux_stat(text, pid, rate, 4096)

    def test_delta_has_separate_self_and_reaped_child_cpu(self):
        result = cpu.delta(self.snapshot(), self.snapshot(capture=3, user=30, child=52))
        self.assertEqual(result["cpu_seconds"]["user"], .2)
        self.assertEqual(result["self_cpu_percent"], 10)
        self.assertEqual(result["reaped_child_cpu_seconds"], .5)
        self.assertIn("excludes live", result["child_scope"])

    def test_identity_scale_clock_and_counter_discontinuities_fail(self):
        before = self.snapshot()
        mutations = [{"pid": 124}, {"identity": {"start_ticks": 999}},
                     {"backend": "different"}, {"scale": {"clock_ticks_per_second": 1000}},
                     {"seconds_per_unit": .001}, {"captured_monotonic": 1},
                     {"captured_monotonic": float("nan")},
                     {"cpu_units": dict(zip(cpu.COUNTERS, (9, 3, 2, 1)))}]
        for mutation in mutations:
            with self.subTest(mutation=mutation), self.assertRaises(ValueError):
                cpu.delta(before, {**self.snapshot(capture=2), **mutation})

    @unittest.skipUnless(sys.platform in ("darwin", "linux"), "requires native kernel reader")
    def test_native_units_calibrate_against_process_cpu_clock(self):
        reader = cpu.Reader()
        before = reader.snapshot(os.getpid())
        started = time.process_time()
        while time.process_time() - started < .15:
            sum(range(10000))
        expected = time.process_time() - started
        after = reader.snapshot(os.getpid())
        observed = cpu.delta(before, after)["cpu_seconds"]
        self.assertAlmostEqual(observed["user"] + observed["system"], expected, delta=.04)

    @unittest.skipUnless(sys.platform in ("darwin", "linux"), "requires native kernel reader")
    def test_child_cpu_is_absent_while_live_and_rolls_up_after_wait(self):
        reader = cpu.Reader()
        before = reader.snapshot(os.getpid())
        command = "import time,sys\nstart=time.process_time()\nwhile time.process_time()-start < .15: sum(range(10000))\nprint('ready',flush=True)\nsys.stdin.readline()\n"
        child = subprocess.Popen([sys.executable, "-B", "-c", command], stdin=subprocess.PIPE,
                                 stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        try:
            self.assertTrue(select.select([child.stdout], [], [], 10)[0], "child readiness timed out")
            self.assertEqual(child.stdout.readline(), "ready\n")
            live = reader.snapshot(os.getpid())
            child_usage = reader.snapshot(child.pid)
            child_self = sum(child_usage["cpu_units"][name] for name in ("user", "system")) * child_usage["seconds_per_unit"]
            self.assertGreater(child_self, .12)
            self.assertEqual(cpu.delta(before, live)["reaped_child_cpu_seconds"], 0)
            child.communicate(input="finish\n", timeout=10)
            self.assertEqual(child.returncode, 0)
            after = reader.snapshot(os.getpid())
            self.assertGreater(cpu.delta(live, after)["reaped_child_cpu_seconds"], .12)
            with self.assertRaises((OSError, ValueError)):
                reader.snapshot(child.pid)
        finally:
            if child.poll() is None:
                child.kill()
                child.wait(timeout=10)
            for stream in (child.stdin, child.stdout, child.stderr):
                stream.close()

    def test_observer_complete_and_failure_receipts_preserve_samples(self):
        for failure in (False, True):
            with self.subTest(failure=failure), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                fleet = root / "fleet"
                fleet.mkdir()
                ready = {"max_active_repositories_per_node": 100, "binary_sha256": "a" * 64,
                         "nodes": [{"pid": pid} for pid in (123, 124, 125)]}
                (fleet / "ready.json").write_text(json.dumps(ready))
                args = SimpleNamespace(fleet_dir=fleet, output_dir=root / "observed", samples=2, interval=.1)
                calls = []
                def snapshot(pid):
                    calls.append(pid)
                    if failure and len(calls) == 4:
                        raise OSError("sensitive diagnostic text")
                    return self.snapshot(pid, capture=1 if len(calls) <= 3 else 2)
                with patch.object(cpu.campaign, "validate_fleet", return_value=ready), \
                     patch.object(cpu, "Reader", return_value=SimpleNamespace(snapshot=snapshot)), \
                     patch.object(cpu.time, "sleep"):
                    if failure:
                        with self.assertRaises(OSError):
                            cpu.observe(args)
                    else:
                        self.assertTrue(cpu.observe(args)["complete"])
                record = json.loads((args.output_dir / "observation.json").read_text())
                samples = (args.output_dir / "samples.jsonl").read_text().splitlines()
                self.assertEqual(record["samples"], len(samples))
                self.assertEqual(len(samples), 1 if failure else 2)
                self.assertEqual(record["complete"], not failure)
                self.assertEqual(record["error"], "OSError" if failure else None)
                self.assertNotIn("sensitive diagnostic text", json.dumps(record))
                self.assertEqual(record["samples_sha256"], cpu.benchmark.file_sha256(args.output_dir / "samples.jsonl"))


if __name__ == "__main__":
    unittest.main()
