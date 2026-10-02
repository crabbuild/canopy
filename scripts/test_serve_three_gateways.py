"""Check fleet wiring and fail-closed teardown without pretending to measure it."""

import json
from pathlib import Path
import subprocess
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import serve_three_gateways as fleet


class FakeProxy:
    instances = []

    def __init__(self, upstreams, **kwargs):
        self.upstreams, self.options = upstreams, kwargs
        self.url = ("https" if kwargs.get("tls_context") else "http") + f"://127.0.0.1:{6000 + len(self.instances)}"
        self.max_connections, self.buffer_bytes = 256, 65536
        self.failure = None
        self.worker = SimpleNamespace(is_alive=lambda: True)
        self.instances.append(self)

    def __enter__(self):
        return self

    def __exit__(self, *_):
        pass

    def snapshot(self):
        return {"accepted_connections": 0}


class FakeProcess:
    def __init__(self, index, timeout_node):
        self.pid, self.returncode = 9000 + index, None
        self.timeout_node = index == timeout_node

    def poll(self):
        return self.returncode

    def send_signal(self, _):
        pass

    def wait(self, timeout):
        if self.timeout_node and self.returncode is None:
            raise subprocess.TimeoutExpired("fixture", timeout)
        if self.returncode is None:
            self.returncode = 0
        return self.returncode

    def kill(self):
        self.returncode = -9


class FleetTests(unittest.TestCase):
    def run_fleet(self, root, timeout_node=None):
        binary = root / "canopy"
        binary.write_bytes(b"fixture, not an executable or performance result")
        args = SimpleNamespace(binary=binary, work_dir=root / "fleet", config_template=None,
                               storage_url="s3://disposable/fixture", max_active_repositories=100,
                               startup_timeout=30)
        processes, calls = [], []
        def start(binary, directory, settings, instance, **kwargs):
            calls.append((settings, kwargs))
            process = FakeProcess(len(processes), timeout_node)
            processes.append(process)
            return process, "http://" + kwargs["listen_address"]
        FakeProxy.instances = []
        stop = SimpleNamespace(wait=lambda _: True, set=lambda: None)
        with patch.object(fleet, "LocalProxy", FakeProxy), \
             patch.object(fleet, "peer_certificate", return_value=(root / "ca", root / "cert", root / "key")), \
             patch.object(fleet.ssl, "SSLContext"), patch.object(fleet.signal, "signal"), \
             patch.object(fleet.threading, "Event", return_value=stop), \
             patch.object(fleet, "port", side_effect=[5000, 5001, 5002]), \
             patch.object(fleet, "start", side_effect=start), patch("builtins.print"):
            if timeout_node is None:
                fleet.serve(args)
            else:
                with self.assertRaisesRegex(RuntimeError, "graceful node exits"):
                    fleet.serve(args)
        return args, calls, processes

    def test_three_distinct_nodes_advertise_only_the_ingress(self):
        with tempfile.TemporaryDirectory() as directory:
            args, calls, processes = self.run_fleet(Path(directory))
            self.assertEqual(len(calls), 3)
            self.assertEqual(len({settings["node_id"] for settings, _ in calls}), 3)
            self.assertEqual(len({kwargs["signing_key"] for _, kwargs in calls}), 3)
            for index, (settings, kwargs) in enumerate(calls):
                self.assertEqual(kwargs["public_url"], "http://127.0.0.1:6000")
                self.assertEqual(kwargs["listen_address"], f"127.0.0.1:{5000 + index}")
                self.assertEqual(settings["peer_endpoint"], f"https://127.0.0.1:{6001 + index}")
            outcome = json.loads((args.work_dir / "outcome.json").read_text())
            self.assertIsNone(outcome["error"])
            self.assertTrue(all(node["exit_code"] == 0 and not node["forced"]
                                for node in outcome["shutdown"]))
            self.assertEqual([process.returncode for process in processes], [0, 0, 0])

    def test_forced_shutdown_is_retained_and_not_a_success(self):
        with tempfile.TemporaryDirectory() as directory:
            args, _, processes = self.run_fleet(Path(directory), timeout_node=1)
            outcome = json.loads((args.work_dir / "outcome.json").read_text())
            self.assertIn("graceful node exits", outcome["error"])
            self.assertEqual(outcome["shutdown"][1], {"index": 1, "exit_code": -9, "forced": True})
            self.assertEqual([process.returncode for process in processes], [0, -9, 0])


if __name__ == "__main__":
    unittest.main()
