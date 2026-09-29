"""Verify the capacity driver against an overloaded real HTTP fixture."""

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import hashlib
import json
from pathlib import Path
import tempfile
import threading
import time
from types import SimpleNamespace
import unittest
import uuid

import benchmark_repositories as benchmark


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *_args):
        pass

    def do_GET(self):
        with self.server.guard:
            self.server.requests += 1
            self.server.request_ids.append(self.headers.get("X-Request-ID"))
            self.server.auth_headers.append(self.headers.get("Authorization"))
            self.server.active += 1
            self.server.peak = max(self.server.peak, self.server.active)
        try:
            time.sleep(.1)
            body = b"overloaded"
            self.send_response(503)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        finally:
            with self.server.guard:
                self.server.active -= 1


class ScheduledLoad(unittest.TestCase):
    def test_overload_has_no_hidden_retries_or_missing_arrivals(self):
        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        server.guard = threading.Lock()
        server.requests = server.active = server.peak = 0
        server.request_ids = []
        server.auth_headers = []
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        client = benchmark.Client(f"http://127.0.0.1:{server.server_port}", "fixture-token", 2)
        try:
            with tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                manifest = root / "manifest.json"
                manifest.write_text(json.dumps({"version": 1, "complete": True,
                    "requested_repositories": 1, "repositories": [{"name": "fixture", "owner": "canopy",
                    "repository_id": str(uuid.uuid4()), "commit": None}]}))
                args = SimpleNamespace(manifest=manifest, active_repositories=1, seed=42,
                    duration=1, rate=100, concurrency=2, distribution="uniform",
                    operation="metadata", timeout=2, output=root / "report.json")
                report = benchmark.measure(args, client, None)
                samples = [json.loads(line) for line in (root / "report.samples.jsonl").read_text().splitlines()]
                self.assertEqual(sorted(sample["sequence"] for sample in samples), list(range(100)))
                self.assertEqual(report["failed_arrivals"], 100)
                self.assertEqual(report["outcomes"]["http_503"], server.requests)
                self.assertGreater(report["outcomes"]["driver_busy"], 0)
                self.assertLessEqual(server.peak, 2)
                for sample in samples:
                    if sample["elapsed_ms"] is not None:
                        self.assertGreaterEqual(sample["elapsed_ms"], sample["service_ms"])
                ids = [sample["request_id"] for sample in samples if sample["elapsed_ms"] is not None]
                self.assertCountEqual(ids, server.request_ids)
                self.assertEqual(len(set(ids)), len(ids))
                for request_id in ids:
                    uuid.UUID(request_id)
                self.assertNotIn("fixture-token", args.output.read_text())
        finally:
            client.close()
            server.shutdown()
            server.server_close()
            thread.join()

    def test_multiple_ingresses_report_each_gateway_without_losing_arrivals(self):
        servers = [ThreadingHTTPServer(("127.0.0.1", 0), Handler) for _ in range(2)]
        threads = []
        clients = []
        for server in servers:
            server.guard = threading.Lock()
            server.requests = server.active = server.peak = 0
            server.request_ids = []
            server.auth_headers = []
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            threads.append(thread)
            clients.append(benchmark.Client(f"http://127.0.0.1:{server.server_port}",
                                            "fixture-token", 2))
        try:
            with tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                manifest = root / "manifest.json"
                manifest.write_text(json.dumps({"version": 1, "complete": True,
                    "requested_repositories": 1, "repositories": [{"name": "fixture", "owner": "canopy",
                    "repository_id": str(uuid.uuid4()), "commit": None}]}))
                args = SimpleNamespace(manifest=manifest, active_repositories=1, seed=42,
                    duration=1, rate=20, concurrency=16, distribution="uniform",
                    operation="metadata", timeout=2, output=root / "report.json")
                report = benchmark.measure(args, clients, None)
                samples = [json.loads(line) for line in
                           (root / "report.samples.jsonl").read_text().splitlines()]
                self.assertEqual(sorted(sample["sequence"] for sample in samples), list(range(20)))
                self.assertEqual({sample["ingress_index"] for sample in samples}, {0, 1})
                self.assertEqual(sum(server.requests for server in servers),
                                 report["outcomes"].get("http_503", 0))
                for index, server in enumerate(servers):
                    self.assertEqual(server.requests,
                                     report["ingresses"][index]["outcomes"].get("http_503", 0))
                    self.assertGreater(server.requests, 0)
                    self.assertIsNotNone(report["ingresses"][index]["service_ms"]["p95"])
        finally:
            for client in clients:
                client.close()
            for server in servers:
                server.shutdown()
                server.server_close()
            for thread in threads:
                thread.join()

    def test_stock_git_sends_authorization_and_request_id_headers(self):
        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        server.guard = threading.Lock()
        server.requests = server.active = server.peak = 0
        server.request_ids = []
        server.auth_headers = []
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            with tempfile.TemporaryDirectory() as directory:
                request_id = str(uuid.uuid4())
                with self.assertRaises(RuntimeError):
                    benchmark.git("ls-remote", f"http://127.0.0.1:{server.server_port}/repo.git",
                                  cwd=directory, token="fixture-token", request_id=request_id)
                self.assertGreater(server.requests, 0)
                self.assertEqual(server.request_ids, [request_id] * server.requests)
                self.assertEqual(server.auth_headers, ["Bearer fixture-token"] * server.requests)
        finally:
            server.shutdown()
            server.server_close()
            thread.join()

    def test_stock_git_clone_and_cold_fetch_validate_populated_repositories(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source"
            remote = root / "canopy" / "fixture.git"
            remote.parent.mkdir()
            benchmark.git("init", "-b", "main", str(source), cwd=root, token="fixture-token")
            benchmark.git("config", "user.name", "Fixture", cwd=source, token="fixture-token")
            benchmark.git("config", "user.email", "fixture@example.invalid", cwd=source,
                          token="fixture-token")
            body = b"stock Git transfer fixture\n"
            (source / "README.md").write_bytes(body)
            benchmark.git("add", "README.md", cwd=source, token="fixture-token")
            benchmark.git("commit", "-m", "Fixture", cwd=source, token="fixture-token")
            benchmark.git("init", "--bare", "-b", "main", str(remote), cwd=root,
                          token="fixture-token")
            benchmark.git("push", str(remote), "HEAD:refs/heads/main", cwd=source,
                          token="fixture-token")
            entry = {"name": "fixture", "owner": "canopy",
                     "repository_id": str(uuid.uuid4()),
                     "commit": benchmark.git("rev-parse", "HEAD", cwd=source,
                                             token="fixture-token"),
                     "readme_sha256": hashlib.sha256(body).hexdigest()}
            manifest = root / "manifest.json"
            manifest.write_text(json.dumps({"version": 1, "complete": True,
                "requested_repositories": 1, "repositories": [entry]}))
            ingresses = [SimpleNamespace(base_url=root.as_uri()) for _ in range(2)]
            for operation in ("clone", "cold_fetch"):
                args = SimpleNamespace(manifest=manifest, active_repositories=1, seed=42,
                    duration=1, rate=2, concurrency=2, distribution="uniform",
                    operation=operation, timeout=2, git_timeout=30,
                    work_dir=root / f"{operation}-scratch", output=root / f"{operation}.json")
                report = benchmark.measure(args, ingresses, "fixture-token")
                self.assertEqual(report["outcomes"], {"ok": 2})
                self.assertEqual(report["eligible_repositories"], 1)
                self.assertEqual([item["outcomes"] for item in report["ingresses"]],
                                 [{"ok": 1}, {"ok": 1}])
                self.assertEqual(list(args.work_dir.iterdir()), [])
                self.assertNotIn("fixture-token", args.output.read_text())
            args = SimpleNamespace(manifest=manifest, active_repositories=1, seed=42,
                duration=1, rate=2, concurrency=2, distribution="uniform",
                operation="push_branch", timeout=2, git_timeout=30,
                work_dir=root / "push-scratch", output=root / "push.json")
            report = benchmark.measure(args, ingresses, "fixture-token")
            self.assertEqual(report["outcomes"], {"ok": 2})
            refs = benchmark.git("for-each-ref", "--format=%(objectname)",
                f"refs/heads/canopy-benchmark/{report['push_run_id']}", cwd=remote,
                token="fixture-token").splitlines()
            self.assertEqual(refs, [report["push_commit"]] * 2)
            self.assertNotIn("fixture-token", args.output.read_text())


if __name__ == "__main__":
    unittest.main()
