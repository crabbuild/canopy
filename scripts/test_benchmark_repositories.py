"""Verify the capacity driver against an overloaded real HTTP fixture."""

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
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


if __name__ == "__main__":
    unittest.main()
