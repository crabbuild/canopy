"""Ensure a real HTTP measurement counts both failures and corrupt responses."""

import contextlib
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import io
import json
from pathlib import Path
import socket
import tempfile
import threading
from types import SimpleNamespace
import unittest

from benchmark_browse import measure


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *_args):
        pass

    def do_POST(self):
        self.connection.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.rfile.read(int(self.headers["Content-Length"]))
        with self.server.guard:
            self.server.requests += 1
            sequence = self.server.requests
        body = b"verified" if sequence % 3 else b"corrupt"
        self.send_response(503 if sequence % 5 == 0 else 200)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


class BrowseMeasurementTests(unittest.TestCase):
    def test_errors_and_invalid_bodies_are_counted_without_retries(self):
        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        server.guard, server.requests = threading.Lock(), 0
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            with tempfile.TemporaryDirectory() as temporary:
                output = Path(temporary)
                (output / "queries.json").write_text(json.dumps({"queries": [{
                    "repository": "fixture", "operation": "refs", "path": "/browse",
                    "payload": {}, "response_sha256": hashlib.sha256(b"verified").hexdigest(),
                }]}))
                with contextlib.redirect_stdout(io.StringIO()):
                    measure(SimpleNamespace(output=output, base_url=f"http://127.0.0.1:{server.server_port}",
                                            duration=0.25, concurrency=[2], repeats=1), "fixture-token")
                report = json.loads((output / "measurements.json").read_text())["results"][0]
                samples = [json.loads(line) for line in (output / "samples.jsonl").read_text().splitlines()]
                self.assertEqual(len(samples), server.requests - 1)  # One untimed warmup.
                self.assertEqual(report["requests"], len(samples))
                self.assertEqual(sum(report["outcomes"].values()), len(samples))
                self.assertGreater(report["outcomes"]["http_503_or_invalid_body"], 0)
                self.assertGreater(report["outcomes"]["http_200_or_invalid_body"], 0)
                self.assertAlmostEqual(report["successful_requests_per_second"],
                                       report["outcomes"]["ok"] / report["elapsed_seconds"])
                self.assertIsNotNone(report["latency_ms_successful"])
        finally:
            server.shutdown()
            server.server_close()
            thread.join()


if __name__ == "__main__":
    unittest.main()
