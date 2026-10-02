"""Scheduled creation must validate identity, expose errors and never retry."""

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import tempfile
import threading
from types import SimpleNamespace
import unittest
import uuid

import benchmark_repositories as benchmark


class Creation(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *_):
        pass

    def do_POST(self):
        request = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        name = request["name"]
        with self.server.guard:
            self.server.requests.append((name, self.headers.get("X-Request-ID")))
        body = json.dumps({"name": name, "owner": "canopy",
                           "repository_id": ("wrong" if self.server.malformed else
                                             self.server.identifier or str(uuid.uuid4()))}).encode()
        self.send_response(self.server.status)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


class CreationTests(unittest.TestCase):
    def test_uuid_contract_excludes_nil_non_rfc_and_noncanonical_values(self):
        self.assertTrue(benchmark.canonical_repository_uuid(str(uuid.uuid4())))
        for value in (str(uuid.UUID(int=0)), str(uuid.UUID(int=2**128 - 1)),
                      str(uuid.uuid4()).upper(), uuid.uuid4().hex, "not-a-uuid", None):
            self.assertFalse(benchmark.canonical_repository_uuid(value))

    def test_creations_validate_uuid_and_preserve_every_scheduled_outcome(self):
        server = ThreadingHTTPServer(("127.0.0.1", 0), Creation)
        server.guard, server.requests = threading.Lock(), []
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        client = benchmark.Client(f"http://127.0.0.1:{server.server_port}", "fixture-token", 2)
        try:
            with tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                manifest = root / "corpus.json"
                manifest.write_text(json.dumps({"version": 1, "complete": True,
                    "requested_repositories": 1, "repositories": [{"name": "background",
                    "owner": "canopy", "repository_id": str(uuid.uuid4()), "commit": None}]}))
                for case, status, malformed, expected in (
                        ("valid", 200, False, "ok"),
                        ("overload", 503, False, "http_503"),
                        ("invalid", 200, True, "invalid_response"),
                        ("nil", 200, False, "invalid_response")):
                    server.status, server.malformed = status, malformed
                    server.identifier = str(uuid.UUID(int=0)) if case == "nil" else None
                    args = SimpleNamespace(manifest=manifest, active_repositories=1, seed=42,
                        duration=1, rate=2, concurrency=2, distribution="uniform",
                        operation="create", timeout=2, output=root / f"{case}.json")
                    report = benchmark.measure(args, client, "fixture-token")
                    self.assertEqual(report["outcomes"], {expected: 2})
                    self.assertEqual(report["failed_arrivals"], 0 if expected == "ok" else 2)
                    self.assertEqual(report["error_fraction"], 0 if expected == "ok" else 1)
                    self.assertGreaterEqual(report["elapsed_including_drain_seconds"], args.duration)
                    self.assertLessEqual(report["successful_rps_including_drain"], args.rate)
                    self.assertLessEqual(report["successful_rps_in_schedule_window"], args.rate)
                    samples = [json.loads(line) for line in
                               args.output.with_suffix(".samples.jsonl").read_text().splitlines()]
                    self.assertEqual({sample["sequence"] for sample in samples}, {0, 1})
                    self.assertEqual(len({sample["created_name"] for sample in samples}), 2)
                    for sample in samples:
                        self.assertTrue(sample["created_name"].startswith("create-" + report["create_run_id"]))
                        self.assertLessEqual(len(sample["created_name"]), 64)
                        self.assertEqual(sample["created_repository_id"] is not None, expected == "ok")
                        if expected == "ok":
                            uuid.UUID(sample["created_repository_id"])
                    self.assertNotIn("fixture-token", args.output.read_text())
                    if expected == "ok":
                        recovered = {sample["created_name"]: sample["created_repository_id"]
                                     for sample in samples}
                        class Restored:
                            def __init__(self):
                                self.requests = []

                            def request(self, path, **kwargs):
                                self.requests.append(path)
                                name = path.rsplit("/", 1)[-1]
                                return 200, json.dumps({"name": name,
                                    "repository_id": recovered[name]}).encode()
                        restored = Restored()
                        check = SimpleNamespace(manifest=manifest, reports=[args.output],
                                                output=root / "recovered.json")
                        result = benchmark.verify_creations(check, restored, "fixture-token")
                        self.assertEqual(result["verified_creations"], 2)
                        self.assertEqual(result["unacknowledged_arrivals_not_asserted"], 0)
                        self.assertEqual(len(restored.requests), 2)
                        recovered[samples[0]["created_name"]] = str(uuid.uuid4())
                        check.output = root / "mismatch.json"
                        with self.assertRaisesRegex(RuntimeError, "recovery differs"):
                            benchmark.verify_creations(check, restored, "fixture-token")
                        self.assertFalse(check.output.exists())
                        samples[0]["created_repository_id"] = str(uuid.uuid4())
                        args.output.with_suffix(".samples.jsonl").write_text(
                            "".join(json.dumps(sample) + "\n" for sample in samples))
                        restored.requests.clear()
                        with self.assertRaisesRegex(ValueError, "samples have changed"):
                            benchmark.verify_creations(check, restored, "fixture-token")
                        self.assertEqual(restored.requests, [])
                self.assertEqual(len(server.requests), 8)
                for _, request_id in server.requests:
                    uuid.UUID(request_id)
                server.status, server.malformed, server.identifier = 200, False, str(uuid.uuid4())
                args.output = root / "duplicate.json"
                duplicate = benchmark.measure(args, client, "fixture-token")
                self.assertEqual(duplicate["outcomes"], {"ok": 1, "invalid_response": 1})
                self.assertEqual(duplicate["acknowledged_created_repositories"], 1)
                self.assertIsNone(duplicate["active_repositories"])
                self.assertEqual(len(server.requests), 10)
        finally:
            client.close()
            server.shutdown()
            server.server_close()
            thread.join()


if __name__ == "__main__":
    unittest.main()
