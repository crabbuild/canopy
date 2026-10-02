"""Wire parsing and stock-Git validation; not a live RustFS capacity test."""

import io
import json
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import subprocess
import tempfile
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import patch
import uuid

import benchmark_repositories as benchmark
import measure_git_pack as probe


class PackTests(unittest.TestCase):
    def parse(self, wire, *, bound=4096):
        destination = io.BytesIO()
        clock = iter(index / 1000 for index in range(1000))
        result = probe.stream_pack(io.BytesIO(wire), destination, 0,
                                   max_pack_bytes=bound, timeout=30,
                                   clock=lambda: next(clock))
        return result, destination.getvalue()

    def wire(self, data=b"PACKpayload"):
        return probe.packet(b"NAK\n") + probe.packet(b"\x02progress\n") + probe.packet(b"\x01" + data) + b"0000"

    def test_first_byte_precedes_packet_remainder_and_ignores_progress(self):
        destination = io.BytesIO()
        observations = []
        class Reader(io.BytesIO):
            def read(self, size):
                value = super().read(size)
                observations.append((size, value, len(destination.getvalue())))
                return value
        ticks = iter(index / 1000 for index in range(1000))
        result = probe.stream_pack(Reader(self.wire()), destination, 0,
                                   max_pack_bytes=4096, timeout=30,
                                   clock=lambda: next(ticks))
        first = next(index for index, (_, value, _) in enumerate(observations) if value == b"P")
        self.assertEqual(observations[first][0], 1)
        self.assertEqual(observations[first + 1][1], b"ACKpayload")
        self.assertEqual(result["pack_bytes"], len(b"PACKpayload"))
        self.assertEqual(result["progress_payload_bytes"], len(b"progress\n"))
        self.assertEqual(result["response_body_bytes"], len(self.wire()))
        self.assertLess(result["post_first_pack_byte_ms"], result["post_last_pack_byte_ms"])
        self.assertEqual(destination.getvalue(), b"PACKpayload")

    def test_split_pack_signature_and_multiple_data_packets(self):
        wire = probe.packet(b"NAK\n") + b"".join(probe.packet(b"\x01" + value)
                                                for value in (b"P", b"AC", b"Kpayload")) + b"0000"
        result, body = self.parse(wire)
        self.assertEqual(body, b"PACKpayload")
        self.assertEqual(result["pack_bytes"], len(body))

    def test_invalid_wire_never_becomes_a_successful_sample(self):
        invalid = [b"", b"zzzz", b"0001", b"0004", b"ffff", self.wire()[:-1],
                   self.wire() + b"extra", probe.packet(b"NAK\n") + b"0000",
                   probe.packet(b"NAK\n") + probe.packet(b"\x02progress") + b"0000",
                   probe.packet(b"NAK\n") + probe.packet(b"\x03secret remote error") + b"0000",
                   probe.packet(b"ACK " + b"a" * 40 + b"\n") + self.wire(),
                   probe.packet(b"NAK\n") + probe.packet(b"\x04PACKbad") + b"0000",
                   self.wire(b"not a pack"), probe.packet(b"NAK\n") + probe.packet(b"\x01") + b"0000"]
        for wire in invalid:
            with self.subTest(wire=wire), self.assertRaises(ValueError):
                self.parse(wire)
        with self.assertRaisesRegex(ValueError, "declared bound"):
            self.parse(self.wire(), bound=4)

    def test_deadline_is_not_reset_by_progress_packets(self):
        with self.assertRaisesRegex(ValueError, "total deadline"):
            probe.stream_pack(io.BytesIO(self.wire()), io.BytesIO(), 0,
                              max_pack_bytes=4096, timeout=1, clock=lambda: 2)

    def test_want_and_repository_path_validation(self):
        oid = "a" * 40
        self.assertEqual(probe.request_body(oid), probe.packet(
            f"want {oid} side-band-64k ofs-delta\n".encode()) + b"0000" + probe.packet(b"done\n"))
        for invalid in (None, "a" * 39, "a" * 64, "g" * 40, "a" * 40 + "\ndone"):
            with self.assertRaises(ValueError):
                probe.request_body(invalid)
        with tempfile.TemporaryDirectory() as directory, \
             patch.object(probe.http.client, "HTTPConnection", side_effect=AssertionError("unexpected connection")):
            with self.assertRaises(ValueError):
                probe.download("http://127.0.0.1:1", {"owner": "canopy", "name": "../escape"},
                               oid, "token", Path(directory) / "unused", timeout=30, max_pack_bytes=4096)

    def test_http_errors_redirects_and_compression_are_not_pack_success(self):
        for status, content_type, encoding in ((503, "application/x-git-upload-pack-result", "identity"),
                (302, "application/x-git-upload-pack-result", "identity"),
                (200, "text/html", "identity"),
                (200, "application/x-git-upload-pack-result", "gzip")):
            response = SimpleNamespace(status=status, getheader=lambda key, default: {
                "Content-Type": content_type, "Content-Encoding": encoding}.get(key, default))
            connection = SimpleNamespace(request=lambda *args, **kwargs: None,
                getresponse=lambda: response, close=lambda: None)
            with self.subTest(status=status, content_type=content_type, encoding=encoding), \
                 tempfile.TemporaryDirectory() as directory, \
                 patch.object(probe.http.client, "HTTPConnection", return_value=connection), \
                 patch.object(probe, "stream_pack", side_effect=AssertionError("unexpected pack parsing")):
                path = Path(directory) / "unused.pack"
                with self.assertRaises(ValueError):
                    probe.download("http://127.0.0.1:1", {"owner": "canopy", "name": "source"},
                                   "a" * 40, "token", path, timeout=30, max_pack_bytes=4096)
                self.assertFalse(path.exists())

    def test_failed_sample_is_retained_without_token_or_error_text(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            receipt = root / "critical.json"
            receipt.write_text(json.dumps({"complete": True, "error": None,
                "repositories": [{"repository_id": str(uuid.uuid4()), "owner": "canopy", "name": name}
                                 for name in ("source", "mirror")],
                "refs": {"refs/heads/main": "a" * 40}, "base_url": "http://127.0.0.1:1",
                "binary_sha256": "b" * 64}))
            fleet = root / "fleet"
            fleet.mkdir()
            ready = {"max_active_repositories_per_node": 100,
                     "proxy_url": "http://127.0.0.1:1", "binary_sha256": "b" * 64}
            (fleet / "ready.json").write_text(json.dumps(ready))
            args = SimpleNamespace(receipt=receipt, fleet_dir=fleet, repository_index=0,
                output_dir=root / "output", samples=3, timeout=30, max_pack_bytes=4096)
            identity = json.loads(receipt.read_text())["repositories"][0]["repository_id"]
            with patch.object(probe.campaign, "validate_fleet", return_value=ready), \
                 patch.object(benchmark.Client, "request", return_value=(200, json.dumps(
                     {"repository_id": identity}).encode())), \
                 patch.object(probe, "download", side_effect=ValueError("private-token-sensitive")) as download:
                with self.assertRaises(ValueError):
                    probe.measure(args, "private-token-sensitive")
                self.assertEqual(download.call_count, 1)
            output = (args.output_dir / "measurement.json").read_text()
            record = json.loads(output)
            self.assertFalse(record["complete"])
            self.assertEqual(record["error"], "ValueError")
            self.assertEqual(record["samples"], [{"index": 0, "complete": False, "error": "ValueError"}])
            self.assertNotIn("private-token-sensitive", output)

    def test_real_git_pack_over_fragmented_http_and_strict_validation(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source"
            token = "local-fixture-token"
            benchmark.git("init", "-b", "main", str(source), cwd=root, token=token)
            (source / "fixture.txt").write_text("complete graph\n")
            benchmark.git("add", ".", cwd=source, token=token)
            benchmark.git("-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid",
                          "commit", "-m", "fixture", cwd=source, token=token)
            oid = benchmark.git("rev-parse", "HEAD", cwd=source, token=token)
            body = probe.request_body(oid)
            environment = {"PATH": os.environ["PATH"], "GIT_CONFIG_NOSYSTEM": "1",
                           "GIT_CONFIG_GLOBAL": os.devnull}
            wire = subprocess.run(["git", "upload-pack", "--stateless-rpc", str(source)],
                                  input=body, capture_output=True, check=True, env=environment).stdout
            failures = []
            identity = str(uuid.uuid4())
            class Handler(BaseHTTPRequestHandler):
                def log_message(self, *_):
                    pass
                def do_GET(self):
                    if self.path != "/api/repositories/source":
                        self.send_error(404)
                        return
                    content = json.dumps({"repository_id": identity}).encode()
                    self.send_response(200)
                    self.send_header("Content-Length", str(len(content)))
                    self.end_headers()
                    self.wfile.write(content)
                def do_POST(self):
                    try:
                        self.assertions()
                        self.send_response(200)
                        self.send_header("Content-Type", "application/x-git-upload-pack-result")
                        self.send_header("Content-Length", str(len(wire)))
                        self.end_headers()
                        for index in range(0, len(wire), 3):
                            self.wfile.write(wire[index:index + 3])
                            self.wfile.flush()
                    except BaseException as error:
                        failures.append(type(error).__name__)
                def assertions(self):
                    if self.path != "/canopy/source.git/git-upload-pack" \
                            or self.headers["Authorization"] != "Bearer " + token \
                            or self.rfile.read(int(self.headers["Content-Length"])) != body:
                        raise AssertionError("request mismatch")
            server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
            worker = threading.Thread(target=server.serve_forever, daemon=True)
            worker.start()
            try:
                path = root / "measured.pack"
                result = probe.download(f"http://127.0.0.1:{server.server_port}",
                    {"owner": "canopy", "name": "source"}, oid, token, path,
                    timeout=30, max_pack_bytes=4096)
                self.assertEqual(failures, [])
                self.assertGreater(result["pack_bytes"], 32)
                self.assertLessEqual(result["http_headers_ms"], result["post_first_pack_byte_ms"])
                verified = probe.validate_pack(path, oid, root / "verified.git", token)
                self.assertTrue(verified["fsck_strict_full"])
                self.assertEqual(benchmark.git("show", "measured:fixture.txt",
                    cwd=root / "verified.git", token=token), "complete graph")
                corrupt = root / "corrupt.pack"
                data = bytearray(path.read_bytes())
                data[-1] ^= 1
                corrupt.write_bytes(data)
                with self.assertRaisesRegex(ValueError, "indexing failed"):
                    probe.validate_pack(corrupt, oid, root / "corrupt.git", token)
                with self.assertRaisesRegex(RuntimeError, "cat-file"):
                    probe.validate_pack(path, "0" * 40, root / "wrong-tip.git", token)
                incomplete_graph = root / "incomplete-graph.pack"
                incomplete_graph.write_bytes(subprocess.run(
                    ["git", "pack-objects", "--stdout"], cwd=source, env=environment,
                    input=(oid + "\n").encode(), capture_output=True, check=True).stdout)
                with self.assertRaises((ValueError, RuntimeError)):
                    probe.validate_pack(incomplete_graph, oid, root / "incomplete-graph.git", token)
                ready = {"max_active_repositories_per_node": 100,
                         "proxy_url": f"http://127.0.0.1:{server.server_port}", "binary_sha256": "b" * 64}
                fleet = root / "fleet"
                fleet.mkdir()
                (fleet / "ready.json").write_text(json.dumps(ready))
                receipt = root / "critical.json"
                receipt.write_text(json.dumps({"complete": True, "error": None,
                    "repositories": [{"repository_id": identity, "owner": "canopy", "name": "source"},
                                     {"repository_id": str(uuid.uuid4()), "owner": "canopy", "name": "mirror"}],
                    "refs": {"refs/heads/main": oid}, "base_url": ready["proxy_url"],
                    "binary_sha256": ready["binary_sha256"]}))
                args = SimpleNamespace(receipt=receipt, fleet_dir=fleet, repository_index=0,
                    output_dir=root / "serial", samples=2, timeout=30, max_pack_bytes=4096)
                with patch.object(probe.campaign, "validate_fleet", return_value=ready):
                    measured = probe.measure(args, token)
                self.assertTrue(measured["complete"])
                self.assertTrue(all(sample["complete"] for sample in measured["samples"]))
                self.assertEqual(len(measured["samples"]), 2)
                self.assertEqual(json.loads((args.output_dir / "measurement.json").read_text()), measured)
                self.assertEqual(failures, [])
            finally:
                server.shutdown()
                worker.join(timeout=5)
                server.server_close()


if __name__ == "__main__":
    unittest.main()
