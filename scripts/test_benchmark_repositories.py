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


class LfsHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *_args):
        pass

    def do_PUT(self):
        size = int(self.headers["Content-Length"])
        body = self.rfile.read(size)
        oid = self.path.rsplit("/", 1)[-1]
        status = 200 if hashlib.sha256(body).hexdigest() == oid else 422
        if status == 200:
            with self.server.guard:
                self.server.blobs[self.path] = body
        self.send_response(status)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def do_GET(self):
        with self.server.guard:
            body = self.server.blobs.get(self.path)
        self.send_response(200 if body is not None else 404)
        self.send_header("Content-Length", str(len(body) if body is not None else 0))
        self.end_headers()
        if body is not None:
            self.wfile.write(body)


class GitRefsHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *_args):
        pass

    def reply(self, body, content_type):
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    @staticmethod
    def packet(line):
        body = line.encode()
        return f"{len(body) + 4:04x}".encode() + body

    def do_GET(self):
        algorithm = "sha256" if len(self.server.oid) == 64 else "sha1"
        self.reply(self.packet("version 2\n") + self.packet("ls-refs\n")
                   + self.packet(f"object-format={algorithm}\n") + b"0000",
                   "application/x-git-upload-pack-advertisement")

    def do_POST(self):
        body = self.rfile.read(int(self.headers["Content-Length"]))
        with self.server.guard:
            self.server.commands.append({"body": body,
                "protocol": self.headers.get("Git-Protocol"),
                "request_id": self.headers.get("X-Request-ID")})
        self.reply(self.packet(f"{self.server.oid} refs/heads/main\n")
                   + self.packet(f"{self.server.oid} refs/tags/fixture\n") + b"0000",
                   "application/x-git-upload-pack-result")


class ScheduledLoad(unittest.TestCase):
    def test_ls_remote_uses_stock_git_v2_and_rejects_wrong_tips(self):
        server = ThreadingHTTPServer(("127.0.0.1", 0), GitRefsHandler)
        server.guard, server.commands, server.oid = threading.Lock(), [], "1" * 40
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        client = benchmark.Client(f"http://127.0.0.1:{server.server_port}", "fixture-token", 2)
        try:
            with tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                manifest = root / "manifest.json"
                corpus = {"version": 1, "complete": True,
                    "requested_repositories": 2, "repositories": [
                        {"name": "fixture", "owner": "canopy",
                         "repository_id": str(uuid.uuid4()), "commit": server.oid},
                        {"name": "empty", "owner": "canopy",
                         "repository_id": str(uuid.uuid4()), "commit": None}]}
                for name, advertised, expected_tip, expected in (
                        ("valid", "1" * 40, "1" * 40, "ok"),
                        ("wrong-tip", "2" * 40, "1" * 40, "invalid_response"),
                        ("sha256", "3" * 64, "3" * 64, "ok")):
                    server.oid = advertised
                    corpus["repositories"][0]["commit"] = expected_tip
                    manifest.write_text(json.dumps(corpus))
                    args = SimpleNamespace(manifest=manifest, active_repositories=1, seed=42,
                        duration=1, rate=2, concurrency=2, distribution="uniform",
                        operation="ls_remote", timeout=2, git_timeout=2,
                        work_dir=root / f"{name}-scratch", output=root / f"{name}.json")
                    report = benchmark.measure(args, client, "fixture-token")
                    self.assertEqual(report["outcomes"], {expected: 2})
                    self.assertEqual(report["eligible_repositories"], 1)
                    self.assertEqual(report["git_discovery_kind"], "v2_ls_refs_main_tip")
                    self.assertNotIn("fixture-token", args.output.read_text())
                self.assertEqual(len(server.commands), 6)
                for command in server.commands:
                    self.assertIn(b"command=ls-refs\n", command["body"])
                    self.assertNotIn(b"command=fetch\n", command["body"])
                    self.assertEqual(command["protocol"], "version=2")
                    uuid.UUID(command["request_id"])
                for command in server.commands[-2:]:
                    self.assertIn(b"object-format=sha256", command["body"])

                before = len(server.commands)
                args.operation = "refs"
                args.output = root / "capabilities.json"
                report = benchmark.measure(args, client, "fixture-token")
                self.assertEqual(report["outcomes"], {"ok": 2})
                self.assertEqual(report["git_discovery_kind"], "v2_capabilities_only")
                self.assertEqual(len(server.commands), before)
        finally:
            client.close()
            server.shutdown()
            server.server_close()
            thread.join()

    def test_parallel_verify_rejects_identity_mismatch(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            entries = [{"name": f"repo-{index}", "owner": "canopy",
                        "repository_id": str(uuid.uuid4()), "commit": None}
                       for index in range(2)]
            manifest = root / "manifest.json"
            manifest.write_text(json.dumps({"version": 1, "complete": True,
                "requested_repositories": 2, "repositories": entries}))

            class MismatchClient:
                def request(self, path):
                    entry = next(entry for entry in entries if path.endswith(entry["name"]))
                    identity = entry["repository_id"] if entry is entries[0] else str(uuid.uuid4())
                    return 200, json.dumps({"repository_id": identity}).encode()

            args = SimpleNamespace(manifest=manifest, work_dir=root / "verified",
                                   concurrency=2)
            with self.assertRaisesRegex(RuntimeError, "identity differs"):
                benchmark.verify(args, MismatchClient(), "fixture-token")

    def test_incremental_seed_and_verify_preserve_both_commits(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            remotes = root / "remotes"
            remotes.mkdir()

            class LocalClient:
                def __init__(self):
                    self.entries = {}
                    self.lfs = {}

                def request(self, path, payload=None):
                    if payload is not None:
                        name = payload["name"]
                        entry = {"name": name, "owner": "canopy",
                                 "repository_id": str(uuid.uuid4())}
                        self.entries[name] = entry
                        remote = remotes / "canopy" / f"{name}.git"
                        remote.parent.mkdir(exist_ok=True)
                        benchmark.git("init", "--bare", "-b", "main", str(remote),
                                      cwd=root, token="fixture-token")
                        return 200, json.dumps(entry).encode()
                    name = path.rsplit("/", 1)[-1]
                    return 200, json.dumps(self.entries[name]).encode()

                def lfs_put(self, path, content, request_id=None):
                    if hashlib.sha256(content).hexdigest() != path.rsplit("/", 1)[-1]:
                        return 422
                    self.lfs[path] = content
                    return 200

                def lfs_get(self, path, expected_size, request_id=None):
                    body = self.lfs.get(path)
                    if body is None:
                        return 404, 0, None
                    return 200, len(body), hashlib.sha256(body).hexdigest()

            client = LocalClient()
            manifest = root / "manifest.json"
            args = SimpleNamespace(manifest=manifest, work_dir=root / "seed", seed=42,
                                   repositories=2, populated=1, incremental_fixture=True,
                                   lfs_fixture_bytes=128,
                                   base_url=remotes.as_uri())
            self.assertEqual(benchmark.seed(args, client, "fixture-token")["populated"], 1)
            entries = benchmark.corpus(manifest)["repositories"]
            populated = [entry for entry in entries if entry["commit"] is not None]
            self.assertEqual(len(populated), 1)
            self.assertNotEqual(populated[0]["base_commit"], populated[0]["commit"])
            self.assertEqual(populated[0]["lfs_size"], 128)
            check = SimpleNamespace(manifest=manifest, work_dir=root / "verified",
                                    base_url=remotes.as_uri(), concurrency=1)
            self.assertEqual(benchmark.verify(check, client, "fixture-token")
                             ["git_v0_v2_samples"], 1)
            parallel = SimpleNamespace(manifest=manifest, work_dir=root / "verified-parallel",
                                       base_url=remotes.as_uri(), concurrency=2)
            self.assertEqual(benchmark.verify(parallel, client, "fixture-token"),
                             {"verified_repositories": 2, "git_v0_v2_samples": 1})
            baseline_manifest = root / "baseline.json"
            baseline = SimpleNamespace(manifest=baseline_manifest, work_dir=root / "baseline-seed",
                seed=42, repositories=1, populated=1, incremental_fixture=False,
                lfs_fixture_bytes=0,
                base_url=remotes.as_uri())
            benchmark.seed(baseline, client, "fixture-token")
            self.assertEqual(benchmark.corpus(baseline_manifest)["version"], 1)
            check = SimpleNamespace(manifest=baseline_manifest,
                                    work_dir=root / "baseline-verified",
                                    base_url=remotes.as_uri(), concurrency=1)
            self.assertEqual(benchmark.verify(check, client, "fixture-token")
                             ["git_v0_v2_samples"], 1)

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

    def test_lfs_upload_and_streamed_download_across_ingresses(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            body = b"LFS fixture\n" * (3 * 1024 * 1024 // len(b"LFS fixture\n"))
            oid = hashlib.sha256(body).hexdigest()
            path = f"/canopy/fixture.git/info/lfs/objects/{oid}"
            blobs = {path: body}
            guard = threading.Lock()
            servers = [ThreadingHTTPServer(("127.0.0.1", 0), LfsHandler) for _ in range(2)]
            threads = []
            clients = []
            for server in servers:
                server.blobs = blobs
                server.guard = guard
                thread = threading.Thread(target=server.serve_forever, daemon=True)
                thread.start()
                threads.append(thread)
                clients.append(benchmark.Client(f"http://127.0.0.1:{server.server_port}",
                                                "fixture-token", 5))
            try:
                manifest = root / "manifest.json"
                manifest.write_text(json.dumps({"version": 1, "complete": True,
                    "requested_repositories": 1, "repositories": [
                        {"name": "fixture", "owner": "canopy",
                         "repository_id": str(uuid.uuid4()), "commit": None,
                         "lfs_oid": oid, "lfs_size": len(body)}]}))
                args = SimpleNamespace(manifest=manifest, active_repositories=1, seed=42,
                    duration=1, rate=2, concurrency=2, distribution="uniform",
                    operation="lfs_download", timeout=5, output=root / "download.json")
                report = benchmark.measure(args, clients, "fixture-token")
                self.assertEqual(report["outcomes"], {"ok": 2})
                self.assertEqual([item["outcomes"] for item in report["ingresses"]],
                                 [{"ok": 1}, {"ok": 1}])
                with guard:
                    blobs[path] = b"X" + body[1:]
                args.output = root / "corrupt-download.json"
                report = benchmark.measure(args, clients, "fixture-token")
                self.assertEqual(report["outcomes"], {"invalid_response": 2})
                self.assertEqual(report["failed_arrivals"], 2)
                with guard:
                    blobs[path] = body
                args.operation = "lfs_upload"
                args.lfs_bytes = 1024
                args.output = root / "upload.json"
                report = benchmark.measure(args, clients, "fixture-token")
                self.assertEqual(report["outcomes"], {"ok": 2})
                samples = [json.loads(line) for line in
                           (root / "upload.samples.jsonl").read_text().splitlines()]
                self.assertEqual(len({sample["lfs_oid"] for sample in samples}), 2)
                for sample in samples:
                    uploaded = blobs[f"/canopy/fixture.git/info/lfs/objects/{sample['lfs_oid']}"]
                    self.assertEqual(len(uploaded), 1024)
                    self.assertEqual(hashlib.sha256(uploaded).hexdigest(), sample["lfs_oid"])
                self.assertNotIn("fixture-token", args.output.read_text())
                recovery = SimpleNamespace(manifest=manifest, reports=[args.output],
                    work_dir=root / "lfs-ack-check", output=root / "lfs-ack-check.json",
                    git_timeout=30)
                recovered = benchmark.verify_writes(recovery, clients[1], "fixture-token")
                self.assertEqual(recovered["verified_lfs_objects"], 2)
                self.assertEqual(recovered["verified_git_refs"], 0)
                with guard:
                    blobs[f"/canopy/fixture.git/info/lfs/objects/{samples[0]['lfs_oid']}"] = b"X" * 1024
                recovery.work_dir = root / "corrupt-lfs-ack-check"
                recovery.output = root / "corrupt-lfs-ack-check.json"
                with self.assertRaisesRegex(RuntimeError, "acknowledged LFS"):
                    benchmark.verify_writes(recovery, clients[0], "fixture-token")
                self.assertFalse(recovery.output.exists())
                # A lost acknowledgement is not proof of either success or
                # rollback. Only the remaining acknowledged object is asserted.
                with guard:
                    del blobs[f"/canopy/fixture.git/info/lfs/objects/{samples[0]['lfs_oid']}"]
                samples[0]["result"] = "client_timeout"
                sample_path = args.output.with_suffix(".samples.jsonl")
                sample_path.write_text("".join(json.dumps(sample) + "\n" for sample in samples))
                report["samples_sha256"] = benchmark.file_sha256(sample_path)
                report["outcomes"], report["failed_arrivals"] = {"ok": 1, "client_timeout": 1}, 1
                args.output.write_text(json.dumps(report))
                recovery.work_dir = root / "partial-lfs-ack-check"
                recovery.output = root / "partial-lfs-ack-check.json"
                recovered = benchmark.verify_writes(recovery, clients[0], "fixture-token")
                self.assertEqual(recovered["verified_lfs_objects"], 1)
                self.assertEqual(recovered["unacknowledged_arrivals_not_asserted"], 1)
                args.lfs_bytes = 16 * 1024 * 1024
                args.concurrency = 32
                args.output = root / "too-large.json"
                with self.assertRaises(ValueError):
                    benchmark.measure(args, clients, "fixture-token")
                self.assertFalse(args.output.exists())
            finally:
                for client in clients:
                    client.close()
                for server in servers:
                    server.shutdown()
                    server.server_close()
                for thread in threads:
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
            recovery = SimpleNamespace(manifest=manifest, reports=[args.output],
                work_dir=root / "push-ack-check", output=root / "push-ack-check.json",
                git_timeout=30)
            recovered = benchmark.verify_writes(recovery, ingresses[1], "fixture-token")
            self.assertEqual(recovered["verified_git_refs"], 2)
            self.assertEqual(recovered["verified_git_repositories"], 1)
            self.assertEqual(recovered["verified_lfs_objects"], 0)
            reference = f"refs/heads/canopy-benchmark/{report['push_run_id']}/0000000"
            benchmark.git("update-ref", reference, entry["commit"], cwd=remote,
                          token="fixture-token")
            recovery.work_dir = root / "wrong-push-ack-check"
            recovery.output = root / "wrong-push-ack-check.json"
            with self.assertRaisesRegex(RuntimeError, "acknowledged Git ref"):
                benchmark.verify_writes(recovery, ingresses[0], "fixture-token")
            self.assertFalse(recovery.output.exists())
            benchmark.git("update-ref", reference, report["push_commit"], cwd=remote,
                          token="fixture-token")
            sample_path = args.output.with_suffix(".samples.jsonl")
            sample_path.write_text(sample_path.read_text() + "{}\n")
            recovery.work_dir = root / "tampered-push-ack-check"
            recovery.output = root / "tampered-push-ack-check.json"
            with self.assertRaisesRegex(ValueError, "sample digest"):
                benchmark.verify_writes(recovery, ingresses[0], "fixture-token")
            self.assertFalse(recovery.work_dir.exists())
            entry["base_commit"] = entry["commit"]
            benchmark.git("branch", "benchmark-base", cwd=source, token="fixture-token")
            increment = b"new commit for an incremental transfer\n"
            (source / "incremental.txt").write_bytes(increment)
            benchmark.git("add", "incremental.txt", cwd=source, token="fixture-token")
            benchmark.git("commit", "-m", "Incremental", cwd=source, token="fixture-token")
            entry["commit"] = benchmark.git("rev-parse", "HEAD", cwd=source,
                                            token="fixture-token")
            entry["incremental_sha256"] = hashlib.sha256(increment).hexdigest()
            benchmark.git("push", str(remote), "HEAD:refs/heads/main",
                          "benchmark-base:refs/heads/benchmark-base", cwd=source,
                          token="fixture-token")
            manifest.write_text(json.dumps({"version": 2, "complete": True,
                "requested_repositories": 1, "repositories": [entry]}))
            for operation in ("incremental_fetch", "incremental_pull"):
                args = SimpleNamespace(manifest=manifest, active_repositories=1, seed=42,
                    duration=1, rate=2, concurrency=2, distribution="uniform",
                    operation=operation, timeout=2, git_timeout=30,
                    work_dir=root / f"{operation}-scratch", output=root / f"{operation}.json")
                report = benchmark.measure(args, ingresses, "fixture-token")
                self.assertEqual(report["outcomes"], {"ok": 2})
                self.assertIsNotNone(report["incremental_client_setup_seconds"])
                self.assertEqual([item["outcomes"] for item in report["ingresses"]],
                                 [{"ok": 1}, {"ok": 1}])
                self.assertNotIn("fixture-token", args.output.read_text())

    def test_sha256_acknowledged_push_checks_exact_body_and_both_protocols(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source, remote = root / "source", root / "canopy" / "fixture.git"
            remote.parent.mkdir()
            for path, bare in ((source, False), (remote, True)):
                benchmark.git("init", "--object-format=sha256", "-b", "main",
                              *(["--bare"] if bare else []), str(path), cwd=root, token="fixture-token")
            benchmark.git("config", "user.name", "Fixture", cwd=source, token="fixture-token")
            benchmark.git("config", "user.email", "fixture@example.invalid", cwd=source, token="fixture-token")
            run_id = uuid.uuid4().hex
            repository_id = str(uuid.uuid4())
            reference = f"refs/heads/canopy-benchmark/{run_id}/0000000"
            manifest = root / "manifest.json"
            manifest.write_text(json.dumps({"version": 1, "complete": True,
                "requested_repositories": 1, "repositories": [{"name": "fixture", "owner": "canopy",
                "repository_id": repository_id, "commit": None}]}))
            report_path = root / "push.json"
            samples_path = report_path.with_suffix(".samples.jsonl")
            samples_path.write_text(json.dumps({"sequence": 0, "repository_id": repository_id,
                                              "result": "ok"}) + "\n")
            for case, ending in (("wrong-body", ""), ("exact-body", "\n")):
                (source / "README.md").write_text(f"benchmark run {run_id}{ending}")
                benchmark.git("add", "README.md", cwd=source, token="fixture-token")
                benchmark.git("commit", "-m", case, cwd=source, token="fixture-token")
                benchmark.git("push", str(remote), f"HEAD:{reference}", cwd=source, token="fixture-token")
                commit = benchmark.git("rev-parse", "HEAD", cwd=source, token="fixture-token")
                self.assertEqual(len(commit), 64)
                report_path.write_text(json.dumps({"version": 1, "operation": "push_branch",
                    "scheduled": 1, "outcomes": {"ok": 1}, "failed_arrivals": 0,
                    "manifest_sha256": benchmark.file_sha256(manifest),
                    "samples_sha256": benchmark.file_sha256(samples_path),
                    "push_run_id": run_id, "push_commit": commit}))
                args = SimpleNamespace(manifest=manifest, reports=[report_path],
                    work_dir=root / f"{case}-scratch", output=root / f"{case}.json", git_timeout=30)
                client = SimpleNamespace(base_url=root.as_uri())
                if case == "wrong-body":
                    with self.assertRaisesRegex(RuntimeError, "acknowledged Git body"):
                        benchmark.verify_writes(args, client, "fixture-token")
                    self.assertFalse(args.output.exists())
                else:
                    recovered = benchmark.verify_writes(args, client, "fixture-token")
                    self.assertEqual(recovered["verified_git_refs"], 1)
                    self.assertEqual(recovered["git_protocols"], [0, 2])
                    self.assertEqual(recovered["unacknowledged_arrivals_not_asserted"], 0)

    def test_write_verification_rejects_inconsistent_evidence_before_io(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            repository_id = str(uuid.uuid4())
            manifest = root / "manifest.json"
            manifest.write_text(json.dumps({"version": 1, "complete": True,
                "requested_repositories": 1, "repositories": [{"name": "fixture", "owner": "canopy",
                "repository_id": repository_id, "commit": None}]}))
            report_path = root / "writes.json"
            samples_path = report_path.with_suffix(".samples.jsonl")
            base_samples = [{"sequence": 0, "repository_id": repository_id, "result": "ok", "lfs_oid": "1" * 64},
                            {"sequence": 1, "repository_id": repository_id, "result": "driver_busy"}]
            base_report = {"version": 1, "operation": "lfs_upload", "scheduled": 2,
                "outcomes": {"ok": 1, "driver_busy": 1}, "failed_arrivals": 1,
                "manifest_sha256": benchmark.file_sha256(manifest),
                "lfs_run_id": "2" * 32, "lfs_size_bytes": 128}
            for case in ("duplicate-sequence", "missing-sample", "unknown-repository",
                         "wrong-outcomes", "wrong-failures", "wrong-manifest",
                         "no-acks", "duplicate-run", "invalid-lfs-oid", "invalid-sample",
                         "invalid-repository", "invalid-result"):
                with self.subTest(case=case):
                    samples = [dict(sample) for sample in base_samples]
                    report = dict(base_report)
                    if case == "duplicate-sequence":
                        samples[1]["sequence"] = 0
                    elif case == "missing-sample":
                        samples.pop()
                    elif case == "unknown-repository":
                        samples[0]["repository_id"] = str(uuid.uuid4())
                    elif case == "wrong-outcomes":
                        report["outcomes"] = {"ok": 2}
                        report["failed_arrivals"] = 0
                    elif case == "wrong-failures":
                        report["failed_arrivals"] = 0
                    elif case == "wrong-manifest":
                        report["manifest_sha256"] = "0" * 64
                    elif case == "no-acks":
                        samples[0]["result"] = "driver_busy"
                        report["outcomes"], report["failed_arrivals"] = {"driver_busy": 2}, 2
                    elif case == "invalid-lfs-oid":
                        samples[0]["lfs_oid"] = []
                    elif case == "invalid-sample":
                        samples[0] = None
                    elif case == "invalid-repository":
                        samples[0]["repository_id"] = []
                    elif case == "invalid-result":
                        samples[0]["result"] = []
                    samples_path.write_text("".join(json.dumps(sample) + "\n" for sample in samples))
                    report["samples_sha256"] = benchmark.file_sha256(samples_path)
                    report_path.write_text(json.dumps(report))
                    args = SimpleNamespace(manifest=manifest,
                        reports=[report_path] * (2 if case == "duplicate-run" else 1),
                        work_dir=root / f"{case}-scratch", output=root / f"{case}.json", git_timeout=30)
                    # There is deliberately no client: validation must precede
                    # scratch creation and any HTTP or Git operation.
                    with self.assertRaises(ValueError):
                        benchmark.verify_writes(args, None, "fixture-token")
                    self.assertFalse(args.work_dir.exists())
                    self.assertFalse(args.output.exists())


if __name__ == "__main__":
    unittest.main()
