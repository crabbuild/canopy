"""New-object workload and receipt verification against real local stock Git.

The file:// backend tests driver mechanics only, not Canopy, RustFS or throughput.
"""

import hashlib
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
import uuid

import benchmark_repositories as benchmark


class PushCommitTests(unittest.TestCase):
    def test_each_ack_has_distinct_payload_and_child_commit_and_survives_verification(self):
        token = "fixture-token"
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            backend = root / "backend"
            (backend / "canopy").mkdir(parents=True)
            remote = backend / "canopy" / "fixture.git"
            source = root / "source"
            def git(*args, cwd=source):
                return benchmark.git(*args, cwd=cwd, token=token)
            git("init", "--bare", "-b", "main", str(remote), cwd=root)
            git("init", "-b", "main", str(source), cwd=root)
            git("config", "user.name", "Canopy Test")
            git("config", "user.email", "test@example.invalid")
            readme = b"immutable corpus tip\n"
            (source / "README.md").write_bytes(readme)
            git("add", ".")
            git("commit", "-m", "Original")
            tip = git("rev-parse", "HEAD")
            git("push", str(remote), "HEAD:refs/heads/main")
            manifest = root / "corpus.json"
            entry = {"name": "fixture", "owner": "canopy", "repository_id": str(uuid.uuid4()),
                     "commit": tip, "readme_sha256": hashlib.sha256(readme).hexdigest()}
            benchmark.save(manifest, {"version": 1, "complete": True,
                "requested_repositories": 1, "repositories": [entry]})
            client = SimpleNamespace(base_url=backend.as_uri())
            args = SimpleNamespace(operation="push_commit", manifest=manifest, seed=42,
                active_repositories=1, distribution="uniform", rate=2, duration=1, concurrency=2,
                timeout=30, git_timeout=30, git_payload_bytes=128 * 1024,
                work_dir=root / "load-clients", output=root / "load.json")
            report = benchmark.measure(args, client, token)
            self.assertEqual(report["outcomes"], {"ok": 2})
            self.assertEqual(report["acknowledged_new_git_payload_bytes"], 2 * args.git_payload_bytes)
            self.assertIsNone(report["push_commit"])
            self.assertGreater(report["git_push_command_ms"]["p50"], 0)
            self.assertGreater(report["git_client_preparation_ms"]["p50"], 0)
            self.assertEqual(git("rev-parse", "refs/heads/main", cwd=remote), tip)
            samples_path = args.output.with_suffix(".samples.jsonl")
            samples = [json.loads(line) for line in samples_path.read_text().splitlines()]
            self.assertEqual(len({sample["push_commit"] for sample in samples}), 2)
            for sample in samples:
                commit = sample["push_commit"]
                self.assertGreater(sample["git_push_command_ms"], 0)
                self.assertGreater(sample["git_client_preparation_ms"], 0)
                self.assertEqual(git("rev-parse", f"{commit}^", cwd=remote), tip)
                expected = benchmark.push_payload(report["push_run_id"], sample["sequence"], args.git_payload_bytes)
                expected_oid = hashlib.sha1(f"blob {len(expected)}\0".encode() + expected).hexdigest()
                self.assertEqual(git("rev-parse", f"{commit}:canopy-benchmark.bin", cwd=remote), expected_oid)
            checks = SimpleNamespace(manifest=manifest, reports=[args.output], git_timeout=30,
                                     work_dir=root / "verification", output=root / "verified.json")
            verified = benchmark.verify_writes(checks, client, token)
            self.assertEqual(verified["verified_git_refs"], 2)
            self.assertEqual(verified["git_protocols"], [0, 2])
            # A valid commit OID for the wrong original tip must still fail recovery.
            original_commit = samples[0]["push_commit"]
            samples[0]["push_commit"] = tip
            samples_path.write_text("".join(json.dumps(sample) + "\n" for sample in samples))
            report["samples_sha256"] = benchmark.file_sha256(samples_path)
            benchmark.save(args.output, report)
            checks.work_dir, checks.output = root / "wrong-ref-clients", root / "wrong-ref.json"
            with self.assertRaisesRegex(RuntimeError, "Git ref differs"):
                benchmark.verify_writes(checks, client, token)
            self.assertFalse(checks.output.exists())
            samples[0]["push_commit"] = original_commit
            samples_path.write_text("".join(json.dumps(sample) + "\n" for sample in samples))
            report["samples_sha256"] = benchmark.file_sha256(samples_path)
            report["git_payload_size_bytes"] += 1
            benchmark.save(args.output, report)
            checks.work_dir, checks.output = root / "wrong-body-clients", root / "wrong-body.json"
            with self.assertRaisesRegex(RuntimeError, "Git body differs"):
                benchmark.verify_writes(checks, client, token)
            self.assertFalse(checks.output.exists())

    def test_payloads_are_reproducible_distinct_and_not_compressible_fixture_repeats(self):
        run = "a" * 32
        with self.assertRaisesRegex(ValueError, "32 bytes"):
            benchmark.push_payload(run, 1, 1)
        one = benchmark.push_payload(run, 1, 1024)
        self.assertEqual(len(one), 1024)
        self.assertEqual(one, benchmark.push_payload(run, 1, 1024))
        self.assertNotEqual(one, benchmark.push_payload(run, 2, 1024))
        self.assertGreater(len(set(one)), 200)


if __name__ == "__main__":
    unittest.main()
