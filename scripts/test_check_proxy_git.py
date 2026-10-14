"""Critical-probe guards; the real wire operations still need a live provider."""

import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch
import uuid

import benchmark_repositories as benchmark
import check_proxy_git as probe


class ProbeTests(unittest.TestCase):
    def receipt(self):
        return {"version": 1, "complete": True, "repositories": [
            {"repository_id": str(uuid.uuid4()), "name": "source", "owner": "canopy"},
            {"repository_id": str(uuid.uuid4()), "name": "mirror", "owner": "canopy"}],
            "refs": {"refs/heads/main": "a" * 40}, "payload_sha256": "b" * 64,
            "note": "retained critical note"}

    def test_bad_recovery_receipts_are_rejected_before_requests_or_git(self):
        for mutation in (lambda record: record.update(complete=False),
                         lambda record: record["repositories"][1].update(
                             repository_id=record["repositories"][0]["repository_id"]),
                         lambda record: record["repositories"][0].update(repository_id=str(uuid.UUID(int=0))),
                         lambda record: record["repositories"][1].update(name="../escape"),
                         lambda record: record.update(refs={}),
                         lambda record: record.update(payload_sha256="wrong")):
            record = self.receipt()
            mutation(record)
            with tempfile.TemporaryDirectory() as directory:
                destination = Path(directory) / "unused"
                client = SimpleNamespace(request=lambda *args, **kwargs: self.fail("unexpected request"))
                with patch.object(benchmark, "git", side_effect=AssertionError("unexpected Git")):
                    with self.assertRaises(RuntimeError):
                        probe.verify_receipt(record, "http://127.0.0.1:1", destination, client, "fixture-token")
                self.assertFalse(destination.exists())

    def test_changed_remote_identity_fails_before_clone(self):
        record = self.receipt()
        client = SimpleNamespace(request=lambda *args, **kwargs: (200, json.dumps(
            {"repository_id": str(uuid.uuid4())}).encode()))
        with tempfile.TemporaryDirectory() as directory, \
             patch.object(benchmark, "git", side_effect=AssertionError("unexpected clone")):
            with self.assertRaisesRegex(RuntimeError, "identity differs"):
                probe.verify_receipt(record, "http://127.0.0.1:1", Path(directory) / "verify", client, "fixture-token")

    def test_remote_inventory_preserves_unicode_and_rejects_duplicates(self):
        oid = "a" * 40
        with patch.object(benchmark, "git", return_value=f"{oid}\trefs/heads/🌳\n{oid}\trefs/tags/版本"):
            self.assertEqual(probe.remote_refs("http://127.0.0.1:1/repo.git", Path("."), "fixture-token"),
                             {"refs/heads/🌳": oid, "refs/tags/版本": oid})
        with patch.object(benchmark, "git", return_value=f"{oid}\trefs/heads/main\n{oid}\trefs/heads/main"):
            with self.assertRaisesRegex(RuntimeError, "duplicate remote ref"):
                probe.remote_refs("http://127.0.0.1:1/repo.git", Path("."), "fixture-token")

    def test_git_result_keeps_credentials_out_of_argv_and_isolates_configuration(self):
        result = SimpleNamespace(returncode=1, stdout=b"", stderr=b"remote rejection")
        with patch.dict("os.environ", {"GIT_TRACE": "1", "AWS_SECRET_ACCESS_KEY": "not-for-git",
                                       "CANOPY_GIT_TOKEN": "not-global", "GIT_CONFIG_GLOBAL": "user-file"}), \
             patch.object(benchmark.subprocess, "run", return_value=result) as run:
            observed = benchmark.git_result("push", "http://127.0.0.1:1/repo.git", cwd=Path("."),
                                            token="private-fixture-token", request_id="fixture-request")
            self.assertIs(observed, result)
            arguments, options = run.call_args
            self.assertNotIn("private-fixture-token", " ".join(arguments[0]))
            environment = options["env"]
            self.assertNotIn("GIT_TRACE", environment)
            self.assertNotIn("AWS_SECRET_ACCESS_KEY", environment)
            self.assertNotIn("CANOPY_GIT_TOKEN", environment)
            self.assertEqual(environment["GIT_CONFIG_VALUE_1"], "Authorization: Bearer private-fixture-token")
            self.assertEqual(environment["GIT_CONFIG_VALUE_2"], "X-Request-ID: fixture-request")


if __name__ == "__main__":
    unittest.main()
