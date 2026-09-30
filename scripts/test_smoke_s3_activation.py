import argparse
import contextlib
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

import smoke_s3_activation as activation


class ActivationHarnessTests(unittest.TestCase):
    def test_qualification_passes_required_benchmark_arguments(self):
        entries = [{"name": f"repo-{i}", "repository_id": str(i)} for i in range(64)]
        client = Mock()
        client.request.side_effect = lambda path: (
            200, json.dumps({"repository_id": path.rsplit("-", 1)[1]}).encode())
        process = Mock(returncode=0)
        process.poll.return_value = 0

        def seed(args, *_):
            self.assertFalse(args.incremental_fixture)
            self.assertEqual(args.lfs_fixture_bytes, 0)
            return {"seeded": 64, "populated": 3}

        def verify(args, *_):
            self.assertEqual(args.concurrency, 8)
            return {"verified_repositories": 64, "git_v0_v2_samples": 3}

        with tempfile.TemporaryDirectory() as root:
            args = argparse.Namespace(binary=Path(__file__),
                storage_url="s3://fixture/activation", work_dir=Path(root) / "run",
                concurrency=8)
            with patch.dict(activation.os.environ, {"CANOPY_GIT_TOKEN": "local-test-token"}), \
                    patch.object(activation, "start", return_value=(process, "http://fixture")), \
                    patch.object(activation, "Client", return_value=client), \
                    patch.object(activation, "seed", side_effect=seed), \
                    patch.object(activation, "verify", side_effect=verify), \
                    patch.object(activation, "corpus", return_value={"repositories": entries}), \
                    patch.object(activation.time, "sleep"), \
                    contextlib.redirect_stdout(io.StringIO()):
                activation.qualify(args)
            report = json.loads((args.work_dir / "report.json").read_text())
            self.assertEqual(report["phase"], "complete")
            self.assertTrue(report["cold_recovery_passed"])
            self.assertTrue(report["git_recovery_passed"])
            self.assertTrue(report["shutdown_passed"])


if __name__ == "__main__":
    unittest.main()
