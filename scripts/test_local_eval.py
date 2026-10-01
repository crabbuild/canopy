"""Focused guards for persistent evaluation identity and secret handling."""
import argparse
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import local_eval


class EvaluationIdentityTests(unittest.TestCase):
    def test_restart_reuses_identity_and_private_credentials(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            binary = root / "canopy"
            binary.write_bytes(b"release fixture")
            state = root / "state"
            state.mkdir()
            args = argparse.Namespace(state_dir=state, binary=binary, port=18080,
                provider_image="fixture@sha256:abc", disk_gib=64, active_repositories=3)
            first = local_eval.initialize(args)
            original = (state / "secrets.json").read_bytes()
            config = (state / "config.json").read_bytes()
            args.port = 18081
            self.assertEqual(first, local_eval.initialize(args))
            self.assertEqual(original, (state / "secrets.json").read_bytes())
            self.assertEqual(config, (state / "config.json").read_bytes())
            if os.name == "posix":
                self.assertEqual(0o600, (state / "secrets.json").stat().st_mode & 0o777)
            self.assertNotIn(b"CANOPY_GIT_TOKEN", config)
            with patch.dict(os.environ, {"AWS_SESSION_TOKEN": "unrelated-token",
                                         "AWS_ENDPOINT": "unrelated-endpoint"}):
                environment = local_eval.environment(state)
            self.assertNotIn("AWS_SESSION_TOKEN", environment)
            self.assertNotIn("AWS_ENDPOINT", environment)

    def test_existing_unrelated_directory_is_rejected(self):
        with tempfile.TemporaryDirectory() as temporary:
            state = Path(temporary)
            (state / "unrelated").write_text("preserve me")
            with self.assertRaisesRegex(RuntimeError, "not empty"):
                local_eval.initialize(argparse.Namespace(state_dir=state))

    def test_stale_pid_does_not_signal_an_unrelated_process(self):
        with tempfile.TemporaryDirectory() as temporary:
            state = Path(temporary)
            (state / "server.pid").write_text("123")
            with patch.object(local_eval, "run", return_value="another-service"):
                self.assertIsNone(local_eval.owned_pid(state, {"binary": "/test/canopy"}))

    def test_changed_binary_is_rejected_before_provider_operations(self):
        with tempfile.TemporaryDirectory() as temporary:
            state = Path(temporary)
            binary = state / "canopy"
            binary.write_bytes(b"changed release")
            with self.assertRaisesRegex(RuntimeError, "binary changed"):
                local_eval.start(argparse.Namespace(state_dir=state),
                    {"binary": str(binary), "binary_sha256": "old"})


if __name__ == "__main__":
    unittest.main()
