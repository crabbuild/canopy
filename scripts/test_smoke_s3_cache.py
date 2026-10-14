"""Keep the incremental cursor fixture separate from reachable clone hydration."""
import argparse
import os
from pathlib import Path
import shutil
import tempfile
import unittest
import uuid
from unittest.mock import Mock, patch

import smoke_s3_cache as smoke


class StopBeforeIncrement(Exception):
    pass


class CacheFixtureTests(unittest.TestCase):
    def test_warm_ref_snapshot_counts_deleted_probe_but_lists_only_live_refs(self):
        names = [f"refs/tags/warm-{index:03}" for index in range(300)]
        listing = b"new\tHEAD\nnew\trefs/heads/main\n" + b"\n".join(
            f"new\t{name}".encode() for name in names)
        specific_reads = 0

        def git(*command, cwd=None):
            nonlocal specific_reads
            if command[:1] == ("rev-parse",):
                return b"old" if command[-1] == "HEAD~1" else b"new"
            if "ls-remote" in command:
                if command[-1] == names[0]:
                    specific_reads += 1
                    return b"" if specific_reads == 1 else f"old\t{names[0]}".encode()
                return listing
            return b""

        prefix = "read Git ref snapshot refs=302\n"
        log = Mock(read_text=Mock(side_effect=[prefix, prefix + "reused Git ref snapshot\n" * 10]))
        report = {}
        with patch.object(smoke, "git", side_effect=git):
            smoke.warm_ref_pages(Path("source"), "http://fixture/repo.git", Path("work"),
                                 log, report, extra_tombstones=1)
        self.assertEqual(report["warm_ref_count"], 301)
        self.assertEqual(report["warm_snapshot_row_count"], 302)
        self.assertTrue(report["warm_ref_generation_passed"])

    def test_receive_cursor_is_warmed_before_incremental_measurement(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            binary = root / "canopy"
            binary.write_bytes(b"unit-test binary")
            args = argparse.Namespace(binary=binary, storage_url="s3://fixture",
                                      work_dir=root / "run")
            calls = []

            def start(*_args):
                settings = _args[2]
                identifiers = [uuid.UUID(settings[key]) for key in ("tenant_id", "application_id", "node_id")]
                identifiers.append(uuid.UUID(settings["storage_url"].rsplit("/", 1)[1]))
                self.assertEqual(len(set(identifiers)), 4)
                self.assertTrue(all(identifier.version == 4 for identifier in identifiers))
                (args.work_dir / "first.log").write_text(
                    "hydrated Git cache objects=0 scanned=260 "
                    "from_sequence=0 through_sequence=260 bytes=0\n")
                return Mock(poll=Mock(return_value=0)), "http://fixture"

            def git(*command, cwd=None):
                calls.append(command)
                if command[:1] == ("init",):
                    Path(command[-1]).mkdir()
                elif "clone" in command:
                    shutil.copytree(args.work_dir / "source", Path(command[-1]))
                elif command == ("commit", "-m", "Small incremental push"):
                    raise StopBeforeIncrement
                return b"fixture-oid"

            random_bytes = os.urandom
            with patch.dict(os.environ, {"CANOPY_GIT_TOKEN": "local-test-token"}), \
                 patch.object(smoke, "start", side_effect=start), \
                 patch.object(smoke, "create_repository", return_value=("http://fixture/repo.git", {})), \
                 patch.object(smoke, "git", side_effect=git), \
                 patch.object(smoke, "cached_objects", return_value={str(i): [1, 1] for i in range(260)}), \
                 patch.object(smoke.os, "urandom", side_effect=lambda size:
                              b"fixture-body" if size == 2 * 1024**2 else random_bytes(size)):
                with self.assertRaises(StopBeforeIncrement):
                    smoke.qualify(args)

            probe = "refs/tags/canopy-cache-cursor-probe"
            self.assertIn(("-c", smoke.AUTH, "push", "http://fixture/repo.git", f"HEAD:{probe}"), calls)
            self.assertIn(("-c", smoke.AUTH, "push", "http://fixture/repo.git", f":{probe}"), calls)


if __name__ == "__main__":
    unittest.main()
