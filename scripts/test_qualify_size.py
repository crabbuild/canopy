"""Preflight checks for the large, non-sparse RustFS qualification."""

import importlib.util
from pathlib import Path
import unittest
from unittest.mock import patch


SPEC = importlib.util.spec_from_file_location(
    "qualify_size", Path(__file__).with_name("qualify_size.py")
)
qualify_size = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(qualify_size)


class SizeCapacityTests(unittest.TestCase):
    def check(self, scratch_gib, provider_gib):
        with patch.object(qualify_size.shutil, "disk_usage") as usage, patch.object(
            qualify_size, "run", return_value=f"Filesystem 1B-blocks Used Available\n/data 1 1 {provider_gib * 1024**3}"
        ):
            usage.return_value.free = scratch_gib * 1024**3
            qualify_size.require_size_capacity(Path("/scratch"), "provider")

    def test_accepts_both_volumes_at_threshold(self):
        self.check(40, 40)

    def test_rejects_insufficient_scratch(self):
        with self.assertRaisesRegex(RuntimeError, "scratch=39.0 GiB"):
            self.check(39, 50)

    def test_rejects_insufficient_provider(self):
        with self.assertRaisesRegex(RuntimeError, "provider=39.0 GiB"):
            self.check(50, 39)

    def test_rejects_unparseable_provider_capacity(self):
        with patch.object(qualify_size.shutil, "disk_usage") as usage, patch.object(
            qualify_size, "run", return_value="not a df result"
        ):
            usage.return_value.free = 50 * 1024**3
            with self.assertRaisesRegex(RuntimeError, "could not read free space"):
                qualify_size.require_size_capacity(Path("/scratch"), "provider")


if __name__ == "__main__":
    unittest.main()
