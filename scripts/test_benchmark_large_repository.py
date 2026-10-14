"""Qualification evidence must identify code in every workspace crate."""
from pathlib import Path
import tempfile
import unittest

from benchmark_large_repository import source_tree_digest


class SourceIdentityTests(unittest.TestCase):
    def test_workspace_code_and_manifest_changes_invalidate_identity(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "Cargo.toml").write_text("[workspace]")
            (root / "Cargo.lock").write_text("lock")
            for name in ("canopy-server", "canopy-git-format", "canopy-object-storage"):
                crate = root / "crates" / name
                (crate / "src").mkdir(parents=True)
                for path in (crate / "Cargo.toml", crate / "src" / "lib.rs",
                             crate / "src" / "schema.sql"):
                    path.write_text("before")
                    before = source_tree_digest(root)
                    path.write_text("after")
                    self.assertNotEqual(before, source_tree_digest(root), str(path))


if __name__ == "__main__":
    unittest.main()
