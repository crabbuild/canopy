"""Guard against partial Cellule upgrades and alternate dependency sources."""

import copy
from pathlib import Path
import tempfile
import unittest

from check_cellule import check, workspace_manifests


class CelluleReleaseTests(unittest.TestCase):
    def setUp(self):
        self.manifest = {"dependencies": {
            "cellule-app": {"version": "=0.2.0"},
            "cellule-runtime": {"version": "=0.2.0"},
        }}
        self.lock = {"package": [
            {"name": name, "version": "0.2.0", "checksum": "digest",
             "source": "registry+https://github.com/rust-lang/crates.io-index"}
            for name in ("cellule-app", "cellule-runtime", "cellule-types")
        ]}

    def test_accepts_aligned_direct_and_transitive_crates(self):
        self.assertEqual(check(self.manifest, self.lock), ("0.2.0", 3))

    def test_rejects_ranges_and_alternate_sources(self):
        for spec in ({"version": "0.2.0"}, {"version": "^0.2.0"},
                     {"version": "=0.2.0", "git": "https://example.com/cellule"},
                     {"version": "=0.2.0", "path": "../cellule"},
                     {"version": "=0.2.0", "registry": "private"}):
            with self.subTest(spec=spec):
                self.manifest["dependencies"]["cellule-app"] = spec
                with self.assertRaisesRegex(ValueError, "exact stable crates.io"):
                    check(self.manifest, self.lock)

    def test_rejects_partial_direct_upgrade(self):
        self.manifest["dependencies"]["cellule-app"]["version"] = "=0.3.0"
        with self.assertRaisesRegex(ValueError, "same release"):
            check(self.manifest, self.lock)

    def test_workspace_members_cannot_hide_another_cellule_release(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "Cargo.toml").write_text('[workspace]\nmembers = ["crates/*"]\n')
            for name, version in (("server", "0.2.0"), ("storage", "0.1.0")):
                member = root / "crates" / name
                member.mkdir(parents=True)
                (member / "Cargo.toml").write_text(
                    f'[dependencies]\ncellule-runtime = "={version}"\n')
            with self.assertRaisesRegex(ValueError, "same release"):
                check(workspace_manifests(root), self.lock)

    def test_rejects_hidden_target_alias_on_another_release(self):
        self.manifest["target"] = {"cfg(unix)": {"dependencies": {
            "runtime": {"package": "cellule-runtime", "version": "=0.1.0"},
        }}}
        with self.assertRaisesRegex(ValueError, "same release"):
            check(self.manifest, self.lock)

    def test_rejects_old_or_replaced_transitive_crate(self):
        for field, value in (("version", "0.1.0"), ("source", "git+https://example.com/cellule"),
                             ("source", None), ("checksum", None)):
            with self.subTest(field=field, value=value):
                lock = copy.deepcopy(self.lock)
                lock["package"][-1][field] = value
                with self.assertRaisesRegex(ValueError, "cellule-types must resolve once"):
                    check(self.manifest, lock)

    def test_rejects_duplicate_locked_crate(self):
        self.lock["package"].append(self.lock["package"][-1].copy())
        with self.assertRaisesRegex(ValueError, "resolve once"):
            check(self.manifest, self.lock)

    def test_rejects_missing_locked_crate(self):
        self.lock["package"].pop(0)
        with self.assertRaisesRegex(ValueError, "missing from Cargo.lock"):
            check(self.manifest, self.lock)


if __name__ == "__main__":
    unittest.main()
