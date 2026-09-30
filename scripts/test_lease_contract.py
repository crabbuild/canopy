"""Catch a server lease change that would make takeover smoke tests race it."""

from pathlib import Path
import re
import unittest

from lease_contract import NODE_LEASE_WAIT_SECONDS


class LeaseContract(unittest.TestCase):
    def test_unclean_takeover_wait_exceeds_server_lease(self):
        source = (Path(__file__).resolve().parent.parent / "src" / "server.rs").read_text()
        match = re.search(r"pub\(crate\) const LEASE_MS: i64 = ([0-9_]+);", source)
        self.assertIsNotNone(match)
        lease_ms = int(match.group(1).replace("_", ""))
        self.assertGreater(NODE_LEASE_WAIT_SECONDS * 1000, lease_ms + 1000)
