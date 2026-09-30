"""Wait bound for smoke tests that take over after an unclean node exit.

Keep this above the signed node lease in src/server.rs. Ownership must expire
before a different process can safely fence and restore its Cells.
"""

NODE_LEASE_WAIT_SECONDS = 32
