"""Explicit native claims for small evaluation fixtures; no capacity qualification."""


def fixture_native_limits():
    # Two GiB of native claims leaves space for the 1.5-GiB tmpfs and server
    # within the four-GiB container profile. CPU units are admission weights,
    # not a kernel CPU quota; cgroups still enforce the two-CPU fixture quota.
    return {
        "total": {"processes": 16, "cpu_units": 16,
                  "memory_bytes": 2 * 1024**3, "descriptors": 1024},
        "maintenance_reserved": {"processes": 2, "cpu_units": 5,
                                 "memory_bytes": 768 * 1024**2, "descriptors": 128},
        "read": {"processes": 1, "cpu_units": 1,
                 "memory_bytes": 128 * 1024**2, "descriptors": 32},
        "pack": {"processes": 1, "cpu_units": 4,
                 "memory_bytes": 512 * 1024**2, "descriptors": 64},
    }
