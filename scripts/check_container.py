#!/usr/bin/env python3
"""Verify actual Linux cgroup and mount limits for a running Canopy container."""
import argparse
import json
import subprocess


def output(*arguments):
    result = subprocess.run(arguments, capture_output=True, text=True, check=True)
    return result.stdout.strip()


def verify(container):
    state = json.loads(output("docker", "inspect", container))[0]
    host = state["HostConfig"]
    if not state["State"]["Running"]:
        raise ValueError("Canopy container is not running")
    if state["Config"]["User"] != "10001:10001" or not host["ReadonlyRootfs"]:
        raise ValueError("expected unprivileged Canopy user and read-only root filesystem")
    if host["Privileged"] or host.get("CapAdd") or "ALL" not in (host.get("CapDrop") or []):
        raise ValueError("container capabilities differ from the bounded profile")
    if not any(option.split(":")[0] == "no-new-privileges" for option in (host.get("SecurityOpt") or [])):
        raise ValueError("no-new-privileges is required")
    status = output("docker", "exec", container, "cat", "/proc/1/status")
    if "NoNewPrivs:\t1" not in status.splitlines():
        raise ValueError("kernel no-new-privileges flag is not enabled")
    if host["LogConfig"]["Type"] != "local":
        raise ValueError("bounded local container logging is required")
    if host["LogConfig"]["Config"] != {"max-size": "10m", "max-file": "3"}:
        raise ValueError("container log rotation differs from the bounded profile")
    # Inspect the kernel's effective values. HostConfig alone cannot prove the
    # engine/kernel honored limits, and cgroup v1 needs separate qualification.
    limits = output("docker", "exec", container, "sh", "-ec",
                    "cat /sys/fs/cgroup/memory.max /sys/fs/cgroup/memory.swap.max "
                    "/sys/fs/cgroup/cpu.max /sys/fs/cgroup/pids.max").splitlines()
    memory, swap, cpu, tasks = limits
    quota, period = cpu.split()
    if not memory.isdecimal() or not 0 < int(memory) <= 4 * 1024**3 or swap != "0":
        raise ValueError("hard memory limit of at most 4 GiB and zero swap are required")
    if not quota.isdecimal() or int(quota) <= 0 or int(period) <= 0 or int(quota) > 2 * int(period):
        raise ValueError("CPU bandwidth must be limited to at most two CPUs")
    if not tasks.isdecimal() or not 0 < int(tasks) <= 256:
        raise ValueError("process/thread limit of at most 256 is required")
    if not host["Init"] or not 0 < host["ShmSize"] <= 16 * 1024**2:
        raise ValueError("init and shared memory bounded to 16 MiB are required")
    mounts = state["Mounts"]
    if any(mount["RW"] for mount in mounts):
        raise ValueError("additional writable bind/volume mounts bypass scratch containment")
    if set(host["Tmpfs"]) != {"/var/lib/canopy"}:
        raise ValueError("unexpected writable tmpfs mount")
    filesystem, block_size, blocks = output("docker", "exec", container, "stat", "-f", "-c",
                                           "%T %S %b", "/var/lib/canopy").split()
    scratch = int(block_size) * int(blocks)
    if filesystem != "tmpfs" or not 0 < scratch <= 2 * 1024**3:
        raise ValueError("Canopy state must use tmpfs capped at 2 GiB")
    config = json.loads(output("docker", "exec", container, "cat", "/etc/canopy/config.json"))
    if config["data_dir"] != "/var/lib/canopy" or config["listen"] != "0.0.0.0:8080":
        raise ValueError("container data directory or listener differs from the profile")
    if not 0 < config["local_disk_limit_bytes"] <= scratch:
        raise ValueError("application disk admission exceeds the filesystem capacity")
    descriptor_line = next(line for line in output("docker", "exec", container, "cat", "/proc/1/limits").splitlines()
                           if line.startswith("Max open files"))
    soft, hard = descriptor_line.split()[3:5]
    if soft != "16384" or hard != "16384":
        raise ValueError("soft and hard open-file limits must both be 16384")
    # The pinned runtime reserves eight descriptors per active Cell. Directory
    # also consumes one slot; keep 1024 descriptors for sockets, Git and I/O.
    required_descriptors = 8 * (config["max_active_repositories"] + 1) + 1024
    if required_descriptors > int(soft):
        raise ValueError("active Cell admission leaves insufficient descriptor headroom")
    return {"memory_bytes": int(memory), "swap_bytes": 0,
            "cpu_quota": int(quota), "cpu_period": int(period),
            "processes_and_threads": int(tasks), "scratch_bytes": scratch,
            "file_descriptors_per_process": int(soft)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("container", help="running container ID or name")
    arguments = parser.parse_args()
    try:
        print(json.dumps(verify(arguments.container), sort_keys=True))
    except (ValueError, KeyError, subprocess.CalledProcessError) as error:
        # Never dump docker inspect: it includes provider credentials.
        raise SystemExit(f"Container containment check failed: {error}") from error


if __name__ == "__main__":
    main()
