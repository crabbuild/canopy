#!/usr/bin/env python3
"""Read kernel CPU counters for the three bound Canopy processes.

Self CPU and reaped-child CPU are separate. Child counters are charged at reap,
not continuously while children run: this does NOT establish total live process
tree CPU, Git-only attribution, or operation-window CPU without closed child
boundaries. No process attachment, signals, sampling of payloads, or retries.
"""

import argparse
import ctypes
from datetime import datetime, timezone
import json
import math
import os
from pathlib import Path
import sys
import time

import benchmark_repositories as benchmark
import benchmark_three_node_campaign as campaign


COUNTERS = ("user", "system", "reaped_child_user", "reaped_child_system")


class DarwinUsage(ctypes.Structure):
    # rusage_info_v2 from the platform SDK's sys/resource.h. Time fields use
    # Mach absolute units; calibrate through mach_timebase_info, not /1e9 alone.
    _fields_ = [("uuid", ctypes.c_ubyte * 16)] + [(name, ctypes.c_uint64) for name in (
        "user_time", "system_time", "pkg_idle_wkups", "interrupt_wkups", "pageins",
        "wired_size", "resident_size", "phys_footprint", "proc_start_abstime",
        "proc_exit_abstime", "child_user_time", "child_system_time",
        "child_pkg_idle_wkups", "child_interrupt_wkups", "child_pageins",
        "child_elapsed_abstime", "diskio_bytesread", "diskio_byteswritten")]


class Timebase(ctypes.Structure):
    _fields_ = [("numer", ctypes.c_uint32), ("denom", ctypes.c_uint32)]


def require(condition, message):
    if not condition:
        raise ValueError(message)


def parse_linux_stat(text, pid, ticks_per_second, page_size):
    # comm may contain whitespace and ')'; fields after its last ')' are fixed.
    head, marker, tail = text.rpartition(")")
    require(marker and head.split("(", 1)[0].strip() == str(pid), "invalid proc stat PID")
    fields = tail.split()
    require(len(fields) >= 22 and ticks_per_second > 0 and page_size > 0, "invalid proc stat layout")
    require(fields[0] not in ("Z", "X", "x"), "process is not live")
    values = [int(fields[index]) for index in (11, 12, 13, 14, 19, 21)]
    require(all(value >= 0 for value in values) and values[4] > 0, "invalid proc stat counters")
    return {"pid": pid, "identity": {"start_ticks": values[4]},
            "backend": "linux-proc-stat", "seconds_per_unit": 1 / ticks_per_second,
            "scale": {"clock_ticks_per_second": ticks_per_second},
            "cpu_units": dict(zip(COUNTERS, values[:4])), "rss_bytes": values[5] * page_size}


class Reader:
    def __init__(self):
        self.platform = sys.platform
        if self.platform == "darwin":
            self.library = ctypes.CDLL("/usr/lib/libproc.dylib", use_errno=True)
            self.library.proc_pid_rusage.argtypes = [ctypes.c_int, ctypes.c_int, ctypes.c_void_p]
            self.library.proc_pid_rusage.restype = ctypes.c_int
            system = ctypes.CDLL("/usr/lib/libSystem.B.dylib")
            system.mach_timebase_info.argtypes = [ctypes.POINTER(Timebase)]
            system.mach_timebase_info.restype = ctypes.c_int
            base = Timebase()
            require(system.mach_timebase_info(ctypes.byref(base)) == 0
                    and base.numer > 0 and base.denom > 0, "invalid Mach timebase")
            self.scale = {"mach_numer": base.numer, "mach_denom": base.denom}
            self.seconds_per_unit = base.numer / base.denom / 1_000_000_000
        elif self.platform == "linux":
            self.ticks = os.sysconf("SC_CLK_TCK")
            self.page_size = os.sysconf("SC_PAGE_SIZE")
        else:
            raise RuntimeError("kernel CPU reader requires macOS or Linux")

    def snapshot(self, pid):
        require(isinstance(pid, int) and not isinstance(pid, bool) and pid > 0, "invalid PID")
        started = time.monotonic()
        if self.platform == "darwin":
            usage = DarwinUsage()
            if self.library.proc_pid_rusage(pid, 2, ctypes.byref(usage)) != 0:
                raise OSError(ctypes.get_errno(), "kernel CPU snapshot failed")
            require(usage.proc_start_abstime > 0 and usage.proc_exit_abstime == 0,
                    "process is not live")
            result = {"pid": pid, "identity": {"start_abstime": usage.proc_start_abstime,
                      "executable_uuid": bytes(usage.uuid).hex()},
                      "backend": "darwin-proc-pid-rusage-v2", "scale": self.scale,
                      "seconds_per_unit": self.seconds_per_unit,
                      "cpu_units": dict(zip(COUNTERS, (usage.user_time, usage.system_time,
                           usage.child_user_time, usage.child_system_time))),
                      "rss_bytes": usage.resident_size}
        else:
            result = parse_linux_stat(Path(f"/proc/{pid}/stat").read_text(), pid, self.ticks, self.page_size)
        finished = time.monotonic()
        return {**result, "read_started_monotonic": started, "read_finished_monotonic": finished,
                "captured_monotonic": (started + finished) / 2}


def delta(before, after):
    require(all(before[key] == after[key] for key in ("pid", "identity", "backend", "scale", "seconds_per_unit")),
            "process identity or CPU scale changed")
    seconds = after["captured_monotonic"] - before["captured_monotonic"]
    require(math.isfinite(seconds) and seconds > 0, "invalid CPU observation interval")
    units = {name: after["cpu_units"][name] - before["cpu_units"][name] for name in COUNTERS}
    require(all(value >= 0 for value in units.values()), "CPU counter decreased")
    cpu = {name: value * after["seconds_per_unit"] for name, value in units.items()}
    return {"pid": after["pid"], "interval_seconds": seconds, "cpu_seconds": cpu,
            "self_cpu_percent": 100 * (cpu["user"] + cpu["system"]) / seconds,
            "reaped_child_cpu_seconds": cpu["reaped_child_user"] + cpu["reaped_child_system"],
            "child_scope": "charged at reap; excludes live/unreaped children, not Git-only attribution"}


def observe(args):
    ready = campaign.read_json(args.fleet_dir / "ready.json")
    plan = {"node_active_limit": ready.get("max_active_repositories_per_node")}
    ready = campaign.validate_fleet(args.fleet_dir, plan)
    reader = Reader()
    args.output_dir.mkdir(parents=True, exist_ok=False)
    path = args.output_dir / "observation.json"
    samples_path = args.output_dir / "samples.jsonl"
    result = {"version": 1, "complete": False, "error": None, "samples": 0,
              "declared": {"samples": args.samples, "interval_seconds": args.interval},
              "driver_sha256": benchmark.file_sha256(Path(__file__)),
              "fleet_validator_sha256": benchmark.file_sha256(Path(campaign.__file__)),
              "fleet_ready_sha256": benchmark.file_sha256(args.fleet_dir / "ready.json"),
              "binary_sha256": ready["binary_sha256"], "node_pids": [node["pid"] for node in ready["nodes"]],
              "platform": sys.platform,
              "scope": "kernel self and reaped-child CPU only; no live-tree total, Git-only attribution, child-boundary closure, operation CPU, throughput or provider-cost proof"}
    benchmark.save(path, result)
    previous = None
    try:
        with samples_path.open("x") as output:
            for index in range(args.samples):
                current = [reader.snapshot(pid) for pid in result["node_pids"]]
                intervals = [delta(old, new) for old, new in zip(previous, current)] if previous else []
                sample = {"index": index, "utc": datetime.now(timezone.utc).isoformat(),
                          "processes": current, "intervals": intervals}
                output.write(json.dumps(sample) + "\n")
                output.flush()
                result["samples"] += 1
                previous = current
                if index + 1 < args.samples:
                    time.sleep(args.interval)
        campaign.validate_fleet(args.fleet_dir, plan)
        result["samples_sha256"] = benchmark.file_sha256(samples_path)
        result["complete"] = True
    except BaseException as error:
        result["error"] = type(error).__name__
        raise
    finally:
        if samples_path.exists():
            result["samples_sha256"] = benchmark.file_sha256(samples_path)
        benchmark.save(path, result)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fleet-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--samples", type=int, default=120)
    parser.add_argument("--interval", type=float, default=1)
    args = parser.parse_args()
    require(2 <= args.samples <= 86400 and math.isfinite(args.interval) and 0.1 <= args.interval <= 30,
            "invalid observation bounds")
    observe(args)


if __name__ == "__main__":
    main()
