#!/usr/bin/env python3
"""Measure repository density inside the checked-in bounded Linux profile.

The caller owns the disposable S3 prefix. Reports survive failures; only this
run's Compose project and private configuration are removed. This tmpfs profile
does not qualify NVMe capacity or a remote production object store.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import random
import subprocess
import sys
import tempfile
import threading
import time
import uuid

from benchmark_repositories import Client, save
from check_container import verify
from lease_contract import NODE_LEASE_WAIT_SECONDS
from smoke_container import REPOSITORY, command, port, ready


def resources(container):
    values = command("docker", "exec", container, "sh", "-ec",
                     "cat /sys/fs/cgroup/memory.current /sys/fs/cgroup/memory.peak "
                     "/sys/fs/cgroup/pids.current; "
                     "stat -f -c '%S %b %f' /var/lib/canopy; "
                     "find /proc/[0-9]*/fd -mindepth 1 -maxdepth 1 -printf . 2>/dev/null | wc -c; "
                     "cat /sys/fs/cgroup/cpu.stat /sys/fs/cgroup/memory.events").splitlines()
    block, total, free = map(int, values[3].split())
    return {"memory_bytes": int(values[0]), "peak_memory_bytes": int(values[1]),
            "processes_and_threads": int(values[2]), "scratch_used_bytes": block * (total - free),
            "sampled_descriptors": int(values[4]),
            **{key: int(value) for key, value in (line.split() for line in values[5:])}}


def qualify(args):
    workspace = Path(os.environ["CARGO_TARGET_DIR"]).resolve()
    report = args.output.resolve()
    if not workspace.is_dir() or not report.is_relative_to(workspace):
        raise ValueError("output must be inside the existing checkout-specific CARGO_TARGET_DIR")
    if not 1 <= args.active_repositories <= min(args.repositories, 9999):
        raise ValueError("active count must be between 1 and min(repository count, 9999)")
    report.mkdir(parents=True, exist_ok=False)
    project = "canopy-density-" + uuid.uuid4().hex[:12]
    outcome = {"read_exit_codes": {}, "recovery_passed": None, "shutdown_passed": False,
               "cleanup_passed": False, "error": None}
    phase, container = "setup", None
    samples, sample_errors = [], []
    stop = threading.Event()

    def mark(value):
        nonlocal phase
        phase = value
        print("PHASE: " + value, flush=True)

    def sample():
        while not stop.wait(5):
            try:
                samples.append({"phase": phase, "monotonic_seconds": time.monotonic(),
                                **resources(container)})
            except (RuntimeError, ValueError) as error:
                sample_errors.append({"phase": phase, "error": str(error)})

    with tempfile.TemporaryDirectory(prefix=".proof-", dir=REPOSITORY / "deploy") as temporary:
        directory = Path(temporary)
        (directory / ".env").touch(mode=0o600)
        base = ["docker", "compose", "--project-directory", str(directory), "-p", project]
        profile = json.loads(command(*base, "-f", str(REPOSITORY / "deploy/compose.yaml"),
                                     "config", "--format", "json"))
        service = profile["services"]["canopy"]
        # Unexpected process death must remain visible; qualification never
        # restarts silently and changes the measured resident working set.
        service.update(image=args.image, restart="no")
        if args.network:
            profile["networks"] = {"default": {"external": True, "name": args.network}}
        published_port = port()
        service["ports"][0]["published"] = str(published_port)
        url = f"http://127.0.0.1:{published_port}"
        environment = {key: value for key, value in os.environ.items() if key.startswith("AWS_")}
        environment.update(CANOPY_GIT_TOKEN="local-test-token", RUST_LOG="info",
                           CANOPY_NODE_SIGNING_KEY_HEX=os.environ["CANOPY_NODE_SIGNING_KEY_HEX"])
        service.setdefault("environment", {}).update(environment)
        config = {"storage_url": args.storage_url.rstrip("/") + "/" + uuid.uuid4().hex,
                  "tenant_id": str(uuid.uuid4()), "application_id": str(uuid.uuid4()),
                  "node_id": str(uuid.uuid4()), "fleet_digest": "11" * 32, "image_digest": "22" * 32,
                  "owner": "canopy", "listen": "0.0.0.0:8080", "public_url": url,
                  "peer_endpoint": "https://density.example.invalid", "data_dir": "/var/lib/canopy",
                  "local_disk_limit_bytes": 1536 * 1024**2,
                  "max_active_repositories": args.active_repositories}
        (directory / "config.json").write_text(json.dumps(config))
        profile_file = directory / "profile.json"
        profile_file.touch(mode=0o600)
        profile_file.write_text(json.dumps(profile).replace("$", "$$"))
        compose = [*base, "-f", str(profile_file)]
        manifest = report / "corpus.json"
        common = [sys.executable, "-B", str(REPOSITORY / "scripts/benchmark_repositories.py"),
                  "--base-url", url, "--manifest", str(manifest), "--timeout", "5"]
        driver_env = dict(os.environ, CANOPY_GIT_TOKEN="local-test-token")
        sampler = None

        def driver(name, arguments):
            with (report / (name + ".log")).open("w") as log:
                return subprocess.run([*common, *arguments], env=driver_env,
                                      stdout=log, stderr=subprocess.STDOUT).returncode

        def logs(name):
            result = subprocess.run(["docker", "logs", container], capture_output=True, check=True)
            (report / (name + ".log")).write_bytes(result.stdout + result.stderr)

        try:
            command(*compose, "up", "-d", "canopy")
            container = command(*compose, "ps", "-q", "canopy")
            ready(url, compose)
            limits = verify(container)
            image = json.loads(command("docker", "image", "inspect", "--format", "{{json .}}", args.image))
            environment_report = {"image_id": image["Id"],
                                  "image_source_revision": (image["Config"].get("Labels") or {}).get("org.opencontainers.image.revision"),
                                  "binary_sha256": command("docker", "exec", container, "sha256sum", "/usr/local/bin/canopy").split()[0],
                                  "kernel": command("docker", "exec", container, "uname", "-srmo"),
                                  "git_version": command("docker", "exec", container, "git", "--version"),
                                  "driver_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                                  "limits": limits, "repositories": args.repositories,
                                  "active_repositories": args.active_repositories,
                                  "storage_url": config["storage_url"], "provider_description": args.provider_description,
                                  "topology": "host client, container server; provider placement declared separately",
                                  "scratch_filesystem": "tmpfs", "recovery_requested": args.recover}
            save(report / "environment.json", environment_report)
            samples.append({"phase": phase, **resources(container)})
            sampler = threading.Thread(target=sample, daemon=True)
            sampler.start()
            mark("seed")
            if driver("seed", ["--timeout", "30", "seed", "--repositories", str(args.repositories),
                               "--populated", "3", "--work-dir", str(report / "seed")]):
                raise RuntimeError("seed failed; see seed.log and incomplete corpus")
            mark("idle")
            time.sleep(15)
            entries = json.loads(manifest.read_text())["repositories"]
            client = Client(url, "local-test-token", 30)
            try:
                for entry in random.Random(20260926).sample(entries, min(3, len(entries))):
                    status, _ = client.request("/api/repositories/" + entry["name"])
                    if status != 200:
                        raise RuntimeError("prewarm request failed")
            finally:
                client.close()
            for name, active, operation, rate, duration in (
                    ("metadata", min(3, args.repositories), "metadata", 20, 15),
                    ("refs", min(3, args.repositories), "refs", 10, 10),
                    ("uniform", args.repositories, "metadata", args.rate, args.duration)):
                mark(name)
                outcome["read_exit_codes"][name] = driver(name, [
                    "run", "--active-repositories", str(active), "--operation", operation,
                    "--rate", str(rate), "--duration", str(duration), "--concurrency", "32",
                    "--output", str(report / (name + ".json"))])
            stop.set()
            sampler.join()
            samples.append({"phase": "after-reads", **resources(container)})
            if args.recover:
                mark("kill-and-restore")
                command("docker", "kill", "--signal", "KILL", container)
                logs("before-kill")
                time.sleep(NODE_LEASE_WAIT_SECONDS)
                command(*compose, "up", "-d", "--force-recreate", "canopy")
                container = command(*compose, "ps", "-q", "canopy")
                ready(url, compose)
                verify(container)
                outcome["recovery_passed"] = driver("recovery", ["--timeout", "30", "verify",
                                                               "--work-dir", str(report / "restored")]) == 0
                samples.append({"phase": "after-recovery", **resources(container)})
            mark("graceful-shutdown")
            started = time.monotonic()
            command(*compose, "stop", "--timeout", "120", "canopy")
            outcome["shutdown_seconds"] = time.monotonic() - started
            state = json.loads(command("docker", "inspect", "--format", "{{json .State}}", container))
            outcome["shutdown_passed"] = state["ExitCode"] == 0 and not state["OOMKilled"]
            outcome["oom_killed"] = state["OOMKilled"]
        except Exception as error:
            outcome["error"] = str(error)
        finally:
            stop.set()
            if sampler is not None:
                sampler.join()
            try:
                if container:
                    logs("server")
            finally:
                try:
                    command(*compose, "down", "--timeout", "30")
                    outcome["cleanup_passed"] = True
                finally:
                    save(report / "resources.json", {"samples": samples, "errors": sample_errors})
                    outcome["last_phase"] = phase
                    save(report / "outcome.json", outcome)
    passed = (outcome["error"] is None and outcome["shutdown_passed"] and outcome["cleanup_passed"]
              and outcome["recovery_passed"] is not False and len(outcome["read_exit_codes"]) == 3
              and not any(outcome["read_exit_codes"].values()) and not sample_errors
              and not any(item.get("oom", 0) or item.get("oom_kill", 0) for item in samples))
    print("Artifacts: " + str(report), flush=True)
    return 0 if passed else 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", required=True)
    parser.add_argument("--network")
    parser.add_argument("--storage-url", required=True)
    parser.add_argument("--provider-description", required=True, help="provider version and placement")
    parser.add_argument("--output", type=Path, required=True, help="new directory under CARGO_TARGET_DIR")
    parser.add_argument("--repositories", type=int, default=1000)
    parser.add_argument("--active-repositories", type=int, default=1000)
    parser.add_argument("--rate", type=int, default=10)
    parser.add_argument("--duration", type=int, default=120)
    parser.add_argument("--recover", action="store_true", help="also SIGKILL and verify on fresh tmpfs")
    args = parser.parse_args()
    if min(args.repositories, args.rate, args.duration) <= 0:
        parser.error("repository count, rate and duration must be positive")
    raise SystemExit(qualify(args))


if __name__ == "__main__":
    main()
