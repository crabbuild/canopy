#!/usr/bin/env python3
"""Qualify the bounded container against a caller-provided disposable S3 prefix."""
import argparse
import json
from native_limits import fixture_native_limits
import os
from pathlib import Path
import subprocess
import tempfile
import time
import urllib.error
import urllib.request
import uuid

from check_container import verify
from smoke_s3_process import create_repository, git, port, clone_and_verify

AUTH = "http.extraHeader=Authorization: Bearer local-test-token"
REPOSITORY = Path(__file__).resolve().parent.parent


def command(*args, **options):
    result = subprocess.run(args, capture_output=True, text=True, **options)
    if result.returncode:
        # Commands never contain provider credentials. Do not print inspect or
        # resolved Compose configuration, both of which contain the environment.
        raise RuntimeError(f"{args[0]} exited {result.returncode}: {result.stderr[-4000:]}")
    return result.stdout.strip()


def ready(url, compose):
    deadline = time.monotonic() + 180
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(url + "/readyz", timeout=2) as response:
                if response.status == 200:
                    return
        except (OSError, urllib.error.HTTPError):
            pass
        time.sleep(0.5)
    logs = command(*compose, "logs", "--no-color", "--tail", "30", "canopy")
    raise RuntimeError(f"Container did not become ready: {logs}")


def qualify(args):
    workspace = Path(os.environ["CARGO_TARGET_DIR"]).resolve()
    if not workspace.is_dir():
        raise ValueError("CARGO_TARGET_DIR must be an existing checkout-specific workspace directory")
    # Docker Desktop/Colima may not share the external workspace volume. Only
    # disposable configuration is placed in the checkout's shared parent;
    # client repositories and all large generated data stay on Workspace.
    with tempfile.TemporaryDirectory(prefix=".proof-", dir=REPOSITORY / "deploy") as config_temp, \
            tempfile.TemporaryDirectory(prefix="container-proof-", dir=workspace) as data_temp:
        directory, data = Path(config_temp), Path(data_temp)
        (directory / ".env").touch(mode=0o600)
        project = "canopy-containment-" + uuid.uuid4().hex[:12]
        base = ["docker", "compose", "--project-directory", str(directory), "-p", project]
        profile = json.loads(command(*base, "-f", str(REPOSITORY / "deploy/compose.yaml"), "config", "--format", "json"))
        service = profile["services"]["canopy"]
        service["image"] = args.image
        if args.network:
            profile["networks"] = {"default": {"external": True, "name": args.network}}
        published_port = port()
        service["ports"][0]["published"] = str(published_port)
        url = f"http://127.0.0.1:{published_port}"
        environment = {key: value for key, value in os.environ.items() if key.startswith("AWS_")}
        environment.update(CANOPY_GIT_TOKEN="local-test-token",
                           CANOPY_NODE_SIGNING_KEY_HEX=os.environ["CANOPY_NODE_SIGNING_KEY_HEX"], RUST_LOG="info")
        service.setdefault("environment", {}).update(environment)
        config = {
            "storage_url": args.storage_url.rstrip("/") + "/" + uuid.uuid4().hex,
            "tenant_id": str(uuid.uuid4()), "application_id": str(uuid.uuid4()), "node_id": str(uuid.uuid4()),
            "fleet_digest": "11" * 32, "image_digest": "22" * 32, "owner": "canopy",
            "listen": "0.0.0.0:8080", "public_url": url, "peer_endpoint": "https://container.example.invalid",
            "data_dir": "/var/lib/canopy", "native_limits": fixture_native_limits(), "local_disk_limit_bytes": 1536 * 1024**2,
            "max_active_repositories": 3,
        }
        config_file = directory / "config.json"
        profile_file = directory / "profile.json"
        compose = [*base, "-f", str(profile_file)]

        def write():
            config_file.write_text(json.dumps(config))
            # Compose interprets dollars even in JSON strings. Preserve provider
            # values literally and restrict the materialized file to its owner.
            profile_file.touch(mode=0o600, exist_ok=True)
            profile_file.write_text(json.dumps(profile).replace("$", "$$"))

        def start():
            write()
            command(*compose, "up", "-d", "--force-recreate", "canopy")
            ready(url, compose)
            container = command(*compose, "ps", "-q", "canopy")
            print("BOUNDARY: " + json.dumps(verify(container), sort_keys=True), flush=True)
            return container

        try:
            container = start()
            clone_url, _ = create_repository(url, "bounded")
            local = data / "source"
            git("init", "-b", "main", str(local))
            git("config", "user.name", "Canopy Test", cwd=local)
            git("config", "user.email", "canopy@example.invalid", cwd=local)
            git("lfs", "install", "--local", cwd=local)
            git("lfs", "track", "*.lfs", cwd=local)
            readme, lfs = b"Bounded repository\n", os.urandom(4 * 1024**2)
            (local / "README.md").write_bytes(readme)
            (local / "asset.lfs").write_bytes(lfs)
            with (local / "blob.bin").open("wb") as output:
                for _ in range(64):
                    output.write(os.urandom(1024**2))
            git("add", ".", cwd=local)
            git("commit", "-m", "Durable Git and LFS", cwd=local)
            git("-c", AUTH, "push", clone_url, "HEAD:refs/heads/main", cwd=local)
            published = git("rev-parse", "HEAD", cwd=local)
            for protocol in (0, 2):
                clone = data / f"healthy-v{protocol}"
                git("-c", f"protocol.version={protocol}", "-c", AUTH, "clone", clone_url, str(clone))
                git("lfs", "install", "--local", cwd=clone)
                git("-c", AUTH, "lfs", "pull", cwd=clone)
                assert git("rev-parse", "HEAD", cwd=clone) == published
                assert (clone / "blob.bin").read_bytes() == (local / "blob.bin").read_bytes()
                assert (clone / "asset.lfs").read_bytes() == lfs
                git("fsck", "--strict", cwd=clone)
            print("PASS: stock Git v0/v2 and Git LFS under the deployment resource profile", flush=True)
            # Recreate with stricter limits to exercise ENOSPC without allocating
            # gigabytes of pressure on a developer's machine. All other policy is shared.
            command(*compose, "stop", "canopy")
            service["tmpfs"] = ["/var/lib/canopy:rw,nosuid,nodev,size=536870912,uid=10001,gid=10001,mode=0700"]
            service["mem_limit"] = service["memswap_limit"] = 2 * 1024**3
            config["local_disk_limit_bytes"] = 384 * 1024**2
            container = start()
            clone_and_verify(clone_url, data / "before-pressure", published, readme, lfs)
            # A small delta reconstructs another 64 MiB incompressible object.
            # The CGI input fits before unpack fails, preserving Git's ENOSPC
            # report instead of a simultaneous input-pipe failure.
            pressure = local / "blob.bin"
            with pressure.open("r+b") as output:
                output.write(b"Changed for native disk pressure\n")
            git("add", "blob.bin", cwd=local)
            git("commit", "-m", "Scratch pressure", cwd=local)
            block_size, free = command("docker", "exec", container, "stat", "-f", "-c", "%S %a", "/var/lib/canopy").split()
            fill = int(block_size) * int(free) // 1024**2 - 48
            if fill <= 0:
                raise RuntimeError("fixture lacks scratch headroom before pressure")
            command("docker", "exec", container, "dd", "if=/dev/zero", "of=/var/lib/canopy/qualification-pressure", "bs=1048576", f"count={fill}", "status=none")
            rejected = subprocess.run(["git", "-c", "credential.helper=", "-c", AUTH, "-c", "http.postBuffer=1048576", "push", clone_url, "HEAD:refs/heads/main"], cwd=local, capture_output=True, timeout=120)
            if rejected.returncode == 0:
                raise AssertionError("pressure push unexpectedly published")
            log_result = subprocess.run(["docker", "logs", container], capture_output=True, text=True, check=True)
            logs = log_result.stdout + log_result.stderr
            if b"No space left on device" not in rejected.stderr and "No space left on device" not in logs:
                raise AssertionError("push failed without evidence of filesystem ENOSPC: " + rejected.stderr.decode(errors="replace") + "\n" + logs[-4000:])
            command("docker", "exec", container, "rm", "/var/lib/canopy/qualification-pressure")
            assert git("-c", AUTH, "ls-remote", clone_url, "refs/heads/main") == published + b"\trefs/heads/main"
            print("PASS: native scratch ENOSPC rejects push and preserves the published ref", flush=True)
            command("docker", "kill", "--signal", "KILL", container)
            # Restore the normal profile and recreate with a wholly fresh tmpfs.
            service["tmpfs"] = ["/var/lib/canopy:rw,nosuid,nodev,size=2147483648,uid=10001,gid=10001,mode=0700"]
            service["mem_limit"] = service["memswap_limit"] = 4 * 1024**3
            config["local_disk_limit_bytes"] = 1536 * 1024**2
            start()
            clone_and_verify(clone_url, data / "after-kill", published, readme, lfs)
            git("-c", AUTH, "push", clone_url, "HEAD:refs/heads/main", cwd=local)
            final = data / "after-retry"
            git("-c", AUTH, "clone", clone_url, str(final))
            assert (final / "blob.bin").read_bytes() == pressure.read_bytes()
            git("fsck", "--strict", cwd=final)
            print("PASS: SIGKILL, lost local state, Git/LFS restore and rejected-push retry", flush=True)
        finally:
            if profile_file.exists():
                command(*compose, "down", "--timeout", "30")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", required=True)
    parser.add_argument("--network", help="existing Docker network providing access to the test object store")
    parser.add_argument("--storage-url", required=True, help="disposable S3 prefix; a unique child is created")
    qualify(parser.parse_args())


if __name__ == "__main__":
    main()
