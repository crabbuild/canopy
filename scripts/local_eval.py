#!/usr/bin/env python3
"""Start/stop an isolated, persistent local Canopy evaluation node and RustFS.

Credentials stay in a mode-0600 file outside the checkout. This development
profile binds only loopback; it does not establish Linux container containment.
"""
import argparse
import hashlib
import json
from native_limits import fixture_native_limits
import os
from pathlib import Path
import secrets
import signal
import socket
import subprocess
import time
import urllib.request
import uuid

PROVIDER_IMAGE = "ghcr.io/rustfs/rustfs@sha256:0c3c7030ffb93afde8d359fb1db957b85033ede05115518bd0dede51f4353f6a"


def run(*args, env=None):
    return subprocess.run(args, env=env, check=True, capture_output=True,
                          text=True).stdout.strip()


def save(path, value, private=False):
    temporary = path.with_suffix(path.suffix + ".tmp")
    fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    os.fchmod(fd, 0o600)
    with os.fdopen(fd, "w") as output:
        json.dump(value, output, indent=2)
        output.write("\n")
    temporary.replace(path)
    if not private:
        path.chmod(0o644)


def environment(state):
    # Never inherit unrelated provider credentials or endpoints.
    env = {k: v for k, v in os.environ.items() if not k.startswith("AWS_")}
    env.update(json.loads((state / "secrets.json").read_text()))
    env.update(AWS_DEFAULT_REGION="us-east-1", AWS_REGION="us-east-1",
               AWS_EC2_METADATA_DISABLED="true", AWS_CONFIG_FILE=os.devnull,
               AWS_SHARED_CREDENTIALS_FILE=os.devnull, AWS_ALLOW_HTTP="true",
               RUST_LOG="canopy=info,canopy_server=info,cellule_runtime=warn,cellule_ltx=warn")
    return env


def owned_pid(state, metadata):
    try:
        pid = int((state / "server.pid").read_text())
        command = run("ps", "-p", str(pid), "-o", "command=")
        if str(state / "config.json") in command and metadata["binary"] in command:
            return pid
    except (OSError, ValueError, subprocess.CalledProcessError):
        pass
    return None


def ready(url):
    try:
        with urllib.request.urlopen(url + "/readyz", timeout=2) as response:
            return response.status == 200
    except OSError:
        return False


def initialize(args):
    state = args.state_dir
    if (state / "deployment.json").exists():
        return json.loads((state / "deployment.json").read_text())
    if any(state.iterdir()):
        raise RuntimeError("state directory is not empty; use a new directory")
    if not args.binary:
        raise RuntimeError("first start requires --binary pointing to a release build")
    binary = args.binary.resolve()
    if not binary.is_file():
        raise RuntimeError("build canopy before starting the evaluation node")
    identifier = uuid.uuid4().hex[:12]
    metadata = {
        "binary": str(binary), "binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
        "container": "canopy-eval-" + identifier,
        "volume": "canopy-eval-" + identifier + "-data",
        "provider_image": args.provider_image,
        "bucket": "canopy-eval", "url": f"http://127.0.0.1:{args.port}",
    }
    credentials = {
        "CANOPY_GIT_TOKEN": "cnp_" + secrets.token_hex(32),
        "CANOPY_NODE_SIGNING_KEY_HEX": secrets.token_hex(32),
        "AWS_ACCESS_KEY_ID": "canopy-" + identifier,
        "AWS_SECRET_ACCESS_KEY": secrets.token_hex(32),
    }
    config = {
        "storage_url": "s3://canopy-eval/" + identifier,
        "tenant_id": str(uuid.uuid4()), "application_id": str(uuid.uuid4()),
        "node_id": str(uuid.uuid4()), "fleet_digest": secrets.token_hex(32),
        "image_digest": metadata["binary_sha256"], "owner": "canopy",
        "public_url": metadata["url"], "listen": f"127.0.0.1:{args.port}",
        "peer_endpoint": "https://local-evaluation.example.invalid",
        "data_dir": str(state / "node"),
        "native_limits": fixture_native_limits(), "local_disk_limit_bytes": args.disk_gib * 1024**3,
        "max_active_repositories": args.active_repositories,
    }
    save(state / "secrets.json", credentials, private=True)
    save(state / "config.json", config)
    save(state / "deployment.json", metadata)
    return metadata


def start(args, metadata):
    state = args.state_dir
    if owned_pid(state, metadata):
        if not ready(metadata["url"]):
            raise RuntimeError("node process exists but is not ready; inspect server.log")
        return
    binary = Path(metadata["binary"])
    if hashlib.sha256(binary.read_bytes()).hexdigest() != metadata["binary_sha256"]:
        raise RuntimeError("binary changed; this preview requires a fresh storage prefix/state directory")
    with socket.socket() as probe:
        # A drained listener can leave accepted connections in TIME_WAIT.
        # Match the server's normal address reuse without permitting a second
        # live listener on the same endpoint.
        probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        probe.bind(("127.0.0.1", int(metadata["url"].rsplit(":", 1)[1])))
    credentials = json.loads((state / "secrets.json").read_text())
    exists = subprocess.run(["docker", "container", "inspect", metadata["container"]],
                            capture_output=True).returncode == 0
    if exists:
        run("docker", "start", metadata["container"])
    else:
        run("docker", "volume", "create", metadata["volume"])
        # Pass values through docker's inherited environment, not its argv.
        provider_env = {**os.environ, "RUSTFS_ACCESS_KEY": credentials["AWS_ACCESS_KEY_ID"],
                        "RUSTFS_SECRET_KEY": credentials["AWS_SECRET_ACCESS_KEY"]}
        run("docker", "run", "-d", "--name", metadata["container"],
            "--restart", "unless-stopped", "--user", "10001:10001",
            "--memory", "2g", "--cpus", "2", "--pids-limit", "256",
            "--log-driver", "local", "--log-opt", "max-size=10m",
            "--log-opt", "max-file=3", "-p", "127.0.0.1::9000",
            "-v", metadata["volume"] + ":/data",
            "-e", "RUSTFS_ACCESS_KEY", "-e", "RUSTFS_SECRET_KEY",
            "-e", "RUSTFS_OBS_LOG_DIRECTORY=/data/logs",
            metadata["provider_image"], "/data", env=provider_env)
    endpoint = "http://" + run("docker", "port", metadata["container"], "9000/tcp")
    credentials["AWS_ENDPOINT"] = endpoint
    save(state / "secrets.json", credentials, private=True)
    env = environment(state)
    for _ in range(60):
        try:
            run("aws", "--endpoint-url", endpoint, "s3api", "head-bucket",
                "--bucket", metadata["bucket"], env=env)
            break
        except subprocess.CalledProcessError:
            try:
                run("aws", "--endpoint-url", endpoint, "s3api", "create-bucket",
                    "--bucket", metadata["bucket"], env=env)
                break
            except subprocess.CalledProcessError:
                time.sleep(1)
    else:
        raise RuntimeError("RustFS did not become ready; inspect its container logs")
    with (state / "server.log").open("ab") as output:
        process = subprocess.Popen([str(binary), str(state / "config.json")],
                                   env=env, stdout=output, stderr=output,
                                   stdin=subprocess.DEVNULL, start_new_session=True)
    (state / "server.pid").write_text(str(process.pid))
    for _ in range(150):
        if process.poll() is not None:
            raise RuntimeError("Canopy exited; inspect server.log")
        if ready(metadata["url"]):
            return
        time.sleep(0.2)
    # Startup remains supervised by the running node; don't silently kill it.
    raise RuntimeError("Canopy readiness timed out; inspect server.log before retrying")


def stop(state, metadata):
    pid = owned_pid(state, metadata)
    if pid:
        os.kill(pid, signal.SIGTERM)
        for _ in range(600):
            if not owned_pid(state, metadata):
                break
            time.sleep(0.2)
        else:
            raise RuntimeError("node is still draining; provider left running")
    run("docker", "stop", "--time", "30", metadata["container"])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("start", "stop", "status"))
    parser.add_argument("--state-dir", required=True, type=Path)
    parser.add_argument("--binary", type=Path)
    parser.add_argument("--port", type=int, default=18080)
    parser.add_argument("--disk-gib", type=int, default=64)
    parser.add_argument("--active-repositories", type=int, default=3)
    parser.add_argument("--provider-image", default=PROVIDER_IMAGE)
    args = parser.parse_args()
    if not 1 <= args.port <= 65535 or args.disk_gib < 1 or not 1 <= args.active_repositories <= 9999:
        parser.error("port must be 1..65535, disk-gib positive, and active-repositories 1..9999")
    args.state_dir = args.state_dir.resolve()
    if args.action == "start":
        args.state_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
        metadata = initialize(args)
        start(args, metadata)
    else:
        metadata = json.loads((args.state_dir / "deployment.json").read_text())
        if args.action == "stop":
            stop(args.state_dir, metadata)
    print(json.dumps({"url": metadata["url"], "ready": ready(metadata["url"]),
                      "pid": owned_pid(args.state_dir, metadata),
                      "state_dir": str(args.state_dir),
                      "credentials_file": str(args.state_dir / "secrets.json")}, indent=2))


if __name__ == "__main__":
    main()
