"""Run the large-size gate against an isolated, disposable RustFS bucket."""
import os
from pathlib import Path
import subprocess
import tempfile
import time
import uuid


def run(*args, **kwargs):
    result = subprocess.run(args, check=True, capture_output=True, text=True, **kwargs)
    return (result.stdout + (result.stderr if args[:2] == ("docker", "logs") else "")).strip()


def main():
    name = f"canopy-size-{uuid.uuid4().hex[:12]}"
    with tempfile.TemporaryDirectory(prefix=name) as temporary:
        directory = Path(temporary).resolve()
        image = "rustfs/rustfs:1.0.0-beta.8-glibc"
        owner = f"{os.getuid()}:{os.getgid()}"
        # A VM can silently bind its own empty directory at an unshared host path.
        # Prove this is the requested test volume before the provider writes data.
        probe = directory / ".canopy-bind-probe"
        probe.write_text(name)
        try:
            observed = run("docker", "run", "--rm", "--user", owner, "--entrypoint", "cat",
                           "-v", f"{directory}:/data", image, "/data/.canopy-bind-probe")
        except subprocess.CalledProcessError as error:
            raise RuntimeError(f"Docker must share the host test volume: {directory}") from error
        if observed != name:
            raise RuntimeError("Docker cannot see the selected temporary volume")
        probe.unlink()
        started = False
        env = {**{key: value for key, value in os.environ.items() if not key.startswith("AWS_")}, "AWS_ACCESS_KEY_ID": "canopy-test-access",
               "AWS_SECRET_ACCESS_KEY": "canopy-test-secret", "AWS_DEFAULT_REGION": "us-east-1",
               "AWS_EC2_METADATA_DISABLED": "true", "AWS_CONFIG_FILE": os.devnull,
               "AWS_SHARED_CREDENTIALS_FILE": os.devnull}
        try:
            run("docker", "run", "-d", "--name", name,
                "--user", owner,
                "-p", "127.0.0.1::9000", "-v", f"{directory}:/data",
                "-e", "RUSTFS_OBS_LOG_DIRECTORY=/data/logs",
                "-e", "RUSTFS_ACCESS_KEY=canopy-test-access", "-e", "RUSTFS_SECRET_KEY=canopy-test-secret",
                image, "/data")
            started = True
            try:
                endpoint = "http://" + run("docker", "port", name, "9000/tcp")
            except subprocess.CalledProcessError:
                raise RuntimeError("RustFS startup failed: " + run("docker", "logs", name)) from None
            command = ["aws", "--endpoint-url", endpoint, "s3api", "create-bucket", "--bucket", "canopy-size"]
            for attempt in range(60):
                if run("docker", "inspect", "--format", "{{.State.Running}}", name) != "true":
                    raise RuntimeError("RustFS exited: " + run("docker", "logs", name))
                try:
                    result = subprocess.run(command, env=env, capture_output=True, timeout=5)
                    if result.returncode == 0:
                        break
                except subprocess.TimeoutExpired:
                    pass
                time.sleep(1)
            else:
                raise RuntimeError("RustFS fixture did not become ready")
            env["CANOPY_TEST_S3_ENDPOINT"] = endpoint
            env["CANOPY_TEST_S3_BUCKET"] = "canopy-size"
            subprocess.run(["cargo", "test", "--locked", "--test", "multi_server", "size::",
                            "--", "--ignored", "--nocapture"], env=env, check=True,
                           cwd=Path(__file__).resolve().parents[1])
        except Exception:
            if started:
                subprocess.run(["docker", "logs", "--tail", "60", name])
            raise
        finally:
            if started:
                subprocess.run(["docker", "rm", "-f", name], capture_output=True)


if __name__ == "__main__":
    main()
