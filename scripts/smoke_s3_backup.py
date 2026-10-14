"""Independent same-provider backup and restore of a disposable S3 deployment."""
import hashlib
import json
import os
import signal
import subprocess
import time
import urllib.parse
import urllib.request
import uuid


def qualify(binary, directory, settings, processes):
    from smoke_s3_process import start, create_repository, git, clone_and_verify
    from smoke_s3_api import request

    source_url = settings["storage_url"] + "/backup-source"
    backup_url = settings["storage_url"] + "/backup-copy"
    restored_url = settings["storage_url"] + "/backup-restored"
    source, base = start(binary, directory, {**settings, "storage_url": source_url}, "backup-source")
    processes.append(source)
    url, repository = create_repository(base, "saved")
    local = directory / "backup-client"
    git("init", "-b", "main", str(local))
    git("config", "user.name", "Canopy Backup", cwd=local)
    git("config", "user.email", "backup@example.invalid", cwd=local)
    git("lfs", "install", "--local", cwd=local)
    git("lfs", "track", "*.lfs", cwd=local)
    readme, lfs = b"git-backup" * (8 * 1024 * 1024), b"canopy-lfs" * (8 * 1024 * 1024)
    (local / "README.md").write_bytes(readme)
    (local / "asset.lfs").write_bytes(lfs)
    git("add", ".", cwd=local)
    git("commit", "-m", "Independent snapshot", cwd=local)
    started = time.monotonic()
    git("-c", "http.extraHeader=Authorization: Bearer local-test-token", "push", url, "main", cwd=local)
    push_seconds = time.monotonic() - started
    oid = git("rev-parse", "HEAD", cwd=local)
    request(base, "/api/repositories/saved/issues", "POST", {
        "repository_id": repository, "id": str(uuid.uuid4()),
        "title": "Retain collaboration", "body": "Backup without original Cell storage"})
    issue = request(base, "/api/repositories/saved/issues/1")
    empty_oid = hashlib.sha256(b"").hexdigest()
    empty_url = f"{url}/info/lfs/objects/{empty_oid}"
    for method, body in (("PUT", b""), ("GET", None)):
        empty = urllib.request.Request(empty_url, data=body, method=method,
                                       headers={"Authorization": "Bearer local-test-token"})
        with urllib.request.urlopen(empty, timeout=30) as response:
            assert response.status == 200 and response.read() == b""
    source.send_signal(signal.SIGTERM)
    source.wait(timeout=30)
    assert source.returncode == 0
    configuration = directory / "backup-source.json"
    pin = str(uuid.uuid4())
    backup_prefix = urllib.parse.urlsplit(backup_url).path.lstrip("/")
    restore_prefix = urllib.parse.urlsplit(restored_url).path.lstrip("/")

    def administer(action, *roots, fails=False):
        environment = {key: value for key, value in os.environ.items() if key != "CANOPY_GIT_TOKEN"}
        result = subprocess.run([str(binary), "backup", str(configuration), action, pin, *roots],
                                capture_output=True, env=environment, timeout=120)
        if fails:
            assert result.returncode != 0, "backup accepted a corrupt destination"
            return
        if result.returncode:
            error = result.stderr.decode(errors="replace")
            for name in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "CANOPY_NODE_SIGNING_KEY_HEX"):
                if os.environ.get(name):
                    error = error.replace(os.environ[name], "[redacted]")
            raise RuntimeError(f"backup {action} failed: {error}")
        return json.loads(result.stdout)

    saved = administer("create", backup_prefix)
    assert saved["cells"] == 2 and saved["external_objects"] == 3
    assert administer("create", backup_prefix) == saved
    aws = ["aws"]
    if os.environ.get("AWS_ENDPOINT"):
        aws.extend(["--endpoint-url", os.environ["AWS_ENDPOINT"]])
    lfs_url = f"{backup_url}/repos/{uuid.UUID(repository).hex}/lfs/{hashlib.sha256(lfs).hexdigest()}.parts/0000000000000000"
    corrupt = directory / "corrupt-backup-lfs"
    corrupt.write_bytes(b"corrupt fixture bytes")
    subprocess.run([*aws, "s3", "cp", str(corrupt), lfs_url, "--only-show-errors"], check=True, capture_output=True)
    administer("create", backup_prefix, fails=True)
    retained = subprocess.run([*aws, "s3", "cp", lfs_url, "-"], check=True, capture_output=True)
    assert retained.stdout == corrupt.read_bytes(), "conditional copy overwrote an existing object"
    repair = directory / "repair-backup-lfs-part"
    repair.write_bytes(lfs[:8 * 1024 * 1024])
    subprocess.run([*aws, "s3", "cp", str(repair), lfs_url, "--only-show-errors"], check=True, capture_output=True)
    assert administer("create", backup_prefix) == saved
    # This source was created above under the caller's random process-smoke UUID.
    parsed = urllib.parse.urlsplit(source_url)
    assert parsed.scheme == "s3" and "/process-smoke/" in parsed.path and parsed.path.endswith("/backup-source")
    subprocess.run([*aws, "s3", "rm", source_url + "/", "--recursive", "--only-show-errors"], check=True, capture_output=True)
    listing = subprocess.run([*aws, "s3api", "list-objects-v2", "--bucket", parsed.netloc,
                              "--prefix", parsed.path.lstrip("/") + "/", "--max-keys", "1"],
                             check=True, capture_output=True)
    assert json.loads(listing.stdout).get("KeyCount", 0) == 0
    assert administer("verify", backup_prefix) == saved
    restored = administer("restore", backup_prefix, restore_prefix)
    assert restored["cells"] == 2 and restored["external_objects"] == 3
    node, restored_base = start(binary, directory, {**settings, "storage_url": restored_url,
                                                  "node_id": str(uuid.uuid4())}, "backup-new-node")
    processes.append(node)
    started = time.monotonic()
    clone_and_verify(f"{restored_base}/canopy/saved.git", directory / "backup-clone", oid, readme, lfs)
    clone_seconds = time.monotonic() - started
    assert request(restored_base, "/api/repositories/saved/issues/1") == issue
    node.send_signal(signal.SIGTERM)
    node.wait(timeout=30)
    assert node.returncode == 0
    print(f"LFS qualification: bytes={len(lfs)}, stock push={push_seconds:.2f}s, restored clone+verification={clone_seconds:.2f}s", flush=True)
    print("PASS: stock 80 MiB Git blob plus 80 MiB and empty LFS transfers, then real backup CLI copies SQLite and external bodies; deleting every original source object still permits verification, isolated restore, exact stock clone/LFS and issue recovery", flush=True)
