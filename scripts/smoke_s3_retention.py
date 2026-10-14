"""Qualify fenced Cellule collection with retained backup pins and cold clones."""

import json
import signal
import subprocess
import time
import urllib.parse
import uuid


def qualify(binary, directory, settings, processes):
    from smoke_s3_process import start, create_repository, git

    settings = {**settings, "storage_url": settings["storage_url"] + "/retention"}
    server, base = start(binary, directory, settings, "retention-source")
    processes.append(server)
    url, _ = create_repository(base, "retained")
    source = directory / "retention-client"
    git("init", "-b", "main", str(source))
    git("config", "user.name", "Retention qualification", cwd=source)
    git("config", "user.email", "retention@example.invalid", cwd=source)
    config = directory / "retention-source.json"

    def cli(*args, fails=False):
        result = subprocess.run([str(binary), *args], capture_output=True, timeout=180)
        if fails:
            assert result.returncode != 0, "administration accepted an unsafe collection"
            return
        if result.returncode:
            raise RuntimeError(result.stderr.decode(errors="replace"))
        return json.loads(result.stdout)

    for revision in range(6):
        for index in range(64):
            (source / f"file-{index}").write_bytes(
                (f"revision={revision}, file={index}\n".encode() * 1024))
        git("add", ".", cwd=source)
        git("commit", "-m", f"Revision {revision}", cwd=source)
        git("-c", "http.extraHeader=Authorization: Bearer local-test-token",
            "push", url, "HEAD:refs/heads/main", cwd=source)
        if revision == 0:
            # Freeze a published snapshot before backup capture. A serving
            # owner's asynchronous checkpoint may legitimately change controls.
            server.send_signal(signal.SIGTERM)
            server.wait(timeout=90)
            assert server.returncode == 0
            pin = str(uuid.uuid4())
            backup_prefix = urllib.parse.urlsplit(settings["storage_url"]).path.lstrip("/") + "-backup"
            # An enrolled backup needs a workspace independent of the serving node.
            backup_config = directory / "retention-backup.json"
            backup = json.loads(config.read_text())
            backup.update(node_id=str(uuid.uuid4()), data_dir=str(directory / "retention-backup-work"))
            backup_config.write_text(json.dumps(backup))
            saved = cli("backup", str(backup_config), "create", pin, backup_prefix)
            server, base = start(binary, directory, settings, "retention-next", data_instance="retention-source")
            processes.append(server)
            url = f"{base}/canopy/retained.git"

    operation = str(uuid.uuid4())
    cli("gc", str(config), operation, "1", "128", fails=True)
    cli("maintenance", str(config), "begin", operation)
    server.send_signal(signal.SIGTERM)
    server.wait(timeout=90)
    assert server.returncode == 0
    time.sleep(0.02)
    partial = cli("gc", str(config), operation, "1", "1")
    assert partial["deleted_objects"] == 1 and not partial["complete"]
    assert partial["retained_pins"] == 1
    deleted = partial["deleted_objects"]
    for _ in range(100):
        report = cli("gc", str(config), operation, "1", "100000")
        deleted += report["deleted_objects"]
        if report["complete"]:
            break
    else:
        raise AssertionError("bounded collection did not converge")
    assert deleted > 1
    assert cli("backup", str(backup_config), "verify", pin, backup_prefix) == saved
    cli("maintenance", str(config), "end", operation)
    recovered, base = start(binary, directory, settings, "retention-cold")
    processes.append(recovered)
    clone = directory / "retention-clone"
    git("-c", "http.extraHeader=Authorization: Bearer local-test-token", "clone",
        f"{base}/canopy/retained.git", str(clone))
    git("fsck", "--full", cwd=clone)
    assert git("rev-parse", "HEAD", cwd=clone) == git("rev-parse", "HEAD", cwd=source)
    assert git("rev-list", "--objects", "--all", cwd=clone).splitlines() == git("rev-list", "--objects", "--all", cwd=source).splitlines()
    result = {"deleted_objects": deleted, "last_report": report,
              "backup_verified": True, "cold_clone_verified": True}
    (directory / "retention-results.json").write_text(json.dumps(result, indent=2))
    print(f"PASS: collected {deleted} obsolete Cellule objects; backup pin and cold stock-Git clone remain exact", flush=True)
    return result
