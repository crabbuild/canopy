#!/usr/bin/env python3
"""Exercise a real Canopy process through Git, LFS, restart, and lease takeover.

The caller provides an S3-compatible test bucket and provider credentials in the
environment. This script writes only below a unique prefix in that bucket.
"""

import argparse
from http.server import BaseHTTPRequestHandler, HTTPServer
import hashlib
import json
import os
from pathlib import Path
import secrets
import signal
import socket
import subprocess
import tempfile
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid


def port():
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return listener.getsockname()[1]


def run(*args, cwd=None):
    result = subprocess.run(
        args,
        cwd=cwd,
        env={**os.environ, "GIT_TERMINAL_PROMPT": "0"},
        capture_output=True,
        check=False,
    )
    if result.returncode:
        raise RuntimeError(f"{args[0]} failed: {result.stderr.decode(errors='replace')}")
    return result.stdout.strip()


def git(*args, cwd=None):
    return run("git", "-c", "credential.helper=", *args, cwd=cwd)


def wait_ready(process, address, log):
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError(f"Canopy exited: {log.read_text(errors='replace')}")
        try:
            with urllib.request.urlopen(f"http://{address}/readyz", timeout=1) as response:
                if response.status == 200:
                    return
        except (OSError, urllib.error.HTTPError):
            pass
        time.sleep(0.2)
    raise RuntimeError(f"Canopy did not become ready: {log.read_text(errors='replace')}")


def start(binary, directory, settings, instance):
    address = f"127.0.0.1:{port()}"
    config = {
        **settings,
        "listen": address,
        "public_url": f"http://{address}",
        "data_dir": str(directory / instance),
    }
    path = directory / f"{instance}.json"
    path.write_text(json.dumps(config))
    log = directory / f"{instance}.log"
    output = log.open("wb")
    process = subprocess.Popen([str(binary), str(path)], stdout=output, stderr=output)
    output.close()
    try:
        wait_ready(process, address, log)
    except BaseException:
        process.kill()
        process.wait()
        raise
    return process, f"http://{address}"


def create_repository(base_url, name):
    request = urllib.request.Request(
        f"{base_url}/api/repositories",
        data=json.dumps({"name": name}).encode(),
        headers={
            "Authorization": "Bearer local-test-token",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        created = json.load(response)
        return created["clone_url"], created["repository_id"]


def rename_repository(base_url, old_name, new_name, repository_id):
    request = urllib.request.Request(
        f"{base_url}/api/repositories/{old_name}",
        data=json.dumps({"name": new_name, "repository_id": repository_id}).encode(),
        headers={
            "Authorization": "Bearer local-test-token",
            "Content-Type": "application/json",
        },
        method="PATCH",
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        renamed = json.load(response)
        assert renamed["repository_id"] == repository_id
        return renamed["clone_url"]


def default_branch(base_url, name, reference=None):
    url = f"{base_url}/api/repositories/{name}/default-branch"
    headers = {"Authorization": "Bearer local-test-token", "Content-Type": "application/json"}
    with urllib.request.urlopen(urllib.request.Request(url, headers=headers), timeout=30) as response:
        current = json.load(response)
    if reference is None:
        return current
    payload = {"repository_id": current["repository_id"], "reference": reference,
               "expected_generation": current["generation"]}
    request = urllib.request.Request(url, data=json.dumps(payload).encode(), headers=headers, method="PUT")
    with urllib.request.urlopen(request, timeout=30) as response:
        changed = json.load(response)
        assert changed["reference"] == reference
        assert changed["generation"] == current["generation"] + 1
        return changed


def api_get(base_url, path, token):
    request = urllib.request.Request(f"{base_url}{path}", headers={"Authorization": f"Bearer {token}"})
    for _ in range(10):
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                return json.load(response)
        except urllib.error.HTTPError as error:
            if error.code != 503:
                raise
            time.sleep(1)
    raise RuntimeError("repository discovery admission did not recover")


def verify_discovery(base_url, token, expected_names):
    path = "/api/repositories"
    found = []
    for _ in range(100):
        page = api_get(base_url, path, token)
        assert len(page["repositories"]) <= 32
        found.extend(page["repositories"])
        cursor = page["next_cursor"]
        if cursor is None:
            break
        path = f"/api/repositories?after={urllib.parse.quote(cursor)}"
    else:
        raise RuntimeError("repository discovery did not terminate")
    assert sorted(entry["name"] for entry in found) == sorted(expected_names)
    for entry in found:
        detail = api_get(base_url, f"/api/repositories/{entry['name']}", token)
        assert detail["repository_id"] == entry["repository_id"]
        assert detail["clone_url"] == entry["clone_url"]
        assert detail["role"] in ("read", "write", "admin")


def api_status(base_url, path, token, method="GET", payload=None):
    data = None if payload is None else json.dumps(payload).encode()
    request = urllib.request.Request(
        f"{base_url}{path}",
        data=data,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
        method=method,
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return response.status
    except urllib.error.HTTPError as error:
        return error.code


def clone_and_verify(url, directory, expected_oid, expected_readme, expected_lfs=None, token="local-test-token", branch="main"):
    git(
        "-c",
        f"http.extraHeader=Authorization: Bearer {token}",
        "clone",
        url,
        str(directory),
    )
    if expected_lfs is not None:
        git("lfs", "install", "--local", cwd=directory)
        git(
            "-c",
            f"http.extraHeader=Authorization: Bearer {token}",
            "lfs",
            "pull",
            cwd=directory,
        )
    assert git("symbolic-ref", "HEAD", cwd=directory) == f"refs/heads/{branch}".encode()
    assert git("rev-parse", "HEAD", cwd=directory) == expected_oid
    assert (directory / "README.md").read_bytes() == expected_readme
    if expected_lfs is not None:
        assert (directory / "asset.lfs").read_bytes() == expected_lfs


def push_with_lost_reply(base_url, local):
    captured = {}
    push_id = str(uuid.uuid4())

    class Proxy(BaseHTTPRequestHandler):
        def log_message(self, *_args):
            pass

        def forward(self):
            assert not self.headers.get("Transfer-Encoding"), "smoke proxy requires Content-Length"
            body = self.rfile.read(int(self.headers.get("Content-Length", "0")))
            headers = {key: value for key, value in self.headers.items()
                       if key.lower() not in ("host", "content-length", "connection")}
            request = urllib.request.Request(
                f"{base_url}{self.path}", data=body if self.command == "POST" else None,
                headers=headers, method=self.command,
            )
            with urllib.request.urlopen(request, timeout=30) as response:
                reply = response.read()
                if self.command == "POST" and self.path.endswith("/git-receive-pack"):
                    captured.update(body=body, reply=reply, headers=headers)
                    self.close_connection = True
                    self.connection.shutdown(socket.SHUT_RDWR)
                    return
                self.send_response(response.status)
                for name, value in response.headers.items():
                    if name.lower() not in ("transfer-encoding", "content-length", "connection"):
                        self.send_header(name, value)
                self.send_header("Content-Length", str(len(reply)))
                self.end_headers()
                self.wfile.write(reply)

        do_GET = forward
        do_POST = forward

    with HTTPServer(("127.0.0.1", 0), Proxy) as proxy:
        worker = threading.Thread(target=proxy.serve_forever, daemon=True)
        worker.start()
        try:
            result = subprocess.run([
                "git", "-c", "credential.helper=", "-c",
                "http.extraHeader=Authorization: Bearer local-test-token", "-c",
                f"http.extraHeader=Idempotency-Key: {push_id}", "push",
                f"http://127.0.0.1:{proxy.server_port}/canopy/other.git",
                "HEAD:refs/heads/replayed",
            ], cwd=local, env={**os.environ, "GIT_TERMINAL_PROMPT": "0"},
                capture_output=True, timeout=30, check=False)
            assert result.returncode != 0
            assert b"ok refs/heads/replayed" in captured["reply"]
        finally:
            proxy.shutdown()
            worker.join()
    return push_id, captured


def seed_large_repository(base_url, directory):
    url, _ = create_repository(base_url, "large")
    local = directory / "large-source"
    git("init", "-b", "main", str(local))
    git("config", "user.name", "Canopy Test", cwd=local)
    git("config", "user.email", "canopy@example.invalid", cwd=local)
    hashes = {}
    for index in range(2):
        name = f"random-{index}.bin"
        path = local / name
        with path.open("wb") as output:
            for _ in range(40):
                output.write(os.urandom(1024 * 1024))
        with path.open("rb") as data:
            hashes[name] = hashlib.file_digest(data, "sha256").hexdigest()
        git("add", name, cwd=local)
        git("commit", "-m", f"Large object {index}", cwd=local)
    # One push must carry both incompressible blobs, exceeding the former 64 MiB
    # request limit. Client packet buffering stays small, exercising chunked HTTP.
    started = time.monotonic()
    git("-c", "http.postBuffer=1048576", "-c",
        "http.extraHeader=Authorization: Bearer local-test-token",
        "push", url, "HEAD:refs/heads/main", cwd=local)
    print(f"PASS: one push uploaded 80 MiB of random blob data in {time.monotonic() - started:.2f}s", flush=True)
    return git("rev-parse", "HEAD", cwd=local), hashes


def verify_large_clone(base_url, directory, expected):
    oid, hashes = expected
    for protocol in (0, 2):
        clone = directory / f"large-clone-v{protocol}"
        started = time.monotonic()
        # Keep even a small object count packed so the size assertion measures
        # the received transfer rather than Git's loose-object unpack policy.
        git("-c", f"protocol.version={protocol}", "-c", "fetch.unpackLimit=1", "-c",
            "http.extraHeader=Authorization: Bearer local-test-token", "clone",
            f"{base_url}/canopy/large.git", str(clone))
        assert git("rev-parse", "HEAD", cwd=clone) == oid
        for name, digest in hashes.items():
            with (clone / name).open("rb") as data:
                assert hashlib.file_digest(data, "sha256").hexdigest() == digest
        git("fsck", "--full", cwd=clone)
        packs = list((clone / ".git/objects/pack").glob("*.pack"))
        size = sum(pack.stat().st_size for pack in packs)
        assert size > 64 * 1024 * 1024, "qualification pack must exceed the old response limit"
        print(f"PASS: protocol v{protocol} clone restored {size} pack bytes in {time.monotonic() - started:.2f}s", flush=True)


def seed_many_objects(base_url, directory, count):
    url, _ = create_repository(base_url, "many")
    local = directory / "many-source"
    git("init", "-b", "main", str(local))
    git("config", "user.name", "Canopy Test", cwd=local)
    git("config", "user.email", "canopy@example.invalid", cwd=local)
    for index in range(count):
        (local / f"file-{index}").write_bytes(f"original {index}\0\n".encode())
    git("add", ".", cwd=local)
    git("commit", "-m", "Many objects", cwd=local)
    initial = git("rev-parse", "HEAD", cwd=local)
    for stage in ("initial", "incremental"):
        if stage == "incremental":
            (local / "file-0").write_bytes(b"changed\0\n")
            git("add", ".", cwd=local)
            git("commit", "-m", "One changed object", cwd=local)
            git("tag", "-a", "release", "-m", "Annotated release", cwd=local)
        started = time.monotonic()
        git("-c", "http.extraHeader=Authorization: Bearer local-test-token",
            "push", "--tags", url, "HEAD:refs/heads/main", cwd=local)
        print(f"PASS: {count}-file {stage} push in {time.monotonic() - started:.2f}s", flush=True)
    refs = [f"refs/tags/snapshot-{index:03}" for index in range(300)]
    head = git("rev-parse", "HEAD", cwd=local).decode()
    subprocess.run(
        ["git", "update-ref", "--stdin"], cwd=local,
        input="".join(f"update {name} {head}\n" for name in refs).encode(),
        capture_output=True, check=True,
    )
    for start in range(0, len(refs), 64):
        git("-c", "http.extraHeader=Authorization: Bearer local-test-token",
            "push", url, *refs[start:start + 64], cwd=local)
    print("PASS: published 300 additional refs across bounded push transactions", flush=True)
    return initial, git("rev-parse", "HEAD", cwd=local), git("rev-parse", "release", cwd=local)


def verify_many_objects(base_url, directory, count, expected):
    initial, head, tag = expected
    clone = directory / "many-restored"
    started = time.monotonic()
    git("-c", "http.extraHeader=Authorization: Bearer local-test-token", "clone",
        f"{base_url}/canopy/many.git", str(clone))
    assert git("rev-parse", "HEAD", cwd=clone) == head
    assert git("rev-parse", "HEAD^", cwd=clone) == initial
    assert git("rev-parse", "release", cwd=clone) == tag
    refs = git("for-each-ref", "--format=%(objectname) %(refname)", "refs/tags/snapshot-*", cwd=clone)
    expected_refs = [head + f" refs/tags/snapshot-{index:03}".encode() for index in range(300)]
    assert refs.splitlines() == expected_refs
    for index in range(count):
        expected_body = b"changed\0\n" if index == 0 else f"original {index}\0\n".encode()
        assert (clone / f"file-{index}").read_bytes() == expected_body
    git("fsck", "--full", cwd=clone)
    print(f"PASS: {count}-file clone restored both commits, annotated tag and 300 refs after takeover in {time.monotonic() - started:.2f}s", flush=True)


def seed_sqlite_chunks(base_url, directory):
    url, _ = create_repository(base_url, "sqlite-chunks")
    local = directory / "chunk-source"
    git("init", "--bare", str(local))
    expected = {}

    def write(kind, body):
        path = directory / f"chunk-{kind}"
        path.write_bytes(body)
        oid = git("hash-object", "-w", "-t", kind, str(path), cwd=local).decode()
        expected[kind] = (oid, path)
        return oid

    blob = write("blob", b"chunk recovery leaf\n")
    tree = write("tree", b"".join(
        f"100644 file-{index:05}\0".encode() + bytes.fromhex(blob)
        for index in range(32_000)
    ))
    commit = write("commit", (
        f"tree {tree}\nauthor Canopy <test@example.invalid> 0 +0000\n"
        "committer Canopy <test@example.invalid> 0 +0000\n\n"
    ).encode() + b"c" * 1_100_000 + b"\n")
    tag = write("tag", (
        f"object {commit}\ntype commit\ntag release\n"
        "tagger Canopy <test@example.invalid> 0 +0000\n\n"
    ).encode() + b"t" * 1_100_000 + b"\n")
    git("update-ref", "refs/heads/main", commit, cwd=local)
    git("symbolic-ref", "HEAD", "refs/heads/main", cwd=local)
    git("update-ref", "refs/tags/release", tag, cwd=local)
    started = time.monotonic()
    git("-c", "http.extraHeader=Authorization: Bearer local-test-token",
        "push", url, "refs/heads/main", "refs/tags/release", cwd=local)
    print(f"PASS: large tree, commit and tag pushed into SQLite chunks in {time.monotonic() - started:.2f}s", flush=True)
    return expected


def verify_sqlite_chunks(base_url, directory, expected):
    clone = directory / "chunk-restored"
    started = time.monotonic()
    git("-c", "http.extraHeader=Authorization: Bearer local-test-token",
        "clone", "--bare", f"{base_url}/canopy/sqlite-chunks.git", str(clone))
    for kind, (oid, path) in expected.items():
        # run()/git() strip whitespace for command output. Compare raw object bytes here.
        actual = subprocess.run(["git", "cat-file", kind, oid], cwd=clone,
                                capture_output=True, check=True).stdout
        assert actual == path.read_bytes(), f"{kind} changed during recovery"
    assert git("rev-parse", "refs/heads/main", cwd=clone).decode() == expected["commit"][0]
    assert git("rev-parse", "refs/tags/release", cwd=clone).decode() == expected["tag"][0]
    git("fsck", "--strict", "--full", cwd=clone)
    print(f"PASS: SQLite chunks restored exact tree/commit/tag bytes and OIDs after takeover in {time.monotonic() - started:.2f}s", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--storage-url", required=True, help="S3 bucket URL, e.g. s3://test-bucket")
    parser.add_argument("--work-parent", type=Path, required=True)
    parser.add_argument("--large-clone", action="store_true", help="Qualify a push and v0/v2 clones above 64 MiB, including takeover")
    parser.add_argument("--many-objects", type=int, default=0, metavar="COUNT", help="Qualify many small objects, an incremental push, and takeover recovery")
    parser.add_argument("--sqlite-chunks", action="store_true", help="Qualify large tree, commit and tag objects stored in SQLite and restored after takeover")
    args = parser.parse_args()
    if args.many_objects < 0:
        parser.error("--many-objects must be nonnegative")
    for name in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "CANOPY_NODE_SIGNING_KEY_HEX"):
        if not os.environ.get(name):
            parser.error(f"{name} must be set")
    os.environ["CANOPY_GIT_TOKEN"] = "local-test-token"
    run_id = uuid.uuid4()
    settings = {
        "storage_url": f"{args.storage_url.rstrip('/')}/process-smoke/{run_id}",
        "tenant_id": str(uuid.uuid4()),
        "application_id": str(uuid.uuid4()),
        "node_id": str(uuid.uuid4()),
        "fleet_digest": "11" * 32,
        "image_digest": "22" * 32,
        "owner": "canopy",
        "peer_endpoint": "https://smoke.example.invalid",
        "local_disk_limit_bytes": 1 << 30,
    }
    with tempfile.TemporaryDirectory(prefix="canopy-process-", dir=args.work_parent) as temp:
        directory = Path(temp)
        processes = []
        try:
            first, base_url = start(args.binary, directory, settings, "first")
            processes.append(first)
            url, repository_id = create_repository(base_url, "example")
            local = directory / "local"
            git("init", "-b", "main", str(local))
            git("config", "user.name", "Canopy Test", cwd=local)
            git("config", "user.email", "canopy@example.invalid", cwd=local)
            git("lfs", "install", "--local", cwd=local)
            git("lfs", "track", "*.lfs", cwd=local)
            (local / "README.md").write_bytes(b"Canopy process smoke\n")
            lfs_body = b"canopy-lfs" * 130_000
            (local / "asset.lfs").write_bytes(lfs_body)
            git("add", ".", cwd=local)
            git("commit", "-m", "Initial commit", cwd=local)
            git(
                "-c",
                "http.extraHeader=Authorization: Bearer local-test-token",
                "push",
                url,
                "HEAD:refs/heads/main",
                "HEAD:refs/heads/reused",
                cwd=local,
            )
            oid = git("rev-parse", "HEAD", cwd=local)
            git(
                "-c", "http.extraHeader=Authorization: Bearer local-test-token",
                "push", url, ":refs/heads/reused", cwd=local,
            )
            other_url, _ = create_repository(base_url, "other")
            other = directory / "other"
            git("init", "-b", "main", str(other))
            git("config", "user.name", "Canopy Test", cwd=other)
            git("config", "user.email", "canopy@example.invalid", cwd=other)
            other_readme = b"Another Repository Cell\n"
            (other / "README.md").write_bytes(other_readme)
            git("add", "README.md", cwd=other)
            git("commit", "-m", "Other repository", cwd=other)
            git(
                "-c",
                "http.extraHeader=Authorization: Bearer local-test-token",
                "push",
                other_url,
                "HEAD:refs/heads/main",
                cwd=other,
            )
            other_oid = git("rev-parse", "HEAD", cwd=other)
            (other / "accepted.txt").write_bytes(b"Accepted part of a mixed push\n")
            git("add", "accepted.txt", cwd=other)
            git("commit", "-m", "Mixed push commit", cwd=other)
            partial_oid = git("rev-parse", "HEAD", cwd=other)
            blob = git("rev-parse", "HEAD:accepted.txt", cwd=other).decode()
            for atomic, accepted, rejected in (
                (False, "partial", "rejected"),
                (True, "atomic-accepted", "atomic-rejected"),
            ):
                command = [
                    "git", "-c", "credential.helper=", "-c",
                    "http.extraHeader=Authorization: Bearer local-test-token", "push",
                ]
                if atomic:
                    command.append("--atomic")
                result = subprocess.run(
                    [*command, other_url, f"HEAD:refs/heads/{accepted}", f"+{blob}:refs/heads/{rejected}"],
                    cwd=other, env={**os.environ, "GIT_TERMINAL_PROMPT": "0"},
                    capture_output=True, timeout=30, check=False,
                )
                assert result.returncode != 0
                assert b"[remote rejected]" in result.stderr
                published = git(
                    "-c", "http.extraHeader=Authorization: Bearer local-test-token",
                    "ls-remote", other_url, f"refs/heads/{accepted}",
                )
                assert published == (b"" if atomic else partial_oid + b"\trefs/heads/partial")
            replay_id, lost_reply = push_with_lost_reply(base_url, other)
            reader_token = f"cnp_{secrets.token_hex(32)}"
            assert api_status(
                base_url,
                "/api/accounts",
                "local-test-token",
                "POST",
                {"name": "reader", "token": reader_token, "scope": "read"},
            ) == 200
            assert api_status(
                base_url,
                "/api/repositories/example/collaborators/reader",
                "local-test-token",
                "PUT",
                {"role": "read"},
            ) == 200
            git("-c", "http.extraHeader=Authorization: Bearer local-test-token",
                "push", url, "HEAD:refs/heads/trunk", cwd=local)
            selected_head = default_branch(base_url, "example", "refs/heads/trunk")
            url = rename_repository(base_url, "example", "renamed", repository_id)
            assert default_branch(base_url, "renamed") == selected_head
            verify_discovery(base_url, reader_token, ["renamed"])
            verify_discovery(base_url, "local-test-token", ["renamed", "other"])
            clone_and_verify(url, directory / "renamed-live", oid, b"Canopy process smoke\n", lfs_body, branch="trunk")
            clone_and_verify(url, directory / "reader-live", oid, b"Canopy process smoke\n", lfs_body, reader_token, branch="trunk")
            chunks = seed_sqlite_chunks(base_url, directory) if args.sqlite_chunks else None
            large = seed_large_repository(base_url, directory) if args.large_clone else None
            many = seed_many_objects(base_url, directory, args.many_objects) if args.many_objects else None
            if large is not None and many is not None:
                clone_and_verify(f"{base_url}/canopy/other.git", directory / "evicted-other", other_oid, other_readme)
                clone_and_verify(url, directory / "evicted-renamed", oid, b"Canopy process smoke\n", lfs_body, branch="trunk")
                print("PASS: four repository Cells restored Git/LFS through resident eviction on one node", flush=True)
            first.send_signal(signal.SIGTERM)
            first.wait(timeout=30)
            if first.returncode:
                raise RuntimeError("Canopy did not shut down cleanly")
            second, base_url = start(args.binary, directory, settings, "second")
            processes.append(second)
            url = f"{base_url}/canopy/renamed.git"
            assert default_branch(base_url, "renamed") == selected_head
            verify_discovery(base_url, reader_token, ["renamed"])
            clone_and_verify(url, directory / "clean-clone", oid, b"Canopy process smoke\n", lfs_body, branch="trunk")
            clone_and_verify(f"{base_url}/canopy/other.git", directory / "clean-other", other_oid, other_readme)
            second.kill()
            second.wait(timeout=10)
            time.sleep(11)  # Wait past the signed node advertisement's 10-second lease.
            third, base_url = start(args.binary, directory, settings, "third")
            processes.append(third)
            url = f"{base_url}/canopy/renamed.git"
            assert default_branch(base_url, "renamed") == selected_head
            verify_discovery(base_url, reader_token, ["renamed"])
            clone_and_verify(url, directory / "takeover-clone", oid, b"Canopy process smoke\n", lfs_body, branch="trunk")
            clone_and_verify(url, directory / "reader-takeover", oid, b"Canopy process smoke\n", lfs_body, reader_token, branch="trunk")
            clone_and_verify(f"{base_url}/canopy/other.git", directory / "takeover-other", other_oid, other_readme)
            assert git("rev-parse", "refs/remotes/origin/partial", cwd=directory / "takeover-other") == partial_oid
            assert git("show", "refs/remotes/origin/partial:accepted.txt", cwd=directory / "takeover-other") == b"Accepted part of a mixed push"
            assert not git(
                "-c", "http.extraHeader=Authorization: Bearer local-test-token",
                "ls-remote", f"{base_url}/canopy/other.git",
                "refs/heads/rejected", "refs/heads/atomic-accepted", "refs/heads/atomic-rejected",
            )
            other_restored_url = f"{base_url}/canopy/other.git"
            git("-c", "http.extraHeader=Authorization: Bearer local-test-token",
                "push", other_restored_url, ":refs/heads/replayed", cwd=other)
            replay = urllib.request.Request(
                f"{other_restored_url}/git-receive-pack", data=lost_reply["body"],
                headers=lost_reply["headers"], method="POST",
            )
            with urllib.request.urlopen(replay, timeout=30) as response:
                assert response.headers["X-Canopy-Push-Id"] == replay_id
                assert response.read() == lost_reply["reply"]
            assert not git("-c", "http.extraHeader=Authorization: Bearer local-test-token",
                           "ls-remote", other_restored_url, "refs/heads/replayed")
            assert not git(
                "-c", "http.extraHeader=Authorization: Bearer local-test-token",
                "ls-remote", url, "refs/heads/reused",
            )
            git(
                "-c", "http.extraHeader=Authorization: Bearer local-test-token",
                "push", url, "HEAD:refs/heads/reused", cwd=local,
            )
            assert git(
                "-c", "http.extraHeader=Authorization: Bearer local-test-token",
                "ls-remote", url, "refs/heads/reused",
            ) == oid + b"\trefs/heads/reused"
            assert api_status(
                base_url,
                "/api/repositories/renamed/collaborators/reader",
                "local-test-token",
                "DELETE",
            ) == 204
            assert api_status(
                base_url,
                "/canopy/renamed.git/info/refs?service=git-upload-pack",
                reader_token,
            ) == 404
            verify_discovery(base_url, reader_token, [])
            assert api_status(base_url, "/api/repositories/renamed", reader_token) == 404
            if large is not None:
                verify_large_clone(base_url, directory, large)
            if chunks is not None:
                verify_sqlite_chunks(base_url, directory, chunks)
            if many is not None:
                verify_many_objects(base_url, directory, args.many_objects, many)
            third.send_signal(signal.SIGTERM)
            third.wait(timeout=30)
            if third.returncode:
                raise RuntimeError("takeover owner did not shut down cleanly")
            print("PASS: Git/LFS, repository discovery, default branch, ACL, ref outcomes, and a dropped push reply survived restart, disk loss, and lease takeover")
        except Exception:
            for log in directory.glob("*.log"):
                errors = [line for line in log.read_text(errors="replace").splitlines() if "ERROR" in line or "WARN" in line]
                for line in errors[-10:]:
                    for name in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "CANOPY_NODE_SIGNING_KEY_HEX"):
                        line = line.replace(os.environ[name], "[redacted]")
                    print(f"{log.name}: {line}", flush=True)
            raise
        finally:
            for process in processes:
                if process.poll() is None:
                    process.kill()
                    process.wait()


if __name__ == "__main__":
    main()
