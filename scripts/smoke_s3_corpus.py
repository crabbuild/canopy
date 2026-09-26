"""Qualify immutable HEAD history from an existing, read-only repository input."""

import hashlib
import os
import subprocess
import time


def _environment():
    return {**os.environ, "GIT_TERMINAL_PROMPT": "0", "GIT_OPTIONAL_LOCKS": "0",
            "GIT_NO_REPLACE_OBJECTS": "1"}


def _git(*args, cwd=None):
    return subprocess.run(["git", "-c", "credential.helper=", *args], cwd=cwd,
                          env=_environment(), capture_output=True, check=True).stdout.strip()


def _inventory(repository, head):
    ids = sorted(set(_git("rev-list", "--objects", "--no-object-names", head, cwd=repository).splitlines()))
    digest = hashlib.sha256()
    process = subprocess.Popen(["git", "cat-file", "--batch"], cwd=repository,
                               env=_environment(), stdin=subprocess.PIPE,
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    try:
        for oid in ids:
            process.stdin.write(oid + b"\n")
            process.stdin.flush()
            header = process.stdout.readline()
            fields = header.split()
            assert len(fields) == 3 and fields[0] == oid, "invalid corpus object response"
            remaining = int(fields[2])
            assert remaining >= 0
            digest.update(header)
            while remaining:
                chunk = process.stdout.read(min(1 << 20, remaining))
                assert chunk, "truncated corpus object"
                digest.update(chunk)
                remaining -= len(chunk)
            assert process.stdout.read(1) == b"\n", "invalid corpus object terminator"
        process.stdin.close()
        assert process.wait(timeout=30) == 0, "corpus object reader failed"
        return len(ids), digest.hexdigest()
    finally:
        if process.poll() is None:
            process.kill()
            process.wait()
        for stream in (process.stdin, process.stdout, process.stderr):
            stream.close()


def seed(source, directory, url):
    head = _git("rev-parse", "HEAD", cwd=source).decode()
    bundle = directory / "corpus.bundle"
    # Bundle creation only reads the input checkout. All generated Git state
    # belongs to the qualification workspace; no fetch or reset touches source.
    _git("bundle", "create", str(bundle), "HEAD", cwd=source)
    local = directory / "corpus-input.git"
    _git("init", "--bare", "-b", "main", str(local))
    _git("fetch", str(bundle), "HEAD:refs/heads/main", cwd=local)
    assert _git("rev-parse", "HEAD", cwd=local).decode() == head, "source HEAD changed during bundling"
    expected = _inventory(source, head)
    assert _inventory(local, head) == expected, "bundle changed corpus bytes"
    hooks = directory / "corpus-hooks"
    hooks.mkdir()
    started = time.monotonic()
    _git("-c", f"core.hooksPath={hooks}", "-c",
         "http.extraHeader=Authorization: Bearer local-test-token",
         "push", url, "refs/heads/main", cwd=local)
    print(f"PASS: corpus HEAD {head}, {expected[0]} reachable objects, pushed in {time.monotonic() - started:.2f}s", flush=True)
    return head, expected


def verify(directory, url, expected):
    head, inventory = expected
    for protocol in ("0", "2"):
        clone = directory / f"corpus-restored-v{protocol}.git"
        started = time.monotonic()
        _git("-c", f"protocol.version={protocol}", "-c",
             "http.extraHeader=Authorization: Bearer local-test-token",
             "clone", "--bare", url, str(clone))
        cloned_at = time.monotonic()
        assert _git("rev-parse", "HEAD", cwd=clone).decode() == head
        assert _inventory(clone, head) == inventory, "recovered corpus object bytes differ"
        _git("fsck", "--strict", "--full", cwd=clone)
        print(f"PASS: corpus v{protocol} clone {cloned_at - started:.2f}s, verification {time.monotonic() - cloned_at:.2f}s, {inventory[0]} objects, SHA-256 {inventory[1]}", flush=True)
