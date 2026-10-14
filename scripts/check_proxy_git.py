#!/usr/bin/env python3
"""Check critical stock-Git operations through one ingress and retain receipts.

The seed uses two new disposable repositories. Verification can target a fresh
fleet after caller-recorded owner loss. Functional step wall times are not a
scheduled throughput benchmark or proof that owners restarted.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import time
import uuid

import benchmark_repositories as benchmark


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def refs(local, token):
    listing = benchmark.git("for-each-ref", "--format=%(objectname) %(refname)", cwd=local, token=token)
    result = {}
    for line in listing.splitlines():
        oid, reference = line.split(" ", 1)
        require(reference not in result, "duplicate local ref")
        result[reference] = oid
    return result


def remote_refs(url, cwd, token, protocol="2"):
    listing = benchmark.git("-c", f"protocol.version={protocol}", "ls-remote", "--refs", url,
                            cwd=cwd, token=token, request_id=str(uuid.uuid4()))
    result = {}
    for line in listing.splitlines():
        oid, reference = line.split("\t", 1)
        require(reference not in result, "duplicate remote ref")
        result[reference] = oid
    return result


def create(client, name):
    status, body = client.request("/api/repositories", {"name": name}, request_id=str(uuid.uuid4()))
    require(status == 200, f"critical fixture creation failed: HTTP {status}")
    entry = json.loads(body)
    require(entry.get("name") == name and benchmark.canonical_repository_uuid(entry.get("repository_id"))
            and isinstance(entry.get("owner"), str) and re.fullmatch(r"[a-z0-9_-]{1,64}", entry["owner"]),
            "critical fixture has an invalid identity")
    return {key: entry[key] for key in ("name", "owner", "repository_id")}


def verify_receipt(receipt, base_url, work_dir, client, token):
    require(receipt.get("version") == 1 and receipt.get("complete") is True,
            "require a complete critical-operation receipt")
    repositories = receipt.get("repositories")
    require(isinstance(repositories, list) and len(repositories) == 2,
            "receipt must contain both original and mirrored identities")
    require(len({entry["repository_id"] for entry in repositories}) == 2,
            "critical fixture identities must be distinct")
    for entry in repositories:
        require(benchmark.canonical_repository_uuid(entry.get("repository_id"))
                and all(isinstance(entry.get(key), str) and re.fullmatch(r"[a-z0-9_-]{1,64}", entry[key])
                        for key in ("name", "owner")), "invalid critical recovery identity")
    expected = receipt.get("refs")
    require(isinstance(expected, dict) and bool(expected) and "refs/heads/main" in expected
            and all(isinstance(reference, str) and reference.startswith("refs/")
                    and isinstance(oid, str) and re.fullmatch(r"[0-9a-f]{40}", oid)
                    for reference, oid in expected.items()), "invalid critical ref inventory")
    require(isinstance(receipt.get("payload_sha256"), str)
            and re.fullmatch(r"[0-9a-f]{64}", receipt["payload_sha256"]), "invalid critical payload digest")
    work_dir.mkdir(parents=True, exist_ok=False)
    for index, entry in enumerate(repositories):
        status, body = client.request(f"/api/repositories/{entry['name']}", request_id=str(uuid.uuid4()))
        require(status == 200 and json.loads(body).get("repository_id") == entry["repository_id"],
                "critical repository identity differs")
        url = f"{base_url}/{entry['owner']}/{entry['name']}.git"
        for protocol in ("0", "2"):
            require(remote_refs(url, work_dir, token, protocol) == expected, "critical remote refs differ")
            clone = work_dir / f"repo-{index}-v{protocol}.git"
            benchmark.git("-c", f"protocol.version={protocol}", "clone", "--mirror", url, str(clone),
                          cwd=work_dir, token=token, request_id=str(uuid.uuid4()))
            require(refs(clone, token) == expected, "critical mirror refs differ")
            benchmark.git("fsck", "--strict", "--full", cwd=clone, token=token)
            payload = benchmark.git("show", "main:payload.txt", cwd=clone, token=token).encode() + b"\n"
            require(hashlib.sha256(payload).hexdigest() == receipt["payload_sha256"], "critical payload differs")
            require(benchmark.git("notes", "show", "main", cwd=clone, token=token) == receipt["note"],
                    "critical Git note differs")
    return {"verified_repositories": 2, "protocols": [0, 2], "exact_ref_inventories": 4,
            "owner_recovery": "not established by this verifier; caller must record process loss and fresh state"}


def seed(args, client, token):
    require(not args.receipt.exists(), "seed requires a new receipt")
    args.work_dir.mkdir(parents=True, exist_ok=False)
    record = {"version": 1, "complete": False, "error": None, "repositories": [], "steps": [],
              "driver_sha256": benchmark.file_sha256(Path(__file__)),
              "git_driver_sha256": benchmark.file_sha256(Path(benchmark.__file__)),
              "binary_sha256": benchmark.file_sha256(args.binary),
              "base_url": args.base_url, "timing_scope": "functional probe wall time, not scheduled performance"}
    def step(name, action):
        started = time.monotonic()
        try:
            value = action()
        except BaseException as error:
            record["steps"].append({"name": name, "ok": False, "error": type(error).__name__,
                                    "wall_seconds": time.monotonic() - started})
            raise
        record["steps"].append({"name": name, "ok": True, "wall_seconds": time.monotonic() - started})
        benchmark.save(args.receipt, record)
        print(f"verified operation: {name}", flush=True)
        return value
    local = args.work_dir / "source"
    def git(*command, cwd=local):
        return benchmark.git(*command, cwd=cwd, token=token, request_id=str(uuid.uuid4()))
    try:
        record["git_version"] = git("--version", cwd=args.work_dir)
        prefix = "critical-" + uuid.uuid4().hex[:12]
        for suffix in ("source", "mirror"):
            def create_entry(suffix=suffix):
                entry = create(client, f"{prefix}-{suffix}")
                record["repositories"].append(entry)
                return entry
            step(f"create-{suffix}", create_entry)
        urls = [f"{args.base_url}/{entry['owner']}/{entry['name']}.git" for entry in record["repositories"]]
        url, mirror_url = urls
        git("init", "-b", "main", str(local), cwd=args.work_dir)
        git("config", "user.name", "Canopy Critical Operations")
        git("config", "user.email", "critical@example.invalid")
        payload = (f"{prefix}/critical-payload\n" * 512).encode()
        (local / "payload.txt").write_bytes(payload)
        for revision in (1, 2, 3):
            (local / "README.md").write_text(f"revision={revision}\n")
            git("add", ".")
            git("commit", "-m", f"Revision {revision}")
        tip, old = git("rev-parse", "HEAD"), git("rev-parse", "HEAD~1")
        git("tag", "lightweight")
        git("tag", "-a", "版本", "-m", "Annotated release")
        git("notes", "add", "-m", "retained critical note")
        for branch in ("feature", "café", "開発", "🌳"):
            git("branch", branch)
        git("update-ref", "refs/custom/critical", tip)
        step("atomic-multiple-ref-publication", lambda: git("push", "--atomic", "--mirror", url))
        expected = refs(local, token)
        require(remote_refs(url, args.work_dir, token) == expected, "initial atomic inventory differs")
        blob = git("rev-parse", "HEAD:payload.txt")
        for atomic in (False, True):
            accepted, rejected = ("atomic-accepted", "atomic-rejected") if atomic else ("partial", "rejected")
            def reject(atomic=atomic, accepted=accepted, rejected=rejected):
                result = benchmark.git_result("push", *(["--atomic"] if atomic else []), url,
                    f"HEAD:refs/heads/{accepted}", f"+{blob}:refs/heads/{rejected}",
                    cwd=local, token=token, request_id=str(uuid.uuid4()))
                require(result.returncode != 0 and b"[remote rejected]" in result.stderr,
                        "expected a decoded remote ref rejection, not a transport error")
                actual = remote_refs(url, args.work_dir, token)
                wanted = {**expected, **({f"refs/heads/{accepted}": tip} if not atomic else {})}
                require(actual == wanted, "mixed/atomic refusal published the wrong ref inventory")
            step("atomic-refusal" if atomic else "mixed-refusal", reject)
            if not atomic:
                expected["refs/heads/partial"] = tip
        step("force-with-correct-lease", lambda: git("push", f"--force-with-lease=refs/heads/feature:{tip}",
                                                     url, f"{old}:refs/heads/feature"))
        expected["refs/heads/feature"] = old
        def stale_lease():
            result = benchmark.git_result("push", f"--force-with-lease=refs/heads/feature:{tip}",
                url, f"{tip}:refs/heads/feature", cwd=local, token=token, request_id=str(uuid.uuid4()))
            require(result.returncode != 0 and b"stale info" in result.stderr,
                    "stale lease did not produce its expected client refusal")
            require(remote_refs(url, args.work_dir, token) == expected, "stale lease changed refs")
        step("stale-lease-refusal", stale_lease)
        shallow = args.work_dir / "shallow"
        def shallow_history():
            git("clone", "--depth=1", "--branch=main", url, str(shallow), cwd=args.work_dir)
            require(git("rev-list", "--count", "HEAD", cwd=shallow) == "1", "shallow depth differs")
            git("fetch", "--deepen=1", cwd=shallow)
            require(git("rev-list", "--count", "HEAD", cwd=shallow) == "2", "deepened depth differs")
            git("fetch", "--unshallow", cwd=shallow)
            require(git("rev-list", "--count", "HEAD", cwd=shallow) == "3", "unshallow history differs")
            git("fsck", "--strict", "--full", cwd=shallow)
        step("shallow-deepen-unshallow", shallow_history)
        for protocol in ("0", "2"):
            def partial(protocol=protocol):
                destination = args.work_dir / f"filtered-v{protocol}"
                git("-c", f"protocol.version={protocol}", "clone", "--no-checkout", "--single-branch",
                    "--branch=main", "--filter=blob:none", url, str(destination), cwd=args.work_dir)
                missing = git("rev-list", "--objects", "--all", "--missing=print", cwd=destination)
                require("?" + blob in missing.splitlines(), "filtered clone did not actually omit the payload")
                actual = git("show", "main:payload.txt", cwd=destination).encode() + b"\n"
                require(actual == payload, "lazy fetched payload differs")
                git("fsck", "--strict", "--full", cwd=destination)
            step(f"filtered-lazy-fetch-v{protocol}", partial)
        # Keep the final graph stable for later owner-loss verification.
        (local / "README.md").write_text("revision=4\n")
        git("commit", "-am", "Incremental revision")
        newest = git("rev-parse", "HEAD")
        git("notes", "add", "-m", "retained critical note")
        step("incremental-push", lambda: git("push", "--atomic", url, "main", "refs/notes/commits"))
        expected["refs/heads/main"] = newest
        expected["refs/notes/commits"] = git("rev-parse", "refs/notes/commits")
        step("fast-forward-pull", lambda: git("pull", "--ff-only", cwd=shallow))
        require(git("rev-parse", "HEAD", cwd=shallow) == newest, "pulled tip differs")
        step("delete-branch", lambda: git("push", url, ":refs/heads/café"))
        expected.pop("refs/heads/café")
        git("fetch", "origin", "refs/heads/*:refs/remotes/origin/*", cwd=shallow)
        # First materialize a tracking ref, then remove it remotely and prune it.
        git("push", url, "HEAD:refs/heads/prune-me")
        git("fetch", "origin", "refs/heads/*:refs/remotes/origin/*", cwd=shallow)
        git("push", url, ":refs/heads/prune-me")
        def prune():
            git("fetch", "--prune", "origin", "refs/heads/*:refs/remotes/origin/*", cwd=shallow)
            require(not git("for-each-ref", "refs/remotes/origin/prune-me", cwd=shallow), "pruned ref survived")
        step("fetch-prune", prune)
        mirror = args.work_dir / "mirror.git"
        git("clone", "--mirror", url, str(mirror), cwd=args.work_dir)
        require(refs(mirror, token) == expected, "mirror clone inventory differs")
        step("mirror-push", lambda: git("push", "--mirror", mirror_url, cwd=mirror))
        invalid = benchmark.Client(args.base_url, "intentionally-invalid-test-token", args.timeout)
        try:
            def refused():
                status, _ = invalid.request(f"/{record['repositories'][0]['owner']}/{record['repositories'][0]['name']}.git/info/refs?service=git-upload-pack")
                require(status == 401, "invalid credential was not refused with HTTP 401")
            step("invalid-credential-refusal", refused)
        finally:
            invalid.close()
        record.update(complete=True, refs=expected, payload_sha256=hashlib.sha256(payload).hexdigest(),
                      note="retained critical note")
        step("exact-v0-v2-mirror-fsck", lambda: verify_receipt(record, args.base_url,
             args.work_dir / "initial-verification", client, token))
        return record
    except BaseException as error:
        record.update(complete=False, error=type(error).__name__)
        raise
    finally:
        benchmark.save(args.receipt, record)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", required=True)
    parser.add_argument("--work-dir", type=Path, required=True)
    parser.add_argument("--receipt", type=Path, required=True)
    parser.add_argument("--timeout", type=benchmark.positive, default=30)
    commands = parser.add_subparsers(dest="command", required=True)
    create_parser = commands.add_parser("seed")
    create_parser.add_argument("--binary", type=Path, required=True)
    verify_parser = commands.add_parser("verify")
    verify_parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    token = os.environ.get("CANOPY_GIT_TOKEN")
    if not token:
        parser.error("CANOPY_GIT_TOKEN is required")
    args.base_url = args.base_url.rstrip("/")
    client = benchmark.Client(args.base_url, token, args.timeout)
    try:
        if args.command == "seed":
            result = seed(args, client, token)
        else:
            require(not args.output.exists(), "verification requires a new output")
            receipt_digest = benchmark.file_sha256(args.receipt)
            result = verify_receipt(json.loads(args.receipt.read_text()), args.base_url,
                                    args.work_dir, client, token)
            require(benchmark.file_sha256(args.receipt) == receipt_digest, "receipt changed during verification")
            result["receipt_sha256"] = receipt_digest
            benchmark.save(args.output, result)
        print(json.dumps(result, indent=2))
    finally:
        client.close()


if __name__ == "__main__":
    main()
