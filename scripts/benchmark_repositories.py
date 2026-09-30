#!/usr/bin/env python3
"""Seed disposable repositories and measure scheduled HTTP and stock-Git work.

Use CANOPY_GIT_TOKEN for authentication. Reports contain no credentials. This
measures repository creation, metadata, Git v2 capabilities, stock-Git ref listing, clone, fetch, pull, unique-ref push or
direct-basic LFS transfers. Seed and verify use stock Git for a declared corpus
sample. Incremental workloads require an opt-in two-commit corpus. Production
throughput still needs separate qualification.
"""

import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import hashlib
import http.client
import json
import math
import os
from pathlib import Path
import random
import re
import subprocess
import tempfile
import threading
import time
from urllib.parse import quote, urlsplit
import uuid


class Client:
    def __init__(self, base, token, timeout):
        url = urlsplit(base)
        if url.scheme not in ("http", "https") or not url.hostname or url.username or url.password or url.query or url.fragment:
            raise ValueError("base URL must be HTTP(S), without credentials, query or fragment")
        self.connection_type = http.client.HTTPSConnection if url.scheme == "https" else http.client.HTTPConnection
        self.base_url = base.rstrip("/")
        self.host, self.port, self.prefix = url.hostname, url.port, url.path.rstrip("/")
        self.token, self.timeout = token, timeout
        self.local = threading.local()
        self.connections = []
        self.lock = threading.Lock()

    def connection(self):
        connection = getattr(self.local, "connection", None)
        if connection is None:
            connection = self.connection_type(self.host, self.port, timeout=self.timeout)
            self.local.connection = connection
            with self.lock:
                self.connections.append(connection)
        return connection

    def request(self, path, payload=None, *, git=False, request_id=None):
        connection = self.connection()
        headers = {"Authorization": f"Bearer {self.token}", "Content-Type": "application/json"}
        if request_id is not None:
            headers["X-Request-ID"] = request_id
        if git:
            headers["Git-Protocol"] = "version=2"
        try:
            connection.request("GET" if payload is None else "POST", self.prefix + path,
                               body=None if payload is None else json.dumps(payload), headers=headers)
            response = connection.getresponse()
            body = response.read(2 * 1024 * 1024 + 1)
            if len(body) > 2 * 1024 * 1024:
                raise ValueError("benchmark response exceeds 2 MiB")
            return response.status, body
        except BaseException:
            # Reuse this bounded per-thread connection object after reconnecting.
            connection.close()
            raise

    def lfs_put(self, path, content, request_id=None):
        connection = self.connection()
        headers = {"Authorization": f"Bearer {self.token}",
                   "Content-Type": "application/octet-stream",
                   "Content-Length": str(len(content))}
        if request_id is not None:
            headers["X-Request-ID"] = request_id
        try:
            connection.request("PUT", self.prefix + path, body=content, headers=headers)
            response = connection.getresponse()
            if len(response.read(2 * 1024 * 1024 + 1)) > 2 * 1024 * 1024:
                raise ValueError("LFS upload response exceeds 2 MiB")
            return response.status
        except BaseException:
            connection.close()
            raise

    def lfs_get(self, path, expected_size, request_id=None):
        connection = self.connection()
        headers = {"Authorization": f"Bearer {self.token}"}
        if request_id is not None:
            headers["X-Request-ID"] = request_id
        try:
            connection.request("GET", self.prefix + path, headers=headers)
            response = connection.getresponse()
            if response.status != 200:
                if len(response.read(2 * 1024 * 1024 + 1)) > 2 * 1024 * 1024:
                    raise ValueError("LFS error response exceeds 2 MiB")
                return response.status, 0, None
            digest = hashlib.sha256()
            size = 0
            while chunk := response.read(1024 * 1024):
                size += len(chunk)
                if size > expected_size:
                    raise ValueError("LFS response exceeds declared size")
                digest.update(chunk)
            return response.status, size, digest.hexdigest()
        except BaseException:
            connection.close()
            raise

    def close(self):
        for connection in self.connections:
            connection.close()


def git_result(*args, cwd, token, timeout=120, request_id=None):
    """Run stock Git with isolated configuration; caller may inspect rejection."""
    environment = {key: value for key, value in os.environ.items()
                   if not key.startswith(("GIT_", "AWS_", "RUSTFS_", "CANOPY_"))}
    environment.update(GIT_TERMINAL_PROMPT="0", GIT_CONFIG_NOSYSTEM="1",
                       GIT_CONFIG_GLOBAL=os.devnull, GIT_CONFIG_COUNT="2",
                       GIT_CONFIG_KEY_0="credential.helper", GIT_CONFIG_VALUE_0="",
                       GIT_CONFIG_KEY_1="http.extraHeader",
                       GIT_CONFIG_VALUE_1=f"Authorization: Bearer {token}")
    if request_id is not None:
        environment.update(GIT_CONFIG_COUNT="3", GIT_CONFIG_KEY_2="http.extraHeader",
                           GIT_CONFIG_VALUE_2=f"X-Request-ID: {request_id}")
    return subprocess.run(["git", *args], cwd=cwd, env=environment,
                          capture_output=True, timeout=timeout, check=False)


def git(*args, cwd, token, timeout=120, request_id=None):
    result = git_result(*args, cwd=cwd, token=token, timeout=timeout, request_id=request_id)
    if result.returncode:
        raise RuntimeError(f"Git {args[0]} failed (exit {result.returncode})")
    return result.stdout.strip().decode()


def save(path, value):
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2) + "\n")
    temporary.replace(path)


def file_sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while block := source.read(1024 * 1024):
            digest.update(block)
    return digest.hexdigest()


def canonical_repository_uuid(value):
    if not isinstance(value, str):
        return False
    try:
        identifier = uuid.UUID(value)
    except ValueError:
        return False
    return (str(identifier) == value and identifier.variant == uuid.RFC_4122
            and identifier.version is not None and 1 <= identifier.version <= 8)


def seed(args, client, token):
    if args.manifest.exists():
        raise ValueError("seed requires a new manifest; use verify for an existing corpus")
    args.work_dir.mkdir(parents=True, exist_ok=False)
    generator = random.Random(args.seed)
    selected = set(generator.sample(range(args.repositories), min(args.populated, args.repositories)))
    prefix = f"density-{uuid.uuid4().hex[:12]}"
    manifest = {"version": 2 if args.incremental_fixture else 1,
                "complete": False, "prefix": prefix, "seed": args.seed,
                "requested_repositories": args.repositories, "repositories": [],
                "git_version": git("--version", cwd=args.work_dir, token=token)}
    started = time.monotonic()
    try:
        for index in range(args.repositories):
            name = f"{prefix}-{index:05d}"
            creating = time.monotonic()
            status, body = client.request("/api/repositories", {"name": name})
            create_ms = (time.monotonic() - creating) * 1000
            if status != 200:
                raise RuntimeError(f"repository seed failed with HTTP {status}")
            entry = json.loads(body)
            if entry["name"] != name or not re.fullmatch(r"[A-Za-z0-9_-]+", entry["owner"]):
                raise RuntimeError("seed response has an invalid repository identity")
            uuid.UUID(entry["repository_id"])
            record = {key: entry[key] for key in ("name", "owner", "repository_id")}
            record["create_ms"] = round(create_ms, 3)
            record["commit"] = None
            if args.incremental_fixture:
                record["base_commit"] = None
            manifest["repositories"].append(record)
            if index in selected:
                local = args.work_dir / name
                git("init", "-b", "main", str(local), cwd=args.work_dir, token=token)
                git("config", "user.name", "Canopy Density", cwd=local, token=token)
                git("config", "user.email", "density@example.invalid", cwd=local, token=token)
                content = f"{name}\nseed={args.seed}\n".encode()
                (local / "README.md").write_bytes(content)
                git("add", "README.md", cwd=local, token=token)
                git("commit", "-m", "Density fixture", cwd=local, token=token)
                if args.incremental_fixture:
                    record["base_commit"] = git("rev-parse", "HEAD", cwd=local, token=token)
                    git("branch", "benchmark-base", cwd=local, token=token)
                    increment = f"{name}\nincremental={args.seed}\n".encode()
                    (local / "incremental.txt").write_bytes(increment)
                    git("add", "incremental.txt", cwd=local, token=token)
                    git("commit", "-m", "Incremental fixture", cwd=local, token=token)
                    record["incremental_sha256"] = hashlib.sha256(increment).hexdigest()
                url = f"{args.base_url.rstrip('/')}/{entry['owner']}/{name}.git"
                refs = (["HEAD:refs/heads/main", "benchmark-base:refs/heads/benchmark-base"]
                        if args.incremental_fixture else ["HEAD:refs/heads/main"])
                git("push", url, *refs, cwd=local, token=token)
                record["commit"] = git("rev-parse", "HEAD", cwd=local, token=token)
                record["readme_sha256"] = hashlib.sha256(content).hexdigest()
                if args.lfs_fixture_bytes:
                    lfs_body = hashlib.shake_256(
                        f"{prefix}/{name}/{args.seed}".encode()).digest(args.lfs_fixture_bytes)
                    lfs_oid = hashlib.sha256(lfs_body).hexdigest()
                    path = f"/{entry['owner']}/{name}.git/info/lfs/objects/{lfs_oid}"
                    if client.lfs_put(path, lfs_body) != 200:
                        raise RuntimeError("LFS seed upload failed")
                    record["lfs_oid"] = lfs_oid
                    record["lfs_size"] = len(lfs_body)
            if (index + 1) % 25 == 0:
                save(args.manifest, manifest)
                print(f"seeded {index + 1}/{args.repositories} repositories", flush=True)
        manifest["complete"] = True
    finally:
        manifest["seed_seconds"] = round(time.monotonic() - started, 3)
        save(args.manifest, manifest)
    return {"seeded": len(manifest["repositories"]), "populated": len(selected),
            "seconds": manifest["seed_seconds"]}


def corpus(path):
    manifest = json.loads(path.read_text())
    entries = manifest["repositories"]
    if manifest["version"] not in (1, 2) or not manifest["complete"] or len(entries) != manifest["requested_repositories"]:
        raise ValueError("manifest must contain a complete version-1 or version-2 corpus")
    for entry in entries:
        if any(not re.fullmatch(r"[A-Za-z0-9_-]+", entry[key]) for key in ("name", "owner")):
            raise ValueError("manifest contains invalid repository names")
        uuid.UUID(entry["repository_id"])
        if "lfs_oid" in entry:
            oid, size = entry["lfs_oid"], entry.get("lfs_size")
            if not isinstance(oid, str) or not re.fullmatch(r"[0-9a-f]{64}", oid):
                raise ValueError("corpus has an invalid LFS object ID")
            if not isinstance(size, int) or isinstance(size, bool) or size < 1:
                raise ValueError("corpus has an invalid LFS object size")
        if manifest["version"] == 2 and entry["commit"] is not None:
            base_commit = entry.get("base_commit")
            digest = entry.get("incremental_sha256")
            if not isinstance(base_commit, str) or not re.fullmatch(r"[0-9a-f]{40}|[0-9a-f]{64}", base_commit):
                raise ValueError("version-2 corpus has an invalid base commit")
            if not isinstance(digest, str) or not re.fullmatch(r"[0-9a-f]{64}", digest):
                raise ValueError("version-2 corpus has an invalid incremental digest")
    return manifest


def verify(args, client, token):
    manifest = corpus(args.manifest)
    args.work_dir.mkdir(parents=True, exist_ok=False)
    def verify_one(entry):
        request_id = str(uuid.uuid4())
        context = f"{entry['name']} ({entry['repository_id']}), request_id={request_id}"
        try:
            status, body = client.request(f"/api/repositories/{quote(entry['name'])}",
                                          request_id=request_id)
        except (TimeoutError, OSError, ValueError, http.client.HTTPException) as error:
            # Include no response body, authorization header or provider secrets.
            raise RuntimeError(f"identity request failed ({type(error).__name__}) for {context}") from None
        if status != 200:
            raise RuntimeError(f"HTTP {status} verifying repository {context}")
        try:
            decoded = json.loads(body)
            identity = decoded["repository_id"]
            if not isinstance(identity, str):
                raise ValueError("identity is not a string")
            uuid.UUID(identity)
        except (ValueError, KeyError, TypeError, UnicodeDecodeError):
            raise RuntimeError(f"malformed identity response for {context}") from None
        if identity != entry["repository_id"]:
            raise RuntimeError(f"restored repository identity differs from manifest for {context}")
        if entry["commit"] is None:
            return 0
        if "lfs_oid" in entry:
            path = f"/{entry['owner']}/{entry['name']}.git/info/lfs/objects/{entry['lfs_oid']}"
            status, size, digest = client.lfs_get(path, entry["lfs_size"])
            if (status, size, digest) != (200, entry["lfs_size"], entry["lfs_oid"]):
                raise RuntimeError("restored LFS bytes differ from manifest")
        for protocol in ("0", "2"):
            clone = args.work_dir / f"{entry['name']}-v{protocol}"
            url = f"{args.base_url.rstrip('/')}/{entry['owner']}/{entry['name']}.git"
            git("-c", f"protocol.version={protocol}", "clone", url, str(clone), cwd=args.work_dir, token=token)
            if git("rev-parse", "HEAD", cwd=clone, token=token) != entry["commit"]:
                raise RuntimeError("restored commit differs from manifest")
            if hashlib.sha256((clone / "README.md").read_bytes()).hexdigest() != entry["readme_sha256"]:
                raise RuntimeError("restored file bytes differ from manifest")
            if manifest["version"] == 2:
                if git("rev-parse", "refs/remotes/origin/benchmark-base", cwd=clone,
                       token=token) != entry["base_commit"]:
                    raise RuntimeError("restored incremental base differs from manifest")
                if hashlib.sha256((clone / "incremental.txt").read_bytes()).hexdigest() != entry["incremental_sha256"]:
                    raise RuntimeError("restored incremental bytes differ from manifest")
            git("fsck", "--strict", "--full", cwd=clone, token=token)
        return 1

    populated = 0
    entries = manifest["repositories"]
    # Submit one bounded batch at a time: even a 10,000-entry manifest must not
    # allocate 10,000 queued futures or hide a failed verification behind them.
    with ThreadPoolExecutor(max_workers=args.concurrency) as executor:
        for start in range(0, len(entries), args.concurrency):
            populated += sum(executor.map(verify_one, entries[start:start + args.concurrency]))
            completed = min(start + args.concurrency, len(entries))
            if completed % 25 == 0 or completed == len(entries):
                print(f"verified {completed}/{len(entries)} identities", flush=True)
    return {"verified_repositories": len(manifest["repositories"]), "git_v0_v2_samples": populated}


def write_runs(paths, manifest_path, entries):
    """Validate all acknowledgement evidence before making recovery requests."""
    manifest_digest = file_sha256(manifest_path)
    runs, identifiers, total = [], set(), 0
    for path in paths:
        with path.open("rb") as source:
            report_bytes = source.read(2 * 1024 * 1024 + 1)
        if len(report_bytes) > 2 * 1024 * 1024:
            raise ValueError("write report exceeds 2 MiB")
        report = json.loads(report_bytes)
        if not isinstance(report, dict):
            raise ValueError("write report must be a JSON object")
        operation = report.get("operation")
        if report.get("version") != 1 or operation not in ("push_branch", "lfs_upload"):
            raise ValueError("write verification requires a push_branch or lfs_upload report")
        if report.get("manifest_sha256") != manifest_digest:
            raise ValueError("write report does not match the corpus digest")
        scheduled, outcomes = report.get("scheduled"), report.get("outcomes")
        if (not isinstance(scheduled, int) or isinstance(scheduled, bool)
                or not 1 <= scheduled <= 1_000_000 or not isinstance(outcomes, dict)
                or any(not isinstance(value, int) or isinstance(value, bool) or value < 0
                       for value in outcomes.values())
                or sum(outcomes.values()) != scheduled
                or report.get("failed_arrivals") != scheduled - outcomes.get("ok", 0)):
            raise ValueError("write report has inconsistent arrival accounting")
        total += scheduled
        if total > 1_000_000:
            raise ValueError("write verification is limited to one million total arrivals")
        run_id = report.get("push_run_id" if operation == "push_branch" else "lfs_run_id")
        if not isinstance(run_id, str) or not re.fullmatch(r"[0-9a-f]{32}", run_id):
            raise ValueError("write report has an invalid run identifier")
        if (operation, run_id) in identifiers:
            raise ValueError("duplicate write run")
        identifiers.add((operation, run_id))
        if operation == "push_branch":
            commit = report.get("push_commit")
            if not isinstance(commit, str) or not re.fullmatch(r"[0-9a-f]{40}|[0-9a-f]{64}", commit):
                raise ValueError("write report has an invalid push commit")
        else:
            size = report.get("lfs_size_bytes")
            if not isinstance(size, int) or isinstance(size, bool) or not 1 <= size <= 16 * 1024 * 1024:
                raise ValueError("write report has an invalid LFS size")
        samples_path = path.with_suffix(".samples.jsonl")
        if file_sha256(samples_path) != report.get("samples_sha256"):
            raise ValueError("write sample digest differs or is absent")
        seen, observed, acknowledged, sample_digest = set(), Counter(), [], hashlib.sha256()
        with samples_path.open("rb") as samples:
            for line in iter(lambda: samples.readline(16 * 1024 + 1), b""):
                if len(line) > 16 * 1024:
                    raise ValueError("write sample exceeds 16 KiB")
                sample_digest.update(line)
                sample = json.loads(line)
                if not isinstance(sample, dict):
                    raise ValueError("write sample must be a JSON object")
                sequence = sample.get("sequence")
                if (not isinstance(sequence, int) or isinstance(sequence, bool)
                        or not 0 <= sequence < scheduled or sequence in seen
                        or not isinstance(sample.get("repository_id"), str)
                        or sample.get("repository_id") not in entries
                        or not isinstance(sample.get("result"), str)
                        or sample.get("result") not in outcomes):
                    raise ValueError("write sample has an invalid identity, sequence or outcome")
                seen.add(sequence)
                observed[sample["result"]] += 1
                if sample["result"] == "ok":
                    if operation == "lfs_upload" and (
                            not isinstance(sample.get("lfs_oid"), str)
                            or not re.fullmatch(r"[0-9a-f]{64}", sample["lfs_oid"])):
                        raise ValueError("acknowledged LFS sample has an invalid object ID")
                    acknowledged.append(sample)
        if sample_digest.hexdigest() != report["samples_sha256"]:
            raise ValueError("write sample digest changed during validation")
        if len(seen) != scheduled or observed != Counter(outcomes):
            raise ValueError("write samples do not match the report's arrival accounting")
        runs.append((report, acknowledged, hashlib.sha256(report_bytes).hexdigest()))
    if not any(acknowledged for _, acknowledged, _ in runs):
        raise ValueError("write verification requires at least one acknowledged arrival")
    return runs


def verify_writes(args, client, token):
    """Read every acknowledged load-test write, without retrying failed arrivals.

    The caller must establish the desired owner restart/recovery beforehand;
    this read-only verifier neither kills a node nor proves that it restarted.
    """
    manifest = corpus(args.manifest)
    entries = {entry["repository_id"]: entry for entry in manifest["repositories"]}
    if len(entries) != len(manifest["repositories"]):
        raise ValueError("write verification requires unique repository identities")
    if args.output.exists():
        raise ValueError("write verification requires a new output path")
    runs = write_runs(args.reports, args.manifest, entries)
    args.work_dir.mkdir(parents=True, exist_ok=False)
    refs, lfs, repositories = 0, 0, set()
    for index, (report, acknowledged, _) in enumerate(runs):
        if report["operation"] == "lfs_upload":
            for sample in acknowledged:
                entry = entries[sample["repository_id"]]
                oid, size = sample["lfs_oid"], report["lfs_size_bytes"]
                path = f"/{entry['owner']}/{entry['name']}.git/info/lfs/objects/{oid}"
                if client.lfs_get(path, size) != (200, size, oid):
                    raise RuntimeError("acknowledged LFS object is missing or differs")
                lfs += 1
            continue
        by_repository = {}
        for sample in acknowledged:
            by_repository.setdefault(sample["repository_id"], []).append(
                f"refs/heads/canopy-benchmark/{report['push_run_id']}/{sample['sequence']:07d}")
        commit = report["push_commit"]
        algorithm = "sha256" if len(commit) == 64 else "sha1"
        body = f"benchmark run {report['push_run_id']}\n".encode()
        expected_blob = hashlib.new(algorithm, f"blob {len(body)}\0".encode() + body).hexdigest()
        for repository_id, references in by_repository.items():
            entry = entries[repository_id]
            url = f"{client.base_url}/{entry['owner']}/{entry['name']}.git"
            for protocol in ("0", "2"):
                local = args.work_dir / f"run-{index}-{repository_id}-v{protocol}.git"
                git("init", "--bare", f"--object-format={algorithm}", str(local),
                    cwd=args.work_dir, token=token, timeout=args.git_timeout)
                for start in range(0, len(references), 128):
                    page = references[start:start + 128]
                    try:
                        git("-c", f"protocol.version={protocol}", "fetch", "--quiet", "--no-tags", url,
                            *[f"{reference}:{reference}" for reference in page], cwd=local,
                            token=token, timeout=args.git_timeout, request_id=str(uuid.uuid4()))
                    except RuntimeError as error:
                        raise RuntimeError("could not fetch acknowledged Git refs") from error
                    for reference in page:
                        if git("rev-parse", reference, cwd=local, token=token,
                               timeout=args.git_timeout) != commit:
                            raise RuntimeError("acknowledged Git ref differs")
                if git("rev-parse", f"{commit}:README.md", cwd=local, token=token,
                       timeout=args.git_timeout) != expected_blob:
                    raise RuntimeError("acknowledged Git body differs")
                git("fsck", "--strict", "--full", cwd=local, token=token, timeout=args.git_timeout)
            refs += len(references)
            repositories.add(repository_id)
    result = {"version": 1, "manifest_sha256": file_sha256(args.manifest),
              "write_report_sha256": [digest for _, _, digest in runs],
              "verified_git_refs": refs, "verified_git_repositories": len(repositories),
              "git_protocols": [0, 2] if refs else [], "verified_lfs_objects": lfs,
              "unacknowledged_arrivals_not_asserted": sum(report["failed_arrivals"] for report, _, _ in runs),
              "owner_recovery": "not established by this verifier; caller must record owner restart evidence"}
    save(args.output, result)
    return result


def percentiles(values):
    values = sorted(values)
    return {name: None if not values else round(values[min(len(values) - 1, math.ceil(q * len(values)) - 1)], 3)
            for name, q in (("p50", .5), ("p95", .95), ("p99", .99), ("max", 1))}


def creation_runs(paths, manifest_path):
    """Validate the complete, digest-bound arrival ledger before recovery I/O."""
    manifest_digest = file_sha256(manifest_path)
    runs, identifiers, names, identities = [], set(), set(), set()
    total = 0
    for path in paths:
        with path.open("rb") as source:
            raw = source.read(2 * 1024 * 1024 + 1)
        if len(raw) > 2 * 1024 * 1024:
            raise ValueError("creation report exceeds 2 MiB")
        report = json.loads(raw)
        if not isinstance(report, dict) or report.get("version") != 1 or report.get("operation") != "create":
            raise ValueError("creation verification requires a create report")
        run_id, scheduled, outcomes = report.get("create_run_id"), report.get("scheduled"), report.get("outcomes")
        failed = report.get("failed_arrivals")
        if (not isinstance(run_id, str) or not re.fullmatch(r"[0-9a-f]{32}", run_id)
                or run_id in identifiers or report.get("manifest_sha256") != manifest_digest):
            raise ValueError("creation report has an invalid identity or provenance")
        identifiers.add(run_id)
        if (not isinstance(scheduled, int) or isinstance(scheduled, bool) or not 1 <= scheduled <= 1_000_000
                or not isinstance(outcomes, dict)
                or any(not isinstance(value, int) or isinstance(value, bool) or value < 0 for value in outcomes.values())
                or sum(outcomes.values()) != scheduled
                or not isinstance(failed, int) or isinstance(failed, bool)
                or failed != scheduled - outcomes.get("ok", 0)):
            raise ValueError("creation report has inconsistent arrivals")
        total += scheduled
        if total > 1_000_000:
            raise ValueError("creation verification exceeds one million arrivals")
        samples_path = path.with_suffix(".samples.jsonl")
        if file_sha256(samples_path) != report.get("samples_sha256"):
            raise ValueError("creation samples have changed")
        seen, observed, acknowledged, digest = set(), Counter(), [], hashlib.sha256()
        with samples_path.open("rb") as samples:
            for line in iter(lambda: samples.readline(16 * 1024 + 1), b""):
                if len(line) > 16 * 1024:
                    raise ValueError("creation sample exceeds 16 KiB")
                digest.update(line)
                sample = json.loads(line)
                if not isinstance(sample, dict):
                    raise ValueError("creation sample must be an object")
                sequence, result = sample.get("sequence"), sample.get("result")
                if (not isinstance(sequence, int) or isinstance(sequence, bool)
                        or not 0 <= sequence < scheduled or sequence in seen
                        or not isinstance(result, str) or result not in outcomes
                        or sample.get("created_name") != f"create-{run_id}-{sequence:07d}"):
                    raise ValueError("creation sample has an invalid arrival")
                seen.add(sequence)
                observed[result] += 1
                if result == "ok":
                    identifier = sample.get("created_repository_id")
                    if (not canonical_repository_uuid(identifier)
                            or identifier in identities or sample["created_name"] in names):
                        raise ValueError("creation receipt has an invalid or duplicate repository identity")
                    names.add(sample["created_name"])
                    identities.add(identifier)
                    acknowledged.append((sample["created_name"], identifier))
        if (digest.hexdigest() != report["samples_sha256"] or len(seen) != scheduled
                or observed != Counter(outcomes)):
            raise ValueError("creation samples do not match the report")
        runs.append((report, acknowledged, hashlib.sha256(raw).hexdigest()))
    if not identities:
        raise ValueError("creation verification requires an acknowledged arrival")
    return runs


def verify_creations(args, client, token):
    corpus(args.manifest)
    if args.output.exists():
        raise ValueError("creation verification requires a new output")
    runs = creation_runs(args.reports, args.manifest)
    count = 0
    for _, acknowledged, _ in runs:
        for name, identifier in acknowledged:
            status, body = client.request(f"/api/repositories/{name}", request_id=str(uuid.uuid4()))
            if status != 200:
                raise RuntimeError(f"acknowledged repository recovery failed with HTTP {status}: {name}")
            decoded = json.loads(body)
            if not isinstance(decoded, dict) or decoded.get("repository_id") != identifier or decoded.get("name") != name:
                raise RuntimeError(f"acknowledged repository recovery differs: {name}")
            count += 1
    result = {"version": 1, "verified_creations": count,
              "manifest_sha256": file_sha256(args.manifest),
              "creation_report_sha256": [digest for _, _, digest in runs],
              "unacknowledged_arrivals_not_asserted": sum(report["failed_arrivals"] for report, _, _ in runs),
              "owner_recovery": "not established by this verifier; caller must record owner restart evidence"}
    save(args.output, result)
    return result


def prepare_incremental(active, clients, token, work_dir, timeout):
    """Stage base-only Git repositories before the arrival clock starts."""
    root = work_dir / "incremental-templates"
    root.mkdir()
    templates = {}
    for index, entry in enumerate(active):
        destination = root / entry["repository_id"]
        ingress = clients[index % len(clients)]
        url = f"{ingress.base_url}/{entry['owner']}/{entry['name']}.git"
        git("init", "--bare", "-b", "benchmark-base", str(destination),
            cwd=root, token=token, timeout=timeout)
        git("fetch", "--quiet", url,
            "refs/heads/benchmark-base:refs/heads/benchmark-base",
            cwd=destination, token=token, timeout=timeout)
        if git("rev-parse", "HEAD", cwd=destination, token=token,
               timeout=timeout) != entry["base_commit"]:
            raise RuntimeError("incremental template differs from manifest")
        templates[entry["repository_id"]] = destination
        if (index + 1) % 25 == 0:
            print(f"prepared {index + 1}/{len(active)} incremental clients", flush=True)
    return templates


def git_transfer(operation, url, entry, token, work_dir, timeout, request_id,
                 template=None):
    """Run one disposable stock-Git transfer and validate its advertised tip."""
    if operation == "ls_remote":
        listing = git("-c", "protocol.version=2", "ls-remote", url, "refs/heads/main",
                      cwd=work_dir, token=token, timeout=timeout, request_id=request_id)
        return listing == f"{entry['commit']}\trefs/heads/main"
    with tempfile.TemporaryDirectory(prefix="canopy-git-read-", dir=work_dir) as temporary:
        destination = Path(temporary) / "repo"
        if operation == "clone":
            git("clone", "--quiet", url, str(destination), cwd=temporary, token=token,
                timeout=timeout, request_id=request_id)
            tip = git("rev-parse", "HEAD", cwd=destination, token=token, timeout=timeout)
            readme = destination / "README.md"
            return tip == entry["commit"] and readme.is_file() and (
                hashlib.sha256(readme.read_bytes()).hexdigest() == entry["readme_sha256"])
        if operation == "cold_fetch":
            git("init", "--bare", "--quiet", str(destination), cwd=temporary,
                token=token, timeout=timeout)
            git("fetch", "--quiet", url, "refs/heads/main", cwd=destination,
                token=token, timeout=timeout, request_id=request_id)
            return git("rev-parse", "FETCH_HEAD", cwd=destination, token=token,
                       timeout=timeout) == entry["commit"]
        if operation in ("incremental_fetch", "incremental_pull"):
            if template is None:
                raise ValueError("incremental transfer requires a prepared base")
            clone_args = (["clone", "--quiet", "--shared", "--bare"]
                          if operation == "incremental_fetch" else
                          ["clone", "--quiet", "--shared"])
            git(*clone_args, str(template), str(destination), cwd=temporary,
                token=token, timeout=timeout)
            if operation == "incremental_fetch":
                git("fetch", "--quiet", url, "refs/heads/main", cwd=destination,
                    token=token, timeout=timeout, request_id=request_id)
                return git("rev-parse", "FETCH_HEAD", cwd=destination,
                           token=token, timeout=timeout) == entry["commit"]
            git("pull", "--quiet", "--ff-only", url, "refs/heads/main",
                cwd=destination, token=token, timeout=timeout, request_id=request_id)
            incremental = destination / "incremental.txt"
            return (git("rev-parse", "HEAD", cwd=destination, token=token,
                        timeout=timeout) == entry["commit"] and
                    incremental.is_file() and
                    hashlib.sha256(incremental.read_bytes()).hexdigest() == entry["incremental_sha256"])
        raise ValueError("unsupported Git transfer operation")


def push_branch(url, source, reference, token, timeout, request_id):
    """Publish one unique benchmark ref; Git checks the receive-pack report."""
    git("push", "--quiet", url, f"HEAD:{reference}", cwd=source, token=token,
        timeout=timeout, request_id=request_id)


def measure(args, client, token):
    driver_sha256 = file_sha256(Path(__file__))
    clients = client if isinstance(client, list) else [client]
    manifest = corpus(args.manifest)
    incremental = args.operation in ("incremental_fetch", "incremental_pull")
    git_read = args.operation in ("ls_remote", "clone", "cold_fetch") or incremental
    git_write = args.operation == "push_branch"
    creation = args.operation == "create"
    git_operation = git_read or git_write
    lfs_download = args.operation == "lfs_download"
    lfs_upload = args.operation == "lfs_upload"
    if lfs_upload and (args.lfs_bytes == 0 or
                       args.lfs_bytes * args.concurrency > 256 * 1024 * 1024):
        raise ValueError("LFS uploads require positive bytes and at most 256 MiB in-flight payloads")
    eligible = ([entry for entry in manifest["repositories"] if entry.get("lfs_oid") is not None]
                if lfs_download else
                [entry for entry in manifest["repositories"] if entry.get("base_commit") is not None]
                if incremental else
                [entry for entry in manifest["repositories"] if entry["commit"] is not None]
                if git_read else manifest["repositories"])
    if not creation and (args.active_repositories is None or args.active_repositories > len(eligible)):
        raise ValueError("active repository count exceeds eligible corpus")
    total = math.ceil(args.duration * args.rate)
    if total > 1_000_000:
        raise ValueError("one run is limited to one million scheduled arrivals")
    samples_path = args.output.with_suffix(".samples.jsonl")
    if args.output.exists() or samples_path.exists():
        raise ValueError("run requires new output paths")
    if git_operation:
        if args.work_dir is None:
            raise ValueError("stock-Git runs require --work-dir")
        args.work_dir.mkdir(parents=True, exist_ok=False)
    push_run_id = uuid.uuid4().hex if git_write else None
    create_run_id = uuid.uuid4().hex if creation else None
    lfs_run_id = uuid.uuid4().hex if lfs_upload else None
    lfs_tail = (hashlib.shake_256(lfs_run_id.encode()).digest(max(0, args.lfs_bytes - 32))
                if lfs_upload else None)
    source = args.work_dir / "source" if git_write else None
    push_commit = None
    if git_write:
        git("init", "-b", "main", str(source), cwd=args.work_dir, token=token)
        git("config", "user.name", "Canopy Benchmark", cwd=source, token=token)
        git("config", "user.email", "benchmark@example.invalid", cwd=source, token=token)
        (source / "README.md").write_text(f"benchmark run {push_run_id}\n")
        git("add", "README.md", cwd=source, token=token)
        git("commit", "-m", "Benchmark fixture", cwd=source, token=token)
        push_commit = git("rev-parse", "HEAD", cwd=source, token=token)
    generator = random.Random(args.seed)
    active = [] if creation else generator.sample(eligible, args.active_repositories)
    templates = {}
    setup_started = time.monotonic()
    if incremental:
        templates = prepare_incremental(active, clients, token, args.work_dir,
                                        args.git_timeout)
    setup_seconds = round(time.monotonic() - setup_started, 3)
    # Skew has a declared hot tenth, not an implicit warm-cache assumption.
    hot = active[:max(1, len(active) // 10)]
    counts, latencies, service_times, dispatch_times = Counter(), [], [], []
    completed_in_window = 0
    created_identities = set()
    ingress_counts = [Counter() for _ in clients]
    ingress_latencies = [[] for _ in clients]
    ingress_service_times = [[] for _ in clients]
    slots = threading.BoundedSemaphore(args.concurrency)
    lock = threading.Lock()
    started = time.monotonic()
    started_at = datetime.now(timezone.utc).isoformat()
    with samples_path.open("x") as samples:
        def record(sample):
            nonlocal completed_in_window
            with lock:
                if creation and sample["result"] == "ok":
                    identifier = sample["created_repository_id"]
                    if identifier in created_identities:
                        sample["result"] = "invalid_response"
                        sample["receipt_error"] = "duplicate_created_identity"
                        sample["created_repository_id"] = None
                    else:
                        created_identities.add(identifier)
                counts[sample["result"]] += 1
                if (sample["result"] == "ok" and
                        sample.get("completion_offset_seconds", math.inf) <= args.duration):
                    completed_in_window += 1
                ingress_counts[sample["ingress_index"]][sample["result"]] += 1
                if sample["elapsed_ms"] is not None:
                    latencies.append(sample["elapsed_ms"])
                    service_times.append(sample["service_ms"])
                    dispatch_times.append(sample["dispatch_delay_ms"])
                    ingress_latencies[sample["ingress_index"]].append(sample["elapsed_ms"])
                    ingress_service_times[sample["ingress_index"]].append(sample["service_ms"])
                samples.write(json.dumps(sample) + "\n")

        def execute(sequence, entry, scheduled):
            request_id = str(uuid.uuid4())
            ingress_index = sequence % len(clients)
            ingress = clients[ingress_index]
            dispatched = time.monotonic()
            result = "transport_error"
            uploaded_oid = None
            created_id = None
            created_name = f"create-{create_run_id}-{sequence:07d}" if creation else None
            try:
                if creation:
                    status, body = ingress.request("/api/repositories",
                        {"name": created_name}, request_id=request_id)
                    valid = False
                    if status == 200:
                        try:
                            decoded = json.loads(body)
                            identifier = decoded["repository_id"]
                            valid = (decoded["name"] == created_name and
                                     isinstance(decoded["owner"], str) and
                                     re.fullmatch(r"[A-Za-z0-9_-]+", decoded["owner"]) is not None and
                                     canonical_repository_uuid(identifier))
                            created_id = identifier if valid else None
                        except (ValueError, KeyError, TypeError):
                            pass
                elif args.operation == "metadata":
                    status, body = ingress.request(f"/api/repositories/{entry['name']}", request_id=request_id)
                    decoded = json.loads(body) if status == 200 else None
                    valid = isinstance(decoded, dict) and decoded.get("repository_id") == entry["repository_id"]
                elif args.operation == "refs":
                    status, body = ingress.request(f"/{entry['owner']}/{entry['name']}.git/info/refs?service=git-upload-pack", git=True, request_id=request_id)
                    valid = status == 200 and body.startswith(b"000eversion 2\n")
                elif git_read:
                    url = f"{ingress.base_url}/{entry['owner']}/{entry['name']}.git"
                    valid = git_transfer(args.operation, url, entry, token, args.work_dir,
                                         args.git_timeout, request_id,
                                         templates.get(entry["repository_id"]))
                elif git_write:
                    url = f"{ingress.base_url}/{entry['owner']}/{entry['name']}.git"
                    reference = f"refs/heads/canopy-benchmark/{push_run_id}/{sequence:07d}"
                    push_branch(url, source, reference, token, args.git_timeout, request_id)
                    valid = True
                elif lfs_download:
                    path = f"/{entry['owner']}/{entry['name']}.git/info/lfs/objects/{entry['lfs_oid']}"
                    status, size, digest = ingress.lfs_get(path, entry["lfs_size"], request_id)
                    valid = (status, size, digest) == (200, entry["lfs_size"], entry["lfs_oid"])
                elif lfs_upload:
                    marker = hashlib.sha256(f"{lfs_run_id}:{sequence}".encode()).digest()
                    body = (marker + lfs_tail)[:args.lfs_bytes]
                    uploaded_oid = hashlib.sha256(body).hexdigest()
                    path = f"/{entry['owner']}/{entry['name']}.git/info/lfs/objects/{uploaded_oid}"
                    status = ingress.lfs_put(path, body, request_id)
                    valid = status == 200
                else:
                    raise ValueError("unsupported benchmark operation")
                result = ("ok" if valid else
                          (f"http_{status}" if not git_operation and status != 200 else "invalid_response"))
            except subprocess.TimeoutExpired:
                result = "client_timeout"
            except RuntimeError:
                result = "git_error"
            except (OSError, ValueError, KeyError, http.client.HTTPException):
                pass
            finally:
                finished = time.monotonic()
                record({"sequence": sequence, "request_id": request_id, "repository_id": entry["repository_id"], "ingress_index": ingress_index, "result": result,
                        "lfs_oid": uploaded_oid,
                        "created_name": created_name, "created_repository_id": created_id,
                        "completion_offset_seconds": finished - started,
                        "elapsed_ms": (finished - scheduled) * 1000,
                        "service_ms": (finished - dispatched) * 1000,
                        "dispatch_delay_ms": (dispatched - scheduled) * 1000})
                slots.release()

        with ThreadPoolExecutor(max_workers=args.concurrency) as executor:
            for sequence in range(total):
                scheduled = started + sequence / args.rate
                delay = scheduled - time.monotonic()
                if delay > 0:
                    time.sleep(delay)
                population = hot if args.distribution == "skewed" and generator.random() < .9 else active
                entry = {"repository_id": None} if creation else generator.choice(population)
                if slots.acquire(blocking=False):
                    executor.submit(execute, sequence, entry, scheduled)
                else:
                    record({"sequence": sequence, "repository_id": entry["repository_id"],
                            "ingress_index": sequence % len(clients),
                            "created_name": f"create-{create_run_id}-{sequence:07d}" if creation else None,
                            "created_repository_id": None,
                            "result": "driver_busy", "elapsed_ms": None})
            # Observe the full offered-load window even when the last request
            # completes before its final inter-arrival interval expires.
            remaining = started + args.duration - time.monotonic()
            if remaining > 0:
                time.sleep(remaining)
    elapsed = time.monotonic() - started
    result = {"version": 1, "started_at_utc": started_at,
              "driver_sha256": driver_sha256,
              "corpus_repositories": len(manifest["repositories"]),
              "eligible_repositories": None if creation else len(eligible),
              "active_repositories": None if creation else len(active), "distribution": args.distribution,
              "acknowledged_created_repositories": len(created_identities) if creation else None,
              "operation": args.operation, "seed": args.seed,
              "create_run_id": create_run_id,
              "creation_population": "unique new names, not selected corpus identities" if creation else None,
              "git_discovery_kind": ("v2_capabilities_only" if args.operation == "refs" else
                                     "v2_ls_refs_main_tip" if args.operation == "ls_remote" else None),
              "offered_rps": args.rate, "schedule_seconds": args.duration,
              "elapsed_including_drain_seconds": round(elapsed, 3),
              "incremental_client_setup_seconds": setup_seconds if incremental else None,
              "concurrency": args.concurrency, "request_timeout_seconds": args.timeout,
              "git_timeout_seconds": args.git_timeout if git_operation else None,
              "push_run_id": push_run_id, "push_commit": push_commit,
              "lfs_run_id": lfs_run_id,
              "lfs_size_bytes": args.lfs_bytes if lfs_upload else None,
              "scheduled": total, "outcomes": dict(counts),
              "ingresses": [{"index": index, "outcomes": dict(outcomes),
                             "scheduled_latency_ms": percentiles(ingress_latencies[index]),
                             "service_ms": percentiles(ingress_service_times[index])}
                            for index, outcomes in enumerate(ingress_counts)],
              "failed_arrivals": total - counts["ok"],
              "successful_rps_including_drain": round(counts["ok"] / elapsed, 3),
              "successful_completions_in_schedule_window": completed_in_window,
              "successful_rps_in_schedule_window": round(completed_in_window / args.duration, 3),
              "error_fraction": (total - counts["ok"]) / total,
              "scheduled_latency_ms": percentiles(latencies),
              "service_ms": percentiles(service_times), "dispatch_delay_ms": percentiles(dispatch_times),
              "latency_population": "all completed HTTP or stock-Git attempts, including errors and client validation; driver_busy arrivals are counted failures without fabricated latency",
              "manifest_sha256": hashlib.sha256(args.manifest.read_bytes()).hexdigest()}
    result["samples_sha256"] = file_sha256(samples_path)
    if sum(counts.values()) != total:
        raise RuntimeError("benchmark lost scheduled outcomes")
    save(args.output, result)
    return result


def positive(value):
    parsed = int(value)
    if parsed < 1:
        raise argparse.ArgumentTypeError("must be positive")
    return parsed


def bounded_lfs_bytes(value):
    parsed = int(value)
    if not 0 <= parsed <= 16 * 1024 * 1024:
        raise argparse.ArgumentTypeError("LFS benchmark body must be 0 to 16 MiB")
    return parsed


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", required=True)
    parser.add_argument("--additional-base-url", action="append", default=[],
                        help="additional gateway ingress for round-robin run requests")
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--timeout", type=positive, default=10)
    parser.add_argument("--seed", type=int, default=20260926)
    commands = parser.add_subparsers(dest="command", required=True)
    create = commands.add_parser("seed")
    create.add_argument("--repositories", type=positive, default=1000)
    create.add_argument("--populated", type=positive, default=3)
    create.add_argument("--incremental-fixture", action="store_true",
                        help="seed a second commit and benchmark-base ref for incremental fetch/pull")
    create.add_argument("--lfs-fixture-bytes", type=bounded_lfs_bytes, default=0,
                        help="also upload one direct-basic LFS object per populated repository")
    create.add_argument("--work-dir", type=Path, required=True)
    check = commands.add_parser("verify")
    check.add_argument("--work-dir", type=Path, required=True)
    check.add_argument("--concurrency", type=positive, default=1,
                       help="bounded parallel identity checks (default: serial)")
    writes = commands.add_parser("verify-writes", help="check acknowledged load-test writes after owner recovery")
    writes.add_argument("--report", type=Path, action="append", dest="reports", required=True,
                        help="push_branch or lfs_upload report with its digest-bound samples; repeat for multiple runs")
    writes.add_argument("--work-dir", type=Path, required=True)
    writes.add_argument("--output", type=Path, required=True)
    writes.add_argument("--git-timeout", type=positive, default=120)
    creations = commands.add_parser("verify-creations", help="check every acknowledged created repository")
    creations.add_argument("--report", type=Path, action="append", dest="reports", required=True)
    creations.add_argument("--output", type=Path, required=True)
    run = commands.add_parser("run")
    run.add_argument("--active-repositories", type=positive,
                     help="required except for creation, which targets unique new names")
    run.add_argument("--distribution", choices=("uniform", "skewed"), default="uniform")
    run.add_argument("--operation", choices=("create", "metadata", "refs", "ls_remote", "clone", "cold_fetch",
                                           "incremental_fetch", "incremental_pull", "push_branch",
                                           "lfs_download", "lfs_upload"),
                     default="metadata")
    run.add_argument("--rate", type=positive, default=20)
    run.add_argument("--duration", type=positive, default=30)
    run.add_argument("--concurrency", type=positive, default=32)
    run.add_argument("--work-dir", type=Path, help="new client directory for stock-Git operations")
    run.add_argument("--git-timeout", type=positive, default=120)
    run.add_argument("--lfs-bytes", type=bounded_lfs_bytes, default=1024 * 1024,
                     help="unique LFS upload bytes per scheduled arrival, at most 16 MiB")
    run.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    token = os.environ.get("CANOPY_GIT_TOKEN")
    if not token:
        parser.error("CANOPY_GIT_TOKEN is required")
    if args.command == "run" and args.concurrency > 256:
        parser.error("driver concurrency is limited to 256")
    if args.command == "run" and args.operation != "create" and args.active_repositories is None:
        parser.error("--active-repositories is required for non-creation operations")
    if args.command == "verify" and args.concurrency > 32:
        parser.error("verify concurrency is limited to 32")
    if args.command != "run" and args.additional_base_url:
        parser.error("additional gateways are supported only for run")
    clients = [Client(base, token, args.timeout)
               for base in [args.base_url, *args.additional_base_url]]
    try:
        client = clients if args.command == "run" else clients[0]
        result = {"seed": seed, "verify": verify, "verify-writes": verify_writes,
                  "verify-creations": verify_creations,
                  "run": measure}[args.command](args, client, token)
        print(json.dumps(result, indent=2))
    finally:
        for client in clients:
            client.close()
    if args.command == "run" and result["failed_arrivals"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
