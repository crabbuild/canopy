#!/usr/bin/env python3
"""Qualify a real repository against local_eval.py, with a durable JSON report.

Imports branches/tags, checks warm stock Git clones, then restarts the node
(which discards its runtime workspace) and verifies a cold clone and fsck.
Snapshot mode imports one root commit with the source HEAD tree, not history.
Run on a dedicated evaluation node: the recovery stage restarts that node.
"""
import argparse
import base64
import hashlib
import json
import os
import platform
from pathlib import Path
import signal
import sqlite3
import subprocess
import sys
import threading
import time
import urllib.request

import local_eval


def source_tree_digest(root):
    digest = hashlib.sha256()
    sources = [root / "Cargo.toml", root / "Cargo.lock"]
    sources.extend(path for path in (root / "crates").rglob("*")
                   if path.is_file() and (path.suffix in (".rs", ".sql")
                                          or path.name == "Cargo.toml"))
    for source_file in sorted(sources):
        digest.update(str(source_file.relative_to(root)).encode() + b"\0"
                      + source_file.read_bytes())
    return digest.hexdigest()


def references(directory):
    output = local_eval.run("git", "-C", str(directory), "for-each-ref",
                            "--format=%(objectname) %(refname)", "refs/heads", "refs/tags")
    return dict(line.split(" ", 1)[::-1] for line in output.splitlines())


def update_refs(directory, commands):
    if not commands:
        return
    subprocess.run(["git", "-C", str(directory), "update-ref", "--stdin"],
                   input="option no-deref\n" + "\n".join(commands) + "\n",
                   text=True, check=True, capture_output=True)


def process_memory(pid):
    output = local_eval.run("ps", "-axo", "pid=,ppid=,rss=")
    processes = [tuple(map(int, line.split())) for line in output.splitlines()]
    family = {pid}
    while True:
        children = {child for child, parent, _ in processes if parent in family}
        added = children - family
        if not added:
            break
        family.update(added)
    return sum(rss * 1024 for child, _, rss in processes if child in family)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-dir", required=True, type=Path)
    parser.add_argument("--source", required=True, type=Path)
    parser.add_argument("--work-dir", required=True, type=Path)
    parser.add_argument("--name", default="kubernetes")
    parser.add_argument("--mode", choices=("full", "snapshot"), default="full")
    parser.add_argument("--timeout", type=int, default=3600, help="seconds per Git operation")
    args = parser.parse_args()
    args.state_dir = args.state_dir.resolve()
    args.work_dir = args.work_dir.resolve()
    args.work_dir.mkdir(parents=True, exist_ok=False)
    metadata = json.loads((args.state_dir / "deployment.json").read_text())
    config = json.loads((args.state_dir / "config.json").read_text())
    env = local_eval.environment(args.state_dir)
    auth = base64.b64encode((config["owner"] + ":" + env["CANOPY_GIT_TOKEN"]).encode()).decode()
    git_env = {k: v for k, v in os.environ.items() if not k.startswith("GIT_")}
    git_env.update(GIT_CONFIG_COUNT="2", GIT_CONFIG_KEY_0="credential.helper",
                   GIT_CONFIG_VALUE_0="", GIT_CONFIG_KEY_1="http.extraHeader",
                   GIT_CONFIG_VALUE_1="Authorization: Basic " + auth,
                   GIT_TERMINAL_PROMPT="0", GIT_LFS_SKIP_SMUDGE="1")
    report = {
        "mode": args.mode, "name": args.name, "status": "running", "stages": [],
        "source": str(args.source.resolve()), "canopy_binary_sha256": metadata["binary_sha256"],
        "canopy_revision": local_eval.run("git", "rev-parse", "HEAD"),
        "canopy_source_tree_sha256": source_tree_digest(Path(__file__).resolve().parent.parent),
        "canopy_working_tree_dirty": bool(local_eval.run("git", "status", "--porcelain")),
        "provider_image": metadata["provider_image"], "git_version": local_eval.run("git", "--version"),
        "host_platform": platform.platform(), "host_cpu_count": os.cpu_count(),
        "disk_limit_bytes": config["local_disk_limit_bytes"],
        "active_repository_limit": config["max_active_repositories"],
        "node_process_tree_peak_rss_bytes": 0,
        "started_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    report_path = args.work_dir / "report.json"
    stopped = threading.Event()

    def sample():
        with (args.work_dir / "resources.jsonl").open("w") as output:
            while not stopped.is_set():
                pid = local_eval.owned_pid(args.state_dir, metadata)
                if pid:
                    try:
                        rss = process_memory(pid)
                        report["node_process_tree_peak_rss_bytes"] = max(
                            report["node_process_tree_peak_rss_bytes"], rss)
                        output.write(json.dumps({"time": time.time(), "pid": pid,
                                                 "process_tree_rss_bytes": rss}) + "\n")
                        # MAX(sequence) uses the integer primary key. Avoid
                        # table scans or long transactions on the live Cell.
                        for database in (args.state_dir / "node" / "runtime-v1").glob("*/repository.sqlite"):
                            try:
                                connection = sqlite3.connect(database.as_uri() + "?mode=ro", uri=True, timeout=0.2)
                                try:
                                    high_water = connection.execute("SELECT max(sequence) FROM objects").fetchone()[0]
                                finally:
                                    connection.close()
                                size = database.stat().st_size
                                wal = Path(str(database) + "-wal")
                                wal_size = wal.stat().st_size if wal.exists() else 0
                                output.write(json.dumps({"time": time.time(),
                                    "repository": database.parent.name, "object_sequence": high_water,
                                    "sqlite_bytes": size, "sqlite_wal_bytes": wal_size}) + "\n")
                                report["peak_repository_sqlite_and_wal_bytes"] = max(
                                    report.get("peak_repository_sqlite_and_wal_bytes", 0), size + wal_size)
                            except (OSError, sqlite3.Error):
                                pass
                        output.flush()
                    except (OSError, subprocess.CalledProcessError):
                        pass
                stopped.wait(5)

    def checkpoint():
        local_eval.save(report_path, report)

    def git(stage, *arguments, cwd=None):
        entry = {"name": stage, "status": "running"}
        report["stages"].append(entry)
        checkpoint()
        print(f"{stage}: started", flush=True)
        started = time.monotonic()
        try:
            with (args.work_dir / (stage + ".log")).open("wb") as output:
                child = subprocess.Popen(["git", *arguments], cwd=cwd, env=git_env,
                                         stdout=output, stderr=output, start_new_session=True)
                try:
                    code = child.wait(timeout=args.timeout)
                except BaseException:
                    # Stop the whole client worker group, including pack-objects.
                    os.killpg(child.pid, signal.SIGTERM)
                    try:
                        child.wait(timeout=10)
                    except subprocess.TimeoutExpired:
                        os.killpg(child.pid, signal.SIGKILL)
                        child.wait()
                    raise
            entry["exit_code"] = code
            if code:
                raise RuntimeError(f"{stage} failed; see {stage}.log")
            entry["status"] = "passed"
        except BaseException:
            entry["status"] = "failed"
            raise
        finally:
            entry["seconds"] = round(time.monotonic() - started, 3)
            checkpoint()
            print(f"{stage}: {entry['status']} ({entry['seconds']}s)", flush=True)

    def api(path, payload=None, method=None):
        request = urllib.request.Request(metadata["url"] + path,
            data=None if payload is None else json.dumps(payload).encode(), method=method,
            headers={"Authorization": "Bearer " + env["CANOPY_GIT_TOKEN"],
                     "Content-Type": "application/json"})
        with urllib.request.urlopen(request, timeout=60) as response:
            return json.load(response)

    sampler = threading.Thread(target=sample, daemon=True)
    sampler.start()
    checkpoint()
    try:
        if not local_eval.ready(metadata["url"]):
            raise RuntimeError("evaluation node must be ready before benchmarking")
        if local_eval.run("git", "-C", str(args.source), "rev-parse", "--is-shallow-repository") != "false":
            raise RuntimeError("source must contain full history; shallow source is not a scale gate")
        report["source_head"] = local_eval.run("git", "-C", str(args.source), "rev-parse", "HEAD")
        report["source_tree"] = local_eval.run("git", "-C", str(args.source), "rev-parse", "HEAD^{tree}")
        mirror = args.work_dir / "source.git"
        git("copy-source", "clone", "--mirror", "--no-hardlinks", str(args.source.resolve()), str(mirror))
        # A normal local checkout has remote-tracking release branches. Preserve
        # those as ordinary branches in the independent import fixture.
        remote = local_eval.run("git", "-C", str(mirror), "for-each-ref",
            "--format=%(objectname) %(refname) %(symref)", "refs/remotes/origin")
        local_branches = references(mirror)
        changes = []
        for line in remote.splitlines():
            parts = line.split()
            oid, reference = parts[:2]
            if len(parts) == 2 and reference != "refs/remotes/origin/HEAD":
                branch = reference.replace("refs/remotes/origin/", "refs/heads/", 1)
                existing = local_branches.get(branch)
                if existing and existing != oid:
                    # A checkout can legitimately lag origin. Retain both tips,
                    # keeping its HEAD tree as the independently verified tree.
                    branch = reference.replace("refs/remotes/origin/", "refs/heads/upstream/", 1)
                    if branch in local_branches:
                        raise RuntimeError("upstream branch naming collision; use a source mirror")
                changes.append(f"update {branch} {oid}")
            changes.append(f"delete {reference}")
        update_refs(mirror, changes)
        if args.mode == "snapshot":
            commit_env = {**git_env, "GIT_AUTHOR_NAME": "Canopy Evaluation",
                "GIT_AUTHOR_EMAIL": "evaluation@example.invalid",
                "GIT_COMMITTER_NAME": "Canopy Evaluation",
                "GIT_COMMITTER_EMAIL": "evaluation@example.invalid"}
            commit = local_eval.run("git", "-C", str(mirror), "commit-tree",
                report["source_tree"], "-m", "Kubernetes HEAD snapshot (no history)", env=commit_env)
            # Deletion and update of master must be separate update-ref batches.
            update_refs(mirror, [f"delete {ref}" for ref in references(mirror)])
            update_refs(mirror, [f"update refs/heads/master {commit}"])
            local_eval.run("git", "-C", str(mirror), "symbolic-ref", "HEAD", "refs/heads/master")
        expected = references(mirror)
        default_ref = local_eval.run("git", "-C", str(mirror), "symbolic-ref", "HEAD")
        if default_ref not in expected or expected[default_ref] != (
                commit if args.mode == "snapshot" else report["source_head"]):
            raise RuntimeError("source HEAD must name an imported branch")
        report["default_branch"] = default_ref
        local_eval.save(args.work_dir / "expected-refs.json", expected)
        report["ref_count"] = len(expected)
        report["reachable_commits"] = int(local_eval.run("git", "-C", str(mirror), "rev-list", "--count", "--branches", "--tags"))
        git("reachable-objects", "-C", str(mirror), "rev-list", "--objects", "--branches", "--tags")
        with (args.work_dir / "reachable-objects.log").open("rb") as objects:
            report["reachable_objects"] = sum(1 for _ in objects)
        checkpoint()
        created = api("/api/repositories", {"name": args.name})
        report["repository_id"] = created["repository_id"]
        url = created["clone_url"]
        report["clone_url"] = url
        git("push", "-C", str(mirror), "push", "--atomic", url,
            "refs/heads/*:refs/heads/*", "refs/tags/*:refs/tags/*")
        endpoint = f"/api/repositories/{args.name}/default-branch"
        head = api(endpoint)
        api(endpoint, {"repository_id": created["repository_id"], "reference": default_ref,
                       "expected_generation": head["generation"]}, "PUT")
        for protocol in ("0", "2"):
            clone = args.work_dir / ("warm-v" + protocol + ".git")
            git("warm-clone-v" + protocol, "-c", "protocol.version=" + protocol,
                "clone", "--mirror", url, str(clone))
            if references(clone) != expected:
                raise RuntimeError("warm clone refs differ from the import fixture")
            git("warm-fsck-v" + protocol, "-C", str(clone), "fsck", "--strict", "--full")
        pull_work = args.work_dir / "pull-work"
        git("prepare-pull-client", "clone", "--shared", "--single-branch", "--branch",
            default_ref.removeprefix("refs/heads/"), str(args.work_dir / "warm-v2.git"), str(pull_work))
        git("configure-pull-client", "-C", str(pull_work), "remote", "set-url", "origin", url)
        # The binary's startup contract removes runtime-v1 before recovering
        # authoritative Cells from the provider; no manual deletion is needed.
        commit_env = {**git_env, "GIT_AUTHOR_NAME": "Canopy Evaluation",
            "GIT_AUTHOR_EMAIL": "evaluation@example.invalid",
            "GIT_COMMITTER_NAME": "Canopy Evaluation",
            "GIT_COMMITTER_EMAIL": "evaluation@example.invalid"}
        increment = local_eval.run("git", "-C", str(mirror), "commit-tree",
            report["source_tree"], "-p", default_ref, "-m", "Incremental evaluation commit",
            env=commit_env)
        incremental_ref = "refs/heads/canopy-evaluation-" + created["repository_id"]
        if incremental_ref in expected:
            raise RuntimeError("evaluation branch collides with imported refs")
        report["evaluation_branch"] = incremental_ref
        git("incremental-prepare", "-C", str(mirror), "update-ref", incremental_ref, increment)
        git("advance-default-branch", "-C", str(mirror), "update-ref", default_ref, increment)
        git("incremental-push", "-C", str(mirror), "push", "--atomic", url,
            incremental_ref + ":" + incremental_ref, default_ref + ":" + default_ref)
        git("incremental-fetch", "-C", str(args.work_dir / "warm-v2.git"), "fetch", "origin", "--prune")
        git("incremental-pull", "-C", str(pull_work), "pull", "--ff-only", "origin", default_ref.removeprefix("refs/heads/"))
        if local_eval.run("git", "-C", str(pull_work), "rev-parse", "HEAD") != increment:
            raise RuntimeError("pull client did not reach the incremental commit")
        git("incremental-fsck", "-C", str(pull_work), "fsck", "--strict", "--full")
        expected = references(mirror)
        report["incremental_commit"] = increment
        local_eval.save(args.work_dir / "expected-restored-refs.json", expected)
        checkpoint()
        restart = {"name": "fresh-workspace-restart", "status": "running"}
        report["stages"].append(restart)
        checkpoint()
        started = time.monotonic()
        try:
            subprocess.run([sys.executable, str(Path(local_eval.__file__)), "stop",
                            "--state-dir", str(args.state_dir)], check=True)
            subprocess.run([sys.executable, str(Path(local_eval.__file__)), "start",
                            "--state-dir", str(args.state_dir)], check=True)
            restart["status"] = "passed"
        except BaseException:
            restart["status"] = "failed"
            raise
        finally:
            restart["seconds"] = round(time.monotonic() - started, 3)
            checkpoint()
        cold = args.work_dir / "cold.git"
        git("cold-clone", "clone", "--mirror", url, str(cold))
        if references(cold) != expected:
            raise RuntimeError("cold clone refs differ after object-store recovery")
        git("cold-fsck", "-C", str(cold), "fsck", "--strict", "--full")
        tree = local_eval.run("git", "-C", str(cold), "rev-parse", default_ref + "^{tree}")
        if tree != report["source_tree"]:
            raise RuntimeError("restored Kubernetes HEAD tree differs from source")
        report["restored_ref_count"] = len(expected)
        report["restored_tree"] = tree
        report["status"] = "passed"
    except BaseException as error:
        report["status"] = "failed"
        report["error"] = type(error).__name__ + ": " + str(error)
        raise
    finally:
        stopped.set()
        sampler.join(timeout=10)
        report["finished_at_utc"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        checkpoint()
        print(str(report_path), flush=True)


if __name__ == "__main__":
    main()
