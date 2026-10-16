#!/usr/bin/env python3
"""Import real Git histories, verify browse APIs against Git, and measure reads.

Uses CANOPY_GIT_TOKEN. Corpus entries need name, source (local bare repository),
branch and commit. Never mutates source repositories. Timed reads use closed-loop
clients; throughput is achieved throughput, not a scheduled-arrival capacity claim.
"""

import argparse
import base64
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import math
import os
from pathlib import Path
import random
import subprocess
import threading
import time

from benchmark_repositories import Client, save


def encoded(value):
    return base64.urlsafe_b64encode(value).decode().rstrip("=")


def decoded(value):
    return base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))


def git(repo, *args):
    return subprocess.check_output(["git", "-C", repo["source"], *args])


def git_environment(token):
    env = {key: value for key, value in os.environ.items() if not key.startswith("GIT_")}
    env.update(GIT_TERMINAL_PROMPT="0", GIT_CONFIG_NOSYSTEM="1",
               GIT_CONFIG_GLOBAL=os.devnull, GIT_CONFIG_COUNT="2",
               GIT_CONFIG_KEY_0="credential.helper", GIT_CONFIG_VALUE_0="",
               GIT_CONFIG_KEY_1="http.extraHeader", GIT_CONFIG_VALUE_1=f"Authorization: Bearer {token}")
    return env


def require(condition, message):
    if not condition:
        raise ValueError(message)


def post(client, repo, query):
    payload = {"repository_id": repo["repository_id"], "query": query}
    status, body = client.request(f"/api/repositories/{repo['name']}/browse", payload)
    require(status == 200, f"{repo['name']} {query['kind']}: HTTP {status}: {body[:200]!r}")
    return json.loads(body)["view"], body


def import_repositories(args, client, token):
    corpus = json.loads(args.corpus.read_text())
    for repo in corpus["repositories"]:
        require("repository_id" not in repo, "import requires repositories not previously imported")
        status, body = client.request("/api/repositories", {"name": repo["name"]})
        require(status == 200, f"create {repo['name']}: HTTP {status}: {body[:200]!r}")
        repo.update(json.loads(body))
        save(args.corpus, corpus)
        env = git_environment(token)
        started = time.monotonic()
        result = subprocess.run(["git", "-C", repo["source"], "push", "--atomic",
                                 repo["clone_url"], "refs/heads/*:refs/heads/*", "refs/tags/*:refs/tags/*"],
                                env=env, capture_output=True, timeout=3600)
        seconds = time.monotonic() - started
        (args.output / f"{repo['name']}-push.log").write_bytes(result.stdout + result.stderr)
        repo["import"] = {"seconds": seconds, "exit_code": result.returncode,
                          "objects_per_second": repo["object_count"] / seconds,
                          "source_pack_mib_per_second": repo["pack_bytes"] / 1024**2 / seconds}
        save(args.corpus, corpus)
        require(result.returncode == 0, f"{repo['name']} import failed; see push log")
        print(f"IMPORTED {repo['name']}: {seconds:.2f}s, {repo['object_count'] / seconds:.1f} objects/s", flush=True)
        status, body = client.request(f"/api/repositories/{repo['name']}/default-branch")
        require(status == 200, "default branch read failed")
        current = json.loads(body)
        # Client.request intentionally only supports GET/POST; use the same connection for PUT.
        connection = client.connection()
        connection.request("PUT", f"/api/repositories/{repo['name']}/default-branch",
                           json.dumps({"repository_id": repo["repository_id"],
                                       "reference": f"refs/heads/{repo['branch']}",
                                       "expected_generation": current["generation"]}),
                           {"Authorization": f"Bearer {token}", "Content-Type": "application/json"})
        response = connection.getresponse()
        response.read()
        require(response.status == 200, "default branch update failed")
    save(args.output / "imports.json", corpus)


def check_commit(repo, actual):
    raw = git(repo, "cat-file", "commit", actual["oid"])
    headers, message = raw.split(b"\n\n", 1)
    lines = headers.splitlines()
    require(actual["tree_oid"] == lines[0].removeprefix(b"tree ").decode(), "commit tree mismatch")
    parents = []
    for line in lines[1:]:
        if not line.startswith(b"parent "):
            break
        parents.append(line[7:].decode())
    require(actual["parents"] == parents, "commit parents mismatch")
    for field in ("author", "committer"):
        original = next((line[len(field) + 1:] for line in lines if line.startswith(field.encode() + b" ")), None)
        require(actual[f"{field}_base64"] == (encoded(original) if original is not None else None),
                f"commit {field} attribution mismatch")
    require(decoded(actual["message_base64"]) == message[:4096], "commit message mismatch")
    require(actual["message_truncated"] == (len(message) > 4096), "commit truncation mismatch")


def tree_entries(repo, revision, path=b""):
    target = revision + (":" + os.fsdecode(path) if path else "^{tree}")
    result = []
    for record in git(repo, "ls-tree", "-z", target).split(b"\0"):
        if record:
            meta, name = record.split(b"\t", 1)
            mode, kind, oid = meta.split()
            result.append((name, mode.decode(), kind.decode(), oid.decode()))
    return sorted(result)


def verify(args, client):
    corpus = json.loads(args.corpus.read_text())
    queries, proofs = [], []
    for repo in corpus["repositories"]:
        clone = args.output / "clones" / f"{repo['name']}.git"
        clone.parent.mkdir(exist_ok=True)
        started = time.monotonic()
        cloned = subprocess.run(["git", "clone", "--mirror", repo["clone_url"], str(clone)],
                                env=git_environment(client.token), capture_output=True, timeout=1800)
        clone_seconds = time.monotonic() - started
        (args.output / f"{repo['name']}-clone.log").write_bytes(cloned.stdout + cloned.stderr)
        require(cloned.returncode == 0, "clone-back failed")
        checked = subprocess.run(["git", "-C", str(clone), "fsck", "--full"], capture_output=True, timeout=600)
        (args.output / f"{repo['name']}-fsck.log").write_bytes(checked.stdout + checked.stderr)
        require(checked.returncode == 0, "clone-back fsck failed")
        original_objects = set(git(repo, "rev-list", "--objects", "--all", "--no-object-names").splitlines())
        restored_objects = set(subprocess.check_output(["git", "-C", str(clone), "rev-list", "--objects", "--all", "--no-object-names"]).splitlines())
        require(original_objects == restored_objects, "clone-back object graph differs")
        expected_refs = dict(line.split() for line in git(repo, "for-each-ref", "--format=%(refname) %(objectname)",
                                                        "refs/heads", "refs/tags").decode().splitlines())
        seen, query = {}, {"kind": "refs"}
        while True:
            view, _ = post(client, repo, query)
            page = view["refs"]
            seen.update((entry["name"], entry["oid"]) for entry in page["entries"])
            if page["next_after"] is None:
                break
            query = {"kind": "refs", "after": page["next_after"], "generation": page["generation"]}
        require(seen == expected_refs, f"{repo['name']}: imported refs mismatch")
        variants = [("refs", {"kind": "refs"})]
        files = []
        for record in git(repo, "ls-tree", "-rlz", repo["commit"]).split(b"\0"):
            if not record:
                continue
            meta, path = record.split(b"\t", 1)
            mode, kind, oid, size = meta.split()
            if kind == b"blob" and mode == b"100644" and 0 < int(size) <= 256 * 1024:
                files.append((int(size), path, oid.decode()))
        selected = []
        for target in (1024, 16 * 1024, 128 * 1024):
            candidate = min(files, key=lambda item: abs(item[0] - target))
            if candidate not in selected:
                selected.append(candidate)
        directories = {b"", *(path.rpartition(b"/")[0] for _, path, _ in selected)}
        for path in sorted(directories):
            query = {"kind": "tree", "commit": repo["commit"], "path_base64": encoded(path)}
            view, _ = post(client, repo, query)
            actual = view["tree"]
            check_commit(repo, actual["commit"])
            expected = tree_entries(repo, repo["commit"], path)
            require([(decoded(entry["name_base64"]), entry["mode"], entry["oid"]) for entry in actual["entries"]]
                    == [(name, mode, oid) for name, mode, _, oid in expected[:32]], "tree page mismatch")
            require(actual["next_after"] == (encoded(expected[31][0]) if len(expected) > 32 else None), "tree cursor mismatch")
            variants.append(("tree", query))
        for size, path, oid in selected:
            query = {"kind": "file", "commit": repo["commit"], "path_base64": encoded(path)}
            view, _ = post(client, repo, query)
            actual = view["file"]
            require(actual["content_status"] == "included" and actual["size"] == size and actual["oid"] == oid,
                    "file metadata mismatch")
            require(decoded(actual["content_base64"]) == git(repo, "cat-file", "blob", oid), "blob bytes mismatch")
            variants.append(("blob", query))
        history = git(repo, "rev-list", "--first-parent", "-n", "545", repo["commit"]).decode().splitlines()
        for offset in (0, 32, 128, 512):
            if offset >= len(history):
                continue
            query = {"kind": "history", "commit": history[offset]}
            view, _ = post(client, repo, query)
            actual = view["history"]
            expected = history[offset:offset + 32]
            require([commit["oid"] for commit in actual["commits"]] == expected, "first-parent history mismatch")
            for commit in actual["commits"]:
                check_commit(repo, commit)
            require(actual["next_commit"] == (history[offset + 32] if len(history) > offset + 32 else None), "history cursor mismatch")
            variants.append(("commits", query))
        status, _ = client.request(f"/api/repositories/{repo['name']}/browse",
                                   {"repository_id": repo["repository_id"], "query": {"kind": "attribution"}})
        require(status == 422, "unexpected attribution support; update benchmark coverage")
        for operation, query in variants:
            _, body = post(client, repo, query)
            queries.append({"repository": repo["name"], "operation": operation,
                            "path": f"/api/repositories/{repo['name']}/browse",
                            "payload": {"repository_id": repo["repository_id"], "query": query},
                            "response_sha256": hashlib.sha256(body).hexdigest(), "response_bytes": len(body)})
        proofs.append({"repository": repo["name"], "refs_verified": len(seen),
                       "clone_seconds": clone_seconds, "clone_fsck_passed": True,
                       "clone_objects_verified": len(restored_objects),
                       "commit_metadata_and_attribution_verified": sum(min(32, len(history) - offset) for offset in (0, 32, 128, 512) if offset < len(history)),
                       "files": [{"path": os.fsdecode(path), "bytes": size, "oid": oid} for size, path, oid in selected],
                       "attribution_query_status": status})
        print(f"VERIFIED {repo['name']}: {len(seen)} refs, trees, exact blob bytes, history and author/committer metadata", flush=True)
    save(args.output / "queries.json", {"queries": queries})
    save(args.output / "verification.json", {"repositories": proofs, "attribution_api": "not implemented; browse kind rejected with 422"})


def distribution(values):
    values = sorted(values)
    return {**{f"p{p}": values[max(0, math.ceil(len(values) * p / 100) - 1)] for p in (50, 95, 99)},
            "min": values[0], "max": values[-1], "mean": sum(values) / len(values)} if values else None


def measure(args, token):
    queries = json.loads((args.output / "queries.json").read_text())["queries"]
    groups = {}
    for query in queries:
        groups.setdefault((query["repository"], query["operation"]), []).append(query)
    if len(groups) > 1:
        groups[("all", "mixed")] = [variants[min(1, len(variants) - 1)] if operation == "blob" else variants[0]
                                      for (_, operation), variants in list(groups.items())]
    plan = [(key, concurrency, repeat) for key in groups for concurrency in args.concurrency for repeat in range(args.repeats)]
    random.Random(42).shuffle(plan)
    report = {"method": "closed-loop, persistent HTTP/1.1 connections, full response read, no retries",
              "mixed_workload": "equal rotation over one query for each repository/API pair; medium sampled blob",
              "duration_per_group_seconds": args.duration, "concurrency": args.concurrency,
              "repeats": args.repeats, "results": []}
    samples_path = args.output / "samples.jsonl"
    require(not samples_path.exists(), "measurement output already exists")
    with samples_path.open("w") as samples:
        for (repo, operation), concurrency, repeat in plan:
            group_unix_seconds = time.time()
            client = Client(args.base_url, token, 130)
            variants = groups[(repo, operation)]
            # Establish and validate the entire working set before timed requests.
            for variant in variants:
                status, body = client.request(variant["path"], variant["payload"])
                require(status == 200 and hashlib.sha256(body).hexdigest() == variant["response_sha256"], "warmup validation failed")
            gate = threading.Barrier(concurrency + 1)
            started = [0.0]

            def worker(index):
                observations = []
                gate.wait()
                sequence = index
                while time.monotonic() < started[0] + args.duration:
                    variant_index = sequence % len(variants)
                    variant = variants[variant_index]
                    before = time.monotonic()
                    try:
                        status, body = client.request(variant["path"], variant["payload"])
                        elapsed = (time.monotonic() - before) * 1000
                        outcome = "ok" if status == 200 and hashlib.sha256(body).hexdigest() == variant["response_sha256"] else f"http_{status}_or_invalid_body"
                        size = len(body)
                    except Exception as error:
                        elapsed = (time.monotonic() - before) * 1000
                        status, size, outcome = 0, 0, type(error).__name__
                    observations.append({"latency_ms": elapsed, "status": status, "bytes": size,
                                         "outcome": outcome, "variant": variant_index})
                    sequence += 1
                return observations

            try:
                with ThreadPoolExecutor(max_workers=concurrency) as pool:
                    tasks = [pool.submit(worker, index) for index in range(concurrency)]
                    started[0] = time.monotonic()
                    gate.wait()
                    observations = [sample for task in tasks for sample in task.result()]
                    elapsed = time.monotonic() - started[0]
            finally:
                client.close()
            identity = {"repository": repo, "operation": operation, "concurrency": concurrency, "repeat": repeat + 1}
            outcomes = Counter(sample["outcome"] for sample in observations)
            result = {**identity, "unix_seconds": group_unix_seconds,
                      "requests": len(observations), "outcomes": dict(outcomes), "elapsed_seconds": elapsed,
                      "successful_requests_per_second": outcomes["ok"] / elapsed,
                      "response_mib_per_second": sum(sample["bytes"] for sample in observations if sample["outcome"] == "ok") / 1024**2 / elapsed,
                      "latency_ms_all_attempts": distribution([sample["latency_ms"] for sample in observations]),
                      "latency_ms_successful": distribution([sample["latency_ms"] for sample in observations if sample["outcome"] == "ok"])}
            for sample in observations:
                samples.write(json.dumps({**identity, **sample}) + "\n")
            samples.flush()
            report["results"].append(result)
            save(args.output / "measurements.json", report)
            successful = result["latency_ms_successful"]
            successful_p95 = f"{successful['p95']:.1f}" if successful else "n/a"
            print(f"MEASURED {repo}/{operation} c={concurrency} repeat={repeat + 1}: {result['successful_requests_per_second']:.1f} successful req/s, p95(success)={successful_p95} ms, p95(all)={result['latency_ms_all_attempts']['p95']:.1f} ms, errors={len(observations) - outcomes['ok']}", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("import", "verify", "measure"))
    parser.add_argument("--base-url", required=True)
    parser.add_argument("--corpus", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--duration", type=float, default=8)
    parser.add_argument("--concurrency", type=int, nargs="+", default=[1, 4, 16])
    parser.add_argument("--repeats", type=int, default=2)
    args = parser.parse_args()
    require(args.duration > 0 and args.repeats > 0 and all(1 <= n <= 64 for n in args.concurrency), "invalid workload")
    args.output.mkdir(parents=True, exist_ok=True)
    token = os.environ["CANOPY_GIT_TOKEN"]
    if args.action == "measure":
        measure(args, token)
        return
    client = Client(args.base_url, token, 130)
    try:
        if args.action == "import":
            import_repositories(args, client, token)
        else:
            verify(args, client)
    finally:
        client.close()


if __name__ == "__main__":
    main()
