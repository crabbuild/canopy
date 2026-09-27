"""Discarded merge replies, durable publication, and native Git after owner loss."""
import http.client
import json
import subprocess
import tempfile
import time
import uuid
from urllib.parse import urlsplit
from smoke_s3_api import request
from smoke_s3_pulls import revision

REPOSITORY = "/api/repositories/other"
AUTH = "http.extraHeader=Authorization: Bearer local-test-token"


def seed(base_url, repository_id, local, source, base, author_token):
    subprocess.run(["git", "-c", AUTH, "push", f"{base_url}/canopy/other.git",
                    f"{base}:refs/heads/review-base", f"{source}:refs/heads/review-source"],
                   cwd=local, check=True, capture_output=True)
    request(base_url, f"{REPOSITORY}/branch-rules", "PUT", {
        "repository_id": repository_id,
        "rule": {"reference": "refs/heads/review-base", "expected_version": 0,
                 "enabled": True, "deny_deletions": False, "fast_forward_only": False,
                 "required_checks": [], "require_pull_request": True, "required_approvals": 1},
    })
    created = request(base_url, f"{REPOSITORY}/pulls", "POST", {
        "repository_id": repository_id, "id": str(uuid.uuid4()), "title": "Durable merge",
        "body": "Merge after review", "draft": False,
        "source_ref": "refs/heads/review-source", "source_oid": source,
        "base_ref": "refs/heads/review-base", "base_oid": base,
    }, token=author_token)
    api = f"{REPOSITORY}/pulls/{created['number']}"
    current = revision(request(base_url, api)["pull"])
    request(base_url, f"{api}/reviews", "POST", {
        "repository_id": repository_id, "id": str(uuid.uuid4()), "revision": current,
        "kind": "approve", "body": "Approved for merge",
    })
    payload = {"repository_id": repository_id, "id": str(uuid.uuid4()),
               "revision": current, "strategy": "fast_forward"}
    url = urlsplit(base_url)
    connection = http.client.HTTPConnection(url.hostname, url.port, timeout=30)
    try:
        connection.request("POST", f"{api}/merge", json.dumps(payload),
                           {"Authorization": "Bearer local-test-token", "Content-Type": "application/json"})
        deadline = time.monotonic() + 30
        while request(base_url, api)["pull"]["state"] != "merged":
            if time.monotonic() >= deadline:
                raise TimeoutError("merge did not publish")
            time.sleep(0.05)
        # Publication is observed through another connection; never consume the
        # original response. The retry below must recover the original result.
    finally:
        connection.close()
    result = request(base_url, f"{api}/merge", "POST", payload)
    assert result["merge"]["oid"] == source
    comparison_input = {"repository_id": repository_id, "target": {"kind": "merged"}, "query": {"kind": "files"}}
    comparison = request(base_url, f"{api}/comparison", "POST", comparison_input)
    assert comparison["comparison"]["revision"] == current
    assert comparison["comparison"]["files"]
    candidates = seed_candidates(base_url, repository_id, local, source, base, author_token)
    return api, payload, result, request(base_url, api), candidates, comparison_input, comparison


def verify(base_url, local, expected):
    api, payload, result, pull, candidates, comparison_input, comparison = expected
    assert request(base_url, f"{api}/comparison", "POST", comparison_input) == comparison
    assert request(base_url, f"{api}/merge", "POST", payload) == result
    assert request(base_url, api) == pull
    with tempfile.TemporaryDirectory(prefix="merged-clone-", dir=local.parent) as directory:
        subprocess.run(["git", "-c", AUTH, "clone", "--bare", "--single-branch", "--branch", "review-base",
                        f"{base_url}/canopy/other.git", directory], check=True, capture_output=True)
        oid = subprocess.run(["git", "rev-parse", "HEAD"], cwd=directory,
                             check=True, capture_output=True).stdout.strip().decode()
        assert oid == result["merge"]["oid"]
        subprocess.run(["git", "fsck", "--strict"], cwd=directory, check=True, capture_output=True)
    verify_candidates(base_url, local, candidates)
    print("PASS: discarded merge reply replays the same result and stock Git sees the merged commit after recovery", flush=True)


def seed_candidates(base_url, repository_id, local, source, base, author_token):
    request(base_url, f"{REPOSITORY}/check-contexts/candidate-tests", "PUT", {
        "repository_id": repository_id, "expected_version": 0,
        "reporter": "canopy", "enabled": True,
    })
    states = []
    for strategy in ("merge_commit", "squash", "rebase"):
        branch = f"candidate-{strategy}"
        subprocess.run(["git", "-c", AUTH, "push", f"{base_url}/canopy/other.git",
                        f"{base}:refs/heads/{branch}"], cwd=local, check=True, capture_output=True)
        request(base_url, f"{REPOSITORY}/branch-rules", "PUT", {
            "repository_id": repository_id,
            "rule": {"reference": f"refs/heads/{branch}", "expected_version": 0,
                     "enabled": True, "deny_deletions": True, "fast_forward_only": True,
                     "required_checks": ["candidate-tests"], "require_pull_request": True, "required_approvals": 1},
        })
        created = request(base_url, f"{REPOSITORY}/pulls", "POST", {
            "repository_id": repository_id, "id": str(uuid.uuid4()), "title": f"Durable {strategy}",
            "body": "Check the native candidate", "draft": False,
            "source_ref": "refs/heads/review-source", "source_oid": source,
            "base_ref": f"refs/heads/{branch}", "base_oid": base,
        }, token=author_token)
        api = f"{REPOSITORY}/pulls/{created['number']}"
        current = revision(request(base_url, api)["pull"])
        payload = {"repository_id": repository_id, "id": str(uuid.uuid4()),
                   "revision": current, "strategy": strategy, "message": "" if strategy == "rebase" else f"Checked {strategy}"}
        candidate = request(base_url, f"{api}/merge-candidates", "POST", payload)
        assert candidate["candidate"]["result"]["state"] == "ready"
        request(base_url, f"{api}/reviews", "POST", {
            "repository_id": repository_id, "id": str(uuid.uuid4()), "revision": current,
            "kind": "approve", "body": "Candidate approved",
        })
        oid = candidate["candidate"]["result"]["oid"]
        check = str(uuid.uuid4())
        request(base_url, f"{REPOSITORY}/commits/{oid}/checks", "POST", {
            "repository_id": repository_id, "id": check, "context": "candidate-tests", "context_version": 1,
        })
        request(base_url, f"{REPOSITORY}/checks/{check}", "PUT", {
            "repository_id": repository_id, "expected_version": 1, "state": "success", "summary": "Native candidate passed",
        })
        merge = {"repository_id": repository_id, "id": str(uuid.uuid4()), "revision": current,
                 "strategy": strategy, "candidate_id": payload["id"]}
        comparison = request(base_url, f"{api}/comparison", "POST", {
            "repository_id": repository_id, "target": {"kind": "current", "revision": current}, "query": {"kind": "files"},
        })
        states.append({"api": api, "payload": payload, "candidate": candidate, "merge": merge, "result": None, "comparison": comparison})
    return states


def verify_candidates(base_url, local, states):
    for state in states:
        api, payload, candidate = state["api"], state["payload"], state["candidate"]
        assert request(base_url, f"{api}/merge-candidates/{payload['id']}") == candidate
        assert request(base_url, f"{api}/merge-candidates", "POST", payload) == candidate
        with tempfile.TemporaryDirectory(prefix="candidate-fetch-", dir=local.parent) as directory:
            subprocess.run(["git", "init", "--bare", directory], check=True, capture_output=True)
            subprocess.run(["git", "-c", AUTH, "fetch", f"{base_url}/canopy/other.git", candidate["fetch_ref"]],
                           cwd=directory, check=True, capture_output=True)
            actual = subprocess.run(["git", "rev-parse", "FETCH_HEAD"], cwd=directory,
                                    check=True, capture_output=True).stdout.strip().decode()
            assert actual == candidate["candidate"]["result"]["oid"]
            subprocess.run(["git", "fsck", "--strict", actual], cwd=directory, check=True, capture_output=True)
        # First verification runs after clean restart: candidates, approvals and
        # candidate-specific checks must all restore before publication succeeds.
        merged = request(base_url, f"{api}/merge", "POST", state["merge"])
        assert merged["merge"]["oid"] == candidate["candidate"]["result"]["oid"]
        if state["result"] is None:
            state["result"] = merged
        else:
            assert merged == state["result"]
        assert request(base_url, api)["pull"]["merge"] == merged["merge"]
        assert merged["merge"]["revision"] == state["merge"]["revision"]
        assert request(base_url, f"{api}/comparison", "POST", {
            "repository_id": payload["repository_id"], "target": {"kind": "merged"}, "query": {"kind": "files"},
        }) == state["comparison"]
    print("PASS: native merge/squash/rebase candidates fetch and publish after recovery, then replay after owner loss", flush=True)
