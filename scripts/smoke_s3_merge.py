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
    return api, payload, result, request(base_url, api)


def verify(base_url, local, expected):
    api, payload, result, pull = expected
    assert request(base_url, f"{api}/merge", "POST", payload) == result
    assert request(base_url, api) == pull
    with tempfile.TemporaryDirectory(prefix="merged-clone-", dir=local.parent) as directory:
        subprocess.run(["git", "-c", AUTH, "clone", "--bare", "--single-branch", "--branch", "review-base",
                        f"{base_url}/canopy/other.git", directory], check=True, capture_output=True)
        oid = subprocess.run(["git", "rev-parse", "HEAD"], cwd=directory,
                             check=True, capture_output=True).stdout.strip().decode()
        assert oid == result["merge"]["oid"]
        subprocess.run(["git", "fsck", "--strict"], cwd=directory, check=True, capture_output=True)
    print("PASS: discarded merge reply replays the same result and stock Git sees the merged commit after recovery", flush=True)
