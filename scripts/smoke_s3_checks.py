"""Configured reporters and ordered check reruns across process recovery."""

import uuid
from smoke_s3_api import request

REPOSITORY = "/api/repositories/renamed"


def seed(base_url, repository_id, oid):
    request(base_url, f"{REPOSITORY}/check-contexts/unit-tests", "PUT", {
        "repository_id": repository_id, "expected_version": 0,
        "reporter": "canopy", "enabled": True,
    })
    commit = f"{REPOSITORY}/commits/{oid}/checks"
    old = {"repository_id": repository_id, "id": str(uuid.uuid4()),
           "context": "unit-tests", "context_version": 1}
    newer = {**old, "id": str(uuid.uuid4())}
    for run in (old, newer):
        assert request(base_url, commit, "POST", run) == {"id": run["id"]}
    # A late success from the older attempt must not hide the newer failure.
    for run, state in ((newer, "failure"), (old, "success")):
        request(base_url, f"{REPOSITORY}/checks/{run['id']}", "PUT", {
            "repository_id": repository_id, "expected_version": 1,
            "state": state, "summary": f"Attempt completed: {state}",
        })
    current = request(base_url, commit)
    assert current["checks"][0]["run"]["id"] == newer["id"]
    assert current["checks"][0]["run"]["state"] == "failure"
    historical = request(base_url, f"{REPOSITORY}/checks/{old['id']}")
    return commit, old, current, historical


def verify(base_url, expected):
    commit, old, current, historical = expected
    assert request(base_url, commit, "POST", old) == {"id": old["id"]}
    assert request(base_url, commit) == current
    assert request(base_url, f"{REPOSITORY}/checks/{old['id']}") == historical
    print("PASS: check policy, results and newest-attempt selection survive recovery and old retries", flush=True)
