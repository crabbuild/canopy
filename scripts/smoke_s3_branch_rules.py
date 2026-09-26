"""Branch publication policy survives clean restart and lease takeover."""

import subprocess
import uuid
from smoke_s3_api import request

REPOSITORY = "/api/repositories/renamed"
AUTH = "http.extraHeader=Authorization: Bearer local-test-token"


def push(base_url, local, refs, accepted):
    result = subprocess.run(
        ["git", "-c", "credential.helper=", "-c", AUTH, "push",
         f"{base_url}/canopy/renamed.git", *refs], cwd=local,
        capture_output=True, timeout=30, check=False,
    )
    assert (result.returncode == 0) == accepted, result.stderr.decode()
    return result.stderr


def seed(base_url, repository_id, local, oid):
    reference = "refs/heads/protected"
    push(base_url, local, [f"{oid}:{reference}"], True)
    request(base_url, f"{REPOSITORY}/check-contexts/branch-ci", "PUT", {
        "repository_id": repository_id, "expected_version": 0,
        "reporter": "canopy", "enabled": True,
    })
    request(base_url, f"{REPOSITORY}/branch-rules", "PUT", {
        "repository_id": repository_id,
        "rule": {"reference": reference, "expected_version": 0, "enabled": True,
                 "deny_deletions": True, "fast_forward_only": True, "require_pull_request": False, "required_approvals": 0,
                 "required_checks": ["branch-ci"]},
    })
    # An isolated commit advances the protected branch without changing the
    # existing Git/LFS recovery fixture or its default branch.
    tree = subprocess.run(["git", "rev-parse", f"{oid}^{{tree}}"], cwd=local,
                          capture_output=True, check=True).stdout.strip().decode()
    tip = subprocess.run(["git", "commit-tree", tree, "-p", oid, "-m", "Protected tip"],
                         cwd=local, capture_output=True, check=True).stdout.strip().decode()
    rejected = push(base_url, local, [f"{tip}:{reference}", f"{tip}:refs/heads/branch-candidate"], False)
    assert b"hook declined" in rejected
    for commit in (oid, tip):
        run_id = str(uuid.uuid4())
        request(base_url, f"{REPOSITORY}/commits/{commit}/checks", "POST", {
            "repository_id": repository_id, "id": run_id, "context": "branch-ci", "context_version": 1,
        })
        request(base_url, f"{REPOSITORY}/checks/{run_id}", "PUT", {
            "repository_id": repository_id, "expected_version": 1, "state": "success", "summary": "Passed",
        })
    push(base_url, local, [f"{tip}:{reference}"], True)
    rules = request(base_url, f"{REPOSITORY}/branch-rules")
    return oid, tip, rules


def verify(base_url, local, expected):
    original, tip, rules = expected
    assert request(base_url, f"{REPOSITORY}/branch-rules") == rules
    rejected = push(base_url, local, [f"+{original}:refs/heads/protected"], False)
    assert b"fast-forward" in rejected
    push(base_url, local, [":refs/heads/protected"], False)
    current = subprocess.run(
        ["git", "-c", AUTH, "ls-remote", f"{base_url}/canopy/renamed.git", "refs/heads/protected"],
        capture_output=True, timeout=30, check=True,
    ).stdout.strip().decode()
    assert current == f"{tip}\trefs/heads/protected"
    print("PASS: protected branch, required checks and force/delete rejection survive recovery", flush=True)
