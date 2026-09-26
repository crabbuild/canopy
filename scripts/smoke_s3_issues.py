"""Issue and comment persistence checks for the S3 process qualification."""

import json
import urllib.request
import uuid


API = "/api/repositories/renamed/issues"


def _request(base_url, path, method="GET", payload=None):
    request = urllib.request.Request(
        f"{base_url}{path}", method=method,
        data=None if payload is None else json.dumps(payload).encode(),
        headers={"Authorization": "Bearer local-test-token", "Content-Type": "application/json"},
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        assert response.status == (204 if method == "PUT" else 200)
        return None if response.status == 204 else json.load(response)


def seed(base_url, repository_id):
    issue = {"repository_id": repository_id, "id": str(uuid.uuid4()),
             "title": "Recover collaboration state", "body": "Original issue body 🌲"}
    comment = {"repository_id": repository_id, "id": str(uuid.uuid4()),
               "body": "Original comment"}
    assert _request(base_url, API, "POST", issue) == {"number": 1}
    assert _request(base_url, f"{API}/1/comments", "POST", comment) == {"number": 1}
    _request(base_url, f"{API}/1", "PUT", {
        "repository_id": repository_id, "expected_version": 1,
        "title": "Recovery checked", "body": "Edited description", "state": "closed",
    })
    _request(base_url, f"{API}/1/comments/1", "PUT", {
        "repository_id": repository_id, "expected_version": 1, "body": "Edited comment",
    })
    detail = _request(base_url, f"{API}/1")
    comments = _request(base_url, f"{API}/1/comments")
    assert detail["issue"]["state"] == "closed" and detail["issue"]["version"] == 2
    assert comments["comments"][0]["body"] == "Edited comment"
    assert comments["comments"][0]["version"] == 2
    return issue, comment, detail, comments


def verify(base_url, expected):
    issue, comment, detail, comments = expected
    # Retrying original creates must preserve subsequent edits after recovery.
    assert _request(base_url, API, "POST", issue) == {"number": 1}
    assert _request(base_url, f"{API}/1/comments", "POST", comment) == {"number": 1}
    assert _request(base_url, f"{API}/1") == detail
    assert _request(base_url, f"{API}/1/comments") == comments
    page = _request(base_url, f"{API}?state=closed")
    assert [entry["number"] for entry in page["issues"]] == [1]
    assert page["next_after"] is None
    print("PASS: issue, edited comment, versions and original-create replay match durable state", flush=True)
