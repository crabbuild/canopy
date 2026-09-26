"""Issue and comment persistence checks for the S3 process qualification."""

import uuid
from smoke_s3_api import request


API = "/api/repositories/renamed/issues"


def seed(base_url, repository_id):
    issue = {"repository_id": repository_id, "id": str(uuid.uuid4()),
             "title": "Recover collaboration state", "body": "Original issue body 🌲"}
    comment = {"repository_id": repository_id, "id": str(uuid.uuid4()),
               "body": "Original comment"}
    assert request(base_url, API, "POST", issue) == {"number": 1}
    assert request(base_url, f"{API}/1/comments", "POST", comment) == {"number": 1}
    request(base_url, f"{API}/1", "PUT", {
        "repository_id": repository_id, "expected_version": 1,
        "title": "Recovery checked", "body": "Edited description", "state": "closed",
    })
    request(base_url, f"{API}/1/comments/1", "PUT", {
        "repository_id": repository_id, "expected_version": 1, "body": "Edited comment",
    })
    detail = request(base_url, f"{API}/1")
    comments = request(base_url, f"{API}/1/comments")
    assert detail["issue"]["state"] == "closed" and detail["issue"]["version"] == 2
    assert comments["comments"][0]["body"] == "Edited comment"
    assert comments["comments"][0]["version"] == 2
    return issue, comment, detail, comments


def verify(base_url, expected):
    issue, comment, detail, comments = expected
    # Retrying original creates must preserve subsequent edits after recovery.
    assert request(base_url, API, "POST", issue) == {"number": 1}
    assert request(base_url, f"{API}/1/comments", "POST", comment) == {"number": 1}
    assert request(base_url, f"{API}/1") == detail
    assert request(base_url, f"{API}/1/comments") == comments
    page = request(base_url, f"{API}?state=closed")
    assert [entry["number"] for entry in page["issues"]] == [1]
    assert page["next_after"] is None
    print("PASS: issue, edited comment, versions and original-create replay match durable state", flush=True)
