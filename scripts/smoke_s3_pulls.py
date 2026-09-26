"""Pull/review identity, revision eligibility and edits across process recovery."""

import secrets
import uuid
from smoke_s3_api import request

REPOSITORY = "/api/repositories/other"


def revision(pull):
    return {"pull_version": pull["version"],
            "source_oid": pull["source"]["oid"], "source_version": pull["source"]["version"],
            "base_oid": pull["base"]["oid"], "base_version": pull["base"]["version"]}


def seed(base_url, repository_id, source_oid, base_oid):
    token = f"cnp_{secrets.token_hex(32)}"
    request(base_url, "/api/accounts", "POST", {"name": "pull-author", "token": token, "scope": "write"})
    request(base_url, f"{REPOSITORY}/collaborators/pull-author", "PUT", {"role": "read"}, expected_status=200)
    creation = {"repository_id": repository_id, "id": str(uuid.uuid4()),
                "title": "Process recovery pull", "body": "Original description", "draft": False,
                "source_ref": "refs/heads/partial", "source_oid": source_oid,
                "base_ref": "refs/heads/main", "base_oid": base_oid}
    number = request(base_url, f"{REPOSITORY}/pulls", "POST", creation, token=token)["number"]
    api = f"{REPOSITORY}/pulls/{number}"
    old = {"repository_id": repository_id, "id": str(uuid.uuid4()),
           "revision": revision(request(base_url, api)["pull"]), "kind": "approve", "body": "First review"}
    request(base_url, f"{api}/reviews", "POST", old)
    edit = {"repository_id": repository_id, "expected_version": 1,
            "title": "Edited recovery pull", "body": "Final description", "state": "closed", "draft": False}
    request(base_url, api, "PUT", edit, token=token)
    request(base_url, api, "PUT", {**edit, "expected_version": 2, "state": "open"}, token=token)
    fresh = {**old, "id": str(uuid.uuid4()), "revision": revision(request(base_url, api)["pull"])}
    request(base_url, f"{api}/reviews", "POST", fresh)
    reviews = request(base_url, f"{api}/reviews")
    assert [review["id"] for review in reviews["reviews"] if review["applicable"]] == [fresh["id"]]
    comparison_input = {"repository_id": repository_id, "target": {"kind": "current", "revision": fresh["revision"]},
                        "query": {"kind": "files"}}
    comparison = request(base_url, f"{api}/comparison", "POST", comparison_input, token=token)
    changed = comparison["comparison"]["files"]
    path = next(file["path_base64"] for file in changed if file["after"] is not None)
    file_input = {**comparison_input, "query": {"kind": "file", "path_base64": path, "side": "after"}}
    preview = request(base_url, f"{api}/comparison", "POST", file_input, token=token)
    assert preview["file"]["content_status"] == "included"
    return api, token, creation, old, request(base_url, api), reviews, comparison_input, comparison, file_input, preview


def verify(base_url, expected):
    api, token, creation, old, pull, reviews, comparison_input, comparison, file_input, preview = expected
    assert request(base_url, f"{REPOSITORY}/pulls", "POST", creation, token=token) == {"number": pull["pull"]["number"]}
    request(base_url, f"{api}/reviews", "POST", old)
    assert request(base_url, api) == pull
    assert request(base_url, f"{api}/reviews") == reviews
    assert request(base_url, f"{api}/comparison", "POST", comparison_input, token=token) == comparison
    assert request(base_url, f"{api}/comparison", "POST", file_input, token=token) == preview
    print("PASS: edited pull, bound reviews, comparisons, previews and old retries retain exact state across recovery", flush=True)
