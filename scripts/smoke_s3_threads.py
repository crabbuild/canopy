"""Line discussion identities, anchors and resolution across process recovery."""
import uuid
from smoke_s3_api import request


def seed(base_url, pull_state):
    api, token, _, _, _, _, comparison_input = pull_state[:7]
    patch = pull_state[-1]["patch"]
    hunk = patch["hunks"][0]
    side = "after" if hunk["new_lines"] else "before"
    line = hunk["new_start"] if side == "after" else hunk["old_start"]
    creation = {"repository_id": comparison_input["repository_id"], "id": str(uuid.uuid4()),
                "target": comparison_input["target"], "path_base64": patch["path_base64"],
                "side": side, "line": line, "body": "Durable line discussion"}
    number = request(base_url, f"{api}/threads", "POST", creation, token=token)["number"]
    thread_api = f"{api}/threads/{number}"
    reply = {"repository_id": creation["repository_id"], "id": str(uuid.uuid4()), "body": "Durable reply"}
    result = request(base_url, f"{thread_api}/comments", "POST", reply, token=token)
    request(base_url, thread_api, "PUT", {"repository_id": creation["repository_id"], "expected_version": 1, "resolved": True}, expected_status=204)
    anchored = {**comparison_input, "target": {"kind": "thread", "number": number},
                "query": {"kind": "patch", "path_base64": patch["path_base64"]}}
    assert request(base_url, f"{api}/comparison", "POST", anchored, token=token)["patch"] == patch
    return {"api": api, "token": token, "number": number, "creation": creation, "reply": reply,
            "reply_result": result, "thread": request(base_url, thread_api),
            "comments": request(base_url, f"{thread_api}/comments"), "anchored": anchored, "patch": patch}


def verify(base_url, expected):
    api, token = expected["api"], expected["token"]
    thread_api = f"{api}/threads/{expected['number']}"
    assert request(base_url, f"{api}/threads", "POST", expected["creation"], token=token) == {"number": expected["number"]}
    assert request(base_url, f"{thread_api}/comments", "POST", expected["reply"], token=token) == expected["reply_result"]
    assert request(base_url, thread_api, token=token) == expected["thread"]
    assert request(base_url, f"{thread_api}/comments", token=token) == expected["comments"]
    assert request(base_url, f"{api}/comparison", "POST", expected["anchored"], token=token)["patch"] == expected["patch"]
    print("PASS: line discussion, replies, resolution and exact anchored patch survive recovery and retries", flush=True)
