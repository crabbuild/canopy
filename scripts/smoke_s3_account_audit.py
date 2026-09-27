"""Account audit history through public HTTP, with a stable page for recovery."""
import hashlib
import json
import secrets
import time
import urllib.error
import uuid

from smoke_s3_api import request

PATH = "/api/audit/accounts"


def seed(base_url):
    member = "cnp_" + secrets.token_hex(32)
    creation = {"name": "audit-member", "token": member, "scope": "admin"}
    for _ in range(2):
        request(base_url, "/api/accounts", "POST", creation)
    try:
        request(base_url, PATH, token=member)
    except urllib.error.HTTPError as error:
        assert error.code == 403
    else:
        raise AssertionError("member admin read the site history")
    tokens = "/api/accounts/audit-member/tokens"
    credential = {"id": str(uuid.uuid4()), "token": "cnp_" + secrets.token_hex(32),
                  "scope": "admin", "expires_at_ms": int(time.time() * 1000) + 3600000}
    for _ in range(2):
        request(base_url, tokens, "POST", credential, token=member, expected_status=204)
    request(base_url, tokens + "/" + credential["id"], "DELETE", token=credential["token"], expected_status=204)
    request(base_url, tokens + "/" + credential["id"], "DELETE", expected_status=204)
    # Cross a page boundary with actual credential changes on one account.
    for _ in range(30):
        request(base_url, tokens, "POST", {"id": str(uuid.uuid4()), "token": "cnp_" + secrets.token_hex(32), "scope": "read"}, expected_status=204)
    for _ in range(2):
        request(base_url, "/api/accounts/audit-member/disable", "POST", expected_status=204)
    recent = request(base_url, PATH)
    assert len(recent["events"]) == 32 and recent["next_before"]
    old_path = PATH + "?before=" + recent["next_before"]
    older = request(base_url, old_path)
    matching = [event for event in recent["events"] + older["events"] if event["account"] == "audit-member"]
    assert len(matching) == 34
    revoked = next(event for event in matching if event["action"] == "token.revoked")
    assert revoked["actor"] == "audit-member"
    assert revoked["actor_token_id"] == revoked["token_id"] == credential["id"]
    assert revoked["expires_at_ms"] == credential["expires_at_ms"]
    encoded = json.dumps([recent, older])
    for secret in (member, credential["token"]):
        assert secret not in encoded and hashlib.sha256(secret.encode()).hexdigest() not in encoded
    # Pin the upper boundary so unrelated later changes do not move this page.
    path = PATH + "?before=" + str(int(recent["events"][0]["id"]) + 1)
    expected = (path, recent, old_path, older)
    verify(base_url, expected)
    return expected


def verify(base_url, expected):
    path, recent, old_path, older = expected
    assert request(base_url, path) == recent
    assert request(base_url, old_path) == older
    print("PASS: private account history preserves actor, token metadata, pagination and retry identity", flush=True)
