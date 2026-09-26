"""Credential admission and retained-usage checks against a real Canopy process."""

import secrets
import urllib.error
import urllib.parse
import uuid

from smoke_s3_api import request

API = "/api/accounts/limited/tokens"


def credential():
    return {"id": str(uuid.uuid4()), "token": f"cnp_{secrets.token_hex(32)}", "scope": "read"}


def expect(base, path, method, payload=None, *, token="local-test-token", status=204):
    try:
        return request(base, path, method, payload, token=token, expected_status=status)
    except urllib.error.HTTPError as error:
        assert error.code == status, (error.code, status)
        return None


def records(base):
    rows, cursor = [], None
    for _ in range(10):
        suffix = "" if cursor is None else f"?after={urllib.parse.quote(cursor)}"
        page = request(base, API + suffix)
        rows.extend(page["tokens"])
        cursor = page["next_after"]
        if cursor is None:
            return rows
    raise RuntimeError("token pagination did not terminate")


def seed(base):
    administrator = f"cnp_{secrets.token_hex(32)}"
    expect(base, "/api/accounts", "POST",
           {"name": "limited", "token": administrator, "scope": "admin"}, status=200)
    initial = records(base)[0]["id"]
    issued = []
    for _ in range(63):
        entry = credential()
        expect(base, API, "POST", entry, token=administrator)
        issued.append(entry)
    blocked = credential()
    expect(base, API, "POST", blocked, status=429)
    expect(base, API, "POST", issued[0])
    assert len(records(base)) == 64
    expect(base, f"{API}/{issued[0]['id']}", "DELETE")
    expect(base, API, "POST", blocked)
    rows = records(base)
    assert len(rows) == 65
    for row in rows:
        if row["id"] != initial and row["enabled"]:
            expect(base, f"{API}/{row['id']}", "DELETE")
    for _ in range(len(rows), 256):
        entry = credential()
        expect(base, API, "POST", entry, token=administrator)
        expect(base, f"{API}/{entry['id']}", "DELETE")
    state = (administrator, initial, credential())
    verify(base, state)
    print("PASS: 64 active-token limit frees on revocation; retries do not consume issuance slots", flush=True)
    return state


def verify(base, state):
    administrator, initial, refused = state
    expect(base, API, "POST", refused, token=administrator, status=429)
    expect(base, API, "POST", refused, status=429)
    expect(base, API, "POST", {"id": initial, "token": administrator, "scope": "admin"})
    expect(base, "/api/repositories", "GET", token=refused["token"], status=401)
    rows = records(base)
    assert len(rows) == 256
    assert [row["id"] for row in rows if row["enabled"]] == [initial]
    print("PASS: retained credential history enforces 256 issuances per 24 hours across actors and recovery", flush=True)
