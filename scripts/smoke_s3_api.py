"""JSON requests shared by collaboration process probes."""

import json
import urllib.request


def request(base_url, path, method="GET", payload=None):
    request = urllib.request.Request(
        f"{base_url}{path}", method=method,
        data=None if payload is None else json.dumps(payload).encode(),
        headers={"Authorization": "Bearer local-test-token", "Content-Type": "application/json"},
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        assert response.status == (204 if method == "PUT" else 200)
        return None if response.status == 204 else json.load(response)
