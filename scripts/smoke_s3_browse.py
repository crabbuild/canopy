"""Pinned repository browser snapshots and embedded assets across owner loss."""
import urllib.request
from smoke_s3_api import request

REPOSITORY = "/api/repositories/other"


def seed(base_url, repository_id, commit):
    queries = [
        {"kind": "tree", "commit": commit, "path_base64": ""},
        {"kind": "history", "commit": commit},
    ]
    tree = request(base_url, f"{REPOSITORY}/browse", "POST", {"repository_id": repository_id, "query": queries[0]})
    entry = next(entry for entry in tree["view"]["tree"]["entries"] if entry["kind"] == "file")
    queries.append({"kind": "file", "commit": commit, "path_base64": entry["path_base64"]})
    return [(payload, request(base_url, f"{REPOSITORY}/browse", "POST", payload))
            for query in queries for payload in [{"repository_id": repository_id, "query": query}]]


def verify(base_url, snapshots):
    for payload, expected in snapshots:
        assert request(base_url, f"{REPOSITORY}/browse", "POST", payload) == expected
    for path, content_type in (("/", "text/html"), ("/assets/canopy.js", "text/javascript"), ("/assets/canopy.css", "text/css"), ("/assets/issues.js", "text/javascript"), ("/assets/issues.css", "text/css")):
        with urllib.request.urlopen(f"{base_url}{path}", timeout=30) as response:
            assert response.status == 200
            assert response.headers["Content-Type"].startswith(content_type)
            assert response.headers["Cache-Control"] == "no-store"
            assert "frame-ancestors 'none'" in response.headers["Content-Security-Policy"]
            assert response.read()
    print("PASS: pinned repository tree, history and file views survive recovery; embedded UI assets remain available", flush=True)
