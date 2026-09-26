CREATE TABLE objects (
    oid BLOB PRIMARY KEY CHECK(length(oid) = 20),
    kind TEXT NOT NULL CHECK(kind IN ('blob', 'tree', 'commit', 'tag')),
    size INTEGER NOT NULL CHECK(size >= 0),
    digest BLOB NOT NULL CHECK(length(digest) = 32),
    storage TEXT NOT NULL CHECK(storage IN ('inline', 'external')),
    body BLOB,
    external_sha256 BLOB,
    CHECK(
        (storage = 'inline' AND body IS NOT NULL AND external_sha256 IS NULL AND size = length(body))
        OR
        (storage = 'external' AND kind = 'blob' AND body IS NULL AND length(external_sha256) = 32)
    )
) WITHOUT ROWID;

CREATE TABLE refs (
    name TEXT PRIMARY KEY,
    oid BLOB CHECK(oid IS NULL OR length(oid) = 20),
    version INTEGER NOT NULL CHECK(version > 0)
) WITHOUT ROWID;

CREATE TABLE lfs_objects (
    sha256 BLOB PRIMARY KEY CHECK(length(sha256) = 32),
    size INTEGER NOT NULL CHECK(size >= 0),
    digest BLOB NOT NULL CHECK(length(digest) = 32)
) WITHOUT ROWID;

CREATE TABLE repository_identity (
    singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
    owner TEXT NOT NULL
) WITHOUT ROWID;

CREATE TABLE repository_members (
    account TEXT PRIMARY KEY,
    role TEXT NOT NULL CHECK(role IN ('read', 'write'))
) WITHOUT ROWID;

CREATE TABLE pushes (
    id BLOB PRIMARY KEY CHECK(length(id) = 16),
    actor TEXT NOT NULL,
    request_digest BLOB NOT NULL CHECK(length(request_digest) = 32),
    response_id BLOB CHECK(response_id IS NULL OR length(response_id) = 16)
) WITHOUT ROWID;

CREATE TABLE push_responses (
    id BLOB PRIMARY KEY CHECK(length(id) = 16),
    push_id BLOB NOT NULL REFERENCES pushes(id),
    status INTEGER NOT NULL CHECK(status BETWEEN 100 AND 599),
    headers TEXT NOT NULL,
    size INTEGER NOT NULL CHECK(size BETWEEN 0 AND 67108864),
    digest BLOB NOT NULL CHECK(length(digest) = 32)
) WITHOUT ROWID;

CREATE TABLE push_response_chunks (
    response_id BLOB NOT NULL REFERENCES push_responses(id),
    part INTEGER NOT NULL CHECK(part >= 0),
    body BLOB NOT NULL CHECK(length(body) BETWEEN 1 AND 524288),
    PRIMARY KEY(response_id, part)
) WITHOUT ROWID;
