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
    oid BLOB NOT NULL CHECK(length(oid) = 20),
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
