PRAGMA application_id = 1128353357;
PRAGMA user_version = 1;

CREATE TABLE segment_identity (
    singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
    repository_id BLOB NOT NULL CHECK(length(repository_id) = 16),
    operation_id BLOB NOT NULL CHECK(length(operation_id) = 16),
    object_format TEXT NOT NULL CHECK(object_format IN ('sha1', 'sha256')),
    pack_digest BLOB NOT NULL CHECK(length(pack_digest) = 32),
    git_checksum BLOB NOT NULL,
    first_ordinal INTEGER NOT NULL CHECK(first_ordinal BETWEEN 0 AND 4294967295),
    object_count INTEGER NOT NULL CHECK(object_count BETWEEN 1 AND 4294967295),
    edge_count INTEGER NOT NULL CHECK(edge_count >= 0),
    inventory_digest BLOB NOT NULL CHECK(length(inventory_digest) = 32),
    CHECK(length(git_checksum) = CASE object_format WHEN 'sha1' THEN 20 ELSE 32 END),
    CHECK(first_ordinal + object_count <= 4294967295)
) WITHOUT ROWID;

-- Canonical identity fields and typed graph meaning reuse the Repository Cell
-- model. There are no SQL object bodies or preferred-location updates here.
CREATE TABLE objects (
    oid BLOB PRIMARY KEY CHECK(length(oid) IN (20, 32)),
    kind TEXT NOT NULL CHECK(kind IN ('blob', 'tree', 'commit', 'tag')),
    size INTEGER NOT NULL CHECK(size >= 0),
    digest BLOB NOT NULL CHECK(length(digest) = 32),
    edge_count INTEGER CHECK(edge_count >= 0),
    edge_digest BLOB CHECK(length(edge_digest) = 32),
    CHECK((edge_count IS NULL) = (edge_digest IS NULL)),
    CHECK(kind != 'blob' OR edge_count IS NULL OR edge_count = 0)
) WITHOUT ROWID;

CREATE TABLE object_edges (
    parent BLOB NOT NULL REFERENCES objects(oid),
    child BLOB NOT NULL CHECK(length(child) IN (20, 32)),
    expected_kind TEXT NOT NULL CHECK(expected_kind IN ('blob', 'tree', 'commit', 'tag')),
    PRIMARY KEY(parent, child)
) WITHOUT ROWID;
-- A child may be in a dependency generation, rather than this physical pack.
-- The catalog verifier must certify that dependency before publication.
