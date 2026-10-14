PRAGMA application_id = 1128353358;
PRAGMA user_version = 1;

CREATE TABLE directory_identity (
    singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
    repository_id BLOB NOT NULL CHECK(length(repository_id) = 16),
    operation_id BLOB NOT NULL CHECK(length(operation_id) = 16),
    object_format TEXT NOT NULL CHECK(object_format IN ('sha1', 'sha256')),
    object_count INTEGER NOT NULL CHECK(object_count > 0),
    first_oid BLOB NOT NULL,
    last_oid BLOB NOT NULL,
    inventory_digest BLOB NOT NULL CHECK(length(inventory_digest) = 32),
    CHECK(length(first_oid) = CASE object_format WHEN 'sha1' THEN 20 ELSE 32 END),
    CHECK(length(last_oid) = length(first_oid)),
    CHECK(first_oid <= last_oid)
) WITHOUT ROWID;

-- Reuse canonical metadata and graph inventory, never native pack offsets.
-- The selected catalog's descriptor tree resolves the source key.
CREATE TABLE objects (
    oid BLOB PRIMARY KEY CHECK(length(oid) IN (20, 32)),
    kind TEXT NOT NULL CHECK(kind IN ('blob', 'tree', 'commit', 'tag')),
    size INTEGER NOT NULL CHECK(size >= 0),
    digest BLOB NOT NULL CHECK(length(digest) = 32),
    edge_count INTEGER NOT NULL CHECK(edge_count >= 0),
    edge_digest BLOB NOT NULL CHECK(length(edge_digest) = 32),
    source_operation BLOB NOT NULL CHECK(length(source_operation) = 16),
    source_digest BLOB NOT NULL CHECK(length(source_digest) = 32),
    location_version INTEGER NOT NULL CHECK(location_version > 0),
    CHECK(kind != 'blob' OR edge_count = 0),
    CHECK(kind != 'commit' OR edge_count > 0),
    CHECK(kind != 'tag' OR edge_count = 1)
) WITHOUT ROWID;
