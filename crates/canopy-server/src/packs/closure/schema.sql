CREATE TABLE inputs (
    digest BLOB PRIMARY KEY CHECK(length(digest)=32)
) WITHOUT ROWID;
CREATE TABLE objects (
    oid BLOB PRIMARY KEY CHECK(length(oid) IN (20,32)),
    kind TEXT NOT NULL CHECK(kind IN ('blob','tree','commit','tag')),
    size INTEGER NOT NULL CHECK(size>=0),
    digest BLOB NOT NULL CHECK(length(digest)=32),
    edge_count INTEGER NOT NULL CHECK(edge_count>=0),
    edge_digest BLOB NOT NULL CHECK(length(edge_digest)=32),
    pending INTEGER NOT NULL DEFAULT 0 CHECK(pending>=0),
    done INTEGER NOT NULL DEFAULT 0 CHECK(done IN (0,1))
) WITHOUT ROWID;
CREATE TABLE object_edges (
    parent BLOB NOT NULL REFERENCES objects(oid),
    child BLOB NOT NULL CHECK(length(child) IN (20,32)),
    expected_kind TEXT NOT NULL CHECK(expected_kind IN ('blob','tree','commit','tag')),
    PRIMARY KEY(parent,child)
) WITHOUT ROWID;
CREATE INDEX edge_child ON object_edges(child,parent);
CREATE INDEX ready_objects ON objects(oid) WHERE pending=0 AND done=0;
CREATE TABLE lookups (
    oid BLOB PRIMARY KEY CHECK(length(oid) IN (20,32)),
    resolved INTEGER NOT NULL DEFAULT 0 CHECK(resolved IN (0,1))
) WITHOUT ROWID;
CREATE INDEX unresolved_lookups ON lookups(oid) WHERE resolved=0;
CREATE TABLE base_objects (
    oid BLOB PRIMARY KEY CHECK(length(oid) IN (20,32)),
    kind TEXT NOT NULL CHECK(kind IN ('blob','tree','commit','tag')),
    size INTEGER NOT NULL CHECK(size>=0),
    digest BLOB NOT NULL CHECK(length(digest)=32),
    edge_count INTEGER NOT NULL CHECK(edge_count>=0),
    edge_digest BLOB NOT NULL CHECK(length(edge_digest)=32)
) WITHOUT ROWID;
