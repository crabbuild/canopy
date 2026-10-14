-- Proposed fresh-format storage schema. Not a migration and not loaded by Canopy.
-- Earlier per-object Cell design fixture. Superseded for the large-team release
-- by docs/large-team-scalability.md; do not install this as that release schema.
-- Compose with the unchanged product tables using check_packed_repository_schema.py.
PRAGMA foreign_keys = ON;

CREATE TABLE pack_operations (
    id BLOB PRIMARY KEY CHECK(length(id) = 16),
    kind TEXT NOT NULL CHECK(kind IN ('ingest', 'compact')),
    incarnation BLOB NOT NULL CHECK(length(incarnation) = 16),
    owner_epoch INTEGER NOT NULL CHECK(owner_epoch >= 0),
    attempt INTEGER NOT NULL CHECK(attempt > 0),
    state TEXT NOT NULL CHECK(state IN ('open', 'ready', 'complete')),
    created_ms INTEGER NOT NULL CHECK(created_ms >= 0),
    updated_ms INTEGER NOT NULL CHECK(updated_ms >= created_ms)
) WITHOUT ROWID;

CREATE INDEX pack_operations_by_state ON pack_operations(state, kind, id);

CREATE TABLE packs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    operation_id BLOB NOT NULL REFERENCES pack_operations(id),
    digest BLOB NOT NULL CHECK(length(digest) = 32),
    object_format TEXT NOT NULL CHECK(object_format IN ('sha1', 'sha256')),
    git_checksum BLOB NOT NULL,
    size INTEGER NOT NULL CHECK(size BETWEEN 1 AND 549755813888),
    manifest_digest BLOB NOT NULL CHECK(length(manifest_digest) = 32),
    index_size INTEGER NOT NULL CHECK(index_size BETWEEN 1 AND 549755813888),
    index_digest BLOB NOT NULL CHECK(length(index_digest) = 32),
    index_manifest_digest BLOB NOT NULL CHECK(length(index_manifest_digest) = 32),
    object_count INTEGER NOT NULL CHECK(object_count BETWEEN 1 AND 4294967295),
    inventory_digest BLOB NOT NULL CHECK(length(inventory_digest) = 32),
    staged_count INTEGER NOT NULL DEFAULT 0 CHECK(staged_count >= 0 AND staged_count <= object_count),
    staged_digest BLOB NOT NULL CHECK(length(staged_digest) = 32),
    last_oid BLOB,
    sealed_generation INTEGER UNIQUE CHECK(sealed_generation IS NULL OR sealed_generation > 0),
    state TEXT NOT NULL DEFAULT 'staging'
        CHECK(state IN ('staging', 'sealed', 'retired', 'deleting', 'deleted')),
    CHECK(length(git_checksum) = CASE object_format WHEN 'sha1' THEN 20 ELSE 32 END),
    CHECK(last_oid IS NULL OR length(last_oid) = length(git_checksum)),
    CHECK((staged_count = 0) = (last_oid IS NULL)),
    CHECK((state = 'staging') = (sealed_generation IS NULL)),
    CHECK(state = 'staging' OR (staged_count = object_count AND staged_digest = inventory_digest))
);
CREATE UNIQUE INDEX live_pack_digest ON packs(digest) WHERE state != 'deleted';
CREATE INDEX packs_by_operation ON packs(operation_id, id);
CREATE INDEX packs_by_state ON packs(state, id);
CREATE INDEX packs_by_generation ON packs(sealed_generation, id);

CREATE TABLE pack_operation_inputs (
    operation_id BLOB NOT NULL REFERENCES pack_operations(id),
    pack_id INTEGER NOT NULL REFERENCES packs(id),
    PRIMARY KEY(operation_id, pack_id)
) WITHOUT ROWID;
CREATE INDEX pack_inputs_by_pack ON pack_operation_inputs(pack_id, operation_id);

CREATE TABLE objects (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    oid BLOB NOT NULL UNIQUE CHECK(length(oid) IN (20, 32)),
    kind TEXT NOT NULL CHECK(kind IN ('blob', 'tree', 'commit', 'tag')),
    size INTEGER NOT NULL CHECK(size >= 0),
    digest BLOB NOT NULL CHECK(length(digest) = 32),
    pack_id INTEGER NOT NULL REFERENCES packs(id),
    location_version INTEGER NOT NULL DEFAULT 1 CHECK(location_version > 0),
    edge_count INTEGER NOT NULL CHECK(edge_count >= 0),
    edge_digest BLOB NOT NULL CHECK(length(edge_digest) = 32),
    CHECK(kind != 'blob' OR edge_count = 0)
);
CREATE INDEX objects_by_pack ON objects(pack_id, sequence);

CREATE TABLE object_edges (
    parent BLOB NOT NULL REFERENCES objects(oid),
    child BLOB NOT NULL REFERENCES objects(oid),
    expected_kind TEXT NOT NULL CHECK(expected_kind IN ('blob', 'tree', 'commit', 'tag')),
    waiting INTEGER NOT NULL CHECK(waiting IN (0, 1)),
    PRIMARY KEY(parent, child)
) WITHOUT ROWID;
CREATE INDEX object_edges_by_child ON object_edges(child, waiting, parent);

CREATE TABLE object_closure (
    oid BLOB PRIMARY KEY REFERENCES objects(oid) CHECK(length(oid) IN (20, 32))
) WITHOUT ROWID;

-- Keep a certified child here until all waiting reverse edges are propagated.
CREATE TABLE object_pending (
    sequence INTEGER PRIMARY KEY REFERENCES objects(sequence),
    oid BLOB NOT NULL UNIQUE REFERENCES objects(oid),
    received_edges INTEGER NOT NULL DEFAULT 0 CHECK(received_edges >= 0),
    edge_digest BLOB NOT NULL CHECK(length(edge_digest) = 32),
    last_child BLOB CHECK(last_child IS NULL OR length(last_child) IN (20, 32)),
    remaining_children INTEGER NOT NULL DEFAULT 0 CHECK(remaining_children >= 0),
    edges_complete INTEGER NOT NULL DEFAULT 0 CHECK(edges_complete IN (0, 1)),
    CHECK((received_edges = 0) = (last_child IS NULL)),
    CHECK(remaining_children <= received_edges)
);
CREATE INDEX pending_ready ON object_pending(edges_complete, remaining_children, sequence);

CREATE TABLE refs (
    name TEXT PRIMARY KEY,
    oid BLOB CHECK(oid IS NULL OR length(oid) IN (20, 32)),
    version INTEGER NOT NULL CHECK(version > 0)
) WITHOUT ROWID;
CREATE INDEX refs_by_oid ON refs(oid) WHERE oid IS NOT NULL;

CREATE TABLE ref_generation (
    singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
    generation INTEGER NOT NULL CHECK(typeof(generation) = 'integer' AND generation >= 0),
    pack_generation INTEGER NOT NULL DEFAULT 0
        CHECK(typeof(pack_generation) = 'integer' AND pack_generation >= 0),
    default_branch TEXT NOT NULL,
    visibility TEXT NOT NULL DEFAULT 'private' CHECK(visibility IN ('private', 'public'))
) WITHOUT ROWID;
INSERT INTO ref_generation(singleton, generation, default_branch)
    VALUES (1, 0, 'refs/heads/main');

-- Identity and format protection belongs in SQL as well as typed commands.
-- repository_identity is supplied by the unchanged product schema.
CREATE TRIGGER packs_format BEFORE INSERT ON packs BEGIN
    SELECT CASE WHEN NOT EXISTS (
        SELECT 1 FROM repository_identity WHERE singleton = 1 AND object_format = NEW.object_format
    ) THEN RAISE(ABORT, 'repository object format mismatch') END;
END;
CREATE TRIGGER objects_format BEFORE INSERT ON objects BEGIN
    SELECT CASE WHEN NOT EXISTS (
        SELECT 1 FROM packs p JOIN repository_identity r ON r.singleton = 1
        WHERE p.id = NEW.pack_id AND p.state = 'staging'
        AND p.object_format = r.object_format
        AND length(NEW.oid) = CASE r.object_format WHEN 'sha1' THEN 20 ELSE 32 END
    ) THEN RAISE(ABORT, 'invalid object placement') END;
END;
CREATE TRIGGER objects_identity BEFORE UPDATE OF sequence, oid, kind, size, digest, edge_count, edge_digest ON objects BEGIN
    SELECT RAISE(ABORT, 'canonical object identity is immutable');
END;
CREATE TRIGGER objects_move BEFORE UPDATE OF pack_id, location_version ON objects BEGIN
    SELECT CASE WHEN NEW.location_version != OLD.location_version + 1 OR NOT EXISTS (
        SELECT 1 FROM packs WHERE id = NEW.pack_id AND state = 'sealed'
    ) THEN RAISE(ABORT, 'invalid object location switch') END;
END;
CREATE TRIGGER packs_retire BEFORE UPDATE OF state ON packs
WHEN NEW.state IN ('retired', 'deleting', 'deleted') BEGIN
    SELECT CASE WHEN EXISTS (SELECT 1 FROM objects WHERE pack_id = OLD.id)
        THEN RAISE(ABORT, 'pack still owns objects') END;
END;
