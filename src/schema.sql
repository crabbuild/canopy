CREATE TABLE object_uploads (
    id BLOB PRIMARY KEY CHECK(length(id) = 16)
) WITHOUT ROWID;

CREATE TABLE object_chunks (
    upload_id BLOB NOT NULL REFERENCES object_uploads(id),
    part INTEGER NOT NULL CHECK(part BETWEEN 0 AND 127),
    body BLOB NOT NULL CHECK(length(body) BETWEEN 1 AND 524288),
    PRIMARY KEY(upload_id, part)
) WITHOUT ROWID;

CREATE TABLE objects (
    oid BLOB PRIMARY KEY CHECK(length(oid) = 20),
    kind TEXT NOT NULL CHECK(kind IN ('blob', 'tree', 'commit', 'tag')),
    size INTEGER NOT NULL CHECK(size >= 0),
    digest BLOB NOT NULL CHECK(length(digest) = 32),
    storage TEXT NOT NULL CHECK(storage IN ('inline', 'external', 'chunked')),
    body BLOB,
    external_sha256 BLOB,
    chunk_id BLOB UNIQUE REFERENCES object_uploads(id) CHECK(chunk_id IS NULL OR length(chunk_id) = 16),
    CHECK(
        (storage = 'inline' AND body IS NOT NULL AND external_sha256 IS NULL AND chunk_id IS NULL AND size = length(body))
        OR
        (storage = 'external' AND kind = 'blob' AND body IS NULL AND chunk_id IS NULL AND length(external_sha256) = 32)
        OR
        (storage = 'chunked' AND kind != 'blob' AND body IS NULL AND external_sha256 IS NULL AND chunk_id IS NOT NULL AND size BETWEEN 786433 AND 67108864)
    )
) WITHOUT ROWID;

CREATE TABLE object_closure (
    oid BLOB PRIMARY KEY REFERENCES objects(oid) CHECK(length(oid) = 20)
) WITHOUT ROWID;

CREATE TABLE refs (
    name TEXT PRIMARY KEY,
    oid BLOB CHECK(oid IS NULL OR length(oid) = 20),
    version INTEGER NOT NULL CHECK(version > 0)
) WITHOUT ROWID;

CREATE TABLE ref_generation (
    singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
    generation INTEGER NOT NULL CHECK(typeof(generation) = 'integer' AND generation >= 0),
    default_branch TEXT NOT NULL
) WITHOUT ROWID;
INSERT INTO ref_generation (singleton, generation, default_branch) VALUES (1, 0, 'refs/heads/main');

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

CREATE TABLE issues (
    number INTEGER PRIMARY KEY AUTOINCREMENT,
    id BLOB NOT NULL UNIQUE CHECK(length(id) = 16),
    creation_digest BLOB NOT NULL CHECK(length(creation_digest) = 32),
    author TEXT NOT NULL,
    title TEXT NOT NULL CHECK(length(CAST(title AS BLOB)) BETWEEN 1 AND 256),
    body TEXT NOT NULL CHECK(length(CAST(body AS BLOB)) <= 16384),
    state TEXT NOT NULL CHECK(state IN ('open', 'closed')),
    version INTEGER NOT NULL CHECK(typeof(version) = 'integer' AND version > 0),
    created_ms INTEGER NOT NULL CHECK(created_ms >= 0),
    updated_ms INTEGER NOT NULL CHECK(updated_ms >= created_ms)
);
CREATE INDEX issues_by_state ON issues(state, number);

CREATE TABLE issue_comments (
    number INTEGER PRIMARY KEY AUTOINCREMENT,
    issue_number INTEGER NOT NULL REFERENCES issues(number),
    id BLOB NOT NULL UNIQUE CHECK(length(id) = 16),
    creation_digest BLOB NOT NULL CHECK(length(creation_digest) = 32),
    author TEXT NOT NULL,
    body TEXT NOT NULL CHECK(length(CAST(body AS BLOB)) BETWEEN 1 AND 16384),
    version INTEGER NOT NULL CHECK(typeof(version) = 'integer' AND version > 0),
    created_ms INTEGER NOT NULL CHECK(created_ms >= 0),
    updated_ms INTEGER NOT NULL CHECK(updated_ms >= created_ms)
);
CREATE INDEX comments_by_issue ON issue_comments(issue_number, number);

CREATE TABLE check_contexts (
    name TEXT PRIMARY KEY,
    reporter TEXT NOT NULL,
    enabled INTEGER NOT NULL CHECK(enabled IN (0, 1)),
    version INTEGER NOT NULL CHECK(typeof(version) = 'integer' AND version > 0)
) WITHOUT ROWID;

CREATE INDEX check_contexts_by_enabled ON check_contexts(enabled, name);

CREATE TABLE check_runs (
    number INTEGER PRIMARY KEY AUTOINCREMENT,
    id BLOB NOT NULL UNIQUE CHECK(length(id) = 16),
    oid BLOB NOT NULL REFERENCES objects(oid) CHECK(length(oid) = 20),
    context TEXT NOT NULL REFERENCES check_contexts(name),
    context_version INTEGER NOT NULL CHECK(context_version > 0),
    reporter TEXT NOT NULL,
    state TEXT NOT NULL CHECK(state IN ('queued', 'in_progress', 'success', 'failure', 'cancelled')),
    version INTEGER NOT NULL CHECK(typeof(version) = 'integer' AND version > 0),
    summary TEXT NOT NULL CHECK(length(CAST(summary AS BLOB)) <= 4096),
    created_ms INTEGER NOT NULL CHECK(created_ms >= 0),
    updated_ms INTEGER NOT NULL CHECK(updated_ms >= created_ms)
);
CREATE INDEX check_runs_by_commit ON check_runs(oid, context, context_version, number);
