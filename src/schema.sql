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
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    oid BLOB NOT NULL UNIQUE CHECK(length(oid) = 20),
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
);

CREATE INDEX objects_structure_sequence ON objects(sequence) WHERE kind != 'blob';

CREATE TABLE object_edges (
    parent BLOB NOT NULL REFERENCES objects(oid),
    child BLOB NOT NULL REFERENCES objects(oid),
    PRIMARY KEY(parent, child)
) WITHOUT ROWID;
CREATE INDEX object_edges_by_child ON object_edges(child, parent);

CREATE TABLE object_closure (
    oid BLOB PRIMARY KEY REFERENCES objects(oid) CHECK(length(oid) = 20)
) WITHOUT ROWID;

CREATE TABLE refs (
    name TEXT PRIMARY KEY,
    oid BLOB CHECK(oid IS NULL OR length(oid) = 20),
    version INTEGER NOT NULL CHECK(version > 0)
) WITHOUT ROWID;

CREATE INDEX refs_by_oid ON refs(oid) WHERE oid IS NOT NULL;

CREATE TABLE ref_generation (
    singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
    generation INTEGER NOT NULL CHECK(typeof(generation) = 'integer' AND generation >= 0),
    default_branch TEXT NOT NULL,
    visibility TEXT NOT NULL DEFAULT 'private' CHECK(visibility IN ('private', 'public'))
) WITHOUT ROWID;
INSERT INTO ref_generation (singleton, generation, default_branch) VALUES (1, 0, 'refs/heads/main');

CREATE TABLE lfs_objects (
    sha256 BLOB PRIMARY KEY CHECK(length(sha256) = 32),
    size INTEGER NOT NULL CHECK(size >= 0),
    digest BLOB NOT NULL CHECK(length(digest) = 32)
) WITHOUT ROWID;

CREATE TABLE repository_identity (
    singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
    repository_id BLOB NOT NULL CHECK(length(repository_id) = 16),
    owner TEXT NOT NULL
) WITHOUT ROWID;

CREATE TABLE repository_members (
    account TEXT PRIMARY KEY,
    role TEXT NOT NULL CHECK(role IN ('read', 'write'))
) WITHOUT ROWID;

CREATE TABLE membership_versions (
    account TEXT PRIMARY KEY,
    version INTEGER NOT NULL CHECK(typeof(version) = 'integer' AND version > 0)
) WITHOUT ROWID;

CREATE TABLE pushes (
    id BLOB PRIMARY KEY CHECK(length(id) = 16),
    actor TEXT NOT NULL,
    request_digest BLOB NOT NULL CHECK(length(request_digest) = 32),
    response_id BLOB CHECK(response_id IS NULL OR length(response_id) = 16),
    rejected INTEGER CHECK(rejected IN (0, 1)),
    CHECK((response_id IS NULL) = (rejected IS NULL))
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

CREATE TABLE push_plan_chunks (
    response_id BLOB NOT NULL REFERENCES push_responses(id),
    part INTEGER NOT NULL CHECK(part >= 0),
    body BLOB NOT NULL CHECK(length(body) BETWEEN 1 AND 65536),
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

CREATE TABLE commit_parents (
    child BLOB NOT NULL REFERENCES objects(oid) CHECK(length(child) = 20),
    parent BLOB NOT NULL REFERENCES objects(oid) CHECK(length(parent) = 20),
    PRIMARY KEY(child, parent)
) WITHOUT ROWID;

CREATE TABLE commit_ancestry (
    ancestor BLOB NOT NULL REFERENCES objects(oid) CHECK(length(ancestor) = 20),
    descendant BLOB NOT NULL REFERENCES objects(oid) CHECK(length(descendant) = 20),
    PRIMARY KEY(ancestor, descendant)
) WITHOUT ROWID;

CREATE TABLE branch_rules (
    reference TEXT PRIMARY KEY,
    version INTEGER NOT NULL CHECK(typeof(version) = 'integer' AND version > 0),
    enabled INTEGER NOT NULL CHECK(enabled IN (0, 1)),
    deny_deletions INTEGER NOT NULL CHECK(deny_deletions IN (0, 1)),
    fast_forward INTEGER NOT NULL CHECK(fast_forward IN (0, 1)),
    require_pull_request INTEGER NOT NULL CHECK(require_pull_request IN (0, 1)),
    required_approvals INTEGER NOT NULL CHECK(required_approvals BETWEEN 0 AND 16 AND (require_pull_request = 1 OR required_approvals = 0))
) WITHOUT ROWID;
CREATE TABLE branch_required_checks (
    reference TEXT NOT NULL REFERENCES branch_rules(reference),
    context TEXT NOT NULL,
    PRIMARY KEY(reference, context)
) WITHOUT ROWID;
CREATE INDEX branch_rules_by_enabled ON branch_rules(enabled, reference);

CREATE TABLE pull_requests (
    number INTEGER PRIMARY KEY AUTOINCREMENT,
    id BLOB NOT NULL UNIQUE CHECK(length(id) = 16),
    creation_digest BLOB NOT NULL CHECK(length(creation_digest) = 32),
    author TEXT NOT NULL,
    title TEXT NOT NULL CHECK(length(CAST(title AS BLOB)) BETWEEN 1 AND 256),
    body TEXT NOT NULL CHECK(length(CAST(body AS BLOB)) <= 16384),
    state TEXT NOT NULL CHECK(state IN ('open', 'closed', 'merged')),
    draft INTEGER NOT NULL CHECK(draft IN (0, 1)),
    version INTEGER NOT NULL CHECK(typeof(version) = 'integer' AND version > 0),
    source_ref TEXT NOT NULL REFERENCES refs(name),
    base_ref TEXT NOT NULL REFERENCES refs(name) CHECK(source_ref != base_ref),
    initial_source_oid BLOB NOT NULL REFERENCES objects(oid) CHECK(length(initial_source_oid) = 20),
    initial_base_oid BLOB NOT NULL REFERENCES objects(oid) CHECK(length(initial_base_oid) = 20),
    created_ms INTEGER NOT NULL CHECK(created_ms >= 0),
    updated_ms INTEGER NOT NULL CHECK(updated_ms >= created_ms)
);
CREATE INDEX pulls_by_state ON pull_requests(state, number);

CREATE TABLE pull_reviews (
    number INTEGER PRIMARY KEY AUTOINCREMENT,
    id BLOB NOT NULL UNIQUE CHECK(length(id) = 16),
    creation_digest BLOB NOT NULL CHECK(length(creation_digest) = 32),
    pull_number INTEGER NOT NULL REFERENCES pull_requests(number),
    reviewer TEXT NOT NULL,
    membership_version INTEGER NOT NULL CHECK(membership_version >= 0),
    kind TEXT NOT NULL CHECK(kind IN ('comment', 'approve', 'request_changes')),
    body TEXT NOT NULL CHECK(length(CAST(body AS BLOB)) <= 16384),
    pull_version INTEGER NOT NULL CHECK(pull_version > 0),
    source_oid BLOB NOT NULL REFERENCES objects(oid) CHECK(length(source_oid) = 20),
    source_version INTEGER NOT NULL CHECK(source_version > 0),
    base_oid BLOB NOT NULL REFERENCES objects(oid) CHECK(length(base_oid) = 20),
    base_version INTEGER NOT NULL CHECK(base_version > 0),
    created_ms INTEGER NOT NULL CHECK(created_ms >= 0)
);
CREATE INDEX reviews_by_pull ON pull_reviews(pull_number, number);

CREATE TABLE pull_review_heads (
    pull_number INTEGER NOT NULL REFERENCES pull_requests(number),
    reviewer TEXT NOT NULL,
    review_number INTEGER NOT NULL REFERENCES pull_reviews(number),
    PRIMARY KEY(pull_number, reviewer)
) WITHOUT ROWID;

CREATE TABLE pull_merges (
    id BLOB PRIMARY KEY CHECK(length(id) = 16),
    binding BLOB NOT NULL CHECK(length(binding) = 32),
    pull_number INTEGER NOT NULL UNIQUE REFERENCES pull_requests(number),
    oid BLOB NOT NULL REFERENCES objects(oid) CHECK(length(oid) = 20),
    merged_ms INTEGER NOT NULL CHECK(merged_ms >= 0),
    pull_version INTEGER NOT NULL CHECK(pull_version > 0),
    source_oid BLOB NOT NULL REFERENCES objects(oid) CHECK(length(source_oid) = 20),
    source_version INTEGER NOT NULL CHECK(source_version > 0),
    base_oid BLOB NOT NULL REFERENCES objects(oid) CHECK(length(base_oid) = 20),
    base_version INTEGER NOT NULL CHECK(base_version > 0)
) WITHOUT ROWID;

CREATE TABLE merge_candidates (
    id BLOB PRIMARY KEY CHECK(length(id) = 16),
    binding BLOB NOT NULL CHECK(length(binding) = 32),
    pull_number INTEGER NOT NULL REFERENCES pull_requests(number),
    actor TEXT NOT NULL,
    request TEXT NOT NULL CHECK(length(CAST(request AS BLOB)) <= 131072),
    created_ms INTEGER NOT NULL CHECK(created_ms >= 0),
    result TEXT NOT NULL CHECK(length(CAST(result AS BLOB)) <= 262144),
    source_oid BLOB NOT NULL REFERENCES objects(oid),
    base_oid BLOB NOT NULL REFERENCES objects(oid),
    oid BLOB REFERENCES objects(oid)
) WITHOUT ROWID;

CREATE TABLE pull_threads (
    number INTEGER PRIMARY KEY AUTOINCREMENT,
    id BLOB NOT NULL UNIQUE CHECK(length(id) = 16),
    creation_digest BLOB NOT NULL CHECK(length(creation_digest) = 32),
    pull_number INTEGER NOT NULL REFERENCES pull_requests(number),
    author TEXT NOT NULL,
    body TEXT NOT NULL CHECK(length(CAST(body AS BLOB)) BETWEEN 1 AND 16384),
    resolved INTEGER NOT NULL CHECK(resolved IN (0, 1)),
    version INTEGER NOT NULL CHECK(typeof(version) = 'integer' AND version > 0),
    pull_version INTEGER NOT NULL CHECK(pull_version > 0),
    source_oid BLOB NOT NULL REFERENCES objects(oid),
    source_version INTEGER NOT NULL CHECK(source_version > 0),
    base_oid BLOB NOT NULL REFERENCES objects(oid),
    base_version INTEGER NOT NULL CHECK(base_version > 0),
    merge_base BLOB NOT NULL REFERENCES objects(oid),
    path BLOB NOT NULL CHECK(length(path) BETWEEN 1 AND 4096),
    side TEXT NOT NULL CHECK(side IN ('before', 'after')),
    line INTEGER NOT NULL CHECK(line BETWEEN 1 AND 20000),
    blob_oid BLOB NOT NULL REFERENCES objects(oid),
    created_ms INTEGER NOT NULL CHECK(created_ms >= 0),
    updated_ms INTEGER NOT NULL CHECK(updated_ms >= created_ms)
);
CREATE INDEX threads_by_pull ON pull_threads(pull_number, number);

CREATE TABLE pull_thread_comments (
    number INTEGER PRIMARY KEY AUTOINCREMENT,
    id BLOB NOT NULL UNIQUE CHECK(length(id) = 16),
    creation_digest BLOB NOT NULL CHECK(length(creation_digest) = 32),
    thread_number INTEGER NOT NULL REFERENCES pull_threads(number),
    author TEXT NOT NULL,
    body TEXT NOT NULL CHECK(length(CAST(body AS BLOB)) BETWEEN 1 AND 16384),
    created_ms INTEGER NOT NULL CHECK(created_ms >= 0)
);
CREATE INDEX comments_by_thread ON pull_thread_comments(thread_number, number);
