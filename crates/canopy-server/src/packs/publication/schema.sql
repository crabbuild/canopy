-- Fresh canopy-pack-v1 repository schema. No legacy Git bodies or per-object placement.
-- Product OID membership and ancestry decisions require certified catalog facts.
CREATE TABLE refs (
    name TEXT PRIMARY KEY,
    oid BLOB CHECK(oid IS NULL OR length(oid) IN (20, 32)),
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

CREATE TABLE lfs_locks (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    id TEXT NOT NULL UNIQUE,
    path TEXT NOT NULL UNIQUE,
    locked_at TEXT NOT NULL,
    owner TEXT NOT NULL
);

CREATE TABLE repository_identity (
    object_format TEXT NOT NULL CHECK(object_format IN ('sha1', 'sha256')),
    singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
    repository_id BLOB NOT NULL CHECK(length(repository_id) = 16),
    owner TEXT NOT NULL,
    push_cert_seed BLOB NOT NULL CHECK(length(push_cert_seed) = 32),
    artifact_sequence INTEGER NOT NULL DEFAULT 0 CHECK(typeof(artifact_sequence) = 'integer' AND artifact_sequence >= 0)
) WITHOUT ROWID;
-- One bounded watermark, not an ever-growing namespace tombstone table.
CREATE TRIGGER repository_artifact_sequence_monotonic BEFORE UPDATE OF artifact_sequence ON repository_identity
WHEN NEW.artifact_sequence != OLD.artifact_sequence + 1
BEGIN SELECT RAISE(ABORT, 'artifact allocation must advance once'); END;
CREATE TRIGGER repository_artifact_sequence_retained BEFORE DELETE ON repository_identity
WHEN OLD.artifact_sequence > 0
BEGIN SELECT RAISE(ABORT, 'artifact allocation watermark must be retained'); END;
-- SQLite REPLACE can bypass DELETE triggers with recursive_triggers disabled.
CREATE TRIGGER repository_artifact_sequence_not_replaced BEFORE INSERT ON repository_identity
WHEN EXISTS(SELECT 1 FROM repository_identity WHERE singleton=NEW.singleton AND artifact_sequence>0)
BEGIN SELECT RAISE(ABORT, 'artifact allocation watermark cannot be replaced'); END;

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
    options TEXT NOT NULL DEFAULT '[]' CHECK(length(CAST(options AS BLOB)) <= 65536),
    response_id BLOB CHECK(response_id IS NULL OR length(response_id) = 16),
    completion_digest BLOB CHECK(completion_digest IS NULL OR length(completion_digest) = 32),
    rejected INTEGER CHECK(rejected IN (0, 1)),
    rejection_reason TEXT,
    -- Bounded exact logical publication outcome. HTTP completion additionally
    -- records its response in this same transaction; no per-object outcome rows.
    publication BLOB CHECK(publication IS NULL OR length(publication) BETWEEN 1 AND 128),
    publication_plan_digest BLOB CHECK(publication_plan_digest IS NULL OR length(publication_plan_digest) = 32),
    CHECK((publication IS NULL) = (publication_plan_digest IS NULL)),
    CHECK((response_id IS NULL) = (rejected IS NULL)),
    CHECK((response_id IS NULL) = (completion_digest IS NULL))
) WITHOUT ROWID;
CREATE TRIGGER push_publication_immutable BEFORE UPDATE OF publication,publication_plan_digest ON pushes
WHEN OLD.publication IS NOT NULL AND (NEW.publication IS NOT OLD.publication OR NEW.publication_plan_digest IS NOT OLD.publication_plan_digest)
BEGIN SELECT RAISE(ABORT, 'push publication outcome is immutable'); END;
CREATE TRIGGER push_completion_immutable BEFORE UPDATE OF response_id,completion_digest,rejected,rejection_reason,options ON pushes
WHEN OLD.response_id IS NOT NULL AND (NEW.response_id IS NOT OLD.response_id OR NEW.completion_digest IS NOT OLD.completion_digest OR NEW.rejected IS NOT OLD.rejected OR NEW.rejection_reason IS NOT OLD.rejection_reason OR NEW.options IS NOT OLD.options)
BEGIN SELECT RAISE(ABORT, 'push completion is immutable'); END;
CREATE TRIGGER push_identity_immutable BEFORE UPDATE OF id,actor,request_digest ON pushes
WHEN NEW.id IS NOT OLD.id OR NEW.actor IS NOT OLD.actor OR NEW.request_digest IS NOT OLD.request_digest
BEGIN SELECT RAISE(ABORT, 'push identity is immutable'); END;
CREATE TRIGGER push_identity_not_replaced BEFORE INSERT ON pushes
WHEN EXISTS(SELECT 1 FROM pushes WHERE id=NEW.id)
BEGIN SELECT RAISE(ABORT, 'push identity cannot be replaced'); END;

CREATE TABLE push_certificates (
    digest BLOB PRIMARY KEY CHECK(length(digest) = 32),
    push_id BLOB NOT NULL UNIQUE REFERENCES pushes(id),
    actor TEXT NOT NULL,
    signer TEXT NOT NULL,
    key TEXT NOT NULL,
    size INTEGER NOT NULL CHECK(size > 0),
    recorded_at_ms INTEGER NOT NULL CHECK(recorded_at_ms >= 0)
) WITHOUT ROWID;

CREATE TABLE push_certificate_chunks (
    push_id BLOB NOT NULL REFERENCES pushes(id),
    part INTEGER NOT NULL CHECK(part >= 0),
    body BLOB NOT NULL CHECK(length(body) BETWEEN 1 AND 524288),
    PRIMARY KEY(push_id, part)
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
    oid BLOB NOT NULL CHECK(length(oid) IN (20, 32)),
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
    initial_source_oid BLOB NOT NULL CHECK(length(initial_source_oid) IN (20, 32)),
    initial_base_oid BLOB NOT NULL CHECK(length(initial_base_oid) IN (20, 32)),
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
    source_oid BLOB NOT NULL CHECK(length(source_oid) IN (20, 32)),
    source_version INTEGER NOT NULL CHECK(source_version > 0),
    base_oid BLOB NOT NULL CHECK(length(base_oid) IN (20, 32)),
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
    oid BLOB NOT NULL CHECK(length(oid) IN (20, 32)),
    merged_ms INTEGER NOT NULL CHECK(merged_ms >= 0),
    pull_version INTEGER NOT NULL CHECK(pull_version > 0),
    source_oid BLOB NOT NULL CHECK(length(source_oid) IN (20, 32)),
    source_version INTEGER NOT NULL CHECK(source_version > 0),
    base_oid BLOB NOT NULL CHECK(length(base_oid) IN (20, 32)),
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
    source_oid BLOB NOT NULL CHECK(length(source_oid) IN (20, 32)),
    base_oid BLOB NOT NULL CHECK(length(base_oid) IN (20, 32)),
    oid BLOB CHECK(oid IS NULL OR length(oid) IN (20, 32))
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
    source_oid BLOB NOT NULL CHECK(length(source_oid) IN (20, 32)),
    source_version INTEGER NOT NULL CHECK(source_version > 0),
    base_oid BLOB NOT NULL CHECK(length(base_oid) IN (20, 32)),
    base_version INTEGER NOT NULL CHECK(base_version > 0),
    merge_base BLOB NOT NULL CHECK(length(merge_base) IN (20, 32)),
    path BLOB NOT NULL CHECK(length(path) BETWEEN 1 AND 4096),
    side TEXT NOT NULL CHECK(side IN ('before', 'after')),
    line INTEGER NOT NULL CHECK(line BETWEEN 1 AND 20000),
    blob_oid BLOB NOT NULL CHECK(length(blob_oid) IN (20, 32)),
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


CREATE TABLE catalog_generations (
    generation INTEGER PRIMARY KEY CHECK(typeof(generation) = 'integer' AND generation >= 0),
    catalog BLOB,
    certificate BLOB,
    CHECK((generation = 0 AND catalog IS NULL AND certificate IS NULL)
       OR (generation > 0 AND catalog IS NOT NULL AND certificate IS NOT NULL AND length(catalog) BETWEEN 1 AND 256 AND length(certificate) = 32))
) WITHOUT ROWID;
INSERT INTO catalog_generations VALUES(0, NULL, NULL);
CREATE TRIGGER catalog_generations_immutable BEFORE UPDATE ON catalog_generations
BEGIN SELECT RAISE(ABORT, 'catalog generations are immutable'); END;
CREATE TRIGGER catalog_generations_not_replaced BEFORE INSERT ON catalog_generations
WHEN EXISTS(SELECT 1 FROM catalog_generations WHERE generation=NEW.generation)
BEGIN SELECT RAISE(ABORT, 'catalog generations cannot be replaced'); END;
CREATE TRIGGER catalog_generations_bounded BEFORE INSERT ON catalog_generations
WHEN (SELECT count(*) FROM (SELECT generation FROM catalog_generations LIMIT 8192)) >= 8192
BEGIN SELECT RAISE(ABORT, 'catalog generation quota exceeded'); END;
CREATE TABLE catalog_state (
    singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
    generation INTEGER NOT NULL REFERENCES catalog_generations(generation)
) WITHOUT ROWID;
INSERT INTO catalog_state VALUES(1, 0);

-- Exact logical outcomes for catalog-only maintenance. Kept separately from
-- network push response/product facts; no per-object outcome rows are created.
CREATE TABLE catalog_compactions (
    id BLOB PRIMARY KEY CHECK(length(id)=16),
    actor TEXT NOT NULL,
    request_digest BLOB NOT NULL CHECK(length(request_digest)=32),
    verification_digest BLOB NOT NULL CHECK(length(verification_digest)=32),
    result BLOB NOT NULL CHECK(length(result) BETWEEN 1 AND 128)
) WITHOUT ROWID;
CREATE TRIGGER catalog_compactions_immutable BEFORE UPDATE ON catalog_compactions
BEGIN SELECT RAISE(ABORT, 'compaction outcomes are immutable'); END;
CREATE TRIGGER catalog_compactions_not_replaced BEFORE INSERT ON catalog_compactions
WHEN EXISTS(SELECT 1 FROM catalog_compactions WHERE id=NEW.id)
BEGIN SELECT RAISE(ABORT, 'compaction outcomes cannot be replaced'); END;

-- Staging attempts retain their creating namespace with a NULL generation.
-- A one-way late bind pins a generation floor and every later generation. This permits
-- read-only frontier refresh without a new durable pin/Claim per publication.
-- Replacement/abort preserves the old floor until its independent pin is reaped.
CREATE TABLE catalog_leases (
    incarnation BLOB NOT NULL CHECK(length(incarnation) = 16),
    admission_sequence INTEGER NOT NULL CHECK(typeof(admission_sequence) = 'integer' AND admission_sequence > 0),
    operation BLOB NOT NULL CHECK(length(operation) = 16),
    owner_epoch BLOB NOT NULL CHECK(length(owner_epoch) = 8 AND owner_epoch != zeroblob(8)),
    artifact_operation BLOB NOT NULL CHECK(length(artifact_operation) = 16),
    generation INTEGER REFERENCES catalog_generations(generation),
    -- Normalize the unbound staging phase for the deferred exact binding. A
    -- nullable FK alone would skip validation of every other identity field.
    binding_generation INTEGER GENERATED ALWAYS AS (coalesce(generation, -1)) STORED,
    expires_at_ms INTEGER NOT NULL CHECK(typeof(expires_at_ms) = 'integer' AND expires_at_ms >= 0),
    attestation BLOB CHECK(attestation IS NULL OR length(attestation) BETWEEN 1 AND 1024),
    attestation_digest BLOB CHECK(attestation_digest IS NULL OR length(attestation_digest) = 32),
    CHECK((attestation IS NULL) = (attestation_digest IS NULL)),
    CHECK(generation IS NOT NULL OR attestation IS NULL),
    PRIMARY KEY(incarnation, admission_sequence)
) WITHOUT ROWID;
CREATE INDEX catalog_leases_by_expiry ON catalog_leases(expires_at_ms, incarnation, admission_sequence);
CREATE INDEX catalog_leases_by_generation ON catalog_leases(generation, expires_at_ms);
CREATE TRIGGER catalog_generations_retained BEFORE DELETE ON catalog_generations
WHEN OLD.generation=0 OR OLD.generation >= (SELECT min(generation) FROM catalog_leases)
BEGIN SELECT RAISE(ABORT, 'catalog generation is retained'); END;
CREATE UNIQUE INDEX catalog_leases_by_artifact ON catalog_leases(artifact_operation);
CREATE TRIGGER catalog_lease_not_replaced BEFORE INSERT ON catalog_leases
WHEN EXISTS(SELECT 1 FROM catalog_leases WHERE incarnation=NEW.incarnation AND admission_sequence=NEW.admission_sequence)
  OR EXISTS(SELECT 1 FROM catalog_leases WHERE artifact_operation=NEW.artifact_operation)
BEGIN SELECT RAISE(ABORT, 'catalog attempt pin cannot be replaced'); END;
CREATE UNIQUE INDEX catalog_leases_binding ON catalog_leases(incarnation, admission_sequence, operation, owner_epoch, artifact_operation, binding_generation, expires_at_ms);
CREATE TRIGGER catalog_lease_identity_immutable BEFORE UPDATE OF incarnation, admission_sequence, operation, owner_epoch, artifact_operation, generation ON catalog_leases
WHEN NEW.incarnation != OLD.incarnation OR NEW.admission_sequence != OLD.admission_sequence
  OR NEW.operation != OLD.operation OR NEW.owner_epoch != OLD.owner_epoch
  OR NEW.artifact_operation != OLD.artifact_operation
  OR (NEW.generation IS NOT OLD.generation AND NOT
      (OLD.generation IS NULL AND NEW.generation IS NOT NULL AND OLD.attestation IS NULL))
BEGIN SELECT RAISE(ABORT, 'catalog attempt identity is immutable'); END;
CREATE TRIGGER catalog_lease_attestation_immutable BEFORE UPDATE OF attestation, attestation_digest ON catalog_leases
WHEN OLD.attestation IS NOT NULL AND (NEW.attestation IS NOT OLD.attestation OR NEW.attestation_digest IS NOT OLD.attestation_digest)
BEGIN SELECT RAISE(ABORT, 'catalog attempt attestation is immutable'); END;

CREATE TABLE catalog_operations (
    id BLOB PRIMARY KEY CHECK(length(id) = 16),
    actor TEXT NOT NULL CHECK(length(CAST(actor AS BLOB)) BETWEEN 1 AND 64),
    request_digest BLOB NOT NULL CHECK(length(request_digest) = 32),
    incarnation BLOB NOT NULL CHECK(length(incarnation) = 16),
    owner_epoch BLOB NOT NULL CHECK(length(owner_epoch) = 8 AND owner_epoch != zeroblob(8)),
    admission_sequence INTEGER NOT NULL CHECK(typeof(admission_sequence) = 'integer' AND admission_sequence > 0),
    artifact_operation BLOB NOT NULL CHECK(length(artifact_operation) = 16),
    generation INTEGER REFERENCES catalog_generations(generation),
    -- Normalize the unbound staging phase for the deferred exact binding. A
    -- nullable FK alone would skip validation of every other identity field.
    binding_generation INTEGER GENERATED ALWAYS AS (coalesce(generation, -1)) STORED,
    expires_at_ms INTEGER NOT NULL CHECK(typeof(expires_at_ms) = 'integer' AND expires_at_ms >= 0),
    attestation BLOB CHECK(attestation IS NULL OR length(attestation) BETWEEN 1 AND 1024),
    attestation_digest BLOB CHECK(attestation_digest IS NULL OR length(attestation_digest) = 32),
    CHECK((attestation IS NULL) = (attestation_digest IS NULL)),
    CHECK(generation IS NOT NULL OR attestation IS NULL),
    FOREIGN KEY(incarnation, admission_sequence, id, owner_epoch, artifact_operation, binding_generation, expires_at_ms)
        REFERENCES catalog_leases(incarnation, admission_sequence, operation, owner_epoch, artifact_operation, binding_generation, expires_at_ms)
        DEFERRABLE INITIALLY DEFERRED
) WITHOUT ROWID;
CREATE INDEX catalog_operations_by_expiry ON catalog_operations(expires_at_ms, id);
CREATE UNIQUE INDEX catalog_operations_by_lease ON catalog_operations(incarnation, admission_sequence);
CREATE INDEX catalog_operations_by_generation ON catalog_operations(generation);

CREATE TRIGGER push_responses_immutable BEFORE UPDATE ON push_responses
BEGIN SELECT RAISE(ABORT, 'push outcome bytes are immutable'); END;
CREATE TRIGGER push_responses_not_replaced BEFORE INSERT ON push_responses
WHEN EXISTS(SELECT 1 FROM push_responses WHERE id=NEW.id)
BEGIN SELECT RAISE(ABORT, 'push outcome bytes cannot be replaced'); END;

CREATE TRIGGER push_response_chunks_immutable BEFORE UPDATE ON push_response_chunks
BEGIN SELECT RAISE(ABORT, 'push outcome bytes are immutable'); END;
CREATE TRIGGER push_response_chunks_not_replaced BEFORE INSERT ON push_response_chunks
WHEN EXISTS(SELECT 1 FROM push_response_chunks WHERE response_id=NEW.response_id AND part=NEW.part)
BEGIN SELECT RAISE(ABORT, 'push outcome bytes cannot be replaced'); END;

CREATE TRIGGER push_certificates_immutable BEFORE UPDATE ON push_certificates
BEGIN SELECT RAISE(ABORT, 'push outcome bytes are immutable'); END;
CREATE TRIGGER push_certificates_not_replaced BEFORE INSERT ON push_certificates
WHEN EXISTS(SELECT 1 FROM push_certificates WHERE digest=NEW.digest)
BEGIN SELECT RAISE(ABORT, 'push outcome bytes cannot be replaced'); END;

CREATE TRIGGER push_certificate_chunks_immutable BEFORE UPDATE ON push_certificate_chunks
BEGIN SELECT RAISE(ABORT, 'push outcome bytes are immutable'); END;
CREATE TRIGGER push_certificate_chunks_not_replaced BEFORE INSERT ON push_certificate_chunks
WHEN EXISTS(SELECT 1 FROM push_certificate_chunks WHERE push_id=NEW.push_id AND part=NEW.part)
BEGIN SELECT RAISE(ABORT, 'push outcome bytes cannot be replaced'); END;
