CREATE TABLE repositories (
    owner TEXT NOT NULL,
    name TEXT NOT NULL,
    repository_id BLOB NOT NULL UNIQUE CHECK(length(repository_id) = 16),
    state TEXT NOT NULL CHECK(state IN ('pending', 'ready')),
    PRIMARY KEY(owner, name)
) WITHOUT ROWID;

CREATE TABLE accounts (
    name TEXT PRIMARY KEY,
    enabled INTEGER NOT NULL CHECK(enabled IN (0, 1))
) WITHOUT ROWID;

CREATE TABLE access_tokens (
    digest BLOB PRIMARY KEY CHECK(length(digest) = 32),
    id BLOB NOT NULL UNIQUE CHECK(length(id) = 16),
    account TEXT NOT NULL REFERENCES accounts(name),
    scope TEXT NOT NULL CHECK(scope IN ('read', 'write', 'admin')),
    enabled INTEGER NOT NULL CHECK(enabled IN (0, 1)),
    created_ms INTEGER NOT NULL CHECK(created_ms >= 0),
    expires_ms INTEGER CHECK(expires_ms IS NULL OR expires_ms > created_ms)
) WITHOUT ROWID;

CREATE INDEX access_tokens_account ON access_tokens(account, id);
CREATE INDEX access_tokens_active ON access_tokens(account, expires_ms) WHERE enabled = 1;
CREATE INDEX access_tokens_issued ON access_tokens(account, created_ms);

CREATE TABLE ssh_keys (
    fingerprint BLOB PRIMARY KEY CHECK(length(fingerprint) = 32),
    id BLOB NOT NULL UNIQUE CHECK(length(id) = 16),
    account TEXT NOT NULL REFERENCES accounts(name),
    public_key TEXT NOT NULL,
    scope TEXT NOT NULL CHECK(scope IN ('read', 'write')),
    enabled INTEGER NOT NULL CHECK(enabled IN (0, 1)),
    created_ms INTEGER NOT NULL CHECK(created_ms >= 0)
) WITHOUT ROWID;

CREATE TABLE lfs_grants (
    digest BLOB PRIMARY KEY CHECK(length(digest) = 32),
    id BLOB NOT NULL UNIQUE CHECK(length(id) = 16),
    key_id BLOB NOT NULL REFERENCES ssh_keys(id),
    repository_id BLOB NOT NULL REFERENCES repositories(repository_id),
    operation TEXT NOT NULL CHECK(operation IN ('download', 'upload')),
    expires_ms INTEGER NOT NULL CHECK(expires_ms >= 0)
) WITHOUT ROWID;

CREATE INDEX lfs_grants_expiry ON lfs_grants(expires_ms);

CREATE INDEX ssh_keys_account ON ssh_keys(account, id);
CREATE INDEX ssh_keys_active ON ssh_keys(account) WHERE enabled = 1;
CREATE INDEX ssh_keys_issued ON ssh_keys(account, created_ms);

CREATE INDEX repositories_owner_id ON repositories(owner, repository_id) WHERE state = 'ready';

CREATE TABLE repository_discovery (
    account TEXT NOT NULL REFERENCES accounts(name),
    repository_id BLOB NOT NULL REFERENCES repositories(repository_id),
    PRIMARY KEY(account, repository_id)
) WITHOUT ROWID;

CREATE TABLE public_repository_candidates (
    repository_id BLOB PRIMARY KEY REFERENCES repositories(repository_id)
) WITHOUT ROWID;

CREATE TABLE account_events (
    id INTEGER PRIMARY KEY CHECK(id > 0),
    occurred_ms INTEGER NOT NULL CHECK(occurred_ms >= 0),
    actor TEXT,
    actor_token_id BLOB CHECK(actor_token_id IS NULL OR length(actor_token_id) = 16),
    action TEXT NOT NULL CHECK(action IN ('account.created', 'account.disabled', 'token.issued', 'token.revoked', 'ssh_key.registered', 'ssh_key.revoked')),
    account TEXT NOT NULL,
    token_id BLOB CHECK(token_id IS NULL OR length(token_id) = 16),
    ssh_key_id BLOB CHECK(ssh_key_id IS NULL OR length(ssh_key_id) = 16),
    scope TEXT CHECK(scope IS NULL OR scope IN ('read', 'write', 'admin')),
    expires_ms INTEGER,
    CHECK((actor IS NULL) = (actor_token_id IS NULL)),
    CHECK(token_id IS NULL OR ssh_key_id IS NULL),
    CHECK((token_id IS NULL AND ssh_key_id IS NULL) = (scope IS NULL))
);
