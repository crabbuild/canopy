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
    action TEXT NOT NULL CHECK(action IN ('account.created', 'account.disabled', 'token.issued', 'token.revoked')),
    account TEXT NOT NULL,
    token_id BLOB CHECK(token_id IS NULL OR length(token_id) = 16),
    scope TEXT CHECK(scope IS NULL OR scope IN ('read', 'write', 'admin')),
    expires_ms INTEGER,
    CHECK((actor IS NULL) = (actor_token_id IS NULL)),
    CHECK((token_id IS NULL) = (scope IS NULL))
);
