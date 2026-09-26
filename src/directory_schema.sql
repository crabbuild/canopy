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
    account TEXT NOT NULL REFERENCES accounts(name),
    scope TEXT NOT NULL CHECK(scope IN ('read', 'write', 'admin')),
    enabled INTEGER NOT NULL CHECK(enabled IN (0, 1))
) WITHOUT ROWID;

CREATE INDEX access_tokens_account ON access_tokens(account);
