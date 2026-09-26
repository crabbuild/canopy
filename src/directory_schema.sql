CREATE TABLE repositories (
    owner TEXT NOT NULL,
    name TEXT NOT NULL,
    repository_id BLOB NOT NULL UNIQUE CHECK(length(repository_id) = 16),
    state TEXT NOT NULL CHECK(state IN ('pending', 'ready')),
    PRIMARY KEY(owner, name)
) WITHOUT ROWID;
