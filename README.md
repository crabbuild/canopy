# Canopy

Canopy is an independent Git hosting service built on Cellule. The dedicated
`canopy-server` crate owns its product schema, Git gateway and HTTP API. A
Directory Cell maps an owner and repository name to a stable UUID. Each UUID
identifies its own SQLite Repository Cell. Ordinary Git objects, refs and LFS
metadata live in that Cell; large Git blob and LFS bytes use immutable
repository-scoped external objects. Canopy has no Crab product or Xet
dependency.

## Current implementation

This repository is an implementation under construction. The `canopy` binary
starts one leased Cellule node and serves repositories created through its API.
It probes the object store's fencing capabilities, publishes and renews a signed node
advertisement, and restores the repository Cell from object storage when its
local SQLite file is lost. `git-http-backend` supplies Git smart HTTP wire
handling; the SQLite Cell is the durable authority, and a bare Git repository
is only a rebuildable cache. Integration tests use stock `git` and `git-lfs`
clients to push and clone, including a restart with a fresh local SQLite file.

`POST /api/repositories` with `{"name":"example"}` creates a repository for the
configured owner and returns its UUID and clone URL. `GET /api/repositories`
lists ready repositories, with an `after` cursor for additional pages. Both
endpoints require the configured token. Git and LFS use
`/<owner>/<repository_name>.git`. Repository creation reserves a UUID in the
Directory Cell, provisions its own Repository Cell, then marks the name ready.
Later requests recover that Cell on demand from the directory.

The current service uses one static token for every repository under one owner,
buffers requests and responses, and caps Git and LFS
payloads at 64 MiB. Local recovery currently admits a 512 MiB SQLite database.
There is no account or organization model, per-repository ACL, multi-node
routing, backup, repository browser, issue or pull request API, or production
capacity evidence. The local Cellule path dependencies in `Cargo.toml` are
for development only.

## Run the current service

Copy [config.example.json](config.example.json) and set the object storage URL,
tenant and application IDs, owner name, network addresses and data
directory. The object store must support
conditional create/update and ranged reads; startup probes these operations.
Configure credentials through the provider's environment variables. Set
`CANOPY_GIT_TOKEN` and `CANOPY_NODE_SIGNING_KEY_HEX` (a 32-byte key encoded as
64 hex characters) in the process environment. Then run:

```sh
CARGO_TARGET_DIR=$HOME/Workspace/crabbuild-target/canopy-local cargo run --locked --bin canopy -- config.json
```

`GET /healthz` reports process liveness and `GET /readyz` reports Cell
readiness. Git and LFS requests require `Authorization: Bearer <token>` or
HTTP Basic credentials using the configured owner and token. Stop with
SIGINT or SIGTERM to drain requests and withdraw the node advertisement.

## Verify the current slice

Use a checkout-specific target directory on the mounted Workspace volume:

```sh
CARGO_TARGET_DIR=$HOME/Workspace/crabbuild-target/canopy-local cargo test --locked
CARGO_TARGET_DIR=$HOME/Workspace/crabbuild-target/canopy-local cargo clippy --all-targets --locked -- -D warnings
cargo fmt --all -- --check
```

The integration tests need `git` and `git-lfs` on `PATH`. See
[delivery plan](docs/delivery-plan.md) for the remaining release gates and
[contracts](docs/contracts.md) for persisted identities and storage rules.

For a black-box process smoke, build `canopy`, provide a test S3-compatible
bucket and credentials through the provider's environment variables, and run:

```sh
python3 scripts/smoke_s3_process.py \
  --binary "$HOME/Workspace/crabbuild-target/canopy-local/debug/canopy" \
  --storage-url s3://your-test-bucket \
  --work-parent "$HOME/Workspace/crabbuild-target/canopy-local"
```

The script requires `CANOPY_NODE_SIGNING_KEY_HEX` and provider credentials in
the environment. It pushes two repositories with stock Git and LFS, restarts
with fresh local databases, kills the new owner, waits for lease expiry, and
clones both from a third process. It writes under a unique prefix in the
supplied bucket.
