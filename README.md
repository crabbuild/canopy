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
handling, including protocol v2 negotiation. The SQLite Cell is the durable
authority, and a bare Git repository is only a rebuildable cache. Integration
tests use stock `git` and `git-lfs` clients to push and clone, including a
restart with a fresh local SQLite file.

`POST /api/repositories` with `{"name":"example"}` creates a repository for the
configured owner and returns its UUID and clone URL. `GET /api/repositories`
lists ready repositories, with an `after` cursor for additional pages.
`PATCH /api/repositories/<old_name>` with
`{"name":"new_name","repository_id":"<returned UUID>"}` atomically renames a ready
repository. The UUID is a precondition and remains unchanged; a retry with
the same UUID and new name returns the renamed repository. These endpoints
require the configured owner's token. Git and LFS use
`/<owner>/<repository_name>.git`. Repository creation reserves a UUID in the
Directory Cell, provisions its own Repository Cell, then marks the name ready.
Later requests recover that Cell on demand from the directory.

The configured token bootstraps a durable owner account in the Directory Cell.
`POST /api/accounts` creates another account with a client-generated `cnp_`
token followed by 64 random hexadecimal digits and a `read`, `write`, or
`admin` token scope. Its JSON fields are `name`, `token`, and `scope`.
Only the configured owner can create accounts and change collaborators.
`PUT /api/repositories/<name>/collaborators/<account>` with
`{"role":"read"}` or `{"role":"write"}` grants access to one repository;
`DELETE` on the same URL revokes it. The owner retains access. Git smart HTTP
and LFS require both sufficient token scope and repository role. A ref update
rechecks the writer in the ref transaction; an LFS upload rechecks the writer
when publishing metadata.

The current service supports one repository owner and one token per account.
It buffers requests and responses and caps Git and LFS payloads at 64 MiB.
Local recovery currently admits a 512 MiB SQLite database. There is no
account lifecycle API, organization model, collaborator-visible repository
listing, multi-node routing, backup, repository browser, issue or pull request
API, or production capacity evidence. `Cargo.toml` pins Cellule to a specific
Git revision, so a
fresh Canopy checkout builds without a local Cellule checkout.

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
HTTP Basic credentials using the matching account name and token. Stop with
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
the environment. It pushes two repositories with stock Git and LFS, grants a
collaborator access, renames one repository, restarts with fresh local databases,
kills the new owner, waits for lease expiry, and clones from a third process.
It verifies collaborator access after takeover and denial after revocation.
It writes under a unique prefix in the supplied bucket.
