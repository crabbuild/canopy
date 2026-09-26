# Canopy

Canopy is an independent Git hosting service built on Cellule. The dedicated
`canopy-server` crate owns its product schema, Git gateway and HTTP API. One
repository UUID identifies one SQLite Repository Cell. Ordinary Git objects,
refs and LFS metadata live in that Cell; large Git blob and LFS bytes use
immutable repository-scoped external objects. Canopy has no Crab product or
Xet dependency.

## Current implementation

This repository is an implementation under construction, not a deployable
service. Its integration test starts an HTTP listener and a real Cellule
runtime, then uses stock `git` and `git-lfs` clients to push and clone a
repository. It verifies a large Git blob and an LFS object after restarting
the disposable HTTP gateway. A separate host test shuts down the first
Cellule node, discards its SQLite file, restores the repository Cell on a
second node and clones the same commit. `git-http-backend` supplies Git smart HTTP wire
handling; the SQLite Cell is the durable authority, and a bare Git repository
is only a rebuildable cache.

The current gateway serves one configured private repository at `/repo.git`.
It uses one static token, buffers requests and responses, and caps Git and LFS
payloads at 64 MiB. There is no standalone server binary, account or
organization model, repository directory, multi-node ownership, backup,
repository browser, issue or pull request API, or production capacity evidence.
The local Cellule path dependencies in `Cargo.toml` are for development only.

## Verify the current slice

Use a checkout-specific target directory on the mounted Workspace volume:

```sh
CARGO_TARGET_DIR=$HOME/Workspace/crabbuild-target/canopy-local cargo test --locked
CARGO_TARGET_DIR=$HOME/Workspace/crabbuild-target/canopy-local cargo clippy --all-targets --locked -- -D warnings
cargo fmt --all -- --check
```

The integration test needs `git` and `git-lfs` on `PATH`. See
[delivery plan](docs/delivery-plan.md) for the remaining release gates and
[contracts](docs/contracts.md) for persisted identities and storage rules.
