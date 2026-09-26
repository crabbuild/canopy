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
lists ready repositories that the authenticated account can access. Pass the
returned `next_cursor` as `after` until it is null. Pages inspect at most 32
UUID-ordered candidates and can be short or empty with a non-null cursor,
including when access was revoked or Cell movement capacity runs out.
A page that cannot make progress returns 503 with `Retry-After: 1`.
`GET /api/repositories/<name>` returns the UUID, clone URL, repository role,
default branch and ref generation; missing or inaccessible names return 404.
`PATCH /api/repositories/<old_name>` with
`{"name":"new_name","repository_id":"<returned UUID>"}` atomically renames a ready
repository. The UUID is a precondition and remains unchanged; a retry with
the same UUID and new name returns the renamed repository. Create and rename
require the configured owner's token; list and get also admit collaborators.
Git and LFS use
`/<owner>/<repository_name>.git`. Repository creation reserves a UUID in the
Directory Cell, provisions its own Repository Cell, then marks the name ready.
Later requests recover that Cell on demand from the directory.

`GET /api/repositories/<name>/default-branch` returns `repository_id`, the fully
qualified `reference` (initially `refs/heads/main`), and `generation`.
Repository readers can inspect it. An owner with an admin-scoped token can
`PUT` the same URL with
`{"repository_id":"<UUID>","reference":"refs/heads/trunk","expected_generation":7}`.
Use the UUID and generation returned by GET. A concurrent ref or HEAD change
returns 409; read current state before retrying. The target must exist unless
there are no live branches. Stock Git discovery and clone use this durable HEAD,
including after restart. Protocol v2 also reports an unborn target. Branch
deletion preserves HEAD's name; recreating the branch makes it live again.

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
Incoming Git requests stream to temporary files charged to the same disk budget
as the node's SQLite files. Push requests admit up to 512 MiB; fetch requests
up to 64 MiB. Push replies remain buffered and capped at 64 MiB. Clone and fetch
responses stream with backpressure and have no 64 MiB response ceiling. LFS
transfers and individual external Git blobs remain capped at 64 MiB.
Trees, commits and tags above 768 KiB use 512 KiB SQLite chunks, up to 64 MiB
per object. Publication verifies every chunk and the complete object identity;
partial uploads stay invisible to Git.
Each node admits eight Git/LFS transfers across all repositories. Overload
returns 503 with `Retry-After: 1`; retry after capacity is available. Health,
readiness and management routes remain outside this transfer limit. LFS body
reception has a 120-second deadline (408 on timeout). Eight is an initial
operational bound, not a measured production capacity target.
Ref advertisements use generation-checked pagination; sustained concurrent
changes return a retryable 503. Gzip-compressed Git requests are supported.
Gzip is fully validated before Git runs; decoded bytes have the same request
size limits and share disk admission with the encoded upload.
Disposable Git caches retain shared disk reservations. Hydration admits bytes
before writing; native Git writes are measured before durable ref publication.
Exhaustion returns 507. Native Git's peak scratch usage is not yet hard bounded.
Local recovery currently admits a 512 MiB SQLite database. The node keeps the
Directory Cell and up to three Repository Cells resident. Additional repositories
evict an inactive repository and restore from durable state when accessed again.
Requests and streamed responses pin their repository; admission returns 503 when
no repository can be safely released. A terminal ownership-release failure leaves
that repository unavailable until node restart; confirmed-release cleanup errors
are retried on later admission. There is no
account lifecycle API, organization model, API to list a repository's
collaborators, multi-node routing, backup, repository browser, issue or pull request
API, or production capacity evidence. `Cargo.toml` pins Cellule to a specific
Git revision, so a
fresh Canopy checkout builds without a local Cellule checkout.

Before ref publication, bounded certificate batches verify the durable Git
graph: commit trees and parents, tree entries and tag targets must exist with
the correct object type. Each batch covers at most 128 objects and 64 MiB of
SQLite object bytes. Ref publication checks certified tips atomically with
permissions and ref versions; branch tips must be commits. Submodule gitlinks may name commits in another repository.
SQLite certificates let later pushes reuse validated history. Push ingestion streams
candidates from accepted ref tips, excludes previously published history, and
reads missing objects through one persistent Git batch process. Object sizes
are checked before allocation and canonical OIDs before storage. SQLite lookups
group up to 128 candidate IDs; object publication groups up to 128 records and
768 KiB of inline bytes in one Cell transaction, with at most 64 MiB of SQLite
object bytes verified per batch. A conflicting record rejects
the whole batch. Recovery tests include annotated tags, submodules and
`git fsck` on the restored clone.

Cold cache hydration reads OID-ordered pages of at most 128 records and 768 KiB
of inline bodies. It verifies inline identities on a blocking worker; chunked
and external bodies retain their own verification before cache writes. Pages
reduce SQLite query overhead, but recovery still rebuilds the complete cache.

## Recover a lost push reply

For a receive-pack POST, a client or proxy can supply `Idempotency-Key` as one
canonical lowercase, hyphenated UUID. Use one ID per logical operation. Canopy
binds it to the repository, authenticated account and request digest. Repeating
the same request returns the recorded status, headers and per-ref report without
applying the refs again, including after owner takeover. Reusing an ID with
different request bytes or another account returns HTTP 409. Current token
scope and repository access are checked on every replay.

Replay requires the same body, content type and Git protocol setting. A new
`git push` invocation may generate different pack bytes; reusing its header does
not guarantee replay. An HTTP client or proxy must retain the original request.
Recorded replies include `X-Canopy-Push-Id`. If no ID was supplied, the server
generates one; a client that loses that reply cannot discover the generated ID.
Advertisements and fetches ignore this header.

The Repository Cell stages the reply in SQLite chunks, then publishes its
pointer and accepted ref updates in one transaction. Git rejection and no-op
reports are recorded too. Failures before publication remain pending and can be
retried. Response bodies are limited to 64 MiB and serialized response headers
to 64 KiB. Completed records and abandoned staging chunks currently have no
expiry or collector and consume the repository database allowance.

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
It also verifies that a deleted branch stays absent through takeover and can
then be recreated through stock Git.
Mixed push checks prove accepted refs survive recovery, rejected refs stay
absent, and `git push --atomic` rejects the entire mixed update.
A proxy drops a successful push reply; replay after takeover returns the original
report without undoing a later branch deletion.
It writes under a unique prefix in the supplied bucket.

Add `--large-clone` to send two 40 MiB random blobs in a single push, then clone the
repository using protocol v0 and v2 after takeover. Each clone must receive a
pack larger than 64 MiB, reproduce both file hashes and pass `git fsck`. This is
a transfer-size qualification; it does not establish production capacity.

Add `--many-objects 256` to qualify a 256-file initial push, a one-file update
with an annotated tag, and a verified clone after takeover. Combine it with
`--large-clone` to exercise four repositories through resident eviction and
verify Git/LFS recovery on the same node before restart. For local container
stores, place data and logs on the mounted workspace and verify free
inodes as well as bytes before qualification. A full container filesystem can
turn storage publications into unresolved mutations even with free byte space.


Add `--sqlite-chunks` to push a 32,000-entry tree and commit/tag messages above
1 MiB, then verify exact raw bytes and OIDs after takeover with a strict fsck.
This exercises SQLite chunk storage independently of external large blobs.
Add `--corpus-repository /path/to/existing/repository` to qualify that checkout's
HEAD history. The script only reads the source, creates a bundle and temporary
fixtures under `--work-parent`, then verifies every reachable object's type,
size and bytes in protocol v0/v2 clones after takeover, plus strict `git fsck`.
Other source branches and tags are outside this qualification.
The chunk, default-branch and repository-discovery layouts change the unreleased
schema; use a fresh development storage prefix when moving from older builds.
