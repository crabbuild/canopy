# Deliver Canopy

The executable acceptance gates below define when Canopy can be called a Git
hosting service. Each gate needs a black-box client action, the durable side
effect, and the same visible result after a new node restores Cell state.
Do not infer completion from compilation or a disposable cache test.

| Gate | Deliverable | Acceptance proof | State |
| --- | --- | --- | --- |
| 0 Independent build | Pin an immutable Cellule revision; build `canopy-server` without local paths or Crab product crates | Fresh checkout builds in CI | Partial: immutable Git revision pinned and local fresh-checkout proof; hosted CI pending a Canopy remote |
| 1 Node process | `canopy` binary, validated config, CellNode lease/renewal, listener, readiness, drain | Start/stop against durable store; no worker or lease leak | Partial: S3-compatible process restart and clean drain pass; worker/lease fault matrix remains |
| 2 Repository lifecycle | Directory Cell, create/list/get/rename, account identity, token scopes, repository ACL | Two users see only authorized repositories; failed creation converges on one UUID | Partial: durable accounts and per-repository Git/LFS roles survive recovery; account lifecycle, collaborator listing and get remain |
| 3 Git object path | Bounded pack ingest, SQLite object chunks, verified external large blobs, quotas | Push delta pack; restore exact bytes and OIDs after owner loss; reject corruption | Partial: disk-accounted 512 MiB pushes, incremental Git reads and bounded atomic SQLite object batches work; per-object buffers and 64 MiB blob ceiling remain |
| 4 Atomic push | Durable push session, graph closure proof, ACL and branch rules in finalization, recorded retry outcome | Concurrent and multi-ref pushes, ABA, owner death at every publication boundary | Partial: ref CAS, ACL, ABA protection, ordinary mixed push results, atomic rejection and exact HTTP push replay survive recovery; typed graph closure is enforced; branch rules and publication fault matrix remain |
| 5 Fetch | Bounded streaming upload-pack, snapshot refs, cold recovery | Clone/fetch after owner takeover while refs move; large corpus capacity evidence | Partial: consistent paginated refs, gzip requests and backpressured fetch work; v0/v2 clones above 80 MiB pass after takeover; native scratch limits and corpus capacity proof remain |
| 6 LFS | Batch/basic transfer, verified bytes, quotas and transfer admission | Stock `git-lfs` push/pull after owner loss; wrong hash/size and interruption fail closed | Partial: stock push/pull after gateway restart works |
| 7 Collaboration | Issues, comments, checks, rules, pulls, reviews, merge, releases, repository UI | Create, review, check, merge and reload across owner change | Open |
| 8 Recovery and operations | Two-node routing, backups, restore, conservative GC, audit and metrics | Kill owner, lose local disk, restore from backup, clone and inspect collaboration data | Partial: process lease takeover and cold clone pass for two repository Cells; multi-node routing/backup/GC/telemetry remain |
| 9 Public service | Public visibility, organizations/teams, search and webhooks | ACL-safe anonymous reads, revocation, index rebuild and webhook retry | Open |

The **internal preview** requires gates 0–5, including real storage and
two-node owner loss. A private beta requires gates 0–8. A public release
requires gate 9 and measured limits for repository count, hot repository
throughput, pack size, concurrent clients, restore time and storage cost.
Hosted CI runners, packages, forks and GitHub API compatibility require
separate product decisions.

## Next reviewable changes

1. Expand the S3-compatible process smoke into a node/lease fault matrix and
   test the target production object store. Run the checked-in CI workflow on
   a Canopy remote. The pinned Cellule revision currently lives on a public
   branch; [Cellule PR #5](https://github.com/crabbuild/cellule/pull/5)
   proposes the UUID partition contract. The storage capability probe also
   needs to land upstream before Canopy can pin a revision on `main`.
2. Enforce native Git scratch limits and qualify residency under faults and larger
   hot sets. The current SQL worker admits four active Cells total: the
   directory plus three repositories. Inactive repositories now release their
   Cell and reload on demand; admission returns 503 when no repository is safe
   to evict. The resident limit is not a production capacity target.
   Hydration and retained caches now use shared disk admission; native Git's
   completed writes are measured before publication, but its peak usage remains
   unbounded. Add crash-left cache reconciliation and qualify cleanup failures.
   Requests now spool under shared disk admission, CGI reads stream through a
   bounded queue, and ingest uses incremental enumeration plus a persistent Git
   batch reader. Bounded object batches now share a Cell publication receipt,
   and existence queries group up to 128 IDs. Add SQLite chunks for large
   trees, commits and tags, plus a real corpus benchmark and bounded graph
   certification for large initial pushes. Keep the bare repo disposable.
3. Qualify durable push replay at every staging/publication boundary. Exact
   HTTP replay now binds a UUID to account and request digest and atomically
   publishes the complete response with ref changes. Typed graph connectivity
   now gates both ref commands. Add branch rules; define retention and quotas
   for completed outcomes and abandoned staging chunks before persistent use.
   Keep testing distinct IDs for identical bytes after refs change: Cellule
   command deduplication alone does not identify an HTTP operation.
4. Complete account lifecycle, token rotation/revocation, collaborator-visible
   listing, and audit records. Test revocation during in-flight Git and LFS
   operations, including a node takeover.
5. Implement collaboration as vertical slices: issues; checks and branch
   rules; pull requests, reviews and merge; releases and assets; UI. Each
   slice ships with its own public action and owner-recovery proof.

Keep LFS bodies and unreferenced Git objects under conservative retention
until a fenced collector can prove the complete root set. No automatic GC
should remove bytes while fetch, backup or a pending merge can still read them.

## Transfer-size qualification

The initial 2026-09-25 streaming-response build on Darwin arm64, using Apple Git 2.50.1,
passed `smoke_s3_process.py --large-clone` against RustFS
`1.0.0-beta.8-glibc`. Two separate 40 MiB random-blob pushes
remained within the current ingress limit. After clean restart, owner death,
lease expiry and local database loss, each stock Git clone received an
83,912,143-byte pack, reproduced both SHA-256 file hashes and passed `git fsck`.

| Client protocol | Cache state | Clone plus verification time |
| --- | --- | --- |
| v0 | Cold after takeover | 33.63 seconds |
| v2 | Warm from the preceding clone | 7.24 seconds |

These times include client checks and use different cache states. They prove
transfer size and recovery behavior, not comparative protocol speed or service
capacity. The same run passed Git/LFS, ACL, mixed/atomic ref outcomes and exact
replay of a dropped push reply. Unix subprocess tests separately verify bounded
output backpressure, disconnect cleanup, and errors after HTTP headers.

The subsequent disk-backed input build passed the same qualification with both
40 MiB blobs sent in **one** push using a 1 MiB client
[`http.postBuffer`](https://git-scm.com/docs/git-config#Documentation/git-config.txt-httppostBuffer). The
push completed in 19.12 seconds. After takeover, v0 and v2 each restored an
83,912,144-byte pack with matching file hashes and clean `git fsck` results
(28.49 seconds cold and 3.89 seconds warm, including verification). The suite
also proves HTTP 507 on exhausted upload admission, successful retry after
capacity is released, and cleanup after input cancellation/disconnect.


## Incremental object ingestion qualification

The batch-reader build on Darwin arm64 with Apple Git 2.50.1 passed the
256-file process smoke against RustFS `1.0.0-beta.8-glibc`, with provider data
and logs on a dedicated directory on the mounted workspace. The initial push
created 258 Git objects. Its follow-up changed one file and added an annotated
tag. After owner loss, the clone retained both commit OIDs, the tag, every file's
bytes and a clean `git fsck` result.

| Action | Observed debug-build time |
| --- | --- |
| Initial 256-file push | 22.13 seconds |
| One-file update and annotated tag | 1.04 seconds |
| Clone and verification after takeover | 1.05 seconds |

A separate run of the same executable passed the 80 MiB single-push case in
21.23 seconds. After takeover, v0 and v2 each restored an 83,912,144-byte pack
with matching hashes and clean `git fsck` results: 36.86 seconds cold and
6.26 seconds warm, including verification. Both process runs also passed the
Git/LFS, ACL, mixed/atomic ref result and dropped-reply replay checks.

The reader tests separately prove exclusion of old history, direct tree/blob
roots, nested tags, canonical reads despite replacement refs, binary bodies,
40 MiB body reads, malformed/truncated output rejection, size admission before
allocation, failed child exit and cancellation cleanup. Focused Git HTTP and
owner-recovery integration tests and Clippy also pass.

These are correctness and workload observations, not a production throughput
claim or an old/new performance comparison. Earlier qualification attempts
exposed a nearly exhausted Docker inode pool, repeated provider HTTP 500s and
unresolved Cell mutations. Moving test data to the mounted workspace allowed
the many-object test to pass. A separate large-transfer attempt observed Cell
fencing under concurrent host load; lease/storage fault qualification remains
open. Canopy must continue to fail closed when a mutation lacks durable proof.
That build required separate `--many-objects 256` and `--large-clone` runs.
The residency build below supports combining both extra fixture repositories
in one process.

## Atomic object batch qualification

The SQLite batch build passed the same two process workloads on Darwin arm64
with Apple Git 2.50.1 and RustFS `1.0.0-beta.8-glibc`, using fresh bind-mounted
provider data and logs. The 256-file workload crosses the 128-record publication
limit, then makes an incremental update and annotated tag. Recovery verified
both commit OIDs, the tag, all file bytes and `git fsck`.

| Action | Observed debug-build time |
| --- | --- |
| Initial 256-file push | 0.55 seconds |
| One-file update and annotated tag | 0.64 seconds |
| Clone and verification after takeover | 1.00 seconds |
| One push containing two 40 MiB random blobs | 36.90 seconds |
| v0 clone and verification, cold after takeover | 54.84 seconds |
| v2 clone and verification, warm | 9.45 seconds |

Both large clones restored an 83,912,145-byte pack with matching file hashes
and clean `git fsck` results. Both process runs passed Git/LFS, ACL, mixed and
atomic ref outcomes, dropped-reply replay, clean restart, disk loss and lease
takeover. These observations are not a controlled comparison with earlier
builds or production throughput evidence; the host had substantial concurrent
load during qualification.

Cell tests separately prove one receipt for 128 records, exact command replay,
the aggregate 768 KiB inline boundary, and whole-batch rollback after a later
invalid OID, corrupt existing inline row or conflicting external record.
Codec tests cover the count and byte limits, bounded decoding and a maximal
payload beneath the 1 MiB wire ceiling. Focused directory, repository, smart
HTTP and owner-restart integration tests, formatting, Clippy and the binary
build pass. No dependency or schema migration was introduced. The old
single-object publication APIs were removed; `PutObjects` owns validation and
publication for the canonical path.

## Repository residency qualification

The node now retains the Directory Cell and at most three Repository Cells.
Requests pin their repository, including streamed replies. On a cold admission,
the least recently used unpinned, settled repository releases its exact Cell
generation through Cellule. Canopy waits for worker closure and authoritative
owner release before dropping cached handles and deleting local SQLite files.
Acquisition and release are tracked through client cancellation and graceful
shutdown. This removes the previous three-repository lifetime ceiling without
increasing the resident worker limit.

The focused multi-server tests publish six distinct repositories, check each
released Cell's `Idle` state and absent owner, verify its local SQLite directory
was removed, and clone all six through repeated same-node restoration. Stock
Git and `git-lfs` reproduce each commit OID, ordinary file and LFS body; each
clone passes `git fsck`. Three paused LFS uploads prevent a fourth admission
(503); disconnecting one permits admission while the other uploads complete
and their bytes remain readable. Body tests verify pin lifetime through data,
trailers and disconnect. The existing account, ACL, rename and restart test
also passes, as do Clippy, formatting and the binary build.

The RustFS process qualification now combines `--large-clone --many-objects 256`.
Four repositories successfully cross the three-slot resident limit. Stock
clients restore Git/LFS on the same owner, then repeat recovery after clean
restart, owner death, lease expiry and local database loss. ACL revocation,
mixed/atomic ref outcomes and exact replay of a dropped push reply pass.

| Action | Observed debug-build time |
| --- | --- |
| One push containing two 40 MiB random blobs | 20.88 seconds |
| Initial 256-file push | 1.64 seconds |
| One-file update and annotated tag | 2.43 seconds |
| v0 clone and verification, cold after takeover | 32.59 seconds |
| v2 clone and verification, warm | 5.70 seconds |
| 256-file clone and verification after takeover | 1.37 seconds |

Each large clone restored an 83,912,145-byte pack with matching hashes and a
clean `git fsck`. Environment: Darwin arm64, Apple Git 2.50.1, RustFS
`1.0.0-beta.8-glibc`, fresh provider data and logs on the mounted workspace.
These are workload observations, not production throughput evidence. Cache
byte accounting, a larger concurrent hot set, release/acquisition fault
injection and multi-node routing remain open. Admission still serializes
repository transitions; no production concurrency target is claimed.

The only dependency change exposes the already locked `http-body` package as
a direct dependency so the response wrapper can preserve data frames, trailers
and size hints. No package version or checksum changed.

## Release and cleanup fault qualification

Fault tests exposed and fixed two failures in the first residency build:
removing the manager entry before local deletion lost the ability to retry a
failed cleanup; a repeated create could report success for an existing repository
after its Cell's release failed. The manager now distinguishes serving,
handle-refresh-required and confirmed-released entries. Released entries retain
their slot until cleanup completes. After any release error, requests require a
fresh resident handle from Cellule before creating a route; absent authority to
serve produces HTTP 503.

The tests pause a specific repository's canonical `Idle` control write through
a real object-store wrapper, after SQLite closure and before owner release.
They use the production HTTP server, Cell runtime, Git clients and persisted
control records, with these observed outcomes:

| Injected event | Verified outcome |
| --- | --- |
| Idle write succeeds but its reply is lost | Cellule resolves the exact published control; admission succeeds, local files are removed, and stock Git restores the original commit and bytes |
| Client disconnects while release is paused | Release and acquisition finish; retrying the pending creation succeeds; the evicted repository clones correctly |
| Idle write is denied before storage accepts it | New admission, repeated creation of the affected repository and its Git advertisement return 503; local SQLite remains; another repository serves normally; a fresh node restores exact Git content |
| Local directory deletion fails after successful release (Unix permissions) | HTTP 503 with durable control already Idle; SQLite files remain; repairing permissions lets the next clone retry cleanup and restore exact content |

The four fault tests and the existing three multi-server tests pass. Both
regressions were reproduced against the prior production code before applying
the fixes. Clippy, formatting and the binary build pass. The
test-only `async-trait` dependency reuses the already locked package; no package
version or checksum changed.

This qualifies those boundaries on the local object-store fixture. Automatic
recovery of a terminal failed release within the same node is not implemented;
restart remains required. Lease expiry during release, SQL worker close errors,
acquisition failures, process death at each boundary and the production storage
provider still need separate fault qualification. Cache byte accounting remains
open.


## Git cache admission qualification

Each disposable cache now owns its directory and shared disk reservation.
Construction admits compressed object bytes, config, HEAD and refs before
writing. Warm caches retain admission; active readers retain their generation
through replacement. Native receive-pack writes are measured after execution
and admitted before object staging or durable ref publication. Cache cleanup
precedes reservation release; failed deletion retains the charge until process
restart. Process guards now own cache and input lifetimes, so cancellation
signals the process group before releasing those owners.

A real HTTP test sends a delta pack that fits upload admission but expands
beyond available cache capacity. It receives 507, observes no stored Git object
or ref, then retries the identical request UUID and bytes successfully after
freeing capacity. Cold hydration separately fails with 507, releases partial
cache admission and rebuilds successfully after capacity is restored. The test
also verifies warm-cache accounting and release when a push replaces it.
Focused tests cover compressed-file accounting, last-reader cleanup, native
write reconciliation and Unix deletion failure. Existing subprocess cancellation,
Git/LFS, residency and release-fault tests pass; Clippy, formatting and the
binary build pass. No dependency or schema change was needed.

The combined four-repository RustFS process smoke passed on Darwin arm64 with
Apple Git 2.50.1 and RustFS `1.0.0-beta.8-glibc`, using fresh provider data and
logs on the mounted workspace:

| Action | Observed debug-build time |
| --- | --- |
| One push containing two 40 MiB random blobs | 17.23 seconds |
| Initial 256-file push | 0.42 seconds |
| One-file update and annotated tag | 0.51 seconds |
| v0 clone and verification, cold after takeover | 26.86 seconds |
| v2 clone and verification, warm | 3.33 seconds |
| 256-file clone and verification after takeover | 0.47 seconds |

Both large clones restored an 83,912,147-byte pack with matching file hashes
and clean `git fsck` results. The run also passed resident eviction, Git/LFS,
ACL, mixed/atomic ref outcomes, exact dropped-reply replay, clean restart,
owner death, lease expiry and local database loss. These observations do not
establish production throughput or a controlled comparison with earlier builds.
Native Git's peak scratch use, filesystem allocation overhead, cleanup after
process death and durable storage quotas remain open; completed-write accounting
does not satisfy those release gates.


## Consistent ref pagination and compressed Git requests

A repository-wide generation now commits atomically with each accepted ref plan.
Every bounded ref page includes that generation; continuations reject changes,
including deletion/recreation and ABA updates. The gateway attempts at most three
scans before returning a retryable 503. Object writes and exact push replay do not
advance the generation. This extends the unreleased schema version 1; no shipped
schema migration or old request-digest reader is introduced.

The real HTTP fixture publishes 300 refs across multiple pages, then changes two
refs on opposite sides of a page boundary in one transaction. During 32 concurrent
updates, the observed run returned 28 consistent advertisements and four retryable
503s. Deterministic Cell checks cover stale continuation rejection, empty terminal
pages, deletion/recreation, ABA, rejected plans and exact replay. Stock Git lists
and clones all 300 refs after gateway replacement and passes `git fsck`; owner
recovery preserves the durable generation. Focused tests, Clippy and the binary
build pass.

This workload exposed a stock-client clone failure: larger fetch requests used
gzip, but the gateway omitted CGI content encoding. The gateway now forwards
identity/gzip semantics, rejects unsupported or stacked encodings, and binds gzip
to the versioned push request digest. Explicit gzip, x-gzip and case-insensitive
gzip fetches pass. Reusing a completed push UUID with changed encoding returns
409; explicit identity encoding replays the original result without changing refs.
Native decompression and pack expansion still need resource enforcement.


The combined RustFS process smoke now publishes 300 additional refs in its
256-file repository and verifies every restored ref OID after owner death, lease
expiry and local disk loss. The corrected qualification passed all four repository
Cells, resident eviction, Git/LFS, ACL, mixed/atomic outcomes and dropped-reply
replay. Its 80 MiB push completed in 17.60 seconds; v0/v2 clones restored
83,912,147-byte packs with matching hashes and clean `git fsck` in 33.39/5.42
seconds. The 256-file/300-ref recovery check completed in 1.48 seconds. Environment:
Darwin arm64, Apple Git 2.50.1, RustFS `1.0.0-beta.8-glibc`, fresh bind-mounted
provider data. These are correctness/workload observations, not capacity claims.
