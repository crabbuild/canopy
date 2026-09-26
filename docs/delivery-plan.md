# Deliver Canopy

The executable acceptance gates below define when Canopy can be called a Git
hosting service. Each gate needs a black-box client action, the durable side
effect, and the same visible result after a new node restores Cell state.
Do not infer completion from compilation or a disposable cache test.

| Gate | Deliverable | Acceptance proof | State |
| --- | --- | --- | --- |
| 0 Independent build | Pin an immutable Cellule revision; build `canopy-server` without local paths or Crab product crates | Fresh checkout builds in CI | Partial: immutable Git revision pinned and local fresh-checkout proof; hosted CI pending a Canopy remote |
| 1 Node process | `canopy` binary, validated config, CellNode lease/renewal, listener, readiness, drain | Start/stop against durable store; no worker or lease leak | Partial: S3-compatible process restart and clean drain pass; worker/lease fault matrix remains |
| 2 Repository lifecycle | Directory Cell, create/list/get/rename, account identity, token scopes, repository ACL | Two users see only authorized repositories; failed creation converges on one UUID | Partial: durable accounts, per-repository Git/LFS roles, default branches and authorized repository list/get survive recovery; account lifecycle and collaborator roster remain |
| 3 Git object path | Bounded pack ingest, SQLite object chunks, verified external large blobs, quotas | Push delta pack; restore exact bytes and OIDs after owner loss; reject corruption | Partial: disk-accounted 512 MiB pushes, incremental Git reads, bounded atomic object batches and SQLite chunks for large trees/commits/tags work; per-object buffers and 64 MiB object ceilings remain |
| 4 Atomic push | Durable push session, graph closure proof, ACL and branch rules in finalization, recorded retry outcome | Concurrent and multi-ref pushes, ABA, owner death at every publication boundary | Partial: ref CAS, ACL, ABA protection, ordinary mixed push results, atomic rejection and exact HTTP push replay survive recovery; typed graph closure uses bounded certificate commands; branch rules and publication fault matrix remain |
| 5 Fetch | Bounded streaming upload-pack, snapshot refs, cold recovery | Clone/fetch after owner takeover while refs move; large corpus capacity evidence | Partial: paginated refs/objects, gzip requests and backpressured fetch work; v0/v2 clones above 80 MiB and a 13,591-object real history pass after takeover; native scratch limits and production capacity proof remain |
| 6 LFS | Batch/basic transfer, verified bytes, quotas and transfer admission | Stock `git-lfs` push/pull after owner loss; wrong hash/size and interruption fail closed | Partial: stock push/pull after gateway restart works; shared node transfer admission and LFS reception deadlines implemented; quotas remain |
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
   and existence queries group up to 128 IDs. Large trees, commits and tags now
   use verified SQLite chunks. Graph certification now uses commands capped at
   128 objects and 64 MiB of SQLite bytes. Cold hydration reads bounded object
   pages. Expand real corpus qualification, measure traversal memory and native
   resource limits, and keep the bare repo disposable.
3. Qualify durable push replay at every staging/publication boundary. Exact
   HTTP replay now binds a UUID to account and request digest and atomically
   publishes the complete response with ref changes. Typed graph connectivity
   now gates both ref commands. Add branch rules; define retention and quotas
   for completed outcomes and abandoned staging chunks before persistent use.
   Keep testing distinct IDs for identical bytes after refs change: Cellule
   command deduplication alone does not identify an HTTP operation.
4. Complete account lifecycle, token rotation/revocation, collaborator roster
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


## Bounded gzip input qualification

Gzip requests now validate and decode completely into a disk-admitted spool
before Git starts. Encoded and decoded bytes share admission and independently
obey the request size limit. The original encoded bytes still define exact push
replay. Corrupt checksums, truncation and trailing garbage return 400 without
publishing Git objects or refs. Decoded overflow returns 413; decoded disk
exhaustion returns 507, and retrying the identical push UUID succeeds after
capacity is freed. Valid concatenated gzip members are supported.

The decoder has a 120-second deadline and cancellation checks on compressed
reads and decoded writes. A deterministic queued-worker test proves that
cancellation retains admission until the worker exits. Other focused tests cover
wire digest preservation, exact decoded-size boundaries, multiple members,
corruption after partial output and reservation cleanup. The HTTP suite verifies
64 MiB plus one decoded byte is rejected, and a compressed ref deletion retries
and replays without advancing the ref generation twice.

A valid 11 MiB decoded protocol-v2 request exposed Git CGI's smaller default
input buffer. That failure was reproduced before aligning the subprocess buffer
with Canopy's existing 64 MiB fetch admission limit; the same request now passes.
The Git backend accepts only decoded input. Ten input tests, smart HTTP, stock
Git/LFS, owner recovery, formatting, Clippy and the binary build pass.

The RustFS process qualification with `--many-objects 256` also passes: 300 refs,
both commits, the annotated tag and exact file bytes restore after owner death,
lease expiry and disk loss, with clean `git fsck`. Git/LFS, ACL, mixed/atomic
outcomes and dropped-reply replay pass. The recovery check took 0.68 seconds in
this debug-build observation on Darwin arm64 with Apple Git 2.50.1 and RustFS
`1.0.0-beta.8-glibc`; it is not production capacity evidence. This change adds no
dependency, schema or request-digest format change. Native pack expansion, peak
scratch usage and global request-concurrency admission remain open.


## Node transfer admission

Each node now admits eight Git/LFS repository requests across its entire
repository set. Excess requests receive 503 and `Retry-After: 1` immediately.
Admission begins before cold repository resolution and lasts through tracked
handler work, blocking input workers, native Git and outstanding HTTP data
frames. Gzip workers retain their permits after timeout until cancellation has
actually stopped the worker. A disconnected client does not interrupt an
already authorized push publication; its durable identity resolves retries.
Shutdown waits for tracked request work before draining Cells.

LFS batch and object PUT body reception now has a 120-second deadline, returning
408 on timeout. Existing request-size limits are unchanged. The lower-layer Git
fixtures explicitly omit node admission; the composed server always supplies it.
The existing Tokio dependency gains only the `test-util` development feature
for a deterministic virtual-time timeout test. No lockfile, schema, request-digest
or runtime dependency revision changes are needed.

A real TCP test holds eight uploads across two repositories. Both a Git request
and an LFS request receive 503 with the retry header, while health, readiness and
repository listing remain available. Disconnecting one upload permits a Git
advertisement; the other seven uploads finish and download with identical bytes.
Response-body tests cover trailers, errors, unread body disposal and cloned
frames retained after EOF. The queued gzip cancellation test also proves permit
retention; the subprocess cancellation test proves admission is released with
process cleanup. All 35 library tests, eight multi-server tests, the stock Git
smart HTTP suite and the direct Git backend test pass. Formatting, Clippy and
the binary build pass.

The additional production code provides one shared admission path and a body
owner that cover background work and outstanding response bytes beyond the
handler lifetime. Eight is an initial bound, not a measured concurrency target. Native Git
peak scratch/RAM, slow outgoing LFS sockets, fairness across accounts,
management/authentication capacity and production throughput remain open.


The RustFS process smoke with `--many-objects 256` also passes for this build:
initial push 0.64 seconds, incremental push 0.93 seconds, and recovery of both
commits, the annotated tag, all 300 additional refs and exact file bytes in
1.09 seconds with clean `git fsck`. Stock Git/LFS, ACL checks, mixed/atomic push
outcomes and dropped-reply replay survive restart, disk loss and lease takeover.
These are debug-build observations on Darwin arm64 with Apple Git 2.50.1 and
RustFS `1.0.0-beta.8-glibc`, not production performance targets.


## Large Git metadata objects in SQLite

Trees, commits and annotated tags above 768 KiB now use SQLite chunks, with a
64 MiB per-object ceiling. Each staging command writes at most 512 KiB; ordinary
object reads and fetch hydration reconstruct and verify those parts. Objects
remain invisible until the existing typed `PutObjects` command verifies their
chunk count, every part length, canonical Git OID and BLAKE3 in its publication
transaction. Its codec advances to 2. It limits aggregate SQLite verification
work to 64 MiB per batch in addition to the existing 128-record and 768 KiB
inline-payload ceilings. Large blobs and LFS retain their external body path.

The new module owns staging and shared chunk reconstruction. The code growth
implements a missing storage capability and keeps publication and reads on one
validation path. It does not add a second ref-publication path. The current
unreleased schema changes; use a fresh development storage prefix, with no
compatibility reader or dependency revision change.

Cell checks prove staged objects are invisible, exact staging retries replay,
duplicate uploads converge, malformed chunks reject the entire object batch,
and corruption is detected during reads. Missing dependencies in chunked trees,
commits and tags still reject ref publication. A stock-Git server test pushes a
32,000-entry tree and commit/tag messages above 1 MiB, then starts a fresh node,
clones, compares raw bytes and OIDs, and passes `git fsck --strict --full`.
The existing server, smart HTTP and object-batch checks pass with this layout.

`scripts/smoke_s3_process.py --sqlite-chunks` adds the same large-object fixture
to real-process owner-death and cold-recovery qualification. Whole-object buffers,
large initial graph traversals, durable staging retention and production-scale
performance remain open; this removes the previous 768 KiB non-blob rejection.


The process smoke with `--sqlite-chunks --many-objects 256` passes against
RustFS `1.0.0-beta.8-glibc`: chunked metadata push 4.93 seconds and recovery
1.17 seconds, exact raw object bytes/OIDs and strict fsck confirmed after owner
death, lease expiry and local disk loss. The 256-file initial/incremental pushes
took 0.71/0.96 seconds; the 300-ref recovery check took 0.97 seconds. Stock Git/LFS,
ACL, mixed/atomic ref outcomes and dropped-reply replay also pass. These are
debug-build observations on Darwin arm64 with Apple Git 2.50.1. Scoped tests,
formatting, Clippy, Python syntax validation and the binary build pass.


## Bounded graph certification before ref publication

Initial-history traversal no longer runs inside the ref transaction. One async
postorder preparation path serves direct finalization and HTTP push completion.
It groups dependency-ordered candidates into `CertifyObjects` commands of at most
128 object IDs and 64 MiB of SQLite verification bytes. The Cell command reads
and verifies stored objects itself and requires each typed child to have a durable
certificate, querying dependencies in groups of 128. Repeated references to the
same object/type share a proof. The final transaction checks only the new tips,
ACL, ref CAS, namespace conflicts and outcome publication.

Valid certificate batches survive later failure and can be reused. No partly
certified graph can publish a ref. This replaces the former single-transaction
traversal; there is no fallback to that path. The extra preparation module owns
async traversal and error/lifetime handling while `graph.rs` owns authoritative
certificate checks and parsers. Operation 6 uses codec 1; schema and dependency
revisions are unchanged, but the module digest changes with the new behavior.

A 270-leaf test deliberately omits the last dependency. Exactly 256 certificates
commit in bounded batches, while the root stays uncertified and refs/generation
stay unchanged. Adding the dependency and retrying certifies the remaining graph
and publishes one ref generation. Direct low-level ref publication without a root
certificate is rejected. Wire-level command checks bypass the preparer to prove
incorrect ordering, missing/wrongly typed dependencies and malformed object graphs
are rejected by the registered Cell handler; a failed batch rolls back earlier
certificates in that batch. Existing ACL, CAS, ABA, atomic/mixed push, replay,
large-object clone and owner-recovery tests pass.

Remaining capacity work includes traversal frontier memory, large distinct edge
sets, native Git scratch and a representative real-repository corpus. Certificate
batches add durable publications; latency measurements must include them. The
lease/storage publication fault matrix still needs owner loss during preparation
at each boundary; this change does not close that gate.


An initial process run passed recovery but observed 2.94/4.03 seconds for the
256-file initial/incremental pushes. Inspection exposed per-child metadata
calls in preparation. These now use 128-child pages and reuse returned immutable
metadata; repeated roots are deduplicated too. The partial-progress and typed
command rejection tests still pass after this change, along with all nine
multi-server tests, owner restart and stock smart HTTP. Timings from this host
are observations, with no controlled throughput or latency claim.


The final RustFS process run with `--sqlite-chunks --many-objects 256` passes:
large metadata push 2.08 seconds; 256-file initial/incremental pushes 0.95/0.89
seconds; chunked-object recovery 1.36 seconds; 300-ref repository recovery 1.07
seconds. Exact object bytes/OIDs and strict fsck survive owner death, lease
expiry and local disk loss. Git/LFS, ACL, mixed/atomic outcomes and dropped-reply
replay pass. Environment: Darwin arm64, Apple Git 2.50.1, RustFS
`1.0.0-beta.8-glibc`, debug build. These observations do not isolate host load
from code changes or establish production capacity. Formatting, scoped tests,
Clippy and the binary build pass.

## Durable default branch

Repository Cells now persist symbolic HEAD alongside `ref_generation`, initially
`refs/heads/main`. The SDK changes it through one SQL compare-and-set: immutable
owner authorization, expected generation, and target existence are checked in
the update itself. A repository with no live branches can choose an unborn
target. Every accepted selection advances the generation, including selecting
the same name; replaying the same SDK mutation does not advance it twice.

`GET` and `PUT /api/repositories/<name>/default-branch` expose this state. GET
requires repository read access. PUT requires an admin-scoped owner token and
the repository UUID plus expected generation. UUID mismatch, stale state and
an absent target with other live branches return 409. Invalid references return
422. A failed/ambiguous publication asks the caller to read current state before
retrying. Collaborators cannot change HEAD, even with an admin-scoped token.

Ref pages now carry HEAD in their existing single-statement snapshot. The cache
key includes HEAD and generation, and a new cache creates its accounted HEAD
file before serving readers. Branch deletion remains allowed and leaves the
symbolic target unchanged. No automatic first-push branch selection is added.
Schema version 1 remains unreleased; use a fresh development storage prefix.

Direct Cell tests cover invalid input, absent targets, owner checks, stale
generations after pushes and HEAD ABA, exact mutation replay, pagination fencing
and concurrent changes with one winner. Stock Git v0/v2 discovery and clones
switch from a warm main cache to trunk and restore the expected checked-out
bytes after owner restart on fresh local storage. Protocol-v2 empty clone and
raw unborn `ls-refs` also pass; deletion/recreation of the default branch retains
the chosen name. All ten multi-server tests, repository Cell, Git backend,
smart HTTP, owner-restart and four disk-cache tests pass. Clippy with warnings
denied, formatting and the binary build pass; dependencies are unchanged.

The RustFS process smoke with `--sqlite-chunks --many-objects 256` also passes.
It changes HEAD to trunk, renames the repository, and checks the exact saved
HEAD/generation after clean restart and after SIGKILL, lease expiry and fresh
local disk recovery. Owner and collaborator clones check out trunk and restore
Git/LFS bytes. The same run preserves 300 refs, large SQLite objects, mixed and
atomic push outcomes, and dropped-reply replay. Environment: Darwin arm64,
Apple Git 2.50.1, RustFS `1.0.0-beta.8-glibc`, debug binary. This is recovery
evidence; default-branch updates still rebuild a cache and have no production
latency claim.

Branch protection, account lifecycle, repository get/list permissions, backup,
GC, native resource ceilings, representative capacity evidence and the broader
publication fault matrix remain open gates.

## Authorized repository discovery

Authenticated collaborators can now list their accessible repositories and read
`GET /api/repositories/<name>`. Get returns identity, clone URL, membership role,
default branch and ref generation, with 404 for missing or inaccessible names.
Owners retain a Directory-only listing path. Both use the same UUID pagination
contract; there is no retained name-cursor reader in this unreleased API.

The Directory Cell records account/repository candidates before the product
grants an ACL in a Repository Cell. Candidates survive revoke and regrant;
listing rechecks each authoritative Cell ACL. This avoids both publishing access
without a listing candidate and deleting a concurrent grant's candidate during
revocation cleanup. Candidate-only interrupted work remains invisible in result
entries. Direct SDK ACL primitives require their coordinator to record a
candidate first. Candidate compaction and quotas remain open.

Each page examines at most 32 candidates. Owner/UUID and account/UUID indexes
support a merge of bounded key ranges. Local SQLite 3.53.4 `EXPLAIN QUERY PLAN`
inspection showed index searches without a temporary sort after the query was
adjusted to project the discovery index's ordering column.

The first larger test exposed a Cellule admission constraint: the pinned runtime
uses a movement budget of two completed moves per 1,000 ms. Retrying a complete
cold page could repeatedly scan the same prefix without finishing. The manager
now returns its completed prefix with a continuation on capacity exhaustion;
no-progress admission returns 503 with `Retry-After: 1`. Revoked entries advance
the cursor too. Empty pages are valid while a continuation remains. Other
storage/runtime failures still fail the request. This preserves the runtime's
movement limits without a dependency patch or a second residency policy.

A stock HTTP test creates and grants 33 repositories, verifies bounded ordered
pagination without duplicates, renames across a cursor, revokes the first 32,
and reaches the sole remaining accessible repository through empty pages.
Fresh-owner recovery preserves the index, ACLs and metadata, and regrant restores
visibility at the same position. An unrelated account sees no entries and cannot
read metadata. Directory tests verify pending-name exclusion, owner-only candidate
insertion, self-grant rejection, exact replay, candidate-only invisibility to
the Cell ACL, and candidate recovery through rename. The eleven multi-server
tests and Directory Cell test pass; the older owner-only listing expectation
was replaced with an assertion for the reader's single authorized repository.
Clippy with warnings denied and the binary build pass; dependencies are unchanged.


The RustFS process run with `--sqlite-chunks --many-objects 256` passes as well.
Owner and reader list/get checks survive repository rename, clean restart,
SIGKILL, lease expiry and fresh local disk restore. Revocation after takeover
removes the repository from the reader's list and makes metadata get return
404. The same run verifies Git/LFS, default branch, 300 refs, SQLite chunks,
mixed/atomic outcomes and dropped-reply push replay. Environment: Darwin arm64,
Apple Git 2.50.1, RustFS `1.0.0-beta.8-glibc`, debug build. These are functional
and recovery checks, not a production capacity measurement. The Directory schema
changes require a fresh development prefix; persistent upgrade support remains
a release gate.

## Bounded cold object reads

Cold hydration now reads up to 128 records and 768 KiB of inline bodies per
page. One metadata query selects the prefix and one body query reads its exact
IDs at or after that receipt. Immutable records and the absence of GC protect
the two observations; future collection must fence active readers. Inline
verification runs on a blocking worker and moves owned SQL bodies into the
result. Chunked and external descriptors still require body verification on
read. Short pages continue until an empty page; corrupt records fail closed.
The old single-record API and its payload clone are removed.

The Repository Cell test covers empty and exact 128-record boundaries, multiple
pages without missing or duplicate OIDs, exact 768 KiB payloads, short pages,
zero-length bodies, chunked/external descriptors, and corrupt size/digest/OID
rejection. Stock smart HTTP and owner-restart tests pass with the new reader.
All-target check, clippy with warnings denied, format and binary build pass.
No dependency, lockfile, schema or command codec changes are required.

The new `--corpus-repository` process-smoke option bundles an existing checkout's
HEAD history without updating its source. Generated fixtures live under the
specified workspace parent. After owner death and fresh-disk takeover, protocol
v0/v2 bare clones must match the source's HEAD and a SHA-256 inventory covering
every reachable object's OID, type, size and raw bytes, then pass strict/full
`git fsck`. Other source refs are outside this fixture's scope.

The combined RustFS run with SQLite chunks, 4,096 files and the existing ripgrep
checkout passes. The source HEAD was
`3fce3b5bb0236da2df6d99672afb8a719642eca7`: 2,287 commits, 13,591 reachable
objects and 121,466,167 raw object bytes. Source and both recovered clone
inventories have SHA-256
`50eb7182122b3524f290a5f5ac2a4620a092b0627916946c40b0d5a72899a29e`.

| Observed operation | Seconds |
| --- | ---: |
| 4,096-file initial push | 8.08 |
| One-file update plus annotated tag | 2.70 |
| 4,096-file recovery clone and verification, including 300 extra refs | 2.41 |
| ripgrep initial push | 207.17 |
| ripgrep cold protocol v0 clone | 75.75 |
| ripgrep subsequent warm protocol v2 clone | 1.91 |
| ripgrep v0 / v2 byte inventory and strict fsck | 0.68 / 0.71 |

Environment: Darwin arm64, Apple Git 2.50.1, RustFS
`1.0.0-beta.8-glibc`, debug Canopy build, storage on the mounted workspace.
The pre-change 4,096-file run observed 8.77 / 3.55 / 3.25 seconds for initial
push / incremental push / recovery verification. These individual runs are
functional evidence, not a controlled speedup measurement. The corpus protocol
times have different cache states and do not compare protocol performance.
The same final run passes Git/LFS, discovery, ACL revocation, default branch,
mixed/atomic outcomes and lost-reply replay across restart, disk loss and lease
takeover. Full hydration, debug corpus latency, native scratch, traversal memory,
release-build load measurements and broader production capacity remain open.
