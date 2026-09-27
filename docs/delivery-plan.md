# Deliver Canopy

The executable acceptance gates below define when Canopy can be called a Git
hosting service. Each gate needs a black-box client action, the durable side
effect, and the same visible result after a new node restores Cell state.
Do not infer completion from compilation or a disposable cache test.

| Gate | Deliverable | Acceptance proof | State |
| --- | --- | --- | --- |
| 0 Independent build | Pin an immutable Cellule revision; build `canopy-server` without local paths or Crab product crates | Fresh checkout builds in CI | Partial: immutable Git revision pinned and local fresh-checkout proof; hosted CI pending a Canopy remote |
| 1 Node process | `canopy` binary, validated config, CellNode lease/renewal, listener, readiness, drain | Start/stop against durable store; no worker or lease leak | Partial: S3-compatible process restart, selected-release admission and supervised fleet maintenance drain pass; worker/lease fault matrix remains |
| 2 Repository lifecycle | Directory Cell, create/list/get/rename, account identity, token scopes, repository ACL | Two users see only authorized repositories; failed creation converges on one UUID | Partial: accounts and disablement, token issuance/listing/revocation/expiry and account issuance limits, repository roles/rosters, default branches and authorized repository list/get survive recovery; browser account/token administration implemented; account deletion and audit records remain |
| 3 Git object path | Bounded pack ingest, SQLite object chunks, verified external large blobs, quotas | Push delta pack; restore exact bytes and OIDs after owner loss; reject corruption | Partial: disk-accounted 512 MiB pushes, incremental Git reads, bounded atomic object batches and SQLite chunks for large trees/commits/tags work; external blobs stream up to 5 GiB; non-blob buffers/64 MiB ceilings and native process resource bounds remain |
| 4 Atomic push | Durable push session, graph closure proof, ACL and branch rules in finalization, recorded retry outcome | Concurrent and multi-ref pushes, ABA, owner death at every publication boundary | Partial: ref CAS, ACL, ABA protection, ordinary mixed push results, atomic rejection and exact HTTP push replay survive recovery; typed graph closure uses bounded certificate commands; exact branch rules, required checks and verified ancestry implemented; publication fault matrix remains |
| 5 Fetch | Bounded streaming upload-pack, snapshot refs, cold recovery | Clone/fetch after owner takeover while refs move; large corpus capacity evidence | Partial: paginated refs/objects, gzip requests and backpressured fetch work; v0/v2 clones above 80 MiB and a 13,591-object real history pass after takeover; native scratch limits and production capacity proof remain |
| 6 LFS | Batch/basic transfer, verified bytes, quotas and transfer admission | Stock `git-lfs` push/pull after owner loss; wrong hash/size and interruption fail closed | Partial: bounded streaming LFS with a 5 GiB acceptance ceiling, shared transfer admission and deadlines implemented; stock push/pull after restart works; quotas and full-scale capacity proof remain |
| 7 Collaboration | Issues, comments, checks, rules, pulls, reviews, merge, releases, repository UI | Create, review, check, merge and reload across owner change | Partial: issue/comment, check/rule, pull/review, comparison, review requirements, atomic fast-forward merges and native merge/squash candidates, repository browser, issue/pull UI and bounded unified diffs and line discussions implemented; rebase, discussion moderation and releases remain |
| 8 Recovery and operations | Two-node routing, backups, restore, conservative GC, audit and metrics | Kill owner, lose local disk, restore from backup, clone and inspect collaboration data | Partial: signed HTTPS routing across live nodes, survivor takeover without restart, cold clone, fenced Unix runtime reclamation, conservative maintenance admission, enrolled owner recovery, same-provider backup and isolated restore implemented; full maintenance/routing/backup fault matrices, GC and telemetry remain |
| 9 Public service | Public visibility, organizations/teams, search and webhooks | ACL-safe anonymous reads, revocation, index rebuild and webhook retry | Partial: public Git/LFS, browser and collaboration reads, owner visibility controls and privacy revocation implemented; organizations, search, index rebuilding and webhooks remain |

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
   hot sets. Each node reserves one SQL slot for Directory takeover and admits
   three repository gateway entries, backed by local or remote Cells. Inactive
   local repositories release their Cell and reload on demand; unpinned remote
   entries can be dropped without releasing their owner. Admission returns 503
   when no entry is safe to evict. The resident limit is not a production capacity target.
   Hydration and retained caches now use shared disk admission; native Git's
   completed writes are measured before publication, but its peak usage remains
   unbounded. All native workers now discard host configuration, object paths,
   tracing and provider credentials, with temporary paths inside their cache.
   Enforce the remaining byte ceiling with filesystem quotas or a proven bound
   on every native write; periodic sampling and per-file limits alone cannot
   prove aggregate peak usage. Managed runtime recovery now fences live nodes
   and Unix Git descendants before reclaiming crash-left files. Qualify OS power
   loss and Windows process containment next. A node supervisor retains startup
   and drain across caller cancellation; abrupt runtime destruction retains local
   exclusion until process restart when SQL drain is unconfirmed.
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
   and current branch rules now gate both ref commands. Define retention and quotas
   for completed outcomes and abandoned staging chunks before persistent use.
   Keep testing distinct IDs for identical bytes after refs change: Cellule
   command deduplication alone does not identify an HTTP operation.
4. Complete account deletion and audit records.
   Browser administration supports account creation/disablement and token
   issuance, expiry selection, listing and revocation.
   Active-credential and rolling issuance limits are enforced in the Directory.
   Qualify admitted Git/LFS operations during revocation and owner takeover.
5. Continue collaboration as vertical slices: rebase and conflict resolution; discussion editing/moderation;
   issue labels/assignees; releases and assets; collaboration UI. Each
   slice ships with its own public action and owner-recovery proof.

6. Complete offline operations in this order:
   - Expand the implemented maintenance recovery into a complete fault matrix.
     The enrolled worker fences expired sessions, restores their pinned roots
     and releases their Cells. Remaining acceptance: SIGKILL at each claim,
     restore and drain boundary; competing workers and long lease failures;
     no old writer can publish, and resume stays denied until every catalog entry
     is settled. A force-resume flag is not sufficient evidence.
   - Expand the implemented enrolled backup/restore into an interruption matrix:
     SIGKILL at pin publication, runtime/body copy and completion; competing
     workers; stalled storage and lease loss. Capture must reject concurrent
     changes, incomplete destinations must never serve, and same-operation
     retries must converge. Pinned SQLite roots provide the external body manifest.
   - Extend independent same-provider copy qualification to the production store
     and cross-provider export. Preserve identity/release validation, exact Git/LFS
     bytes and collaboration/ACL state after source deletion. Add old-release
     migration only with explicit format/version proof.
   - Add retention and collection only after roots include active transfers,
     pending operations, recovery roots and backup pins. Acceptance: concurrent
     fetch/merge/backup and injected collector failure never remove required
     bytes; crash-left uploads become collectible only after the grace period.

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

## Release-build recovery timing

The node now emits debug timing for Repository Cell acquisition and cache
hydration, including page reads, body retrieval, cache writes and raw/admitted
cache bytes. The run and smoke instructions use `--release`. The corpus runner
preserves Git's stderr and source exception on failure.

An optimized build passed the same ripgrep HEAD history fixture on Darwin arm64
with Apple Git 2.50.1 and RustFS `1.0.0-beta.8-glibc`. After SIGKILL, lease expiry
and fresh local disk, both protocol clones matched all 13,591 objects and the
previous SHA-256 inventory, then passed strict/full fsck. The base Git/LFS,
discovery, default-branch, ACL, mixed/atomic ref and dropped-reply checks passed.

| Observed operation or phase | Seconds |
| --- | ---: |
| Initial corpus push | 102.83 |
| Cold protocol v0 clone | 47.94 |
| Repository Cell acquisition within that cold clone | 14.05 |
| Full cache hydration within that cold clone | 31.65 |
| Object page reads and inline verification within hydration | 26.57 |
| Cache writes within hydration | 5.08 |
| Subsequent warm protocol v2 clone | 2.18 |
| Each clone's byte inventory and strict fsck | 0.80 |

The 121,466,167 raw object bytes produced a 32,099,929-byte admitted bare cache.
Phase times are nested and must not be added to the enclosing operation time.
This single run identifies object reads and acquisition as the dominant measured
costs; it does not attribute page time to a particular storage or CPU operation.
The pinned runtime uses sparse activation and actor-serialized local queries.
Compression policy remains unchanged because cache writes were the smaller
component. Next performance work should measure cold SQLite page fetches and
root validation before changing the cache representation or dependency policy.
No production throughput, percentile or concurrent-load claim follows from
these measurements; debug/release and cold/warm runs are different conditions.

Before this successful release run, a timed debug rerun failed during corpus
object publication with `InvocationError::Pending`. No corpus recovery check
ran in that attempt. The request identity had not expired; the typed dependency
error retains mutation evidence but omits the underlying `OutcomeUnknown`
source, so the cause remains unresolved. The successful release run does not
close that fault gate. Canopy continues to reject unproven publication; no
retry, deadline extension or dependency patch was introduced.

## Durable token lifecycle

Accounts now support multiple scoped credentials with opaque IDs and creation
timestamps. Admins list, issue and revoke their own account's tokens; the site
owner can manage all enabled accounts. Listing returns bounded UUID pages of
metadata without digests or secrets. Exact active issuance retries converge;
revoked IDs and secrets remain reserved. Rotation uses a separately issued
replacement, followed by client/deployment updates and revocation of the old ID.

The Directory token module owns transaction-level authorization and the guard
against revoking the site's final active admin. HTTP token management supplies
the exact actor digest and site policy from server configuration. The related
account-creation path now also checks the site-admin credential inside the SQL
transaction that issues the first token. Trusted startup bootstrap uses the same
creation implementation without requiring a pre-existing credential. The pinned
Cellule SQL batch contract executes its statements in one command transaction
and records the result for command replay; the token decision precedes the
conditional update so successful self-revocation is not reported as failure.

The HTTP recovery test proves:

- Own-account and site-owner authority, denied foreign administration, insufficient
  scope and mismatched Basic usernames.
- Exact issue retries, global digest conflicts, invalid input, bounded ordered
  pagination over 35 entries, and metadata that contains no digest or secret.
- Paused token and account creation requests whose credentials are revoked after
  HTTP authentication cannot issue credentials when their bodies arrive.
- Two concurrent self-revocations leave one site-admin token active; read-scoped
  tokens do not satisfy that guard.
- Stock Git cloning with an issued credential, persistent denial of revoked
  credentials for Git/LFS, and a successful clone after fresh-owner restore using
  the rotated configured admin token.

Directory recovery and the existing two-repository Git/LFS test pass, as do
all-target clippy with warnings denied and the release binary build. The RustFS
process smoke with `--sqlite-chunks --many-objects 256` also passes: it rotates
the reader token before restart, confirms the old token stays denied for API,
Git and LFS after SIGKILL/lease takeover, and clones with the replacement. The
same run retains the default branch, ACL behavior, 300 refs, SQLite object chunks,
mixed/atomic ref outcomes and lost-reply replay. Environment: Darwin arm64,
Apple Git 2.50.1, RustFS `1.0.0-beta.8-glibc`, release build.

This changes the unreleased Directory schema; use a fresh development prefix.
Deployment configuration must contain an active owner admin token on restart.
Token revocation blocks new authentication; admitted Git/LFS operations may
finish subject to current repository ACLs. Account disable/delete, token expiry,
issuance quotas, retained-record cleanup and audit remain open. Dependencies
and lockfiles are unchanged.

## Owner collaborator roster

Repository owners can inspect explicit grants through
`GET /api/repositories/<name>/collaborators`. The response keeps the immutable
owner separate, identifies the Repository Cell UUID, and returns at most 32
account-ordered grants. A full page supplies a name cursor; an exact multiple
requires an empty terminal page. Pages observe current membership independently.
The existing member primary key supports the scan without a schema change.

The Repository Cell SDK checks owner authority in the same bounded read batch
as its membership rows. The pinned Cellule query path runs against the
actor-serialized durable logical head. HTTP requires both admin token scope and
repository ownership, and retains the residency pin through the read. Default
branch reads and updates now share their existing routing/authorization helper
with the roster; their access policy is unchanged. Grant/revoke mutations keep
using the canonical Cell ACL, while Directory candidates remain discovery data.

The HTTP integration test creates 33 grants, verifies both pagination boundaries,
updates a role, removes a grant, follows an existing cursor, renames the repository,
and restores the exact roster using fresh local storage. It proves that anonymous
requests, outsiders, collaborators with admin-scoped tokens, and the owner using
a read-scoped token cannot list grants. Direct SDK tests prove owner enforcement
without HTTP and an empty roster after revocation. The existing default-branch
recovery test also passes. All-target Clippy with warnings denied and the release
binary build pass.

The release RustFS process smoke with `--sqlite-chunks --many-objects 256`
passes. The owner sees the same UUID and explicit reader grant before shutdown,
after clean restart, and after SIGKILL/lease expiry with fresh local storage.
Revocation then produces an empty roster and denies reader discovery and Git
access. The same run verifies rotated credentials, Git/LFS bytes, SQLite chunks,
300 refs, mixed/atomic push outcomes and lost-reply replay. RustFS startup emits
fresh-volume initialization warnings; the qualification exits successfully.

No dependency, lockfile, schema or codec changes. This slice adds one read API;
account disable/delete, audit records and broader collaboration gates remain open.


## Repository-local issues and comments

The Repository Cell now stores numbered issues and comments alongside the ACL
and Git metadata. Seven HTTP operations support creation, list/detail reads,
comments, complete text edits, closing and reopening. All writes carry the
expected repository UUID. Read members with a write-scoped token may participate;
authors and repository writers may edit. The Cell checks current membership and
edit authority in the mutation transaction, including after body reception.

Client UUIDs bind creation to the original author/content and comment parent.
Exact retries after edits or recovery return the original number without changing
current contents. Edits compare an explicit version and reject stale updates.
The Cell captures the decision, applies its guarded write and records the result
in one SQL batch. The pinned dependency executes this inside the application
transaction, and its normal mutation identity records exact command replays.

Issue summary pages contain at most 32 entries; comments contain at most 16
bodies. Titles admit 256 UTF-8 bytes and bodies 16 KiB. Request reception is
bounded to 128 KiB / 30 seconds. Indexed number scans, an issue-state index and a
parent/comment-number index bound each read. Summaries omit bodies; full comment
pages remain below the SQL result limit. There is no whole-history scan or new
external content store on these paths.

Proof completed:

- Direct Cell calls: owner/member authorization without HTTP, exact mutation
  receipt replay, read members creating discussions, writer moderation, stale
  edits, all four mutations denied after membership revocation, maximum body
  size and a UTF-8 title near its byte limit.
- HTTP: two simultaneous retries create one issue; two edits at one version
  produce one success and one conflict. Creates do not overwrite subsequent
  edits. Foreign authors, token-scope limits, repository UUID mismatches and
  comment-parent mismatches are checked.
- HTTP pagination over 33 issues and 17 comments with maximum-sized bodies,
  state filters, terminal empty pages, malformed/oversized input and repository
  isolation. A paused issue upload and paused comment upload both fail after
  ACL revocation. Rename and fresh-local-storage owner restore retain exact
  issue/comment content and versions; writes continue after restoration.
- Existing two-repository Git/LFS recovery, Repository Cell integration, all-target
  Clippy with warnings denied, formatting and the release binary build pass.

The release process smoke against RustFS `1.0.0-beta.8-glibc`, with
`--sqlite-chunks --many-objects 256`, passes. It creates an issue/comment, edits
both and closes the issue, then checks exact content, identities, timestamps and
versions before shutdown, after clean restart and after SIGKILL/lease expiry on
fresh local storage. Retrying the original creates at each stage preserves those
edits. The same run verifies Git/LFS, ACL revocation, token rotation, 300 refs,
SQLite chunks, mixed/atomic outcomes and lost-push-reply replay. The process exits
successfully; fresh-volume RustFS startup diagnostics remain as previously noted.

This adds approximately 1,000 Rust source lines for the models, four transactional
mutations, three bounded reads and seven HTTP operations; their common admission,
result decoding and text validation are shared within the issue boundary.
No dependencies or lockfiles changed. This extends the unreleased Repository
schema and module source digest; old development prefixes require replacement.
The full collaboration gate remains open: no labels, assignees, edit history,
attachments, delete/moderation API, notifications, search or UI yet. Aggregate
issue/comment quotas and retention policy also remain open.


## Configured commit checks

Commit checks now have owner-configured contexts and an explicit reporter
account. Run creation binds a UUID to commit/context/policy version/reporter;
updates compare a run version and allow only active attempts to progress. Every
write rechecks Cell membership and authority. Terminal results remain immutable.
Reruns get new identities and ordered creation numbers, so delayed callbacks and
old start retries cannot replace the newest attempt. Changing context policy
invalidates older-version results; disabled context names stay reserved.

The check SDK owns three bounded reads and three transactional mutations. Six
HTTP operations expose policy configuration, current commit results, run start,
run read and updates. Context/commit pages contain at most 32 entries; summaries
are at most 4 KiB. Name, enabled/name and commit/context/version/creation indexes
keep current-state reads independent of disabled-context and attempt histories.
No new dependencies, external data store or process runner was introduced.

The HTTP test pushes real commits and verifies configured-reporter authority,
read-token restrictions, concurrent exact starts, conflicting UUID bindings,
late successful callbacks behind a newer attempt, concurrent terminal outcomes,
reporter changes, enablement changes during a paused upload, membership
revocation, history reads, non-commit rejection, 33-context pagination, rename
and fresh-local-storage recovery. New starts after recovery become current while
old start retries preserve the saved result. Direct SDK tests prove the same
owner/reporter boundary without HTTP, exact runtime receipt replay, terminal
immutability, maximum summary size and revocation. Repository Cell integration,
all-target Clippy and the release build pass.

The release process smoke against RustFS `1.0.0-beta.8-glibc`, with
`--sqlite-chunks --many-objects 256`, also passes. It completes a newer attempt
with failure, then an older attempt with success, and verifies that the newer
failure remains selected before shutdown, after clean restart and after
SIGKILL/lease expiry with fresh local storage. Replaying the older start preserves
that selection and both original result records. The same run retains issue
edits, collaborator/token behavior, Git/LFS, SQLite chunks, 300 refs, mixed/atomic
outcomes and lost-reply replay. The process exits successfully. Initialization
diagnostics from the fresh RustFS volume are unchanged.

The new Rust surface is about 900 lines for check models, three Cell writes,
three bounded reads and six HTTP operations. Common admission, validation and
outcome decoding stay within the check boundary. The process probes share their
JSON request helper rather than duplicating it for checks and issues.

Branch protection implementation checklist (delivered in the following section):

1. Add owner-managed, versioned rules for exact branch refs, including deletion,
   force-update policy and required context identities. Keep administrative
   bypasses explicit and default to applying policy to every writer.
2. Produce native Git per-ref rejections before receive-pack reports accepted
   updates. Git's [update hook](https://git-scm.com/docs/githooks#_update) can reject
   one ref; ordinary pushes must retain allowed siblings, while `--atomic` must
   reject the whole requested group. Use bounded request/ref inputs and retain
   the existing encoded-input and subprocess resource limits.
3. Enforce current rules and the newest matching check result in `refs::apply_refs`,
   shared by `FinalizePush` and `CompletePush`. A hook snapshot alone is insufficient:
   context/rule changes or a new queued run during the push must not bypass policy.
   Force-update decisions need ancestry evidence verified from immutable Git
   data at the Cell boundary, rather than a caller assertion.
4. Verify absent/pending/failed/old-version checks, reruns during publication,
   protected deletion/force pushes, ordinary mixed outcomes, atomic outcomes,
   owner recovery and exact lost-reply replay. A completed replay must not apply
   refs again under newly changed policy.

This schema extension requires a fresh development prefix. Check reporting is
functional but does not run CI jobs. Branch protection is covered below. History listing,
check logs/artifacts, notifications, retention, quotas, PR integration and UI
remain open, along with the broader delivery gates.


## Branch protection

Exact-branch policy now connects configured checks to authoritative publication.
Owners replace versioned rules; enabled rules can require up to 16 check contexts,
reject deletion and require fast-forward history. No writer bypass exists.
HTTP configuration uses the repository UUID plus expected rule version. Listings
are bounded and include disabled policies; disabled names retain their versions.

The native Git update hook supplies per-ref rejections and preserves ordinary
mixed versus atomic push semantics. Both typed and HTTP publication use the same
Cell policy check before any ref writes. Changes to rules, contexts or newest
attempts are interpreted at publication. Verified commit-parent links and bounded
ancestry certificate commands support constant indexed ancestry decisions in
that transaction. The SDK prepares multi-page and merge ancestry ahead of it.

Evidence collected:

- Stock Git E2E: absent, queued, failed and stale-version checks; a late old
  success behind a newer attempt; successful promotion; ordinary mixed and atomic
  rejection; owner force/delete rejection; safe shell quoting of valid unusual
  refs; owner-only configuration, rule CAS and input validation; rename and
  recovery on fresh local storage; disable/re-enable and subsequent promotion.
- Direct Cell: independently encoded forged ancestry claims reject and roll back
  earlier valid steps; low-level publication cannot bypass missing proofs; a
  132-edge merged path spans proof pages; current queued attempts and disabled
  contexts reject captured push intent. Completed command replay preserves refs
  after stricter rules. Rule reads page past 32 entries and preserve tombstones.
- Existing Repository Cell and smart-HTTP integration pass, including malformed
  pack reports spanning response chunks, encoded input, replay, Git/LFS and
  cache admission. The two-repository HTTP/Git/LFS recovery regression passes.

All-target Clippy with warnings denied, formatting and the release binary build
pass. The release process smoke against RustFS `1.0.0-beta.8-glibc`, with
`--sqlite-chunks --many-objects 256`, exits successfully. It verifies protected
promotion and force/delete rejection before restart, after clean restart and
after SIGKILL/lease expiry on fresh local storage. An old successful push replays
after deletion and a new required-check rule without recreating the branch.
Issue/check state, Git/LFS, token/ACL recovery, SQLite chunks, 300 refs and mixed
versus atomic outcomes remain covered in the same run. Fresh-volume RustFS
initialization diagnostics are unchanged. The fixture asserts the exact three
ref-generation increments introduced by the branch probe before checking recovery.

Approximately 1,030 new Rust source/schema lines cover the versioned policy API,
verified ancestry preparation/commands, safe native-hook generation and the
shared final publication gate. The proof tables keep graph traversal outside
the ref transaction; the hook is only a native reporting boundary. There is one
policy query shared by preflight and authoritative publication.

Remaining work: final-publication fault injection, bounded total ancestry search,
certificate retention, native Git scratch peaks and cross-OS hook proof. PR
approval policy and PR/review/merge APIs are the next collaboration slice. Full
hosting delivery gates above remain open. No dependency or lockfile changes;
new schema tables and operation registrations require a fresh development prefix.


## Pull request and review lifecycle

Repository Cells now store proposals and immutable reviews alongside refs and ACL.
Six HTTP operations open/list/read/edit pulls and submit/list reviews. Source/base
names remain fixed; creation pins expected live tips, while reads join current
ref state rather than copying it into every pull after a push. Closing, reopening
and draft changes advance an editorial version. Exact create retries preserve
later edits and original numbers.

Reviews bind to exact source/base OIDs, retained ref versions and pull version.
Only other repository writers can approve or request changes on ready pulls;
members can comment. Newest decisions per reviewer supersede older decisions,
while comments and old retries leave that order unchanged. Eligibility reflects
current state. Branch/editorial ABA cannot revive a review. Membership generations
advance with grant changes and survive removal, preventing revoke/regrant from
reviving an approval. Repeated identical grants keep their generation.

Verified locally:

- HTTP with real Git source/base commits: concurrent exact create/review retries,
  conflicting identities, expected versions, author and token boundaries, old
  retry ordering, source/base movement, ABA, draft/close/reopen, deletion and
  recreation, role downgrade/regrant, same-role retry, and a paused review upload
  rejected after revocation. Lists page through 33 pulls and maximum-size review
  bodies. Rename and fresh-local-storage recovery preserve edited pulls and
  review history; new decisions after recovery retain correct ordering.
- Direct Cell: SDK authority without HTTP, exact receipt replay, mismatched parent
  UUID binding, stale ref versions, maximum text sizes, unauthorized ACL changes,
  revoke/regrant, immutable owner review authority and retry preservation after
  edits. Existing graph/ref/check/issue integration still passes.
- Existing collaborator roster and two-repository HTTP/Git/LFS recovery pass;
  Clippy with warnings denied passes.

The release binary build and S3-compatible process smoke against RustFS
`1.0.0-beta.8-glibc`, with `--sqlite-chunks --many-objects 256`, pass. The probe
records an edited/reopened pull and reviews, then checks exact snapshots and old
create/review retries through clean restart and SIGKILL/lease takeover with fresh
local storage. Later edits and newest review applicability remain unchanged.
The same run retains branch/check policy, issues, Git/LFS, token/ACL recovery,
SQLite chunks, 300 refs, mixed/atomic outcomes and lost-push-reply replay. The
process exits successfully; fresh-volume initialization diagnostics are unchanged.

At the proposal/review milestone, the remaining publication work was:

1. Add bounded comparison/file APIs against explicit source/base snapshots.
2. Add versioned review requirements to branch policy, with a single eligibility
   decision reused by read views and authoritative merge publication.
3. Prepare real merge candidates with native Git; persist candidate objects and
   surface conflicts before asking clients to merge. Preserve original ref/pull
   preconditions and support the selected merge strategy explicitly.
4. Apply current ACL, branch checks, reviews and ref CAS with the merged pull
   state in one Cell command. Replays return the original merge result without
   moving refs again. Direct pushes must honor any required-PR rule too.
5. Prove competing merges, new pushes/reviews during preparation, owner failure,
   lost replies, and stock-Git clone of the published merge result after recovery.

New schema tables require a fresh development prefix. No dependencies changed.
Approximately 1,000 Rust lines implement the model/read boundary, three guarded
mutations and six HTTP operations, with shared validation/result handling inside
the pull module. Full delivery gates and production capacity remain open.


## Pull comparison and file preview

Repository HTTP now reads exact-revision changes from Cell Git objects. The
comparison selects a unique best common ancestor and walks its tree against the
source tree, skipping equal subtrees. No native cache hydration is required.
Changed leaves include additions, removals, edits, executable changes, symlinks,
Gitlinks and file/directory replacements. Paths remain raw bytes with canonical
base64 transport; optional display paths are UTF-8. Renames are separate removal
and addition records. File previews return at most 256 KiB; external large blobs
and Gitlinks expose metadata without fetching their content.

Membership and editorial/ref versions are checked before and after each read.
The endpoint uses the existing node transfer admission and response lifetime
accounting. Traversal, output, depth, path memory and elapsed work have explicit
limits documented in `contracts.md`. Changed-file pagination currently recomputes
the bounded comparison; caching and large-history performance remain open.

Verified locally:

- Real Git pushes and HTTP reads: native `merge-base --all` and full raw
  `diff-tree -r -z --no-renames` modes/OIDs/path comparison, including diverged
  base-only changes, multiple pages, binary bytes, non-UTF-8 names created with
  index plumbing, symlinks, executable files, Gitlinks and file/directory swaps.
- Exact before/after previews, large external blob metadata, invalid paths and
  cursors, repository identity binding, read-scoped token access, and membership
  revocation while a request body is paused. Source movement and ABA reject old
  views; repository rename and fresh-local-storage recovery preserve snapshots.
- A 600-parent merge proves SQL parent-edge pagination. Criss-cross and unrelated
  histories agree with native Git and produce explicit conflict responses.
- A 10,001-file change set returns 413 without a partial page, while an individual
  file remains readable. Oversized HTTP bodies fail. Eight outstanding LFS
  uploads block comparison admission with 503/Retry-After; other APIs stay live.
- Unit graph/tree tests, Clippy over all targets with warnings denied, and the
  release binary build pass. The Python recovery probe parses successfully.

The release S3-compatible process probe passes against RustFS
`1.0.0-beta.8-glibc` with `--sqlite-chunks --many-objects 256`. Exact comparison
and preview snapshots survive clean restart and SIGKILL/lease takeover with
fresh local storage. Existing pull/review retries, branch/check rules, issues,
Git/LFS, token/ACL recovery, 300 refs, SQLite chunks and dropped-push-reply replay
also pass. The runner exits successfully; fresh-volume initialization diagnostics
are unchanged. Hosted CI and target production-store qualification remain open.

Approximately 875 Rust implementation lines own bounded graph/tree traversal,
file previews and the HTTP admission/lifetime boundary. Existing Git publication
and storage commands remain the canonical write path. No dependency, lockfile or
schema changes. Full delivery gates remain open. The following milestone adds review policy
and atomic fast-forward publication; synthesized candidates, patches, inline
discussions and UI remain.


## Reviewed fast-forward merges

An explicit fast-forward strategy now closes the path from PR review to durable
Git side effect. One command validates the exact proposal/ref revision, current
writer authority, current review requirements and ancestry, then uses the shared
ref publisher for graph/CAS/namespace/check policy. Base movement, merged state
and a retry record commit together. Exact HTTP request retries recover the
original result without moving refs again, including after branch deletion and
repository rename. Merged pulls are terminal and retain their result details.

Versioned exact-branch rules add mandatory `require_pull_request` and
`required_approvals` fields. Required-PR rules reject every direct publication,
including owner pushes, deletion and recreation. The merge command alone creates
an internal capability for one validated base update. The canonical ref publisher
still enforces every other check. Counts use one latest decision per reviewer,
updated with the review transaction; history flags and publication share the
same eligibility predicate. This avoids history scans for approval counts.
Ancestry preparation now caps discovered commits and parent edges.

Verified locally:

- HTTP/native Git: two-reviewer policy, newest decision and old retry ordering,
  comment preservation, revoke/regrant, objections arriving during paused merge
  upload, required check results, insufficient scopes, source movement/ABA,
  editorial/draft changes, unrelated histories and writer revocation mid-upload.
- Required-PR owner pushes/deletions fail. Ordinary mixed pushes preserve the
  allowed sibling; atomic pushes reject the group. Even approved proposals do
  not make direct Git pushes eligible. Existing branch-policy recovery tests pass.
- Concurrent exact requests return one durable result. Competing pulls publish
  exactly one winner against a base version. Changed intent/actor and fresh IDs
  after completion conflict. Terminal pulls cannot reopen. Source deletion,
  rename, fresh local storage and stock Git clone/fsck retain the merged commit.
- Direct Cell: raw `FinalizePush` cannot bypass required PRs; unauthorized merge
  attempts and current review checks apply without HTTP. Exact runtime rejection
  replay remains rejected after later approval. A constraint failure injected
  after ref/pull writes rolls back ref, ref generation and pull state. A fresh
  merge succeeds after removing the injected fault. Runtime receipt and
  application request replay preserve the result under stronger later policy.
- Shared Git/LFS/comparison/merge admission and existing PR/review lifecycle pass.
  Clippy with warnings denied and the release build pass.

The release S3-compatible process probe passes against RustFS
`1.0.0-beta.8-glibc` with `--sqlite-chunks --many-objects 256`. It sends a reviewed
merge on a dedicated branch, observes publication through a second connection,
and discards the original reply. Exact replay, merge/pull snapshots and stock-Git
clone/fsck match after clean restart and SIGKILL/lease takeover with fresh local
storage. Existing comparisons, reviews, checks, issues, Git/LFS, token/ACL recovery,
SQLite chunks, 300 refs and dropped-push-reply replay also pass. The runner exits
successfully; fresh-volume initialization diagnostics are unchanged.

Approximately 700 new/changed Rust implementation lines cover review requirements,
the indexed decision heads, merge command/result contract and HTTP endpoints.
Ownership stays in the Repository Cell; Git publication has one canonical path.
Schema version 1 is unreleased: new merge/head tables, rule fields and operation
8 codec 2 / operation 9 require a fresh development storage prefix. No dependencies
or lockfiles changed.

Remaining collaboration work keeps the full hosting scope intact:

1. Native Git preparation of merge commits, squash and rebase, explicit conflicts,
   candidate object retention and checks bound to the selected candidate.
2. Reuse authoritative publication for those strategies with validated candidate
   identity, original revision preconditions and exact replay.
3. Fault injection across every preparation/publication ownership boundary,
   quota/retention policies and measured history/concurrency capacity.
4. Text patches, line discussions, releases/assets and repository UI.

Other delivery gates above, including routing, backup/GC, production limits and
public-service functionality, remain open. Fast-forward success does not satisfy
the synthesized-merge or full hosting gates.


## Native merge and squash preparation

Canopy now prepares durable native Git candidates for merge commits and squash.
Operation 10 owns reservation and completion. One application UUID fixes the
creator, original pull/ref versions, strategy, message and first timestamp.
Generated objects enter the existing verified storage path. Canonical commit
bytes and graph closure gate the ready result. Ready metadata and an immutable
`refs/canopy/merge-candidates/<UUID>` ref commit together, enabling stock Git/CI
fetches before publication. The reserved namespace is enforced in the Cell
publisher and native push reports, including owner and mixed/atomic pushes.

Operation 9 codec 2 publishes these candidates through the existing reviewed
merge transaction. It rechecks the original revision, current reviews, candidate
identity/strategy, ancestry and required checks on the exact candidate OID.
Checks on the source alone are insufficient. Base/pull/retry updates remain
atomic. Fast-forward uses the same path without a candidate. There is no
implicit strategy fallback. Conflicted and unrelated results never publish.

Verified locally:

- Native merge/squash: divergent edits, exact Git merge-tree equivalence, parent
  ordering, renamed files, binary content and a generated large blob using the
  external storage path. Stock Git fetches candidates before publication.
- Prepare before approvals/checks, then enforce each at publication. Current
  token/ACL checks reject unauthorized and read-only preparation; revocation
  during a paused request rejects preparation. Candidate reads allow members.
- Exact/concurrent preparation retries retain one result. Changed actor/intent
  conflicts. Candidate strategy/pull mismatches, source movement and ABA reject
  publication. Readability and retryability do not imply current eligibility.
- Conflict paths preserve raw bytes, including non-UTF-8 and newlines. Unrelated
  histories produce an explicit result. Criss-cross histories with two best
  merge bases succeed through native consolidation.
- Fresh-local-storage recovery retains candidate/ref/check/review state; later
  publication, exact merge replay, stock clone and fsck all pass.
- Raw typed ref publication rejects the reserved namespace. Existing direct-Cell
  atomicity, fast-forward HTTP merges, branch policies, native hook parsing and
  shared transfer admission pass. Clippy with warnings denied and release build
  pass. The Python process recovery probe parses.

The release-binary process probe also passes against RustFS
`1.0.0-beta.8-glibc` with `--sqlite-chunks --many-objects 256`, on Darwin arm64
with Apple Git 2.50.1. It prepares both native strategies, restarts with fresh
local storage, fetches their immutable refs with stock Git, and publishes using
restored approvals and candidate-specific checks. After SIGKILL, lease takeover
and another fresh local directory, it fetches the same objects and replays the
same merge results. Existing dropped-reply fast-forward publication, comparisons,
reviews, checks/rules, issues, Git/LFS, token/ACL recovery, SQLite chunks and 300
refs also pass. The runner exits successfully; the provider's fresh-volume
initialization diagnostics are unchanged.

The new Rust surface is approximately 1,000 implementation lines across native
Git execution, bounded domain records/commands and HTTP ownership/admission.
The existing object ingestion and final branch publisher remain canonical.
No dependency or lockfile changed. The unreleased schema requires a fresh prefix.

Completion audit: this advances collaboration gate 7; it does not close the full
hosting goal. Rebase, conflict resolution, text patches, line discussions,
releases/assets and repository UI remain. Operations gates still include native
peak resource limits, preparation/publication owner-loss fault injection,
retention/quotas, backup/GC, multi-node routing, audit/observability and measured
capacity. Public visibility, organizations, search, webhooks and account
lifecycle remain. Hosted CI needs a Canopy remote. All gates above remain open
until their full acceptance proof is recorded.


## Repository browser and embedded interface

Canopy now serves an embedded repository interface at `/`. Its bearer-authenticated
API reads verified SQLite objects directly, with no bare-cache hydration. Default
HEAD resolves atomically to a ref tip; immutable commit/tag roots pin tree, file
and first-parent history pages through later branch movement. Raw-byte paths and
commit metadata preserve non-UTF-8 data. The shared Git reader now owns both PR
comparisons and browsing; comparison limits and behavior stay unchanged.

The UI connects with a memory-only token, lists/creates repositories, selects
branches/tags, navigates directories, displays literal file content and follows
history. Merge commits expose all parents. Empty/loading/error states reach the
real API. Logout aborts requests, clears content and invalidates old-session
replies. CSP, no-store and text-only rendering protect repository-supplied bytes.
No frontend framework, runtime dependency, lockfile or schema change was added.

Verified on Darwin arm64, Apple Git 2.50.1:

- Four focused Git-reader unit tests: native permission normalization, raw names,
  merge-base selection, and commit header/graph boundaries.
- Browser integration: 44-entry paginated trees, 32-entry history pages matching
  stock `git log --first-parent`, ordered merge parents, nested files, binary and
  non-UTF-8 bytes, executable/symlink/Gitlink modes, annotated tags, large-file
  metadata, stale ref cursors, grant revocation during paused uploads, invalid
  paths, response headers, and identical snapshots after fresh-local recovery.
- Three existing comparison integrations: changed-file/file-byte equivalence,
  wide and criss-cross histories, ambiguity/unrelated behavior, and oversized
  change-set rejection. Shared transfer overload integration also passes.
- Clippy with warnings denied, Rust formatting, JavaScript syntax, Python probe
  parsing, and the release binary build pass.
- Release process probe against RustFS `1.0.0-beta.8-glibc` with
  `--sqlite-chunks --many-objects 256`: pinned tree/file/history snapshots and
  static UI assets match after clean restart, fresh local storage, SIGKILL and
  lease takeover. Existing Git/LFS, replay, checks/rules, issue/pull/review,
  merge/squash candidates, token/ACL, chunked objects and 300-ref proof passes.
  The runner exits successfully; provider fresh-volume diagnostics are unchanged.
- Live Chrome with that release binary: rejected-token feedback, successful
  login, API-backed repository creation, branch/tag selection, nested file
  navigation, human-readable history, keyboard skip, explicit logout, and no
  retained login after reload. Inspected desktop and 390×844 mobile views; mobile
  history and file pages have no document-level horizontal overflow. Hostile HTML
  is literal text with zero script/image nodes. No page console errors observed.

Saved-download verification remains a gap: the browser download event timed out,
then browser security policy denied access to Chrome's downloads page. The UI
provides an octet-stream blob link; API tests prove its source bytes, but a saved
file was not confirmed in this environment. Other browsers, complete accessibility
and resource/capacity matrices still need qualification.

The added implementation is approximately 590 Rust lines for bounded browse
records/queries, HTTP admission and static assets, plus the dependency-free web
interface. Existing graph/tree/object mechanics moved into one shared reader;
they were not duplicated. This growth delivers the first usable repository UI.

Completion audit: verified progress on gate 7; the full hosting goal remains
active. Collaboration editors, text patches/line discussions, rebase/conflict
resolution, releases/assets, public visibility, organizations, search and
webhooks remain. Operations work still includes native peak resource bounds,
fault injection, retention/quotas, backup/GC, multi-node routing, audit/metrics
and production capacity. Hosted CI still needs a Canopy remote.


## Issue collaboration interface qualification

The embedded interface now supports issue creation, open/closed/all lists,
32-issue pages, 16-comment pages, issue/comment edits and close/reopen. New posts
use the existing immutable creation UUID contract; edits use expected versions.
The repository metadata response supplies the current viewer account and token
scope for appropriate controls. API/Cell checks remain authoritative.

Verified on Darwin arm64 with Chrome and the real Canopy server backed by RustFS
`1.0.0-beta.8-glibc`:

- Seeded 35 issues and 18 comments. The UI showed 32 then 3 issues, and 16 then
  2 comments. Filter navigation and empty closed-state rendering work.
- Edited an issue title, closed it, edited an existing comment and posted a new
  comment beyond page one. Each appeared through fresh API reads. The new comment
  was immediately visible at its returned position.
- Published a competing API edit while the form held an older version. Saving
  returned 409, preserved the local draft read-only, and offered a reload. The
  newer issue remained unchanged and became visible on reload.
- Restarted the server with a fresh local directory and the same durable store.
  UI-created issue/comment changes remained visible. API checks retained original
  body/identity fields and exact paged comments; the UI subsequently reopened
  the recovered issue with the next version.
- A test reverse proxy forwarded an issue create, then deliberately discarded
  the successful reply. The UI froze the original payload for retry. Revoked the
  author's grant, retried to 404, restored it and retried again. The original
  issue #36 was returned; an independent API list contained exactly one new issue.
- A repository reader with write scope could create and edit authored discussion
  records but could not edit another author's comment. A read-scoped token had
  no New issue, Edit issue or Add comment control.
- A 300-byte Unicode title triggered the 256-byte validation before submission.
  Repository-supplied script/image markup remained literal text with no generated
  script/image elements. Session clearing and existing navigation are reused.
- Desktop and 390×844 mobile issue/detail views inspected. Mobile details have no
  document-level horizontal overflow. No page console errors observed.

Focused Rust issue integration passes, including new viewer account/scope
assertions, durable versions, ACLs and recovery. The repository discovery/ACL
pagination integration also passes; its expected residency/movement retries
completed. Clippy with warnings denied, Rust formatting, JavaScript syntax,
Python probe parsing and the release binary build pass. Recovered issue assets
return the correct content types, no-store, nosniff and the existing CSP.
The checked-in process probe now includes both issue assets; the live UI recovery
qualification above was run separately from the full process fault suite.

Implementation adds a small issue UI module and stylesheet, metadata for the
viewer, and static asset routes. Existing issue mutations are unchanged. No
schema, dependency or lockfile change. Saved-download verification from the
previous browser milestone remains a gap. Drafts and uncertain submission IDs
are page-local; leaving the page discards them, as stated by the editor.

Completion audit: verified progress on collaboration gate 7. Pull/review/check
interfaces, patches/line discussions, rebase/conflict resolution, releases/assets,
account lifecycle, public-service features and all outstanding operations and
capacity gates remain open. The full hosting goal remains active.

## Pull-request review and merge interface milestone — 2026-09-26

Classification: verified progress on collaboration gate 7. The embedded browser
now creates and edits pull requests, handles drafts, shows revision-bound review
history and side-by-side changed files, displays check results, and publishes
fast-forward or prepared native merge/squash results. Preparation and publication
remain separate. The existing Cell operations own every durable side effect.

The shared discussion module centralizes issue/pull authorization observations,
UTF-8 validation, paging and submission recovery. UUID intents freeze their
payload after uncertain replies; versioned edits require reload after conflict.
Candidate GET/POST now return the resolved repository UUID so the browser can
reject a mismatched Cell before combining reads. No schema, command codec,
dependency or lockfile changes.

Live qualification used Chrome, stock Git and the real server with RustFS
`1.0.0-beta.8-glibc`, on Darwin arm64:

- Approved a non-author request with an empty approval body, inspected the exact
  before/after source file, and fast-forwarded its protected base through the UI.
  A proxy discarded the successful merge reply. Retry recovered the same merge;
  the API and stock Git agreed on the published source OID.
- Approved a divergent request and prepared a native two-parent merge candidate.
  Publication returned a visible 409 while its required check was missing.
  Reported success for the candidate OID through the check API, refreshed the UI,
  observed that result and published the candidate.
- Prepared a conflicting request. The UI showed `src/main.rs` and exposed neither
  candidate publication nor an implicit fast-forward action.
- Created a draft request through the UI, edited it to ready, and prepared squash.
  An independent API edit advanced its editorial version. The stale publication
  failed with 409; reload identified the older candidate and removed both merge
  controls. A fresh squash candidate published successfully.
- Stock Git fetched the published branches: the merge had two parents; squash
  had one; both result trees matched. The fast-forward ref matched its source.
  `git fsck --full` passed on the fetched object database.
- A pull author could comment but had no self-approval/change-request option.
  A read-scoped member had no edit, review, prepare or publish control. Literal
  script/image input produced no injected DOM nodes. Desktop and 390×844 mobile
  views were inspected; the mobile document width remained 390 pixels.
- Exercised the shared issue form after extraction: discarded a successful
  create reply, retried to the same issue, edited its title and added a comment.
  An independent list contained exactly the original seed and one new issue.
- Restarted with a fresh local directory and the same durable store. Eleven API
  snapshots covering pulls, reviews, candidates, checks, issue and comment records
  matched exactly. The browser opened the recovered merged result as a read-only
  member. No page console errors were observed.

Focused candidate integration passes all three tests, including candidate UUID
identity, fetchability, native graph/path semantics, policy enforcement, recovery,
conflicts and stale publication. JavaScript syntax, Python probe parsing and
Clippy with warnings denied pass. Static-asset process qualification now includes
the discussion/pull scripts and pull stylesheet. Issue and pull lists share a
larger wrapped-title hit target for mobile navigation; conflict paths preserve
non-UTF-8 byte identities instead of substituting replacement characters.
After rebuilding, a wrapped mobile title opened by click and the detail still
had no horizontal overflow. All seven served scripts/styles matched source
bytes after another fresh-directory restart, with no-store and nosniff headers.

The release build and full process probe also pass with `--sqlite-chunks
--many-objects 256`. Clean restart, fresh-disk recovery and SIGKILL/lease takeover
retain collaboration records, candidate/check state, token rotation/revocation,
ACL roster, default branch and all eight embedded assets. Git/LFS and dropped
push/merge replies recover correctly. The 256-file incremental push took 3.52s;
the post-takeover clone restored two commits, an annotated tag and 300 extra refs
in 6.33s. Large tree/commit/tag chunks restored exact bytes/OIDs in 7.32s. These
are local qualification measurements, not production capacity claims. RustFS
logged missing internal metadata while formatting its new test store; all
qualification assertions passed and the process exited successfully.

Implementation growth is the new review/comparison/merge interface and its
stylesheet. The shared form extraction removes duplicate retry policy; existing
Git, review and merge mechanics remain canonical. Drafts and retry identities
remain page-local. Pending-candidate resume is wired to the existing idempotent
API; this milestone's live browser fixture exercised ready/conflicted/stale
candidates, not a pending preparation interrupted mid-flight.

Completion audit: this is not completion of the full hosting goal. Unified text
patches, inline discussions, historical comparisons, rebase/conflict resolution,
check/policy administration UI, releases/assets, public visibility, organizations,
search, webhooks and account lifecycle remain. Native resource bounds, fault
coverage, backup/GC, multi-node routing, observability, hosted CI and production
capacity gates remain open. The earlier saved-download verification gap remains.


## Historical pull comparisons milestone — 2026-09-26

Classification: verified progress. Merged requests now retain the exact
pre-publication pull/source/base revision in the same transaction as ref movement
and the merge result. The browser defaults merged Changed files to that snapshot.
Every review links to its immutable reviewed revision, so later source pushes,
editorial edits and branch deletion do not erase the inspected changes.

Comparison requests now use one tagged target: current with an exact expected
revision, review with a review number belonging to this pull, or merged. Current
views still reject ref/version movement. Historical views resolve stored roots;
no arbitrary caller-supplied historical OIDs are accepted. All targets enforce
current repository access before and after bounded traversal. Existing Git object
verification, path handling, pagination, admission and resource budgets are shared.

The merge record includes its saved revision, with source/base object foreign
keys for future retention. Operation 9 uses codec 3. Schema 1 remains unreleased;
this change requires a fresh development storage prefix. The former comparison
request shape is replaced in all repository clients, tests and process probes.
No compatibility reader, dependency or lockfile change was added.

Evidence:

- Four focused comparison tests pass. The new real-server/stock-Git scenario
  records a review, moves the source, rejects the stale current comparison,
  merges the newer revision, and deletes both refs. Reviewed and merged file
  bytes remain distinct and exact. Wrong-pull review IDs and missing merge
  records return 404; invalid/ambiguous selectors return 422.
- A read-scoped member can read snapshots. Revocation during a paused comparison
  body returns 404. A fresh local directory restores both comparison/list and
  file-preview results exactly; revoked access remains denied and merge retries
  retain their original revision and result.
- Both focused merge tests and all three native-candidate tests pass. They retain
  publication authority, competing-writer checks, review/check policy, native Git
  graph/path semantics, object fetchability and durable replay.
- A selector test exposed serde's internally tagged unit variant accepting extra
  fields despite the enum's unknown-field policy. The merged selector uses an
  empty struct variant; the strict rejection test now passes. The upstream
  derive implementation confirms the different deserialization paths.
- Chrome with a real RustFS-backed server: opened a merged request after both
  branches were deleted. Changed files showed the later merged source; the first
  review link showed the earlier source. Labels state the selected revision and
  distinguish live branches. Reloading the historical deep link after a fresh-
  directory restart retained the earlier bytes; switching to merged changes
  retained the later bytes. The viewer had read-only access throughout.
- Desktop and 390×844 mobile views inspected; document width remained 390 pixels.
  No page console errors. JavaScript syntax, Rust formatting, probe parsing and
  Clippy with warnings denied pass.

The release binary build and full RustFS process probe pass with
`--sqlite-chunks --many-objects 256`. The probe now retains historical comparison
responses before publication and checks them afterward for fast-forward, native
merge and squash, including fresh-disk recovery and lease takeover. Existing
Git/LFS, token rotation/revocation, ACL, branch policy, chunks, 300 extra refs,
embedded assets and lost push/merge reply checks also pass. The post-takeover
256-file clone took 0.61s; exact large tree/commit/tag restoration took 0.91s.
These local measurements do not establish production capacity. RustFS logged
missing internal metadata while initializing its fresh test store; qualification
completed with all assertions passing and exit status zero.

Implementation growth stores the missing historical authority and exposes it
through the existing reader and browser. Merge result decoding is shared by
normal reads and exact retries; the history regression sits beside the existing
comparison suite. No separate Git storage or diff implementation was introduced.

Completion audit: the full hosting goal remains active. This retains reviewed
and merged snapshots, not every push as an independent historical revision.
Unified patches, inline discussions, rebase/conflict resolution, administration,
releases/assets, public-service features, account lifecycle, native resource
bounds, backup/GC, routing, observability, hosted CI and production capacity gates
remain open. The saved-download verification gap also remains.

## Unified pull request patches milestone — 2026-09-26

Implemented a `patch` query on the existing comparison endpoint and made unified
hunks the default expanded-file view. Current, reviewed and merged targets retain
their existing authority, path, membership, graph and object verification rules.
Patch reads compute from verified Cell objects; they do not hydrate a native Git
cache. No schema, runtime codec, dependency or lockfile change is required.

The bounded line algorithm emits three context lines, exact old/new coordinates,
UTF-8 text with CR preserved and missing-final-newline flags. Added/deleted files,
empty files, executable modes, symlinks, raw-byte paths and file/directory changes
retain their entry metadata. Binary, oversized and Gitlink states have explicit
responses. Work and output exhaustion fail the whole request with 413. The
[contract](contracts.md#unified-text-patches) records the 256 KiB per-side limit,
20,000 lines, bounded frontier/comparison work, 8192 output lines and conservative
2 MiB response ceiling. A cancelled blocking worker retains transfer admission
until its bounded work ends.

The browser requests one patch per expanded file and renders text nodes, old/new
line numbers, addition/deletion prefixes, CR markers and missing-newline markers.
Immutable base/source file links reuse the existing browser. The old side-by-side
preview rendering and its CSS were removed. Full file bytes remain available
through the existing file API and browser. Inline discussions are still pending.

Proof:

- Five focused unit tests pass. The minimal edit count is checked against an
  independent dynamic-programming oracle for all 3969 pairs of binary-alphabet
  sequences up to five lines; hunks reconstruct the exact destination. Additional
  cases cover separated hunks/offsets, CRLF, Unicode, missing LF, large contiguous
  insert/delete runs and input/trace/comparison/output limits.
- Five real-server comparison integrations pass. Structured hunks converted to
  unified patches pass stock `git apply --check` and `git apply`, restoring exact
  bytes for separated edits, additions, deletions, CRLF, Unicode/HTML, newline-only
  changes and empty files. Existing changed-path fixtures also verify patch
  metadata/status for mode changes, symlinks, Gitlinks, raw-byte paths, binary and
  large files. A 20,001-line API request returns 413 without a partial result.
- Reviewed and merged patches differ after source movement and restore exactly
  after both refs are deleted and local state is replaced. Read-scoped membership
  works; revocation while a patch body is paused returns 404.
- Real Chrome/RustFS checks show merged and reviewed hunks, literal HTML (no script
  element), CRLF, missing LF, binary/large/empty states and visible 413 errors.
  The source file link opens the exact merged commit after both refs are deleted.
  At 390 px the document stays 390 px wide; long lines scroll inside focusable
  diff regions. After fresh-local-state restoration and reconnect, reviewed hunks
  remain correct and the browser reports no console errors. Temporary browser
  tabs, viewport override, server and storage fixture were cleaned up.
- Rust formatting, Clippy with warnings denied, JavaScript syntax and Python
  syntax pass. Debug and release binaries build successfully.

The release RustFS probe with `--sqlite-chunks --many-objects 256` passes all
assertions and exits zero. Exact patch responses survive clean restart,
fresh-disk recovery and SIGKILL/lease takeover. Existing reviews, issues,
fast-forward/merge/squash publication, dropped replies, Git/LFS, ACL, token
rotation/revocation, branch policy, embedded assets and chunk restoration remain
verified. The first probe attempt caught a fixture tuple-position error introduced
while adding patch snapshots; restoring the token's existing position fixed that
harness error, and the complete probe was rerun. Fresh RustFS startup logged
missing internal metadata; the successful run completed all assertions.

The post-takeover 256-file clone took 0.51s and exact large tree/commit/tag
restoration took 0.74s. These are local observations, not production capacity
claims. Patch limits intentionally reject high-edit-distance or oversized work;
no persistent diff cache or large-history performance target is delivered here.

Implementation growth is the bounded diff engine, its behavior tests and the
unified rendering path. Existing authorization, immutable-object reads, full-file
browsing and HTTP admission remain canonical. No second storage or Git protocol
path was added.

Completion audit: the full hosting goal remains active. Inline discussions,
rebase/conflict resolution, administration, releases/assets, public-service
features, account lifecycle, native resource bounds, backup/GC, routing,
observability, hosted CI and production capacity gates remain open. Saved reviews
and merges retain snapshots; independent per-push history and the earlier saved
file-download verification gap remain outside this milestone.

## Durable line discussions milestone — 2026-09-26

Implemented line discussions across repository Cell storage, HTTP and the pull
request interface. Select a base/source line number in a text diff to start one;
read paged discussions/replies, reply, resolve/reopen, and inspect the highlighted
original line after refs move or disappear. Discussion text is literal and
immutable. Editing/deletion/moderation and range anchors are not implemented.

Anchors come from the verified bounded patch reader, including context lines.
They retain the exact pull/ref revision, merge base, raw-byte path, side, line and
blob identity. Callers cannot choose authoritative OIDs. The final SQL transaction
rechecks current access and live revision eligibility or the immutable selected
review/merge record. All metadata and replies live in SQLite. Source/base/merge-base
and blob object foreign keys add explicit retention roots for future collection.

Thread/reply UUID bindings preserve exact retries under fresh runtime identities;
thread retry lookup precedes live-ref validation but follows current access.
Resolution uses expected versions and permits the thread author, pull author or
repository writer with write token scope. New replies do not silently reopen a
thread. Resolution does not change canonical review/check/merge requirements.
The [contract](contracts.md#line-discussions) specifies endpoints, permissions,
limits and status codes. The new tables change unreleased schema 1: use a fresh
development prefix. No dependency, lockfile or runtime operation codec changed.

Evidence:

- Six real-server comparison integrations pass. The new thread scenario verifies
  concurrent identical creation, invalid/out-of-hunk/binary/path anchors, read-only
  token rejection, exact source blob/revision binding, deleted-file base anchors,
  reply identity conflicts, author resolution, unauthorized resolution and stale
  versions. Threads and replies paginate across 17 records each.
- Old current-target creation retries survive source movement; a new UUID with
  that stale target conflicts. Saved-review and merged-target discussions can be
  created after both branches are deleted. Thread-target comparisons remain exact
  and cannot be used with another parent pull. Revocation during a paused reply
  body returns 404; revoked creators cannot replay creation. Fresh local state
  restores thread metadata, replies and original patch responses exactly.
- The shared-admission test includes discussion creation: eight paused LFS
  transfers yield 503/Retry-After while metadata/health requests remain available.
  Anchor workers and response frames retain their permit as in comparisons.
- The pinned Cellule `sql.rs` and `sql/api.rs` confirm transactional batch execution,
  rollback ownership, parameter-count checks, bounded results and typed command
  publication. Existing pull mutation result decoding is reused. Git mutation and
  review-policy ownership stay in their existing modules.
- Real Chrome/RustFS: a deliberately discarded successful creation reply displays
  Retry submission and preserves the original body/UUID. Retrying yields one
  discussion. Posting a reply, resolve/reopen and stale-update refresh work.
  The original line is highlighted; HTML stays text. At 390 px the document stays
  390 px wide. Fresh-local-state recovery retains the discussion, reply and resolved
  state; a read-only token sees the same anchor with no mutation forms and no
  console errors. State-only forms now give refresh guidance without asking the
  user to copy a nonexistent text draft. Temporary fixtures and viewport were
  cleaned up after verification.

The release RustFS process probe passes with `--sqlite-chunks --many-objects 256`.
It now seeds a line discussion, reply and resolution, then checks exact records,
creation/reply retries and anchored patch bytes after clean restart, disk loss and
SIGKILL/lease takeover. Existing Git/LFS, issue/review, native merge/squash,
fast-forward, lost replies, ACL/token changes, branch policy, embedded assets and
chunk restoration pass too. The run exits zero; fresh RustFS startup reports
missing internal metadata before qualification proceeds. Post-takeover clone took
0.51s; exact large tree/commit/tag restoration took 0.57s. These local observations
do not establish production throughput. Final copy/error-message changes were
verified with the focused integration, browser reload and rebuilt binaries.

Rust formatting, Clippy with warnings denied, JavaScript syntax and process-script
parsing pass. Debug/release binaries build. Growth is the new discussion domain,
transport, UI and behavior tests; existing verified Git reading, bounded SQL,
submission retry handling and pull result decoding remain canonical.

Completion audit: the full hosting goal remains active. Discussion editing and
moderation, rebase/conflict resolution, administration, releases/assets, public
service features, account lifecycle, native resource bounds, backup/GC, routing,
observability, hosted CI and production capacity gates remain open. The separate
saved-file-download proof gap and general per-push history also remain.


## Native Git environment isolation qualification

On 2026-09-26, all four native Git entry points moved to
`src/native_git.rs`: smart HTTP, post-push ref enumeration, incremental object
reads and merge preparation. The shared policy removes inherited configuration,
object paths, trace settings and provider credentials. Home and temporary paths
resolve inside the absolute disposable cache. This closes an accounting escape
prerequisite; aggregate native peak disk usage remains an open release gate.

Proof on Darwin arm64 with Apple Git 2.50.1 and RustFS 1.0.0-beta.8-glibc:

- The previous release binary fails its first push with HTTP 500 under the new
  hostile-host-environment process fixture. The rebuilt binary passes the same
  fixture through graceful restart, fresh local state and SIGKILL/lease takeover.
- A separate-process regression proves that host configuration is absent, a
  synthetic provider secret does not reach a Git shell helper, relative cache
  roots resolve correctly and an object write stays in the intended cache.
- The 37 Git-focused unit tests, smart HTTP advertisement integration,
  all-target Clippy, formatting and release build pass.
- `smoke_s3_process.py --sqlite-chunks --many-objects 256 --large-clone` passes:
  Git/LFS, merge/squash, policy hooks, exact reply retries and collaboration state
  survive recovery. Both v0 and v2 restore an 83,912,145-byte pack with matching
  file hashes and `git fsck`. Large SQLite tree/commit/tag bodies and 300 extra
  refs also restore. The host override directory contains only its input config;
  no redirected objects, traces or temporary files appear.

Local clone observations: 7.53 seconds cold v0, 3.36 seconds warm v2. Different
cache states; these are recovery observations, not a throughput target. The
production addition is one small policy shared by four callers, replacing the
candidate-only copy. No dependency, schema or wire-format change. Filesystem
allocation overhead, crash-left cleanup, native peak memory/disk and cross-OS
qualification remain open.


## Managed local runtime recovery qualification

On 2026-09-26, `src/server/workspace.rs` replaced anonymous node directories with
one marked `runtime-v1/` directory beneath a locked `data_dir`. The manager retains
that owner with detached request work. Git caches use a recognizable prefix and
per-cache worker locks. On Unix, native Git and its descendants inherit the lock
descriptor across exec. Startup acquires every abandoned worker fence before
reclaiming local state; cache destruction respects the same fence and suppresses
TempDir's implicit deletion retry when cleanup fails.

Proof:

- Five focused workspace tests cover a competing owner, preserving unrelated
  files, rejecting unknown markers and runtime symlinks, safe nested symlink
  removal, permission-failure recovery and an orphan Git shell descendant.
  Killing its Git parent does not allow cleanup; releasing the descendant does.
  Dropping the cache while that descendant is alive also leaves its files intact.
- A server API integration rejects a second node sharing the same data directory
  and restores the same repository UUID after graceful shutdown and reuse.
- Six residency/fault integrations remain green. Their filesystem probes now
  target the managed directory while retaining release/cleanup assertions.
- The 38 Git-focused unit tests and the separate host-environment regression
  pass, along with all-target Clippy, formatting and the release build.
- The real RustFS process probe passes with `--sqlite-chunks --many-objects 256
  --large-clone`. Its first restart uses fresh local storage; after killing the
  second process, the probe leaves scratch bytes and corrupts its local Directory
  SQLite file. The third process reuses that exact data directory. All old Git
  cache paths and the scratch sentinel disappear before restored API results
  become visible. Tokens/ACLs, issues, reviews, line discussions, branch rules,
  merge/squash candidates and exact retries retain their acknowledged state.
- Both v0 and v2 clones restore an 83,912,143-byte pack with matching file hashes
  and clean `git fsck`. Local observations: 6.48 seconds cold v0, 3.57 seconds warm
  v2; these are recovery observations with different cache states. Large SQLite
  tree/commit/tag objects and 300 additional refs also restore exactly.

Qualification host: Darwin arm64, Apple Git 2.50.1, RustFS 1.0.0-beta.8-glibc.
The added production surface owns a real lifecycle boundary: startup exclusion,
worker survival and cache reclamation. No dependency, public configuration or
Cell schema change. Windows orphan containment, startup cancellation during Cell
acquisition, OS power loss, Linux qualification and native peak disk/memory limits
remain open. Earlier development builds' anonymous directories remain untouched;
there is no adoption or compatibility reader for them.


## Cancellation-safe node lifecycle qualification

On 2026-09-26, `src/server/lifecycle.rs` introduced one supervisor that owns
initialization, HTTP serving, Cell drain and local workspace exclusion.
`CanopyServer` is now its control handle. Cancelling startup, dropping a ready
handle or cancelling the shutdown wait requests cleanup without aborting admitted
Cellule work. A failed node drain retains the workspace lock until process
restart, including startup rollback.

Dependency proof used the pinned Cellule revision `56b35ab`: CellNode retains a
runtime drain task, while CellRuntime shutdown sends an actor message and then
closes the SQL worker pool. Failure does not prove every worker was joined.
Tokio's JoinHandle detaches on drop, so the control channel, supervised ownership
and explicit drain are necessary; dropping the old server's listener task handle
alone did not stop the service.

Evidence:

- Three lifecycle integration tests use real Cellule and a paused ObjectStore.
  They cancel startup during Serving publication; drop a ready handle; cancel a
  shutdown waiting for Idle publication; and inject an Idle publication denial.
  The local lock remains held at each paused boundary. Successful cleanup permits
  immediate reuse of the same address and directory with durable repository UUIDs
  preserved. Failed drain retains exclusion.
- The same-directory workspace integration and all six residency/fault
  integrations pass. All-target Clippy, formatting and release build pass.
- The real RustFS process probe passes with `--sqlite-chunks --many-objects 256`:
  graceful restart, SIGKILL/lease takeover and corrupted-local-SQLite recovery
  preserve Git/LFS, collaboration, branch rules, merge/squash and exact retries.
  Large SQLite tree/commit/tag objects and 300 additional refs restore exactly.

This is a small ownership change plus fault-injection coverage. There is one
startup/drain path and no new dependency, configuration, schema or HTTP contract.
The Tokio runtime must remain alive until cleanup finishes. Runtime destruction,
panics, power loss, Windows containment, native peak resource limits and the
remaining service delivery gates are still open.

## Workspace exclusion after runtime destruction

On 2026-09-26, a regression test destroyed Tokio after server readiness and
reproduced premature release of the local workspace lock. A second case destroys
Tokio while the Directory Cell's Serving publication is paused. Both cases now
retain exclusion, and a new runtime in the same process receives `WouldBlock`
when it tries to start a server using that data directory.

The workspace owns the release decision. Before SQL Cell acquisition it records
that drain is required; only successful node shutdown clears that requirement.
Its synchronous destructor retains the lock descriptor until process exit if
closure is unconfirmed. This replaces separate startup/shutdown Arc-retention
branches and covers destruction of their async supervisor. It does not promise
graceful shutdown when the executor itself is destroyed.

Proof:

- The new test fails on the preceding implementation and passes with this guard.
- All four lifecycle integrations pass, including cancellation during startup
  and shutdown, ready-handle drop, and failed drain.
- Same-directory graceful restart and all six residency/fault integrations pass.
- All-target Clippy, formatting and the release build pass.
- The release binary passes the RustFS process probe with `--sqlite-chunks
  --many-objects 256`: graceful restart, SIGKILL/lease takeover and reuse of
  corrupted local state preserve Git/LFS, collaboration, branch rules,
  merge/squash and exact retries. Large SQLite tree/commit/tag bytes and OIDs,
  both commits, the annotated tag and 300 additional refs restore exactly.

The production code grows by 24 lines to move the invariant into the workspace
owner. No dependency, schema, configuration or wire contract changes. OS power
loss, unusual filesystems, Windows process containment and native peak resource
limits remain open, along with the other service delivery gates above.

## Durable account disablement qualification

On 2026-09-26, `POST /api/accounts/<account>/disable` added site-owner account
disablement. The Directory Cell checks the exact active admin credential and
protects the configured site owner inside the mutation transaction. Repeating
disablement returns 204. The existing enabled-account authentication join fences
all credentials on subsequent API, Git and LFS requests, without walking token
records or Repository Cells. Disabled names remain reserved.

Evidence:

- A direct Directory Cell test rejects a revoked owner credential, an owner
  read token, another account's admin token and an unknown credential. It proves
  site-owner protection, missing-account handling, exact mutation replay, denied
  authentication and rejection of account recreation with a new secret.
- The HTTP integration clones with a member credential before disablement,
  disables an account with multiple credentials, and rejects every credential
  through API, both Git advertisements/RPCs and LFS batch/object GET/PUT routes.
  A token issuance body admitted before disablement cannot create a replacement.
  A fresh local node restores the denial, reserved name, original issue content
  and attribution. The owner can still clone exact repository bytes and run fsck.
- The existing Directory recovery, token rotation, collaborator roster and
  33-repository discovery integrations pass. Discovery recovers through its
  existing movement-budget retries. All-target Clippy, formatting, Python smoke
  syntax and release build pass.
- The RustFS release process probe passes with `--sqlite-chunks --many-objects
  256`. It checks disabled credentials before repeating the operation after both
  graceful restart and SIGKILL/lease takeover with corrupted local SQLite.
  Git/LFS, collaboration, branch rules, merge/squash, exact retries, large SQLite
  objects and 300 additional refs also preserve their durable state.

The added production surface implements one Directory mutation and its HTTP
entry point, reusing the existing account column and authentication path. No
dependency, configuration or schema change. Requests authenticated before
disablement may finish under existing repository ACL rules. Re-enablement,
deletion, account listing, administrative UI, audit records, token expiry and
quotas remain open. The full hosting-service goal is still incomplete.

## Signed HTTPS Cell routing qualification

On 2026-09-26, gateways gained routing to the current Directory and Repository
Cell owners using Cellule's signed peer protocol. A repository still owns one
SQLite Cell. A remote gateway holds a disposable binding and Git cache, while
queries and mutations execute through the same typed Cell commands at the owner.
There is no second SQL implementation or automatic mutation retry.

Owner resolution checks durable control, live signed enrollment and matching
HTTPS endpoint. TLS verifies certificates and hostnames; a private deployment
may configure a PEM CA certificate. The receiver checks session, signature,
release, product principal, action and target scope. TLS termination and enrolled
fleet nodes are trusted infrastructure; this is signed peer authentication,
not mTLS. Warm local Directory calls avoid ownership-store reads.

Evidence:

- A real Cellule two-node integration pushes and clones through opposite owners,
  verifies exact bytes with stock Git and fsck, and checks that repository SQLite
  exists only at its owner. Directory authentication and both repositories recover
  through the surviving gateway after graceful owner shutdown.
- Missing CA trust, unsigned requests, wrong signing keys, unknown sessions,
  expired envelopes and incorrect principal/action scopes are rejected. A proxy
  consumes a completed mutation reply and substitutes HTTP 503. The gateway
  reports uncertainty; the durable account exists and an explicit retry succeeds.
- Initial lifecycle regressions exposed that a restored Cell can own authority
  before becoming resident. Resolution now uses Cellule's catalog/control-backed
  `local_handle` for that case, avoiding a second acquisition against its own node.
  All four lifecycle, six residency/fault, account-disablement, token-rotation and
  workspace-restart integrations pass, alongside the new peer integration.
- The release binary passes the RustFS process probe with `--sqlite-chunks
  --many-objects 256`. Two live HTTPS nodes push and clone stock Git/LFS through
  opposite repository owners. Killing the Directory owner produces 503 before
  lease expiry; the surviving process then restores Directory and repository
  state and clones exact Git/LFS bytes without restarting. The same run preserves
  collaboration, branch rules, merge/squash and exact retries through graceful
  restart and corrupted-local-SQLite takeover, including large SQLite objects and
  300 additional refs. An initial process-fixture failure correctly rejected a
  CA certificate presented as a server certificate; the fixture now uses a
  separate CA-signed leaf with the proper server usage and hostname.
- All-target Clippy, formatting, release build, Python syntax checks and example
  configuration digest checks pass. Cellule's pinned source supplies the peer
  dispatch, transport-uncertainty, live enrollment and lazy-local-owner contracts.

The additional production code owns HTTP transport, routing and authentication
at the server boundary; the runtime retains dispatch, leases and mutation identity.
Reqwest now enables verified rustls transport; rcgen and tokio-rustls support the
TLS test fixture. The dependency lock changes were reviewed. No Cellule pin,
patch or schema change is required. The example image digest was corrected to
32 bytes. The process smoke requires OpenSSL for its private test CA and server
certificate.

First acquisition determines placement. Directory takeover reserves one local
SQL slot; three repository gateway entries share the remaining admission. There
is no distributed pin spanning a multi-command Git request, and owner movement
can fail that request. Concurrent placement, partitions, larger hot sets,
production storage and throughput, native peak resource bounds, backup/restore,
GC and telemetry still need qualification or implementation. The service delivery
gates remain incomplete.

## Eviction admission and cross-gateway ref contention

The two-node, eight-repository integration reproduced a stock Git clone failure
while both nodes were healthy: the routing boundary returned
`Runtime(Capacity("movement budget"))` as HTTP 503 during sequential eviction.
The pinned Cellule actor allows two completed releases per one-second window.
Its `ReleaseIdleCell` branch returns this exact error before closing admission,
starting transfer preflight, closing SQLite or publishing ownership changes.

Canopy now waits one second and retries that release once. The runtime rechecks
its exact Cell generation, lease and settled-work conditions. The wait remains
inside the tracked, serialized residency operation, so cancellation does not
abandon ownership work. All other release errors follow the existing state
retention and handle-refresh path. This is admission waiting, with no Git request
or Cell mutation replay. Routing logs now include the source error needed to
distinguish these failures.

The new integration exceeds the combined resident capacity, clones every history
through each ingress, inspects durable ownership bounds and proves actual owner
changes. It then gates two stock Git pushes after both clients have sent updates
based on the same advertised ref. Exactly one succeeds; the other must report a
ref rejection, and the winning commit and file survive shutdown of the first
node and cloning all eight repositories through the survivor. The original test
failed before the admission change and passes with it.

Both HTTPS peer integrations and all six residency/fault integrations pass.
The strengthened race also passes with an assertion that distinguishes a ref
rejection from a transport/admission error. All-target Clippy, formatting,
Python smoke syntax and the release build pass.

The release binary also passes the expanded RustFS process probe with
`--sqlite-chunks --many-objects 256`: eight Git/LFS repositories exceed residency,
serve through opposite owners, and restore exact bytes after SIGKILL and lease
expiry through the surviving gateway without restarting it. Earlier phases retain
collaboration, branch policies, merge/squash, exact retries, large SQLite objects
and 300 additional refs across graceful restart and corrupted local state.
The active Colima engine could not see the external workspace bind mount, so this
run used a dedicated RustFS container with capped tmpfs data/log mounts. It proves
Canopy process recovery against a live S3-compatible service; provider disk and
power-loss durability are outside this run's evidence.

The production change adds ten lines at the existing release boundary. No new
configuration, dependency, storage format or schema. Shared-manager admission can
wait for one second under this pressure; the current three-gateway limit remains
an initial bound, not production capacity. Placement races, partitions, requests
spanning owner movement and larger hot-set throughput still need broader proof.

## Public repository visibility

On 2026-09-26, Repository Cells gained private-by-default visibility with an
owner-only generation-checked update. Anonymous identities are typed explicitly
and share the same read policy as authenticated public readers. Git/LFS writes,
approvals and merges retain explicit write authority. Public readers with a
write-scoped credential can participate in discussions, including PR comments
without a collaborator grant. Invalid supplied credentials remain errors.

Directory discovery publishes an owner-authorized candidate before visibility,
then rechecks each candidate's current Repository Cell access. Candidates remain
after privatization; pagination advances across inaccessible candidates without
returning their names. This avoids a cross-Cell cleanup racing a new public change.
The browser adds public discovery, visibility badges and owner controls. The
visibility dialog names the exposed data and explains that copies cannot be recalled.

Evidence:

- `tests/multi_server/visibility.rs`: stock anonymous clone and LFS pull with exact
  contents; issue, pull, review, line-discussion, check and browser reads; denied
  anonymous mutations; invalid credentials; owner-only and stale-generation
  visibility updates; no duplicate owner discovery; public non-member comments
  with denied approval; public and private state across fresh local recovery.
- `tests/repository_cell/visibility.rs`: Cell-local owner enforcement, anonymous
  read role, explicit writer role, exact receipt replay, stale/ABA denial and
  rejected direct Git publication by a public reader. Directory, Repository Cell
  and stock smart-HTTP integration targets pass. The existing private two-repository
  Git/LFS recovery test also passes.
- The direct Repository Cell test exposed an older rollback fixture that omitted
  mandatory merged-revision fields already present in baseline `d6d7d40`. Its
  injected row now supplies those fields, preserving the duplicate-row failure
  and all ref/generation/pull-state rollback assertions.
- All-target Clippy with warnings denied, Rust formatting, JavaScript syntax and
  Python probe parsing pass. The release binary builds.
- Real browser actions: anonymous empty listing, owner public toggle, anonymous
  files and issue detail without write controls, deep-link reload, then owner
  privatization and anonymous disappearance. No browser JavaScript errors.
- Release process smoke against RustFS `1.0.0-beta.8-glibc`: existing collaboration,
  checks, policies, dropped replies, token/account revocation and Git/LFS survive
  restart and takeover. Large tree/commit/tag SQLite chunks, a 256-file history
  and 300 refs restore exactly. Eight repositories across two HTTPS-connected
  nodes exceed resident capacity; anonymous Git/LFS and discovery work through
  the opposite gateway and survive SIGKILL takeover without restarting the
  survivor. Privatization then denies anonymous Git/LFS and discovery.

The RustFS fixture uses bounded Docker tmpfs because this host's active Docker
VM does not mount the workspace volume. This proves Canopy process recovery
against an S3-compatible service; it does not prove provider disk or power-loss
durability. Both initialization schemas changed; use a fresh development prefix.
No upgrade migration, dependency change or lockfile change was introduced.

The new source surface implements one visibility authority, a typed reader,
Directory coordination, HTTP/UI controls and shared query-policy changes; it
also removes duplicate read predicates. Public-service gate 9 remains partial:
organizations/teams, search, webhooks, candidate index rebuilding/retention and
production quotas remain open, along with the earlier operational gates.


## Deployment admission and fleet maintenance

On 2026-09-26, `Deployment` gained durable tenant/application identity and exact
compiled-release/image enrollment. First startup may initialize only an empty
catalog. Concurrent initialization and interrupted Prepared/Activating phases
resume the same deterministic operation. Existing catalogs without release
metadata are rejected. Repository and Directory provisioning both use the
runtime's release admission contract.

The CLI now supports `canopy maintenance <config.json> begin|status|end`, with an
operation UUID for begin/end. A closed release makes each node stop ingress and
perform supervised drain. Lease renewal continues during drain. Completion
requires no unfenced node advertisements and settled controls for every catalog
entry. Expired advertisements, missing roots and owned Cells remain unfinished.
The executable exits when its supervisor completes, including maintenance-driven
shutdown without an OS signal.

Evidence:

- Five deployment unit tests pass: concurrent initialization, interruption of
  either bootstrap phase, rejection of an unregistered catalog, corrupt release
  descriptor denial, and expired-advertisement refusal to resume.
- Two-node integration passes: real Git push, maintenance admission denial,
  exact-operation replay, wrong-operation rejection, both nodes drained, wrong
  identity/image rejection, fresh-local-storage clone and strict Git fsck.
- All five lifecycle integration tests pass. The new case pauses Idle control
  publication and proves maintenance cannot complete until Cell release settles.
  Existing startup/shutdown cancellation and failed-drain exclusion tests pass.
- All-target Clippy with warnings denied, Rust formatting, Python syntax,
  whitespace checks and the release build pass.
- The release process smoke against RustFS `1.0.0-beta.8-glibc` passes with large
  SQLite tree/commit/tag chunks, a 256-file history and 300 additional refs.
  Existing Git/LFS, collaboration, authorization and replay checks survive
  restart, local disk loss and owner takeover. Eight repositories across two
  HTTPS peers exceed resident capacity and survive SIGKILL of one owner.
- The same process run invokes maintenance without Git/node signing secrets,
  observes the surviving process exit successfully, verifies `drained: true`,
  resumes the selected release, then clones and verifies Git/LFS from a new
  node with fresh local storage. Normal SIGTERM drain also exits successfully.

The RustFS fixture uses bounded Docker tmpfs. This proves Canopy process and
S3-compatible protocol behavior, not provider disk or power-loss durability.
No schema, dependency or lockfile changes were needed. The compiled module
source digest changes, and release enrollment is now mandatory; use a fresh
preview prefix. Existing data is not migrated or deleted.

The added product code owns a real deployment authority, CLI and node lifecycle
integration; it does not add another Cell state machine. Gates 1 and 8 remain
partial. A crash during maintenance can leave a conservative unfinished drain;
fenced recovery, retained operation history, backup capture/restore, upgrades,
GC and operational telemetry still need their own acceptance evidence.

## Interrupted maintenance recovery

On 2026-09-26, `canopy maintenance <config.json> recover <operation-uuid>` gained
an enrolled recovery worker. It validates the exact maintenance operation and
release, uses the existing workspace exclusion and disk budget, then fences
expired sessions with the runtime's takeover API. One SQLite worker restores
existing catalog entries sequentially. Each handle drains and releases before
its scratch directory is removed. Normal service startup and offline recovery
share the same SQL bootstrap/takeover implementation.

Evidence:

- Eight deployment unit tests pass. New cases repair both abandoned catalog
  entries and unpublished owners, repeat a completed recovery, reject live owners
  and other operation IDs, and reject a missing published root while preserving
  its authority reference and the Maintenance release.
- All six lifecycle integration tests pass. Cancellation of the recovery caller
  leaves the supervisor enrolled and the workspace locked until its admitted
  Cell finishes publishing, draining and releasing. Maintenance end remains
  denied while the injected control publication is paused.
- The two-node maintenance/Git integration test passes with the shared acquisition
  path. All-target Clippy with warnings denied, Rust formatting, Python syntax,
  whitespace checks and the release build pass.
- The release process smoke passes against RustFS `1.0.0-beta.8-glibc`. After the
  existing two-node owner-loss checks, it pauses a serving node, begins maintenance
  and SIGKILLs that node. Status retains unfinished Cells and end is rejected.
  After lease expiry, the real recovery CLI fences the owner, restores and releases
  its Cells, and reports drained while leaving the release in Maintenance.
  Explicit end then allows a new node with fresh local storage to clone exact Git
  and LFS contents. Normal SIGTERM shutdown also succeeds.
- The same run retains the earlier coverage: eight repositories across two HTTPS
  peers, anonymous visibility/revocation, Git/LFS, collaboration and permissions,
  replay after lost replies, large tree/commit/tag SQLite chunks, a 256-file history
  and 300 additional refs through restart, disk loss and takeover.

Recovery requires a node signing key and local workspace, but no Git token or
HTTP listener. The begin/status/end commands still require neither node secrets
nor local runtime startup. Diagnostics use stderr; successful administration
commands emit one JSON status object on stdout.

The new source implements actual recovery coordination and shares acquisition
and settled-control policy with the existing service. It adds no schema, dependency
or lockfile changes. The RustFS fixture uses bounded Docker tmpfs; it is process
recovery evidence, not provider disk/power-loss qualification. Full claim,
publication and long-restore lease fault coverage remains open, alongside backup,
restore to an isolated destination, retention/GC, upgrades and production capacity.
Gates 1 and 8 remain partial.


## Independent backup and isolated restore

On 2026-09-26, the backup CLI gained create, verify and restore operations.
An enrolled worker captures a stable published cut using two complete catalog/
control reads, creates a runtime pin and copies it to an independent prefix in
the same provider. Pinned repository SQLite snapshots supply the external Git
blob/LFS manifest. Both source and destination bytes are verified before the
backup completion record is published. Verification and restore need no reads
from the original deployment prefix.

A conditional prefix reservation excludes service initialization from backup
and pending restore destinations. Restore installs immutable runtime data and
external bodies before allowing a fresh service to start. Same-operation retries
reconcile interrupted work; replay after completed restore does not reinstall
older authority over subsequent service writes. These commands require the
matching release, a signing identity and an exclusive disk-accounted workspace.

Evidence:

- Ten deployment unit tests pass, including competing service/backup reservations
  and rejection of an existing unmarked deployment.
- Seven lifecycle integration tests pass, including changed-control capture
  rejection and the existing startup/shutdown/maintenance cancellation invariants.
- The stock Git/LFS backup integration deletes every original source object,
  verifies the backup independently, restores to a fresh prefix and clones exact
  external Git/LFS bytes with strict/full fsck and retained issue content.
  Repeated create/restore operations succeed, and replaying a completed restore
  preserves a newly created issue in the running service. Corrupt LFS bodies reject both
  verify and restore; the incomplete destination cannot serve, and repairing the
  source body permits the same restore to finish. Insufficient local disk budget
  rejects verification.
- All-target Clippy with warnings denied, Rust formatting, Python syntax,
  whitespace checks and the release build pass.

The RustFS `1.0.0-beta.8-glibc` process run passed its existing Git, collaboration,
large SQLite-object, 256-file/300-ref, two-HTTPS-peer, privacy and maintenance
recovery phases. Its new backup phase exposed disabled conditional S3 copy in
the generic URL builder. Canopy now selects conditional multipart copy in its
shared executable provider construction, with no dependency patch or overwrite
fallback. A targeted final-release RustFS rerun passed the complete backup phase:
create/retry, corrupt destination rejection without overwriting it, repair/retry,
original-prefix deletion, independent verification, isolated restore, exact stock
Git/LFS clone, issue recovery and clean process shutdown. The fixture uses Docker
tmpfs; this is process/storage-API evidence, not provider disk or power-loss proof.

The new coordinator owns snapshot capture, copy admission and body verification;
shared blob/LFS paths and digest checks prevent a second storage policy. No schema,
dependency or lockfile changes are introduced. The LFS source refactoring changes
the compiled module digest, so existing preview prefixes require explicit future
migration. Cross-provider export, every interruption/lease boundary, old-version
restore, automated retention/GC and production capacity remain open. Gate 8 stays
partial.


## Bounded streaming LFS

On 2026-09-26, LFS object PUT/GET and backup verification moved from whole-object
buffers to bounded streaming. The batch/basic API, SHA-256 object identities and
SQLite metadata remain the same. The acceptance ceiling increases from 64 MiB
to 5 GiB, matching the one-part S3 conditional-copy ceiling in the current storage
library. Git blob limits are unchanged; quotas and full-limit capacity proof remain
open.

Uploads use 8 MiB multipart parts at a unique temporary object key. Hashing runs
in blocking workers under shared transfer admission. Verified bytes are completed,
conditionally copied to the immutable key and read back through the bounded
verifier before temporary-key cleanup and SQLite publication. Hash, length,
body, provider or timeout failures cannot publish a new SQLite reference. The
final reference transaction still rechecks repository write access. Supervision
keeps cleanup and publication alive across caller cancellation; ordinary failures
abort multipart work and remove its temporary key.

Downloads request at most 8 MiB per range, using the observed ETag/version and
checking returned range/size. The final range is withheld until SHA-256 and BLAKE3
match, so HTTP Content-Length cannot indicate a completed corrupt response.
The same reader verifies source and destination bodies during backup/restore.
No schema, dependency or lockfile change is required. The new reader/upload
modules replace buffered transfer work and own its deadlines, admission and
cleanup; their additional code is the transfer state machine, not another data
layout or compatibility path.

Evidence:

- Six focused tests pass: oversize/truncated input, disconnect cleanup, input idle
  timeout, empty/conflicting objects, replacement between ranges and withholding
  corrupt final bytes.
- Both node transfer integration tests pass. Eight stalled uploads retain shared
  admission; disconnect permits retry. A real HTTP download of a corrupted 9 MiB
  object advertises its length but fails before delivering the complete body.
- Stock Git/LFS smart-HTTP authorization/recovery and independent backup/restore
  integrations pass, including corruption rejection and preservation of writes
  made after restore.
- All-target Clippy with warnings denied and the release build pass.

Uploads have a 120-second input idle timeout and a 30-minute transfer deadline.
Each download storage read has a 120-second deadline. Process death/provider
failure can still leave multipart/staging data for lifecycle cleanup; automatic
retention/GC, outgoing socket stall limits, account fairness and production RSS/
throughput qualification remain open. Gate 6 remains partial.


The first process qualification and an isolated reproduction exhausted the original
1 GiB RustFS data tmpfs during repeated copies of the 80 MiB LFS object. `df`
confirmed 100% usage; provider logs reported `No space left on device`. The runtime
reported heartbeat-refresh failure and refused a successful backup receipt. The
fixture was increased to a bounded 4 GiB data tmpfs; no product admission, copy,
lease or test expectation was relaxed. Provider storage amplification and abandoned
multipart/staging retention must be included in production capacity planning.


The final release process qualification passed against RustFS
`1.0.0-beta.8-glibc` with `--sqlite-chunks --many-objects 256` and the 4 GiB data
tmpfs. Stock Git pushed an 83,886,080-byte LFS file in 2.55 seconds; a fresh node
restored from the independent backup and completed stock clone plus exact-byte
verification in 9.70 seconds. The same fixture passed empty LFS PUT/GET,
create/retry, corrupt destination rejection without overwrite, repair/retry,
complete source-prefix deletion, independent verify/restore, issue recovery and
clean shutdown. These are localhost fixture samples, not 5 GiB or production
throughput/RSS qualification.

The same final run retained two live HTTPS peers, eight repositories beyond
resident capacity, anonymous reads and privacy revocation, maintenance drain and
SIGKILL recovery, large SQLite tree/commit/tag objects, 256 files and 300 extra
refs, collaboration/ACL/token state, exact dropped-reply replay, native Git
configuration isolation, restart, disk loss and lease takeover. Formatting,
Python syntax and whitespace checks also pass. No product limit or verification
was weakened to accommodate the fixture's earlier storage exhaustion.


## Credential expiry

On 2026-09-26, account token issuance gained optional absolute `expires_at_ms`.
The Directory stores expiry with immutable credential identity; omitted/null
means non-expiring. Authentication and credential administration check expiry
without a cleanup job. Expired IDs and digests remain reserved, and retries
cannot extend or remove expiry. The site's final enabled non-expiring admin
cannot be revoked, even while an expiring admin is still valid.

Directory commands/queries bind one timestamp inside the owner handler. The
pinned Cellule client captures its context clock before queueing; refreshing
wall time at execution prevents queued requests from retaining expired authority.
One timestamp covers every SQL statement in the operation, including the
self-revocation decision and update. The additional source owns this execution
boundary; repository operations continue using their existing admission and
current ACL rules.

Evidence:

- All three Directory integration tests pass, including a deliberately blocked
  SQL worker: authentication and token issuance queued before expiry both reject
  after execution resumes. Expired admins cannot list, issue, revoke, create or
  disable accounts; retries cannot reactivate credentials.
- Both token HTTP integrations pass. Invalid expiry returns 422, changing an
  issued expiry returns 409, and delayed token/account creation rejects after
  the actor expires. Stock Git and LFS accept a valid temporary credential;
  expired credentials remain rejected after fresh-local-storage recovery, while
  an unexpired temporary reader still authenticates. Existing rotation,
  pagination, self-revocation and concurrent last-admin checks pass.
- The account-disablement recovery integration passes. All-target Clippy with
  warnings denied, formatting, Python syntax, whitespace and release build pass.

The schema, Directory operation set and module digest change; use a fresh
preview prefix. No dependency or lockfile change is needed. Fleet clocks must
remain synchronized. Already admitted Git/LFS/collaboration work may finish after
expiry. Issuance quotas, token/account administration UI, account deletion,
audit records and production capacity remain open; gate 2 remains partial.

The final release process smoke passed against RustFS `1.0.0-beta.8-glibc`
with `--sqlite-chunks --many-objects 256`. A temporary credential first succeeds
for API, Git and LFS, then expires without explicit revocation. Its stored expiry,
reserved identity and authentication denial survive clean restart and SIGKILL,
lease expiry and discarded local SQLite state. The same run passes existing
collaboration/ACL/replay behavior, large SQLite objects, 256 files and 300 refs,
eight repositories across two HTTPS peers, anonymous visibility revocation,
maintenance drain and recovery, and independent backup/restore with 80 MiB and
empty LFS objects after deletion of every original source object. The bounded
RustFS tmpfs fixture proves process/storage-API behavior, not provider disk or
power-loss durability or production capacity.

## Credential issuance limits

On 2026-09-26, the Directory began enforcing 64 active credentials and 256 new
credentials per rolling 24 hours for each target account. Initial account tokens
and all scopes count. Exact active-record retries succeed at capacity; expired
or revoked identities remain reserved. HTTP 429 distinguishes active capacity
from the rolling issuance window. Revocation/expiry free active slots without
resetting issuance history. Account creation retries and site-admin issuance
cannot bypass the target account's limits.

Admission counts and insertion share the existing owner-timed Directory
transaction. Two indexes provide separate active-expiry and issuance-time ranges;
both counts stop at their policy ceiling. No external counter, timer/reset job,
new dependency or configuration option is introduced.

Evidence:

- All four Directory integration tests pass. The new time-boundary case starts
  with published expired/revoked issuance history and a separate full pool of
  expiring credentials. It verifies both rejections, preserved SDK replay results,
  successful fresh attempts after time advances, authentication of newly admitted
  credentials and retention of every historical row.
- All three token HTTP integrations pass. Concurrent site-owner/self-admin
  issuances compete for one slot; exactly one succeeds. The suite exercises
  retry/conflict behavior, denial without reserving credentials, initial-account
  replay, revocation, both limit boundaries, account isolation and fresh-node
  recovery of active and daily usage. Existing expiry, rotation and last-admin
  invariants continue to pass.
- A disposable probe used the actual quota SQL and pinned SQLite 3.49.1 engine
  with 100,000 expired historical rows. Empty and historical-only lookups both
  took 33 VM steps for active count and 28 for recent count. With 64 permanent
  credentials and 300 additional recently expired records, the counts stopped
  at 64/256 using 536/2,075 VM steps. EXPLAIN QUERY PLAN confirmed the intended
  covering index ranges. These are SQL-work observations, not end-to-end latency
  or production capacity claims.
- All-target Clippy with warnings denied, Rust formatting, Python syntax,
  whitespace and the release build pass.

The Directory schema gains indexes and its module digest changes; use a fresh
preview prefix until explicit migrations exist. These policies bound credential
count and issuance rate. Retained credentials, runtime command receipts,
account-count admission, request-rate controls and deployment storage limits
remain separate work. Administration UI, account deletion and audit records
remain open; gate 2 is still partial.

The final RustFS `1.0.0-beta.8-glibc` process run passed with
`--sqlite-chunks --many-objects 256`. Through public HTTP, it fills active
capacity, proves rejection/retry/revocation behavior, then rotates credentials
until 256 retained issuances block both self-admin and site-admin requests.
The recorded usage, metadata, denial and exact-record retry behavior survive
clean restart and SIGKILL/lease expiry/local database loss. The same run passes
Git/LFS, collaboration, ACL/privacy, 256 files and 300 refs, large SQLite objects,
eight repositories across two HTTPS peers, maintenance drain and interrupted
recovery, and independent backup/restore of 80 MiB and empty LFS objects after
source deletion. The bounded tmpfs provider is process and storage-API evidence;
provider disk/power-loss durability and production capacity remain unqualified.


## Browser account and credential administration

On 2026-09-26, Canopy added **Account** navigation to its embedded interface.
Site-owner admins can page through enabled/disabled accounts, create accounts,
manage credentials and disable accounts. Other admin-scoped credentials manage
their own tokens; read/write credentials receive an explicit scope explanation.
The new session endpoint returns the actual credential ID so the browser can
mark **This session** and disconnect after confirmed self-revocation.

`GET /api/accounts` uses an exclusive name cursor, a 32-row bound and owner-time
credential authorization inside the same Directory query as the page. Both new
read endpoints omit credential secrets/digests and disable HTTP caching. The
Directory module digest changes without a schema change: use a fresh preview
prefix until explicit release migrations exist.

The browser uses Web Crypto, displays a secret only in its issuance dialog and
keeps the same payload, ID and absolute expiry after an uncertain result. It
requires explicit closure after showing the secret. Token scopes, repository
grants and site administration remain distinct server authorization boundaries.
Issued-token expiry defaults to 30 days; the first account credential remains
non-expiring under the existing account API. Account disablement is described as
irreversible through the current UI/API; the owner cannot be disabled.

Verification:

- Real HTTP integration: session identity matches the token page, unauthorized
  and mismatched Basic identities fail, read/non-owner-admin tokens cannot list
  accounts, 33 accounts paginate without losing disabled rows, revoked/disabled
  credentials fail, and session ID/account state survive fresh-local-state restore.
- Directory tests: revoked, read and non-owner-admin credentials cannot list
  accounts. A query queued before expiry and executed afterward returns no page.
  All four Directory tests and existing token rotation/recovery pass.
- Chrome against a release binary and disposable RustFS: create account, connect
  using its generated secret, self-service admin view, write-scope explanation,
  issue a 30-day admin token, connect with it, revoke it, and reject reconnection.
  Last permanent owner-admin revocation shows the server's conflict. Account
  disablement changes its row and rejects the account's credential afterward.
- Browser fault injection dropped an actual successful issuance response. The
  dialog retained the original secret, exposed uncertainty, and retried to one
  stored credential. A paused account-list response was canceled by disconnect;
  the private view stayed cleared. Temporary interception was removed.
- Credential pages show 32 entries followed by the remaining entry. Desktop and
  390-pixel mobile list/dialog rendering were inspected; no horizontal overflow
  or JavaScript console errors were observed. A stale rejected-login notice
  found during this pass was cleared when starting a new connection.
- The real RustFS process run passed with `--sqlite-chunks --many-objects 256`.
  New session/account reads and disabled-credential denials are checked across
  clean restart and SIGKILL/lease expiry/local SQLite loss. Existing Git/LFS,
  collaboration, quotas, two HTTPS nodes, maintenance recovery and independent
  backup/restore proof still pass, including 80 MiB and empty LFS objects after
  source deletion. This tmpfs provider run does not prove power-loss durability
  or production capacity.
- All-target Clippy with warnings denied, formatting, JavaScript/Python syntax,
  whitespace and the release build pass.

Gate 2 remains partial. Account deletion, reactivation policy, audit records,
account admission/rate limits and storage retention still require implementation
or product decisions; the browser milestone does not close the broader hosting,
performance or operations gates.

## Bounded streaming Git blobs

On 2026-09-26, external Git blob ingestion, cold-cache hydration and backup body
verification moved from whole-object buffers to bounded transfers. The canonical
SQLite references and immutable SHA-256 object keys remain unchanged. The blob
acceptance ceiling increases to 5 GiB; the transmitted push limit remains
512 MiB, and trees/commits/tags remain bounded at 64 MiB per object.

The native `cat-file --batch` reader separates validated headers from a sized
body stream and checks the trailing delimiter after consumption. Large blobs
flow through 8 MiB multipart parts, incremental hashes and verified conditional
publication. Range readers pin the observed ETag/version and withhold the final
range until Git SHA-1, SHA-256 and BLAKE3 all match. Cache hydration compresses
these ranges in blocking workers that retain disk admission through cancellation.
Backup verification uses the same reader for source and destination objects.

This removes the old buffered external-blob path. The added modules own bounded
transfer state, integrity checks and cleanup rather than a second storage format.
The Repository module digest now covers the external-blob acceptance limit; use
a fresh preview prefix. No dependency, lockfile or schema changes are needed.

Gate 3 remains partial. Native Git peak memory/scratch/CPU, non-blob streaming,
full 5 GiB/provider qualification, storage quotas and safe collection remain open.
Process death can leave staging/multipart debris requiring lifecycle cleanup.

Local diagnostic observations from the release RustFS fixture:

| Operation | Elapsed time |
| --- | --- |
| One push containing two 80 MiB random blobs | 73.55 seconds |
| Cold cache hydration of 167,772,702 raw object bytes after takeover | 31.10 seconds |
| Protocol v0 clone, including hashes and fsck | 74.75 seconds |
| Protocol v2 clone using the warm server cache, including hashes and fsck | 32.19 seconds |

Both clone packs contained 167,823,816 bytes. These timings come from a shared
local development host and a tmpfs RustFS container; they are not a controlled
before/after benchmark or a production throughput guarantee. A 600-second
`ps` sampling window spanning the large push and both clones collected 1,109
samples, targeting a 0.5-second interval. Peak aggregate Canopy RSS was 59.2 MiB;
including its descendants it was 484.1 MiB. This excludes client processes,
RustFS/Docker and the OS page cache, can miss short peaks, and does not cover the
later backup phase. It does not establish a memory limit; native process resource
admission remains required.

Verification:

- The 39-test Git unit suite passed. After binding cache metadata to its verified
  reader, the final five blob/native-pipe tests plus the cache admission test
  passed. They cover truncated/oversize input, immutable conflicts, all three
  hash mismatches, replacement between ranges, preservation of the next native
  Git frame, exact loose-object reconstruction and disk-admission cleanup.
- All-target Clippy with warnings denied, formatting, Python syntax, whitespace
  and the release build passed.
- The RustFS process run with `--large-clone --sqlite-chunks --many-objects 256`
  passed the large push, both stock Git clones, SQLite chunks, restart/SIGKILL
  recovery, collaboration/auth checks, eight repositories on two HTTPS nodes,
  public/private transitions and interrupted maintenance recovery.
- That run stopped in the backup phase on a heartbeat publication error. An
  isolated rerun exposed `No space left on device`: the 4 GiB tmpfs held roughly
  3.7 GiB of provider-internal temporary files. The full run is not recorded as
  passing. No retry or lease bypass was added to Canopy.
- The same final binary then passed the complete isolated backup phase on a
  dedicated disposable Docker volume: 80 MiB native Git plus 80 MiB/empty LFS,
  create/retry, corrupt destination rejection without overwrite, repair/retry,
  original source deletion, independent verification, isolated restore, exact
  stock Git/LFS clone and issue recovery. RustFS ended with 402 MiB of bucket
  data and about 5.1 GiB of internal temporary files (5.4 GiB total).
- The provider volume and container were removed after verification. Container
  auto-removal briefly delayed volume cleanup; removal was retried after the
  container disappeared and the storage partition returned to its original
  free capacity. An earlier unshared bind-mount attempt was also removed using
  only its three identified fixture directories.

This proves the exercised streaming/recovery paths, not full-limit capacity,
provider power-loss behavior, or provider temporary-file retention. The runtime
heartbeat error also hides the original failed storage update after its retries;
upstream diagnostic preservation remains follow-up work.
