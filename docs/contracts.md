# Canopy persisted contracts

These are the current development contracts. Freeze them and add migrations
before admitting persistent customer repositories.

| Surface | Current value | Owner |
| --- | --- | --- |
| Application | `canopy` | `CanopyApplication` |
| Repository namespace | sixteen bytes of value `0x47` | `REPOSITORIES` |
| Directory namespace | sixteen bytes of value `0x48`, one fixed SQL shard per tenant | `DIRECTORY` |
| Repository name | lowercase ASCII owner/name components, each at most 64 bytes | Directory Cell |
| Name reservation | owner/name row stores one canonical UUID and pending/ready state | Directory Cell |
| Repository rename | compare expected UUID, move one ready name atomically, keep Cell identity | Directory Cell |
| Account | lowercase ASCII name, enabled flag | Directory Cell |
| Access token | unique opaque 16-byte ID, SHA-256 secret digest, account, scope (`read`, `write`, `admin`), enabled flag, creation timestamp | Directory Cell |
| Repository access | immutable owner identity plus collaborator role (`read`, `write`); only owner has repository admin access | Repository Cell |
| Repository discovery | retained `(account, repository UUID)` candidates recorded before grants; current Repository Cell ACL filters results | Directory candidate index and repository manager |
| Issues and comments | repository-local numbers, immutable creation UUID/binding, text, author, optimistic version and timestamps | Repository Cell |
| Commit checks | owner-defined reporter/context version, queued attempts, immutable terminal results and newest-created selection | Repository Cell |
| Repository partition | canonical 16-byte UUID, versions 1–8, RFC 4122 variant | `repository_target`, `CellType::entity_uuid` |
| Repository Cell | one SQL Cell per repository UUID | Cellule catalog and authority |
| Local residency | one pinned Directory Cell plus at most three Repository Cells; inactive repositories release ownership before their slot is reused | Repository manager and Cellule transfer preflight |
| Git object format | SHA-1 object IDs from canonical Git type, decimal length, NUL and body | `object_id` |
| Small Git objects | SQLite `objects.body`, maximum 768 KiB | Repository Cell |
| Large trees, commits and tags | SQLite chunks of at most 512 KiB; object size above 768 KiB and at most 64 MiB | `object_chunks`, verified before object publication |
| Object publication | at most 128 records, 768 KiB inline payload and 64 MiB SQLite verification bytes per atomic command | `PutObjects`, operation 5, codec 2 |
| Large Git blobs | immutable `repos/<uuid>/git-blobs/<sha256>` body, SQLite digest/size/reference | `LargeBlobStore` |
| LFS objects | immutable `repos/<uuid>/lfs/<sha256>` body, SQLite digest/size/reference | `LfsService` |
| External byte ceiling | 64 MiB per Git blob or LFS object | current object transfer path |
| Node transfer admission | eight active Git/LFS requests across repositories; immediate 503 with `Retry-After: 1` when full | repository HTTP router |
| LFS request deadline | 120 seconds for batch and object PUT body reception; timeout returns 408 | Git HTTP router |
| Git request admission | 512 MiB for receive-pack, 64 MiB for other requests; 120-second upload deadline | anonymous request spool |
| Ref mutation | check actor's write role, compare expected optional OID and monotonic version; retain deletion records and apply all updates in one Cell transaction | `FinalizePush` |
| Symbolic HEAD | `ref_generation.default_branch`, initially `refs/heads/main`; owner-authorized compare-and-set with ref generation | `RepositoryCell::set_default_branch` |
| HTTP push identity | repository-local UUID bound to account and BLAKE3 request digest; different IDs identify independent operations | `pushes` |
| HTTP push outcome | status, headers and BLAKE3-verified body in SQLite chunks; publish response pointer atomically with accepted refs | `CompletePush`, codec 1 |
| Graph certificates | at most 128 candidate objects and 64 MiB SQLite object bytes per command; typed dependencies must already be certified | `CertifyObjects`, operation 6, codec 1 |
| Git connectivity at ref publication | at most 64 certified new tips, with commit-only branch tips; same transaction as ACL, ref CAS and outcome | `object_closure`, shared ref finalization |
| LFS metadata publication | check actor's write role in the SQLite insert transaction | `record_lfs_object` |

The `objects` table stores one verified kind, size and independent BLAKE3
digest per Git OID. External objects also store SHA-256. Readers verify the
bytes against the SQLite record and recompute the Git OID or LFS SHA-256.
The `refs` table stores name, optional OID and version. Deletion sets the OID
to NULL and advances the version; recreation advances it again. Never-seen
names and deleted names have different expectations, so stale plans fail even
after a ref returns to the same OID or to a deleted state. Deleted rows remain
in SQLite and are excluded from Git advertisements and namespace conflicts.
They must not be collected without a replacement mechanism for fencing stale
ref plans. `FinalizePush` uses codec version 3 for this expectation shape.
Before reporting accepted refs, a push persists all new objects and publishes
the accepted ref changes and replayable response through `CompletePush`. Both
`CompletePush` and the direct `FinalizePush` command share the same ref checks.
Rejected or interrupted pushes may leave unreferenced objects; collection is not
implemented yet.

The `ref_generation` singleton advances once in the same transaction as each
accepted ref plan or default-branch update, including selecting the same branch.
Typed finalization and HTTP completion share the ref-plan update; rejected plans,
failed HEAD preconditions, completed-request replay and object/ACL writes do not
advance it. Ref queries return at most 256 rows with HEAD and generation in one SQLite
statement, including an empty terminal page. A continuation must supply the
first page's generation. Changes invalidate the scan even when tips return to
their previous OIDs or names are deleted and recreated. The gateway discards
partial scans and tries at most three scans, then returns HTTP 503.
Successful scans therefore describe one coherent ref state. That state can
become older while its disposable cache is hydrated; admitted readers retain
the selected generation, and immutable objects remain readable without GC.

HEAD must name a valid `refs/heads/` reference under Canopy's existing ASCII,
255-byte ref policy. Changing it requires the owner, an expected ref generation,
and either a live target branch or no live branches. Owner authorization, target
existence and generation comparison occur in one Cell SQL update. Concurrent
ref changes and HEAD ABA invalidate the precondition. The SDK's mutation identity
replays its recorded result; the HTTP API uses an explicit generation and requires
a fresh GET after an ambiguous reply. It does not silently retry updates.

`GET /api/repositories/<name>/default-branch` requires a read-scoped token and
repository access. It returns `repository_id`, `reference` and `generation`.
`PUT` requires an admin-scoped token and repository ownership, with
`repository_id`, `reference` and `expected_generation` in its JSON body. The UUID
prevents name reuse from retargeting a stale administrative write. Malformed
input returns 422; stale identity/generation or an absent target in a repository
with live branches returns 409. Forbidden repository metadata is hidden as 404;
an authorized collaborator attempting an owner operation receives 403.

The cache key includes HEAD and the coherent ref generation. New generations
get a new, disk-accounted HEAD file; active readers retain their original cache.
Native Git supplies populated HEAD advertisements for protocols v0/v2 and the
protocol-v2 [unborn HEAD response](https://git-scm.com/docs/protocol-v2#_ls_refs).
Empty protocol-v0 clones cannot learn an unborn branch through that extension.
Deleting the selected branch retains the symbolic target without auto-selecting
another branch, consistent with Git's [HEAD layout](https://git-scm.com/docs/gitrepository-layout#_description).

Git owns the [per-ref report and atomic capability](https://git-scm.com/docs/protocol-capabilities#_report_status).
An ordinary push may accept some refs and reject others. The gateway publishes
the actual accepted changes in one Cell transaction before forwarding Git's
report unchanged. A rejected atomic push changes no refs. Malformed packs
retain Git's unpack failure report and publish no refs. The gateway does not
infer transaction success by searching diagnostic text for status fragments.

Both ref commands require certified reachable Git graphs in the ref transaction.
Commit trees and parents, tree entries, and annotated tag targets must exist
with the required object kind. Branch tips must be commits. Tree symlinks and
regular files require blobs; gitlinks refer to another repository and do not
require a local object. The traversal follows Git's
[commit headers](https://github.com/git/git/blob/v2.50.1/commit.c#L435),
[tag targets](https://github.com/git/git/blob/v2.50.1/tag.c#L142), and
[tree connectivity rules](https://github.com/git/git/blob/v2.50.1/fsck.c#L334).
It checks graph structure and content hashes; it is not a replacement for all
of `git fsck`'s metadata, filename and portability checks.

An async postorder traversal prepares certificates before either ref command.
It reads only uncertified history, deduplicates repeated typed roots/edges and
checks child status in pages of 128. Certified children need no separate Cell
call; uncertified children carry their observed immutable metadata into traversal.
It groups at most 128 candidate object IDs in dependency order. Its cache of objects ready
for certification covers only the pending batch. `CertifyObjects` independently
reads each candidate from SQLite, recomputes hashes and graph edges, and requires
all typed children to be certified. It checks child metadata in groups of at most
128 rows. Newly verified inline and chunked bodies share a 64 MiB budget per
command; references to verified external blob bodies do not consume that budget.
The final ref transaction checks only the at-most-64 new tips, their certificates
and branch types, together with current ACL, versions and namespace conflicts.

The certificate command is the trust boundary: incorrect candidate order,
missing or mismatched children, corrupt data or work beyond the byte limit
rejects its whole application savepoint. The client traversal cannot assert a
certificate. Certificates and refs are separate publications. A failed or
interrupted preparation may retain valid certificates from earlier batches;
retries reuse them. Partially certified roots cannot publish refs, and certificate
commits do not advance `ref_generation`. An external blob certificate relies on
the verified immutable upload before its SQLite record; there is no external
network I/O inside certificate or ref transactions.

`RepositoryCell::finalize_push` and HTTP completion share this preparation path.
The low-level `FinalizePush` command requires certificates already present.
A preparation failure is reported as ref invocation `NotStarted`, retaining its
underlying error, since the final ref command has not been dispatched. Certificate
work may itself have a pending outcome; retry rechecks committed state. Direct
callers must keep their supplied finalization identity valid through preparation;
HTTP completion creates its final command identity after preparation.

Certificates rely on immutable object records and retained object bytes. There
is currently no object mutation or collector through the product API. Any
future collector or repair that removes or changes objects must invalidate all
affected ancestor certificates before ref publication resumes; clearing the
entire certificate table is the conservative implementation. Backup and restore
must preserve the database and its referenced external bodies together. Each
certificate transaction has explicit object/byte limits, but large individual
objects, traversal frontier memory and production-scale latency still need
capacity qualification.

For receive-pack POSTs, `Idempotency-Key` must be one canonical lowercase,
hyphenated UUID. Missing IDs are generated, and recorded responses include
`X-Canopy-Push-Id`. The request digest covers the `canopy-git-push-v2` domain,
protocol and gzip flags,
content-type presence, and length-prefixed method, internal repository path,
query, content type and body. Account identity is bound separately. The
repository UUID scopes the record, so a name change preserves its identity.
Authorization secrets and host addresses are not part of the digest.
Explicit identity encoding and absent encoding have the same interpretation;
changing to gzip under a completed request UUID produces a conflict.

A reservation binds the ID before running Git. Response metadata and 512 KiB
body chunks are durable before finalization. `CompletePush` checks the binding,
chunk completeness and current write role, then applies the ref plan and
publishes the response pointer in one Cell transaction. A completed ID returns
its existing outcome. Only a published pointer can expose a staged response;
replay verifies the stored body length and BLAKE3 digest. The HTTP boundary
checks current token scope and repository access even for completed requests.

No-op, rejected and non-200 Git backend responses are also terminal outcomes.
Infrastructure or ref-CAS failures before publication leave the ID pending;
an exact retry can rerun against current refs. A lost reply after publication
returns the original report without changing refs, even when later operations
have changed them. Retrying a new Git invocation is not necessarily an exact
HTTP replay: its body can differ and then the reused ID conflicts. A distinct
ID permits identical bytes to represent a new operation. There is no expiry or
GC for reservations, completed outcomes or abandoned response attempts yet.

All branches currently permit deletion by an authorized writer. The gateway
sets `receive.denyDeleteCurrent=ignore` because its HEAD must not implicitly
create a branch protection policy. Git's
[receive-pack implementation](https://github.com/git/git/blob/v2.50.1/builtin/receive-pack.c#L1428-L1454)
otherwise rejects deletion of the branch named by HEAD even in this bare
cache. Future branch rules belong in the Repository Cell transaction.

A name reservation commits before its Repository Cell is provisioned. A retry
reads the previously reserved UUID and completes the same Cell instead of
assigning another identity. The server marks the row ready only after it has
acquired that Cell. Rename updates only the ready Directory Cell row, so the
same UUID and repository contents survive a URL change. Ready names route
through their UUID on demand; one node can serve multiple repository Cells.
The Directory Cell authenticates token digests; each Repository Cell authorizes
its own Git and LFS access. Repeated bootstrap requires an active admin token
belonging to the configured owner. Account disablement and audit records remain
open.

`GET /api/repositories/<name>/collaborators?after=<account>` requires an
admin-scoped token and Repository Cell ownership. It returns `repository_id`,
`owner`, `collaborators` (objects with `account` and `role`), and `next_after`.
The immutable owner is separate from the explicit grants. The Repository Cell
SDK checks ownership in the same SQL batch that reads its member rows; Directory
discovery candidates never supply roster entries. A resident route stays pinned
through the query. Missing access returns 404; a known collaborator without
owner authority receives 403. Insufficient token scope receives 403.

The page contains at most 32 grants ordered by account name. Omit `after` for
the first page; a supplied empty or invalid account component returns 422.
A full page returns its last account as `next_after`; an exact multiple of 32
requires a final empty page. Stop when `next_after` is null. Pages are independent
observations, not one ACL snapshot: concurrent grants or changes before the
cursor require a fresh scan. Repository rename changes the route without
changing the UUID or membership. No schema change is needed for roster reads.

Repository discovery uses a Directory Cell candidate index, keyed by account
and repository UUID. The product grant path first records a candidate through
an owner-authorized Directory write, then grants access in the Repository Cell.
It reports success only after both publications. An interrupted grant can leave
a candidate without an ACL; every listing still checks that Cell before exposing
metadata. SDK callers coordinating grants must follow the same ordering.
Direct `RepositoryCell::grant_member` is the local ACL primitive and does not
populate the Directory index.

Revoke changes only the authoritative Repository Cell ACL. Candidates remain:
removing one after revocation could race with a new grant and hide valid access.
Regrant reuses the same candidate. Future candidate compaction needs fencing
against grants; no candidate GC runs today. Account and repository quotas remain
open. Owned repositories use the immutable Directory owner field without loading
each Cell. Candidate discovery itself is not authorization, and the SDK names
its primitive `list_candidates` to preserve that distinction.

`GET /api/repositories` requires an authenticated read-scoped token and checks
at most 32 candidates per page. Results use UUID order, so rename preserves
pagination position. The `after` parameter is a returned UUID cursor; arbitrary
cursors never bypass the caller's account filter or Cell ACL. Missing and
revoked candidates contribute no entries. A page may be empty while
`next_cursor` is present. Clients must continue until it is null. Cursors contain
UUIDs rather than private repository names; a cursor may identify a prepared or
revoked candidate and does not grant access to it. Pending creations are excluded.

Cell movement capacity can stop a cold scan early. After any progress, the
manager returns the authorized entries already read and a cursor for the last
candidate checked. With no progress it returns 503 and `Retry-After: 1`. Other
storage/runtime failures return an error rather than an apparently complete
listing. Directory queries use owner/UUID and account/UUID indexes; they do not
enumerate other accounts' repositories. Pages are not a cross-Cell snapshot:
new grants or repositories behind a cursor require a fresh scan, and each ACL
is observed when that candidate is checked. Cold lists still restore candidate
Cells, so the 32-candidate bound is not a production latency guarantee.

`GET /api/repositories/<name>` first checks for a Directory candidate (or owner),
then the current Cell ACL before returning metadata. Missing and unauthorized
names return 404. It returns `owner`, `name`, `repository_id`, `clone_url`, `role`,
`default_branch` and `ref_generation`. `role` describes repository membership;
token scope remains an independent restriction. Repository access is checked at
read admission, as for existing Git/LFS reads. Rename and ACL/default-branch
changes across Cells are not presented as one atomic snapshot.

Repository residency is bounded independently of the number of directory
entries. A request pins its loaded repository before using its Cell or Git/LFS
router. That pin lasts through request processing and the complete response
body, including trailers; EOF, body errors and disconnects release it. Membership
changes and repository initialization also retain their route while using it.
The Directory Cell stays resident for authentication and routing.

When all repository slots are occupied, the manager selects the least recently
used unpinned repository among Cellule's settled transfer candidates. It calls
`release_idle_cell` with the exact Cell generation and current node session.
Cellule rechecks obligations and admission, closes SQLite and confirms durable
owner release before returning success. Only then does Canopy drop its cached
handles and remove that repository's local SQLite directory. Existing Git
subprocesses separately retain their cache generation until their work ends.
External objects and durable Cell state remain intact. Later access acquires
the idle Cell from its published root and rebuilds the disposable Git cache.

If every repository is pinned or the runtime refuses release, the new request
returns 503 without evicting active work. Failed release retains local state and
invalidates the manager's cached handles. Cellule transfer preflight may replace
the old capability even when release is refused. A later request binds fresh
handles only if the runtime confirms a serving resident owner. Otherwise it
returns 503, including a repeated create request for that repository; a terminal
owner-release failure requires a node restart and authoritative-root recovery.
Other resident repositories remain available.

After confirmed release, an entry remains marked released until local directory
deletion succeeds. Access to that repository, or an admission needing its slot,
retries deletion before restoring into the required empty destination. A missing
directory already satisfies cleanup. Local cleanup never authorizes release or
deletes durable objects. Acquisition and release run in tracked tasks so a
client disconnect cannot interrupt their
local lifecycle update. Graceful shutdown waits for those tasks before draining
the Cell node. Owner initialization retains an acquired entry on failure and
retries its idempotent setup on later access. The residency bound does not
establish throughput for a larger concurrent hot set. Disposable Git files use
the same shared disk admission described below.

Token scope is checked at request admission. Repository write access is also
checked in the ref or LFS metadata transaction, so an intervening collaborator
revocation blocks publication. Token revocation denies new authentication;
already admitted Git/LFS reads and writes may finish using their admitted
principal, subject to the Repository Cell's current ACL. Token issuance and
account creation additionally recheck the actor credential within the mutation,
including after request body reception. A denied upload can leave an unreferenced immutable body; no
collector removes those bodies yet. Account creation is idempotent only for
the same account, active token digest and scope. Bootstrap accepts an issued
active owner admin token and does not replace or revive credentials.

Git packs and the bare repository cache are transport and acceleration
artifacts. Neither is authoritative. The gateway can reconstruct cache objects
from the Cell and external store. Integration tests prove exact-root restore
after clean node shutdown and local SQLite loss. The process smoke also proves
Git and LFS fetch after an unclean owner exit, lease expiry and a third process
claiming the Cell from an S3-compatible object store. A proxy also drops a
successful push reply; exact replay after takeover returns its recorded report
and preserves a later ref deletion. Crashes during
individual staging/publication boundaries, simultaneous multi-node routing and
backup restore still need proof before service readiness.

Cold hydration uses `RepositoryCell::object_page(after)` in ascending OID order.
Each page contains at most 128 records and 768 KiB of aggregate inline bodies.
A metadata query chooses the bounded prefix; a second query reads those exact
records with the first query's receipt as its minimum observation. Metadata
overhead fits beneath Cellule's 1 MiB query result ceiling. Callers continue from
the last OID until an empty page; a short page is not end-of-stream.

Records are immutable through product APIs, and no collector removes them.
This makes the two observations safe; a future collector must fence active
reads before deleting records or bodies. The reader rejects changed/missing IDs,
invalid descriptors, and inline size, Git OID or BLAKE3 mismatches. Inline bodies
move into one bounded blocking verification task without a payload clone.
Chunked and external records carry descriptors; their body readers verify the
actual bytes before the gateway writes them to the disposable cache. Refs become
visible there only after complete successful hydration. This bounds each query,
not total repository hydration time or native Git scratch usage. The previous
single-record `next_object` API was removed from this unreleased crate.

Debug events separate Repository Cell acquisition from successful cache
hydration. Hydration reports object count, raw bytes, admitted cache bytes, total
time, page-read/verification time, external/chunk body-read time and cache-write
time. Cache writes include worker scheduling, OID verification and compression.
These elapsed wall times include waiting; they are not CPU profiles or a claim
that all page time belongs to SQLite execution. Acquisition includes authority
and root validation, sparse activation and publication. Failed phases remain
errors and do not emit a successful-completion event.

Push object ingestion runs two Git processes regardless of object count:
[`rev-list --objects --no-object-names --stdin`](https://git-scm.com/docs/git-rev-list)
enumerates objects reachable from accepted new ref tips, excluding the previous
live tips; [`cat-file --batch`](https://git-scm.com/docs/git-cat-file) reads only
candidates missing from SQLite. Deletion-only pushes skip both. Enumeration is
consumed incrementally; ref roots travel over stdin rather than an unbounded
argument list. Incoming objects reachable only from rejected ref tips are not
newly persisted.

Both commands disable replacement refs so stored bytes retain their canonical
identity. The batch reader caps headers at 128 bytes, validates kind and size
before allocation, reads the exact body and delimiter, and recomputes its Git
OID. Missing, truncated, malformed or corrupt output fails the push. Each enumeration read,
object read and process completion has a 120-second timeout. OID hashing uses
a blocking worker so large bodies do not occupy an async executor thread. Stderr is drained
concurrently, retaining at most 64 KiB per process. Dropping the reader kills
both direct children and aborts pipe tasks; normal completion requires both
successful exits before publishing refs or recording the successful report.
Previously published graph closure makes exclusions safe; ref publication
still requires every new tip to have a durable certificate.

Candidate existence queries group up to 128 IDs. Missing records accumulate in
an `ObjectBatch` with at most 128 records and 768 KiB of aggregate inline bodies,
leaving room for metadata beneath the 1 MiB operation input limit. External
blob records count toward the record limit; their bytes are verified and
uploaded before publication. Chunk references count toward the record limit and
a separate 64 MiB aggregate verification budget shared with inline bytes. The
decoder enforces these bounds before publication. One typed Cell command publishes the batch and returns one receipt.
It recomputes inline Git OIDs and BLAKE3 digests, then compares every inserted
or existing row with the complete expected record. A mismatch rejects the
command and rolls back all inserts in that batch. Repeating the same mutation
identity and payload returns the recorded result through Cellule deduplication.

Trees, commits and tags larger than 768 KiB are staged in `object_uploads` and
`object_chunks`; their bodies stay in SQLite. Each part is at most 512 KiB and
one object is at most 64 MiB (128 parts). Part command identities derive from
the caller's upload identity and part index, so retrying the same staging
operation replays committed parts. Staging does not create an `objects` row:
queries and ref validation cannot see incomplete uploads.

`PutObjects` codec 2 accepts a chunk reference containing upload ID, size and
BLAKE3. In the publication transaction it requires the exact chunk count and
part lengths, reconstructs the body, verifies the canonical Git OID and BLAKE3,
and then inserts the immutable object record. A failure rolls back every object
in that batch. Inline and external-blob records use the same publication command.
Repeated identical objects converge on the existing record; unused duplicate
uploads remain staged until a collector can prove they are unreferenced.

Hydration and direct SQLite object reads use the same chunk count, length and
hash checks. Every SQL result contains at most one chunk, fitting Cellule's
1 MiB result limit. Reads reconstruct a bounded whole object in memory. Graph
certification reconstructs chunked objects before applying the same typed edge
checks as inline objects; chunking does not certify missing edges. Part data is
immutable through product APIs, and no collector exists yet. Any future collector
must preserve chunks reachable from objects and invalidate graph certificates
before removing their dependencies.

Object batches remain separate from ref and push-outcome publication. A later
failure can leave earlier batches unreferenced; a retry finds those records
through the bounded existence query. The final transaction still validates
graph closure before exposing new refs. Retention and collection of abandoned
objects remain open. Per-object buffers, complete cold-cache hydration and
large initial graph traversals still need capacity qualification.

Git CGI responses use one subprocess/stream implementation. The HTTP gateway
streams advertisements and fetch replies through four queued chunks of at most
64 KiB each. Backpressure stops stdout reads when that queue fills. The worker
retains the selected bare-cache generation until Git has finished with it, so a
concurrent cache replacement cannot delete files beneath an admitted reader.
Receive-pack collects its response with the existing 64 MiB limit, then persists
objects, validates refs and records the outcome before sending HTTP success.

CGI headers and stderr are each bounded at 64 KiB. The existing 120-second
subprocess deadline includes output backpressure. Once headers are sent, a
process failure or timeout produces an HTTP body error rather than successful
EOF; before headers it fails the request. Dropping the response cancels its
worker. Unix subprocesses use a dedicated process group so cancellation also
kills upload-pack/pack-objects descendants; other platforms currently use
Tokio's direct-child kill-on-drop behavior and still need lifecycle qualification.

Incoming Git bodies stream into anonymous temporary files before CGI execution.
Each write reserves bytes from the same `DiskBudget` used by the node's SQLite
host. All repositories share that budget. Blocking writes retain the file and
reservation together, including when an upload is cancelled. The OS removes
spools when their last handle closes, including on process death. Uploads have
a 120-second deadline; limits are 512 MiB per push and 64 MiB per fetch request.
Oversized bodies return 413, failed request bodies 400, timeouts 408, and exhausted
disk admission 507. Each repository serializes pushes before receiving its body.

The gateway reads the completed spool in bounded chunks to compute the existing
length-prefixed request digest, then checks the durable replay record before
running Git. This keeps the identity format unchanged for chunked and fixed-length
HTTP requests. CGI receives the exact file size and reads stdin directly from the
file. Its worker retains the spool's disk reservation until subprocess work ends.
No Git objects or refs are published from an incomplete HTTP upload.

Git requests support identity and gzip content encoding (`x-gzip` is also
accepted). Unsupported or stacked encodings return 415. The initial spool retains
wire bytes for the unchanged push digest and replay lookup. A pending operation
then validates and decodes the entire gzip stream into a second anonymous spool
before hydrating the cache or starting Git. Both spools share disk admission;
the encoded charge is released when decoding completes. Each decoded write
reserves capacity first. Both encoded and decoded sizes must fit the request's
512 MiB push or 64 MiB fetch limit. Corrupt headers, checksums, truncated members
and trailing garbage return 400; decoded overflow returns 413 and exhausted
admission returns 507. Concatenated valid gzip members are decoded together.

Decoding has a separate 120-second deadline. Cancellation is checked during
compressed reads and decoded writes, including streams of empty members. A
blocking worker retains both files and reservations until it stops. Git receives
only the validated decoded file with identity encoding and its exact length.
Canopy sets [Git CGI's request-buffer limit](https://github.com/git/git/blob/v2.50.1/http-backend.c)
to the admitted 64 MiB fetch limit, replacing its smaller default. Native pack
expansion and scratch usage still need resource enforcement. Exact replay binds
the original wire representation and gzip flag; it does not re-run decoding or
Git for a previously completed operation.

Push reports, LFS transfers and individual object hydration still allocate
bounded whole buffers. Cold fetches still rebuild the complete bare cache.
Production throughput and native process resource usage remain unqualified.
Spooling adds a local-file pass before Git can begin pack ingestion.

### Node transfer admission

The composed `CanopyServer` admits eight repository Git/LFS requests per node,
after authentication and before repository lookup/loading. A full semaphore
returns 503 with `Retry-After: 1` immediately; there is no waiting admission
queue. All repositories and both transports share the same semaphore. Health,
readiness, account and repository management routes bypass this transfer limit.
Authentication and management work need separate capacity qualification.

An admitted handler runs in a tracked task. Client disconnect does not abandon
Cell transitions, cache hydration or push publication halfway through. Shutdown
waits for tracked tasks before draining the Cell node. An authorized push may
therefore finish after its client disconnects; use the existing push identity and
replay contract to resolve its outcome. Input spools retain admission through
blocking writes, hashing, gzip decoding and the native Git worker. A timed-out
or cancelled blocking decoder keeps its permit until its worker exits.

HTTP response bodies retain admission, and emitted `Bytes` frames own a shared
permit until their final clone is dropped. This covers queued response data even
when the body has already reported EOF. The native worker retains its permit
until completion or process-group cleanup signaling on cancellation; OS process
teardown may trail that signal. Capacity is returned only after the last owner
releases the permit. This bounds admitted transfers, not subprocess descendants,
peak RAM, or native Git scratch bytes. The standalone lower-layer `GitHttpApi`
fixtures do not apply the node's admission policy.

LFS batch and PUT bodies retain their existing byte ceilings and now have a
120-second reception deadline; timeout returns 408 (JSON for batch requests).
This is separate from Git input, decode and subprocess deadlines. Outgoing LFS
socket stall limits and fair scheduling between accounts remain unqualified.
The limit of eight is an initial operational policy, not a throughput claim.

### Disposable Git cache admission

Cache construction reserves logical file bytes before writing its bare config,
HEAD, compressed loose objects and loose refs. The layout follows Git's
[repository format](https://git-scm.com/docs/gitrepository-layout). Construction
does not invoke `git init` or copy template hooks. Each cache generation owns
its directory and reservation; warm caches remain charged between requests.
Replacing a generation releases an unused old cache before building the next;
active readers retain their old generation. Blocking hydration workers also
retain ownership when their caller is cancelled. Process guards signal Git
before dropping cache and input owners on cancellation. Git hooks and receive
auto-GC are disabled for these disposable repositories.

After receive-pack finishes, Canopy measures its actual cache file lengths and
resizes the reservation before staging Git objects, push replies or ref changes.
Admission failure returns HTTP 507 and publishes no ref change or completed
push outcome. Retrying the same request identity can succeed after capacity is
freed. Hydration admission failure also returns 507 and discards the incomplete
cache. Cache deletion precedes releasing its reservation; if deletion fails,
the charge is retained for the remainder of the process lifetime.

This accounts retained caches and bounds Canopy's hydration writes. It does
**not** impose a hard limit on native Git's peak scratch usage: native writes
are measured after execution, and a rejected push can temporarily exceed the
budget. File lengths also exclude filesystem allocation and inode overhead.
Crash-left directories and cleanup-failure charges need startup reconciliation;
native scratch enforcement remains a release gate. These reservations are
shared node admission, not per-account durable storage quotas.

Schema version 1 is still changing in this unreleased repository. The chunk,
HEAD, discovery, token-metadata, issue and check layouts and the operation-5 codec change
require a fresh development storage prefix;
there is no upgrade reader for older development databases. The module
descriptor and object paths will become compatibility boundaries at the first
persistent preview. The current build pins an immutable public Cellule
revision; its UUID partition contract is proposed in
[Cellule PR #5](https://github.com/crabbuild/cellule/pull/5).

### Token lifecycle

Account token routes list, issue and revoke credentials in the Directory Cell.
An admin-scoped credential can manage its own account; the configured site owner
can manage any enabled account. The SDK's `TokenAuthority` carries the exact actor
digest and trusted site-owner policy. That policy comes from server configuration,
never request input. Every list and mutation checks the active actor token,
account state and target scope within its SQL operation. Site-admin account
creation uses the same transaction boundary; only trusted startup bootstrap
uses the credential-free SDK primitive.

`GET /api/accounts/<account>/tokens` returns at most 32 ID-ordered entries with
`id`, `scope`, `enabled` and `created_at_ms`. It never returns secrets or digests.
The optional `after` and returned `next_after` are canonical UUID strings.
Continue while `next_after` is present, including a final empty page after an
exact multiple of 32. Pagination observes each page independently; new IDs
inserted before a cursor require a fresh scan. Revoked entries remain listed.

`POST` to that path takes a client-chosen ID, a random `cnp_` secret with 64 hex
digits and scope. Reception admits 8 KiB with a 30-second deadline. Issuance is
immutable: an exact retry of an active record returns 204 without changing its
creation time. Reusing an ID or digest for different fields, or for a revoked
record, returns 409. Initial account-token IDs use the bootstrap/create command's
request ID; token creation time uses that mutation's issued timestamp.

`DELETE /api/accounts/<account>/tokens/<id>` returns 204 for revocation and repeat
revocation by a still-authorized actor. Missing or inaccessible targets return
404. Revoking the site's final active admin token returns 409; read/write tokens
do not satisfy that guard. The decision is recorded before the conditional
update in one Cell transaction, so self-revocation can succeed and concurrent
revocations cannot remove both remaining admins. Current authorization is still
required for every HTTP retry, including after revoking the caller's own token.

Secrets and IDs remain reserved after revocation. Account creation cannot
reactivate one. Token records have no expiry, retention cleanup or issuance
quota yet. Rotation is issue → verify replacement → update clients/deployment →
revoke old ID. A deployment must configure an active owner admin token before
restart; startup rejects a retired configured token instead of recreating it.


### Issues and comments

Issue state belongs to the Repository Cell. `issues` assigns a monotonically
increasing repository-local issue number; `issue_comments` assigns a separate
repository-local comment number and references its parent issue. UUIDs are unique
within each table. Both records retain author, original creation digest, version,
creation timestamp and last-edit timestamp. Issue rows additionally hold title,
body and `open`/`closed` state; comment rows hold body. Comments do not advance the
parent issue's version or edit timestamp. Closed issues still accept comments.
No application state is written to the disposable Git cache.

All issue reads check current Repository Cell membership. Creation permits any
current repository reader; edits permit the record author or a repository writer,
provided that actor still has repository access. HTTP independently requires a
read-scoped token for reads and write scope for every mutation. The SDK takes a
trusted authenticated account assertion, as the existing ACL/ref primitives do.
Authentication happens at HTTP admission; the Cell rechecks membership and edit
authority in the mutation transaction. A token revoked after request admission
may finish an admitted operation; ACL revocation before publication blocks it.

Creation requires a client UUID and stores a length-prefixed BLAKE3 binding of
original author and text. A comment also binds its parent issue. Exact retries
with a fresh runtime mutation identity return the existing number and never reset
later edits, state, timestamps or versions. A UUID used for different original
input returns conflict. The binding is repository-local and remains in SQLite.
No retention cleanup or deletion currently removes these identities. Runtime
replays with an identical mutation identity return the original stored outcome;
HTTP retries authenticate anew and receive a fresh runtime identity.

Edits replace the complete editable fields and compare `expected_version`.
They advance the version once and use the greater of the previous edit time and
the command's issue time. The decision, guarded mutation and result number are
three bounded SQL statements in one Cell command transaction. The pre-mutation
decision is captured before version advancement. Competing edits at one version
cannot both succeed. HTTP does not promise replayable edit responses: after an
ambiguous reply, GET current state before deciding whether another edit is needed.

The HTTP base path is `/api/repositories/<name>/issues`:

| Method/path | Input | Result |
| --- | --- | --- |
| GET base | optional `after` (number), `state` (`open` or `closed`) | `repository_id`, up to 32 `issues` summaries, `next_after` |
| POST base | `repository_id`, `id`, `title`, `body` | 200 with `number` on creation or exact retry |
| GET `/<number>` | none | `repository_id` and complete `issue` |
| PUT `/<number>` | `repository_id`, `expected_version`, `title`, `body`, `state` | 204 |
| GET `/<number>/comments` | optional `after` (comment number) | `repository_id`, up to 16 `comments`, `next_after` |
| POST `/<number>/comments` | `repository_id`, `id`, `body` | 200 with comment `number` on creation or exact retry |
| PUT `/<number>/comments/<comment>` | `repository_id`, `expected_version`, `body` | 204 |

Summaries expose number, UUID, author, title, state, version, `created_at_ms` and
`updated_at_ms`. Detail adds body. Comments expose the same identity/version/time
fields with author and body. Creation digests stay private. Every mutation
requires the expected repository UUID, so rename or name reuse cannot redirect
a stale write to a different Cell. Canonical lowercase UUIDs with an RFC variant
and version 1–8 are required. Missing issue/comment/access returns 404; a current
reader editing another author's record returns 403. Scope failure returns 403;
identity or version conflict returns 409. Invalid JSON/fields/content return 422;
malformed path/query types are rejected by the HTTP extractor with 400.

Titles are nonblank, contain no control characters and use at most 256 UTF-8
bytes. Bodies allow at most 16 KiB and no NUL; issue bodies may be empty, comment
bodies must contain non-whitespace text. The HTTP request limit is 128 KiB with
a 30-second body deadline (413/408). The larger JSON envelope admits escaped
representations without expanding the stored-text limit. Text is unrendered;
any future UI must escape it and sanitize rendered Markdown.

Lists use ascending number cursors, initially zero, with an index for issue
state and an `(issue_number, number)` index for comments. Full pages return the
last number as `next_after`; an exact multiple needs an empty terminal page.
Continue until null. Pages are independent observations: edits/state changes
behind a cursor need a fresh scan. Issue lists omit bodies to bound result size;
comment pages contain at most 256 KiB of body text before wire overhead, below
the runtime's 1 MiB SQL result limit. These are bounded operations, not measured
production capacity. There is no edit history, delete/moderation API, labels,
assignees, attachments, notifications, issue search, or issue UI yet.


### Commit checks

`check_contexts` pins a check name to an account, enabled flag and monotonically
increasing policy version. Only the Repository Cell owner can change it. Version
zero creates a name; later PUTs require the exact version. Disabled names remain
reserved, preventing a disable/re-enable or reporter reassignment from reviving
old results. Enabling requires that the reporter currently belongs to the
repository. Names and reporters follow the 64-byte lowercase component contract.

`check_runs` retains a UUID, commit OID, context and context version, reporter,
state, optimistic run version, summary, timestamps and an internal monotonically
increasing creation number. The OID must identify a stored Git commit; absent
objects, trees, tags and blobs are not check targets. A context does not imply
branch protection; no ref-publication behavior changes in this slice.

Only the configured reporter can start runs. The owner has no implicit reporting
bypass and must explicitly configure itself as reporter if desired. Starting
requires the enabled context's current version and current repository membership.
The UUID binds commit/context/version/reporter. Exact start retries return the
same UUID without updating the row or its creation order; a different binding
conflicts. Retrying after a context change still requires current policy and
reporter authority. An accepted start begins `queued` at version one.

Updates require the attempt's reporter, current repository membership, unchanged
enabled context version/reporter, and an expected run version. Queued and
in-progress attempts can become `in_progress`, `success`, `failure`, or `cancelled`.
Success, failure and cancelled attempts are terminal and immutable; use a new UUID
for a rerun. Every accepted update advances the run version and retains a
nondecreasing edit time. An exact runtime command replay returns its recorded
outcome; HTTP updates use fresh runtime identities and stale versions conflict.

All decisions and guarded writes share one Cell SQL command transaction. The SDK
trusts its authenticated account assertion; the HTTP layer authenticates the
token at admission. Context mutations require an admin-scoped owner token. Run
mutations require a write-scoped token, while the reporter needs only repository
read membership. Membership and context authority are rechecked in the write,
including after receiving a delayed body. Token revocation after HTTP admission
has the same admitted-request boundary as Git/LFS and issue operations.

Commit reads return one entry per currently enabled context. Its run is the row
with the greatest internal creation number for this OID/context/current policy
version. Late callbacks and old start retries never move that order. After a
policy version change, a context without a matching attempt returns `run: null`.
Disabled contexts disappear from commit views but remain in the policy listing;
historical runs stay available by UUID to repository readers. Prior successful
results remain historical data after reporter membership revocation; branch
requirements and their interpretation at publication are not implemented yet.

HTTP routes:

| Method/path under `/api/repositories/<name>` | Request | Response |
| --- | --- | --- |
| GET `/check-contexts` | optional `after` name | `repository_id`, `contexts`, `next_after` |
| PUT `/check-contexts/<context>` | `repository_id`, `expected_version`, `reporter`, `enabled` | 204 |
| GET `/commits/<oid>/checks` | optional `after` name | `repository_id`, `oid`, `checks`, `next_after` |
| POST `/commits/<oid>/checks` | `repository_id`, `id`, `context`, `context_version` | 200 with `id`, including exact retries |
| GET `/checks/<id>` | none | `repository_id`, `check` |
| PUT `/checks/<id>` | `repository_id`, `expected_version`, `state`, `summary` | 204 |

Each context contains name, reporter, enabled and version. Each run contains id,
OID, context, context_version, reporter, state, version, summary, created_at_ms
and updated_at_ms. Mutation UUIDs are canonical lowercase with the supported
RFC variant/version; OIDs use 40 lowercase hexadecimal characters. All writes
carry the repository UUID to prevent stale names from targeting another Cell.
Missing membership/resources/non-commit targets return 404, authority or scope
failure returns 403, identity/version/terminal-state conflicts return 409, and
invalid input returns 422 (malformed extractor input returns 400). Summary text
allows 4 KiB UTF-8 with no NUL. The request envelope admits 32 KiB and a 30-second
body deadline, returning 413/408. Summaries are raw text.

Both policy and commit views return at most 32 context-ordered entries. Continue
with `after=next_after` until null, including a final empty page for exact
multiples. Pages are independent observations. The policy primary key supports
name scans; `(enabled, name)` avoids scanning disabled contexts for commit pages.
The `(oid, context, context_version, number)` run index finds each newest attempt
without scanning attempt history. A full page carries at most 128 KiB of summaries
before metadata/wire overhead, below the 1 MiB SQL result bound. There is no
attempt-history listing, log/artifact upload, runner, event delivery, expiry,
quota, cleanup or aggregate required-check decision yet.
