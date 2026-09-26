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
| Access token | SHA-256 digest of bearer secret, account, scope (`read`, `write`, `admin`), enabled flag | Directory Cell |
| Repository access | immutable owner identity plus collaborator role (`read`, `write`); only owner has repository admin access | Repository Cell |
| Repository partition | canonical 16-byte UUID, versions 1–8, RFC 4122 variant | `repository_target`, `CellType::entity_uuid` |
| Repository Cell | one SQL Cell per repository UUID | Cellule catalog and authority |
| Local residency | one pinned Directory Cell plus at most three Repository Cells; inactive repositories release ownership before their slot is reused | Repository manager and Cellule transfer preflight |
| Git object format | SHA-1 object IDs from canonical Git type, decimal length, NUL and body | `object_id` |
| Small Git objects | SQLite `objects.body`, maximum 768 KiB | Repository Cell |
| Object publication | at most 128 records and 768 KiB aggregate inline bytes per atomic command | `PutObjects`, operation 5, codec 1 |
| Large Git blobs | immutable `repos/<uuid>/git-blobs/<sha256>` body, SQLite digest/size/reference | `LargeBlobStore` |
| LFS objects | immutable `repos/<uuid>/lfs/<sha256>` body, SQLite digest/size/reference | `LfsService` |
| External byte ceiling | 64 MiB per Git blob or LFS object | current object transfer path |
| Git request admission | 512 MiB for receive-pack, 64 MiB for other requests; 120-second upload deadline | anonymous request spool |
| Ref mutation | check actor's write role, compare expected optional OID and monotonic version; retain deletion records and apply all updates in one Cell transaction | `FinalizePush` |
| HTTP push identity | repository-local UUID bound to account and BLAKE3 request digest; different IDs identify independent operations | `pushes` |
| HTTP push outcome | status, headers and BLAKE3-verified body in SQLite chunks; publish response pointer atomically with accepted refs | `CompletePush`, codec 1 |
| Git connectivity | typed commit/tree/tag edges, required local objects, commit-only branch tips; completed subgraphs cached in SQLite | `object_closure`, shared ref finalization |
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
accepted ref plan. Typed finalization and HTTP completion share this update;
rejected plans, completed-request replay and object/ACL writes do not advance it.
Ref queries return at most 256 rows with their generation in one SQLite
statement, including an empty terminal page. A continuation must supply the
first page's generation. Changes invalidate the scan even when tips return to
their previous OIDs or names are deleted and recreated. The gateway discards
partial scans and tries at most three scans, then returns HTTP 503.
Successful scans therefore describe one coherent ref state. That state can
become older while its disposable cache is hydrated; admitted readers retain
the selected generation, and immutable objects remain readable without GC.

Git owns the [per-ref report and atomic capability](https://git-scm.com/docs/protocol-capabilities#_report_status).
An ordinary push may accept some refs and reject others. The gateway publishes
the actual accepted changes in one Cell transaction before forwarding Git's
report unchanged. A rejected atomic push changes no refs. Malformed packs
retain Git's unpack failure report and publish no refs. The gateway does not
infer transaction success by searching diagnostic text for status fragments.

Both ref commands validate the reachable Git graph in the ref transaction.
Commit trees and parents, tree entries, and annotated tag targets must exist
with the required object kind. Branch tips must be commits. Tree symlinks and
regular files require blobs; gitlinks refer to another repository and do not
require a local object. The traversal follows Git's
[commit headers](https://github.com/git/git/blob/v2.50.1/commit.c#L435),
[tag targets](https://github.com/git/git/blob/v2.50.1/tag.c#L142), and
[tree connectivity rules](https://github.com/git/git/blob/v2.50.1/fsck.c#L334).
It checks graph structure and content hashes; it is not a replacement for all
of `git fsck`'s metadata, filename and portability checks.

An iterative postorder traversal checks inline OIDs and BLAKE3 digests, then
inserts an `object_closure` certificate only after all required descendants
are certified. Repeated pushes stop at existing certificates, while still
checking edge types. Certificates and ref changes commit together; rejection
rolls both back. New objects can be staged in any order, but missing descendants
prevent publication. An external blob's certificate relies on the gateway's
verified immutable upload before recording its SQLite reference; it does not
perform network I/O in the Cell transaction.

Certificates rely on immutable object records and retained object bytes. There
is currently no object mutation or collector through the product API. Any
future collector or repair that removes or changes objects must invalidate all
affected ancestor certificates before ref publication resumes; clearing the
entire certificate table is the conservative implementation. Backup and restore
must preserve the database and its referenced external bodies together. Large
first-time traversals still run in one transaction; capacity qualification and
bounded certification work remain in the streaming/performance gate.

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
sets `receive.denyDeleteCurrent=ignore` because its synthetic HEAD must not
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
its own Git and LFS access. Repeated bootstrap requires the same owner token.
Account disablement, token rotation/revocation, collaborator-visible listing
and audit records remain open.

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
revocation blocks publication. Previously admitted reads may finish after
revocation. A denied upload can leave an unreferenced immutable body; no
collector removes those bodies yet. Account creation is idempotent only for
the same account, token digest and scope; changing bootstrap credentials fails
startup instead of replacing the stored account.

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
Previously published graph closure makes exclusions safe; the Cell transaction
still verifies every new tip.

Candidate existence queries group up to 128 IDs. Missing records accumulate in
an `ObjectBatch` with at most 128 records and 768 KiB of aggregate inline bodies,
leaving room for metadata beneath the 1 MiB operation input limit. External
blob records count toward the record limit; their bytes are verified and
uploaded before publication. The decoder enforces both bounds before copying
bodies. One typed Cell command publishes the batch and returns one receipt.
It recomputes inline Git OIDs and BLAKE3 digests, then compares every inserted
or existing row with the complete expected record. A mismatch rejects the
command and rolls back all inserts in that batch. Repeating the same mutation
identity and payload returns the recorded result through Cellule deduplication.

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
Process/client concurrency and production throughput remain unqualified.
Spooling adds a local-file pass before Git can begin pack ingestion.

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

Schema version 1 is still changing in this unreleased repository. The module
descriptor and object paths will become compatibility boundaries at the first
persistent preview. The current build pins an immutable public Cellule
revision; its UUID partition contract is proposed in
[Cellule PR #5](https://github.com/crabbuild/cellule/pull/5).
