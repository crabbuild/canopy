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
| Local residency | a reserved Directory SQL slot and at most three repository gateway entries, bound to local or remote Cells; inactive local Cells release ownership before reuse | Repository manager and Cellule transfer preflight |
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

`GET /api/repositories/<name>/default-branch` requires repository read access,
including anonymous public access. It returns `repository_id`, `reference` and `generation`.
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

Branches permit deletion by an authorized writer unless an enabled rule denies it. The gateway
sets `receive.denyDeleteCurrent=ignore` because its HEAD must not implicitly
create a branch protection policy. Git's
[receive-pack implementation](https://github.com/git/git/blob/v2.50.1/builtin/receive-pack.c#L1428-L1454)
otherwise rejects deletion of the branch named by HEAD even in this bare
cache. Branch rules are enforced again in the Repository Cell transaction.

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

`GET /api/repositories` accepts anonymous readers or an authenticated read-scoped
token and checks at most 32 candidates per page. Results use UUID order, so rename preserves
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
listing. Directory queries merge owner/UUID, account/UUID and public-candidate
UUID ranges, deduplicating repositories that match more than one range. Pages are not a cross-Cell snapshot:
new grants or repositories behind a cursor require a fresh scan, and each ACL
is observed when that candidate is checked. Cold lists still restore candidate
Cells, so the 32-candidate bound is not a production latency guarantee.

`GET /api/repositories/<name>` first checks for a Directory candidate (or owner),
then the current Cell ACL before returning metadata. Missing and unauthorized
names return 404. It returns `owner`, `name`, `repository_id`, `clone_url`, `role`,
`default_branch`, `ref_generation` and `visibility`. `role` describes effective repository access;
token scope remains an independent restriction. Repository access is checked at
read admission, as for existing Git/LFS reads. Rename and ACL/default-branch
changes across Cells are not presented as one atomic snapshot.

Repository residency is bounded independently of the number of directory
entries. A request pins its loaded repository before using its Cell or Git/LFS
router. That pin lasts through request processing and the complete response
body, including trailers; EOF, body errors and disconnects release it. Membership
changes and repository initialization also retain their route while using it.
The Directory Cell has one owner for authentication and routing; other gateways
use its typed peer client.

### Peer routing

`server::peer::NodePeer` uses Cellule's signed `PeerSigner`, `PeerVerifier`,
`PeerDispatcher` and `CellClient::peer` protocol. Both Directory and Repository
Cells use the same existing commands and queries. The HTTP adapter accepts only
enrolled, live sessions in the configured fleet/image/release. Its authorizer
requires the Canopy node principal, `canopy.cell` action, tenant, application and
one of the two product namespaces before the dispatcher resolves a local owner.
Untrusted request fields cannot choose a destination URL.

Outbound routing reads authoritative Cell ownership and resolves that session's
signed advertisement. The endpoint must match the Cell owner and use HTTPS.
TLS verifies the server certificate and hostname; an optional private CA adds a
trust root. Redirects, environment proxies and HTTP retries are disabled. HTTP
failure after sending, oversized responses and response-body errors preserve an
unknown mutation outcome. Cellule retains the original identity for resolution;
the transport does not issue a replacement mutation.

TLS ingress forwards `/internal/cell` to the node listener. Sender authentication
uses signed requests verified against live enrollment; this adapter does not use
the runtime's separate mTLS certificate-verification helper. The object store,
TLS terminator and enrolled signing keys are trusted fleet infrastructure. A
compromised enrolled node has internal Cell authority, not an end-user role.
Peer ingress admits 32 concurrent requests, bounds bodies and replies by Cellule's
`MAX_PEER_REQUEST_BYTES`, and applies a 30-second body deadline. These are initial
limits, not measured production capacity.

Local dispatch avoids ownership-store reads once the Cell is resident. A lazy
restored Cell is resolved through its catalog/control proof using `local_handle`;
it must not be acquired again just because `resident_handle` is absent. Directory
acquisition is serialized and tracked across caller cancellation. An idle or
expired Directory owner is restored locally on demand using the existing CAS and
node takeover proof. An unavailable live owner returns an error until it recovers
or its lease expires.

Repository gateways bind local handles when they own a Cell and peer clients when
another live node owns it. Remote gateway entries consume cache admission but do
not own local SQLite or release the remote Cell on eviction. When remote authority
is released or expires, the next request drops the disposable binding and uses
the existing local acquisition path. Movement during a multi-call request can
fail that request; there is no distributed request pin or transparent mutation
retry. Runtime command fencing and existing ref/ACL checks remain authoritative.

Current placement is first acquisition, not an automatic fleet balancer. Remote
execution does not remove the resident repository bound, transfer limits or native
resource gaps. Focused HTTPS integration and real RustFS process coverage prove
opposite-owner Git/LFS traffic and survivor takeover after SIGKILL without a
gateway restart. The full placement, partition and publication fault matrices
remain open.

### Residency release

When all repository slots are occupied, the manager selects the least recently
used unpinned repository among Cellule's settled transfer candidates. It calls
`release_idle_cell` with the exact Cell generation and current node session.
Cellule rechecks obligations and admission, closes SQLite and confirms durable
owner release before returning success. Only then does Canopy drop its cached
handles and remove that repository's local SQLite directory. Existing Git
subprocesses separately retain their cache generation until their work ends.
External objects and durable Cell state remain intact. Later access acquires
the idle Cell from its published root and rebuilds the disposable Git cache.

Cellule's pinned `ReleaseIdleCell` handler returns
`Error::Capacity("movement budget")` before transfer preflight when its two-per-second
movement admission is exhausted. Canopy waits one second and retries that exact
release once; the runtime rechecks the generation, node lease and settled work.
This bounded admission wait keeps sequential stock Git clones from failing just
because eviction reached the current rate window. It holds the manager's existing
serialization lock and tracked task; it does not retry a Git or Cell mutation.
Other release errors retain the existing recovery path. An exhausted retry or
fully pinned residency still returns 503 without evicting active work. Failed
release retains local state and invalidates the manager's cached handles. Cellule transfer preflight may replace
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
individual staging/publication boundaries, the full multi-node fault matrix and
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
Managed crash-left directories are reclaimed at startup on Unix as described
below; native scratch enforcement remains a release gate. These reservations are
shared node admission, not per-account durable storage quotas.

Every native Git entry point now uses one process environment policy: HTTP
backend, post-push ref enumeration, incremental object readers and candidate
preparation. Only `PATH` and Windows `SystemRoot` survive from the server's
environment. System/global Git configuration and system attributes are disabled;
replacement objects and outbound Git protocols are disabled. The gateway adds
only its validated CGI fields, or the candidate's recorded author and timestamp.
The server retains its provider credentials for object storage; Git and its
helpers do not inherit them. Home and temporary directories point into an
absolute disposable cache, including when the configured data directory was
relative. Executables selected through `PATH` remain trusted dependencies;
this policy is not a filesystem sandbox or a peak disk limit.

This boundary follows Git's documented
[configuration, object-path and trace environment contracts](https://git-scm.com/docs/git/2.50.0#_environment_variables)
and the HTTP backend's
[CGI contract](https://git-scm.com/docs/git-http-backend#_environment).
Git may add its own child environment, including its resolved executable path;
host values are removed before that initialization.

### Local runtime recovery

`data_dir/.canopy-owner.lock` prevents concurrent nodes from using one local
directory. The server and repository manager retain the lock throughout their
lifetime, including detached request work. `data_dir/runtime-v1/` is private to
the node (created with mode 0700 on Unix) and identified by a version marker.
Startup validates that marker and reclaims local SQLite files, Git caches and
spools before opening Cells or advertising the node. These are disposable copies;
acknowledged state is recovered from object storage. Normal shutdown may leave
local files for the next startup to reclaim.

One supervisor owns node startup, the listener, Cell drain and the workspace.
`CanopyServer::start` waits for its readiness result; cancelling that wait closes
the control channel and requests drain after admitted initialization settles.
Dropping a returned handle also requests drain. `shutdown()` waits for completion,
but cancelling the wait leaves that same supervisor running. The Tokio runtime
must stay alive for cleanup to finish. This does not make runtime destruction,
process death or panics graceful.

Shutdown stops ingress, waits for tracked request work, drains Cellule, then
withdraws the advertisement. If node drain returns an error, Canopy retains its
workspace lock for the rest of the process lifetime: worker closure is unproven.
The same rule applies to rollback after a failed startup. Before the first SQL
Cell acquisition, the workspace marks drain as required. Only successful node
shutdown clears that requirement. Its synchronous destructor retains the lock
descriptor if drain is unconfirmed, including when Tokio destroys the supervisor
during startup or serving. Restarting the process is required before reusing that
data directory; creating another Tokio runtime in the same process is insufficient.

Each native Git command holds a shared lock in its cache. On Unix the descriptor
survives exec and is inherited by descendants. Startup acquires every abandoned
cache's exclusive worker lock before deleting any local state. A worker that
outlives a killed server therefore prevents cleanup with `WouldBlock`; retry
after that worker exits. Cache destruction also respects this fence. Failed or
busy deletion retains both the directory and its current disk charge until node
restart. There is no second unchecked TempDir deletion attempt.

The lock semantics follow [`File` locking](https://doc.rust-lang.org/std/fs/struct.File.html#method.try_lock)
and Unix [`flock`](https://man7.org/linux/man-pages/man2/flock.2.html): locks survive
duplicated descriptors and exec until the final descriptor closes. Executables
and the local filesystem remain trusted; lock files must never be removed or
replaced while a node or its workers are active. Cleanup errors stop startup.
Unrecognized markers and a symlink at the runtime root are rejected. Nested
symlinks are unlinked without deleting their targets. Other files in `data_dir`,
including earlier development builds' anonymous temporary directories, are not
automatically adopted or deleted.

Windows lacks the inherited worker fence in this implementation. If a previous
runtime contains a native worker lock, automatic recovery refuses it; operators
must stop all server/Git processes before removing that runtime directory.
Cancellation during Cell publication and release is qualified with the real
runtime and a paused object store. Destroying Tokio during startup publication
and after readiness is also qualified: a second runtime cannot reuse the same
workspace without confirmed drain. OS power loss, unusual filesystems and Windows
process containment still require qualification. This cleanup does not enforce
native peak disk usage or filesystem allocation overhead.

Schema version 1 is still changing in this unreleased repository. The chunk,
HEAD, discovery, token-metadata, issue, check, branch-rule, pull/review, review-head, merge, candidate and membership-version layouts, operations 7–10,
and the operation-5/8/9 codec changes
require a fresh development storage prefix;
there is no upgrade reader for older development databases. The module
descriptor and object paths will become compatibility boundaries at the first
persistent preview. The current build pins an immutable public Cellule
revision; its UUID partition contract is proposed in
[Cellule PR #5](https://github.com/crabbuild/cellule/pull/5).

### Account disablement

`POST /api/accounts/<account>/disable` accepts an admin-scoped credential of the
configured site owner and no body. Success and repeat disablement return 204.
Unknown accounts return 404; an unauthenticated caller returns 401; an
insufficient scope or a different account returns 403. Disabling the site owner
returns 409 even if that account has multiple admin tokens.

`DirectoryCell::disable_account` checks the exact authorizing token digest,
scope and enabled account inside the same SQL batch that changes
`accounts.enabled`. Its decision precedes the update; revoked admin credentials
cannot mutate state through the SDK even if an earlier HTTP check succeeded.
The operation uses the existing Directory Cell and account column; it does not
need to visit every Repository Cell or update every token.

Authentication joins token state with account state. Every credential belonging
to a disabled account fails subsequent API, Git and LFS admission. A request
authenticated before disablement may finish under the existing repository ACL
rules. Token issuance rechecks account state in its Directory transaction, so a
body admitted before disablement cannot issue a usable replacement afterward.

The account name, token identities, repository grants and authored records remain
reserved. Existing repository data stays available to other authorized accounts;
the collaborator roster continues to show stored grants. New grants to a disabled
account are rejected. Neither account creation nor token issuance re-enables it.
Re-enablement, account deletion, account listing and an administrative UI remain
undelivered. An HTTP retry rechecks current administrator authorization.

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

All issue reads check current Repository Cell read access. Creation permits any
current repository reader; edits permit the record author or a repository writer,
provided that actor still has repository access. HTTP permits anonymous public
reads and requires a write-scoped token for every mutation. SDK mutations take a
trusted authenticated account assertion, as the existing ACL/ref primitives do.
Authentication happens at HTTP admission; the Cell rechecks read access and edit
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
branch protection; an enabled branch rule must explicitly require it.

Only the configured reporter can start runs. The owner has no implicit reporting
bypass and must explicitly configure itself as reporter if desired. Starting
requires the enabled context's current version and current repository read access.
The UUID binds commit/context/version/reporter. Exact start retries return the
same UUID without updating the row or its creation order; a different binding
conflicts. Retrying after a context change still requires current policy and
reporter authority. An accepted start begins `queued` at version one.

Updates require the attempt's reporter, current repository read access, unchanged
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
read access. Read access and context authority are rechecked in the write,
including after receiving a delayed body. Token revocation after HTTP admission
has the same admitted-request boundary as Git/LFS and issue operations.

Commit reads return one entry per currently enabled context. Its run is the row
with the greatest internal creation number for this OID/context/current policy
version. Late callbacks and old start retries never move that order. After a
policy version change, a context without a matching attempt returns `run: null`.
Disabled contexts disappear from commit views but remain in the policy listing;
historical runs stay available by UUID to repository readers. Prior successful
results remain trusted after reporter membership revocation. To revoke that
trust, disable or update the context; branch publication then requires a new
result at the new context version.

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
quota or cleanup yet. Required-check decisions belong to branch publication.


### Exact-branch rules

`branch_rules` stores a version, enabled flag, deletion policy and fast-forward
policy for each exact `refs/heads/...` name. `branch_required_checks` stores up to
16 unique context names per rule. There are no patterns or priority conflicts.
Only the immutable repository owner may replace a rule, with its expected
version (zero for a new name). Enabled rules require all named contexts to exist
and be enabled. Disabled rules can retain names of unavailable contexts. Every
replacement increments the version; disabling never releases a name/version.
The typed `SetBranchRule` command is operation 8, codec 2.

`GET /api/repositories/<name>/branch-rules?after=<ref>` requires repository read
access, including anonymous public access. It returns `repository_id`, `rules`, and `next_after`;
32 sorted rules per page, disabled included. A full final page may lead to an
empty terminal page. Each observation is independent. PUT requires an
admin-scoped owner token and `{repository_id, rule}`. `rule` contains `reference`,
`expected_version`, `enabled`, `deny_deletions`, `fast_forward_only`,
`required_checks`, `require_pull_request` and `required_approvals`. Success is 204; version/context conflict is 409; bad values
are 422. Repository identity is a UUID precondition. The body limit is 16 KiB
with a 30-second receive deadline. The Cell command rechecks owner authority.

The one authoritative `refs::apply_refs` function applies to typed
`FinalizePush`, HTTP `CompletePush`, and `MergePull`. Before any ref writes, each enabled rule
checks deletion policy, ancestry and every required check. For a non-deletion,
the selected attempt is the greatest creation number matching the proposed
commit, context and current context version. It must have state `success` and
the configured reporter. Missing/disabled contexts, missing attempts and all
other states reject. Current reporter membership is required to report results,
but does not retroactively invalidate an already accepted result. Rules apply
to every writer without an owner bypass. A required-PR rule rejects every direct
ref mutation, including deletion and recreation. Otherwise deletion depends on
`deny_deletions`; checks and fast-forward policy govern non-deletions. Branch
creation needs checks but has no old ancestry to prove.

Verified commit objects provide `commit_parents(child, parent)` when graph
closure is certified. Only commit-parent edges enter that table. Immutable
`commit_ancestry(ancestor, descendant)` certificates avoid graph traversal in the
ref transaction. Operation 7, codec 1 accepts at most 128 child/parent steps.
Every step must exist in verified parent links and lead either to the claimed
ancestor or to an existing certificate. Any false step rejects and rolls back
the complete command. Preparation reads parents in pages of 128, follows all
merge parents and publishes proof chunks from the known ancestor outward.
Interrupted preparation may retain positive immutable facts without changing
refs. A collector must invalidate graph and ancestry certificates before deleting
objects or their required chunks/edges. No collector is implemented yet.

Native Git receives an executable, disposable `update` hook. Git's documented
[update-hook contract](https://git-scm.com/docs/githooks#_update) supplies exact
ref/old/new arguments and permits per-ref rejection. The hook verifies those
arguments against decoded request commands, safely quotes ref names, checks
snapshot policy and runs `git merge-base --is-ancestor` where required. Ordinary
mixed pushes retain accepted refs; atomic pushes use native Git's group behavior.
The gateway buffers the report and persists objects before authoritative Cell
publication. A rule/check change after preflight that invalidates an accepted
update rejects the entire proposed publication with HTTP 409; the buffered
success report is discarded. A completed push replay returns its original report
without reapplying refs, even if current rules changed. Already rejected refs
stay rejected if policy becomes permissive during that request; retry normally.

The [receive-pack command list](https://git-scm.com/docs/pack-protocol#_reference_update_request_and_packfile_transfer)
is parsed only when a repository has enabled rules. It admits shallow lines,
first-command capabilities and at most 64 unique updates in a 256 KiB prefix;
pack data after the flush remains native Git's responsibility. Malformed command
input is HTTP 400 and too many updates is 413. Unsupported media types retain
native Git's response. Repositories without enabled rules retain native error
reports, including rejected requests containing more commands than can be
published. Final publication always checks current rules, even if none existed
at preflight. An `(enabled, reference)` index bounds the presence probe; rule and
requirement primary keys and the check-attempt index bound final policy lookups.

Reads, proof commands and final per-ref decisions are bounded. Ancestry search
limits discovered commits to 100,000 and parent edges to 250,000; exhaustion is
an error, never a positive certificate. Retained certificate growth, ancestry
latency, native Git scratch peaks and cross-OS hook execution still need
production qualification. Path policies, tag rules, exemptions and UI remain open.
These new tables require a fresh unreleased development prefix; there is no
backfill path for old object certificates lacking parent rows.


### Pull requests and reviews

`pull_requests` owns a repository-local number, UUID, immutable creation digest,
author, editorial content, open/closed state, draft flag, optimistic version,
fixed source/base branch names, initial commit OIDs and timestamps. Pull and issue
numbers have separate sequences. A pull does not store a mutable copy of current
branch state: reads join the two durable `refs` rows in the same Cell observation.
This avoids fanout writes to every open pull after a push. Retained ref tombstones
expose deleted tips as null without losing their versions. Original commit OIDs
remain available in details. Pulls may share the same source/base pair.

Create binds its UUID to author, title, body, draft and source/base names/OIDs.
Both names must be distinct valid branches, with live, unequal tips matching the
request. The branch publication path already guarantees commit type and graph
closure. A retry with the same binding returns the original number before checking
current tips, preserving later changes. A conflicting binding returns conflict.
Current read access is required even for retries. Edits require the author
or a current writer, current read access and the expected editorial version.
Every accepted edit advances that version, invalidating prior review eligibility.
Neither close/reopen nor draft transitions change Git refs.

`pull_reviews` owns immutable review UUIDs, numbers, original payload digests,
parent pull number, reviewer, grant generation, kind, body and exact revision:
pull version, source OID/ref version and base OID/ref version. New reviews require
an open pull, live unequal source/base tips, and that exact revision in the same
SQL transaction. Comments admit authenticated readers; approvals and requested changes
require a writer other than the author and a non-draft pull. A pull author has
no self-approval exemption, including the owner. An exact review retry checks
current membership and original binding, then returns its historical number;
it cannot create a newer decision. A new UUID must satisfy current eligibility.
Body/identity changes and reusing a UUID for another parent conflict.

History reports `applicable` only for non-comment reviews on the current ready,
open pull and exact pull/source/base versions, with eligible current writer
membership. It selects that reviewer's newest non-comment review by creation
number across all revisions. Comments do not hide an approval or objection.
Source or base ABA cannot revive a decision because retained ref versions change.
Editorial ABA and close/reopen change the pull version. These flags are current
observations, not authorization tokens or merge reservations.

`membership_versions(account, version)` survives removal of a grant. Granting a
new role, changing role, or revoking an existing membership advances that account's
generation in the same guarded transaction as the ACL mutation. Repeating the
same role or removing an absent grant does not advance it. A review records the
generation at submission, and applicability requires it still match. Regranting
or restoring write access therefore cannot revive an old approval. The immutable
owner uses generation zero. Unauthorized ACL calls cannot change generations.
Existing ACL access, roster and check-reporting policy stays the same; check
results continue to use their configured context versions, as documented above.
A fresh development prefix is required; old memberships have no generation
backfill. A future migration must initialize those before enabling these APIs.

All three mutations are bounded guarded SQL batches through the registered Cell
SQL command. Their recorded pre-mutation domain decision, conditional write and
result number share the same transaction. Runtime receipt replay preserves the
original outcome. Application UUID bindings provide HTTP retry semantics across
fresh command identities. Failed domain decisions leave pull/review content
unchanged. The SDK accepts an authenticated actor assertion; HTTP authenticates
before reading the body and the Cell rechecks membership/authority at mutation.
As with issues, already admitted token revocation follows the existing admission
boundary; repository revocation is checked again in the write.

Six HTTP operations live under `/api/repositories/<name>/pulls`: GET/POST the
collection, GET/PUT `/<number>`, GET/POST `/<number>/reviews`. Every mutation
carries the repository UUID, checked against the resolved Cell. Create/review
returns 200 with `number`; edits return 204. Missing access/resources yield 404,
insufficient scope/authority 403, identity/version/revision conflicts 409, and
invalid JSON or values 422. Extractor failures can return 400. Bodies are capped
at 128 KiB with a 30-second deadline. Titles allow 256 UTF-8 bytes without control
characters; bodies allow 16 KiB without NUL. Comment reviews require nonblank
body text. No text is rendered as HTML by these APIs.

Pull lists omit bodies, optionally filter by open/closed/merged state and contain at
most 32 number-ordered summaries. Review history pages contain at most 16 full
reviews, bounding body content to 256 KiB before metadata. Both use numeric
`after`/`next_after`; each page independently observes current state. Pull/state
and review/pull indexes support scans. `pull_review_heads(pull_number, reviewer)`
selects one latest non-comment review per reviewer. New decisions advance this
head in the review transaction; comments and historical retries leave it intact.
The old history index is replaced by these bounded head lookups. Grant generation and ref lookups use primary keys.
No aggregate approval count, mergeability or diff computation runs on these reads.

Fast-forward merge publication rechecks current review requirements, grant/ref/pull
versions and branch checks in the transaction that advances the base ref and marks
the pull merged, as specified below. Required-PR branches reject direct pushes.
Rebase, conflict-resolution UI,
cross-repository pulls, retargeting, review dismissal, deletion/moderation,
notifications remain open. A future collector must retain initial pull
OIDs, historical review OIDs, merged source/base and result OIDs, and merge preparation roots in addition to live refs.
Aggregate retention/quotas and production throughput evidence remain open.


### Exact-revision pull comparisons

`POST /api/repositories/<name>/pulls/<number>/comparison` is a read operation.
It accepts canonical `repository_id`, tagged `target`, and tagged `query`:
`files` with optional `after`, `patch` with `path_base64`, or `file` with
`path_base64` and `side` (`before`/`after`). Read token scope suffices. JSON
rejects unknown fields.
Input is bounded to 32 KiB and 30 seconds; computation to 120 seconds.
Missing membership/pull/review/merge/file returns 404, invalid identity/target/path
422, repository identity mismatch or moved current revision 409, budget
exhaustion 413, computation deadline 504, and unavailable/corrupt data 503.
Unrelated histories and multiple best merge bases return distinct 409 messages.

| Target | Fields | Snapshot |
| --- | --- | --- |
| `current` | `revision`: exact `PullRevision` | Observed editorial version and both live ref versions/OIDs |
| `review` | `number`: positive repository review number | Immutable review revision belonging to this pull |
| `merged` | none | Immutable source/base/editorial revision committed with merge publication |
| `thread` | `number`: positive repository discussion number | Immutable revision retained by this pull’s line discussion |

Current views support open, closed, draft and merged proposals with live refs.
The reader checks the requested editorial/source/base OIDs and retained ref
versions before and after traversal, on every page. Equal current tips produce
an empty list; deleted refs conflict. Historical selectors load stored revisions
instead of trusting supplied OIDs; a review from another pull is unavailable.
Historical views survive editorial changes, ref movement, deletion and recreation.
They do not make a past review applicable or authorize another merge.

Every target rechecks current repository read access after upload and again before
returning the result. Token revocation retains the request-admission boundary.
Responses retain `revision` and computed `merge_base`, identifying the immutable
objects compared. No general per-push history is recorded: saved historical
snapshots are reviews, line discussions and successful merges. Pagination, patches and file previews
use the same target; each query applies its traversal/output budgets.

Comparison uses verified immutable `commit_parents` rows, emitted only by object
closure certification. Union ancestry is read in groups of 128 commits and SQL
pages of 512 child/parent edges. Ancestry propagation identifies common nodes;
removing every proper ancestor of those nodes leaves the best bases. Exactly one
is required. This implements the best-common-ancestor definition in
[Git merge-base](https://git-scm.com/docs/git-merge-base); criss-cross histories
are explicitly ambiguous instead of selecting an unspecified base. A merge may
have more parents than one SQL page. Tests use a 600-parent commit and the last
lexicographic parent to prove traversal reaches the second page.

The tree comparison is merge-base to source, equivalent to a three-dot PR view.
Equal subtrees are skipped. Leaves include blobs, symlinks and Gitlinks;
file/directory replacements expand into leaf additions/removals. Regular-file
modes depend only on the owner executable bit, and directory/link modes use type
bits, matching Git's `canon_mode` in
[object.h](https://github.com/git/git/blob/master/object.h) and its use by
[tree-walk.c](https://github.com/git/git/blob/master/tree-walk.c).
There is no rename similarity detection or attribute-driven text conversion.
Changed-file tests compare full modes/OIDs/raw paths with
[git diff-tree](https://git-scm.com/docs/git-diff-tree) using `--raw -r -z --no-renames`.

Files sort lexicographically by raw path bytes and page at 32 rows. Base64 paths
and cursors use URL-safe encoding without padding and canonical round trips;
paths reject NUL, empty components, dot/dot-dot components and more than 4096
bytes. UTF-8 `path` is null when decoding fails. The canonical path remains in
`path_base64`. A cursor is an exclusive path boundary, not a signed capability.
The same revision must accompany every page. Current implementation recomputes
the bounded graph/change set per page; it has no persistent diff cache.

A file preview resolves its path from the merge-base or source tree; it may read
an unchanged file in that tree too. Symlinks and Gitlinks are not traversed. Blob
metadata is checked first. Up to 256 KiB of verified raw bytes is returned as
URL-safe base64 (`included`); larger blobs return `too_large` without reading
external storage. Gitlinks return `gitlink`, null size and null content. An LFS
pointer remains ordinary Git blob text, with no LFS object download. Existing
object reads verify SHA-1 identity and BLAKE3 bytes for returned content.

Resource bounds per request:

| Resource | Limit |
| --- | --- |
| Ancestor commits / parent edges | 100,000 / 250,000 |
| Individual commit/tree bytes read | 8 MiB |
| Cumulative object bytes read | 64 MiB |
| Cumulative parsed tree entries | 250,000 |
| Changed leaves | 10,000 |
| Traversal depth / path bytes | 128 / 4096 |
| Queued traversal path bytes | 8 MiB |
| File content preview | 256 KiB |

Exceeding a budget fails the whole request. A file preview can still succeed
when listing all changes exceeds its changed-leaf limit. None of these bounds is
a production capacity claim. The endpoint shares the node's eight transfer
permits with Git/LFS. Detached request work, blocking graph/tree workers and
outstanding response frames retain admission until released. Repository residency
is pinned while Cell reads run. Comparisons use SQLite objects directly and do
not hydrate the native bare Git cache. Current no-GC storage makes immutable
object traversal safe; future collection must fence active comparison roots.

Historical merged comparisons persist the pre-merge revision in `pull_merges`;
operation 9 codec 3 commits it with publication. Schema 1 remains unreleased and
requires a fresh development prefix. No dependency or lockfile change is needed.
Fast-forward review policy and atomic publication are implemented below. Native
rebase candidate preparation and line-based review remain open delivery gates.


### Unified text patches

`query: {"kind":"patch","path_base64":"..."}` resolves both merge-base/source
leaves using the same path, revision and current-access checks as file previews.
Directories are absent sides; two absent leaves return 404. Symlink targets are
ordinary blob text. Any Gitlink yields `gitlink`, with no submodule traversal.
Either side over 256 KiB yields `too_large` without loading that blob. Otherwise
verified bytes are classified as `binary` if either contains NUL or invalid UTF-8.
LFS pointers are ordinary text and never dereferenced.

Text uses a bounded [Myers shortest edit script](https://doi.org/10.1007/BF01840446),
with common prefix/suffix trimming and direct insertion/deletion for an empty
middle side. Compact frontier traces retain only reachable diagonals. The result
is a valid minimal line edit script; repeated-line choices need not match Git's
presentation heuristics. No external diff/textconv command, native Git cache,
working directory, attribute policy or new dependency participates in reads.

The `patch` result carries `revision`, `merge_base`, `path_base64`, nullable UTF-8
`path`, nullable `before`/`after` mode/OID entries, `status` and `hunks`.
`status` is `text`, `binary`, `too_large` or `gitlink`. Non-text states have no
hunks. Mode-only changes or empty-file additions/deletions also have empty hunks;
entry metadata carries those changes. Three context lines surround changed runs;
overlapping ranges are combined. Hunk coordinates are 1-based; zero-line ranges
use Git's preceding-line anchor (including zero at the beginning).

Each hunk has `old_start`, `old_lines`, `new_start`, `new_lines`, and ordered
`lines`. A line has `kind: context|delete|add`, `text` with only its final LF
removed, and `no_newline`. CR and all other valid UTF-8 bytes remain unchanged.
An unterminated line sets `no_newline: true`. Prefixing text with space/minus/plus,
adding LF and the standard missing-newline marker reconstructs a unified patch;
integration tests pass these hunks through stock `git apply --check` and `apply`
and verify byte-identical output.

| Patch resource | Limit |
| --- | --- |
| Blob bytes / lines per side | 256 KiB / 20,000 |
| Retained frontier coordinates | 1,048,576 (8 MiB on 64-bit hosts, plus vector overhead) |
| Charged line comparison bytes | 128 MiB, plus bounded prefix/suffix scans |
| Output hunk lines | 8192 |
| Conservative JSON response bound | 2 MiB including reserved metadata/path space |

Line/work/output exhaustion returns 413 for the whole request, never a partial
patch. Blob-size status remains a successful metadata response. The algorithm
runs on a blocking worker that retains the shared transfer permit even if its
caller times out or disconnects. Authorization is checked again after computation.
Graph/object/path bounds and the 120-second comparison deadline also apply.
There is no persistent diff cache; every request computes from verified objects.

### Line discussions

Repository-local `pull_threads` and `pull_thread_comments` store immutable text,
creation UUID bindings, anchors and resolution state. They use the existing
registered SQL batch command: membership/eligibility decision, guarded write and
result number share a transaction. The pinned Cellule SQL implementation executes
statements inside the caller's command transaction and delegates rollback of any
failure to its application savepoint. No runtime operation or codec is added.
The unreleased schema 1 adds these tables; use a fresh development prefix.

`GET/POST .../pulls/<number>/threads` lists or creates threads.
`GET/PUT .../threads/<thread>` reads or resolves/reopens one.
`GET/POST .../threads/<thread>/comments` lists or appends replies. All resource
lookups bind repository and parent pull; comment lookups also bind the thread.
Lists use exclusive numeric `after`, `next_after`, ascending repository-local
numbers and 16-record pages. A thread includes `number`, UUID `id`, `author`,
immutable `body`, `resolved`, `version`, timestamps and `anchor`. Replies have
number, UUID, author, immutable body and creation timestamp.

Thread creation requires repository UUID, a new UUID, `target`, `path_base64`,
`side: before|after`, positive `line` and nonblank `body` (at most 16 KiB, no NUL).
Targets may be current, review or merged. A thread target is read-only and is
rejected for creation. The server runs the bounded verified patch query, confirms
the requested line lies in a text hunk on that side and derives the blob identity.
Context lines are eligible; omitted unchanged lines, binary/large files, Gitlinks,
empty hunks and absent sides cannot be anchors. Neither arbitrary OIDs nor raw
Git paths are accepted as authority. All patch traversal and work limits apply.

The retained anchor has the exact pull/ref `revision`, `merge_base`, canonical
`path_base64`, `side`, `line` and `blob_oid`. The final Cell transaction rechecks
current access and either the live revision tuple or the selected saved review/
merge record. A source/base/editorial change between validation and publication
conflicts for a current target. Stored historical selectors remain immutable.
Object foreign keys retain source, base, merge-base and blob roots; a future
collector must also include these roots and their complete Git closure.

A creation UUID is bound to actor, parent pull, target selector, path, side, line
and original body. A current-access-protected lookup handles exact retry before
revalidating live refs, so a lost response is recoverable after branch movement,
delete/recreate or later thread resolution. The write transaction repeats the
identity check for concurrent creators. A changed binding or actor returns 409.
Replies likewise bind their UUID to author, parent pull/thread and original body.
They do not alter resolution or approval state. Text is immutable, as with reviews;
corrections can be appended. Editing/deletion/moderation is not delivered here.

`PUT` carries repository UUID, `expected_version` and `resolved`. The current
thread author, pull author or repository writer can resolve/reopen with a
write-scoped token. Every accepted update increments the version; stale updates
return 409 and never overwrite newer state. New replies are allowed on resolved,
closed or merged discussions. Resolution is informational: merge admission still
uses canonical reviews, checks and branch rules. No thread is automatically
retargeted or treated as a current approval.

Current repository membership is required for every read, creation/retry, reply
and resolution. Read token scope permits reads; write scope permits participating,
even for a repository read-role member. Resolve authority is narrower as above.
Missing resources/access return 404; insufficient scope/authority 403; repository
UUID, identity or revision/version mismatch 409; malformed or invalid anchors 422;
traversal limits 413. Request bodies use the existing 128 KiB/30-second pull input
limit. New anchor validation shares the eight Git/LFS/comparison permits and a
120-second deadline; overload returns 503 with Retry-After. Detached creation and
response bodies retain admission; blocking diff work keeps it after cancellation.

Comparison `target: {"kind":"thread","number":N}` reads the original revision
with current access checks and pull binding, even after both refs disappear.
The browser highlights the anchored line in that snapshot and links immutable
file views. Line buttons open the composer, and lists/details provide replies
and versioned resolve/reopen controls. Text remains literal. Existing submission
handling preserves UUID/payload on ambiguous replies; edit ambiguity requires
reading current state. Read-scoped tokens expose no mutation controls.

### Required reviews and fast-forward merge publication

An enabled exact-branch rule can set `require_pull_request: true` and
`required_approvals: 0..16`. A nonzero count requires the flag. Disabled rules
retain their version and fields but impose no review requirements. Both fields
are mandatory on rule replacement. Required-PR policy rejects direct pushes,
deletions and branch recreation even if deletion/fast-forward flags are false.
It has no owner bypass. The base branch must exist before enabling that rule.
Rule operation 8 now uses codec 2; this unreleased schema requires a fresh prefix.

`GET /api/repositories/<name>/pulls/<number>/review-policy` returns the current
repository UUID and a `policy` observation. Read scope and membership suffice.
One SQL statement binds editorial state, source/base ref versions, current branch
rule, writer grants and review heads. Its `revision` is null if a ref is deleted.
`ready` means open, non-draft and live unequal tips. `reviews_satisfied` is true
when review policy is disabled, or when the count meets the requirement and no
applicable `request_changes` exists. It does not imply Git mergeability, check
success, or permission for this reader to merge. Missing access/pull is 404.

`pull_review_heads` is a projection updated in the same transaction as a new
review. Its key is `(pull_number, reviewer)`, and it advances only to a larger
non-comment creation number with an exact payload/parent binding and current
membership. Counts aggregate these heads rather than scanning review history.
History flags, policy observations and authoritative publication use one shared
eligibility predicate: ready pull, exact pull/ref versions and OIDs, non-author
reviewer, current write authority, unchanged membership generation, and that
reviewer's latest decision. New comments and old retries cannot reorder heads.
A writer revoke/regrant requires a new review. No team/code-owner policy exists.

`POST /api/repositories/<name>/pulls/<number>/merge` accepts `repository_id`, UUID
`id`, exact `revision`, and `strategy: "fast_forward"`. Unknown fields/strategies,
invalid canonical IDs/OIDs or nonpositive/exhausted versions are 422. A
write-scoped token and current repository writer are required. The body limit is
128 KiB with a 30-second upload deadline. Merge work shares eight node transfer
permits with Git/LFS/comparisons and has a 120-second deadline. Its runtime command
identity lasts 180 seconds, covering preparation plus the ordinary 60-second
publication window; this is within the pinned runtime's 24-hour maximum. SDK
callers choose identities that cover their own preparation window. The tracked task
continues after a client disconnect. A timeout or unavailable result instructs
the client to retry the same request ID and revision; it never promises failure
when publication could have succeeded.

Preparation validates current authority and view before searching verified
commit-parent links. It records bounded positive ancestry certificates; it never
moves refs. Source must descend from base for this strategy even when the branch
rule does not require fast-forward pushes. Non-ancestor or unrelated histories
conflict. There is no synthesized commit, implicit rebase or strategy fallback.

Operation 9, codec 3 publishes the merge in one Repository Cell command:

1. Check current write authority. For an existing application UUID, compare its
   binding to actor, pull number, full requested revision and strategy; an exact
   retry returns its original record before testing today's policy or refs.
2. Require the exact current open, non-draft pull and live unequal source/base
   tips, including retained ref versions. Evaluate current approval count and
   requested changes using the shared predicate and current base rule.
3. Require a verified base-to-source ancestry certificate. Construct a private
   `ReviewedMerge` capability for this exact base update. Only this command can
   construct it; callers cannot opt a general push into merge authority.
4. Use canonical `refs::apply_refs` for current writer authority, ref CAS,
   namespace rules, graph certificates, current required checks and branch
   policy. The private capability only satisfies the required-PR gate for its
   exact update. Other checks remain mandatory.
5. Mark the pull `merged`, advance its editorial version, and insert its unique
   `pull_merges` record with request binding, result OID and timestamp. Ref
   generation advances with publication. All changes commit together.

Success returns 200 and `merge: {id, number, oid, merged_at_ms, revision}`. Missing resources
are 404, insufficient authority 403, and stale/conflicting intent, insufficient
reviews, non-fast-forward history or failed branch policy are distinct 409
responses. Service errors are 503 and elapsed work is 504. State is not edited to
`merged` through the ordinary PUT API; such requests return 422. Merged pulls
reject all editorial edits and cannot reopen. Pull details retain their merge
record even after source deletion; list filtering accepts `state=merged`.

Application retry identity survives new runtime command identities, policy
changes, source deletion, repository rename and recovery. Its binding includes
the actor, so another writer cannot reuse the UUID. Current write authority is
still required. Exact runtime receipt replay keeps the runtime's original
success/rejection semantics. New IDs after a successful merge conflict. Racing
pulls against the same base revision can publish at most one winner.

The runtime transaction contract is verified in the pinned Cellule executor:
application writes are under its savepoint; rejected handlers roll them back
before recording the rejection, and handler errors abort the enclosing SQLite
transaction. A direct-Cell test injects a unique-parent constraint failure after
ref and pull writes, then proves the ref, ref generation and pull state remain
unchanged. Removing only the injected record permits a subsequent merge. Other
tests prove direct `FinalizePush` cannot use approvals as a policy bypass.

The candidate extension below adds merge commits and squash. Rebase, conflict
resolution, merge queues and line review remain open. The owner-loss matrix at
every merge publication boundary and production capacity qualification also
remain open. No dependency or lockfile changed.


### Native merge and squash candidates

Preparation and branch publication are separate actions. A writer POSTs
`/api/repositories/<name>/pulls/<number>/merge-candidates` with `repository_id`,
canonical UUID `id`, exact pull `revision`, `strategy` (`merge_commit` or `squash`)
and a nonblank UTF-8 `message` of at most 16 KiB, with no NUL. Fast-forward is
invalid here. A read-scoped member can GET that path plus `/<id>`. Responses
contain `repository_id`, `candidate` and `fetch_ref` (null until ready). The candidate includes
its original request fields, pull `number`, `actor`, `created_at_ms`, and `result`:

| Result state | Fields | Meaning |
| --- | --- | --- |
| `pending` | none | Intent persisted; retry the original preparation request |
| `ready` | `oid`, `tree_oid` | Certified native result with an immutable fetch ref |
| `conflicted` | `paths_base64` | Native Git reported conflicts; cannot publish |
| `unrelated` | none | Native Git found no common ancestor; cannot publish |

Operation 10, codec 1 reserves the UUID against actor, pull and complete intent.
It retains the first timestamp. Retrying completed preparation returns the
original result, even if the pull later changes or merges; current write access
is still required for POST. GET requires current read access. For pending
work, reservation and completion check the current open, nondraft pull and full
source/base/editorial versions. Reviews and checks may be completed afterward.
A changed intent or actor cannot reuse an ID. Concurrent completions retain the
first result. A pending candidate whose revision moved remains historical;
prepare the new revision with a new ID. There is no cleanup/expiry yet.

A fresh disposable bare cache hydrates verified repository objects. Native
[`git merge-tree --write-tree`](https://git-scm.com/docs/git-merge-tree) computes
the three-way tree, handles renames and consolidates multiple merge bases.
Exit status 1 means conflict even when the conflict path list is empty. Paths
are transported as raw NUL-separated bytes, then encoded as URL-safe base64
without padding. Unrelated histories are detected through `merge-base` exit
status, without parsing localized diagnostics or enabling unrelated merges.
[`git commit-tree`](https://git-scm.com/docs/git-commit-tree) creates a commit
with base/source parents for merge commits and only base for squash. Its author
and committer are the preparing account at `<account>@users.canopy.invalid`;
the reserved time is truncated to UTC seconds, and a missing message newline is
appended. The HTTP caller cannot choose the resulting OID or tree.

The shared native subprocess environment is cleared, preserving only executable
lookup and Windows system root. Global/system Git configuration and system
attributes are disabled. Home and temporary paths point into the disposable
cache, replacement objects are disabled, and protocol access is denied. No repository worktree/index,
user merge drivers, hooks or signing commands are used by preparation. The
native worker is trusted to compute the merge tree; the Cell validates exact
canonical commit bytes and certified graph closure before recording readiness.
It does not independently recompute the merge algorithm.

Generated objects use the same verified SQLite/chunk/external-blob ingestion as
pushes. A ready result and `refs/canopy/merge-candidates/<UUID>` commit in one
transaction, advancing the ref generation for cache invalidation and coherent
pagination. The entire `refs/canopy` namespace, including its root, is reserved.
The authoritative publisher rejects direct creation, replacement and deletion
there; native receive hooks give per-ref rejection reports. Mixed pushes may
still publish permitted siblings, while atomic pushes reject the group. Ready
candidate refs stay immutable after publication, so stock Git and CI can fetch
and test the exact commit before or after a merge.

For publication, `/merge` accepts `strategy: "merge_commit"` or `"squash"` with
`candidate_id`. Fast-forward requires candidate_id absent or null. The command
requires a ready candidate belonging to the same pull, strategy and original
revision, validates its bytes/closure again, and uses its OID for the canonical
ref update. Current approvals and checks still apply; successful source checks
do not satisfy required checks on a synthesized commit. Base-to-candidate
ancestry is certified before publication. The ordinary atomic merge transaction
updates the base, pull and retry record; candidate ID is also part of the merge
retry binding. A stale candidate cannot be rebound to a newer revision.

Preparation shares the eight node transfer permits, tracked disconnect lifetime,
30-second/128-KiB request reception and 120-second work deadline. Native stdout
is bounded to 128 KiB, stderr to 64 KiB, and the encoded result to 256 KiB.
Overflow returns 413 without a partial candidate result. Missing resources are
404, insufficient authority 403, invalid input 422, and identity/revision conflict
409. Conflicted/unrelated preparation is a successful 200 domain result.
Native/service failure is 503; timeout is 504 with an uncertain-result retry
instruction. Cancellation and overflow kill the subprocess group while retaining
its cache lifetime. Completed scratch writes are charged before object ingestion;
native peak disk/memory/CPU bounds and crash-left cleanup remain release gates.

Candidate input/result objects and their ancestor closure are retention roots.
No GC runs today. Abandoned pending rows, ready refs, external orphan objects and
historic candidates need quota/retention policy before persistent public use.
Schema 1 remains unreleased: new candidate tables, operation 10 and operation 9
codec 3 require a fresh development prefix. No dependency or lockfile changes.


## Repository browser

`POST /api/repositories/<name>/browse` is a read operation requiring repository
read access (including anonymous public access), the canonical `repository_id` UUID from discovery and a `query`.
A different UUID returns 409; malformed input returns 422. The response is
`{"repository_id":"<UUID>","view":{"<view name>":{...}}}`. The queries are:

| Query | View name | Contract |
| --- | --- | --- |
| `{"kind":"resolve","reference":null}` | `resolved` | Resolve durable default HEAD; a full ref name selects that ref instead |
| `{"kind":"refs","after":null,"generation":null}` | `refs` | Up to 256 ref records examined in name order |
| `{"kind":"tree","commit":"<OID>","path_base64":"","after":null}` | `tree` | Root directory, or the directory at the raw-byte path |
| `{"kind":"file","commit":"<OID>","path_base64":"<path>"}` | `file` | One blob/symlink/Gitlink entry |
| `{"kind":"history","commit":"<OID>"}` | `history` | Up to 32 commits following only the first parent |

`resolved` contains `reference`, nullable `oid`/`version`, and `generation`.
Default HEAD and its ref tip come from one SQL observation. A missing/deleted
reference returns null OID. Subsequent tree/file/history queries use the returned
lowercase SHA-1 object ID, preserving that snapshot through ref changes.
Annotated tags are peeled to commits, with at most 16 object visits. Tags to
blobs/trees are not supported browser roots and return 404. A certified commit
can be read by members even after it becomes unreachable from current refs;
the browser does not make per-ref access promises. Uncertified staged roots are
not readable. Existing object readers verify hashes and chunk completeness.

Ref pages return `generation`, `default_branch`, live `entries` containing
`name`, `oid` and `version`, and `next_after`. Deleted refs consume examination
capacity but are omitted from entries; empty pages can have a continuation.
Pass both `next_after` and the unchanged generation for continuation. Any ref or
HEAD movement returns 409; restart enumeration. The selected ref observation and
ref-list page are independent; they do not form a transaction across requests.

Tree pages return root `commit` metadata, `path_base64`, directory `tree_oid`,
up to 32 `entries`, and `next_after`. Entries have `name_base64`, nullable UTF-8
`name`, full `path_base64`, `kind` (`tree`, `file`, `symlink`, `gitlink`), canonical
six-digit octal `mode` and `oid`. Ordering is raw basename byte order, without a
directories-first grouping. `next_after` is the last basename, encoded with
URL-safe unpadded base64. Keep the same commit/path when paging. Nonempty paths
use that same canonical base64 encoding, up to 4,096 decoded bytes and 128
components; empty tree path selects the root. Slash, dot/dot-dot, empty and NUL
components are rejected; file paths cannot be empty. Invalid cursors return 422.

File views return peeled `commit`, `path_base64`, nullable UTF-8 `path`, `mode`,
`oid`, nullable `size`, `content_status` and nullable `content_base64`. At most
256 KiB of verified blob content is included. Larger blobs return `too_large`
and metadata without reading external payloads. Gitlinks return `gitlink` with
no bytes/size; symlinks return their target bytes without following them.

Commit records contain `oid`, `tree_oid`, ordered `parents`, optional raw
`author_base64`/`committer_base64`, `message_base64` and `message_truncated`.
Tree and contiguous parent headers follow [native Git parsing order](https://github.com/git/git/blob/v2.50.1/commit.c); later
headers or message/signature text cannot invent graph edges. Author/committer
headers are limited to 4 KiB each, parents to 2,048 per commit. Messages contain
at most the first 4 KiB of bytes and need not be UTF-8. History is explicitly
first-parent, not a topologically sorted traversal of all reachable commits.
`next_commit` selects the next page; clients may follow any returned parent.

Reads check repository access before and after traversal. Token admission follows the
existing Directory-token contract: revocation prevents new admissions, while an
already admitted request may finish. Revoked Repository Cell access fails the
final read check; a revoked grant still permits reads when visibility is public. Missing/inaccessible roots and entries return 404;
resource ceilings return 413; unavailable Cell/data returns 503; the 120-second
work deadline returns 504. Bodies are limited to 32 KiB with a 30-second deadline.
The shared reader limits each object to 8 MiB, aggregate reads to 64 MiB and tree
entries to 250,000. Limits fail the whole view without a partial page. Parsing
runs on blocking workers retaining the request's admission permit. Browse shares
the eight-transfer node limit with Git/LFS/comparisons; 503 includes Retry-After
when all permits are occupied. Reads do not hydrate a bare Git cache.

The embedded `/` interface and `/assets/canopy.{js,css}` are public static
resources. Repository JSON requires bearer authentication. JSON and assets
carry `Cache-Control: no-store` and `X-Content-Type-Options: nosniff`; assets
also carry a restrictive CSP, no-referrer and no-frame-ancestors policy.
Tokens exist only in tab memory, with no cookies/local storage or URL token.
Disconnect aborts work, clears repository content and revokes download URLs;
responses from previous sessions cannot repopulate the new session. Blob
previews use text nodes and downloads use octet-stream object URLs. Raw HTML is
inert. Names escape control/bidirectional characters; non-UTF-8 names display
escaped bytes. The UI never opens repository-supplied links automatically.

This adds no schema/command/dependency change. The canonical verified Git reader
is shared by browsing and PR comparison. Complete DAG history, syntax
highlighting, large-file streaming downloads, rendered Markdown
and production browser/capacity matrices remain outside the current browser.


### Issue collaboration interface

The Issues tab uses the existing issue/comment routes and mutation contracts;
there is no second publisher or browser-owned issue state. Repository discovery
now returns `viewer` with the authenticated `account` and `token_scope`, alongside
the existing repository `role`. These are observations for choosing UI controls;
HTTP and the Cell still independently authorize every operation. The UI checks
that issue/comment read responses match the discovered repository UUID before
combining them. Name reuse cannot display a different Cell under stale metadata.

Open/closed/all lists retain the API's 32-issue pages. Details retain 16-comment
pages. New comments navigate to the position before their returned repository-wide
number, making the confirmed post visible even beyond the first page. Authors
with repository read access and a write-scoped token may edit their own records;
repository writers can edit others. Read-scoped tokens have no create/edit/reply
controls. Close/reopen uses the same expected-version issue editor. Text uses
text nodes with preserved whitespace, without HTML/Markdown interpretation.

A form freezes its creation UUID and exact title/body after submission. Pending
fields are read-only. An ambiguous network/server reply retains that UUID and
payload for **Retry submission**; no automatic retry, new identity or changed
payload is generated. Even a later permission rejection cannot thaw an already
uncertain submission. A definite initial validation/permission rejection lets
the user correct the form. Versioned edits are never silently rebased: conflicts
or uncertain replies keep a copyable draft and require loading current state.
The UI reports publication only after a successful API reply, then reads it back.

Drafts and pending identities live only on the current page. Navigating away,
reloading or disconnecting discards them; the form explains this lifetime.
An aborted request may already have published and must be checked in current
state. This UI does not provide offline draft storage or cross-session retry
recovery. Creation UUID generation requires a secure browser context (HTTPS,
or loopback for local development). UTF-8 byte validation follows API limits;
server checks remain authoritative.

`/assets/issues.js` and `/assets/issues.css` share the static asset CSP and cache
policy. The script is loaded after the base interface, reuses its authenticated
request/session cancellation and navigation helpers, and exposes one issue-view
entry point. No frontend dependency, schema or operation codec changes.

### Pull-request collaboration interface

The Pull requests tab uses the canonical pull, review, comparison, candidate,
check and merge APIs. Lists use 32-record pages with open/closed/merged/all
filters; reviews use 16-record pages. Creation chooses existing local branches
from generation-bound ref pages and submits their observed OIDs. Editorial
changes use expected versions; merged requests cannot be edited. Drafts can
receive comments, but cannot receive approval/change decisions or be merged.
Authors cannot approve or request changes on their own request. Read-scoped
tokens have no mutation controls. The server and Cell authorize every action.

Each review and merge intent freezes the pull editorial version, source/base
OIDs and ref versions. Review applicability and policy snapshots are checked
against the displayed revision. The interface never replaces a stale intent
with current tips. Candidate responses include the resolved repository UUID,
allowing reads to reject name reuse before combining state from different Cells.

Changed files are paged and compare the selected source against its merge base.
Merged requests default to the stored pre-merge revision. Each review links to
its historical comparison; its header states the review number, pull version and
source/base OIDs. The separate branch header explicitly labels live tips.
Opening a file reads one unified patch through the exact-revision comparison API.
Text is rendered with text nodes, old/new line numbers, addition/deletion prefixes,
missing-final-newline markers and visible `␍` for carriage returns. The horizontally
scrollable diff region is keyboard focusable; both immutable file views are linked.
Binary, large-file, submodule and empty-hunk states are explained. Non-UTF-8 paths
retain their base64 representation. Renames appear as deletion/addition.
Line discussions are accessible from diff line numbers and their own paged tab.

Merge preparation is a separate action from publication. A ready candidate
exposes its file view, immutable Git fetch ref and check results for its exact
OID. Pending preparation can be resumed by its creator with the same ID and
intent. Conflicted/unrelated/stale candidates offer no publication control.
Clearing a candidate is explicit; an unusable candidate never switches the
publication form to fast-forward. Required review failures suppress publication;
the server rechecks all current branch/check policy when publishing. The check
list includes enabled contexts and their latest runs, and does not identify the
required subset. Missing required checks produce a visible publication error.

`discussion.js` now owns shared issue/pull field validation, paging, identity
checks and form submission. UUID submissions retain exact payloads after an
uncertain reply; versioned edits retain copyable drafts and require reload.
All drafts/pending identities remain page-local. `pulls.js` and `pulls.css` share
the existing authenticated session, cancellation, literal text, CSP and no-store
asset behavior. No new publisher, schema, operation codec or dependency.


## Public visibility and anonymous readers

`ReadIdentity::Anonymous` binds SQL NULL; `ReadIdentity::Account` binds a validated
account. No magic account name represents an anonymous caller. Read queries use
the shared owner/member/public predicate. Mutation methods retain account
identities, and final Git/LFS publication still requires explicit Write access.
Public discussion comments may carry membership version zero; comments never
qualify as approvals, and approval eligibility requires a current writer grant
or ownership. HTTP mutations always require a valid write-capable credential.

`ref_generation.visibility` defaults to private. Owner-only `set_visibility`
checks the expected generation and changes visibility in one SQL transaction,
incrementing the generation. Exact SDK mutation retries return the original
receipt; a new request with an old generation returns false, including after a
private/public/private cycle. HTTP PUT validates the repository UUID and generation,
returns 409 on stale state, and requires the caller to reread after an uncertain
result. Visibility changes invalidate ref-generation-bound caches and cursors.

The service first publishes an owner-authorized Directory public candidate,
then changes Repository Cell visibility. Failed or stale changes can leave
candidates. Candidates survive privatization, avoiding a deletion racing a newer
public change. Discovery always rechecks the Cell. Direct SDK `set_visibility`
is a local primitive; SDK coordinators must publish the candidate first for
service discovery. Candidate GC, quotas and index rebuilding remain open.

Missing Authorization means anonymous. Supplied malformed, revoked or disabled
credentials remain authentication failures; they never fall back to public
access. Public metadata includes `visibility` and a null `viewer` for anonymous
readers. Public read admission includes Git/LFS and collaboration queries;
rosters and all mutation routes retain authenticated authorization. Privacy
changes deny new read admission but do not cancel already admitted responses.
Successful API data responses and Git/LFS responses use `Cache-Control: no-store`;
downloaded data cannot be revoked.

Anonymous LFS download actions omit Authorization headers and set
`authenticated: true`. Under the [Git LFS Batch API contract](https://github.com/git-lfs/git-lfs/blob/main/docs/api/batch.md),
that flag says credentials have already been resolved; false or omitted asks
the client to discover credentials. It does not claim an account is logged in.
Stock `git lfs pull` without credentials verifies this behavior.

Both Directory and Repository initialization schemas changed in this unreleased
build. Use a fresh development prefix; existing data is not migrated or removed.
Source digest validation rejects old modules; mixed-build rolling upgrades are
not supported. Production upgrade migration remains a delivery gate.

## Deployment release enrollment and maintenance

`Deployment` uses the pinned Cellule `ApplicationIdentityStore` and `ReleaseStore`.
The authoritative prefix binds tenant/application IDs before nodes enroll. On a
fresh catalog, the selected descriptor is the registry's exact canonical release
bytes, addressed by its BLAKE3 digest. Initial Prepared → Activating → Ready
transitions share a deterministic bootstrap operation; concurrent initialization
and process loss can adopt only that exact release/image. A catalog that already
contains Cells with no release record is rejected. Existing preview data has no
automatic upgrade path. The configured image digest is an identity assertion,
not binary attestation.

Node startup checks Ready before enrollment, again after publishing its signed
advertisement, and before exposing HTTP. Provisioning uses `ReleaseStore::provision`
to check the descriptor and Ready/activation contract before and after catalog
publication. The release watcher shares the three-second lease-renewal loop.
Missing, unreadable, non-Ready or mismatched release selection closes HTTP
readiness and requests the existing supervised shutdown. Accepted work can drain;
lease renewal continues while accepted HTTP work drains. Final CellNode shutdown
cancels its task group and applies runtime lease fencing while closing SQL;
advertisement withdrawal follows the shutdown attempt. Unsettled controls still
prevent maintenance completion if that attempt fails.
The executable observes supervisor completion as well as OS stop signals.

Maintenance administration prepares the same compiled descriptor and enters
Maintenance with a caller-supplied UUID. It does not select new code or modify Cell
contents. Calls with the same UUID resume/replay the operation; another UUID is
rejected while a transition is pending. Replaying begin after that operation
completed returns the Ready record and cannot close admission again while that
UUID is still the current operation. The release record retains only its latest
operation; administrative callers must not replay older UUIDs after a subsequent
operation starts. Durable operation history and retention remain open.

Drain observation verifies the persisted identity and descriptor, enumerates
unfenced advertised sessions (including expired records), and scans all 256
catalog shards. A catalog entry is settled only with an unowned Idle control
containing a root, or an unowned Tombstoned control. Missing controls, unpublished
roots and Serving/Recovering states remain unsettled. The release must be
unchanged after the scan. Only a stable Maintenance record with zero advertised
sessions and zero unsettled Cells reports `drained: true`; Ready never does.
Session enumeration is bounded at 4,096 and fails closed on overflow. Maintenance
end requires the exact operation and drain proof, then uses the upstream release
CAS. Retrying completed end returns its unchanged Ready record.

The upstream contracts are implemented in pinned Cellule revision
`56b35ab376ff93ec85d502c70bb436c918463958`, specifically `application.rs`,
`release.rs` and `node.rs::advertised_sessions`. `start_maintenance` closes release
admission but does not itself drain writers; Canopy supplies that lifecycle.
Heartbeat expiry is not writer-close evidence. Source store errors propagate;
there is no fallback to an unregistered release.

This does not implement an upgrade controller or force resume. A node killed
before releasing its Cells remains unfinished until fenced recovery completes.
Backup remains separate work: Cellule `BackupPinStore::create` validates a Ready release
and requires participation in maintenance drain, while its restore covers only
runtime objects. Canopy still needs a backup coordinator enrolled through capture,
external blob/LFS manifests and copies, operation retention, destination fencing,
and verified restores from an isolated backup. Maintenance alone is not backup
or recovery proof.


## Recovering interrupted maintenance

`Deployment::recover_maintenance` requires the exact active Maintenance operation
and compiled release before opening its workspace, again after enrollment, and
before each Cell. Its short-lived signed node has a ten-second lease, refreshed
every three seconds while recovery runs. The worker has no HTTP listener, uses
one SQLite worker slot, and restores into disk-accounted local scratch protected
by the same process/worker exclusion as a serving node.

The worker first enumerates advertised sessions and calls the pinned runtime's
`NodeDirectory::claim_expired_for_takeover` for each other session. That API
validates the live claimant, fences the exact expired advertisement with ETag
CAS, and refuses active follower logs. It also handles a node that died before
acquiring any Cells. Existing tombstones are retained; competing, unexpired
recovery claims remain conflicts rather than being overwritten.

A bounded scan of the catalog then skips already settled controls. Each remaining
entry must match a known Canopy namespace and the current registry descriptor.
Missing controls bootstrap from the published catalog proof. Unpublished owners
use `takeover_unpublished`; published owners use `takeover_restored`, which verifies
the authority-pinned root and materializes attached recovery through Cellule.
The same acquisition implementation serves normal node startup and maintenance;
only normal acquisition provisions new catalog entries through release admission.
Recovery consumes existing catalog proofs and does not bypass a Ready gate to
serve user requests.

Each acquired handle drains before its local restore directory is removed.
The supervisor completes node shutdown before withdrawing the recovery worker's
advertisement. Caller cancellation does not cancel that supervisor. Failed drain
keeps local exclusion until process exit, and errors are logged even if the caller
has disappeared. A killed recovery worker itself leaves an expired enrollment
and/or unfinished controls for another enrolled worker to fence after the relevant
lease/claim expires. No TTL alone means a writer is closed.

Recovery does not call maintenance end. An acquisition or root verification error
is returned without changing the release to Ready. `drained` describes writer
closure, not data integrity: it is not an fsck or backup-verification result, and
operators must resolve a reported restoration error before resuming. CLI logs go
to stderr so successful administration output remains a single JSON status object.

Crash qualification at every individual publication/claim boundary, long restore
lease failures and production-store power loss remain part of the fault matrix.
