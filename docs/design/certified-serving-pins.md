# Certified serving generations and physical drain

Serving readers need an authorized immutable catalog/ref snapshot whose retained
artifacts cannot disappear while an owned worker is suspended. The implementation
adds a bounded serving-pin receiver and an owned metadata-read capability. This
is a foundation for production reader conversion. A service-owned producer now
acquires, retains, renews and drains one generation independently of its callers;
the production manager now creates a bounded resident pool and exposes its
borrow through `RepositoryCell::serving_snapshot`. Local browser and comparison object reads now use this capability. The native
workspace conversion also routes local HTTP/SSH discovery and fetch through
accepted joint refs and their certified forward graph. Remaining generated/write
producers, product consumers and remote routes still require conversion.
The branch remains unreleasable until that conversion and the full cutover gates
are complete.

## Atomic selection and independent retention

`AcquireServingPin` selects the current nonzero `GenerationFact` and inserts its
exact generation retention in one Cellule command. Both catalog and refs must be
present. The request needs current Read access, including anonymous public reads;
it does not grant Write/Admin or create a preparation/artifact namespace. The
pin binds repository, logical reader ID, actual owner incarnation/epoch and
admission sequence. Reusing an existing reader ID is a conflict. Replaying the
same original SDK command returns its original receipt, not a new lease.

The hard cap is 4,096 retained serving pins per repository. Count probes are
bounded, generation lookups are indexed, and the serving table has a generation
index. SQL triggers reject identity changes, replacement, backwards lease
updates and insertion above the cap. `RenewServingPin` requires current Read,
actual owner and an unexpired exact pin. Renewal cannot shorten its deadline or
change its selected generation, even if the current head has advanced.

Lease expiry prevents serving and renewal. It never deletes retention: an old
worker can remain suspended after its lease or owner expires. Existing generation
reaping excludes exactly the serving-pinned generations and retains the existing
preparation floor protection independently. This is SQL fact retention, not a
completed remote artifact collector. The final typed GC/backup/restore inventory
must include these roots and all other reader/backup/recovery owners.

## Exact acquisition and renewal custody

Production acquisition and renewal run through `ReadyServingCommand` and the
existing publication coordinator. Raw commands 44/45 remain domain receivers
for the custody envelope and test fixtures; production does not register them.
Commands 41/42/43 use codec version 2 with fresh schema and MAC domains. There is
no decoder, default purpose or data migration for the old journal format.

Reuse the existing custody intent, SDK snapshot/body, authenticated carrier,
ordered predecessor, recorded phase, first-writer retirement and shared bounded
queue. The primary key is `(purpose, operation, step)`, with distinct creating
and serving purposes. Pending/grant indexes, exact loads, stop authentication and
scanner keysets include purpose. The same logical ID can therefore name one
creating request and one serving reader without joining their histories or
coordinator jobs. An SDK request identity remains globally unique in the journal.
Historical serving grants never restart a creating namespace.

Serving intent registration requires current Read rather than Write. Acquisition
uses the existing BeginRequest identity fields for repository, reader ID, request
digest, service account and requested lease; it allocates no artifact namespace.
Anonymous browsers use a pin owned by an authorized service account and remain
subject to their own fresh Read checks. The exact record is not an anonymous
mutation or an account-authentication shortcut. Renewal keeps the acquisition's
logical account/digest and exact token. The domain write, recorded serving result
and SDK acceptance commit together; late errors or ignored phase writes leave
both the pin mutation and SDK acceptance absent.

The coordinator retains both original registration and execution commands across
absent/lost replies and panics. It uses the shared foreground class with the
custody reservation of 28 KiB; that body does not fit the 8 KiB maintenance
reservation. Dropping an observer or closing the queue does not free retained
uncertainty. Known results release retained ownership before returning credits.
Fresh physical capability construction remains separate from historical receipt
recovery, including after actual Cell owner restoration.

`ServingPin::ready_renew` acquires a physical-drain guard before preparing the
original. The ready value, held admission, dispatch and uncertain recovery share
that same guard. Closing the pin waits until a proven unexecuted held command is
discarded or the exact original reaches a known disposition. Cancellation of an
observer cannot release it. The production owner must retain and activate/discard
held tickets and drive uncertain recovery. `ServingOwner` now performs that
ownership and automatic renewal; the resident pool now composes that lifecycle.

The existing bounded custody scanner also visits serving heads and can retire an
expired unexecuted original. A stop records logical closure and never invents an
execution result or releases an accepted serving pin. Reconstruction APIs select
the serving purpose explicitly; creating staging recovery rejects serving actions
and results. Settled history is still stored in SQL and requires the planned
admitted immutable history frames and exact lookup to bound long-term growth.

## Capability construction and admitted reads

### Owned producer and accepted acquisition handoff

`ServingOwner::start` obtains bounded owner admission before spawning a private
supervisor. The supervisor retains its proposed SDK identity, factory plan,
original prepared command and any held ticket outside the restartable worker.
It activates held originals and resolves uncertainty through their exact
evidence. Registration/execution transport loss, worker panic and caller loss
cannot replace an admitted original with a fresh acquisition or renewal.

Before publishing a successful acquisition to borrowers, the producer calls
`ReadyServingCommand::retain_acquisition`. Only an original local acquisition
can use that handoff; a restored journal command or a renewal cannot. The probe
loads the acquisition's exact ordinal, authenticates its recorded acceptance and
checks its original admission sequence. It then reserves the existing exclusive
physical owner, verifies the still-retained exact SQL pin and historical
generation, and brackets that observation with actual owner checks. A later
renewal must not hide the acquisition ordinal. No absent or denied command is
executed by this probe, and no receipt DTO becomes a physical capability.

Accepted acquisition knowledge remains available after lease expiry or Read
revocation. This permits retention and authenticated cleanup rather than fresh
I/O: every serving operation still checks current access, exact pin, actual
owner and a conservative lease deadline. Cleanup may use the administrator's
bounded physical-read slot after read admission closes. A released, rebound or
missing row fails handoff; duplicate physical ownership is rejected.

`ServingSnapshot` carries a private borrow guard and exposes only the generation
fact and admitted headers, bodies, refs and typed graph pages. Clones share that guard until the last clone
drops. Closing a producer refuses new borrows while existing borrows retain their
generation and continue renewal. Renewal is scheduled at one third of the
conservatively observed remaining lease; this is a scheduling policy, not a
guarantee that an overloaded or fenced owner can renew. Read revocation, expiry
or known renewal denial closes new borrowing. Physical roots remain retained.

After the last borrow, the owner stops producing renewals, joins actual pin
workers and prepares the authenticated exact release. Uncertain releases keep
their original. Only a settled denial allows a new release proof to be prepared
after administrator access is restored. Neither a denied release nor an
observer timeout counts as drain. The last producer handle initiates closure;
`ServingDrainObserver` joins its real completion without keeping admission open.
The supervisor and physical pin outlive detached observers. Loss of authority
can keep cleanup pending; physical fencing/adoption remains mandatory work.

Owner, snapshot and physical-I/O admissions each use the configured 2–64 node
limit and half-cap account share, with separate semaphores. Owner admission
covers the producer's entire lifetime, including a built original before queue
admission. Snapshot admission covers waiting and returned borrow lifetimes.
Long-lived snapshots therefore cannot exhaust the separate physical-I/O slots.
Read budgets must be shared once per node; repository-scoped artifact/index
clients must be shared across the resident's generations. Producer tasks use a
private tracker so their drain can be joined explicitly; production
must stop and join them before closing the node tracker or publication budget.

Thirteen focused lifecycle families pass on macOS/Rust 1.98.0. They cover both
formats, six registrar/execution fault modes, five producer restart points,
borrowed renewal and clone lifetime, deterministic lost-ack/revocation ordering,
denied registration/release, closed read admission, original-ordinal handoff,
independent contexts' physical exclusion, canceled observers and blocked actual
artifact-provider I/O. They qualify this component, not resident pooling,
process fencing/adoption, production reader conversion or large-team capacity.
The owner fixtures use certified initialized empty catalogs and missing-object
lookups. Full nonempty native/body/history reads require their own production
conversion and qualification; suspended real provider I/O proves drain ownership,
not full-repository serving performance.

`ServingContext` is explicit trusted configuration: real CellClient/target,
actual PreparationAuthority, shared CatalogIndexes/CatalogFiles, shared node
read budget/TaskTracker, and repository administrator identity. Passing decoded
catalog or generation data cannot construct a serving capability. `ServingPin`
opens only after a fresh exact-pin query and actual owner checks. QueryContext
does not expose its target/owner; SQL repository identity scopes the query,
and the service verifies the configured target and fresh actual owner before
artifact I/O. A pin query result alone grants no serving authority.

`SelectServingGeneration` (query 48, codec 1) observes the current joint
catalog/ref head under current Read access. Input is bounded at 1 KiB and output
at 512 bytes; repository identity scopes the query and its head lookup uses the
singleton/catalog primary keys. It returns no root before joint initialization.
Selection allocates no pin or creating namespace. Its `GenerationFact` is only
an observation: callers must acquire/check an exact serving pin and verify the
actual owner before artifact I/O. A head advance never changes an existing pin.

Every exact pin also has one process-wide physical owner. A private Arc guard
is reserved before tracked construction and retained by the pin, detached
workers and release proofs. Duplicate construction is refused even through
independent contexts/budgets. Only cloning that same capability shares its drain
counter. Weak entries are pruned under a short synchronous mutex; the process
hard cap is 4,096 live owners, independent of the per-repository SQL cap. No
provider/Cell await runs under that mutex. Guard loss permits reconstruction
only after every previous local owner/worker/proof has dropped; SQL and actual
owner must then be rechecked. This local exclusion registry is not a durable
acquisition ledger or a substitute for process fencing and restoration.

The serving budget is explicit, 2–64 concurrent workers, with the existing
nonwaiting node/account admission and half-cap account share. Production must
create it once for the node and pass clones, not create a new budget per request.
Closing prevents new work; it does not abandon an accepted read. Production must
stop and join admission producers before closing and waiting its TaskTracker.

The implemented read operation returns at most 512 authenticated object headers.
It validates OIDs, obtains admission, increments an owned physical-drain guard,
and spawns through the supplied TaskTracker before yielding. The tracked worker
reobserves current Read/exact pin/fresh owner, opens the actual certified catalog,
performs the existing bounded metadata batch and rechecks authorization and lease
before returning. A conservative local deadline starts before the lease query;
query/provider delays cannot extend the lease. An expired or revoked result is
refused after owned I/O finishes. Cancellation detaches the observer; it cannot
drop the tracked worker, admission or drain guard. There is no timeout that drops
an owned metadata/SQLite future.

The pin retains one lazy reader; configured index/file clients share their bounded
caches across generations. This does not expose raw catalog readers or native
workspace mutation authority. Object bodies, native operations, response streams
and all current object/ref/cache/graph/browser consumers still need conversion.

## Bounded resident generation pool

`ServingPool` retains at most four generation slots, including acquisitions and
closing owners. Pending viewers coalesce on one acquisition; known owners are
matched by their accepted token's generation. Current-root selection is an
admitted, tracked observation under the requesting viewer's Read access. A head
advance between selection and acquisition returns the actual accepted joint
fact, never a capability labeled with the earlier observation. Product consumers
must resolve their revision/ref through that accepted snapshot.

The pool admits each viewer before spawning private request work. Its permit
covers selection, acquisition waiting and the returned snapshot borrow. Observer
cancellation detaches accepted work, and the owner's original stays retained.
At capacity, the pool initiates closure of a least-recently-used unborrowed
owner and observes its independently owned release outside the pool lock. A
request shares a two-second rollover budget and performs at most one retry per
configured generation slot. Concurrent waiters can join an already closing
owner. Each retry selects the current generation under current viewer access;
closing owners cannot accept new borrows. Sequential readers can therefore move
through more than four publications without retrying at the client.

The retiring slot remains charged until the real producer has exited. Timeout,
observer cancellation and a lost release acknowledgement cannot remove its slot
or pin, cancel its exact release, or allocate a fifth owner. All borrowed slots
still produce an immediate capacity error. A slow or uncertain release returns
capacity when the bounded observation expires and remains recoverable. There is
no separate retired-owner inventory. Borrowed old generations remain immutable;
their independent owners keep renewing while other generations work.

Eviction pauses acquisition/borrowing and uses a nonwaiting owner handshake.
The producer driver must be idle, with no pending original, outstanding borrow
or physical pin work. An already built/held/uncertain command is never discarded
by that handshake. Busy refusal resumes the same owners. Only after all owners
are paused may the common coordinator reserve the bounded exact-token drain
gate. Accepted drain is owned by a private task: cancellation cannot abandon
its guard or strand paused owners. Actual releases and producer joins precede
coordinator closure. Repeating quiescence after a later Cell-release refusal
works only after every owner/request has really drained.

`RepositoryManager` owns one 64-slot node serving budget. Each initialized local
resident's `RecoveryServices` owns one pool/context and repository-scoped shared
index/file clients, using the actual NodePeer authority and its existing common
publication coordinator. `RepositoryCell` holds a weak pool association; stale
repository handles do not keep serving admission open. Remote routes receive no
local pool. Partial service construction explicitly joins its pool/scanner before
returning failure. Last pool-handle loss initiates its private supervised drain.

Service registration and shutdown inventory share the manager's loaded-resident
mutex. Shutdown cancels the permanent construction barrier under that mutex
before collecting pools. A constructor registers its lifecycle owner before
exposing the weak repository capability, under the same mutex. A constructor
that loses this race joins its private pool, scanners and exact recovery inside
its already tracked residency task, then returns `CellDraining`. No unpublished
pool can become accessible or escape the shutdown inventory and task join.

Recovery quiescence pauses discovery before pool drain. Server shutdown joins
HTTP/SSH ingress, closes and joins serving pools while Cell, heartbeat and
publication admission remain available, then closes the node task tracker and
drains recovery/Cell/workspace. Node serving admission closes only after pool
drain. The standalone recovery drain also enforces that ordering. A borrowed
snapshot clone or detached real I/O cannot permit early publication-budget
closure, Cell shutdown or workspace reuse.

Six pool and three real-manager families pass as part of 56 focused serving/
resident tests. They cover concurrent viewer coalescing and current access,
canceled cold observation/lost acknowledgement, four borrowed generations and
actual slot reuse, deterministic acquisition head races, busy/canceled exclusive
drain, blocked old provider work with independent other-generation release, weak
repository access and real shutdown retaining publication/Cell/heartbeat/workspace
until the last borrow. These empty/copy-root fixtures qualify ownership and
selection semantics, not native publication throughput or full-history serving.
The third manager case deterministically pauses a constructor before publication,
proves that its public capability is unavailable, starts actual server shutdown,
and verifies that workspace/publication ownership remains until rejected
construction cleanup joins. The regression fails against the preceding weak
association ordering and passes with the registration barrier in both formats.

## Certified immutable ref reads

`ServingSnapshot::resolve_ref` and `refs_page` use only the ref snapshot in the
accepted joint fact. The snapshot descriptor is loaded once per retained pin,
validated against repository, object format and the joint generation, and shared
by its viewers. The resident's existing `CatalogIndexes` also owns the shared
`RefStateIndex`, so unchanged authenticated nodes reuse the same bounded cache.
Public descriptors or a cache hit do not grant Read or retain a generation.

Ref and canonical-header reads share a private admitted worker. Current access,
owner and lease are checked before and after artifact work; expiry or revocation
refuses the result. Observer cancellation detaches that worker without returning
its admission or physical guard. Closing a pool cannot release its pin while a
ref descriptor/index download is still pending. The worker finishes its actual
I/O rather than dropping it to manufacture a timeout/drain result.

The new path reuses `RefPage` and `RefExpectation`. Lookup distinguishes a name
that never existed from a retained deletion with a version. Page size is at most
256 records and 512 KiB of name/record charges. Live cursors skip authenticated
zero-weight subtrees; other consumers can request deletion versions. Continuation
requires the first page's ref generation, and a different selected ref generation
returns an explicit changed result. An old borrowed snapshot continues to read
its old immutable refs after the current head advances. Ref generation is the
snapshot's counter, not the catalog counter; catalog-only changes can share refs.

The local browser's `Refs` and `Resolve` operations select this service. Both the
default branch and its tip come from the same immutable snapshot; legacy `refs`
and `ref_generation` rows cannot override them. The HTTP request ceiling is
512 KiB so even supported long ref cursors with JSON escapes can be submitted.
Names above the index's 65,535-byte limit reject as invalid input. Changed page
generations return HTTP 409. This does not yet convert remote owner routing,
object bodies, tree/file/history browsing, native Git or policy/default-branch
producers; those remain required for the full cutover.

Five new ref families and one actual HTTP/manager family exercise both object
formats: count/byte continuation, tombstones, old-generation immutability, cached
and in-flight revocation, canceled real provider I/O, malformed snapshot context,
and immutable authority despite deliberately conflicting legacy SQL. The ref
inventory fixtures inject roots to isolate reader behavior; their tips do not
qualify native graph publication or capacity. Final frozen-source totals are
recorded in the [implementation status](../large-repository-implementation-status.md).

## Sticky closure and exact release

Closure prevents new reads and waits for every owned drain guard. Notify
registration precedes active-count observation, avoiding a lost final wakeup.
Only after physical drain may the private owner mint a purpose-separated MAC
proof binding tenant/application, exact pin and administrator. The release
receiver checks the MAC, current Admin, actual owner and exact row. Lease expiry
does not prevent a drained release.

Release uses the existing publication coordinator's reserved maintenance share,
with an 8 KiB command reservation and 1 KiB input/128-byte output bounds. A
separate job kind preserves the actual reader ID without colliding with a
creating publication or custody retirement that has the same logical ID. The
owner caches an Arc of the original prepared SDK command. Repeated factories
reuse that original even if the caller supplies a different proposed identity.
Absent/lost-reply/panic recovery resolves its exact evidence and retains credits
through uncertainty. Known outcomes clear the retained command before credit
return; a known successful release permanently closes the pin. A caller cannot
reopen it by replaying acquisition or by cloning an old receipt.

An eviction owner can reserve `ServingDrainAdmission` only while the common
coordinator is idle, after pausing its serving producers and borrows. The
reservation accepts at most 16 distinct exact tokens from that repository and
admits only their privately prepared releases. Matching a reader ID alone is
insufficient: owner, original admission sequence and generation must also match.
Current receiver authorization and physical-drain proofs remain mandatory.
Busy reservation leaves all existing admission and commands unchanged.

The guard closes the coordinator only after every selected token has an observed
successful release and no held, dispatched or uncertain work remains. Denial,
absence and detached observers cannot satisfy this condition. Global queue
closure waits for the guard to finish or be dropped so selected releases can
still be admitted. Guard cancellation resumes ordinary admission but never
cancels accepted work, returns its credits or reopens an already closed queue.
The caller must keep serving producers paused through guard completion/drop.
The resident pool wires this scheduling primitive into production eviction;
production shutdown keeps the node publication budget open until releases finish.

A new owner cannot renew/release old-owner pins merely because its epoch is newer.
They remain roots until actual physical fencing/drain and an authenticated
adoption/release protocol is implemented. Conservatively retaining abandoned
roots preserves correctness but does not establish operational quota recovery.
Exact acquisition/renewal command reconstruction is implemented below. Automatic
production handoff, physical fencing/adoption and abandoned-root quota recovery
remain required. Do not compensate with automatic expiry deletion or a synthetic
owner fence.

## Production integration and qualification gates

The resident pool now integrates `ServingOwner` and its accepted acquisition
handoff with manager residency. Command reconstruction alone does not establish
physical ownership. Native bodies and local browser/comparison reads now carry that ownership.
Remaining transfer producers must carry it through complete response streams. Actual
eviction and shutdown already own pool drain. A close must join all producers and
workers before Cell/workspace/artifact release.

Consumer conversion must preserve these boundaries:

1. `RepositoryManager` owns the node read budget. Each local resident owns one
   repository-scoped context and bounded generation pool, sharing its index/file
   caches. Coalesce acquisitions rather than constructing a producer per viewer.
   Query 48 observes a head; acquisition selects and retains its actual accepted
   fact atomically. A head race must not associate a producer with an earlier
   observation's generation. Product reads bind to the accepted joint fact.
2. Eviction pauses acquisition/renewal producers and new borrows before reserving
   exact drain admission. A pause handshake must account for built/held/uncertain
   originals; merely toggling a boolean cannot establish an idle coordinator.
   Refusal resumes the same owners. One blocked old-generation worker must not
   serialize unrelated live-generation renewal.
3. Shutdown stops borrowing and joins all generation producers while Cell,
   administrator authority and publication admission remain usable. Only then
   may the node tracker/publication budget close and resident recovery/Cell/
   workspace drain finish. The current shutdown path enforces this ordering;
   remaining native/stream consumers still need to carry the snapshot guard.
4. Actual process fencing and restored-owner adoption must precede releasing an
   abandoned pin. A historical lease or an expired deadline is insufficient.
   Quota recovery must use that authenticated lifecycle rather than reaping SQL
   roots based on expiry.

Regression families exercise Read/public access, joint initialization, original
acquisition replay after release, token scope, revocation, expiry, monotone
renewal, generation reaping, schema quota/identity guards, blocked real provider
I/O, observer cancellation, actual owner restoration, bounded codecs/MAC domains,
and exact release absence/lost acknowledgement/panic. Registered acquisition and
renewal families exercise all six registrar/execution transport fault modes,
held/canceled observation, closed recovery, late/ignored atomic rollback,
recorded revocation, cold owner restoration after SDK expiry, both-purpose
page-one scanning, shared pending quota and v2-only bounded codecs. Initialization retention is
retired through its actual registered terminal release so an unrelated floor
cannot conceal a serving-retention bug. Trusted generation/quota SQL fixtures
qualify receiver invariants, not native publication or team capacity.

The current full workspace library still has five failing unconverted `objects`
readers. Production producer/reader/final DDL conversion, admitted immutable
custody history and exact lookup, physical input takeover, scanner restart/fault
campaigns, typed GC/backup/isolated restore, OS CPU/RSS/I/O/PID containment, native
acceleration/physical rewrite/fair maintenance, signed native completion/cold clone,
[file attribution](file-attribution.md), and full Linux/Kubernetes/Chromium plus
10,000-SDE mixed-load/recovery/capacity qualification remain mandatory.

## Certified bounded native body reads

`ServingSnapshot::body(oid, limit)` resolves the object through its accepted
catalog and preferred source before opening native Git. A missing catalog entry
returns absent even when an old cached pack physically contains that OID. The
foreground copy limit is at most 64 MiB; larger objects require a separate owned
streaming producer rather than increasing this allocation without bounds.

Each resident's configured `CatalogFiles` shares immutable native pack copies
across borrowed generations. Sixteen fixed load stripes coalesce equal misses;
at most four completed copies are cached and eight file slots are live,
including borrowed/evicted copies and deferred cleanup. Idle copies can be
removed to make room under the shared disk budget. Fully authenticated pack and
index bytes are reserved before download, and their Git checksum/index binding
is checked before they enter the cache. Whole-pack verification occurs on a
bounded admitted blocking job, once per retained copy.

A native body must match the selected canonical kind, size, Git object ID and
BLAKE3 body digest. Header mismatches and limits reject before allocation. The
batch is poisoned across an incomplete, canceled or invalid read and becomes
reusable only after a complete verified frame. Native process ownership includes
its physical-generation guard and cache reference through descendants and
leader reaping. Blocking creation, writes and hashing also retain their work
owners independently of observers. A cache's file-slot/root owner follows failed
or deferred cleanup; its generation guard is not retained by the idle cache.

Resident configuration uses the node's shared foreground native resource scope.
A metadata-only loader has no implicit native resource pool and refuses body
reads. Native cache statistics expose live/cached files, cache hits and completed
downloads. Authorization and conservative lease checks still run before and
after work through the common serving read contract.

This API is a prerequisite for browser, graph and transfer conversion. Local
browser body/commit consumers now use it; remote routes still require conversion; it is
not evidence of completed native streaming, OS containment, publication or
large-repository capacity. Pack reuse reduces repeated downloads, but each
bounded body currently starts a native batch process. Shared persistent readers
and multi-object batching require separate bounded scheduling and qualification.

A private source pack can omit graph parents that reside in other certified
sources. It is sufficient for verified object-body decoding, but is not by itself
a complete native history workspace for `git log` or attribution. Such producers
must hydrate the required graph through certified source selection and retain
all physical inputs through their owned lifetime. Cold load still reads and
verifies a full pack/index pair; these tests establish reuse and bounded ownership,
not a cold-read latency guarantee for multi-gigabyte packs.


## Certified browser objects and typed ancestry

A local browser tree, file or first-parent history request borrows one accepted
joint generation for its whole view. Annotated tags, commit/tree parsing, sizes
and file previews read only certified headers and verified native bodies through
that snapshot. The selected catalog determines presence even when a reused pack
contains more objects. The public 32-entry pages, literal raw-byte paths, mode
handling and 256 KiB preview bound remain. Wrong-format inputs reject as invalid;
zero or absent commit IDs return missing rather than a storage failure.

Comparison files, previews and patches also borrow one generation for their
object and ancestry reads. `ServingSnapshot::edges_page` accepts one to 128
strictly sorted unique nonzero IDs in the repository format. It returns at most
512 typed edges and headers for the parents visited, including explicit absent
headers. A `(parent, child)` cursor resumes within a parent and then advances to
later requested IDs; it must identify a parent in the same requested set.
Continuation headers may repeat the cursor parent. An exact-full page returns a
conservative continuation and can require one final empty read. These pages are
not ordered Git parent lists; commit bodies retain ordered parents for history.

Edge source selection uses the accepted preferred metadata, never legacy Cell
object/parent tables. Local immutable SQLite edge queries run off the executor
with a child physical guard. The common admitted read checks access, actual owner
and conservative deadline before and after work. Observer cancellation detaches
the tracked worker; generation release still waits for its real provider/query
completion. Edges and headers use the same shared authenticated file/index cache.

Merge-base traversal expands groups of at most 128 certified commits, consumes
all edge continuations, and excludes tree edges by expected kind. It rejects
missing/non-commit roots, including equal tips, and retains the existing bounds
of 100,000 commits and 250,000 parent edges. Best common ancestors are computed on
owned bounded graph data. It starts no native process per traversed commit.
Current pull/review/thread metadata authorization still reads its existing Cell
records and live legacy ref rows; their producer/authority conversion is open.
This checkpoint does not establish a fully converted pull lifecycle.

Production HTTP tests use native packs physically verified into metadata shards,
then trusted installation of the joint catalog fact. They exercise both object
formats, raw paths, directory/history continuation, modes, tags, file previews,
merge comparisons and a 532-parent native merge whose relevant parent is beyond
the first edge page. Suspended-provider and revocation tests qualify edge-worker
ownership and cached authorization. This isolates consumer behavior and is not
end-to-end live producer publication, cold latency or large-team qualification.

## Complete native read workspaces

`ServingSnapshot::workspace` takes a bounded sorted set of certified object roots;
its caller must independently authorize those roots. `ref_workspace` instead
chooses every live ref directly from the retained joint ref index. It streams
32-name pages into admitted native `packed-refs` and the graph frontier, with no
repository-sized Rust ref map or root-count cap. Empty live refs yield an empty
native repository. HEAD uses the same immutable ref snapshot's default branch.

Construction resolves preferred object sources through the certified catalog.
It downloads each distinct, authenticated native pack/index pair once directly
into one unpublished cache and verifies its physical binding. It never decodes
all blobs into loose files. A disposable admitted SQLite spool tracks frontier,
expected types, completed membership and input deduplication. Frontier batches
are at most 128 objects; typed edges and membership lookups are bounded to 512.
Gitlinks do not become graph dependencies. A physical pack may include unrelated
objects, so `contains` and bounded verified bodies use completed graph membership,
not native pack presence. Local HTTP/SSH wants use this membership before Git
receives them. Filters remain validated before native traversal.

The spool reuses admitted SQLite growth: reserve database, journal and overhead
before allowing additional pages, replay only rolled-back transactions, and
refuse growth without retaining a successful prefix. Its maximum and page cache
are explicit. Input deduplication requires identical pack/index digests, sizes
and object count for a shared native checksum; a namespace change alone does not
require a second physical copy.

Long construction retains a producer borrow and exact pin. Bounded steps refresh
lease/owner/access observations at most every 250 ms, and always reobserve before
returning. Valid renewals can carry construction beyond its initial conservative
read deadline; an expired exact pin cannot be resurrected. Ordinary short reads
retain their existing original deadline. Cancellation detaches the observer and
keeps provider jobs owned until real completion.

Construction read admission covers the worker and its blocking/provider jobs.
Completed workspaces release that read credit while retaining snapshot ownership,
physical guards, native file admission and disk reservations. Subsequent reads
acquire fresh read admission. Native subprocess owners retain the workspace
through descendants and physical cleanup, including deferred/quarantined cache
removal. Lease expiry alone is never physical drain.

This is a correct bounded construction path, not a demonstrated hot repository
cache or large-team capacity result. Each transfer currently constructs its own
workspace and downloads selected inputs. Sharing/coalescing authorized generation
workspaces, cold I/O acceleration, fair scheduling and history-sized workload
qualification remain required. Legacy generated/write producers still have
object-table hydration and publication paths to replace.
