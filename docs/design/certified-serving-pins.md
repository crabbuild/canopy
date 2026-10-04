# Certified serving generations and physical drain

Serving readers need an authorized immutable catalog/ref snapshot whose retained
artifacts cannot disappear while an owned worker is suspended. The implementation
adds a bounded serving-pin receiver and an owned metadata-read capability. This
is a foundation for production reader conversion; the production manager does
not yet acquire, renew, cache or hand off these pins to its serving consumers.
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

## Capability construction and admitted reads

`ServingContext` is explicit trusted configuration: real CellClient/target,
actual PreparationAuthority, shared CatalogIndexes/CatalogFiles, shared node
read budget/TaskTracker, and repository administrator identity. Passing decoded
catalog or generation data cannot construct a serving capability. `ServingPin`
opens only after a fresh exact-pin query and actual owner checks. QueryContext
does not expose its target/owner; SQL repository identity scopes the query,
and the service verifies the configured target and fresh actual owner before
artifact I/O. A pin query result alone grants no serving authority.

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

A new owner cannot renew/release old-owner pins merely because its epoch is newer.
They remain roots until actual physical fencing/drain and an authenticated
adoption/release protocol is implemented. Conservatively retaining abandoned
roots preserves correctness but does not establish operational quota recovery.
Process-loss acquisition/renewal discovery is still missing. Do not compensate
with automatic expiry deletion or a synthetic owner fence.

## Production integration and qualification gates

The next serving layer must own exact acquisition and renewal commands, preserve
outcomes across cancellation/process loss, and hand off retained capabilities
before observers can detach. Cache/coalesce a bounded set of active generation
owners per repository rather than allocating a pin per browser/SDE. Carry that
ownership through native work, object bodies and response streams; integrate
its drain into actual eviction and shutdown. A close must join all producers and
workers before Cell/workspace/artifact release.

Regression families exercise Read/public access, joint initialization, original
acquisition replay after release, token scope, revocation, expiry, monotone
renewal, generation reaping, schema quota/identity guards, blocked real provider
I/O, observer cancellation, actual owner restoration, bounded codecs/MAC domains,
and exact release absence/lost acknowledgement/panic. Initialization retention is
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
