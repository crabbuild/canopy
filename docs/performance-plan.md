# Repository density and latency

## Required outcome

A node must serve thousands of repository identities, each with its own
Repository Cell, while keeping resource usage bounded and common requests fast.
Ordinary Git objects remain authoritative in SQLite. Large Git blobs and LFS
bodies remain immutable objects in the configured object store. Git smart HTTP
continues to work with stock clients.

Repository count, open Cell count and simultaneous Git work are separate
capacity dimensions. Report all three in every capacity claim. The initial
qualification target is 10,000 repositories on one 8-vCPU, 32-GiB Linux node
with local NVMe and same-region object storage. It is a proposed target, not
measured capacity. Uniform access and concentrated traffic require separate
results; a small active set cannot establish uniform-access performance.

## Current evidence and constraints

- The manager admits three repository entries plus one reserved Directory SQL
  slot. The runtime has one SQL worker. These are qualification limits.
- Ready local routes now pin under a short registry lock. Ownership transitions
  are separately serialized; a paused cold admission or release cannot hold
  the registry lock and block another ready local repository.
- Remote routes still verify authoritative ownership. Authentication and name
  lookup still execute in the Directory Cell; local residency does not bypass
  those authorization boundaries.
- All releases still require Cellule's generation check, settled-work preflight,
  worker close and authoritative owner release before local cleanup. Candidates
  are advisory. Request pins and runtime preflight protect different lifetimes.
- The pinned runtime accepts a bounded shared SQL pool and has a CPU-derived
  `SqlWorkerPool::for_system` constructor. Its 10,000-active-Cell upper bound
  includes the Directory Cell; it is an API limit, not a capacity result.
- The managed SQLite connection requests a 64-KiB page cache. That is not the
  total Cell footprint or a hard process-memory limit.
- Eviction deletes local SQLite state. Git cache rebuilds enumerate all stored
  objects after ref snapshot changes; push also rebuilds its disposable cache.
- Eight shared Git/LFS transfer slots and the bounded Linux profile establish
  containment. They do not establish latency, throughput or thousands of active
  repositories. The profile's tmpfs charges cache bytes to memory; density
  qualification needs a separate bounded NVMe-backed profile.

These facts come from `src/server.rs`, `src/server/residency.rs`,
`src/git_gateway.rs`, `src/git_cache.rs`, `src/repository_http.rs`,
`deploy/compose.yaml`, and the pinned Cellule runtime/worker and LTX/db sources.

## Implementation order and acceptance

### 1. Baseline and request isolation

Deliver a repeatable black-box capacity driver with a fixed seed and a corpus
manifest. Create repositories through the public API, populate them with stock
Git, and exercise metadata, browsing, ref discovery, incremental fetch, clone,
push and LFS. Include empty, small, large-history and large-blob repositories.
Keep fixtures on the dedicated qualification volume and use a disposable store
prefix. Load generation runs off-node. Record the binary and dependency revisions,
Git/provider versions, hardware, network placement and cgroup settings.

Measure cold transition queue time, ownership lookup, restore, initialization,
SQL queue/execution, object hydration, pack generation and durable publication.
Collect RSS/cgroup peak memory, CPU, descriptors, scratch/cache/WAL bytes,
object-store request counts and bytes, errors, queue depths, evictions, hits and
misses. Histograms must distinguish warm and cold paths and operation classes.
Avoid per-repository metric labels; use sampled traces for repository detail.

Acceptance: paused ownership lookup and release do not block an unrelated warm
repository's metadata or stock Git discovery. Concurrent requests for the same
cold repository converge on one serving entry. All pins, denied/lost release,
cleanup failure, cancellation and shutdown tests continue to pass. The lock
isolation part is implemented; the density driver and phase metrics remain open.

### 2. Measured active-Cell admission

Use a CPU-sized bounded SQL worker pool. Select the active-Cell allowance from
measured memory, descriptor and disk budgets, retaining Directory and lifecycle
headroom. Do not simply replace three with ten thousand. Account for native Git
and outgoing streams separately from SQL connection caches.

Replace the serialized cold-transition queue with per-repository activation
coordination only after slots can be reserved atomically. Bound restore
concurrency and queued work at node and account boundaries. Concurrent waiters
share one activation result; canceled clients cannot abandon acquired ownership.
Eviction candidates become unavailable atomically with request pin checks.

Acceptance: runs with 100, 500 and 1,000 active repositories within the same
10,000-repository corpus publish their measured limits. A cold storm and a hot
repository cannot exhaust lifecycle capacity or violate other repositories'
latency budget. Full residency produces bounded backpressure with no overshoot.

### 3. Warm disk state independent of open Cells

Add a runtime-supported close/reopen or validated cache-restore contract.
A retained file is never serving authority. Cache manifests must bind exact Cell
identity, incarnation, schema/release and authoritative root, and reject stale or
corrupt data before serving. Keep ownership fencing and durable publication in
the runtime. Dependency changes need their own review and approval before pinning.

Account for cache bytes and use measured value/size and recent reuse for eviction.
Keep hard safety ceilings and headroom for an admitted push or restore. A scan
across cold repositories must not continually evict the useful working set.

Acceptance: repeated access avoids redundant downloads where cached roots are
valid; changed roots, takeover, corruption, disk pressure and interrupted cleanup
still restore exact Git/LFS contents and reject stale writers.

### 4. Incremental Git object and pack caches

Separate immutable object caching from versioned ref snapshots. A successful
push adds missing objects; a ref update does not rewrite all prior objects.
Keep snapshot/cache generations pinned through native worker completion and
stream consumption. Retain verified objects across derived pack generations;
use native Git pack/index and bitmap reuse where benchmark evidence supports it.
All derived data remains disposable and resource-accounted.

Metadata and ref discovery must not require hydrating every stored object.
Fetch/push preparation must preserve connectivity, object availability and
request-consistent refs. Hidden or unauthorized repository data cannot be exposed
through a cache hit. Preserve branch rules, CAS updates, atomic/mixed outcomes
and exact push-reply replay.

Acceptance: a small commit added to a large warm repository incurs incremental
hydration/write work. Concurrent old/new snapshot fetches, force pushes, deletes,
owner recovery and cache eviction reproduce exact results with strict fsck.

### 5. Routing, bulk traffic and production envelope

Use Cell owner affinity and generation-validated route caching. Never use a
location cache as ownership authority. Measure the Directory separately;
isolate its execution capacity before deciding whether sharding is warranted.
Authorization caches require an explicit invalidation/version-check contract
that preserves token revocation and privacy changes.

Separate metadata, cold restore, Git compute, LFS transfer and maintenance
admission within an aggregate node budget. Evaluate signed object-store LFS
transfers after upload verification/publication is defined for that path.
Any write batching must retain the runtime's durable acknowledgement boundary.

Acceptance: run same-region stock clients through an extended mixed-workload
soak, cold storm, noisy neighbor, SIGKILL/takeover and resource pressure. Restore
acknowledged refs, collaboration and LFS bytes exactly. Record operator-visible
backpressure and recovery time, not only successful throughput.

## Performance qualification rules

Proposed warm metadata targets on the reference node are p95 below 20 ms and p99
below 50 ms, measured by the external client with authentication and response
consumption included. Establish sustainable offered request rate experimentally
and publish it alongside latency and error rate. Use scheduled arrival times to
include queue delay and avoid coordinated omission. Maintain a bounded driver;
count dropped arrivals and timeouts as failures, never omit them from results.

Run skewed and uniform distributions across 10,000 identities, each with 100,
500 and 1,000 active repositories. Vary simultaneous pack workers independently.
Report cold activation latency by database size, local cache state and restore
bytes. Report clone/fetch first-byte latency, throughput, CPU per transferred GiB
and object-store cost. Write latency includes durable acknowledgement.

Publish the failing envelope as well as the passing envelope. No throughput
claim follows from a compile, an in-memory test, or the runtime's maximum Cell
count. The delivery gates in `delivery-plan.md` remain authoritative for the
full hosting service.

## Dependency references

- [SQLite cache size](https://www.sqlite.org/pragma.html#pragma_cache_size):
  suggested page-cache budget; not an aggregate RSS ceiling.
- [Git pack-objects](https://git-scm.com/docs/git-pack-objects): packed-object,
  delta and bitmap reuse.
