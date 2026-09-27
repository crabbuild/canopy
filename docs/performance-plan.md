# Repository density and latency

## Required outcome

A node must serve thousands of repository identities, each with its own
Repository Cell, while keeping resource usage bounded and common requests fast.
Ordinary Git objects remain authoritative in SQLite. Large Git blobs and LFS
bodies remain immutable objects in the configured object store. Git smart HTTP
continues to work with stock clients.

The expanded objective also requires KV, queue, workflow, Blob, Cron, Timer,
durable effects and projections in each same repository Cell. The current
SQL-only benchmark is a baseline; complete capability integration and mixed
primitive workloads are required before density qualification is complete.
See [the repository Cell capability proposal](repository-cell-primitives.md).

Repository count, open Cell count and simultaneous Git work are separate
capacity dimensions. Report all three in every capacity claim. The initial
qualification target is 10,000 repositories on one 8-vCPU, 32-GiB Linux node
with local NVMe and same-region object storage. It is a proposed target, not
measured capacity. Uniform access and concentrated traffic require separate
results; a small active set cannot establish uniform-access performance.

## Node design and resource model

Use three residency states independently of repository identity:

| State | Retained resources | Request path |
| --- | --- | --- |
| Cold | Durable Cell identity, roots and external bodies | Acquire fenced ownership, restore and activate |
| Cached | Verified local database/object files under a disk budget; no open SQL handle or per-repository worker | Validate authority and cache generation, then reopen through the runtime |
| Active | Cell handle, bounded SQL state and request/background pins | Route to the owning worker; enqueue within its admission budget |

The cached state is a proposed runtime contract, not implemented behavior.
Closing a SQL handle, releasing Cell ownership and deleting cached files are
separate decisions. Every transition must account for foreground streams and
primitive obligations. A waiting workflow is durable state; runnable work and
active leases require lifecycle checks before the Cell can sleep.

Share SQL workers, native Git workers, object-store connections and scheduler
runners across repositories. Do not allocate a thread, connection pool, polling
loop or independent timer task for every repository. Use indexed durable due
work and a bounded scheduler that activates only due Cells. Queue and workflow
state stays inside the repository's Cell; the node scheduling index must be
reconstructible and reconcile missed updates after crashes.

Derive active admission from measured resource consumption:

```text
node memory = fixed runtime + active Cell state + Git workers
            + transfer buffers + pending work + filesystem/cache charges

active limit <= min(memory allowance / measured Cell allowance,
                    descriptor allowance / measured Cell descriptor count,
                    runtime active-Cell limit minus lifecycle headroom)
```

This is a sizing model, not an enforcement mechanism: allocations still need
reservations and the deployment needs aggregate memory/process/disk ceilings.
Use representative high-water measurements with all primitive schemas installed;
SQL page-cache settings alone do not measure a Cell. Reserve separate capacity
for Directory requests, lease renewal, durable publication and shutdown.
Bound disk cache and restore scratch independently. Define per-account admission
so one repository scan cannot monopolize the entire cold activation budget.

Warm request latency should consist of authentication, routing, a short SQL
operation and response delivery. Measure each term before introducing caches.
Durable writes must include the runtime's publication work before success;
object-store round trips therefore need a separate write-latency target.
For bulk Git and LFS, report first-byte latency and throughput separately from
metadata latency. Reuse immutable Git objects and pack data across ref changes;
serve ref discovery without a full object hydration where the wire protocol
permits it.

## Current evidence and constraints

- The required `max_active_repositories` setting admits 1–9,999 repository
  entries plus one reserved Directory SQL slot. The example uses 100; the initial
  density run and fault fixtures use three. The runtime uses its CPU-sized SQL
  worker pool, capped at sixteen.
  Thirty-two supervised cold/remote transitions may execute or wait; excess
  admission receives 503. Ready local routes bypass that queue. These are
  qualification limits, not measured production capacity.
- Ready local routes now pin under a short registry lock. Ownership transitions
  are separately serialized; a paused cold admission or release cannot hold
  the registry lock and block another ready local repository.
- Ready repository activation verifies its immutable owner using a read-only
  query; only pending repositories publish owner initialization. This avoids a
  redundant durable command on restore or remote binding. The initial density
  measurements below predate this optimization.
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
isolation part is implemented. The initial density driver below measures metadata
and Git v2 discovery, with stock Git sampling and full identity recovery checks.
Transition queue/service timings are logged. Other workload classes, comprehensive
resource metrics and independent load-generator deployment remain open.

### 2. Measured active-Cell admission

The CPU-sized bounded SQL worker pool and explicit node residency limit are
implemented. Set `max_active_repositories` to 100, 500 or 1,000 for each qualification
run; use a separate node workspace for each. Select the active-Cell allowance from
measured memory, descriptor and disk budgets, retaining Directory and lifecycle
headroom. The setting enables those measurements; a larger configured count alone
is not evidence of sustainable capacity. Do not simply replace three with ten thousand. Account for native Git
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


## Running the initial density driver

`scripts/benchmark_repositories.py` authenticates using the existing
`CANOPY_GIT_TOKEN` environment variable. Point it at a dedicated Canopy instance
and disposable storage prefix. It creates private repositories; it does not
remove them. Keep generated client checkouts on the mounted qualification volume.
The provider fixture owns storage cleanup, separately from the benchmark.

```sh
python3 -B scripts/benchmark_repositories.py --base-url http://127.0.0.1:8080 \
  --manifest "$HOME/Workspace/crabbuild-target/canopy-density/corpus.json" \
  seed --repositories 1000 --populated 3 \
  --work-dir "$HOME/Workspace/crabbuild-target/canopy-density/seed"
python3 -B scripts/benchmark_repositories.py --base-url http://127.0.0.1:8080 \
  --manifest "$HOME/Workspace/crabbuild-target/canopy-density/corpus.json" \
  run --active-repositories 1000 --distribution uniform --operation metadata \
  --rate 10 --duration 30 --concurrency 32 \
  --output "$HOME/Workspace/crabbuild-target/canopy-density/uniform.json"
python3 -B scripts/benchmark_repositories.py --base-url http://127.0.0.1:8081 \
  --manifest "$HOME/Workspace/crabbuild-target/canopy-density/corpus.json" \
  verify --work-dir "$HOME/Workspace/crabbuild-target/canopy-density/restored"
```

Create the report parent directory first; seed/verify require fresh work
directories, and measurement refuses existing output files. For another
checkout/run, use a different qualification directory. `verify` may target a
new node URL after owner takeover. It checks every identity and clones each
populated sample with Git v0/v2, exact commit/file hashes and strict fsck.
The default sample contains three one-commit repositories; the remainder are
empty. This is a density smoke corpus, not realistic large-history qualification.
An interrupted seed leaves an explicitly incomplete manifest and cannot be used
as a complete corpus. Names have a unique prefix; the manifest records them.

Run `--operation refs` for Git v2 discovery or `--distribution skewed` for 90%
of requests to the selected working set's first tenth. A fixed seed determines
working-set selection and offered arrivals. The driver never calls a working set
warm automatically: its count is not the server's resident count. Prewarm a set
that fits the node before claiming warm latency, or label the run as cold/mixed.

Each worker reuses an HTTP connection. Arrivals follow a fixed clock schedule;
end-to-end latency starts at the scheduled instant and includes driver dispatch
delay and complete response consumption. A bounded semaphore limits outstanding
requests. Saturation records `driver_busy` instead of accumulating an unbounded
client queue. HTTP errors and transport failures are recorded without retries.
`--timeout` bounds individual socket operations; it is not a whole-request
deadline. All outcomes go to a sibling `.samples.jsonl`; the JSON summary includes counts,
error totals, scheduled/service/dispatch latency percentiles and the manifest
SHA-256. `started_at_utc` anchors the run to server logs; each sample's
scheduled offset is `sequence / offered_rps`, while latency continues to use the
monotonic clock. Dropped arrivals have no fabricated zero latency. Any failure yields
exit status 1 after reports are written. Latency percentiles include completed
errors; always read them alongside error/drop counts.

The driver unit test runs a real HTTP server that deliberately rejects requests:

```sh
python3 -B -m unittest discover -s scripts -p test_benchmark_repositories.py -v
```

It verifies concurrency bounds, complete scheduled-outcome accounting, absence
of retries, queue-delay inclusion and credential exclusion from the report.

## Initial 1,000-repository read measurements

The `34aa904` production code was exercised against RustFS
`1.0.0-beta.8-glibc` on the shared Darwin arm64 development host, with client,
server and provider colocated. Artifact ID: `canopy-density-8003fccfc019`.
The corpus contains 1,000 private repository identities: 997 empty repositories
and three one-commit Git samples. The server admits three repository Cells plus
Directory. These results qualify this small SQL-only read workload; they do not
establish the proposed Linux production envelope or full primitive capacity.

| Workload | Offered rate / duration | Outcomes | Scheduled latency p95 / p99 |
| --- | --- | --- | --- |
| Metadata, three prewarmed repositories | 20 requests/s / 15 s | 300 successful, no failures | 11.076 / 11.334 ms |
| Git v2 discovery, same resident set | 10 requests/s / 10 s | 100 successful, no failures | 342.510 / 681.942 ms |
| Metadata, uniform choices across 1,000 identities | 10 requests/s / 15 s | 4 successful, 86 HTTP 503, 41 transport errors, 19 driver drops | 5011.409 / 5012.561 ms |

The Git row starts with warm Cells, but Git caches were not explicitly prewarmed.
It cannot be described as a pure warm Git-cache measurement. Percentiles include
completed errors; the uniform row's low median must not be read as successful
service latency. Read socket timeouts were five seconds and driver concurrency
was eight for the first two rows and thirty-two for uniform pressure. No measured
request was retried. A uniform choice distribution over the corpus does not mean
every identity was visited during this short 150-arrival run.

Sequential seeding took 1,233.833 seconds including the three sample pushes.
Repository creation p50/p95/p99/max was 856.472 / 3345.383 / 4587.310 / 9441.362 ms.
Setup used a thirty-second socket timeout. Slow creation is measured behavior,
not evidence of an optimized write path.

Conclusion: warm metadata is fast at the tested modest rate, while broad cold
access fails the service target under the tested three-Cell limit and serialized
transitions. Prioritize measured active admission, bounded concurrent activation
and retained validated local state. Larger realistic corpora, sustained load,
independent clients, resource accounting and all-primitive workloads remain
required.

The same run subsequently passed owner SIGKILL, lease expiry and fresh-workspace
recovery of all 1,000 identities. All three populated samples reproduced exact
commit/file hashes under stock Git v0/v2 and passed strict fsck. Graceful shutdown
and fixture cleanup succeeded. This proves recovery of the SQL-only identity
corpus with three resident repository slots; it does not prove 1,000 simultaneously
active Cells or all-primitive recovery. `binary-source.json` binds the measured
binary to `34aa904`; the fixture's end-of-run Git HEAD includes later work and is
not the binary source revision.


## Configured 100-Cell process qualification

A development build of `d03f3cb` ran with `max_active_repositories: 100` against
RustFS `1.0.0-beta.8-glibc`; artifact ID `canopy-active100-4a68afc585f3`.
The corpus contained 100 identities, with three one-commit Git samples. The
client/provider/server were colocated on the same shared 12-logical-CPU macOS
arm64 development host. This run verifies configuration and recovery, rather
than qualifying the reference Linux performance target.

| Read workload | Rate / duration | Outcomes | Scheduled p95 / p99 |
| --- | --- | --- | --- |
| Three prewarmed repositories, metadata | 20 requests/s / 15 s | 300 successful | 14.074 / 14.479 ms |
| Same resident set, Git v2 discovery | 10 requests/s / 10 s | 100 successful | 108.097 / 147.904 ms |
| Uniform choices over all 100 resident repositories, metadata | 10 requests/s / 15 s | 150 successful | 14.149 / 14.314 ms |

No measured request failed or was retried. Git caches were not prewarmed before
the discovery row. Seeding took 29.330 seconds. After owner SIGKILL and lease
expiry, a fresh local workspace verified all 100 identities and cloned all three
populated samples using Git v0/v2, exact hashes and strict fsck. Shutdown and
fixture cleanup succeeded. Sampled parent-server RSS peaked at 108,544,000 bytes;
child Git processes, provider/client memory and kernel cache charges are excluded.

An earlier attempt of the same development build stopped after 30 creations
with HTTP 503 and pending Directory command evidence at approximately five
seconds. Its underlying delay remains unresolved. The successful repeat's
recorded LTX publication lag peaked at 547 ms during seeding/reads. Those timings
do not explain the earlier failure, whose runtime publication tracing was off.
The small corpus, short schedules, different build profile and active-set sizes
prevent a controlled performance comparison with the initial 1,000-identity run.


## Diagnosing metadata latency

Enable `RUST_LOG=warn,canopy_server::server=debug,cellule_runtime::actor=debug`
for an isolated qualification run. Canopy emits `repository request stage completed`
with `stage`, `elapsed_seconds` and `succeeded` for Directory authentication,
Directory repository lookup and repository metadata. The metadata stage includes
route acquisition plus role/default-branch/visibility reads; it is not solely SQL
execution. Authentication success here means the operation completed, not that a
credential was authorized. No token or token digest is included.

Join the benchmark's UTC start and scheduled sample offsets with those events
and the runtime's publication/compaction timing events. If a latency spike is
confined to client dispatch, it is a driver issue; if authentication stalls across
otherwise unrelated repositories, investigate the shared Directory path and its
worker/storage waits. Timing correlation alone does not establish the cause of
a runtime fence. Preserve errors and publication evidence before changing
scheduling, deadlines or cache policy.


## Initial 1,000-active-Cell read results

The optimized `58eb0b5` run, artifact `canopy-active1000-119a2ac48d4f`, configured
1,000 repository slots and 4 GiB of local disk admission. It seeded 1,000
identities (997 empty and three one-commit samples) against RustFS on the same
shared macOS host. Runtime publication/compaction timing traces were enabled.
All 550 scheduled read requests succeeded without retries or driver drops:

| Workload | Rate / duration | Scheduled p50 / p95 / p99 |
| --- | --- | --- |
| Metadata, three prewarmed repositories | 20 requests/s / 15 s | 6.471 / 11.550 / 25.857 ms |
| Git v2 discovery, same resident set | 10 requests/s / 10 s | 50.386 / 104.543 / 235.143 ms |
| Metadata, uniform choices over the 1,000 resident identities | 10 requests/s / 15 s | 11.070 / 782.653 / 1382.356 ms |

Uniform arrivals 18–31 waited 174–1,469 ms in HTTP service while dispatch delay
remained near 10 ms. Their completion pattern points to a shared wait, but the
specific stage is unproven. The p95/p99 target is not met across this working set.
Git discovery again started without explicit Git-cache prewarming. The read
schedule sampled the corpus; it did not visit every identity.

A parent-process sample during uniform reads observed 8,052 numeric file
descriptors and `vmmap` physical footprint of `457.4M` (reported peak `457.6M`).
These are macOS process observations, not aggregate node budgets: child Git,
provider/client memory and kernel charges are excluded. They demonstrate why
SQL's page-cache reservation alone cannot size the node.

Pinned runtime inspection identifies a candidate for further diagnosis:
`coordination.rs` marks a Cell busy during quiet compaction, blocking new SQL;
`actor.rs` considers compaction after 250 ms of quiet. A long compaction can
therefore add query queue time. The current log events do not identify the Cell,
and timing correlation has not proved this caused the measured spike. Worker
queue/page-I/O contention is another candidate. The new Canopy request-stage
traces and UTC benchmark anchor must be exercised before choosing a fix. This
run predates those traces.

All 1,000 identities and three Git samples subsequently passed recovery checks,
but graceful shutdown returned `Runtime(Fenced)`. The process run therefore failed
its full acceptance gate. Logs report unconfirmed Cell drain and retained workspace
exclusion; the isolated fixture then cleaned up its processes/container/volume.
Source inspection found that Canopy put lease renewal in Cellule's task group,
which is cancelled before runtime drain. With a ten-second node lease, a long
drain can lose the authority it still needs. The next run must verify the corrected
renewal lifetime as well as request-stage latency.
