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
- Eviction deletes local SQLite state. Git snapshot refresh scans bounded object
  metadata pages but reuses verified immutable bodies from a repository-scoped
  cache. Each native push/merge has a private writable generation; successful
  publication precedes hydration of its new objects into the shared cache.
- Eight shared Git/LFS transfer slots and the bounded Linux profile establish
  containment. They do not establish latency, throughput or thousands of active
  repositories. The profile's tmpfs charges cache bytes to memory; density
  qualification needs a separate bounded NVMe-backed profile.
- The Linux profile sets both process descriptor limits to 16,384. Its checker
  requires eight per admitted Repository/Directory Cell plus 1,024 headroom;
  this is a reservation check, not measured aggregate process capacity.

These facts come from `src/server.rs`, `src/server/residency.rs`,
`src/git_gateway.rs`, `src/git_cache.rs`, `src/repository_http.rs`,
`deploy/compose.yaml`, and the pinned Cellule runtime/worker and LTX/db sources.

## Idle ownership cost

The pinned runtime also has a density cost independent of user traffic.
`cellule-runtime/src/publication.rs` schedules each idle active publisher for
renewal three seconds after its previous successful renewal or publication.
`CellPublisher::renew` advances Cell control progress through
`CellAuthority::transition`, which performs a conditional object-store update.
`actor.rs::start_due_renewals` bounds concurrent renewals at 32; it still scans
and renews individual active Cells. Coordination pauses new SQL for that Cell
while renewal is in progress.

For 1,000 otherwise idle active Cells with short provider latency, this implies
approximately 333 control updates per second; 10,000 would imply approximately
3,333. These are scheduling estimates, not measured provider request counts.
Runtime scheduling, provider latency, writes, compaction and retries change the
actual rate. The Directory and node advertisement add their own work. Shared
SQL workers therefore do not, by themselves, establish cheap idle residency.

Add a no-traffic interval at 100, 500 and 1,000 active Cells. Measure conditional
updates, reads, bytes, CPU and renewals in flight, then compare with released
Cells retaining only validated disk cache. Count failures and retries separately.
Before optimizing, audit whether existing node-session fencing can safely replace
redundant per-Cell liveness writes while preserving Cell epochs and root CAS.
Canopy's takeover already requires an expired-session takeover proof, but every
runtime recovery, routing, maintenance and standalone caller must satisfy the
same contract before changing renewal policy. This is dependency design work,
requires approval, and cannot be implemented by simply increasing a timeout.

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

Immutable body reuse is now wired through shared Git alternates and private
snapshot directories. Old fetches retain their snapshot and object owner; native
write failures cannot contaminate the reusable objects. Refresh still scans all
object metadata. An indexed change cursor and derived pack/bitmap reuse remain
open, as do representative large-history throughput measurements.

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

The node HTTP boundary now emits a debug `http_request` span containing a
canonical UUID and the matched route pattern. The benchmark sends `X-Request-ID`
and records the same `request_id` with each attempted arrival. Invalid or absent
correlation values receive a generated UUID; they never affect authorization.
Raw headers, URLs and query strings are not recorded. No trace UUID is allocated
when debug tracing for this boundary is disabled.

`HTTP handler started` and `HTTP handler completed` bracket routing middleware
through response creation; completion records status and elapsed seconds. This
includes awaited authentication and repository operations but excludes response
body streaming and time before the middleware is polled. Compare per-request
handler duration, nested stage duration and client service duration to distinguish
those gaps. Synchronous logging can itself add delay; instrumentation is diagnostic
and its overhead must not be treated as application execution time.

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


## Traced Directory stall with 1,000 active Cells

The optimized `fccf1de` rerun, artifact `canopy-active1000-fixed-1a6092e5a8ba`,
seeded all 1,000 identities in 156.768 seconds. Its first read gate failed:
253 of 300 warm metadata arrivals completed successfully; 47 were dropped by
the bounded eight-slot driver. No request was retried. Scheduled completed-attempt
p95/p99 was 32.541/2638.993 ms. The test stopped before Git discovery, uniform
reads, recovery or graceful shutdown, so this run does not verify the drain fix.
Its process and provider fixture were cleaned up; failure provenance and logs
were retained.

The trace narrows the stall to Directory lookup. Eight lookups took
1,901–1,903 ms and completed at 03:41:54.990–54.992 UTC on 2026-09-27, immediately
after a quiet compaction reported completion at 03:41:54.989849 UTC with a
2,085 ms duration. Directory authentication stayed below 8 ms and recorded
repository metadata stages below 9 ms. These are stage timings, not whole-request
latency: the slow HTTP attempts took 2,388–2,728 ms, so the traced lookup does not
account for the entire wait. Logging, scheduling and other uninstrumented time
remain possible contributors.

The pinned runtime marks a Cell busy while quiet compaction owns the publisher,
and its scheduler blocks queries as well as commands while busy. The existing
upstream actor test explicitly proves commands wait during compaction. This
contract plus the trace supports investigating read admission during compaction;
the event lacks a Cell identifier, so timing alone does not prove which Cell
compacted or fully explain the stall. A dependency fix needs a controlled paused-
compaction query test, fencing/snapshot/publication proof and approval before
changing the pin. Caching authorization or weakening publication deadlines is
not justified by this evidence.

Recovery and lifecycle checks should run independently of latency pass/fail.
The next fixture retains every read failure in its overall result while still
executing SIGKILL recovery and the final graceful drain. This separates verification
of the lease-lifetime fix from the unresolved read-latency gate.


## Independent read and lifecycle gates

The next optimized `fccf1de` run, `canopy-active1000-evidence-1f3d0e0b5a45`,
keeps read failures in the process outcome while continuing independent recovery
and drain checks. It uses the same 1,000-identity corpus shape, 1,000 active slots,
4 GiB local disk allowance, colocated RustFS and shared macOS host. The uniform
read schedule is extended to sixty seconds; this is not a controlled comparison
with earlier fifteen-second runs.

| Workload | Rate / duration | Outcomes | Scheduled p50 / p95 / p99 |
| --- | --- | --- | --- |
| Three prewarmed repositories, metadata | 20 requests/s / 15 s | 300 successful | 8.563 / 11.595 / 11.737 ms |
| Same resident set, Git v2 discovery | 10 requests/s / 10 s | 100 successful | 40.008 / 46.478 / 49.684 ms |
| Uniform choices across 1,000 identities, metadata | 10 requests/s / 60 s | 600 successful | 11.139 / 11.477 / 677.364 ms |

All 1,000 scheduled reads succeeded with no retries or driver drops. Uniform p99
still misses the proposed 50 ms target; its maximum was 1,276.822 ms. During
uniform reads, thirteen HTTP attempts waited 70–1,274 ms in service. Twelve
Directory authentication stages finished together after approximately 106 ms,
immediately following a 105 ms quiet compaction. Directory lookups and repository
metadata remained below 6 ms. Most of the longest HTTP wait is therefore outside
the measured stages. Request ingress/scheduling and synchronous trace output
need separate timing or profiling before claiming compaction explains the full
stall. This repeat does not erase the preceding warm-read failure.

A sample during uniform reads observed 8,052 numeric file descriptors and a
`vmmap` physical footprint of `461.8M`. Both are parent-process observations,
excluding child Git processes, provider/client resource use and kernel caches.
The benchmark is SQL-only and mostly empty, not full primitive capacity proof.

After SIGKILL and lease expiry, a fresh workspace recovered every one of the
1,000 identities. All three populated samples passed stock Git v0/v2 clone,
exact commit/file hashes and strict fsck. Graceful shutdown completed in
10.316 seconds with exit status zero. Neither server log contains warnings
or errors, and fixture process/container/volume cleanup completed. Sampled
parent RSS peaked at 608,337,920 bytes across 754 samples; this is not an
aggregate memory ceiling.

The frozen source revision and optimized binary SHA-256 are retained with the
outcome. The fixture's successful overall status means its functional read,
recovery and shutdown assertions passed; it does not enforce a latency SLO.
Uniform p99 remains a failed performance target, and the earlier traced warm
read failure remains relevant. The twelve-second paused-release regression
and this process run together qualify the corrected renewal lifetime for the
tested SQL-only workload.


## Synchronous logging stall confirmed

The correlated `2a96b43` run (`canopy-request-trace-9b929b9401af`) completed
seeding 1,000 identities in 742.122 seconds. Warm metadata had 273 successes
and 27 driver drops; Git discovery had 100 successes. The two-minute uniform
schedule had 1,162 successes, 20 driver drops, 11 HTTP 503 responses and seven
transport failures. Graceful shutdown and fixture cleanup passed; the overall
read qualification failed. All failed outcomes remain in the reports.

A warm metadata request took 845.281 ms in the HTTP handler while its three
data stages totaled 3.886 ms. Correlated timestamps place the remaining time
between successive log events. A thirty-second macOS stack sample during uniform
reads shows HTTP task stacks in tracing's synchronous `Stderr::write_all`, both
waiting for its mutex and writing to the destination. This confirms that the
diagnostic sink can block service execution. It does not establish logging as
the only source of Directory stalls or explain every 503. Sampling perturbed
part of the uniform schedule; the shared host and longer setup also prevent a
controlled throughput comparison with prior runs.

The implementation moves diagnostic writes off HTTP/runtime threads through
`tracing-appender`'s bounded lossy writer, retaining its drain guard until the
application exits. The queue has 256 records; it is not a byte-memory ceiling.
Logs may be dropped under pressure, so trace-join coverage and the available
drop counter must accompany subsequent analysis. Durable audit data stays in
SQLite. The existing Cellule pin, ownership checks and request deadlines remain
unchanged.

The real-process paused-pipe proof passed (`canopy-logging-pressure-12a0c96e3dc7`,
optimized `ef7c968`). Consumption stopped for 12.053 seconds, exceeding the
ten-second node lease. All 600 HTTP requests succeeded: readiness checks with
every tenth request verifying repository metadata. Git v2 discovery and graceful
shutdown also passed. The logger reported 806 dropped records, proving saturation.
Combined service p50/p95/p99/max was 0.224/1.085/3.131/3.744 ms. This controlled
one-Cell test proves isolation from the blocked pipe; it is not a thousand-Cell
latency result or a metadata-only percentile.

Repeat against a caller-owned disposable provider prefix with the existing test
credentials in the environment and a fresh directory on the qualification volume:

```sh
python3 -B scripts/smoke_s3_logging.py \
  --binary "$CARGO_TARGET_DIR/release/canopy" \
  --storage-url s3://qualification-bucket/logging-proof \
  --work-dir "$CARGO_TARGET_DIR/logging-pressure-run1"
```

The script owns and cleans up its server process. The caller owns provider-prefix
cleanup. Reports and server logs remain in the chosen work directory.


## Thousand-Cell repeat with nonblocking diagnostics

The optimized `78fef9d` run, `canopy-buffered-logging-bc264a7287ff`, seeded
1,000 identities in 77.247 seconds with 1,000 resident slots and a 4 GiB local
disk allowance. The same mostly-empty SQL corpus shape and colocated RustFS
provider were used on the shared macOS host. No sampler ran during this repeat.

| Workload | Rate / duration | Outcomes | Scheduled p50 / p95 / p99 |
| --- | --- | --- | --- |
| Three prewarmed repositories, metadata | 20 requests/s / 15 s | 300 successful | 6.695 / 11.312 / 11.461 ms |
| Same resident set, Git v2 discovery | 10 requests/s / 10 s | 100 successful | 43.224 / 61.301 / 67.875 ms |
| Uniform choices across 1,000 identities, metadata | 10 requests/s / 120 s | 1,200 successful | 2.576 / 10.303 / 11.608 ms |

All 1,600 arrivals succeeded without retries or driver drops. Every attempted
request matched a completed handler trace. Uniform handler p99 was 4.879 ms;
client service time minus handler time had p99 1.248 ms. Uniform scheduled max
was 22.569 ms, including dispatch delay. The earlier hundreds-of-milliseconds
logging gaps did not recur. Server logs contain no warnings or errors; graceful
shutdown and fixture cleanup passed. Sampled parent RSS peaked at 470,302,720
bytes across 225 samples, excluding other processes and kernel charges.

The stalled-sink regression, blocking negative control, real paused-pipe test
and stack profile establish the logging fix independently of this noisy-host
comparison. This repeat meets the proposed warm metadata percentile values at
the tested rates, but it does not qualify the reference Linux node, sustainable
maximum throughput, realistic Git histories or the full primitive set. The last
recorded compaction completed before the read schedules; read admission during
compaction remains an unqualified runtime contract. Crash recovery was not
repeated in this focused diagnosis run; its prior proof remains separately
recorded. Idle ownership cost, broader resource enforcement and mixed-workload
qualification remain open.

## Bounded Linux density qualification

The repeatable `scripts/benchmark_container.py` harness materializes the checked-in
deployment profile, verifies actual cgroup/mount/descriptor limits, and keeps
read, crash-recovery and graceful-shutdown outcomes separate. See the
[invocation and report contract](../deploy/README.md#measure-repository-density-under-these-limits).
It retains reports and incomplete corpora on failure; measured requests are
never retried. The default run still uses mostly empty SQL-only repositories.

The first Linux attempt (`canopy-linux-density-faa843f4e0cf`, optimized source
`c0a5be4`) stopped after creating 124 identities. The next creation returned 503;
the server reported SQLite I/O failure `Too many open files`. The engine's default
soft/hard descriptor limits were 1,024/524,288. Before failure the sampler observed
716 aggregate descriptors and a cgroup memory peak of 84,590,592 bytes; its
five-second interval did not capture the descriptor peak. No read schedule,
recovery or explicit graceful-shutdown assertion ran. Fixture cleanup passed.

The deployment now declares soft/hard limits of 16,384 and validates the actual
process limits plus active-Cell headroom. The repeat uses the identical production
binary and declared workload, changing the deployment descriptor contract.
No dependency pin, runtime ownership rule or request timeout was changed.

The repeated read workload (`canopy-linux-density-a46bc3c8bb61`) seeded all
1,000 repositories in 37.487 seconds. It ran on Linux aarch64 in an 8-CPU,
16,732,602,368-byte Docker VM. Canopy had two CPUs, 4 GiB memory, no swap,
256 tasks, 16,384 descriptors per process and 2 GiB tmpfs. RustFS
`1.0.0-beta.8-glibc` ran in the same VM with its own two-CPU/4-GiB cap; the client
ran on the macOS host. The image ID is
`sha256:310b66ded54aa36a5f759795152a6d3e1daefbec7372a2c1acb9f4c3d23036cd`;
the report binds its `c0a5be4` source label to the executable SHA-256.

| Workload | Rate / duration | Outcomes | Scheduled p50 / p95 / p99 |
| --- | --- | --- | --- |
| Three prewarmed repositories, metadata | 20 requests/s / 15 s | 300 successful | 14.075 / 28.914 / 65.727 ms |
| Same resident set, Git v2 discovery | 10 requests/s / 10 s | 100 successful | 34.740 / 55.681 / 74.012 ms |
| Uniform choices across 1,000 identities, metadata | 10 requests/s / 120 s | 1,200 successful | 10.907 / 25.439 / 45.214 ms |

Every scheduled read succeeded without retry or dropped arrival. The warm
metadata row misses the proposed p95/p99 values; uniform metadata meets the
proposed p99 value but misses p95. Uniform service p99 was 35.163 ms; scheduled
latency also includes host-client dispatch delay. This smaller tmpfs profile
does not establish the proposed reference node's SLO or maximum throughput.

After SIGKILL, lease expiry and recreation with fresh tmpfs, all 1,000 identities
were verified. All three populated samples passed stock Git v0/v2 clone, exact
commit/file hashes and strict fsck. The recovered node drained with exit status
zero in 1.198 seconds. Both server logs contain no warnings/errors; container,
provider and fixture cleanup passed.

Forty-one resource samples recorded a kernel memory high-water mark of
902,483,968 bytes (about 861 MiB), including descendants and tmpfs, a sampled
maximum of 8,072 descriptors, 16 processes/threads and 336,891,904 scratch bytes.
There were no sampler errors, OOM events or CPU-throttled periods in these
observations. Sampling runs through reads, then takes a post-recovery snapshot;
it does not establish descriptor/CPU peaks during recovery or drain. Sampled
CPU usage during the short idle interval averaged approximately 0.065 cores;
provider CPU and object-store operations are not included.

This is evidence for 1,000 admitted repository Cells plus Directory under the
declared small SQL-only workload. It is not full primitive, realistic-history,
large-transfer, 5,000/10,000-Cell or production-provider qualification. Descriptor
capacity was a concrete deployment failure, fixed independently of the remaining
latency work. Retain the failed seed and the passing repeat together.

## Incremental object-body reuse

The optimized process run `canopy-incremental-cache-f9c6c39fb933` exercises the
shared object cache against colocated RustFS `1.0.0-beta.8-glibc` on the macOS
development host. The report records base revision `60261b0`, the archived source
patch and binary SHA-256
`cc9c4fdcbc681470a59581e97a3b10bffdcae7c286f43cce2568fa8d555977a5`.

The initial stock Git push contains a 2 MiB incompressible external blob, a small
README, tree and commit. A clone warms those four verified object files. A second
push adds one file and changes the tree/commit. Stock Git v0/v2 clones then verify
the exact commit and both files with strict fsck. The original four compressed
files keep their paths, lengths and modification times; only three files are
added. Retained compressed bytes total 2,097,772. Hydration diagnostics independently
report `scanned=4, reused=4, objects=0, bytes=0` before receive, followed by
`scanned=7, reused=4, objects=3, bytes=376` for the new published snapshot.

The owner is then killed, its lease expires, and a fresh local workspace restores
the repository. Both Git protocol versions reproduce the same objects and pass
strict fsck. Cold restoration hydrates all seven objects. Graceful shutdown and
fixture cleanup pass; neither server log contains warnings/errors.

This proves body reuse and exact recovery for the small corpus, not a throughput
or large-history latency target. The metadata scan remains proportional to total
object count, and reusable pack/bitmap acceleration remains open. Existing stock
Git tests also cover coherent ref generations under concurrent changes, pressure
and retry, LFS, and native pack semantics; merge-candidate/rebase callers use the
same private-generation path.

The final focused checks passed: eight cache tests (including corrupt final-range
rejection, retry and fenced dependency retention), the Repository Cell suite,
the stock Git suite with teardown accounting and 32 consistent concurrent
advertisements, three native-candidate tests and the rebase test. All-target
Clippy with warnings denied, formatting and the optimized binary build passed.

Repeat with fixture credentials, a caller-owned disposable S3 prefix and a new
directory on the qualification volume:

```sh
python3 -B scripts/smoke_s3_cache.py \
  --binary "$CARGO_TARGET_DIR/release/canopy" \
  --storage-url s3://qualification-bucket/cache-proof \
  --work-dir "$CARGO_TARGET_DIR/cache-proof-run1"
```

The script retains reports/configuration/logs, cleans its Canopy processes, and
leaves object-store-prefix cleanup to the caller. Enable gateway debug logging to
collect scanned/reused/body counts. The immutable object cache remains disposable;
SQLite and verified external bodies are the recovery source.
