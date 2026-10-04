# Resident publication recovery ownership

The selected production repository manager owns recovery services for each locally
resident, certified repository. One node-wide publication budget and one read-round
budget are shared across those owners. This connects the existing exact publication
and retirement primitives to real startup, eviction and shutdown. It is not the
completed producer/reader storage cutover or a repository/team capacity result.

## Ownership and startup

`RepositoryManager` creates one `PublicationBudget` from the existing default
publication limits and one `RecoveryScanBudget` with eight read-round permits,
using the same node `TaskTracker` as admitted requests and residency transitions.
Every local `RecoveryServices` constructor receives clones of those budgets.
Scanner constructors require `RecoveryScanSettings`; they cannot create an
independent read budget implicitly. Settings use the immutable directory owner
for account bookkeeping, not a caller-supplied viewer identity. Admission is not
Read, Admin, owner-fence or artifact-retention authority.

Only after identity and certified catalog initialization succeed does the local
loaded entry start a `RecoverySupervisor::start_retiring` and `CustodySupervisor`
with the actual repository target, artifact store, node preparation authority and
fresh owner fence. The loaded entry retains both scanners and their coordinator
before exposing a serving route. Remote routes do not start recovery scanners.
If the second constructor fails, startup explicitly joins the first worker before
returning the error or relinquishing the residency transition.

Discovery reuses indexed keyset paging of independent recovery pins and pending
custody heads. Root recovery reconstructs registered exact commands. Custody
retirement marks expired authentic originals logically stopped without inventing
an execution result. Neither scanner retries arbitrary original mutations from a
positive execution phase or substitutes a new operation/request identity. See
[registered recovery](mandatory-publication-registration.md),
[custody retirement](durable-custody-command-intents.md) and
[terminal retention](terminal-publication-retention.md).

## Bounded reads and pauses

A read round obtains the shared nonwaiting global/account admission before its
query, metadata reads and visits. The default node cap is eight rounds and the
existing account admission grants at most half that cap to one account. Each
scanner owns at most one round. Refused rounds record a deferral and retry after
the configured interval; they do not create a semaphore waiter or command copy.
Production pages contain at most 128 keys and have a one-second delay. Invalid
page, interval, budget and account bounds reject before task creation.

Round admission bounds concurrent recovery work, not provider bandwidth, whole
process memory/RSS, native descendants or complete CPU/I/O fairness. Nonwaiting
account headroom alone does not prove starvation-free service across thousands
of resident repositories. Fair continuous scheduling and capacity qualification
remain required.

The common `ScanControl` serializes round entry against pause/stop. Pause marks
the owner paused, wakes its interval wait and waits for its current round to
finish. It does not drop a Cell query, authenticated artifact read or visit
future. A round publishes diagnostics before its RAII guard releases; after
pause returns those diagnostics are stable. Resume keeps the same worker, cursor
and cumulative diagnostics. A stop is sticky and cannot be undone by resume.
Notify registration precedes state observation to avoid losing a pause/stop
wake-up. Panic/unwind or owned-future cancellation releases the round guard.

Closing the node scan budget prevents another round and wakes idle/paused
workers. It does not cancel an active round. The node task tracker joins those
workers during shutdown. Scanner failure is logged on join; it does not establish
a command result or authorize discarding uncertainty. Automatic restart after a
scanner panic and process/owner-loss adoption remain separate failure-campaign
work.

## Eviction and Git maintenance

The existing per-repository transition guard and bounded residency slots own
release. Candidate selection marks the loaded entry releasing, preventing a
new local fast-path request. Recovery pauses both scanners before checking the
coordinator under its admission lock.

`close_if_idle` closes only an empty coordinator with no dispatch worker. If any
held, queued, running or uncertain original remains, eviction resumes the same
scanners and restores the serving state. It rejects that candidate and may try
another resident; it never replaces its coordinator or releases its credits.

An idle owner joins both paused scanners. Its Git maintenance worker has its own
child stop token and retained join handle; eviction cancels and joins it before
Cell release or local directory deletion. A started maintenance round finishes
its owned gateway/provider work rather than being dropped by cancellation.
Idle maintenance does not hold a request pin.

After those workers quiesce, eviction refreshes the runtime's actual idle Cell
generation: scan reads can invalidate the generation observed during selection.
Only confirmed Cell release permits directory deletion and residency-slot
transfer. Missing/failed release inventory or an ambiguous release retains a
`RefreshHandle` entry. A later load obtains the runtime's actual resident handle,
rebinds the route and starts fresh recovery services; no synthetic capability is
constructed. Cleanup failures retain the released entry and its charged slot.

## Shutdown and independent progress

Shutdown closes native admission, stops maintenance, closes scan discovery and
stops HTTP/SSH ingress. It joins ingress and the node task tracker, including
accepted requests, residency transitions and owned scan/maintenance rounds.
The repository manager then closes publication admission and drains the retained
coordinators. All per-repository drain futures run together, bounded by the
existing loaded-residency cap and shared publication/transport budgets.
A producer-held command in one repository must not prevent exact recovery in
another repository. The drain owns these futures directly; it does not detach
another task inventory or invent a durable queue.

Each drain joins its scanners, waits for dispatch workers and schedules recovery
only for their retained uncertain tickets. Known resolution returns the original
receipt and releases its existing reservation. A held final proof belongs to its
producer: shutdown neither activates nor discards it. An unresolvable original
or held proof keeps shutdown pending, Cell authority, advertisement heartbeat and
workspace ownership intact. Observation timeouts do not cancel that drain.

Only after repository recovery and native resource ownership drain does shutdown
call `node.shutdown`, confirm workspace cleanup, stop renewal and withdraw the
advertisement. There is no timeout that silently releases unresolved authority.

## Qualification and remaining cutover

Regression coverage includes the shared control/admission barrier, tracked
shutdown with an owned active round, independent pause/resume of real indexed
root and custody scanners, authentic orphan retirement, production idle eviction
and certified restoration, preservation of busy held originals, and production
shutdown with held and absent/lost-reply/panicked exact renewal commands across
repositories. Both Git object formats are exercised by the production families.
Final-source totals and retained diagnostic logs are recorded in the
[implementation status](../large-repository-implementation-status.md).

The current selected packed schema still exposes unconverted legacy consumers.
Production HTTP/SSH/generated staging, authoritative certified serving retention,
all object/ref/graph/browser/policy/check/merge consumers and final DDL removal
must move together. Admitted immutable custody history/exact lookup, retained
physical input adoption, typed GC/backup/isolated restore, OS resource containment,
accelerated reads/physical rewrite, fair continuous maintenance, signed native
completion/cold clone, file attribution and full Linux/Kubernetes/Chromium plus
10,000-developer mixed-load qualification remain mandatory. This local branch is
unpublished and unreleasable until those gates are complete.
