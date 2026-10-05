# Service owned staging lifecycle

`StagingCoordinator` now owns admitted Begin/Claim/Renew/RegisterStagedInputs/Bind commands, input and bound tasks and completed results through observer cancellation and exact outcome recovery. It composes the [stored staging phases](staged-input-retention.md) with fresh deadline observations and the existing private catalog pipeline. Production HTTP/SSH receive-pack now selects this lifecycle. Generated producers, durable takeover reconstruction, complete physical admission and large-team qualification remain required.

## Admission and ownership

Keep one service-owned coordinator per repository. `ReadyStaging::new` prepares the original typed custody command 42 and its exact registrar command 41 without executing either; `submit` performs synchronous local admission and starts service-owned supervision. Failure returns the original ready command and reason, so retry preserves mutation identity and bytes. Foreign targets, incompatible lease duration, duplicate logical IDs, closed admission and capacity reject before execution.

| Default bound | Value |
| --- | --- |
| Admitted operations, including uncertain work | 32 |
| Operations per actor | Eight |
| Input workers and retained completed results | 64 |
| Workers and retained results per actor | Eight |
| Custody envelopes | 1 KiB original execution body; 4 KiB complete registrar intent |
| Command reservation per operation | 28 KiB covering retained/dispatch intent bodies, registrar transport/query decode and original reply/body ceilings; another 4 KiB after checkpoint admission |
| Checkpoint slots per operation | One bounded request and retained result |
| Renewed input lease | 60 seconds |
| Renewal lead time | 30 seconds |
| Overall session lifetime | Four hours |
| Bound local residence ceiling | 60 seconds; at most MAX_LEASE_MS |

Limits require room for another actor, checked operation/worker maxima, a nonzero renewal lead shorter than the lease, and a lifetime from the lease duration through 24 hours. These are bounded initial profiles, not capacity results. SQL operation/pin quotas and existing process/disk/file admission remain independent. The shared actor worker semaphore spans all that actor's operations; one actor cannot take every default worker slot. Admission rejects overload rather than creating an unbounded worker queue. This component does not claim account-fair CPU or I/O service.

The service map retains each admitted job. Dropping StagingTicket, StagingTask or an awaiting future does not cancel admitted command or producer execution. `pending(operation)` retrieves the control ticket internally. Completed worker results remain in a bounded, typed service slot and retain worker/actor admission until handoff. `pending_task<T>(id)` retrieves such a result after a lost observer; a wrong result type rejects. This is trusted service plumbing, not an externally authorized product query.

## Execution and deadlines

After Begin or Claim succeeds, query CheckStaging at its receipt. Derive the local monotonic deadline from a timestamp sampled before the query and the queried remaining lease, capped by MAX_LEASE_MS. Queue and transport time shorten usable custody. A saved or replayed success never establishes a fresh deadline.

The service periodically prepares and executes RenewStaging before that deadline. Each renewal has a fresh mutation identity; an ambiguous renewal retains its original command instead of allocating another. After a known success, another authoritative query checks live identity, format, expiry and current access before advancing the shared deadline. Producers receive StagingContext, which supplies the checked namespace token and format, and observes the shared conservative deadline and lifetime.

`ticket.spawn` owns and supervises the producer future. Its returned StagingTask observes the result. A producer error, panic, expired custody or lost access fences the job. Cancellation aborts and joins the async producer; its worker credit remains charged until every physical owner also drains. Completed retained inputs drop before their credit on fencing, including when an external observer remains alive. Results transfer once through `wait`; resources move to the caller before the result slot releases its ownership. Transferred physical owners can keep the original worker admission charged afterward. Retained results count toward both global and actor worker caps, preventing an unbounded completed-result backlog.

Each `StagingContext` clone now retains the same original worker/actor admission. The internal `physical_owner` supplies that lifetime pin without granting custody or publication authority. `spawn_bound` supplies both the live shared preparation session and this context. A detached job must retain the pin through its physical completion; dropping an async observer, returning a result, or expiring custody cannot return that worker's capacity early. No new worker slot or queue is created by cloning ownership.

Native receive requires a live context and retains its owner in the existing Git process group. Response reconciliation and pack enumeration retain it in blocking jobs; capture files retain it through hashing and provider upload. `PhysicalVerifier::download_staged` validates the input's repository, namespace and format against live context, carries the pin through cache creation/download, isolated native verification, canonical extraction and edge-spool writes, and checks custody again before each new inspection and final witness. Deferred workspace cleanup retains the original worker credit. The unowned physical download entry point is qualification-only.

Use the existing independent native-process, workspace, reader and disk admission inside these producers as well. Worker ownership is a drain barrier, not CPU/I/O fairness or a bound on arbitrary heap allocation. The live HTTP/SSH/generated write factories and resident staging service still need conversion; not all lower request, metadata and policy workers have been connected to this pin yet.

## Durable input checkpoint registration

After sealing a NativeInputCertificate, call `ticket.register_inputs(proof, identity)` before seal or stop. Admission synchronously transfers that bounded envelope and exact mutation identity into one service-owned checkpoint slot. Local checks require an active live stage and matching actor/token/target. A completed successful slot can be replaced while unbound only by a proof naming that exact checkpoint digest, enabling the [request-before-native append sequence](durable-push-request.md). Pending/uncertain/failed slots and unrelated proofs reject; failure returns the original proof without executing. Existing observers retain their original receipt. The slot remains charged while the job is admitted, including uncertainty and retained completion. It adds 4 KiB to the existing 28 KiB command reservation; it cannot form an unbounded queue.

The same supervisor prepares command 29 and stores its exact SDK command before dispatch. Due renewal precedes queued registration; accepted registration precedes Bind or graceful stop. `StagedInputsTicket::wait` observes the original durable receipt or uncertainty/error. Dropping it never discards the queued or executing command; `pending_inputs` retrieves the observer. `recover(ticket)` resolves the exact registration without replacing identity or bytes. Closing returns uncertain registrations with their existing reservations.

A known registration stores its original receipt before a fresh CheckStaging query. That query alone can establish usable custody. Revocation after a lost acknowledgement preserves the original committed receipt while fencing the stage; authoritative absence followed by revocation or expiry rejects registration without attaching the checkpoint. Bind can start only after accepted registration resolves successfully and the fresh live probe succeeds. A retained receipt and completed replay never resurrect permission, expiry or a preparation floor.

## Seal and catalog handoff

Call seal when the input phase should finish. It prevents new producer admission and enters Draining. Existing producers and retained completed results continue under renewed staging custody. Bind does not begin until all input slots have drained through handoff or failure. This prevents a canceled observer from silently losing a physical witness while the service advances to catalog preparation.

Bind uses a newly prepared original command 42 and registrar 41 under the shared registered custody protocol. Known binding preserves the token, creating namespace and artifact expiry, and adds only the current catalog floor. Bound records that durable result and its original receipt; its recorded timestamps are not a fresh live-lease observation. Stage contexts become inactive after handoff. The operation remains admitted through bound preparation. `ticket.open_base` refreshes at the binding receipt and uses the existing PreparationBaseResolver with the supervisor's shared session, validating current access and expiry while inheriting automatic renewal, shutdown fencing and the bound residence ceiling.

A producer can physically verify a native pack and return its private PhysicalPackWitness and sealed metadata segments. Take that result, seal, observe Bound, open the base, and feed the witness/segments to CatalogPreparation. The existing assembler rechecks store, namespace, partition completeness, canonical overlap and closure. Its private factories issue the publication proof. Bind and a generic producer result do not grant canonical or publication authority.

Bound preparation is now automatically renewed by this service, and spawn_bound reuses its worker/result ownership; see the [bound lifecycle contract](bound-preparation-lifecycle.md). After a bound Claim, PreparationSession::ready_inputs can transfer an adopted checkpoint into the existing PublicationCoordinator for exact registration recovery; see the [checkpoint contract](native-input-checkpoint.md). The shared dispatcher now owns exact bound Claim/Renew commands through private factories; see the [bound preparation contract](bound-preparation-dispatch.md). Final publication now uses the [serialized lifecycle handoff](final-publication-lifecycle.md); durable takeover orchestration remains required. Configurations allowing a five-minute staging lease can leave that much remaining catalog-floor retention; the floor-cap and hot-repository progress requirements are unchanged.

## Exact uncertainty and shutdown

Staging and bound Begin/Claim/Renew/Bind retain both original command 42 and registrar 41 through the [custody intent protocol](durable-custody-command-intents.md). Registration must be known before original execution; metadata results are observed before SDK expiry or local execution guards. RegisterStagedInputs retains its separate original checkpoint command and exact SDK invocation/resolution path. Resolution of authoritative absence permits execution of the retained exact command only while the local fence, deadline and residence ceiling allow new execution. Known outcomes are returned before that guard. A committed outcome decodes with its original receipt; it never reruns the handler. Unknown, expired, unreachable, changed-incarnation and malformed published results retain evidence and reservation.

Uncertain stops new producer admission. Existing work can continue only through its previously established deadline. `recover(ticket)` resumes the exact retained command; it cannot replace its identity or bytes. No new renewal, registration or bind is issued while an earlier command remains ambiguous. Panicked command tasks retain pending evidence. Unexpected service-worker failure fences local work and waits for exact recovery before restarting supervision; authenticated retirement can schedule that recovery automatically.

`stop` prevents new workers and waits for accepted input tasks/results to drain while renewal continues. It does not retract an independent SQL pin. `close_and_drain` closes all admission, stops jobs and returns still-charged uncertain tickets once running commands and input slots have drained. Service consumers must take retained completed results before a graceful stop can finish; retrieve lost observers through pending_task. Recovery remains possible after closing. A reached lifetime or lost authority fences and discards untransferred results conservatively.

This service map is process-local. Registered custody intents preserve original command knowledge across process loss, but they do not reconstruct local worker ownership, a fresh current-owner lease or an authenticated physical input inventory. Owner takeover must resolve exact/logical outcomes and reconstruct or adopt retained physical inputs under the new admitted namespace through the [authenticated input checkpoint protocol](native-input-checkpoint.md). ReadyStaging::claim now retains/resolves the exact Claim command and supplies a fresh staging context. Staging checkpoint supervision now exists; exact checkpoint supervision after bound Claim now uses the publication dispatcher. Production producer wiring, durable takeover reconstruction and complete wire-plan/response recovery remain required. Final publication now uses the existing fair coordinator through an observation-only lifecycle ticket; accepted final intent continues through close, while a pre-activation fence discards only proven unexecuted work. Neither local completion nor SQL reaping authorizes remote deletion.

## Cold custody reconstruction

`ReadyStaging::restore(client, target, operation)` authenticates and loads the latest registered original without preparing a new original or registrar identity. It supports all seven custody actions: staging and preparation Begin/Claim/Renew, plus Bind. The coordinator resolves the original phase and receipt before checking fresh lease and actual-owner authority. `StagingTicket::restored_evidence` and `restored_outcome` expose historical knowledge, including denials, after local fencing or shutdown; they do not authorize work. Changed restart lease profiles cannot rewrite or hide the original command's duration or result.

Known staging grants acquire fresh staging custody. Known preparation grants use the same fresh bound-session opener as warm Bind/Claim. Recorded clocks never establish a local deadline. Authenticated frozen commands execute only after authoritative SDK absence; the existing receiver atomically enforces current authorization, token and actual ownership. Old-owner Renew/Bind can settle their original stale denial under the new owner without granting custody. Unknown/expired resolution, unavailable queries and lost replies preserve exact evidence and admission for explicit recovery, including on a closed coordinator. Expired unresolved commands cannot become synthetic denials or fresh retries.

The shared preparation fence now retains a terminal watch value. A bound worker observes that signal independently of coordinator status changes or renewal timers, aborts and joins its callback, and drops owned results/resources before releasing credit. Late subscribers observe the already-fired fence. Bound context checks and cancellation deadlines include the shared session's live lease and ceiling. This qualifies callback ownership; it does not establish OS containment for arbitrary detached subprocesses or I/O.

The command-wire reservation remains 28 KiB, or 32 KiB with a checkpoint, and operation/actor/worker admission caps are unchanged. These bounds do not claim total resident heap or Control/advertisement I/O accounting. Cold restore does not resurrect old workers, authenticate a new physical inventory, reconstruct an unregistered registrar, or resolve an expired unresolved original. The [custody retirement service](durable-custody-command-intents.md#separate-retirement-of-expired-originals) now records a separate stop for expired unresolved heads. Explicit exact recovery observes that typed fact, fences/drains and returns local admission while retaining original evidence without an execution reply. The local staging service now automatically observes authenticated retirement as described below. General production scanner ownership/drain, settled-history archival, retained-input adoption and takeover wiring remain required. The separate [production initialization transition](durable-custody-command-intents.md#production-initialization-after-logical-retirement) now retires its own expired unresolved head and chooses an explicit successor; it does not instantiate the general staging scanner lifecycle.

Seven regression families cover both object formats and all seven command kinds; actual durable owner restore after local SQLite removal; original positive/negative receipts; changed restart profiles; authoritative absence; lost replies/panics/private-query failure; closed-service recovery; expired unresolved originals; and resource drop before credit release when a shared session fences. Their native workers and histories are small fixtures, not a large-team capacity result.

## Evidence and remaining work

Nine service tests cover canceled observers and single typed handoff; operation/account/global and actor worker admission; rejected ready-command reuse; automatic renewal without a waiter or manual tick; Begin/Renew/Bind absent, lost-acknowledgement and post-execution panic recovery with original bind receipts; close/drain retaining uncertainty; fresh renewal queries after revocation; producer error/panic fencing; completed-resource drop before credit release with a live observer; and native SHA-1/SHA-256 physical verification followed by late binding, the existing private catalog proof and durable attestation.

Five additional checkpoint service tests cover canceled observers; absent, lost-reply and panicked exact dispatch in both OID formats; registration-before-Bind ordering and original receipt replay; one-slot and foreign/duplicate/closed refusal without execution; committed recovery followed by current-access revocation; and authoritative expiry after absence. The real receive/publication/cold-clone fixture uses service-owned registration in both formats.

These tests establish protocol composition and ownership on small fixtures. They do not establish four-hour full-history throughput, stable maintenance under peak traffic, source-independent restore or capacity for 10,000 engineers. Production producer wiring, complete resource/descendant admission, authenticated durable input inventories and owner-loss adoption, remaining-floor configuration, complete retained-root reclamation, accelerated readers and mandatory mixed-load/recovery campaigns remain release gates.


## Automatic observation of custody retirement

An uncertain registered custody command starts one shared read-only retirement probe for its existing StagingCoordinator. It scans the already admitted map; it creates neither another durable outbox nor one polling task per operation. Default rounds retain at most 128 operation keys/job references and issue one private metadata query at a time. Byte-ordered keyset rotation uses an explicit beginning-of-pass cursor, advances past unavailable/malformed heads and wraps to revisit earlier keys. A round stops selecting further work after its one-second budget elapses, retains ownership until a slow query completes, then waits one second. This budget is not a query deadline or a cold-storage latency guarantee. Unexpected probe-worker failure retains the original jobs and restarts the single probe after the same delay.

The probe takes a thin exact fingerprint under the job's command lock: target, operation/ordinal, intent digest and original PendingMutation. It does not clone the original command/registrar bodies across its independent query await. Loading uses the existing indexed exact-ordinal lookup and authenticated codec, so a later registered successor cannot hide the old original. Only a valid stop fact for that original schedules existing exact recovery. A missing/private/malformed observation retains admission. A known original execution phase alone does not trigger automatic execution/retry. Checkpoint command 29 and final publication remain under their distinct exact owners and are excluded from custody probing.

Exact recovery independently reloads/authenticates closure, reports typed Stopped with the original evidence, fences the shared session and joins/cancels callbacks. It drops untransferred completed resources before their worker credit and removes the staging operation only after all workers drain. A stop is logical closure, never a fabricated command-42 execution receipt or denial. This continues after observer drop or coordinator closure. The probe relinquishes ownership when no eligible jobs remain; admission and that ownership change use the same mutex to avoid a lost new-job wakeup.

StagingStats exposes completed probe queries, failed observations/fingerprint construction, scheduled exact recoveries, worker restarts and whether a probe is running. Counters saturate and remain bounded. The existing 28/32 KiB command-wire reservation and operation/actor/worker caps are unchanged. Private query transport/decode is independently admitted by the SDK; these counters and wire reservations are not total heap, RSS, provider-I/O or latency qualification. This mechanism observes existing authenticated stops; production repository lifecycle ownership of the stop scanner, physical input adoption and cold producer takeover remain required.


Seven additional regression families exercise all seven original custody actions in SHA-1/SHA-256, closed coordinators and dropped observers, stopped staging/bound renewals with live callbacks and retained completed resources, unavailable private queries without absent-command execution or known-phase retries, an old ordinal after an explicit successor, malformed stop rejection, a corrupt head followed by a valid head and later repair, exclusion of input checkpoints despite an older stop, and 130 admitted operations spanning multiple probe pages plus restart at an earlier key after idle. The native/domain codecs reject the all-zero operation ID; the multi-page fixture uses valid nonzero IDs rather than weakening that invariant. The initial warm fixture observed the preceding binding before the renewal; it now waits for the actual uncertain renewal. The checkpoint fixture now registers an explicit successor instead of using the fresh-operation factory against an existing journal. Final frozen-source evidence is recorded in the implementation status.

## Bounded physical metadata handoff

The admitted native verifier now uploads/releases one metadata shard per step
and retains ordered `SourceRecord` descriptors on admitted disk. The creating
result contains a complete witness and descriptor replay; it contains no worker
context or open metadata database, so transferring it cannot block Bind.
The source tree's digest order is distinct from physical ordinal order.

A bound catalog builder checks its staging context, selects the exact retained
native input checkpoint, and authenticates/copies one stored shard at a time.
It reuses the stored metadata artifact instead of uploading it again. Blocking
closure/directory jobs, file reads/writes and artifact hash jobs retain the same
activity pin. A failure or cancellation cannot yield a private prepared catalog.
Completed private proofs do not retain worker activity across final publication.

This API composition is a prerequisite, not live transport conversion or a
full-history deadline/throughput result. Resident service drain, adopted older
input verification, remaining request/policy work and actual producer wiring
remain release work. Current qualification and limits are tracked in the
[implementation status](../large-repository-implementation-status.md).


## Production receive workflow and shutdown grace

HTTP and SSH transfer one authenticated encoded receive request to `drive_receive`
before awaiting status. Its controller registers wire custody, retrieves staged
native/result/descriptor outputs, registers checkpoints, drains physical workers,
binds once, and orders the existing policy/ref/completed-root protocol. Each
byte-bounded page advances by its actual minted end offset. Returned success is
selected from the durable completed root under current read authorization.

Node shutdown first closes ingress and staging admission. Already-owned receive
controllers get a shared 30-second grace while serving, native admission, Cell
heartbeat and workspace ownership remain available. The grace is a controller
finish window, not a deadline for draining physical jobs or uncertain mutations.
After it expires, ordinary forced close cancels/joins controllers and drains the
existing worker/exact recovery owners before the lower services can close.
Generic `drive` callback producers continue to cancel immediately. Public stop,
lease/authority fencing and forced close retain their previous semantics.

Stock SSH qualification disconnects the request after real native pack upload is
paused, starts shutdown, proves release remains blocked, resumes upload, and
checks the new commit and blob through cold clone and strict fsck. Late Write-to-Read
revocation before Bind still requires a separately authorized durable refusal
path. No current lease, completed receipt or generic callback may bypass that
missing authority transition. Request/result/policy physical pins and authenticated
older-owner adoption remain unfinished release gates.
