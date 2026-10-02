# Service owned staging lifecycle

`StagingCoordinator` now owns admitted Begin/Claim/Renew/RegisterStagedInputs/Bind commands, input and bound tasks and completed results through observer cancellation and exact outcome recovery. It composes the [stored staging phases](staged-input-retention.md) with fresh deadline observations and the existing private catalog pipeline. Production HTTP/SSH/mirror/generated producer selection, durable takeover reconstruction, full process admission and large-team qualification remain required.

## Admission and ownership

Keep one service-owned coordinator per repository. `ReadyStaging::new` prepares the exact SDK BeginStaging command without executing it; `submit` performs synchronous local admission and starts service-owned supervision. Failure returns the original ready command and reason, so retry preserves mutation identity and bytes. Foreign targets, incompatible lease duration, duplicate logical IDs, closed admission and capacity reject before execution.

| Default bound | Value |
| --- | --- |
| Admitted operations, including uncertain work | 32 |
| Operations per actor | Eight |
| Input workers and retained completed results | 64 |
| Workers and retained results per actor | Eight |
| Encoded command envelope | 4 KiB |
| Command reservation per operation | 8 KiB for retained and transport copies; another 4 KiB after checkpoint admission |
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

`ticket.spawn` owns and supervises the producer future. Its returned StagingTask observes the result. A producer error, panic, expired custody or lost access fences the job. Cancellation aborts and joins the producer before releasing its worker credit. Completed retained inputs drop before their credit on fencing, including when an external observer remains alive. Results transfer once through `wait`; resources move to the caller before that slot releases. Retained results count toward both global and actor worker caps, preventing an unbounded completed-result backlog.

Use existing admitted native-process, workspace, reader and disk primitives inside producers. The callback counter does not account for arbitrary heap allocation, unjoined descendants or detached physical readers. Their independent admission and retention obligations remain in force; production integration must preserve them through work and handoff.

## Durable input checkpoint registration

After sealing a NativeInputCertificate, call `ticket.register_inputs(proof, identity)` before seal or stop. Admission synchronously transfers that bounded envelope and exact mutation identity into one service-owned checkpoint slot. Local checks require an active live stage, matching actor/token/target and no prior checkpoint; failure returns the original proof without executing. The slot remains charged while the job is admitted, including uncertainty and retained completion. It adds 4 KiB to the existing 8 KiB command reservation; it cannot form an unbounded queue.

The same supervisor prepares command 29 and stores its exact SDK command before dispatch. Due renewal precedes queued registration; accepted registration precedes Bind or graceful stop. `StagedInputsTicket::wait` observes the original durable receipt or uncertainty/error. Dropping it never discards the queued or executing command; `pending_inputs` retrieves the observer. `recover(ticket)` resolves the exact registration without replacing identity or bytes. Closing returns uncertain registrations with their existing reservations.

A known registration stores its original receipt before a fresh CheckStaging query. That query alone can establish usable custody. Revocation after a lost acknowledgement preserves the original committed receipt while fencing the stage; authoritative absence followed by revocation or expiry rejects registration without attaching the checkpoint. Bind can start only after accepted registration resolves successfully and the fresh live probe succeeds. A retained receipt and completed replay never resurrect permission, expiry or a preparation floor.

## Seal and catalog handoff

Call seal when the input phase should finish. It prevents new producer admission and enters Draining. Existing producers and retained completed results continue under renewed staging custody. Bind does not begin until all input slots have drained through handoff or failure. This prevents a canceled observer from silently losing a physical witness while the service advances to catalog preparation.

BindStaging uses a freshly prepared exact SDK command. Known binding preserves the token, creating namespace and artifact expiry, and adds only the current catalog floor. Bound records that durable result and its original receipt; its recorded timestamps are not a fresh live-lease observation. Stage contexts become inactive after handoff. The operation remains admitted through bound preparation. `ticket.open_base` refreshes at the binding receipt and uses the existing PreparationBaseResolver with the supervisor's shared session, validating current access and expiry while inheriting automatic renewal, shutdown fencing and the bound residence ceiling.

A producer can physically verify a native pack and return its private PhysicalPackWitness and sealed metadata segments. Take that result, seal, observe Bound, open the base, and feed the witness/segments to CatalogPreparation. The existing assembler rechecks store, namespace, partition completeness, canonical overlap and closure. Its private factories issue the publication proof. Bind and a generic producer result do not grant canonical or publication authority.

Bound preparation is now automatically renewed by this service, and spawn_bound reuses its worker/result ownership; see the [bound lifecycle contract](bound-preparation-lifecycle.md). After a bound Claim, PreparationSession::ready_inputs can transfer an adopted checkpoint into the existing PublicationCoordinator for exact registration recovery; see the [checkpoint contract](native-input-checkpoint.md). The shared dispatcher now owns exact bound Claim/Renew commands through private factories; see the [bound preparation contract](bound-preparation-dispatch.md). Final-publication lifecycle serialization and durable takeover orchestration remain required. Configurations allowing a five-minute staging lease can leave that much remaining catalog-floor retention; the floor-cap and hot-repository progress requirements are unchanged.

## Exact uncertainty and shutdown

Begin, Claim, Renew, RegisterStagedInputs and Bind share the same exact invocation/resolution implementation with push and compaction dispatch. Resolution of authoritative absence permits execution of the retained exact command. A committed outcome decodes with its original receipt; it never reruns the handler. Unknown, expired, unreachable, changed-incarnation and malformed published results retain evidence and reservation.

Uncertain stops new producer admission. Existing work can continue only through its previously established deadline. `recover(ticket)` resumes the exact retained command; it cannot replace its identity or bytes. No new renewal, registration or bind is issued while an earlier command remains ambiguous. Panicked command tasks retain pending evidence. Unexpected service-worker failure fences local work and requires explicit exact recovery before restarting supervision.

`stop` prevents new workers and waits for accepted input tasks/results to drain while renewal continues. It does not retract an independent SQL pin. `close_and_drain` closes all admission, stops jobs and returns still-charged uncertain tickets once running commands and input slots have drained. Service consumers must take retained completed results before a graceful stop can finish; retrieve lost observers through pending_task. Recovery remains possible after closing. A reached lifetime or lost authority fences and discards untransferred results conservatively.

This service is process-local ownership, not a durable outbox or authenticated input inventory after process loss. Owner takeover must resolve exact/logical outcomes and reconstruct or adopt retained physical inputs under the new admitted namespace through the [authenticated input checkpoint protocol](native-input-checkpoint.md). ReadyStaging::claim now retains/resolves the exact Claim command and supplies a fresh staging context. Staging checkpoint supervision now exists; exact checkpoint supervision after bound Claim now uses the publication dispatcher. Production producer wiring, final-publication lifecycle serialization, durable takeover reconstruction and complete wire-plan/response recovery remain required. Neither local completion nor SQL reaping authorizes remote deletion.

## Evidence and remaining work

Nine service tests cover canceled observers and single typed handoff; operation/account/global and actor worker admission; rejected ready-command reuse; automatic renewal without a waiter or manual tick; Begin/Renew/Bind absent, lost-acknowledgement and post-execution panic recovery with original bind receipts; close/drain retaining uncertainty; fresh renewal queries after revocation; producer error/panic fencing; completed-resource drop before credit release with a live observer; and native SHA-1/SHA-256 physical verification followed by late binding, the existing private catalog proof and durable attestation.

Five additional checkpoint service tests cover canceled observers; absent, lost-reply and panicked exact dispatch in both OID formats; registration-before-Bind ordering and original receipt replay; one-slot and foreign/duplicate/closed refusal without execution; committed recovery followed by current-access revocation; and authoritative expiry after absence. The real receive/publication/cold-clone fixture uses service-owned registration in both formats.

These tests establish protocol composition and ownership on small fixtures. They do not establish four-hour full-history throughput, stable maintenance under peak traffic, source-independent restore or capacity for 10,000 engineers. Production producer wiring, complete resource/descendant admission, authenticated durable input inventories and owner-loss adoption, remaining-floor configuration, complete retained-root reclamation, accelerated readers and mandatory mixed-load/recovery campaigns remain release gates.
