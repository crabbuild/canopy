# Final publication owned by the bound lifecycle

StagingCoordinator now serializes final push and compaction publication after bound workers/results, due renewal and queued input registration. It uses the existing PublicationCoordinator job, private ready proof, exact SDK command, class/account fair queue and byte reservation. The staging job retains a small ticket; it does not copy the command or create another payload queue. Production handlers and durable owner-loss reconstruction still require integration.

## Admission and handoff

PublicationCoordinator::try_reserve synchronously admits a Held job without dispatch. It applies the same target, logical-operation uniqueness, per-class account/operation limits and factory-derived byte reservation as submit. A contended admission lock returns Capacity with the original ready value; the consumer must retry through its bounded scheduling policy. submit still admits directly to Queued. Held jobs do not consume dispatch concurrency or advance the fair queue.

StagingTicket::publish accepts only a final privately prepared push or compaction sharing the bound session's target, exact lease check, deadline, permanent fence and fixed ceiling. An independently opened session with equal SQL tokens is insufficient. Checkpoint and Claim/Renew factories cannot enter this final handoff. Refusal returns the original ready value. Successful synchronous reservation stores the small ticket and seals new bound worker/checkpoint admission before returning the observation-only StagedPublicationTicket. pending_publication recovers that observer after cancellation. A recorded original bound receipt remains available separately.

The existing workers and retained typed results must drain before activation. The supervisor keeps renewing while they drain; due renewal and any accepted checkpoint resolve first in the existing exact slot, including uncertain commands recovered after close. Final preflight uses the latest known Bind/Claim, renewal or registration receipt as its minimum query watermark. Successful activation joins the existing class/account fair queue once. The lifecycle stops issuing renewal/checkpoint commands while final dispatch or recovery owns the logical operation.

Retrieve the producer's StagingTask result before waiting for publication. Awaiting publication inside an owned producer would prevent that producer's own drain. The intended sequence is:

```rust,ignore
let base = Arc::new(stage.open_base(indexes, files).await?);
let work = stage.spawn_bound(move |_| async move {
    // Build/verify with existing admitted native/disk/reader primitives.
    // Return a private ready_push or ready_compaction value.
    prepare_ready(base).await
})?;
let ready = work.wait().await?;
let observer = stage.publish(&publications, ready)?;
let outcome = observer.wait().await;
// Uncertain: retain the same stage and recover it explicitly.
```

This is a composition sketch; prepare_ready is the producer's existing private factory work. The native receive fixture now follows this handoff and reconstructs a cold stock-Git clone from the resulting catalog. A maintenance fixture constructs compaction in the same bound-owned worker and publishes through the reserved maintenance class.

## Exact outcomes, fencing and shutdown

PublicationTicket::activate is idempotent for an already activated job. It never retries uncertainty. discard_held serializes with activation and succeeds only while execution has not started. It drops the original proof/resources before returning class/account/byte credits and records Discarded, which cannot acknowledge a push. Activation/discard and exact recovery remain available for previously admitted work after coordinator close. close_and_drain returns held and uncertain tickets still charged; it does not activate or silently discard them. Close the staging lifecycle before the shared publication coordinator in normal shutdown.

A custody failure or local ceiling before activation discards the held proof and fences/drains the lifecycle's existing worker/result resources before releasing operation admission. An activated command keeps its exact identity/proof and original outcome regardless of later local fencing. Initial transport submission and resubmission after authoritative absence recheck the shared session's local deadline/fence/ceiling. An inactive guard records NotStarted and cannot become an acknowledgement. Known committed results are resolved before that local guard, so an expired/fenced session cannot erase an earlier durable receipt. Unknown, expired, unreachable, malformed or changed-incarnation outcome evidence remains uncertain and charged.

Final uncertainty appears as StagingState::Uncertain with the original typed PublicationError. StagingCoordinator::recover schedules the same retained PublicationTicket in its fair queue, including after both services close. The lifecycle also observes recovery performed directly by the shared coordinator, preventing a resolved ticket from leaving staging admission stranded. Known final success or rejection becomes Published with its original outcome. The lifecycle permanently fences its session and releases local resources/admission; it does not query the preparation record after completion, since successful publication retires it. The bound receipt, checkpoint receipt and publication result remain distinct.

The response observer is service-internal access to an already admitted result. Externally requested replay must use the existing authenticated replay_push_response preflight and current read authorization. Compaction results cannot become push responses. A known catalog conflict terminates this local lifecycle; a subsequent Claim and freshly reconciled proof must enter a new admitted lifecycle rather than replacing an ambiguous command.

## Release gates

These services retain process-local ownership, not a durable outbox. They do not reconstruct authenticated wire plans/responses after process loss. Staging admission still needs production scheduling, maintenance preparation shares and OS CPU/RSS/file/PID/I/O containment. Fresh-schema selection, HTTP/SSH/mirror/generated producer and reader cutover, hot-root progress, continuous maintenance, physical pack rewriting/read acceleration, complete retained-root collection/drain and source-independent isolated restore remain required. The full-history and 10,000-engineer [mixed-load/recovery gates](../large-team-scalability.md) remain unqualified. No remote deletion authority is added.
