# Final publication owned by the bound lifecycle

StagingCoordinator now serializes final inline push, immutable root push and compaction publication after bound workers/results, due renewal and queued input registration. It uses the existing PublicationCoordinator job, private ready proof, exact SDK command, class/account fair queue and byte reservation. The staging job retains a small ticket; it does not copy the command or create another payload queue. Production handlers and durable owner-loss reconstruction still require integration.

Initial staging admission now retains its first accepted original SDK receipt in the existing logical push row. Recovery reads that authenticated receipt before SDK expiry handling and separately checks current custody. Cold service callers use explicit Claim to acquire a new namespace under the executing owner, including after the original operation was reaped. Root completion updates the pending row while preserving initial knowledge. See the [initial staging receipt contract](initial-staging-receipts.md) for the exact bounds and remaining registered-attempt work.

## Admission and handoff

PublicationCoordinator::try_reserve synchronously admits a Held job without dispatch. It applies the same target, logical-operation uniqueness, per-class account/operation limits and factory-derived byte reservation as submit. A contended admission lock returns Capacity with the original ready value; the consumer must retry through its bounded scheduling policy. submit still admits directly to Queued. Held jobs do not consume dispatch concurrency or advance the fair queue.

StagingTicket::publish accepts only a final privately prepared inline push, immutable root push or compaction sharing the bound session's target, exact lease check, deadline, permanent fence and fixed ceiling. An independently opened session with equal SQL tokens is insufficient. Checkpoint and Claim/Renew factories cannot enter this final handoff. Refusal returns the original ready value. Successful synchronous reservation stores the small ticket and seals new bound worker/checkpoint admission before returning the observation-only StagedPublicationTicket. pending_publication recovers that observer after cancellation. A recorded original bound receipt remains available separately.

The existing workers and retained typed results must drain before activation. The supervisor keeps renewing while they drain; due renewal and any accepted checkpoint resolve first in the existing exact slot, including uncertain commands recovered after close. Final preflight uses the latest known Bind/Claim, renewal or registration receipt as its minimum query watermark. Successful activation joins the existing class/account fair queue once. The lifecycle stops issuing renewal/checkpoint commands while final dispatch or recovery owns the logical operation.

Retrieve the producer's StagingTask result before waiting for publication. Awaiting publication inside an owned producer would prevent that producer's own drain. The intended sequence is:

```rust,ignore
let base = Arc::new(stage.open_base(indexes, files).await?);
let work = stage.spawn_bound(move |_, context| async move {
    context.ensure_live()?;
    // Build/verify with existing admitted native/disk/reader primitives.
    // Retain context ownership in physical jobs that can outlive this future.
    // Return a private ready_push, ready_root_push or ready_compaction value.
    prepare_ready(base).await
})?;
let ready = work.wait().await?;
let observer = stage.publish(&publications, ready)?;
let outcome = observer.wait().await;
// Uncertain: retain the same stage and recover it explicitly.
```

This is a composition sketch; prepare_ready is the producer's existing private factory work. The inline native receive fixture follows this handoff and reconstructs a cold stock-Git clone from the resulting catalog. The immutable root fixture uses the same native capture and registered custody, prepares its exact command in a bound worker and retrieves that result before the handoff. Root-backed cold-clone serving remains required. A maintenance fixture constructs compaction in the same bound-owned worker and publishes through the reserved maintenance class.

## Exact outcomes, fencing and shutdown

PublicationTicket::activate is idempotent for an already activated job. It never retries uncertainty. discard_held serializes with activation and succeeds only while execution has not started. It drops the original proof/resources before returning class/account/byte credits and records Discarded, which cannot acknowledge a push. Activation/discard and exact recovery remain available for previously admitted work after coordinator close. close_and_drain returns held and uncertain tickets still charged; it does not activate or silently discard them. Close the staging lifecycle before the shared publication coordinator in normal shutdown.

A custody failure or local ceiling before activation discards the held proof and fences/drains the lifecycle's existing worker/result resources before releasing operation admission. An activated command keeps its exact identity/proof and original outcome regardless of later local fencing. Initial transport submission and resubmission after authoritative absence recheck the shared session's local deadline/fence/ceiling. An inactive guard records NotStarted and cannot become an acknowledgement. Known committed results are resolved before that local guard, so an expired/fenced session cannot erase an earlier durable receipt. Unknown, expired, unreachable, malformed or changed-incarnation outcome evidence remains uncertain and charged.

Final uncertainty appears as StagingState::Uncertain with the original typed PublicationError. StagingCoordinator::recover schedules the same retained PublicationTicket in its fair queue, including after both services close. The lifecycle also observes recovery performed directly by the shared coordinator, preventing a resolved ticket from leaving staging admission stranded. Known final success or rejection becomes Published with its original outcome. The lifecycle permanently fences its session and releases local resources/admission; it does not query the preparation record after completion, since successful publication retires it. The bound receipt, checkpoint receipt and publication result remain distinct.

The inline response observer is service-internal access to an already admitted result. Root observers use root_response(store), which selects the durable actor/operation/request result under current read authorization and the original receipt before streaming authenticated bytes. Externally requested replay must use the corresponding authenticated replay_push_response or replay_root_push_response preflight. A receipt or caller-selected root grants no artifact access. Compaction results cannot become push responses. A known catalog conflict terminates this local lifecycle; a subsequent Claim and freshly reconciled proof must enter a new admitted lifecycle rather than replacing an ambiguous command.

## Terminal recovery retirement

After an immutable root outcome and its recovery phase are durably known, the service can start `RecoverySupervisor::start_retiring` with current repository administration and actual owner custody. It prepares a private terminal release through the existing maintenance queue, transferring the same recovery headers/journal to the selected immutable push row and freeing that preparation pin atomically. Original uncertain release commands remain charged and recoverable even after the pin disappears. Live staging uncertainty keeps its original owner. Follow the [terminal retention contract](terminal-publication-retention.md) for exact eligibility, retained edges and receipt recovery; this path grants no provider deletion authority. Production startup must wire the service as part of the mandatory hard cutover.

## Release gates

These services retain process-local ownership, not a durable outbox. They do not reconstruct authenticated wire plans/responses after process loss. Staging admission still needs production scheduling, maintenance preparation shares and OS CPU/RSS/file/PID/I/O containment. Fresh-schema selection, HTTP/SSH/mirror/generated producer and reader cutover, hot-root progress, continuous maintenance, physical pack rewriting/read acceleration, complete retained-root collection/drain and source-independent isolated restore remain required. The full-history and 10,000-engineer [mixed-load/recovery gates](../large-team-scalability.md) remain unqualified. No remote deletion authority is added.
