# Exact bound preparation command ownership

Long bound preparations and owner takeover need an exact recovery path for ClaimPreparation and RenewPreparation. ReadyPreparation::claim and PreparationSession::ready_renew prepare those SDK commands before admission to the existing PublicationCoordinator. They reuse LeaseCheck, LeaseRequest, PreparationToken, independent generation pins and the same coordinator job, actor queue and command evidence. The private request is boxed so its exact command/context does not enlarge every ReadyPublication value. No schema, command ID, outbox representation or compatibility adapter is added.

## Admission and recovery

Both commands enter the foreground class with an 8 KiB reservation for two bounded encoded copies. Account/operation limits, operation-ID exclusivity, FIFO account rotation, reserved maintenance slots and concurrent durability waits are shared with checkpoints and push completions. An uncertain command keeps its exact identity, bytes and credits. Dropping an observer does not cancel an accepted command. pending, recover and close_and_drain retain their existing behavior; recovery remains available after closing. Refused admission returns the original ready value.

The factories check repository target equality, a nonzero lease duration within MAX_LEASE_MS and a 4 KiB encoded request. Renewal checks its shared session before and after SDK preparation. Claim intentionally accepts a previous-owner or expired token: the authoritative command checks exact operation identity, actor, phase, current write access, current admitted owner and SQL pin/quota invariants. An expired source may be claimed while it remains present; it cannot be renewed. Claim does not grant custody over old input bytes. Adopt and register the authenticated retained checkpoint before using borrowed physical inputs.

## Original outcome and fresh custody

PublicationOutcome::Preparation returns PreparationCommandOutcome with the command kind, original Committed<PreparationReply> and a separate session Result. Neither the reply's recorded timestamps nor replay alone constructs usable custody. Claim freshly opens CheckPreparation at the original receipt, matches the granted token, floor and format, and exposes a new private session. Renewal freshly queries the same attempt/floor/format and updates the existing shared conservative deadline. Existing fences remain permanent. Each query measures its local deadline from before request dispatch so queue/transport time shortens usable custody.

A known success remains a known success when current permission, expiry or a later Claim prevents usable custody. The original receipt is returned alongside the custody error. Renewal failures fence the original shared session; an ambiguous command retains its old conservative deadline until resolved. Exact rejected/not-started renewals fence that session. Claim does not depend on or revive a previous local session. Final proof factories and authoritative commands continue to recheck custody independently. Preparation outcomes cannot become Git push responses.

Renewal preserves the original generation floor and creating namespace. Claim creates a new admitted attempt and selects the current floor while preserving the previous independent pin. It does not advance an existing floor in place. An indefinitely renewed floor can still exhaust retained-generation capacity; the moving-root progress and capacity gates remain required.

## Remaining lifecycle work

This dispatcher owns one accepted Claim or Renew command, not the entire bound preparation lifecycle. Automatic renewal scheduling, bounded preparation/worker ownership, serialization with checkpoints and final publication, maximum lifetime/floor residence, shutdown drain, production producer integration and durable takeover reconstruction remain required. The process-local exact command survives caller cancellation, not process loss. Unknown or expired SDK evidence cannot justify issuing a replacement command; durable exact/logical recovery must preserve that distinction. Complete retained-root enumeration, writer/reader drain and isolated restore remain required before collection. No remote deletion authority is introduced.

## Validation scope

Six checks cover actual Cell owner restoration and both OID formats, absent/lost/panicked dispatch, canceled observers, refused ready-value reuse and closed recovery, current revocation/expiry/superseding Claim, original receipts with failed fresh custody, permanent session fencing and unchanged renewal floors/old Claim pins. These are bounded correctness fixtures, not full-history import, production cutover, provider durability, complete restore or large-team throughput qualification.
