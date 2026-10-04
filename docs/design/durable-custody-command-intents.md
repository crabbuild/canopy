# Durable custody command intents

Status: local, unpublished production cutover. Repository initialization and the staging/preparation service factories use this protocol. Cold service owner fencing/reconstruction, compact terminal archival and capacity qualification remain required before release.

A prepared command can be lost before Begin grants an artifact namespace. A final publication's existing [registered recovery root](mandatory-publication-registration.md) cannot cover that interval: its body artifacts require an independently admitted namespace. Allocating a fake namespace or reconstructing a fresh SDK identity would cross the custody or exact-command boundary.

## Representation and bounds

The protocol reuses `PreparedCommandSnapshot`, `Stamp`, `Recorded`, `CertificateEnvelope`, the existing Begin/Claim/Renew/Bind domain logic, namespace allocator and independent generation pins. One additional `WITHOUT ROWID` relation, `catalog_custody_commands`, stores command metadata before and after admission. It stores no Git object, object edge, physical pack or inventory entry. Its rows are proportional to custody transitions.

Each row is keyed by logical operation and ordinal. A separate unique key binds the original incarnation and SDK request ID. The authenticated intent binds tenant/application, repository, actor, logical operation/digest, original SDK stamp, ordinal, exact predecessor digest and the snapshot/body digest. The typed body distinguishes preparation Begin/Claim/Renew and staging Begin/Claim/Renew/Bind. It preserves the original encoded bytes, compiled contract, incarnation, identity and expiration. Neither restoration nor registration extends that expiration.

The input is bounded to 1 KiB, SDK snapshot to its existing 2 KiB ceiling, authenticated carrier to 1 KiB, complete stored intent to 4 KiB and recorded phase to 1 KiB with a 512-byte typed reply. Every factory and receiver applies its complete encoded limit; individual ceilings do not authorize a sum exceeding the complete limit. Factory encoding fails before registration or allocation if the combined representation exceeds it. Actor identifiers retain their existing 64-byte limit. Ordinals use the existing recovery protocol's 65,535 ceiling; exhaustion refuses preparation rather than replacing history.

At most one unresolved row exists per logical operation. Indexed admission counts at most 1,024 pending heads; settled history does not consume this unresolved-work quota. A partial index serves that count. The primary key serves latest-head and exact-ordinal discovery. An explicit indexed lookup on operation/incarnation/admission sequence discovers an authentic historical grant for restart Claim without scanning the operation's renewal history. SQL guards prevent changing or replacing an intent, changing a settled phase or its grant identity, and deleting retained knowledge.

## Registration and execution

Command 41 registers the authenticated exact intent. The first matching ordinal wins. Advancing requires a settled predecessor, its exact encoded digest and matching logical actor/digest. Unknown work cannot be skipped. A new registration checks current Write, the actual incarnation and original command expiry. Exact existing knowledge remains discoverable after permission or owner loss; this grants no execution or upload permission.

The factory retains its original snapshot/body through registration. A private query of the exact row proves registration even after a lost acknowledgement or registrar SDK expiry. It returns an identical winner before issuing another registration mutation. A missing or corrupt row after uncertainty remains an error. A competing candidate cannot dispatch its original command. New registration knowledge is observed through the authoritative Cell query, and execution independently verifies the same pointer.

Command 42 accepts only the registered original SDK stamp and typed body at that ordinal. It reuses the domain receiver's current authorization, actual owner fence, exact token/pin, expiration, generation and quota checks. Namespace/pin/domain writes and the original typed result share the same transaction and SDK acceptance. SQL or encoding failure rolls back all of them. Positive results and domain denials both commit an authenticated-intent-bound phase; private service boundaries normalize trusted negative replies back to `Rejected` while preserving their original receipt.

Recovery queries the original ordinal and observes its phase before SDK resolution or any current-custody query. Known history returns the original receipt after later renewals, Claim, owner loss, revoked Write or SDK expiry. A phase missing despite SDK acceptance is treated as uncertainty. Only authoritative SDK `Absent` can restore and execute the original unchanged bytes. `Unknown`, `Expired`, query failure and corrupt metadata retain the original evidence. In particular, an expired unresolved command is not replaced with a new identity.

Historical grants are knowledge, not leases or artifact retention roots. They never restart a clock or retain every old base forever. Fresh authorized queries and actual independent pins determine current custody. After a grant's operation and pin are reaped, Claim can authenticate that exact indexed historical token, recheck current Write/logical availability/quotas and allocate a different namespace/pin under the actual executing fence and sequence. It cannot displace an active successor or recreate a completed outcome.

## Service ownership

`OwnedCustody` retains the exact original 42 and original registrar 41. The StagingCoordinator's Begin/Claim/Renew/Bind and bound Claim/Renew variants use that owner, as do ReadyPreparation factories in the fair PublicationCoordinator. An ordinary lost registrar reply can be resolved by the identical authoritative pointer. Registrar uncertainty and worker panic retain original execution evidence and both command bodies; observer cancellation or closing cannot replace them. Cold restore loads a registered head without issuing a new registrar or original identity.

Each admitted custody job reserves 28 KiB, including retained/dispatch intent bodies, registrar transport/query decode and original body/reply ceilings. The staging checkpoint slot adds its existing 4 KiB, totaling 32 KiB. This accounting leaves operation, actor, worker, class and global byte caps unchanged. It does not measure the entire process heap or native resource use.

Known phase results precede SDK resolution and local custody guards. Only proven absence can invoke an original under a still-valid local fence/deadline/ceiling; expired or unknown evidence stays retained. Fresh post-result queries establish a conservative clock, never from recorded reply timestamps. Direct caller-owned raw session/base renewal methods have been removed; ready renewals transfer into the service-owned coordinator.

Cold service reconstruction still needs an independent current actual-owner check before constructing usable sessions. QueryContext exposes no active owner epoch, so a matching historical SQL token plus a fresh clock query is insufficient. Actual startup already obtains this fact from validated Cell Control and a live node advertisement; service/session reconstruction must carry the same authority. Original outcome knowledge remains recoverable even when current custody is refused.

## Startup integration

Pending repository initialization discovers its latest registered custody head before constructing another original command. It recovers pre-dispatch Begin, accepted/denied Begin and subsequent Claim/Renew results. A matching current owner and fresh exact custody query are required before using a historical grant. Otherwise, a known resolved phase can precede an explicit registered Claim. A known denied final initialization forces Claim of that refused attempt even when its old Begin grant is still readable; a previously accepted successor Claim is recovered rather than repeated. Unknown phases stop initialization.

The certified final initializer, its immutable root graph and [terminal retirement](terminal-publication-retention.md) remain the authority before repository Ready. The existing tracked repository transition owns startup through cancellation. Ready restore observes the immutable initialization and preserves the original intent/phase bytes without allocating another namespace. There is no new product API or permission granted by these private metadata queries.

## Cost and release work

Each new custody transition currently adds one registration mutation plus one execution mutation. Exact known lookup/replay adds no execution mutation; already registered retries avoid another registration mutation. Count these phases, policy/native checkpoints, final registration and completion in serialized service-time and fairness budgets. The earlier two-command illustration is not this protocol's total push cost.

Inline command metadata closes the pre-namespace correctness gap, but retaining one SQL row per renewal forever is not the intended final storage strategy. Before release, compact settled per-operation history into bounded immutable frames in a genuinely admitted namespace, reusing the existing saved-command/root/frame codecs and indexed immutable storage. Keep an authenticated discoverable SQL head and retain exact historical lookup; denied pre-admission work cannot depend on a fabricated namespace. Include this history in typed collection, backup and isolated restore, without treating the historical grant's base descriptors as new live roots. The current implementation conservatively retains rows and does not claim repository/team capacity.

The staging and preparation factories now retain this protocol through admission, cancellation and service closure. Complete cold service reconstruction and current actual-owner session fencing before release. Remove raw custody bindings from production; domain methods remain callable inside the registered receiver and explicit qualification fixtures only. Remove redundant first-admission columns after their consumers and restart proofs use this journal. Complete foreground producers/readers, final schema removal, serving-generation retention, typed collection/backup, OS resource containment, continuous maintenance, physical rewriting and full-history mixed load before publishing the hard cutover.

Qualification covers SHA-1/SHA-256 first-writer races, pre-namespace persistence/discovery, unregistered and losing identities, late registration/phase rollback with SDK absence and exact retry, immutable metadata, all seven transitions, historical receipts after successors, original denied Begin/Renew after real SDK expiry, cold SQLite removal and owner restore, reaped successor Claim, forged tokens, corrupt metadata and bounded indexed lookup. A joint initialized catalog/ref base is tested against the reply ceiling. Real workspace tests check certified repository creation and identical custody metadata after fresh-disk restore. These are focused correctness checks, not a full-history or 10,000-developer capacity claim.

Reproduce the focused checks with the pinned SDK dependencies and Rust 1.98.0:

```sh
cargo +1.98.0 test -p canopy-server --lib packs::publication --locked -- --test-threads=4
cargo +1.98.0 test -p canopy-server --test multi_server workspace --locked -- --test-threads=4
cargo +1.98.0 clippy --workspace --all-targets --locked -- -D warnings
cargo +1.98.0 build -p canopy-server --bin canopy --locked
```

The owned service checkpoint passes 286 publication and nine workspace/lifecycle cases, all-target workspace Clippy with warnings denied and the server build on macOS. The publication suite includes registrar loss before submission, after acceptance and after a panic, cancellation/closed-service recovery, and restored renewal preserving an existing resolver fence. Frozen Rust-source hashes and protected-checkout/dependency checks accompany the validation. Linux/provider CI and the complete runtime/capacity campaign remain release gates.
