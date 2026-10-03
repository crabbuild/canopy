# Terminal publication recovery retention

Completed pushes must release their independent preparation pins without losing original command receipts. Leaving successful pins forever eventually exhausts the 4,096-pin admission cap and retains unnecessary catalog floors. This protocol transfers the same authenticated recovery certificate and phase journal into the existing immutable `pushes` row, then deletes the matching pin in one transaction. It uses the fresh publication schema and existing artifact structures; it adds no durable queue or per-object rows.

The protocol releases a preparation pin. It does not authorize provider deletion. Complete retained-root enumeration, reader and worker drain, backup, isolated restore, and repository-scoped collection remain required before any production artifact deletion.

## Release eligibility and authority

`RegisteredRootRecovery::ready_terminal_release` accepts only the current canonical recovery head with a recorded completed root outcome. A policy head is terminal only when its original refusal is recorded and its pre-frozen fallback root command has completed. Unknown acceptance, passed intermediate pages, denied root commands, historical heads, and a refused page without completed fallback cannot produce a release proof.

The private factory checks the saved actor and request selection against that exact completed outcome. It authenticates the current bundle and each predecessor frame, verifying repository MACs, tenant/application, lease identity, original SDK stamps, and strictly decreasing phase steps. Each header is bounded by 8 KiB. It then authenticates the selected outcome/native metadata and streams the selected response and native audit bodies to verified EOF. Missing or corrupt required bytes prevent proof creation.

The purpose-specific MAC proof binds the original recovery certificate, phase-journal digest, and verified closed-graph digest. `ReleaseTerminalRecovery` is command 40, codec version 1, with input limited to 4 KiB and output to 128 bytes. The actual command receiver separately requires current repository Admin authorization and its admitted owner fence. A proof prepared under an old owner or by a subsequently unauthorized administrator cannot bypass those checks.

## Atomic transfer and original receipts

Before its first write, the receiver verifies the proof and embedded original certificate, exact current pin identity/head/phase, terminal selection, immutable saved push outcome, and absence of an active logical operation. All semantic refusals precede writes.

The receiver obtains its release command's actual SDK mutation evidence and sequence from `CommandContext`. It writes three bounded values to the existing push row: the original recovery certificate, original phase journal, and release identity/result/sequence. An exact CAS then deletes the matching preparation pin. Any later SQL failure rolls back both writes and SDK acceptance. SQL guards prohibit archive replacement, mutation or deletion; the lease deletion guard requires the same certificate and phase in the completed push row.

Archived recovery reuses `RegisteredRootRecovery` and the existing predecessor-frame walk. The original root/page receipt survives SDK identity expiry, removal of original command bodies, local SQL destruction and fresh-owner restoration. A completed release also resolves its original archived receipt before SDK resolution or current custody checks. A competing, separately prepared release identity cannot inherit that receipt; once another release wins, its receiver refuses Missing.

An owner SQL query is not an arbitrary local SQLite read. The pinned Cellule executor refuses `PendingPublication` until its logical head has a durability proof. Archive lookup failures retain original pending evidence and admission credits; SQL presence before that proof cannot acknowledge a release. Product response replay still requires current Read authorization even when the internal recovery receipt is known.

## Retained artifact edges

| Root or edge | Retention after this attempt closes |
| --- | --- |
| Recovery certificate and phase journal | Stored unchanged in the selected push row |
| Original head bundle and predecessor frames/bundles | Required to resolve exact historical command identities and receipts |
| Selected outcome root and native result root | Required immutable metadata for replay and audit |
| Selected response body | Required; its creating namespace can belong to a prior admitted attempt |
| Native plan and signed certificate bodies | Required native audit edges, when present |
| Original wire request, original command bodies, unselected responses and unpublished candidate artifacts | No permanent edge from this closed audit role; other retained snapshots, unknown attempts, readers or backups can still require them |
| Published catalog, refs, packs and indexes | Retained through their own certified catalog/generation and reader/backup roots |

`root_completion::closed_graph` verifies the closed audit edges using the existing `StoredInputRoot`, `ArtifactDescriptor` and native-result representations. It retains no whole body in memory; authenticated parts are streamed one at a time. This verifier is not an exhaustive retained-root inventory or a collector. A future collector must select typed edges by root role rather than treating every descriptor inside a closed bundle as perpetual execution input.

Only the selected successful closed attempt transfers into this push row. An older independent unknown attempt still retains its pin. Expiry alone never authorizes its deletion, a fresh command identity, or takeover.

## Automatic service retirement

`RecoverySupervisor::start_retiring` combines restart discovery with retirement using the repository's existing publication coordinator. The service supplies current administration and actual owner custody. A settled eligible head is privately verified and submitted through the account-fair maintenance queue. The 8 KiB reservation covers two bounded release command copies and remains charged while acceptance is uncertain.

Before each bounded keyset scan, the supervisor also recovers uncertain factory-owned terminal-release jobs from that same coordinator. This is necessary after a committed release removes its pin but loses its acknowledgement: a pin-only scan would no longer discover the still-charged command. Recovery retains the original SDK command, identity and receipt, including after coordinator close or scanner stop/restart. It does not retry compaction or replace a live producer's uncertain command.

Stopping the scanner requests stop between scans and joins its current work. It does not cancel an admitted release. Close and drain the publication coordinator separately. On owner succession, restart the service with current maintenance authority; original receipt lookup remains independent of that fresh authority. Production routing must use the SDK's ownership-aware transport rather than keeping a stopped owner's fixed local handle.

## Qualification and remaining work

Native SHA-1/SHA-256 tests exercise quota release at the existing 4,096-pin cap, missing selected artifacts, current Admin and owner refusals, rollback at the final delete, original-command recovery after absence/lost acknowledgement/post-execution panic, scanner restart, automatic admission, uncertainty after pin disappearance, real SDK expiry, fresh-owner restore, immutable archives, removed original command bodies and current Read revocation. Existing multi-page policy tests retain their original page receipts after terminal archival. A separately prepared losing release is refused without replacing the winner's receipt.

These fixtures prove the covered transaction, receipt and lifecycle invariants. They do not establish throughput for 10,000 developers. Proof preparation is currently serialized by the repository scanner; node-wide fair verification admission, provider I/O budgets and full-history measurements remain mandatory. Production registration/startup/producer/reader conversion, initial staging uncertainty, retained-input Claim/adoption/repreparation, complete typed collection and isolated restore, file-backed intents/reports, OS containment, accelerated reads, physical rewriting and continuous hot-root maintenance remain open under the [implementation plan](../large-repository-implementation-plan.md) and [large-team requirements](../large-team-scalability.md).
