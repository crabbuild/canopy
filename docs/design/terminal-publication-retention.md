# Terminal publication recovery retention

Completed pushes and closed initialization or reviewed-merge attempts must release their independent preparation pins without losing original command receipts. Leaving successful pins forever eventually exhausts the 4,096-pin admission cap and retains unnecessary catalog floors. This protocol transfers the same authenticated recovery certificate and phase journal into the immutable shared `catalog_recovery_receipts` table, then deletes the matching pin in one transaction. It uses the fresh publication schema and existing certificate, journal, SDK receipt and artifact structures; it adds no durable queue or per-object rows. The archive is keyed by original incarnation/admission sequence, with an operation index for typed enumeration. Different attempts of the same logical initialization retain independent original receipts.

The protocol releases a preparation pin. It does not authorize provider deletion. Complete retained-root enumeration, reader and worker drain, backup, isolated restore, and repository-scoped collection remain required before any production artifact deletion.

## Release eligibility and authority

`RegisteredRootRecovery::ready_terminal_release` accepts only the current canonical recovery head with a recorded completed root outcome. A policy head is terminal only when its original refusal is recorded and its pre-frozen fallback root command has completed. A known initialization result is terminal after its exact attempt is closed. A positive must match the immutable selected initialization fact; a known denial may retire only after Claim or bounded operation reaping has removed that exact active binding. A successor of the same logical operation keeps its own pin. A typed merge
result is terminal only after its exact attempt closes. An applied result must
match the permanent UUID row and selected native audit; a denial retains its
original phase even if a later fresh command succeeds with the same UUID.
Unknown acceptance, passed intermediate pages, denied push root commands, historical heads, and a refused page without completed fallback cannot produce a release proof.

The private factory checks the saved actor and request selection against that exact completed outcome. It authenticates the current bundle and each predecessor frame, verifying repository MACs, tenant/application, lease identity, original SDK stamps, and strictly decreasing phase steps. Each header is bounded by 8 KiB. It then authenticates the selected outcome/native metadata and streams the selected response and native audit bodies to verified EOF. Missing or corrupt required bytes prevent proof creation. For positive initialization, the same verifier used by route activation downloads the catalog, directory and ref snapshot and checks their complete typed empty graph. The graph transcript includes the original fact and all three authenticated descriptors. A known negative has no positive graph edges; its original typed denial remains bound by the phase digest.

For native fast-forward merges, `pull_merges.publication` reuses the existing
bounded `StoredInputRoot` representation. The private factory uploads logical
merge intent, base ref, catalog descriptor, ref snapshot and ref generation;
the catalog MAC binds this audit descriptor alongside the conditional ref
proposal. Operation 9 codec 6 saves it atomically with the immutable UUID result.
SQL guards reject result mutation, deletion and replacement. Fresh deployments
use this schema directly; no backward decoder or SQL ref mirror is introduced.

Terminal verification reconstructs the exact actor/request binding and original
result from that UUID row, then reads the purpose-specific audit. It checks the
repository, original result, at most 48 directory range roots and one source
root through a fresh bounded `CatalogReader`, and the exact published base-ref
path/version through the selected immutable ref snapshot. Replays select the
first applied attempt's creating namespace, even when the new attempt proposed
no ref update. Verification does not enumerate historical objects or prove
physical closure again. The permanent audit retains its catalog/ref descendant
edges; the future complete collector must walk these edges as retained roots
before any deletion. Missing or corrupt selected metadata prevents pin release.

The purpose-specific MAC proof binds the original recovery certificate, phase-journal digest, and verified closed-graph digest. `ReleaseTerminalRecovery` is command 40, codec version 2, using purpose `canopy.terminal-recovery-release.v2\0`, with input limited to 4 KiB and output to 128 bytes. The actual command receiver separately requires current repository Admin authorization and its admitted owner fence. A proof prepared under an old owner or by a subsequently unauthorized administrator cannot bypass those checks.

## Atomic transfer and original receipts

Before its first write, the receiver verifies the proof and embedded original certificate, exact current pin identity/head/phase, terminal selection, the immutable selected push, positive initialization or applied merge outcome, and absence of an active binding for that exact incarnation/admission sequence. All semantic refusals precede writes.

The receiver obtains its release command's actual SDK mutation evidence and sequence from `CommandContext`. It inserts one shared archive row containing the original pin key and logical operation, and three bounded values: the original recovery certificate, original phase journal, and release identity/result/sequence. An exact CAS then deletes the matching preparation pin. Any later SQL failure rolls back both writes and SDK acceptance. SQL guards prohibit archive replacement, mutation or deletion; the lease deletion guard requires the same certificate and phase in the exact shared archive row.

Archived recovery reuses `RegisteredRootRecovery` and the existing predecessor-frame walk. The original root/page receipt survives SDK identity expiry, removal of original command bodies, local SQL destruction and fresh-owner restoration. A completed release also resolves its original archived receipt before SDK resolution or current custody checks. A competing, separately prepared release identity cannot inherit that receipt; once another release wins, its receiver refuses Missing.

An owner SQL query is not an arbitrary local SQLite read. The pinned Cellule executor refuses `PendingPublication` until its logical head has a durability proof. Archive lookup failures retain original pending evidence and admission credits; SQL presence before that proof cannot acknowledge a release. Product response replay still requires current Read authorization even when the internal recovery receipt is known.

## Retained artifact edges

| Root or edge | Retention after this attempt closes |
| --- | --- |
| Recovery certificate and phase journal | Stored unchanged in the shared immutable receipt archive |
| Original head bundle and predecessor frames/bundles | Required to resolve exact historical command identities and receipts |
| Selected outcome root and native result root | Required immutable metadata for replay and audit |
| Selected response body | Required; its creating namespace can belong to a prior admitted attempt |
| Native plan and signed certificate bodies | Required native audit edges, when present |
| Original wire request, original command bodies, unselected responses and unpublished candidate artifacts | No permanent edge from this closed audit role; other retained snapshots, unknown attempts, readers or backups can still require them |
| Applied merge audit input root | Retained by the immutable UUID result; includes exact intent and typed catalog/ref descendants, independently of SQL generation reaping |
| Initial empty catalog, directory and ref snapshot | Retained through the immutable initialization fact; its original pin identity also names receipt recovery |
| Published catalog, refs, packs and indexes | Retained through their own certified catalog/generation and reader/backup roots |

`root_completion::closed_graph` verifies the closed audit edges using the existing `StoredInputRoot`, `ArtifactDescriptor` and native-result representations. It retains no whole body in memory; authenticated parts are streamed one at a time. This verifier is not an exhaustive retained-root inventory or a collector. A future collector must select typed edges by root role rather than treating every descriptor inside a closed bundle as perpetual execution input.

Only eligible closed attempts transfer into the shared archive. An older denied initialization can retire independently of its successor after its active binding closes. An older independent unknown attempt still retains its pin. Expiry alone never authorizes its deletion, a fresh command identity, or takeover.

## Automatic service retirement

`RecoverySupervisor::start_retiring` combines restart discovery with retirement using the repository's existing publication coordinator. The service supplies current administration and actual owner custody. A settled eligible head is privately verified and submitted through the account-fair maintenance queue. The 8 KiB reservation covers two bounded release command copies and remains charged while acceptance is uncertain.

Before each bounded keyset scan, the supervisor also recovers uncertain factory-owned terminal-release jobs from that same coordinator. This is necessary after a committed release removes its pin but loses its acknowledgement: a pin-only scan would no longer discover the still-charged command. Recovery retains the original SDK command, identity and receipt, including after coordinator close or scanner stop/restart. It does not retry compaction or replace a live producer's uncertain command.

An active denied initialization or merge is deferred before release preparation or SDK admission. Successful repository startup retires its initial pin before exposing the route, and recovered positive startup discovers the original pin by the immutable initialization outcome’s exact incarnation/admission sequence. Pending startup retires an original known denial only after a successful Claim. Current maintenance fencing comes from the validated durable Cell Control and its live node advertisement; the receiver still independently checks its actual admitted fence and current Admin role. The existing tracked transition owns this constant-size work through cancellation.

Stopping the scanner requests stop between scans and joins its current work. It does not cancel an admitted release. Close and drain the publication coordinator separately. On owner succession, restart the service with current maintenance authority; original receipt lookup remains independent of that fresh authority. Production routing must use the SDK's ownership-aware transport rather than keeping a stopped owner's fixed local handle.

## Qualification and remaining work

Native SHA-1/SHA-256 tests exercise quota release at the existing 4,096-pin cap, missing selected artifacts, current Admin and owner refusals, rollback at the final delete, original-command recovery after absence/lost acknowledgement/post-execution panic, scanner restart, automatic admission, uncertainty after pin disappearance, real SDK expiry, fresh-owner restore, immutable archives, removed original command bodies and current Read revocation. Existing multi-page policy tests retain their original page receipts after terminal archival. A separately prepared losing release is refused without replacing the winner's receipt.

Six typed initialization families additionally qualify both formats, immutable shared archives, denied-old/successful-new receipt separation, missing typed empty metadata, actual Admin/owner checks, last-write rollback, automatic release recovery after pin disappearance, SDK expiry, saved-body loss and fresh-owner restoration. The complete frozen-source publication suite passes 264 tests in 175.94 seconds with four threads and standard stacks.

Five native merge retirement families qualify both formats where applicable:
original applied UUID replay selects the first audit after releasing its pin;
a denied attempt closes and keeps its original refusal after a fresh attempt
succeeds; missing and corrupt audit, catalog, directory, ref snapshot and ref
index metadata prevent release without losing the result; current Admin/owner
checks and a fault at the final delete preserve atomic archive/pin rollback;
and archive plus release receipts survive original command-body removal, SQLite
loss and actual owner restoration. Permanent merge rows reject update, delete
and replacement. These use verified stock-Git pack/catalog bytes and a trusted
synthetic initial certificate, isolating transaction and recovery invariants.
They do not qualify the public endpoint, actual automatic merge retirement,
generation reaping under a merge workload or complete provider garbage collection.

These fixtures prove the covered transaction, receipt and lifecycle invariants. They do not establish throughput for 10,000 developers. Proof preparation is currently serialized by the repository scanner; node-wide fair verification admission, provider I/O budgets and full-history measurements remain mandatory. Complete production producer/reader and background-service wiring, initial Begin/Claim/Renew/pre-registration uncertainty, retained-input Claim/adoption/repreparation, complete typed collection and isolated restore, file-backed intents/reports, OS containment, accelerated reads, physical rewriting and continuous hot-root maintenance remain open under the [implementation plan](../large-repository-implementation-plan.md) and [large-team requirements](../large-team-scalability.md).
