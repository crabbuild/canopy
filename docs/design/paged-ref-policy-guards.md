# Paged ref policy guards

The fresh packed-publication module prepares current direct-push policy predicates in bounded transactions. Command 36 now consumes their live readiness in short atomic catalog/ref/native-outcome publication. Production hard cutover remains required. A guard, a page receipt, a conditional snapshot or a decoded certificate alone grants no publication authority.

## Intent and custody

`PreparedCatalog::ref_policy_preparation` owns the original `PushPlan` and catalog-verified ancestry evidence. It captures the current policy configuration epoch before verification. The intent contains a fresh UUID, update count, canonical plan digest and evidence digest. Its persisted scope also binds the existing preparation token, actor and object format. The token includes the actual owner fence and creating artifact namespace.

The guard is independent of the selected catalog/ref generation. Unrelated catalog advancement can therefore leave policy readiness intact. A rebased prepared catalog must still verify membership/ancestry, the original plan/evidence digests and all conditional ref expectations against its own selected root. A catalog certificate binds that exact selected generation and proposal separately.

`RefPolicyPreparation::page` copies at most 128 updates and encodes at most 256 KiB including certificate and framing. It counts encoded update bytes before copying, so long ref names reduce page length. Each page remaps ancestry bits from the original offset, including offsets within a byte. The existing catalog MAC uses a separate page binding over intent, offset, exact page plan and ancestry; another certificate purpose cannot register a page.

## Service-owned registration

`Arc<RefPolicyPreparation>::ready_page` prepares exact command 33 after the existing private page factory checks catalog/intent custody and the byte envelope. Its private output retains both the original intent/evidence and verified prepared catalog. `ReadyPublication::PolicyPage` shares foreground/account admission and exact recovery with root completion, reserving 512 KiB for two encoded copies bounded by 256 KiB each. Known results decode with their original receipt before fresh custody checks. Only authoritative absence can submit the same command after a live-custody check; unknown/expired evidence remains retained and charged. Regenerating a guard UUID, page, certificate or mutation identity is not recovery.

`StagingTicket::register_policy_page` reuses the lifecycle's existing held publication slot and observation ticket with an explicit intermediate role. It requires the exact shared session fence and clock. Owned workers/results, due renewal and queued input checkpoints drain before activation. Intermediate pages cannot enter the final-completion API. A known successful page resumes Bound rather than acknowledging or terminating a push; a known refusal fences preparation. The latest known page receipt joins the existing minimum-receipt refresh before the next handoff, without changing the original Bind receipt. Historical progress never grants fresh guard authority.

One page per logical operation may be outstanding. While its result is uncertain, the lifecycle blocks another page, bound work and final completion, even if the Cell has already durably registered the complete guard. Dropped observers do not cancel work; the existing pending observer/coordinator recover the original evidence. Shutdown retains started uncertainty until resolved. A stopped held intermediate page drains its accepted workers and is discarded before execution; accepted final commands retain their completion semantics. Closing after a committed page preserves its original receipt but cannot reopen preparation.

These are local service-ownership semantics. Durable process-loss reconstruction, complete typed retention and production orchestration remain required. A failed policy page also needs production refusal orchestration before the hard cutover: fencing safely refuses publication but is not itself a durable final native-response outcome.

## Transactional registration

Command 33, `RegisterRefPolicyPage`, checks shape and purpose MAC, scoped repository, actual admitted owner fence, current write access, format, live matching operation, expiry and independent retention pin. It checks that the certificate's selected generation remains an authenticated retained fact and that the original floor/input custody still match. The selected generation need not be current; registration does not publish roots.

Pages extend a contiguous cursor. A new guard starts at offset zero. A signed future page, a partially overlapping page, a changed scope or an invalid guard refuses before writes. An already covered exact issued page returns fresh current progress without advancing the cursor or adding watches. The original SDK mutation identity also preserves its exact receipt; that receipt may describe historical readiness and must not authorize later publication.

The command checks enabled branch rules with MAC-verified ancestry and current check-context versions/reporters. Direct pushes cannot bypass a required pull request. It installs dependencies for exactly the newest passing attempt of every required context. Watch identity is `(guard, OID, context, context version, run number)`; identical dependencies within/across pages are deduplicated.

All policy checks, dependency selection, capacity checks, watch installation, budget accounting and cursor advancement execute in one Repository Cell transaction. There is no observation gap between checking a run and watching it. Every rejection precedes the first write. A later SQL failure aborts the guard, watches, scalar budget and cursor together at the existing Cell durability boundary.

## Freshness without a global check-report epoch

Branch-rule, required-context and check-context insert/update/delete operations advance one monotonic configuration epoch. These changes are rare relative to ordinary CI reports. Readiness requires exact equality with the captured epoch; returning a rule/context to its old values does not restore an old guard.

Ordinary run reports invalidate exact indexed dependencies:

- A newer attempt of the same OID/context/version invalidates an older watched attempt even when the new run is queued or failing.
- Updating or deleting a watched run invalidates it, including a successful-to-successful edit, reporter change or move to another tuple.
- Updating a different run into the watched tuple invalidates whenever its run number is at least the watched number.
- `INSERT OR REPLACE` observes either existing unique key before insertion. This catches replacement that moves the run's OID/context/version even when SQLite suppresses replacement DELETE triggers.
- Reports for another OID/context version and updates/deletes of older attempts do not invalidate the newest watched dependency.

The secondary watch index begins with `(OID, context, context version, run number)`. Guard-local lookup and cleanup use the existing watch primary key. Reports do not scan every page or advance a global report epoch.

Query 34, `CheckRefPolicyGuard`, rechecks current write access, persisted repository identity, matching live operation/expiry and its pin, scope and configuration epoch. `ready` requires a valid guard and cursor equal to the original count. A read query is advisory and cannot establish a new write's actual owner fence. `CompleteRootPush` checks the live guard and epoch in the same transaction as its current authority, root CAS and outcome writes.

## Conditional ref root

`PreparedCatalog::guarded_ref_snapshot` accepts only a private ready guard from the matching preparation. It re-verifies the exact original plan against its own prepared catalog, requires identical catalog evidence, prepares all conditional ref changes against its selected immutable snapshot and performs a fresh readiness query before issuing a separate root-purpose MAC. The existing coalesced `RefStateIndex` transition, `RefStateSnapshotRoot` and bounded catalog certificate are reused. Missing immutable ref state never falls back to SQL refs or an empty tree.

The returned `RefRootPublicationProof` contains the certificate, intent and ref snapshot descriptor; it does not carry the entire ref plan. It is conditional transport, not the final root/outcome command. The inline SQL ref publisher refuses this purpose. Native response custody must still be linked to the original plan and included in the atomic final command.

Final completion preparation now reuses the existing immutable native-result metadata and artifact descriptors through [typed immutable outcomes](immutable-push-outcomes.md). Its private factory authenticates registered custody and compares that result's original plan with the guard before signing the catalog/ref/result binding. Completed outcome lookup derives the selected result from the durable actor/logical-operation/request identity, never a caller root. Completed response reads authenticate and stream that selected body. Retention must preserve exact response, plan, options and signed-body artifacts without requiring the original wire request/body forever. Private input checkpoints and their independent pins continue to retain that input chain while preparation or uncertainty needs it. The same metadata bytes have distinct typed private-input and completed-outcome traversal obligations; the collector still needs implementation and fault qualification.

The private completion factory also prepares deterministic all-ref rejection response descriptors outside the final transaction, bound to the same exact original native report/plan and signed custody. Current policy/ACL or signed-certificate-replay refusal must atomically select the appropriate durable rejection, rather than retain a cached successful response or assemble an unbounded report inside the Cell. A changed catalog CAS must preserve the existing reconciliation/retry distinction; it cannot publish an older prepared root. Completed replay must return the originally selected outcome even when later checks or owners change.

## Limits and cleanup

| Resource | Bound |
| --- | --- |
| Ref updates in original intent | 100,000 |
| Updates per policy page | 128 |
| Encoded page | 256 KiB |
| Catalog certificate | 1 KiB |
| Guards per Repository Cell | 4,096 |
| Required contexts per branch | 16 |
| Watches per Repository Cell | 2,097,152 |
| Watches deleted per cleanup command | 512 |

The watch budget is one checked integer updated from exact inserted/deleted row counts. Registration checks it before writes, rather than counting the entire watch table per page. Guard admission counts at most the fixed guard cap. Epochs, cursors, totals, run numbers and context versions require integer storage; guard identity is immutable, cursors cannot move backward and validity cannot resurrect. Live watches cannot be deleted or replaced.

Catalog ancestry policy lookups independently limit the exact existing SQL wire encoding to 256 KiB and 128 statements. A count bound alone is insufficient for long ref names. The production SQL transport remains at its existing 1 MiB limit; the fixture derives that descriptor rather than assigning a smaller artificial limit. Bit positions advance by the actual selected page length.

Command 35, `ReapRefPolicyGuard`, requires the current admitted fence and admin access. A live valid guard at the current epoch cannot be reaped. Expired, inactive, old-owner or invalid guards can release at most 512 indexed watches per command, with transactional budget decrement. A late budget failure restores all deleted rows. There is no cascading whole-guard delete.

An invalid guard whose creating operation is still live retains its tiny tombstone even after all watches are gone. Deleting it early would allow an old signed first page to recreate the guard. Once that operation is inactive, or the actual admitted owner has changed, the empty guard can be removed. An old page must then fail operation/fence checks before recreating any state. Exact original mutation replay can return its historical receipt after restore but grants no new write.

These records retain no remote artifact deletion authority. Cleanup scheduling, enumeration, fair cleanup preparation and process-loss service reconstruction remain required; registration now shares the account-fair dispatcher.

## Verification and open gates

The focused native SHA-1/SHA-256 fixtures exercise private certification, independent rebase, exact framing/truncation, long-name byte paging, update-count paging, nonaligned ancestry remapping, ordering/MAC refusals, exact mutation replay, late cursor rollback, guard/watch admission, watched mutation/replacement/deletion, configuration invalidation, immutable state, bounded cleanup, late cleanup rollback and actual owner restore. Synthetic quota/cleanup fixtures exercise fixed boundaries; they do not qualify large-team capacity.

The original plan remains a `Vec`, ancestry remains a bounded bit vector and conditional tree preparation still owns its changed-ref map. File-backed whole-operation intent/RSS bounds remain required. The final short root/native-outcome command is implemented; reviewed merges, production producer/reader conversion, fair hot-root progress, continuous maintenance and full-history mixed-load qualification remain open. The number and cost of page commands must be included in the mandatory 10,000-developer workload measurements.
