# Mandatory publication registration

Status: receiver and registered-admission increment qualified locally on 2026-10-03. All existing publication callers in the qualification fixtures now use exact registration. This contract defines the required hard cutover; production startup, producers, readers and fresh-schema conversion remain incomplete.

## Receiver contract

Commands 33, 36 and 38 must find an authenticated recovery record on their exact preparation pin before executing their domain action. The record must match the actual SDK mutation stamp, repository check, tenant, application and incarnation. A missing registration, a competing SDK identity, or a premature frozen refusal returns `NotStarted`, leaves SDK resolution `Absent`, and changes neither domain state nor the registration journal. Retrying the same original command after registration is allowed.

Register the exact command body and SDK snapshot before submission. Reuse `Record`, `Bundle`, `SavedCommand`, `Journal`, `Frame`, immutable input roots and `catalog_leases.recovery`; no new durable queue or per-object table is needed. Each policy bundle contains its original page command and one pre-frozen refusal command. Successful pages share that refusal. Advancing the pin requires the authenticated settled predecessor, with strictly increasing steps and a known successful page. A refused policy page cannot advance into positive publication.

Domain effects, the original result and sequence, and the phase revision commit in the same SDK transaction. A trusted domain denial is durable knowledge; normalize its typed root reply without inventing another receipt. A later SQL or encoding failure rolls back every effect and SDK acceptance. Retry the original prepared command after repair.

Recovery resolves the authenticated journal and original SDK evidence before reopening bodies or checking fresh custody. Only authoritative absence can execute restored original bytes. Positive cold recovery requires valid custody; a frozen refusal retains its refusal-only role and still checks its actual owner, operation, pin, floor and checkpoint. Original known outcomes remain recoverable after SDK expiry or owner loss. Product response streaming separately requires current Read authorization.

An exact registration retry can refer to a predecessor that has already become historical. `settled_frame` must supply artifact storage to `current_journal` so the existing authenticated history reader can recover that predecessor's original journal. Current-head equality alone is insufficient. The reader checks MACs, matching checks and strictly decreasing steps one bounded frame at a time.

## Protocol and admission

Use recovery purpose `canopy.publication-command-recovery.v3\0` and these command codecs:

| Command | ID | Codec |
| --- | --- | --- |
| RegisterRefPolicyPage | 33 | 2 |
| CompleteRootPush | 36 | 2 |
| CompleteRootOutcome | 38 | 3 |
| RegisterRootRecovery | 39 | 3 |

Keep the existing record and artifact structures. Do not add a compatibility decoder or an unregistered execution fallback.

Live factories persist their exact bundle, then bind it into `ReadyBoundRecovery`, preserving the original session, shared clock, lifecycle fence and policy intent. Cold registered work uses `ReadyRootRecovery`. Both use the existing fair publication queue. Raw `ReadyRootPush` and `ReadyRefPolicyPage` values have no admission variant or `From` conversion; persist and bind before submitting. Their private factory values remain available for exact registration and refusal composition. The obsolete direct dispatch code and unused per-page refusal-state allocation are removed. The current reservation formula charges two copies of the body, recovery header and optional refusal: 32 KiB for a final root command and 544 KiB for an armed policy page. Uncertain work stays charged through cancellation and service closure.

## Remaining implementation sequence

1. Convert production registration, startup, HTTP/SSH/generated producers and readers, and fresh schema together. Remove legacy producer/reader routes. Wire mandatory registration before every page/final submission and retain uncertainty across pre-admission failure, cancellation, process loss and owner loss.
2. Complete initial/Claim/Renew/denied/pre-admission recovery, retained-input adoption/repreparation, typed collection/backup/isolated restore, resource containment, accelerated reads/physical rewriting and fair continuous maintenance.
3. Qualify full Linux/Kubernetes/Chromium histories, hot-root progress and the required large-team mixed workload. Exact-head Linux/provider CI is a publication gate for this increment; local primitive and compatibility checks are not repository/team capacity proof.

## Completed policy admission conversion

All five native policy-refusal families now register their exact page/refusal bundles before admission. The original frozen command is shared across successful pages and registered after the settled head for a later terminal refusal. Registration and binding precede forced renewal; this preserves the original lifecycle clock and avoids trying to register through an already pending renewal. Current Write revocation still selects the authorized frozen refusal rather than minting a new Write-dependent session.

The fallback fault seam is test-only, consumed once at the actual registered terminal-command boundary, and shared across queue copies. It preserves absence, lost acknowledgement and post-execution panic tests while recovery uses the same original command. Late SQL rollback tests first commit the original page's exact negative phase, then trigger faults at the actual response-root update and operation delete. They require the fault message, SDK absence and unchanged domain and phase state; a gate refusal cannot satisfy those assertions. SDK acceptance of a trusted negative phase is checked by decoding the exact typed denial from its original stored result.

All three native policy-dispatch families now freeze one refusal, register pages after settled predecessors and bind before handoff. Held-worker and stopped-handoff cases remain covered. The ownership assertion drops caller references before reacquiring the inputs retained by the exact page job. A final command registered during known page uncertainty is retained after duplicate handoff and submitted unchanged after recovery. Known page receipts survive authority loss; authoritative absence under changed policy or Write access selects the original all-ref rejection report, while expired custody remains a typed terminal denial with no selected response.

Removing raw admission found one deliberate premature-refusal test still submitting an unregistered factory value. It now invokes the original receiver command directly and requires `NotStarted`, original SDK absence and unchanged domain and registration state. Compile-time admission requires a registered capability; receiver tests still qualify attempted bypasses.

## Current evidence

The new genuine native SHA-1/SHA-256 regression first failed because an unregistered refusal was accepted. It now checks unregistered and competing page/root/refusal identities, registration metadata immutability, premature refusal, original submission, receipt recovery, exact replay and late foreign identities.

Eight dispatch/plain-outcome/ref-free families and six joint-completion families have been converted. The publishing owner-loss family now registers after its page head, repeats exact registration, refuses a losing candidate, discards local registration knowledge and restores from the durable pin. Its original repeat-registration failure reproduced twice; the probe located failure after initial registration and before its repeat. Supplying storage to the authenticated predecessor reader makes all three durable-root families pass.

The joint-completion stack overflow reproduced alone. An owned composition task retains the original preparation and workspace while separating setup from polling. Subsequent broad audits found retirement and policy-history stack overflows. Owned retirement and synchronous construction of rooted qualifiers avoid that overlap; ARM64 debug disassembly shows the common native poll frame reduced from roughly 951 KiB to 707 KiB. The original isolated policy-history case passes. No stack sizes, SDK lifetimes, production deadlines or resource/capacity limits were widened.

One retirement run then failed an SDK-expiry assertion after its single monotonic wait. An isolated probe passed four milliseconds after expiry with unchanged identity; the original timing failure's cause remains unproven. The fixture now checks the actual wall-clock expiry boundary after each wait, matching the SDK clock, before requiring both original identities to resolve Expired. The original retirement scenarios pass in the final broad audit.

The policy-dispatch baseline reproduces all three family failures before conversion. The registered focused run passes all three in 38.03 seconds; the five policy-refusal families pass in 21.22 seconds. The earlier serial and normal-concurrency audits both recorded **501 passed and 10 failed**, identifying the ten remaining standalone policy fixtures. Those original failures remain retained as the migration baseline.

All ten standalone policy families now prepare native catalog bytes inside an admitted upload namespace, retain the original wire request and a scoped synthetic native-result checkpoint, and register their original page plus the frozen refusal before execution. They reuse catalog assembly and existing staging/retention types. These synthetic results isolate policy semantics; genuine CGI qualification remains in the independent native capture families. Terminal negative cases use independent staged attempts. Authority changes and abort happen after exact registration when the test targets a domain denial. Exact receipt replay uses the original prepared command, while a newly registered logical replay has its own receipt. A competing identity after abort or owner restoration stays `NotStarted` and SDK `Absent`.

The converted late-cursor test requires the actual SQL fault message, SDK absence, and unchanged domain and registration state, then retries the same command. Cleanup still checks indexed lookup plans, bounded watch reaping, transactional watch-budget rollback and owner-restored original receipt recovery. Negative policy results commit their typed denial and phase atomically while leaving domain state unchanged. The focused run passes all ten families in 8.39 seconds. Thirty-three existing caller families are converted, alongside the new genuine native mandatory-registration family.

Final frozen-source macOS ARM64 qualification passes **655 unique Rust tests**: 531 library tests (6 Git-format / 14 object-storage / 511 server), 104 multi-server tests and 20 CLI/contract/recovery/Smart HTTP tests. The server library finishes in 235.55 seconds and multi-server in 376.05 seconds. Subprocess summaries and focused reruns are excluded from the count. All eight isolated RustFS compatibility cases pass, including SHA-256, signed pushes, SSH, the 4,096-ref mirror, filtered clones and LFS. The separate large-transfer case still requires a dedicated disk with at least 40 GiB free. All 96 Python qualification tests pass in 40.924 seconds. Workspace/all-target Clippy passes with warnings denied in 24.00 seconds. The server binary builds successfully in 80 seconds. Formatting, diff checks, all 409 frozen Rust-source hashes, the protected checkout index, archived document and exact Cellule pin checks pass. Temporary probes are removed. No stack, lifetime, deadline, resource or capacity thresholds were widened. Exact-head Linux/provider CI and full-scale qualification are separate gates.

The published listener fix is independent: PR #32 was merged on 2026-10-03 at `e0957301fa388a69f669cffb31b2126cd982f34d`. Both complete Verify runs passed on its published head `370408f8184c2d0fccb273b6fe6b60ed896123d1`. The fetched `origin/main` and that head have the identical complete tree `45e96c20abad39df2d40c7554c5de2b720ab0dae`; the squash merge therefore includes every published change. The isolated working branch is aligned with that main revision, preserving the local registration increment. There is no open PR #32 conflict to resolve. Those CI checks qualify the published listener tree, not these local registration changes. PR #33 merged at `9438bb865959fb975d5349ba8b9908b461653821`. Both complete exact-head Linux [push](https://github.com/crabbuild/canopy/actions/runs/37164049029) and [PR](https://github.com/crabbuild/canopy/actions/runs/37164077931) Verify runs pass at `32f5559216432c0437ac3e864d71c454ee779e1e`, including 659 unique workspace tests, eight RustFS cases and build. Main and that published head have the identical tree `aacb41ee48e953cf106c75f8319667fa83639b96`. The local production cutover starts from that merged main; its changes require their own qualification. The full implementation and capacity goal remains open.
