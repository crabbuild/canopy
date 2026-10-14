# Outcome-only push completion

The immutable-root replacement is implemented by PreparationSession::root_outcome_completion, ready_root_outcome and command 38. It reuses the outcome certificate, operation/input pin, selected-result row and shared dispatch while storing response/options/signed audit bytes in authenticated native metadata. See the [immutable outcome contract](immutable-push-outcomes.md). The inline APIs below remain temporary implementation pending the mandatory producer/reader hard cutover; they are not a backward-compatibility architecture.

Refused and empty native pushes must save their exact response without rebuilding or uploading a catalog. PreparationSession owns the existing authoritative lease capability separately from PreparationBaseResolver's catalog reader. Both share the same monotonic deadline and irreversible renewal-failure fence. Opening a session validates the canonical repository target and fresh CheckPreparation result; it does not accept a caller-supplied trusted token alone.

## API and stored structures

After BeginPreparation, open PreparationSession at the committed receipt. Call push_outcome to prepare a CatalogPushCompletion, complete_outcome for direct final invocation, or ready_outcome to retain the exact SDK command in PublicationCoordinator. Native ref-success reports and any Some(plan), including an empty plan, reject on this path. Checked ref plans continue through PreparedCatalog and its membership/ancestry proof.

The session factory takes no artifact loader, provider, scratch path, DiskBudget or native scope. It queries current preparation authority and the repository secret. It reuses the 1 KiB certificate envelope, existing lease/token and GenerationFact structures, command 19, pushes/response/options/signed-certificate tables and foreground dispatch queues. No new authoritative SQL row or storage layout is introduced. The envelope body is bounded at 960 bytes. The ordinary path remains Begin plus final completion, with read-only issuance queries.

OutcomeCertificate uses the purpose domain canopy.push-outcome.v1. It binds tenant/application, repository operation and attempt, admitted owner fence, actor, object format, original immutable generation floor, and the existing payload digest covering response identity/status/ordered headers/body/options and signed-witness bytes. The typed codecs reject cross-purpose catalog certificates. Native signed witnesses remain opaque, verified gateway outputs; the factory checks their target/request/actor context.

## Final authority and response recovery

CompleteCatalogPush authenticates the proof and exact payload before using it. A new outcome checks the actual owner fence, exact operation/pin/expiry, current write ACL and immutable floor. It saves the response in the same transaction and removes the operation, while the independent retention pin remains until expiry. It increments no catalog generation and changes no refs. A concurrent catalog advance does not invalidate a response-only proof, because it asserts no new object visibility or ref identity.

An already completed identical payload replays its original result before live-owner checks; this is recovery authority, not new write authority. Exact SDK replay preserves the original committed receipt. A fresh logical completion identity returns the stored logical result. A different actor, request digest or completion payload cannot replace the result. New unfinished proofs from a prior owner reject after actual restoration.

The same admitted invocation can load its own exact response at the committed receipt, even if permissions changed while it ran. This grants no repository read capability. Reconnect/preflight uses replay_push_response and CheckCompletedPush, requiring current read ACL and exact actor/request context before loading stored bytes.

After admission, there is no local lease timeout around the final command. Unknown acceptance retains the exact SDK identity, command bytes, session and queue reservation. Observer cancellation cannot drop them or synthesize a refusal. Recovery uses the existing authoritative resolve/absence/execute path. Terminal ownership releases before queue credits.

## Scope and validation

Tests cover SHA-1/SHA-256 refusal, native error and empty response persistence without catalog artifacts, a moving floor with an unavailable old artifact store, payload tampering and cross-purpose rejection, current write/read revocation, expiry, canceled observers, absent/lost-reply/panicked dispatch recovery, and genuine owner restoration with original completed receipts and stale unfinished rejection. Trusted fixtures supply native signed witnesses and immutable roots; these tests do not qualify production signatures or full-history throughput.

The fresh schema is still not selected by production handlers. HTTP/SSH producers must perform completed-request preflight before Begin, route failed/empty requests to this session API, retain exact uncertainty after admission and use the same atomic completion for ref publication. Production reader conversion, service reconstruction, large inline payload roots, full-history resource bounds and mixed-load qualification remain required.
