# Shared foreground and maintenance publication dispatch

`PublicationCoordinator` now dispatches privately prepared push completions and catalog compactions through the same bounded admission, fair queues, durability waits and uncertainty recovery. It reuses Cellule's exact `PreparedCommand`, preparation leases, purpose-bound certificates, catalog CAS and immutable outcomes. Production HTTP/SSH integration, continuous maintenance preparation, durable service reconstruction and capacity qualification remain open.

## Typed factories and outcomes

`PreparedCatalog::ready_push` retains the verified catalog and exact command 19 when publishing refs. `PreparationSession::ready_outcome` retains only the admitted session and exact command 19 for refused/empty outcomes; it requires no catalog artifacts. The prepared-catalog wrapper delegates those outcomes to the same session factory and drops catalog ownership from the ready value. See the [outcome-only contract](outcome-only-completion.md). `PreparedCompaction::ready_compaction` retains the verified compaction and exact command 22; it issues the existing maintenance certificate, checks a 4 KiB input envelope and checks the live lease before and after SDK preparation. Both factories perform verification/certification before admission. Raw descriptors and a caller-selected class cannot construct either ready object.

`ReadyPublication` wraps those private factory outputs. `submit` accepts either output and returns the same `PublicationTicket`. Admission failure returns the original typed ready value, preserving its mutation identity and wire bytes. Logical IDs are unique across both classes in one coordinator.

`PublicationOutcome` distinguishes committed push and compaction results. `PublicationError` preserves the corresponding typed Cellule invocation error, evidence and rejected receipt. `PublicationState` retains the common queued/running/uncertain/finished lifecycle. `ticket.class()` identifies the class. `ticket.response()` accepts only a completed push outcome; a compaction never becomes an HTTP push response. These APIs replace the previous push-only outcome shape; there is no compatibility adapter.

## Bounded class and account admission

| Default | Bound |
| --- | --- |
| Total admitted operations | 32, including uncertain work |
| Maintenance operations | Four reserved slots |
| Foreground operations | Remaining 28 slots |
| Per actor | Eight operations per class |
| Encoded command reservation | 8 MiB per push; 8 KiB per compaction |
| Total command-byte budget | 256 MiB |
| Concurrent durability waits | Eight |
| Maintenance durability waits | At most two |
| Foreground dispatch burst | At most three before eligible maintenance |

Maintenance reserves both operation slots and its maximum encoded-command credits. Foreground cannot consume those reservations; maintenance cannot consume foreground reservations. Counts for the same actor are separate by class, so foreground activity by the administrative actor does not consume its maintenance quota. Account rotation remains FIFO within each class. All maps, queues, actor strings and tickets are bounded by admitted operation counts.

The command reservation covers two bounded encoded copies: the retained command and the dispatch copy. Dispatch consumes its copy, rather than cloning a third payload. Private verification/native scratch keeps its separate disk/process admission. These credits do not claim to account for the entire service's heap, CPU, native descendants or provider traffic.

Configuration requires room for another foreground account, nonzero reserved maintenance slots, checked byte headroom, a burst in 1–32, and a valid maintenance concurrency bound. With multiple durability waits, maintenance cannot use every slot. A one-wait profile permits one maintenance wait; fair class starts then share that serialized dispatch slot. Invalid profiles reject before a coordinator is created.

## Fair starts and exact recovery

The shared queue contains two instances of the existing account-fair queue. With both classes ready and maintenance concurrency available, dispatch at most the configured foreground burst before a maintenance start. At its concurrency cap, maintenance stays queued while ready foreground work proceeds. The burst controls starts, not CPU time, I/O shares, network arrival order, SQL order or publication success.

Catalog/ref CAS and current policy/ACL checks remain in the authoritative command. Two preparations against one old catalog can conflict even when both dispatch fairly. Uploads, native decoding, canonical verification and reconciliation never run inside this queue. A known durable catalog conflict may reenter only with a newly prepared command identity and a properly reconciled certificate.

Dropping an observer does not cancel admitted execution. Pending, malformed published and panicked-task outcomes retain the original ready value and reservation. `pending`, `recover` and `close_and_drain` handle both classes. Recovery joins the same class/account queues. Staging Begin/Renew/Bind now reuse this same exact invocation/resolution implementation with a 4 KiB decoded-result bound; push and compaction results retain their 128-byte bound. Staging has its own long-input admission/lifecycle rather than entering the final-command fair queues; see the [service contract](staging-service-lifecycle.md). Resolve authoritative absence before executing the retained exact command; decode a committed result with its original receipt without rerunning its handler. Unknown, expired, unreachable or changed-incarnation evidence remains uncertain. Never replace its proof or mutation identity while acceptance is unknown.

A terminal result drops dispatch/retained proof ownership before releasing class/account/byte credits. Resolved tickets retain only bounded result/read context. Recovery remains possible after closing admission. The coordinator is service-owned local state, not a durable outbox or permission to delete remote inputs.

## Integration and evidence

Keep one coordinator and geometric planner per repository. Obtain a fresh admitted query-derived maintenance base, call the [geometric planner](geometric-directory-maintenance.md), wrap the verified result in an Arc and call `ready_compaction`, then `submit`. Observe or recover the exact ticket before releasing uncertain inputs. Obtain a fresh frontier for the next preparation. Integrate process admission, fair CPU/I/O shares, renewal/reaping, owner-loss reconstruction and complete retained-root inventory before selecting production handlers.

Tests exercise class/account admission, retained failure values, duplicate logical IDs, maintenance concurrency while foreground completes, canceled observers, current admin revocation, and SHA-1/SHA-256 absent/lost-acknowledgement/panic recovery with original receipts and exactly one logical outcome. The existing push dispatcher tests remain in place with typed-result assertions. The geometric native fixture now prepares and publishes repeatedly through this shared dispatcher until ingress and level debt drain, checking canonical/source/version identity, unchanged refs and old-reader access.

These fixtures establish bounded dispatch and recovery. They do not establish stable maintenance service under 35 pushes/s, full-history amplification, durability grouping, source-independent restore or capacity for 10,000 engineers. The mandatory workload and recovery campaigns remain release gates.
