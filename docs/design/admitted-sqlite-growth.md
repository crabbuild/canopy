# Admitted SQLite growth for repository construction

MetadataBuilder, DirectoryBuilder and ClosureVerifier now grow their private SQLite databases under the existing Cellule DiskBudget. They reuse the same SQLite tables, canonical identities, typed edges, native ordinal partitions and immutable artifact descriptors. No serving format changes or compatibility adapter are involved.

## Allocation and transaction contract

MetadataLimits.max_file_bytes remains an explicit main-file ceiling, validated as a multiple of 4 KiB. Each builder initially admits three times the smaller of 64 KiB and that ceiling, before creating its file. SQLite uses 4 KiB pages, DELETE rollback journals, its configured bounded cache and no mmap. The threefold charge covers main-file capacity, rollback journal and conservative overhead; it is a reservation rather than a claim about actual bytes written.

Every mutable construction path executes through the shared admitted transaction helper. Only SQLite's DiskFull error can trigger growth. Preserve its source through MetadataError so validation limits, integrity failures, cancellation, I/O failures and identity conflicts cannot masquerade as a retryable capacity condition. A failed transaction must leave the connection in autocommit before replay is allowed. Otherwise fail closed.

Before a retry, double the cap, clipped exactly to the configured ceiling. Grow the existing DiskReservation first; then raise max_page_count and verify the resulting cap. Failed admission leaves the old cap and credit intact. A SQLite cap update failure retains the enlarged reservation conservatively until cleanup. At the configured ceiling, return MetadataError::Limit. Actual filesystem exhaustion may produce the same SQLite error; retries are finite and remain bounded by the ceiling and shared budget.

The replay body has no externally visible effects. Inputs remain owned until every attempt finishes. Commit before adopting its returned cursors, counts, digest folds or graph-processing state. Native metadata sealing consumes index ordinals once outside the replay body, then processes the same bounded object page on each SQL attempt. Closure copies recompute local inventory state per attempt. Topological processing copies its constant-size active reverse-fanout cursor and adopts it only after commit.

Verified native objects retain their admitted edge files through the entire metadata batch. Replay seeks to zero, checks exact length, hashes every occurrence byte, validates typed edges and checks the final digest again on each attempt. The outer consuming batch retains ownership and permanently poisons sealing on an unrecovered failure. A partial replay cannot become a complete witness or acknowledged publication.

Closure lookup creation pages incoming OIDs and distinct child OIDs through their existing indexes, at most 512 keys per transaction. Pending-degree initialization also pages incoming vertices. Topological processing still performs at most 512 vertex/edge updates per transaction, and pages reverse fanout independently. A single large object's edge copy remains atomic and can require multiple cap increases; graph history and object bodies do not enter the heap.

## Ownership and operational limits

Sealing closes SQLite, removes its journal, synchronizes and hashes the file, then shrinks admission to the exact immutable length. Failed or canceled work keeps the existing AdmittedFile ordering: SQLite closes and files/workspaces are deleted before admission is released. Queued work retains ownership; cleanup failure retains disk credit for recovery.

At the 256 MiB main-file default, one empty construction spool initially charges 192 KiB instead of 768 MiB. Two such spools initially charge 384 KiB. Growing to the full ceiling can still charge 768 MiB per spool, and old immutable files, cached downloads, native workspaces and edge files retain their independent reservations. Geometric growth can deny a larger quantum before all shared free space is consumed; this is bounded backpressure, not permission to oversubscribe disk. Do not equate the initial charge with peak operation capacity.

Ref ancestry scratch still reserves its explicit maximum up front. Native Git descendant RSS, CPU and I/O admission, large-file profiles, full-history throughput, durable authenticated input adoption, producer integration and continuous maintenance remain separate release gates. These construction changes do not select the production hard-cutover registry or prove capacity for 10,000 engineers.

## Evidence

Dedicated growth checks exercise rollback after a written prefix, repeated growth, main-file/journal bytes under held credit, denied admission preserving prior committed rows and the old cap, a non-power-of-two ceiling, and non-capacity failures that must not retry. Existing native SHA-1/SHA-256 verification now constructs a 1,600-blob inventory and wide tree with a 16 MiB file ceiling under a 2 MiB shared budget, checking exact metadata/edges and edge-file credit release. Existing deep-chain/wide-fanout, artifact-integrity, incomplete-input, canceled-worker, old-reader and compaction checks exercise the same growing builders. These are correctness/resource fixtures; full-history and mixed-load campaigns remain mandatory.
