# RustFS storage optimization qualification — 2026-10-14

The current integration uses the native catalog/publication implementation from
`main` at `aa18585`, published Cellule 0.2.0, repository schema 4 and storage
format `canopy-pack-v2`. It retains same-release recovery and requires fresh
storage prefixes. Current integrated gates and RustFS measurements are pending.
All measurements below belong to the earlier, separate implementation and its
recorded binary; they do not qualify this integration.

The earlier storage changes keep Git bodies in verified, immutable native pack/index
pairs, batch metadata publication, and expose Cellule's retention collector
through drained deployment maintenance. Repository schema 3 requires a fresh
storage prefix. Schema-2 measurements below remain historical evidence. The current product contains no reader or migration path for
the earlier Git-body representation.

The full-history corpus is unchanged from the
[original benchmark](2026-10-13-rustfs-browse.json): bat, starship and libgit2,
194,252 reachable objects, 28,189 commits and 501 branch/tag refs. Its source Git
packs total 152.94 MiB. All builds use published Cellule 0.2.0, with exact pins
and the lockfile checked together. The five direct crates remained at 0.2.0 in
the crates.io check on 2026-10-14.

## Implemented priorities

| Priority | Implementation | Measured evidence |
| --- | --- | --- |
| Retention-aware GC | Verified live roots and backup pins; grace boundary; bounded deletes; durable claim; drained owners and node advertisements | The completed three-repository native trial deleted 276,723 obsolete Cellule objects and retained 810 reachable objects. |
| Immutable Git packs | Separate self-contained structural/blob pack pairs; native and BLAKE3 verification; conditional publication; metadata only in SQLite | Durable Git pack bodies total 194.87 MiB, versus 3.07 GiB of expanded bodies in the original format. |
| Less directory rewriting | Smaller body-free SQLite state and fewer bounded metadata transactions | Before GC, directory backing files fell from 7.36 GiB / 351,653 keys to 5.72 GiB / 270,152 keys. |
| Batched import metadata | Up to 512 object metadata records per transaction, blob certificates in that batch, and up to 8,192 dependency rows per SQL write | Current SQLite files total 776,110,080 bytes, approximately 740 MiB, versus 2.77 GiB originally. SHA-256 wire-size and 8,193-edge boundary tests passed. |

Directory rewrite reduction is the combined effect of fewer modified database
pages and fewer metadata publications. The comparison is not an isolated
measurement of either change. The repository namespace's existing `shards: 1`
is unrelated to LTX page-index sharding and was not changed by this work.

## Completed native storage trial

The storage trial used binary
`81a8084224da322a73eb1c73e6af90499da9a5cedba56a072e6d58f867b8a07c`.
Imports completed in 48.75, 90.53 and 910.33 seconds respectively. These are
observations on a shared host, rather than an isolated throughput comparison.

| RustFS apparent backing bytes | Before GC | After GC |
| --- | ---: | ---: |
| All prefix files | 16,313,052,025 | 1,062,134,213 |
| Directory revisions | 6,141,486,337 | 17,132,798 |
| LTX segments | 9,678,730,257 | 810,393,284 |
| Native Git packs | 204,345,936 | 204,345,936 |
| Native Git indexes | 5,448,648 | 5,448,648 |

The final 0.989 GiB is approximately 95.3% below the original 20.98 GiB.
The comparison counts regular files under each exact RustFS prefix, including
provider framing and metadata; it is not a bucket-wide allocated-disk measure.
The collector ran three bounded passes and did not delete native Git packs.
Receipts and before/after inventories are retained under
`~/.local/share/canopy-storage-optimization/medium-widebatch-20261014/results/`.

The original schema-1 fixture's collection also completed on its exact archived
release. Its remaining sweep removed 260,957 obsolete objects and retained
2,985 reachable objects. That original representation occupies 2,937,040,978
apparent bytes (2.735 GiB) after GC. Thus GC accounts for most of the reduction
from 20.98 GiB; the native representation is approximately 64% smaller than the
collected original representation. No old reader was added to the current
product to collect the archived fixture.

Native packs are larger than the source's 152.94 MiB because Canopy creates
self-contained structural/blob pairs under its bounded native Git memory
policy. They still avoid the original expansion to 3.07 GiB. Cold reads fetch
the complete referenced pair; sparse S3 range reads are not implemented.
Selected serving objects use disposable, disk-accounted loose projections.

The older process profile's `--structural-objects` flag used 1,100,000-byte
commit/tag messages and a 32,000-entry tree. The historical references below to
65 MiB structural coverage refer to the ordinary large-object integration test,
not that isolated process profile. Current qualification specifies the structural
message size explicitly; retained raw logs and source snapshots are unchanged.

## Index-warming checkpoint qualification

The index-warming checkpoint is
`08d4e52c17d89808251d2516d99f797c821f7b48fc56b50034cf353f7e736674`.
Before its first uncertified object, preparation scans both existing graph
indexes using read-only 512-pair pages and indexed tuple seeks. That materializes
cold pages outside the write transaction without producing directory revisions.
Certification retains the 128-object / 64 MiB batching policy and 512-row edge
inserts. The real SQLite regression checks duplicate cursor keys, both index
orders, exact completeness and absence of full scans.
Both complete existing indexes are traversed during graph preparation; page
and statement sizes are bounded, but total warmup read work grows with history.

Ref-target reachability checks obey Cellule's 128-statement SQL limit. The
regression uses 129 distinct commit tips. The earlier native storage trial
exposed this limit when cloning Starship's 164 refs; its storage result does
not constitute a successful three-repository clone qualification.

That source passed formatting, all-target Clippy with warnings denied,
the exact dependency policy, Python's 19 tests, doctests, all eight ignored
real-S3 probes and the paired SHA-1/SHA-256 native-pack probe. The library
suite passed serially: 120 passed, one ignored. Two earlier parallel
workspace-lock failures passed unchanged focused rechecks; those failure
logs remain alongside the successful serial run. Integration results are
Main one, Directory 10, Git HTTP one, multi-server 93 with nine ignored, owner restart
one, repository one and smart HTTP one, all passed.

The complete isolated process profile passed with 80 MiB Git/LFS objects,
65 MiB structural objects, 256 additional objects, 300 refs, SIGKILL and
takeover, two TLS peers, backup with every source object deleted, retained-pin
GC and an exact cold stock-Git clone. Final source and gate evidence are frozen
in `~/.local/share/canopy-storage-optimization/format2-index-warm-final-20261014/`.

At that checkpoint the full 5 GiB Git/LFS and 512 MiB incompressible-pack
qualification was not run: its required 40 GiB free scratch and provider
capacity was unavailable.
Historical evidence retains each original binary and dependency version.

An exploratory build,
`e89d1caea09a29a2bce6a09161ec4e4c6355a3aa4425468b230c813c9be02798`,
capped each certificate batch at 1,024 dependencies to avoid the published
runtime's five-second SQL execution deadline on cold indexes. Repeated crash
trials increased publication history until the provider filled. The final
build instead warms existing indexes with read-only seeks and retains the
larger certificate batches. The failed trial was collected under its exact
compiled release; its logs and partial-import evidence remain separate.

## Limits and retained failure evidence

The shared Docker VM filled during exploratory imports. The first disposable
512-dependency-row fixture was discarded by its exact prefix, with its logs
retained. The final build's libgit2 import was interrupted after durable object
staging; its recovery and retry are recorded separately. These attempts must
not be reported as successful clean import timings.

The subsequent `08d4` three-repository run completed imports and collection,
leaving 1,191,805,853 apparent backing bytes (1.110 GiB). Its bat and starship
clone/API checks passed, but libgit2 exposed two serving limits: a cold SQL read
deadline and a fixed 120-second native CGI lifetime. That run is storage
evidence, not a complete final serving qualification.

The current transport-budget build is
`7b16ef3133c39ad84290459e0c256765bcd0acb0fd3e4ac04eb85e2ebd1a0ec3`.
Reachability queries now use 16 statements per batch within Cellule's
128-statement ceiling, and native Git CGI has a finite 30-minute lifetime.
Storage schema 2 and command codecs 4/3 are unchanged. This build passed the
entire serial ordinary test run, formatting, all-target Clippy, the dependency
policy and Python's 19 tests. The complete isolated size/provider/process qualification passed on this exact
source and frozen release binary in 2,960.21 seconds. It transferred a Git blob
and an LFS object each larger than 5 GiB, plus over 512 MiB of incompressible
pack data. The native Git pack measured 595,260,812 bytes while SQLite measured
675,840 bytes after push. A fresh-server stock clone and strict fsck passed;
restored Git/LFS sizes and hashes matched. All eight real-provider protocol
probes, the paired SHA-1/SHA-256 pack probe and the extended process profile
passed, including retained-pin GC, source-deleted backup/restore and SIGKILL
recovery. Library/provider tests used the debug profile; process qualification
used the frozen release binary.

The current medium run imported bat in 56.05 seconds and starship in 124.66
seconds. Libgit2's first attempt failed after 1,492.34 seconds when graph
certification exceeded Cellule's five-second SQL deadline. Exact-release
maintenance recovery and retry completed: retry took 903.49 seconds, and the
failed attempt plus retry totaled 2,395.83 seconds, excluding recovery. This is
a retained failure and recovered import, not successful clean-import throughput.
Collection completed in three bounded passes: 276,750 obsolete objects were
deleted and all 824 marked objects were retained. The passes took 3,373.37
seconds (56.22 minutes), excluding maintenance setup and restart. The post-GC
prefix occupies 1,259,651,402 apparent bytes (1.173 GiB), 94.4% below the
original measurement. Bat and Starship passed fresh mirror/fsck/object/ref/API comparisons. Libgit2
failed its fresh full mirror on a five-second Cellule read deadline immediately
after explicit wants were hydrated. This build's medium serving qualification
is incomplete; its storage result remains valid.

The next source revision adds a covering `(oid, kind)` index and uses bounded
keyset graph pages, limiting edge rows before type lookup and blob filtering.
Dependency certification checks kind and closure through the covering index,
without fetching unused wide metadata rows. It retains the 128-object / 64 MiB
certificate publication batches and the 512-object metadata batch. Repository
schema 3 has one fresh-cell initialization descriptor; certificate command codec
is 4 and object metadata codec remains 4. Qualification requires a fresh prefix;
no old reader or in-place index upgrade is provided. Its results are pending.

Cellule GC is operator-invoked under maintenance. It does not reclaim orphaned
native Git/LFS bodies or replace a product-level object retention policy.
The separate Cellule collector performance patch is unpublished and is not
consumed by Canopy's registry pins.

Refs, tree, exact blob bytes and commit history are checked against source Git.
Author/committer metadata is checked separately. A line-attribution query still
returns HTTP 422; there is no measured blame endpoint.
