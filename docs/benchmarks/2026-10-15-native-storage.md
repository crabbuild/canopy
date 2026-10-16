# Native storage and full repository qualification — 2026-10-15

The three-repository integrity checks passed before and after Cellule retention GC on RustFS. This run uses committed source `12dc9a19881ca4c87353a7643a374d20bc800d58`, published Cellule **0.2.0**, Repository schema **4**, Directory schema **1** and **`canopy-pack-v2`**. The release executable SHA-256 is `ffa22ca297d764537df7a72870f267e1f58fbacf26267e6243910fdb882919f8`.

All 727 tracked source files in the frozen build snapshot match that commit. The earlier snapshot identifier is retained in the machine-readable evidence. All five direct Cellule pins are exact `=0.2.0`; all six lockfile entries use the published registry release. The official crates.io release check was recorded on 2026-10-15. Breaking format changes require a fresh storage prefix. Only the compiled release and its current formats are supported.

Workspace qualification source `930e6c7a4018b02081ac1421d1f1d5ce9e63691e` differs from the corpus-tested commit only in two `cfg(test)` files. Three large retirement scenarios are constructed on the heap before polling so they fit the default 2 MiB stack. Original cases fail at that limit; all nine HEAD/merge-retirement regressions now pass, covering both Git formats and retaining every receipt, pin, rejection and restore assertion. Production source is unchanged; the corpus and process fixtures execute the selected immutable artifact described below. Interrupted/failed full runs and the first ineffective candidate remain separate evidence.

The cache process harness is qualified separately at `624bd26d7a3d04ae64aaf0ca1f3e50b6a7c89e05`. Its only change from the Rust-qualified source is `scripts/smoke_s3_process.py`: a real blobless clone/lazy read creates hydration scratch, and the fixture checks the current nested `repo.git` location before owner death. All 105 Python tests and a real RustFS crash/corrupt-SQLite/recovery probe pass with those exact script bytes.

The peer proxy harness commit is `1a0fc93d526d205979e03523435ba0c6a9a027c1`. Its only change from that cache harness is `scripts/smoke_s3_peers.py`: the fixture uses the existing bounded stream proxy for TLS termination. The old POST-only proxy returned HTTP 501 for cross-owner Git discovery GETs. All 105 Python tests pass, and the complete two-node scenario passes with eight Git/LFS repositories, opposite-owner routing, public/private reads, owner death and interrupted-maintenance recovery. Rust source and the selected executable are unchanged.

The final Python harness commit is `81925e121c2fad14c25ab993001835b1bc5dc3d3`. Its only additional change is `scripts/smoke_s3_backup.py`: the backup receipt must match the actual external-manifest inventory, require the current native pack/catalog/input families and both LFS objects, and retain that exact inventory on restore. The earlier assertion of three external objects predates native storage; the current fixture produces 26 manifests. All corruption, conditional-copy, source-deletion and exact restored Git/LFS/issue checks remain. All 105 Python tests pass with the final script bytes.

The selected immutable deployment artifact remains `ffa22ca2…919f8`, so corpus, storage, browse and provider measurements refer to one executable. A current-source rebuild in the original target directory matches every file byte except the 16-byte Mach-O linker UUID and its independently verified 32-byte ad-hoc signature page hash (47 bytes differ). Code, data, loader commands and all remaining bytes match exactly. Neither executable was patched. The rebuild and the alternate-target build, whose generated protobuf panic path differs, are preserved separately; the original raw-hash reproduction checks remain recorded as unsuccessful.

## Implemented storage changes

| Priority | Implementation | Evidence |
| --- | --- | --- |
| Cellule retention GC | Verified live roots and backup pins, bounded grace/deletes, confirmed deployment drain and durable worker exclusion. | One complete pass deleted 1,664 eligible Cellule objects; all repository bytes and APIs passed again after restore. |
| Immutable Git packs | Authenticated native pack/index pairs, reusable metadata shards and catalog roots; full fetch reuses verified packs. | Native Git packs occupy 160,389,768 provider bytes for 194,252 objects; no persistent loose-body expansion is required. |
| Less directory rewriting | Native directory runs and catalog subtrees are immutable and reused; Cellule holds bounded publication state. | Cellule directory state is 7,834,274 bytes / 574 keys before GC and 64,051 bytes / 5 keys after GC. Native directory runs are reported separately below. |
| Batched import metadata | Bounded 512-object metadata pages, up to 8,192 objects per shard, and fenced publication of the completed root. | All three full histories imported successfully; 25 metadata artifacts replace per-object authoritative body rows. |

The qualification also fixed renewal during staged metadata replay and final assembly, separated the aggregate closure-spool limit from per-shard limits, retained physical pack completion across repeated fetches, and raised the metadata cache from 16 to 24 files under the existing shared disk budget. The last change keeps libgit2’s 17 shards plus its directory resident; the deterministic control downloads 36 files with 16 slots versus 18 with the current default. Same-release recovery, authorization, fencing and bounded admission remain enforced.

## Complete corpus checks

| Repository | Stored objects compared byte for byte | Refs | Import seconds | Cold mirror seconds |
| --- | ---: | ---: | ---: | ---: |
| bat | 20,952 | 66 | 21.92 | 30.80 |
| starship | 38,333 | 164 | 38.67 | 47.59 |
| libgit2 | 134,967 | 271 | 363.54 | 240.13 |

The pinned source repositories contain 28,189 commits. Their Git packs total 160,372,479 bytes (152.94 MiB); canonical uncompressed bodies total 3,297,080,585 bytes (3.07 GiB). Source and mirror repositories pass `git fsck --full`. Every stored object header and body is compared through paired stock-Git `cat-file --batch-all-objects --batch --buffer` streams. All 501 ref names/targets, symbolic HEAD names and HEAD object IDs match. This is a full body comparison, in addition to object-ID checks.

API checks compare every paginated ref, all pages of 12 selected tree paths (17 pages total), nine exact blob previews and 384 commit records against stock Git. They do not exhaustively enumerate every tree path or every blob through the API; all underlying stored object bodies are checked independently. The complete object/ref/HEAD/fsck/API checks pass again after GC and restoration from S3 into a fresh local workspace. Twelve additional first API reads after separate fresh-workspace restarts return verified responses.

The dedicated attribution endpoint returns 422 and remains unimplemented. It is outside the four API kinds qualified here.

## Storage accounting

Counts below are apparent bytes of regular RustFS backing files under the exact fresh deployment prefix, including provider framing and metadata. They exclude filesystem allocation overhead. Object keys, backing files and logical artifacts are distinct counts. The raw inventories and original classification are retained; `.pack` objects under Cellule paths are correctly classified as Cellule packed objects in this report.

| Category | Before GC bytes | After GC bytes | After GC logical artifacts |
| --- | ---: | ---: | ---: |
| native_pack | 160,389,768 | 160,389,768 | 3 |
| native_index | 5,445,596 | 5,445,596 | 3 |
| native_metadata | 230,728,186 | 230,728,186 | 25 |
| native_directory | 33,846,366 | 33,846,366 | 3 |
| native_catalog_node | 167,004 | 167,004 | 57 |
| input_body | 160,676,473 | 160,676,473 | 48 |
| input_root | 94,093 | 94,093 | 63 |
| cellule_pack | 7,908,265 | 2,118,052 | 75 |
| directory | 7,834,274 | 64,051 | 5 |
| root | 4,233,657 | 42,293 | 5 |
| other | 436,977 | 452,348 | 611 |
| **Total** | **611,853,557** | **594,024,230** | |

Before GC there are 2,771 listed S3 keys plus 92,898 bytes of unlisted provider files; after GC there are 1,154 keys and no unlisted files. The totals include those files. Logical S3 body bytes after GC total 593,423,369. One 9.30-second collector pass deleted 1,664 eligible objects and retained 87 reachable objects; new control objects explain why the net key-count difference is smaller than the deletion count. The complete maintenance/drain/GC sequence took 14.27 seconds.

The original corpus bucket used 22,525,370,449 apparent bytes, close to the previously reported whole-RustFS total of 22,525,385,112 bytes (20.98 GiB). The fresh prefix uses 594,024,230 bytes (566.5 MiB), about **97.36% less**. This compares the historical bucket with a fresh current-format deployment and reflects the combined storage changes; it is not an isolated measurement of GC alone. Historical Cellule directory state used 7,899,358,413 bytes / 351,653 keys. Current native directory runs occupy 33,846,366 bytes separately from the 64,051 bytes of Cellule directory state.

The retained compressed input bodies occupy another 160,676,473 provider bytes and support durable input/replay state. GC intentionally retains product-owned Git packs, catalogs and LFS bodies. Reclaiming unreferenced product artifacts requires its own root/retention policy; this collector does not implement that policy.

After the complete post-GC verification, 12 fresh-workspace reads and 11,474 warm benchmark requests, an additional online inventory observed **631,663,375 bytes (602.4 MiB)** / 4,815 listed keys. Native packs, indexes, metadata, directory runs, catalogs and retained input families were unchanged; Cellule serving/recovery state accounted for the growth. Cellule directory objects then occupied 17,264,316 bytes / 1,535 keys. This was a non-atomic online observation with the server still running, including 247,164 bytes of unlisted provider files; it is not another post-GC bound. No second GC was performed.

## Browse throughput and latency

The client and Canopy run on the shared host over loopback HTTP without TLS; S3 requests reach RustFS in the Docker VM. The warm benchmark uses persistent HTTP/1.1 connections, closed-loop concurrency 1 and 4, two eight-second trials per repository/API group, randomized with seed 42. It reads and validates full response bodies, performs no retries and includes all attempts in latency percentiles. Rates divide successful requests by the sum of actual elapsed trial times; percentiles pool raw samples across both trials using nearest rank. The mixed workload rotates equally across the 12 repository/API pairs. These are short trials on a shared host, not a saturation or capacity guarantee.

| Repository | API | Concurrency | Requests/s | p50 ms | p95 ms | p99 ms | Errors / attempts |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| all | mixed | 1 | 1.67 | 150.1 | 2850.7 | 2937.8 | 0 / 32 |
| all | mixed | 4 | 4.57 | 208.7 | 3701.1 | 4382.6 | 0 / 97 |
| bat | blob | 1 | 3.08 | 292.7 | 647.6 | 1145.4 | 0 / 51 |
| bat | blob | 4 | 8.92 | 443.3 | 834.7 | 1487.3 | 0 / 148 |
| bat | commits | 1 | 0.30 | 3142.1 | 4125.5 | 4125.5 | 0 / 6 |
| bat | commits | 4 | 1.48 | 3110.7 | 4257.9 | 4293.9 | 0 / 28 |
| bat | refs | 1 | 44.45 | 24.9 | 44.2 | 58.0 | 0 / 713 |
| bat | refs | 4 | 149.60 | 16.4 | 61.5 | 78.7 | 0 / 2400 |
| bat | tree | 1 | 4.35 | 158.6 | 610.6 | 784.1 | 0 / 72 |
| bat | tree | 4 | 15.05 | 183.3 | 649.9 | 1036.7 | 0 / 250 |
| libgit2 | blob | 1 | 2.48 | 359.7 | 785.2 | 1080.9 | 0 / 41 |
| libgit2 | blob | 4 | 8.81 | 504.0 | 792.1 | 885.6 | 0 / 146 |
| libgit2 | commits | 1 | 0.37 | 2596.8 | 4647.9 | 4647.9 | 0 / 7 |
| libgit2 | commits | 4 | 1.65 | 2054.9 | 3526.8 | 3527.5 | 0 / 28 |
| libgit2 | refs | 1 | 64.80 | 10.3 | 35.1 | 56.8 | 0 / 1039 |
| libgit2 | refs | 4 | 118.82 | 18.7 | 89.1 | 129.4 | 0 / 1904 |
| libgit2 | tree | 1 | 3.60 | 203.3 | 666.8 | 841.6 | 0 / 59 |
| libgit2 | tree | 4 | 13.29 | 208.7 | 688.9 | 874.2 | 0 / 222 |
| starship | blob | 1 | 3.66 | 207.5 | 564.7 | 667.3 | 0 / 61 |
| starship | blob | 4 | 8.40 | 434.1 | 885.4 | 1100.3 | 0 / 141 |
| starship | commits | 1 | 0.48 | 2330.4 | 3049.1 | 3049.1 | 0 / 9 |
| starship | commits | 4 | 1.71 | 2094.5 | 3082.0 | 3101.8 | 0 / 32 |
| starship | refs | 1 | 52.01 | 11.2 | 40.1 | 53.6 | 0 / 834 |
| starship | refs | 4 | 174.43 | 12.6 | 54.9 | 85.6 | 0 / 2812 |
| starship | tree | 1 | 3.92 | 231.4 | 533.7 | 596.9 | 0 / 63 |
| starship | tree | 4 | 16.76 | 183.5 | 648.0 | 757.4 | 0 / 279 |

Commit history is the slowest measured API: concurrency-4 p95 is 3.1–4.3 seconds. No service-level latency target or large-team capacity claim is established by this run.

Each cold result below is one first verified API request after a graceful shutdown and fresh local workspace. Health/startup time is excluded. RustFS and host filesystem caches may remain warm, so these are individual cold-local-workspace observations, not percentile estimates or a fully cold provider benchmark.

| Repository | Refs ms | Tree ms | Blob ms | Commits ms |
| --- | ---: | ---: | ---: | ---: |
| bat | 298.0 | 2241.5 | 1717.6 | 3115.7 |
| starship | 490.4 | 1907.1 | 1210.1 | 2843.3 |
| libgit2 | 208.6 | 2760.8 | 3872.9 | 5007.8 |

## Qualification and environment

The dependency-policy check, workspace formatting, all-target Clippy with warnings denied, ordinary workspace tests and release compilation pass. The ordinary Rust suites contain 896 unique passing tests, zero failures and nine provider-only cases deferred to the isolated runner. All 105 Python harness tests also pass at the current harness commit.

All nine isolated RustFS provider cases pass: the 595,260,811-byte native pack and 5,377,097,728-byte Git/LFS bodies after restore, SHA-256 round trips and native merge candidates, signed pushes, signed SHA-256 SSH, stock SSH, bulk refs, filtered clones and SSH LFS. The full current extended process profile passes in 493.96 seconds, including large v0/v2 clones, structural objects, repeated restart/takeover, cache corruption recovery, eight repositories through opposite HTTPS peers, interrupted maintenance, backup and retention.

The final backup copies 26 external manifests, rejects corrupt retained bytes, verifies after deleting the disposable source prefix, restores the same inventory, and returns exact Git/LFS content and issue state. The process retention fixture rejects collection outside maintenance, enforces a one-delete partial pass, then completes collection of 256 obsolete Cellule objects while preserving its backup pin and cold stock-Git clone. An independent focused fixture also passes backup/retention and collects 248 objects; these are separate from the 1,664-object corpus collection above.

The nine Rust provider cases use source `624bd26`, whose Rust bytes match the final harness source. After those cases passed, its superseded Python process child was deliberately interrupted; that original whole-run receipt remains failed/interrupted. The complete successful process profile uses `81925e1`. All 724 other source files match the Rust-qualified `930e6c7`; only the three Python fixtures described above differ. The combined qualification receipt records both scopes and their source identities. Earlier failures are retained with their original source versions, rather than relabeled as passes.

The standard workspace gates use:

```sh
python3 scripts/check_cellule.py
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked -- --test-threads=1
python3 -B -m unittest discover -s scripts -p 'test_*.py'
```

The nine isolated provider cases use `python3 scripts/qualify_size.py --release --docker-volume`. The extended process profile uses `scripts/smoke_s3_process.py` with a frozen release executable, disposable S3 prefix/work directory, `--large-clone --many-objects 256 --structural-objects --structural-object-bytes 68157440 --retain-work-dir`, and the fixture credentials in the environment. The 256-object option creates small files; it does not create 256 large structural objects.

Hardware: Apple M2 Max, 12 CPU cores, 32 GiB RAM; macOS 26.5.2; Rust/Cargo 1.97; Apple Git 2.50.1; Git LFS 3.7.1; Python 3.14.6. The shared Docker VM has 6 CPUs and about 7.67 GiB RAM. The corpus RustFS `1.0.0-beta.8-glibc` container is limited to 4 CPUs / 6 GiB RAM. The corpus uses an isolated fresh prefix in the dedicated RustFS fixture; measurements are from the same release executable throughout.

Historical results remain in [the October 14 report](2026-10-14-storage-optimization.md), with their original revisions and format versions. [Machine-readable results](2026-10-15-native-storage.json) retain the source/binary identities, corpus proofs, storage accounting, pooled metrics, qualification receipts and hashes of local raw evidence. The full local evidence lives under `~/.local/share/canopy-storage-optimization/{native-medium-cache-final-20261015,medium-metadata-cache-final-20261015}`. Failed earlier trials remain separate and are not counted as passes.
