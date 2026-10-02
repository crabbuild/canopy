# Verify the merged workspace and latest Cellule on RustFS

PR #14 now incorporates `main`'s three-crate workspace and pins every Cellule
dependency to `70bd25f142f1976fdd63ffe60e46e15ae276ffdc`, the fetched remote
`main` revision. Verification uses a new RustFS 1.0.0 fixture and fresh store
prefixes. Historical results on `30671d5` remain separate evidence.

## Resolve the workspace boundary

| Boundary | Resolution |
| --- | --- |
| Root manifest | Keep the workspace and its three members from `main` |
| Cellule dependencies | Put all five direct pins in `crates/canopy-server/Cargo.toml`; all six lockfile entries resolve to the same revision |
| PR-added tests | Move chunk and cold-owner tests beside the relocated server modules |
| Source digest | Include the relocated chunk module; keep command source hashing enabled |
| Lease guard regression | Read the relocated server module; retain the unchanged 30-second lease and 32-second takeover wait assertion |

The first post-merge Python run failed on the removed `src/server.rs` path.
After repairing that path, all 20 tests passed. A build invocation used the old
`benchmark_idle` example name, and the first relocated-module build exposed a
stale `include_bytes!` path. Both failures are retained in the experiment logs;
the corrected release build and all-target Clippy passed.

## Identify the run

| Input | Value |
| --- | --- |
| Merged Canopy production source | `a62180e`, following merge `03a14f9` of `origin/main` at `37db7c0` |
| Cellule revision | `70bd25f142f1976fdd63ffe60e46e15ae276ffdc` |
| Release server SHA-256 | `dac1ff8f0300c60e26f1b44a02479218379a68467ac740472314561183eb47b6` |
| Release benchmark SHA-256 | `fd9e4cca667c8c78d8bfdd9a68839cf257f9196b47f1b8f43d2d69e1fc4092b2` |
| RustFS image | `ghcr.io/rustfs/rustfs:1.0.0-glibc`, digest `sha256:bffcab0c9d647aab0055d1c69d340b202d0909966b385932d4ead1aeb7602858` |
| Provider limits | Two CPU quota, 2 GiB memory; local Colima VM |
| Provider data | Verified host bind mount; isolated `canopy-pr14-e2e` bucket and unique prefixes |
| Host | Shared macOS arm64, 12 logical CPUs, 32 GiB RAM |
| Local state | Internal `/tmp`; build artifacts and archived logs on a separate volume |

The new dependency and workspace source digests use fresh deployments, not a
bypass of persisted release admission. This run does not establish an in-place
upgrade of an older production deployment. Use the documented maintenance
procedure for an existing deployment.

## Verification gates

| Check | Result |
| --- | --- |
| Corrected locked release server and benchmark build | Passed |
| Formatting and workspace/all-target Clippy, warnings denied | Passed |
| Workspace doc tests | Passed; no executable doc tests in the three crates |
| Python harness | 20 passed |
| Debug workspace unit/binary tests, serial | Passed: Git format 0 tests, object storage 4, server library 112 and binary 1 |
| Other debug integrations, serial | Passed: Directory Cell 10, Git HTTP 1, owner restart 1, Repository Cell 1 and smart HTTP 1 |
| Debug multi-server suite, four threads | Failed: 94 passed, one bulk-ref SQL deadline failure, nine explicit ignores |
| Same bulk-ref fixture, isolated debug rerun | Passed in 109.66 s; does not erase the parallel failure |
| Release workspace unit/binary tests and other integrations, serial | Passed: 117 unit/binary tests and 14 integration tests |
| Release multi-server suite, four threads | Passed: 95 tests, nine explicit ignores, 535.00 s |
| Real-process Git/LFS/API/restart/takeover smoke, including large fixtures | Passed on the full sequential rerun; original backup-repair timeout retained |
| Eight real-provider compatibility tests | Passed sequentially on RustFS, release profile; see the provider table below |
| Two-owner HTTPS routing and takeover | Passed inside the process run: Git/LFS beyond resident capacity, SIGKILL survivor recovery, public-read revocation and maintenance recovery |
| Concurrent cold activation | Passed: all 64 identities, eight clients, no retries; Git v0/v2/fsck samples and graceful shutdown |
| Incremental cache and fresh-owner recovery | Passed with the documented diagnostic filter |
| SQL-only real-store idle benchmark | Failed during seeding after 180 recorded identities; 100-repository window passed, 500/1,000 and released-state windows not reached |

Do not turn pending checks into passes. These are functional verification gates,
not production capacity or latency targets. The older full-corpus cold-activation
and failed load results remain recorded in the
[cold-owner report](2026-09-30-cold-owner-race.md).

The parallel debug failure expired the existing five-second SQL command deadline
during publication of 4,096 long Git refs. It ran while the large process smoke
and a release-test build also used the shared host. The unchanged fixture passed
when selected alone, but other projects and small provider checks still shared
the host. That difference is consistent with load sensitivity, not proof of the
precise cause. No deadline, ref count, name length or atomicity assertion was
relaxed. The snapshot count-work regressions also passed after the merge.

## Exercise the recovery boundary

The process campaign uses stock Git and LFS clients, collaboration APIs and
real Canopy processes. It checks same-directory restart, fresh-disk takeover,
two live HTTPS owners and maintenance recovery before independent backup.

```mermaid
sequenceDiagram
    participant Client as Stock Git / LFS / API
    participant Owner as Canopy owners
    participant Store as RustFS
    participant CLI as Backup CLI
    participant Fresh as Fresh Canopy node
    Client->>Owner: Push objects, refs and collaboration state
    Owner->>Store: Publish durable Cells and immutable bodies
    Note over Owner: Restart, lease takeover, eviction and SIGKILL recovery
    Client->>Owner: Verify exact recovered IDs, bytes and API state
    CLI->>Store: Pin and independently copy Cells and bodies
    Note over CLI,Store: Reject corrupt destination; preserve conditional-copy semantics
    CLI->>Store: Verify after deleting only the disposable source prefix
    CLI->>Store: Restore into a new prefix
    Fresh->>Store: Recover without the original source
    Client->>Fresh: Clone, LFS pull and issue-state comparison
```

The initial combined run reached backup repair after all preceding assertions
passed, but its repeated `backup create` exceeded the existing 120-second
subprocess timeout. Replaying the exact same pin, source and destination passed
in 67.261 seconds with two Cells and three external objects. That isolated
success does not turn the original full campaign into a pass. The second full
campaign passed using the same large-fixture flags and unchanged timeouts,
without overlapping other Canopy test suites. Shared-host load remained
uncontrolled; this difference is not proof of the original timeout's cause.

The successful rerun also rejected the corrupted backup without overwriting it,
repaired and reverified the copy, deleted only its own disposable source prefix,
and restored into a separate prefix. Stock Git/LFS and issue state matched
without the original source. Backup and restored objects remain retained.

| Full-rerun diagnostic | Seconds |
| --- | ---: |
| Large SQLite-chunk tree, commit and tag push | 5.15 |
| Two 80 MiB random Git blobs, one push | 47.03 |
| 1,000-file initial / incremental push | 5.36 / 2.10 |
| Fresh-owner protocol v0 / v2 clone, roughly 160 MiB pack | 22.72 / 14.93 |
| Exact SQLite-chunk bytes and object IDs after takeover | 1.60 |
| 1,000-file recovery, both commits, annotated tag and 300 added refs | 3.41 |
| Backup fixture: 80 MiB Git plus 80 MiB LFS stock push | 19.89 |
| Independent backup restore: exact stock clone and LFS verification | 5.21 |

These are individual phase timings, not a matched baseline or capacity claim.

The separate cold-activation probe killed its owner and respected the unchanged
32-second lease-expiry wait. Eight clients recovered all 64 identities without
retries in 3.447 seconds; request p50/p95/p99 were 400.565 / 541.250 / 558.102 ms.
All three populated fixtures passed Git v0/v2 clone and strict integrity checks,
and graceful shutdown passed. This small recovery check is not full-corpus or
10,000-repository proof.

The first cache-probe invocation omitted the documented Git-gateway debug
filter. Its initial push, clone, exact body and 260 cached-file assertions
passed, but cursor-log assertions failed with an empty log. The corrected
invocation uses `RUST_LOG=canopy_server::git_gateway=debug` on a fresh prefix;
no log wait, cursor, cache or integrity assertion was changed. Both logs are
retained separately.

The corrected cache run passed every gate: 260 files retained their size and
mtime; the incremental receive scanned only three new headers, reused all
three already-cached bodies and hydrated zero bodies through that cursor.
Fresh-state v0/v2 ref discovery read one tip per request without retaining
history; v2 capability discovery hydrated nothing. Ten warm snapshot hits and
zero ref scans covered listings and clones over 301 live refs. Deletion and
recreation remained visible, and fresh-owner clone/fsck and shutdown passed.

Both GitHub Verify jobs for production source `a62180e` also passed, including
debug workspace tests, all-target linting and their own RustFS provider checks.
Those hosted checks are separate from the pinned local-provider runs above.

## Verify stock clients on the real provider

All eight explicitly selected real-provider tests passed using the same release
integration artifact and fresh RustFS prefixes. The ninth ignored test is the
storage-blocked multi-GiB gate, not an additional compatibility pass.

| Gate | Seconds |
| --- | ---: |
| SHA-256 Git round trip | 68.38 |
| SHA-256 native merge candidates | 73.34 |
| Signed push options | 21.25 |
| Signed SHA-256 SSH | 29.23 |
| Stock Git SSH | 43.63 |
| Bulk mirror refs, atomic publication and restart | 331.92 |
| Filtered clones and on-demand object recovery | 132.51 |
| Stock SSH Git LFS | 54.38 |

These times describe this shared local host; they are not latency targets or a
before/after Cellule performance comparison.

## Repeat the idle diagnostic without claiming capacity

The release benchmark requested 100, 500 and 1,000 active SQL-only repositories
with 30-second windows on the same RustFS fixture. Its local state used the
separate build volume, with a 1.5 GiB configured node disk limit. The completed
100-repository window covered all 101 live Cells, including the Directory Cell:

| Metric | Result |
| --- | --- |
| Live Cells before / after | 101 / 101 |
| Distinct Cells with a control renewal | 101 |
| Conditional control updates | 978, or 32.598/s |
| Failed PUTs / published-root changes | 0 / 0 |
| In-flight PUTs before / after | 7 / 1 |
| Measurement boundary | `object_store` API calls, not provider HTTP attempts |

The driver subsequently failed an HTTP operation after 180 recorded repository
identities, before the 500-repository window. The report remains `passed: false`
and `shutdown_passed: false`; neither the 500/1,000 windows nor released-state
recovery completed. No partial-seed resumption, longer timeout or smaller target
was used to declare the requested benchmark passed.

The log recorded several slow node-lease refreshes of 3.0–4.4 seconds. After
failure the provider was still responsive, used about 384 MiB of its 2 GiB
limit, and its host filesystem had 3.1 GiB free. These checks do not isolate
transient provider or host contention. The existing driver's generic
`benchmark HTTP operation failed` message does not distinguish transport
timeout, HTTP rejection or response decoding, so the precise cause remains
unproven. This evidence does not qualify 1,000 active Cells or the documented
10,000-repository reference target on this artifact.

## Keep qualification limits explicit

The non-sparse multi-GiB gate was not started: its existing preflight required
40 GiB free on both scratch and provider, but found only 6.1 GiB on each. The
smaller large-object smoke is not a substitute for that gate. Current-pin
10,000-repository capacity, full-corpus recovery, reference Linux hardware and
performance qualification remain open; this run makes no scaling claim.

## Repeat the checks

From the workspace root:

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo build --release --locked --bin canopy --example benchmark
cargo test --workspace --release --locked --lib --bins -- --test-threads=1
cargo test --release --locked --test multi_server -- --test-threads=4
cargo test --release --locked \
  --test directory_cell --test git_http --test owner_restart \
  --test repository_cell --test smart_http -- --test-threads=1
cargo test --workspace --doc --locked
python3 -B -m unittest discover -s scripts -p 'test_*.py'
python3 -B scripts/qualify_size.py --provider-only --release
```

For caller-owned RustFS storage, supply disposable provider credentials, the
endpoint and node signing key through the environment, then use fresh work
directories. Do not point these tests at a production bucket.

```sh
python3 -B scripts/smoke_s3_process.py \
  --binary target/release/canopy \
  --storage-url s3://disposable-bucket/pr14-verification \
  --work-parent /tmp --retain-work-dir \
  --large-clone --many-objects 1000 --sqlite-chunks

python3 -B scripts/smoke_s3_activation.py \
  --binary target/release/canopy \
  --storage-url s3://disposable-bucket/pr14-activation \
  --work-dir /tmp/canopy-new-activation --concurrency 8

RUST_LOG=canopy_server::git_gateway=debug \
python3 -B scripts/smoke_s3_cache.py \
  --binary target/release/canopy \
  --storage-url s3://disposable-bucket/pr14-cache \
  --work-dir /tmp/canopy-new-cache

target/release/examples/benchmark \
  s3://disposable-bucket/pr14-idle /dedicated-volume/canopy-new-idle \
  100,500,1000 30
```

Retained logs are under experiment `canopy-pr14-e2e-ebY9Ez8T` on the build
volume. Local state is under `/tmp/canopy-pr14-70bd25f-DwQchr8x` and provider
data under the task-owned `canopy-pr14-rustfs-P2X7qaQA` directory. No other
projects' processes or storage were removed.

The failed idle report and local state are retained under the experiment's
`idle-real/` directory on the build volume; its log is `idle-real.log`. The
failed initial process campaign, successful sequential campaign and corrected
cache invocation have separate logs, so their outcomes are not conflated.
