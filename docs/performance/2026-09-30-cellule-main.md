# Requalify the September 30 Cellule main snapshot

This run updates Canopy from Cellule `7d17abca2b80b09b696c8199144ce135d23d7985`
to the fetched `origin/main` revision
`30671d5f8a729dd9ccd3a0c2d0e36c7abb89a988`. All five direct Cellule dependencies
and six lockfile entries use the same revision; other dependency versions are
unchanged. The snapshot includes the merged canonical live-owner read and
shutdown release-failure retention changes.

> This revision is not fully qualified. Local scheduler and recovery checks
> passed, but both full Rust suite attempts had failures, and Docker inode
> exhaustion prevents completing real-store benchmarks. A passing isolated
> test is not a passing full suite.

## Identify the run

| Input | Value |
| --- | --- |
| Canopy source baseline | `e2727cb`, plus the dependency update and activation harness repair |
| Host | Shared macOS arm64 host; 12 logical CPUs, 32 GiB memory |
| Provider | Local RustFS `1.0.0-beta.8-glibc`; 4-CPU, 4-GiB container |
| Live scratch | Internal `/tmp`; builds and archived logs on a separate volume |
| Debug server SHA-256 | `635fabb9c78fa51d0bd1db80b29a4a0d6ffe12c2d4d7e556313becfb9474026e` |
| Release server SHA-256 | `7e85a32ed041cf1e84855601153f7d9656d55941e4b800118ee2f90d94d56bb8` |
| Release idle driver SHA-256 | `5a6d2dd4124f8c51416df8b87c1ae07e78d9487129ca01fc8a668988e08de938` |
| Corpus target | 10,000 identities; 100 populated repositories, two commits each, 128-byte LFS fixtures |
| Fresh 10,000-identity corpus | Failed setup; retained 1,933 identities, including 24 populated repositories, after 1,748.773 s |

Other workloads share the host and Docker VM. These measurements cannot be
treated as controlled before/after proof of Cellule's performance. The separate
build volume also had substantially slower synchronous writes than internal
scratch in a 32-write probe: median 87.785 ms versus 0.050 ms. The external-scratch
seed was stopped and retained as incomplete; the replacement uses internal
scratch to match the earlier local diagnostics.

## Verification results

| Gate | Result |
| --- | --- |
| Debug and release server / idle-driver builds, locked dependencies | Passed |
| `cargo fmt --all -- --check` | Passed |
| `cargo clippy --all-targets --locked -- -D warnings` | Passed |
| Python harness tests after activation repair | 13 passed |
| Full debug Rust suite | Failed: multi-server suite had 91 passed, 2 failed, 9 ignored |
| Debug bulk-ref test, isolated | Passed in 84.26 s; the parallel failure remains recorded |
| Debug large-object test, isolated | Failed with HTTP 503; reproduced with the previous pin too |
| Release large-object test, isolated | Passed in 31.82 s |
| Full release Rust suite, four test threads | Failed: multi-server suite had 92 passed, 1 failed, 9 ignored; large-object test failed under parallel load |
| Remaining release targets after that failure | Owner restart, Repository Cell and smart HTTP: all three passed separately |
| Release doc tests | Passed; zero tests |
| Real-store crash recovery | Passed: 64 identities, eight concurrent clients, three populated Git v0/v2 clone/fsck samples, graceful shutdown |
| In-memory idle scheduler diagnostic | Passed at 100, 500 and 1,000 active repositories; 30-second windows, release and three fresh-workspace identity samples |
| Real-store cache smoke | Failed before readiness: provider probe returned HTTP 500, `No space left on device` |
| Real-provider compatibility command | Blocked at Docker volume creation; none of its eight provider tests ran |
| 10,000-identity verification and single-/two-ingress throughput | Not run: the seed manifest is incomplete and the provider has no free inodes |
| Multi-GiB size gate | Not run: requires 40 GiB free on both scratch and provider, plus usable inodes |

The first debug library run also failed a permissions/cleanup test with
`WouldBlock`. Its isolated rerun and the entire 111-test library rerun passed.
The subsequent full debug attempt passed the library, binary, Directory Cell
and Git round-trip suites before stopping at the two multi-server failures.
Later targets were not executed by that attempt.

## Explain the large-object failure

The unchanged fixture pushes a 65-MiB commit body, a 1.1-MB tag and a tree with
32,000 entries, then checks restored bytes and strict Git integrity. Targeted
tracing on the new pin showed a SQL command exceeding the existing five-second
wall deadline during object ingestion, before ref publication. The pending
mutation fenced the Repository Cell and the client received HTTP 503.

| Probe | Outcome |
| --- | --- |
| Latest pin, parallel debug suite | Failed |
| Latest pin, isolated debug test | Failed |
| Latest pin, isolated debug test with targeted tracing | SQL wall deadline expired with SQL already started |
| Previous `7d17abc` pin, same isolated debug fixture | Failed with HTTP 503 in 31.53 s |
| Latest pin, optimized release test | Passed, including restored byte comparison and `git fsck --strict --full` |
| Latest pin, full release suite with four test threads | Failed with HTTP 503 during Git graph preparation |

The old-pin comparison rules out a failure introduced solely by this dependency
update on this host. The release result is consistent with execution cost
affecting the deadline, but does not establish the exact cost breakdown or
guarantee either profile under other loads. The full release run also failed,
so optimization alone does not close this gate. No deadline was relaxed and no
fixture was reduced. Temporary tracing was removed.

The activation smoke wrapper also omitted arguments now required by the shared
seed and verification helpers. A wrapper-level regression test failed before
the repair. The repair explicitly requests a non-incremental, no-LFS activation
fixture and passes its bounded concurrency to verification.

## Scheduler and recovery diagnostics

The release idle driver used a private in-memory object store and SQL-only
Cells. Each window covered every expected live Cell, with no failed PUTs or
published-root changes. Counts include the Directory Cell and describe calls
at the `object_store` API boundary, not provider HTTP attempts.

| Window, 30 seconds each | Live Cells before / after | Control updates/s | Gate |
| --- | --- | --- | --- |
| 100 active repositories | 101 / 101 | 33.164 | Passed |
| 500 active repositories | 501 / 501 | 118.315 | Passed |
| 1,000 active repositories | 1,001 / 1,001 | 329.951 | Passed |
| All 1,000 repositories released | 1 / 1 | 0.333 | Passed |

The driver restarted with fresh local state, measured the released window,
and checked matching identities for the first, middle and last repositories
before shutting down successfully. It did not read back every identity.
Other host workloads and local correctness
tests overlapped this diagnostic. It establishes scheduler coverage and
publication behavior in this run, not real-store throughput, mixed primitive
capacity, or production residency.

The separate RustFS activation check killed its first owner, waited for the
production node lease to expire, and restored 64 identities with eight clients
and no retries. Cold identity reads completed in 5.594 s; p50/p95/p99 were
640.551 / 990.792 / 1,021.228 ms. All three populated repositories passed both
Git protocol versions and strict integrity checks. This small recovery fixture
does not stand in for the failed 10,000-identity corpus.

## Storage failure and remaining gates

The 100-slot debug server's fresh seed stopped after 1,933 recorded identities
when a create request exceeded its 30-second client timeout. Node heartbeat
refreshes then reported changed sessions, exhausted the remaining lease, and
fenced the node. The process exited with an unconfirmed drain. The manifest
remains `complete: false`; no retries or partial-corpus measurements were used
to turn that attempt into a pass.

The cache smoke's provider probe subsequently returned `No space left on
device`. Read-only checks found **zero free inodes** on Docker's `/dev/vdb1`:
5,242,880 used out of 5,242,880, despite about 3.5 GiB of free blocks. A separate
host-bind probe could not even create its container: Docker failed while
creating an overlay filesystem symlink. The provider-only qualification command
then failed at volume creation. This establishes an environment constraint;
it does not prove that inode exhaustion caused every earlier lease delay or
SQL timeout.

No other projects' containers, images, volumes or data were removed. The
incomplete corpus, provider data and failure logs are retained. Finish these
gates on a provider with free blocks **and** inodes:

1. Repeat the eight real-provider compatibility tests and the cache/process
   recovery smokes against the optimized binary.
2. Seed a fresh complete 10,000-identity corpus and verify every identity plus
   the 100 populated Git/LFS fixtures.
3. Run the single- and two-ingress workload windows, then fresh-state recovery.
4. Repeat the large-object test under concurrent load; do not treat its isolated
   pass as a closed regression gate.
5. Run the non-sparse multi-GiB gate on sufficiently sized scratch and provider
   storage. Earlier-pin size proof does not qualify this revision.

The 8-vCPU, 32-GiB Linux/NVMe/same-region reference target remains unqualified.

## Retained evidence

Local reports and logs are archived under experiment
`canopy-latest-U6wUjSnB/qualification-iPz0ULZ6` on the separate build volume.
The incomplete live fixture remains at `/tmp/canopy-latest-30671d5-oMJvuNPg`.
The task-owned provider volume is
`canopy-idle-diag-20260930-7e9c-data`; it has not been deleted.

| Artifact | SHA-256 |
| --- | --- |
| Incomplete 1,933-identity manifest | `e0b9ac7979aa6f9748ec37d16e1f792cf301cdfd36923d3bd2dcffe7a56e6868` |
| Complete in-memory idle report | `bf3a94684c676abf49553a022ea39ef9be4d915152b93542b05da9e75bc27331` |
| Complete RustFS activation report | `f83264fa5ea483a69117d6dd8b6a307777c1653af673e556c6e03a40cbe93763` |

## Repeat the checks

```sh
cargo fmt --all -- --check
cargo clippy --all-targets --locked -- -D warnings
python3 -B -m unittest discover -s scripts -p 'test_*.py'
cargo test --locked
cargo test --release --locked -- --test-threads=4
python3 -B scripts/qualify_size.py --provider-only --release
cargo build --release --locked --bin canopy --example benchmark_idle
# This diagnostic needs no S3 credentials. Supply a new scratch directory.
target/release/examples/benchmark_idle memory:/// /tmp/canopy-idle-new 100,500,1000 30
```

Use a fresh disposable store prefix for this revision. Changing `Cargo.lock`
changes Canopy's compiled release digest; the new binary does not silently open
an older-release corpus. Set `CARGO_TARGET_DIR` explicitly when placing build
artifacts on a separate volume. Keep live SQLite state and client scratch on a
filesystem representative of the intended deployment.

For workload commands and interpretation rules, see the
[performance plan](../performance-plan.md#running-the-initial-density-driver).
Report all scheduled arrivals, including unsent `driver_busy` arrivals; do not
turn client saturation into a successful server-throughput claim.
