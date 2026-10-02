# Cellule main revision qualification

PR #18 pins all six Cellule packages to
`0f4ca0919b0dfe20a3dcd964d21da03135e42eed`, checked against upstream main on
October 2. Locked metadata resolves the new revision; its debug, release and
fresh RustFS checks are pending. Passing checks for the previous `1914096`
pin do not qualify this update.

The original-corpus campaign still uses the frozen, release-qualified
`0dc04a6` executable. Its results do not transfer to either newer revision.
An earlier disconnected-admission failure also remains unexplained despite
subsequent passing checks.

This follows the [retained catalog admission checkpoint](2026-10-01-retained-catalog-admission.md).
That checkpoint and the [authentication and residency results](2026-10-01-bounded-authentication-and-residency.md)
retain their original Cellule revision and artifacts.

## Current dependency update

Only five direct manifest pins and six lockfile source revisions changed from
`1914096` to `0f4ca09`. Other dependency versions, features, Canopy implementation
and runtime budgets are unchanged. Local `cargo metadata --locked` passed;
no native compilation or running-fleet upgrade was performed for publication.

The upstream delta adds admitted-owner fences for application handlers and
node-log rotation after member expiry. These changes are not evidence of a
Canopy performance improvement. The existing release workflow will check the
new pin against an isolated fresh RustFS fixture; retained-store upgrade,
full-corpus recovery and matched performance require separate verification.

## Previous pin: 1914096

The pin-only candidate is `63c93b0f3fadf12893c9c45832f7367ace04ff24`.
Its [Linux debug CI](https://github.com/crabbuild/canopy/actions/runs/36965371936)
passed 244 top-level Rust tests (zero failed, nine ignored), all 84 Python
tests, formatting, all-target lints, the debug server build and all eight
fresh RustFS compatibility gates. Only five direct pins and six lockfile
source entries changed; other dependency versions and runtime budgets did not.

The new [release workflow](../../.github/workflows/qualify-release.yml) repeats
release workspace tests, all-target release lints, the Python harness and the
eight provider gates, then retains the Linux executable, source/dependency
provenance and logs. Its first attempt failed during tool setup because the
runner's package index had no `awscli` candidate; no tests ran. The corrected
workflow checks the existing runner CLI and initializes evidence before setup.
The [corrected release run](https://github.com/crabbuild/canopy/actions/runs/36968478780)
passed 244 top-level Rust tests (zero failed, nine ignored), all 84 Python tests,
the eight release RustFS gates, formatting, release lints and executable retention.
Its archive, 267 source/workflow hashes, locked metadata and executable checksum
were independently audited. PR-head CI is a separate check.

The later [PR-head release run at `cb78793`](https://github.com/crabbuild/canopy/actions/runs/37023329269)
passed 244 top-level Rust tests (zero failed, nine ignored), 91 Python tests,
all eight fresh RustFS gates, release lints and executable retention. Both
PR and push debug verification also passed. The release archive, 268 exact
source/workflow inputs, locked metadata and retained executable were independently
audited. These results apply to `1914096`, not the new `0f4ca09` pin, and do
not establish the cause or resolution of the earlier failure below.

## PR-head failure and unchanged-source diagnostic

| Check | Exact source / result | Interpretation |
| --- | --- | --- |
| [PR-head release CI](https://github.com/crabbuild/canopy/actions/runs/36969961214) | `5581d5c`: disconnected-admission test failed HTTP 503; 240 top-level tests passed, one failed, nine ignored before Cargo aborted | Python, provider gates and executable retention were skipped; not a release pass |
| [PR debug CI](https://github.com/crabbuild/canopy/actions/runs/36969965999) and [push debug CI](https://github.com/crabbuild/canopy/actions/runs/36969961213) | `5581d5c`: each passed 244 top-level Rust tests, 84 Python tests and eight fresh RustFS gates | Neither run clears the release failure |
| [Unchanged-source release diagnostic](https://github.com/crabbuild/canopy/actions/runs/36972048946) | `49268ba`: all 100 isolated repetitions passed; both full-target runs passed 104 tests, zero failed, nine ignored | Nonreproduction, not a fix, full-workspace qualification or performance evidence |

The failing test is
`residency::faults::disconnected_admission_finishes_release_and_allows_a_later_restore`.
The diagnostic's 264 production, test, script and Cargo inputs exactly match
`5581d5c`. It executes the same retained release test binary for every declared
case, including four-thread full-target runs. No test assertion, HTTP retry,
residency budget or production code changed. Failed repetitions would remain
failed; the workflow does not retry until green. Its first setup-only failure
is retained separately.

The diagnostic closed October 2 at 06:27:34 UTC (October 1 Pacific). An independent
audit verified the GitHub archive digest, exact 111-file inventory, source hashes,
all six Cellule metadata entries, retained ELF checksum and all 102 result/log
bindings. The evidence and auditor were copied and independently reread as
115 files on another local filesystem; this is not an off-machine backup.

| Diagnostic artifact | SHA-256 |
| --- | --- |
| GitHub artifact archive | `80522d2d0d33d6bbcf3d48c8b40a0583f32953bde75ac39218aac038735ff7e9` |
| Release `multi_server` executable | `b9d2fe7dc845b8c8f0791296208ed2b99c848820a498e6a14ae35821d99fda10` |
| Independent diagnostic audit | `ccd848c30d08b75e0958ff3b973e9416df027d1ebb81cd2fa685eca7f6ce018f` |
| Verified local copy manifest | `0735c73eaaa8ba60bb4bf34f02bbd437e10027d1592c30a597a10bb52a817ebe` |

The earlier successful release candidate remains historical evidence. The later
failure remains unresolved despite the passing diagnostic. Establish its cause
with a reproducible cancellation/release probe before claiming a correction.

Native release qualification, retained-store recovery, every-ACK verification,
matched performance and reference capacity remain open for `1914096`.
Upstream peer HTTP CI changes are not evidence of Canopy latency equivalence.

## Historical release qualification for 0dc04a6

The following results bind `0dc04a658bd99668936f7ec58032d054f6fbc141`,
not the current PR pin. Release correctness, fresh RustFS compatibility and
repeated regressions passed on that revision. These checks alone did not
establish retained-store recovery, reference capacity or a performance gain.

## Dependency change and source equivalence

The tested source is `bbd784a40c3867646044a8c716b7ee517f9aca49`. The dependency
change replaces five direct manifest pins and six lockfile source revisions;
it changes no other dependency versions, Canopy implementation or runtime budgets.
Locked metadata resolves all six Cellule packages to the same immutable commit.
At first publication (`240203a7`), all 263 production, test, script and Cargo
files on the PR branch matched this frozen source. The subsequent
[listener handoff verification](2026-10-01-listener-handoff.md) has its own
source, executable and checks; the artifacts below remain historical bindings.

The new revision includes atomic API and upstream test changes. Passing
functional tests is not evidence of a throughput or latency improvement.

## Closed verification results

| Check | Result | Verification boundary |
| --- | --- | --- |
| Locked release build | Passed | Executable retained outside Cargo targets |
| Release workspace | 239 top-level tests passed; zero failed, nine ignored | Nested subprocess tests counted once; ignored provider and size gates are separate |
| Release lints | All targets passed with warnings denied | No deployment implied |
| Authentication overlap | 16 admitted, zero refused, 4,673 retained bytes | Held-worker regression, not throughput |
| Directory integration | All 12 passed | Includes bounded authentication and predecessor descriptor admission |
| Catalog admission guards | All five passed | Release and catalog fault coverage |
| Real RustFS compatibility | All eight exact gates passed | Disposable fresh fixture, not retained-store upgrade or five-GiB transfer |
| Cold activation | All 20 independent repetitions passed | Unchanged regression and exact retained test executable |
| Retained startup and refusal | All three cases passed in each of 20 independent repetitions | Owned in-memory predecessor fixture written by the current test runtime |
| Complete residency suite | All 15 passed at four threads | Not live corpus recovery or capacity |
| Python harness | All 84 passed on the same source | Accounting and qualification guards |

Release verification closed October 1 at 23:52:15 UTC; provider gates closed at
23:59:32 UTC. Repeated regressions closed October 2 at 00:06:16 UTC, still
October 1 in the local Pacific timezone. No assertion was weakened, no HTTP
retry was added, and no runtime budget was raised for these checks.

The eight provider gates cover SHA-256 round trips and native merge candidates,
signed push options, signed SHA-256 SSH, stock SSH, bulk mirror refs, filtered
clones and stock Git LFS over SSH. The existing 10,000-repository RustFS provider
had identical provider snapshots before and after these gates. Neither its
corpus nor the UI preview was upgraded.

Reproduce these historical checks from the frozen `bbd784a4` checkout and its
lockfile (the current PR lockfile instead tests `0f4ca09`):

```sh
cargo build --release --locked --workspace
cargo test --release --locked --workspace -- --test-threads=4
cargo clippy --release --locked --workspace --all-targets -- -D warnings
cargo test --release --locked -p canopy-server --lib server::catalog_admission::tests::
cargo test --release --locked -p canopy-server --test multi_server retained_catalog::
python3 -B scripts/qualify_size.py --provider-only --release
```

The provider command owns a disposable fixture. It is not an upgrade command.

## Preserved artifacts

Release and harness evidence is retained at
`/Users/haipingfu/.codex/canopy-cellule-main-qualification-WKtopc`.
Its 290 files were copied and independently reread on a different local filesystem
at `/Volumes/Workspace/CrabData/canopy-cellule-main-correctness-evidence-gxxlwgeb`.
Provider and repetition evidence is retained at
`/Users/haipingfu/.codex/canopy-cellule-main-provider-gates-4480qw`. Its 328 files
were copied and independently reread at
`/Volumes/Workspace/CrabData/canopy-cellule-main-provider-evidence-14_2ewqe`.
These are local, not off-machine backups.

| Closed artifact | SHA-256 |
| --- | --- |
| Release executable | `a541cef36acae0b21a651316fda2e9b3204ef7ea3f02173b65a7c217cd6026e9` |
| `build-tests.json` | `b8a7b2e8005dac79aa19a10423095555c0b2feba9faf8efb6614be22a489ceb4` |
| `release-workspace.log` | `67fbad383f5a6c2b678bcd9d24144cc945dd52f25092d062cd4f110617be8686` |
| `provider-tests.json` | `7e3e87acd46506561d9051d5fe638d4fd30732cb79fd0583337955943192071e` |
| `provider-tests.log` | `f3b384520a7d926f55e82b5aadf435671ce0403bbf4088a28972cd93c473e902` |
| `residency-repetitions.json` | `ae1bc03205fdb26cb892a589e34452185bcede24f42255fd462335364274b9d1` |

## Remaining upgrade and performance gates

The [subsequent owned-fixture upgrade and recovery](2026-10-01-old-binary-rustfs-upgrade.md)
verified actual old-executable RustFS data, controlled admission, new-executable
restore, twenty scheduled critical workflows and their complete fresh-owner
recovery. Its earlier clone-byte mismatch was traced to missing client LFS
filters; the full-byte assertion was retained. This closes a small prerequisite,
not reference performance. The later
[original corpus activation](2026-10-01-original-corpus-activation.md) passed
full admission and controlled activation of all 10,003 Cells. Its subsequent
full remote Git/LFS verification passed with an independent audit and verified
7,061-file copy. The four completed load phases (40 windows) and concurrent
critical schedule include failed arrivals; see that checkpoint's result tables
for metadata, creation, HTTP v2 discovery and stock-Git ls-remote.

The [full performance plan](../performance-plan.md) uses three nodes behind a
proxy, 10,000 identities and 100 populated Git/LFS fixtures. All 108 windows,
114,960 arrivals and 8,640 scheduled seconds completed and were audited on the
frozen `0dc04a6` runtime, with failed arrivals retained. The subsequent full-corpus
recovery attempt failed; see the [complete campaign and recovery evidence](2026-10-01-original-corpus-activation.md).
Concurrent faults, complete original-corpus fresh-owner recovery,
every acknowledged write after owner loss, higher admission profiles, matched
comparisons, non-sparse five-GiB transfers and isolated Linux capacity remain open.
The separate diagnostic fleet's terminal lease-fencing failure is also unresolved.
New passing suites do not explain it or certify missing historical recovery ledgers.
