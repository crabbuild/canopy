# Cellule main revision qualification

Canopy now pins all six Cellule packages to
`0dc04a658bd99668936f7ec58032d054f6fbc141`, verified against upstream main on
October 1. Release correctness, fresh RustFS compatibility and repeated
regressions passed. This does not establish a retained-store upgrade, recovery
of the existing corpus, reference capacity or a performance improvement.

This follows the [retained catalog admission checkpoint](2026-10-01-retained-catalog-admission.md).
That checkpoint and the [authentication and residency results](2026-10-01-bounded-authentication-and-residency.md)
retain their original Cellule revision and artifacts.

Upstream later advanced to `191409685b001a82bd02780def45102b4fc2f164`.
The PR still pins the independently qualified `0dc04a6`; this checkpoint does
not qualify the newer runtime or peer HTTP CI changes.

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

Reproduce the checks from a checkout with this lockfile:

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

PR #18 remains draft. The [subsequent owned-fixture upgrade and recovery](2026-10-01-old-binary-rustfs-upgrade.md)
verified actual old-executable RustFS data, controlled admission, new-executable
restore, twenty scheduled critical workflows and their complete fresh-owner
recovery. Its earlier clone-byte mismatch was traced to missing client LFS
filters; the full-byte assertion was retained. This closes a small prerequisite,
not reference performance. The later
[original corpus activation](2026-10-01-original-corpus-activation.md) passed
full admission and controlled activation of all 10,003 Cells. Remote Git/LFS
content verification remains a separate open gate.

The [full performance plan](../performance-plan.md) still requires three nodes
behind a proxy, 10,000 identities, 100 populated Git/LFS fixtures, all 108 windows,
114,960 arrivals and 8,640 scheduled seconds. Full remote verification,
concurrent critical operations and faults,
every acknowledged write after owner loss, higher admission profiles, matched
comparisons, non-sparse five-GiB transfers and isolated Linux capacity remain open.
The separate diagnostic fleet's terminal lease-fencing failure is also unresolved.
New passing suites do not explain it or certify missing historical recovery ledgers.
