# Old executable RustFS upgrade and critical workflow recovery

The qualified Cellule `0dc04a6` executable restored data actually produced by the
old `c51dd121` executable on an owned RustFS fixture. Twenty scheduled critical
Git workflows passed through three nodes and a proxy, and all their acknowledged
state survived loss of all three owners and restoration from fresh local disks.

This closes the small retained-store upgrade prerequisite. It does not qualify
the existing 10,000-repository corpus, its complete load matrix, isolated Linux
capacity or a speedup. The [latest Cellule checkpoint](2026-10-01-cellule-main-qualification.md)
records the separate release build, tests and provider gates.

## Upgrade and recovery sequence

```text
Old executable writes two Git/LFS repositories to owned RustFS
  -> full-byte verification -> graceful drain -> same-code maintenance
  -> full catalog and Control admission -> controlled new release activation
  -> three fresh gateways + proxy -> restore old bytes -> verify new writes
  -> 20 scheduled critical workflows -> preserve every receipt and arrival
  -> SIGKILL all three gateways -> 32.008 seconds confirmed owner absence
  -> three new gateways with fresh disks -> verify all retained and new state
  -> graceful drain; all three recovery gateways exit zero
```

The producer used the exact old executable, not a current-runtime predecessor
simulation. Before activation, admission verified the actual old descriptor,
rolling compatibility, all 256 catalog shards, exactly the three expected Cells,
supported initial catalog identities and actual persisted Control code/schema.
Every Cell was durably idle and unowned; there were no advertised writers,
including expired advertisements. Repeated observations had identical catalog
revisions, immutable page digests, canonical Controls, identity and root purpose.

The old executable ended its exact maintenance operation after confirmed drain.
The fixture-only tool then performed prepare, activation and completion, rescanning
the unchanged state at each boundary. It never rewrote immutable catalog identity
or bypassed runtime ownership CAS. This restricted tool is not a general upgrade
command and must not be applied blindly to another deployment.

## Verification results

| Gate | Closed result | Scope |
| --- | --- | --- |
| Old executable producer | Two repositories, two commits each, main/tag/custom refs and full LFS payloads verified | Actual retained old executable; each LFS body is 1,048,593 bytes |
| Old writer drain | Exit zero; zero advertised sessions and zero unsettled Cells | Same-code maintenance before upgrade |
| Upgrade admission | All 256 shards and three expected Cells passed; invalid mode and wrong endpoint refused | Exact owned fixture only |
| New executable restore | Git v0/v2 exact refs, ordinary clones, mirrors, full fsck and full LFS bytes passed | Three fresh gateways behind the proxy |
| New writes | Both fresh commits and LFS payloads acknowledged and freshly verified | New branches preserve all old main/tag/custom refs |
| Critical workflow schedule | 20/20 workflows and 340/340 steps passed; no failed or dropped arrivals | 300-second schedule, 15-second interval, concurrency cap four; observed peak overlap three |
| Owner loss | Exact three native gateway PIDs received SIGKILL; all absent for 32.007527 seconds | Both RustFS providers retained the same identity, start time and resource settings |
| Fresh-owner recovery | Both retained repositories, both new Git/LFS writes and all 40 critical repositories passed | Exact Git v0/v2 inventories, full fsck, payloads, notes and full old/new LFS bytes |
| Final drain | All three fresh recovery gateways exited zero without force | No UI preview or other fleet signalled |

The initial byte-check failure was a client setup error: the producer installed
Git LFS only in its local source repository. A fresh clone without LFS filters
returned the correct 132-byte pointer and README, not the full payload. A real
stock-Git regression failed with user-global configuration disabled, then passed
with explicit LFS filters. The original full-byte assertion was retained.
The earlier reserved-ref attempt also remains recorded: main/tag were accepted,
while `refs/canopy/retained` was correctly rejected as server-owned. No partial
acknowledged state was discarded to make the fixture pass.

## Throughput and latency

This shared Mac/Colima run declared one complete workflow every 15 seconds.
All 20 completed inside the 300-second schedule: **0.066667 workflows/s**.
Including drain, elapsed time was 300.026007 seconds and throughput was
0.066661 workflows/s. This is the delivered rate at the declared arrival clock,
not a saturation capacity measurement.

Each workflow includes two repository creations, atomic publication, mixed and
atomic refusal, correct/stale lease pushes, shallow/deepen/unshallow, filtered
lazy fetch under v0/v2, incremental push, fast-forward pull, branch deletion,
fetch-prune, mirror push, credential refusal and exact mirror/fsck verification.

| Measured client wall time | p50 ms | p95 ms | p99 ms |
| --- | ---: | ---: | ---: |
| Complete scheduled workflow | 18,413.501 | 30,629.526 | 31,908.751 |
| Create source repository | 383.934 | 755.265 | 1,869.551 |
| Create mirror repository | 321.574 | 605.454 | 2,463.179 |
| Atomic multiple-ref publication | 942.296 | 1,993.050 | 2,479.839 |
| Incremental push | 720.945 | 1,541.330 | 1,726.267 |
| Fast-forward pull | 523.296 | 1,163.776 | 1,194.283 |
| Fetch-prune | 248.192 | 602.466 | 666.105 |
| Exact v0/v2 mirror and fsck checks | 3,683.990 | 5,051.374 | 5,529.689 |

These include stock-Git processes, client work and validation. Step timings may
group multiple commands; they are not isolated RPC rates or server-only latency.
There are only 20 samples per step. This run supplies no matched baseline,
server CPU attribution, provider cost or isolated Linux capacity result.

## Reproduction and retained evidence

The checked-in workflow driver can reproduce the declared schedule on a
separately admitted, caller-owned fixture:

```sh
python3 -B scripts/benchmark_critical_git.py \
  --fleet-dir /path/to/admitted-three-node-fleet --node-active-limit 100 \
  run --output-dir /path/to/new-critical-results \
  --duration 300 --interval 15 --concurrency 4
```

Recovery additionally requires recorded owner loss, absence/expiry, unchanged
provider and fresh gateway workspaces. The verifier checks receipts; it does not
perform or prove those caller-controlled fault steps by itself.

Evidence is retained at
`/Users/haipingfu/.codex/canopy-old-binary-rustfs-upgrade-n6W1G1`. Before owner loss,
7,388 closed files were copied and independently reread on another local filesystem
at `/Volumes/Workspace/CrabData/canopy-retained-upgrade-load-v9fs4p1c`. The final
13,292-file copy at
`/Volumes/Workspace/CrabData/canopy-retained-upgrade-recovery-4a3q8dyg` includes
closed recovery receipts, terminal fleets, exact source and failed attempts.
These are local, not off-machine backups.

| Closed artifact | SHA-256 |
| --- | --- |
| Old producer receipt | `a861a56aa31aed2f9110fc26069c9c35c4a7a59179d687ceddd3f5d471f70cd4` |
| Upgrade admission | `37a152bbd70714974e1479e64dd73d7311c74b9d145c12bae83cee7d513978b1` |
| Upgrade activation | `aff121a9ab4eadcefe3c6ce04f46b8a15e770beee260738ec429a5c645566656` |
| Initial new-executable restore | `287a7519598ba3ab5bbeba2d5a48df95f6e582527ba69be18d939726df85e6f7` |
| Critical load report | `7857f31b1d52c88fe0335c7b84fa2120649abf2e92c6d53843bef3332fe83fe2` |
| Arrival ledger | `e880e348c62001480944b4f7462a3b14f19c93cd2c3189ccfe173a9fdf14cb57` |
| Owner-loss receipt | `4faf82d22f6f7e0efd43c2911b6def0f947523dc4ba96ecde8457d967846d973` |
| Post-loss verification | `010bb840514dc798b8bc83c69ef4c0399fb572c338380801dedc1585233ecd75` |

The old executable SHA-256 is
`32b114119960608c0a91d1c783bb69eafec432831bfa452d54d8950b09bc0e99`;
the new qualified executable is
`a541cef36acae0b21a651316fda2e9b3204ef7ea3f02173b65a7c217cd6026e9`.
The measured production source matches `bbd784a40c3867646044a8c716b7ee517f9aca49`.
The subsequent [listener handoff fix](2026-10-01-listener-handoff.md) is a
separately verified build, not a new performance binding for this run.
The producer, upgrade, load and recovery ran October 2 at 00:13–00:36 UTC,
October 1 in the local Pacific timezone.

## Remaining full campaign and CI work

The original 10,000-repository provider was not upgraded or restarted. Its
read-only status observation reports the old Ready release, zero advertised
writers and **301 unsettled Cells**. It requires supported offline recovery and
full same-corpus admission before any new release or gateway launch. Restarting
the small fixture does not explain the original fleet's terminal lease failure.

At PR head `240203a7`, both Python CI jobs and one complete Rust job passed.
The other Rust job failed with `AddrInUse` in the SHA-256 merge-candidate test.
The failure log is retained; a passing parallel job does not erase it. The
[listener handoff follow-up](2026-10-01-listener-handoff.md) reproduces the
reservation gap, removes it from the affected SHA-256 tests and passes release
and fresh-RustFS checks. The changed PR head still needs its own Linux CI.

The [full performance plan](../performance-plan.md) remains unchanged:
10,000 identities, 100 populated Git/LFS fixtures, all 108 windows, 114,960 arrivals
and 8,640 scheduled seconds, independent operation rates/concurrency, uniform and
skewed active sets, ref-only/fresh-object pushes, higher admission profiles,
concurrent faults, full original-corpus/every-ACK recovery, matched comparisons,
non-sparse five-GiB transfers and isolated Linux qualification. PR #18 remains draft.
