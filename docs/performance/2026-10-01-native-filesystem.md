# Compare RustFS filesystem backing

**Status: full native-volume seed complete; owner-loss recovery running; performance unqualified.**
This shared-Mac/Colima diagnostic keeps the 10,000-repository target. It tests
filesystem backing without replacing the bound runtime or weakening correctness.

## Preserve the failed run first

| Gate | Observed result |
| --- | --- |
| Original candidate seed | Failed with HTTP 503 after 3,561 identities and 39 Git/LFS fixtures; original manifest remains incomplete |
| ACK recovery | All 3,561 exact UUID/name pairs and all 39 populated fixtures passed through fresh owners, including v0/v2 clones, exact Git/LFS bytes and strict full fsck |
| Independent ACK audit | Exact unique ledger sets and artifact bindings passed; all 78 local Git clones were rechecked with strict full fsck |
| Critical fixtures | Both identities and four exact v0/v2 ref inventories, payload and notes passed |
| Diagnostic drain | Three unforced exit-0 outcomes after verified launcher SIGTERM; old RustFS stopped with data preserved |
| UI | Separate preview remains live; no UI process or provider was stopped |

The ACK verifier's final report is `e9d6e6c804367a69f93c59fae57f98f90062b7fa48393e107ae8759f8f473167`;
its ledger is `9290073f8b460d31a2cb21fcd62805759cf3e31a413c681cb9a3c195cf4d2008`.
The independent audit is `01f2daa462f7f5ee321fc30dba813013ee2fe00415ff60d210224cace0355350`.
These SHA-256 bindings refer to artifacts in `canopy-e07670e-candidate-W0SFiGMz`,
not a passing 10,000-repository run. Unacknowledged creations are not rollback
assertions. The [failed campaign record](2026-09-30-three-node-proxy.md#preserve-the-candidate-seed-failure)
retains the original failure and process-loss boundary.

## Change the storage backing, not the workload

| Input | Native-volume trial |
| --- | --- |
| Runtime | Same immutable production executable, Cellule `e07670e`; binary SHA-256 `e90728c60cbb941cc8caa1c698bc24bcf6edda1935d98853f0c0319933668f16` |
| RustFS | Same pinned ARM64 manifest `sha256:0c3c7030ffb93afde8d359fb1db957b85033ede05115518bd0dede51f4353f6a` |
| Provider limits | Two CPU quota, 2 GiB memory, same swap/restart/network settings |
| Changed backing | Docker local volume `canopy-native-data-cexfad8i` at `/data`, replacing the Mac-backed bind mount in a new fixture |
| Topology | Three new Canopy processes behind one loopback TCP proxy; verified TLS peer transport |
| Node limits | 100 active repository entries and 1.5 GiB configured local disk per node |
| Corpus | 10,000 identities; 100 two-commit Git fixtures and 100 1 MiB LFS bodies; same RNG seed `20260926` |
| Deadlines | Unchanged 30-second HTTP and 120-second stock-Git deadlines |
| Deployment | New bucket/prefix and identities; failed data is neither resumed nor deleted |

The first cold-preparation script failed an exact environment-list ordering
check before RustFS started. An independent reconciliation checked unique
environment names and identical values, image, entrypoint, arguments, resource
settings and owned volume. Only environment ordering differed. The failed
preparation receipt remains preserved; it was not rerun or overwritten.

The native fleet passed all 17 critical Git checks. The full seed completed
with exit 0 on 2026-10-01: **10,000 identities and 100 populated Git/LFS
fixtures in 3,179.240 seconds**. An independent offline audit checked exact
corpus identities, fixture selection/digests and controller/source bindings.
Seed wall time is not scheduled creation, push or fetch throughput. A complete
seed alone does not prove recovery, a root-cause fix or matched improvement.
Different wall-clock load and observer overhead remain shared-host variables.

```mermaid
flowchart LR
    ack[Every failed-seed ACK verified] --> drain[Graceful diagnostic drain]
    drain --> seed[Native full 10000 seed<br/>complete]
    seed --> loss[Three-owner loss and wait<br/>recorded]
    loss --> recovery[All identities and Git/LFS<br/>verification running]
    recovery --> load[Declared 108-window load plan]
    load --> final[Second owner loss<br/>verify every load ACK and corpus]
```

The seed and recorded fault boundary are also complete. Critical Git setup and
critical-fixture recovery passed; full native owner-loss recovery, the
[declared load matrix](three-node-baseline.json),
higher admission profiles, additional critical-operation load/fault coverage
and matched performance comparisons remain open. No stage can substitute a
smaller corpus or retries for its required evidence.

## Gate the transition and complete load plan

Two separately bound controllers are running, not completed. The recovery
controller waited for the exact seed and its setup controller to exit, then
reread their final receipt. A failed/incomplete corpus, changed process
identity, provider restart/pause/quota/mount change or source drift stops it
before any owner-loss action.

Only a full 10,000-identity manifest with the original RNG-selected 100 exact
two-commit/LFS fixtures can reach the fault stage. The controller verifies the
whole PID/command/kernel-identity batch before sending three SIGKILLs, records
process absence and the unchanged 32-second expiry wait, and starts fresh
owners from the preserved deployment. Every original identity and Git/LFS
fixture, plus both critical repositories, must pass without retries.

The actual fault sent SIGKILL to PIDs 64706/64748/64836 in one verified batch.
All three exited -9; the launcher recorded no forced fallback. After recorded
process absence, the expiry wait lasted **32.037 seconds**. A new fleet started
at 03:45:31 UTC with three new IDs/PIDs and local directories, the same durable
deployment, admission and immutable binary. Both critical repositories passed
four exact v0/v2 ref inventories, payload/notes and strict full fsck. Full
10,000-identity/100-fixture recovery was live at the 1,500-identity checkpoint;
no load window had started at that checkpoint.

The load controller requires that closed recovery evidence, all source and
receipt digests, three recorded SIGKILLs and fresh owners. Its plan is the
unchanged 108 windows: 114,960 offered arrivals over 8,640 offered seconds.
Failed arrivals, interrupted windows and diagnostic gaps remain visible;
post-load verification of every ACK is a separate, still-open gate.

Offline guard tests passed: four recovery tests cover the simulated complete
path, eight failure-control cases, provider-boundary checks and actual full
manifest/fixture validation; two load tests cover closed-recovery evidence and
five no-launch failure cases. All signals, provider calls, Git and load traffic
were mocked in control tests. Live read-only binding checks also passed before
arming. These are controller guards, not actual owner-loss or performance proof.

## Separate diagnostics from performance

The closed ACK-recovery observer retained 173 samples with zero probe errors.
During that window, maximum provider health latency was 457.624 ms, signed
metrics latency 265.171 ms and VM probe latency 354.850 ms. It captured 522
scoped container events without observed pause/unpause/update/restart/kill/OOM
events; 29 state snapshots had unchanged starts, quotas and zero restarts.
Its independent audit is
`866419ea5503df3ca685789f333f8d8fed03f5ed734b2f8bf8ac822099ef9dec`.
This window was after the failed seed, not an observation of the original stall.
Health and metrics timings do not establish durable-write latency.

The separate native-seed observer started after 1,250 identities and closed
after seed exit: 459 samples, zero probe errors, 77 unchanged provider-state
snapshots and 1,377 scoped events with no observed pause/unpause/update/restart/
kill/OOM events in that observation window. An offline integrity audit checked
timing bounds, kernel identities/counter continuity and signed-counter deltas.
This excludes the later intentional owner loss, not all possible shared-host
interventions or the old failed run. It retained
provider gauges, signed counters, health timings, VM uptime/CPU/I/O pressure,
kernel self/reaped-child CPU and continuous scoped events. Errors and gaps stay
visible. It adds host overhead and cannot account for the whole seed, live child
CPU, per-operation CPU/GiB or priced object-store cost.

## Bind the new trial

Artifacts are retained under `canopy-native-filesystem-ceXFad8I`:

| Closed input or receipt | SHA-256 |
| --- | --- |
| `provider-binding.json` | `aa6a531365f550611618c91ce8b6cd88974811f543765915f1436bf71a8ec570` |
| `start_full_trial.py` | `973e1c3cdefaf6ef221d9351970d481e7f8ba1c5773b55ae9ea86d74ed1b45d2` |
| `fleet-native-seed/ready.json` | `aaa9769fa8dfa6fa85491a32881448e9b71fe9941ff798b5b8058f8a29720ef9` |
| `critical-native.json` | `2b019dea0a6e89fe26f22d20860775860469ae2a2fd743f65d836e95d03ca04d` |
| `observe_native_seed.py` | `621ee99b88bb839541165417bb3a4c2c333edadfdbff0003f70d025498f119b3` |
| `finish_native_recovery.py` | `2dac2dd18bdbd416c95ab9ff77cbfb8566cad7bb4ae89dc5c6fa55b51c00500e` |
| `test_finish_native_recovery.py` | `bd919dfdff557d6e5fa59a6647d7731661efa37ec7854efd093cf0a08ab4b837` |
| `transition-guard-tests-final.log` | `9155d1771a85043e16da955d5f7f61465a4ca3da7df0518f116cc2093bce6568` |
| `transition-check-final.json` | `d93ac46cef8392eb61dbc666d49a69be89a46841b5e22b9d0bf464aafb1ac865` |
| `start_native_load.py` | `61d7828f2441c2d12061fce9fdb2c366ceb60c746e75322ee8b615e410c4645e` |
| `test_start_native_load.py` | `9a1009d2f57987931beeeead14cb8e1482e3ffd3bbf6f9bb7520a5cd77ae0e0b` |
| `load-guard-tests-final.log` | `6b307282780ae288f168cfceef2ff771a215534923036eb6875258d24e00622d` |
| `load-check-final.json` | `3076fafdfc8881746f728f8db498b08731676e33cba1e3181df7a7092e2f4a54` |
| `corpus-native-10000.json` | `32e7db9cf06a7b4bb57c906f0f2e70c761ed3ae207c95c7057302b55d50a9a32` |
| `trial.json` | `b618be95af59980fac8f3576f208d17ea83c43ab266c388c82288f0037a5ad12` |
| `seed-native.log` | `c9585a548c6e9c5f2a7b4fedfb8e1c54e59e7b2707e6fc194e9cec1b8612747d` |
| `native-seed-audit.json` | `355912c5fb4f21fa15eec96bdfd5301a967a4676a771a4d05307c7aa1fa49db8` |
| `audit_native_seed.py` | `78e24e4993abb9cfaff1c6121e9ac2b2773d9e30601bf88719d8e0d009a965c1` |
| `native-seed-observer/observation.json` | `7569827510978ba20827e57d1629b72dfc370afa8fb452773844e40c931137b8` |
| `native-initial-owner-loss.json` | `3ed0fdb7df452bf586c1c2908f9b66546e73a8177fbbf088531b07ea73baefe7` |
| `fleet-native-recovery/ready.json` | `2823b1e4b9ab4143fb7c9813e2717cf89d099212f50a5e8f8ad305a1d4043154` |

The recovery and load controllers are still live and do not have final
completion digests. Setup timings are not scheduled throughput.
This is not isolated Linux reference capacity, a proven Cellule bottleneck or
a passing latest-pin executable comparison. Both hosted workflows for source
pin `0573f489` passed
([36806695489](https://github.com/crabbuild/canopy/actions/runs/36806695489),
[36806698917](https://github.com/crabbuild/canopy/actions/runs/36806698917));
the running comparison binary remains the original `e07670e` artifact.
