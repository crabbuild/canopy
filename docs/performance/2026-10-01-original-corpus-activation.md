# Original corpus release activation

The original RustFS corpus passed full new-release admission and controlled
activation without changing any of its **10,003 Cell Controls, catalog entries
or published roots**. The qualified executable uses Cellule `0dc04a6`; its
selected release reached **Ready revision 9**. This closes the release-admission
gate after [same-code maintenance recovery](2026-10-01-original-corpus-recovery.md).
Activation alone does not establish remote Git/LFS content recovery, throughput
or capacity. The subsequent remote verification passed; the completed load
phases below include failed arrivals and do not establish capacity.

## Activation sequence

```text
Old Maintenance 5: all 10,003 Cells idle and unowned
  |
  +-- read-only admission: actual old descriptor + new rolling compatibility
  |                       all 256 shards and actual Control code/schema
  |                       two unchanged complete snapshots
  |
  +-- exact old CLI ends maintenance --> Old Ready 6
  |
  +-- new SDK prepares release ------> Prepared 7
  +-- new SDK begins activation -----> Activating 8
  +-- new SDK completes activation --> New Ready 9
                                       |
                                       +-- preserve and reread closed evidence
                                       +-- three fresh gateways and proxy ready
                                       +-- remote Git/LFS verification passed
                                       +-- full load running: failed arrivals retained
                                       +-- fresh-owner/every-ACK recovery still open
```

The activation controller repeats two complete scans at each of Ready 6,
Prepared 7, Activating 8 and Ready 9. Every snapshot matches the admitted catalog,
canonical Controls, ETags and service root. No serving owner is admitted during
these scans. The later gateway launch is a separate step, not part of this
unchanged-metadata claim.

## Write boundary

The fixture-scoped controller uses the existing Cellule release APIs, wrapped
by a transport gate. It arms only after checking the exact old Ready record and
two stable complete snapshots. While armed, it permits only:

- Creation of the exact new descriptor with its canonical bytes.
- Conditional updates of the exact release key to the three expected records:
  Prepared 7, Activating 8 and Ready 9.

All Control, catalog, node, data, delete, copy and multipart writes remain
refused. The gate disarms after activation. It forwarded **four writes**, with
zero refused attempts during activation. The exact old CLI's preceding
maintenance-end transition is separate from that four-write count.

Three unit tests cover read-only refusal, exact armed paths/bytes/write modes,
and real ReleaseStore transitions with canonical readback. Locked offline
metadata, release build and all-target Clippy passed. The earlier test attempt
that incorrectly listed an exact leaf as a directory prefix is preserved; its
SDK transitions had passed, and the corrected test verifies the exact record.
The controller is a retained qualification tool, not a general upgrade command.

## Closed results

| Check | Result |
| --- | --- |
| Corpus admission | All 256 shards and 10,003 Cells; actual predecessor descriptor and every catalog/Control code/schema supported |
| Admission snapshots | Two identical scans; zero owners, advertisements, unsettled Cells or attempted provider writes |
| Old maintenance end | Exact retained executable exited zero; old Ready revision 6, zero advertisements and unsettled Cells |
| Activation | SDK transitions 7 → 8 → 9; exact new descriptor and selected image verified |
| Activation snapshots | Eight complete scans; all canonical Controls, ETags, catalog and published roots unchanged |
| Provider | Container, volume, image, configuration, start time, resource envelope and restart count unchanged |
| Evidence | 369 closed files and exact inputs copied and independently reread on another local filesystem |

Admission closed October 2 at 02:25:04 UTC; activation closed at 02:32:05 UTC
(October 1 Pacific). The controller took 225.749 seconds including complete
rescans. That is activation wall time, not Git request latency.

## Artifact bindings

| Artifact | SHA-256 |
| --- | --- |
| Qualified Canopy executable | `a61ef0f2cb977348e4e4fc45330334a1f8fd68bf8c44146cd9343b0e6676a6c8` |
| Scoped activation controller | `d5ebef17d975a349bb6db250e2154198dbda53a273ad1078db1958e8b819373f` |
| Admission receipt | `39cdcfce11fdc4e2261beaa51b4640a2321756b014d04887082d2a57ca614fe5` |
| Activation receipt | `316ef6b8449b3a41c09d41dd2578474ad77b1f17147d06ab14be45f035d66981` |
| Backup manifest | `03ca94a54b7683b32c717bc0fbd9b5839f1d0f9f705e696431b79c7d387294e8` |

The Canopy source is `3dda2b47b1cba105642a62b4ad27d7c84d0940d5`, independently
qualified in the [listener handoff checkpoint](2026-10-01-listener-handoff.md).
The selected release is
`9a8df7ae5feba1f1760a843bc88d7870af4ebf515b8bc48e45bb7433569cd5d0`;
activation operation is `d36626b9-361b-47dc-a43a-3e14f9bd1a7d`.

Receipts and exact tools are retained at
`/Users/haipingfu/.codex/canopy-original-corpus-admission-YxGodu`.
The verified copy is
`/Volumes/Workspace/CrabData/canopy-original-upgrade-activation-7ze24cer`.
This is not an off-machine or provider-data backup. No runtime budget was raised,
owner erased, Control force-CAS applied, catalog rewritten, corpus reseeded or
provider restarted. The UI preview and unrelated providers remain unchanged.

## Full remote-content verification

Three fresh gateways reported ready behind the proxy at 02:35:59 UTC with the
exact qualified executable and 100 active repositories per node. The subsequent
read-only remote verifier closed successfully at **03:02:09 UTC**; an independent
offline audit closed at **03:02:40 UTC** on October 2 (October 1 Pacific).

| Check | Closed result |
| --- | --- |
| Original repository identities | All 10,000 names matched their exact UUIDs |
| Original LFS objects | All 100 complete bodies matched their expected sizes and SHA-256 digests |
| Stock Git | 200 clones across protocols v0/v2; exact HEAD, base commit, fixture files and full fsck |
| Original critical fixtures | Both UUIDs, four exact v0/v2 ref inventories and mirrors, payloads, notes and full fsck |
| Independent audit | 10,002 identities, 100 LFS digests, 824 Git commands, all 200 local clones and four mirrors reconciled |
| Evidence preservation | 7,061 closed files and exact inputs copied and independently reread on a different local filesystem |

HTTP checks used concurrency 16 and a 30-second timeout; Git commands retained
their 120-second timeout. No retries, new seed or provider restart were used.
The original fixtures upload LFS objects directly without committing LFS pointer
files: this verifies complete download bytes, **not clone-side LFS hydration**.
The 756.929-second corpus stage is verification wall time, not scheduled request
latency. This check does not establish post-load owner-loss recovery.

| Remote-verification artifact | SHA-256 |
| --- | --- |
| Verification receipt | `1447486483a253c3fad18000a73eaa449dbaa26a1223903c074497d2b431bd4a` |
| Independent audit | `2b1a256a40cec3bcf24359a21559da8b35927d697ec0e25ba5a42982cdb030d0` |
| Verified backup manifest | `783902ec100a41e7882c73478a98b912b06b6ec0293907146a60a7dd7d8b8078` |

Closed receipts are in the qualification root's `upgraded-original-verification`
directory; the verified copy is
`/Volumes/Workspace/CrabData/canopy-original-upgraded-remote-dooxekr0`.

## Scheduled load results

The unchanged **108-window / 114,960-arrival / 8,640-second** campaign remains
running on the qualified `0dc04a6` executable and original RustFS provider.
At October 2, 04:33 UTC (October 1 Pacific), its first four operation phases
were complete: **40 sealed, audited and preserved windows**. Later clone
windows had also begun; they are excluded from this completed-phase summary.

| Completed phase | Windows | Scheduled | OK | Busy drops | HTTP 503 | Other failures |
| --- | --- | --- | --- | --- | --- | --- |
| Metadata | 18 | 43,200 | 41,485 | 1,669 | 46 | 0 |
| Repository creation | 6 | 2,160 | 1,464 | 690 | 6 | 0 |
| HTTP Git v2 discovery | 8 | 57,600 | 10,309 | 9,581 | 37,706 | 4 transport errors |
| Stock Git ls-remote | 8 | 1,200 | 632 | 514 | Not separately classified | 54 Git errors |

These are failed arrival gates. The HTTP discovery probe checks the v2
capability response, not the full stock-Git ref exchange; `ls-remote` runs the
actual Git client and checks the expected main ref. Busy arrivals do not start
requests and have no completed-request latency. Git errors remain client-level
failures rather than being silently classified as HTTP 503s.

Fast refusal can make aggregate latency misleading. In the first uniform
100-RPS discovery window, completed attempts had p50 **7.824 ms**, but successful
attempts had p50/p95/p99 **1,406.853/4,472.563/6,349.508 ms**. Only 1,343 of
12,000 arrivals succeeded, delivering **10.992 successful requests/s** inside
the window. This is not a latency improvement or evidence of 100-RPS capacity.

### Repository creation throughput and latency

Each window lasts 120 seconds with client concurrency 16. Successful completions
during drain count as OK but not as delivered RPS inside the window. Percentiles
below cover completed attempts, including HTTP errors; they exclude busy drops.

| Offered RPS / repetition | OK / scheduled | Busy drops | HTTP 503 | Delivered RPS | Scheduled p95 / p99 (ms) |
| --- | --- | --- | --- | --- | --- |
| 1 / 1 | 120 / 120 | 0 | 0 | 1.000 | 3,205.203 / 5,940.919 |
| 1 / 2 | 120 / 120 | 0 | 0 | 1.000 | 5,220.908 / 6,795.877 |
| 1 / 3 | 120 / 120 | 0 | 0 | 1.000 | 5,282.604 / 7,891.099 |
| 5 / 1 | 394 / 600 | 204 | 2 | 3.150 | 11,488.210 / 13,053.231 |
| 5 / 2 | 196 / 600 | 400 | 4 | 1.558 | 18,279.797 / 20,765.950 |
| 5 / 3 | 514 / 600 | 86 | 0 | 4.250 | 5,185.795 / 12,490.197 |

All 1,464 positive creation ACKs have unique canonical UUIDs and their exact
scheduled names. They do not collide with the original 10,000 repositories,
the two original critical fixtures or the 38 current critical UUIDs. This checks
ledger integrity, not recovery after owner loss. Failed writes are not assumed
to have rolled back.

### Stock Git ref listing

The eight 60-second `ls-remote` windows vary offered rate (1 or 4/s) independently
of client concurrency (1 or 16), with two repetitions per combination. The
first concurrency-16, 1-RPS repetition completed all 60 arrivals; the second
returned **41 OK, 16 Git errors and three busy drops**, with completed-attempt
p95 **34,429.751 ms**. The two concurrency-16, 4-RPS repetitions returned
**236/240** and **111/240** OK, delivering **3.833** and **1.750** successful
operations/s respectively. The variability is retained; no matched speedup or
root cause is established.

### Earlier ten-window metadata snapshot

The unchanged **108-window / 114,960-arrival / 8,640-second** campaign is running
on the same qualified executable and original RustFS provider. This earlier snapshot
covers only its first **ten sealed, audited and preserved metadata windows**,
observed October 2 at 03:36 UTC. All offer 20 requests/s for 120 seconds, with
client concurrency 32 and node residency capped at 100. An active set of 500
does not raise the per-node residency limit.

| Active set / distribution / repetition | OK / 2,400 | Busy drops | HTTP 503 | Delivered RPS in window | Scheduled p95 / p99 (ms) |
| --- | --- | --- | --- | --- | --- |
| 100 / uniform / 1 | 2,344 | 56 | 0 | 19.500 | 1,447.299 / 3,198.533 |
| 100 / uniform / 2 | 2,392 | 8 | 0 | 19.900 | 732.404 / 1,556.094 |
| 100 / uniform / 3 | 2,400 | 0 | 0 | 20.000 | 387.751 / 714.107 |
| 100 / skewed / 1 | 2,400 | 0 | 0 | 20.000 | 169.310 / 388.415 |
| 100 / skewed / 2 | 2,400 | 0 | 0 | 20.000 | 206.813 / 395.493 |
| 100 / skewed / 3 | 2,400 | 0 | 0 | 20.000 | 297.194 / 611.530 |
| 500 / uniform / 1 | 2,349 | 50 | 1 | 19.467 | 1,360.391 / 2,787.405 |
| 500 / uniform / 2 | 2,315 | 76 | 9 | 19.267 | 1,557.115 / 3,131.934 |
| 500 / uniform / 3 | 2,379 | 16 | 5 | 19.783 | 684.082 / 1,993.517 |
| 500 / skewed / 1 | 2,400 | 0 | 0 | 19.983 | 1,012.309 / 1,710.166 |

Total: **23,779 OK / 24,000 scheduled, 206 busy drops and 15 HTTP 503s**.
Busy drops have no completed-request latency and are not silently omitted from
arrival counts. Percentiles cover completed attempts, including HTTP errors;
RPS counts only successful completions inside the schedule window. Successful
requests completed during drain remain in OK counts but not in-window RPS.
No rate, cap, assertion or timeout was relaxed to obtain a passing result.

The concurrent 300-second critical schedule also failed its arrival gate:

| Check | Closed result |
| --- | --- |
| Scheduled workflows | 19 OK / 20 scheduled; one driver-busy drop, no writes for that dropped arrival |
| Attempted workflows | All 19 receipts complete: 323 successful steps and 38 acknowledged repository UUIDs |
| Delivered workflow throughput | 0.060000/s in-window; 0.060223/s including drain |
| Whole-workflow p50 / p95 / p99 | 37.089 / 60.551 / 60.551 seconds, including stock-Git work and validation |
| Correctness boundary | Ledger and receipt integrity passed; recovery of these ACKs after owner loss remains open |

The read-only watcher audits each sealed window's exact sequence, deterministic
selection, outcomes, latency, throughput and resource bindings before copying
four finalized files. It also preserves each attempted workflow's receipt and
local Git data. Changing campaign indexes, logs and live provider data are not
copied. Observation and file-copy work are separate overhead; process self-CPU
and proxy counters are not full-host CPU or cost measurements.

Campaign outputs are at
`/Volumes/Workspace/CrabData/canopy-original-full-0dc04a6-j6promvd`;
per-window copies and manifests are at
`/Users/haipingfu/.codex/canopy-original-full-window-copies-qknece2i`.
The critical audit is `closed-critical-load-audit.json` in the qualification root;
its verified final report/source copy is
`/Users/haipingfu/.codex/canopy-original-closed-critical-dved6eyf`.
These are local evidence copies, not off-machine or provider-data backups.

| Closed critical artifact | SHA-256 |
| --- | --- |
| Report | `66a1468b8e51b2b4254813d06af12e04e802fa5598512f6bb0081cc484d16285` |
| Sample ledger | `1ae9c44266cb49eeffaec80406ac74f3e3a342e6ab5b07fc42761a99fdb9988a` |
| Verified backup manifest | `b8effa1957630581b09d1eec6ec7d25d8f7813449c956b8f203b564db50c8ea3` |

Diagnosis remains open. In the first two windows, dispatch p99 was
12.760/20.340 ms versus service p99 3,190.791/1,550.308 ms. Proxy connection
deltas were balanced (85/85/84 and 106/106/105), with no proxy errors or
rejections. This weakens timer scheduling and connection-count imbalance as
dominant explanations; it does not prove a Directory or RustFS bottleneck.

## Remaining verification

Cellule upstream subsequently advanced by two commits to
`191409685b001a82bd02780def45102b4fc2f164`, observed when publishing this checkpoint.
Those commits change runtime forwarding/compaction and peer HTTP CI gates.
They are **not** the dependency revision tested here. PR #18 now pins that
revision: its Linux debug CI passed 244 Rust tests, 84 Python tests and all eight
fresh RustFS gates. Its release candidate passed, but later PR-head release CI
failed the disconnected-admission test with HTTP 503. All 100 isolated and two
full-target diagnostic runs passed on unchanged source; that does not resolve
the failure. Native qualification, retained-store recovery and performance
remain open. See the
[current dependency checkpoint](2026-10-01-cellule-main-qualification.md).
No result from this activation or frozen campaign transfers to the new pin.

The [full campaign](../performance-plan.md) still requires
the complete schedule and audit, concurrent faults, fresh-owner verification
of the original corpus and every newly acknowledged write, higher admission
profiles, matched comparisons, large
transfers and isolated Linux capacity. The earlier diagnostic lease-fencing
failure remains unexplained; successful activation does not establish its cause.
