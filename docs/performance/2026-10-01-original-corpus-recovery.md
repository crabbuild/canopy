# Original corpus maintenance recovery

At this recovery checkpoint, the original RustFS corpus had
**10,003 durably idle, unowned Cells**:
10,000 repository identities, two critical Git repositories and Directory.
Normal fenced recovery with the exact old executable settled all 301 remaining
Cells. Two complete post-recovery snapshots and an independent receipt audit
passed. All published roots, catalog identities and Cell incarnations stayed
unchanged.

The deployment remained in **old-release Maintenance**, revision 5. No new
release or serving gateway was admitted. This closes the offline metadata and
maintenance-recovery prerequisite, not remote Git/LFS content verification,
same-corpus upgrade, performance or an explanation of the earlier lease failure.

The subsequent [full-corpus release activation](2026-10-01-original-corpus-activation.md)
passed separate new-release admission and reached Ready revision 9 without
changing these Controls or roots. Remote Git/LFS verification remains open.

## Recovery sequence

```text
Old Ready release, revision 3
  -> read-only scan of all 256 shards and 10,003 Controls
  -> exact old descriptor supports every catalog and Control code/schema
  -> three recorded owners canonically retired; zero advertised writers
  -> identical second snapshot; preserve closed evidence
  -> old executable begins exact maintenance operation, revision 5
  -> new recovery identity and scratch disk; normal fenced acquire and drain
  -> old executable reports zero unsettled Cells; worker exits zero
  -> two full read-only scans; independent canonical JSON and receipt audit
  -> remain in old Maintenance; upgrade and serving admission still closed
```

The readers use a transport wrapper that refuses every mutation before it
reaches RustFS. Its test rejects put, create, multipart, copy and both delete
paths while preserving the original bytes. Unsupported command mode and wrong
endpoint are rejected before provider access. Both tools passed locked offline
release builds, unit tests and all-target Clippy with warnings denied; dependency
versions match the qualified Cellule `0dc04a6` metadata.

Recovery used the retained **old** executable, its exact selected descriptor and
image setting, a new NodeID, an owned test signing key and a fresh workspace.
It did not erase ownership, force a Control CAS, widen leases, rewrite catalog
identity, change runtime budgets, restart RustFS or reseed the corpus. The worker
drained acquired Cells through the existing runtime before removing only its
closed restore scratch.

## Verification results

| Check | Closed result |
| --- | --- |
| Before recovery | 9,702 idle and 301 serving Controls; all 10,003 expected Cells present |
| Old release support | Exact persisted descriptor, initial catalog code/schema and actual Control code/schema verified across all 256 shards |
| Previous owners | All three canonically retired and not live; zero advertisements, including expired advertisements |
| Stable pre-recovery observation | Two identical complete snapshots; zero attempted provider writes |
| Old maintenance recovery | Exit zero; zero advertisements and zero unsettled Cells; recovery command took 234.461 seconds |
| Post-recovery observation | Two identical complete snapshots; all 10,003 Controls idle, unowned and rooted |
| Previously idle Cells | All 9,702 canonical Controls and their ETags unchanged |
| Recovered Cells | All 301 retain incarnation and code/schema; fencing epoch and revision advance |
| Durable metadata | All published roots, catalog shard revisions/page digests and service-root bytes/ETag unchanged |
| Independent audit | Receipt/source bindings verified; exact counts and root transaction/commit sequence checks passed; all owned workers and children absent |
| RustFS | Same container, volume, image, configuration, start time, resource envelope and restart count; no OOM |

The durations are maintenance and complete-scan wall times on the shared
Mac/Colima host. They are not Git request latency, throughput, a matched
improvement or isolated Linux capacity. The corpus manifests still describe
100 populated Git/LFS fixtures and two critical repositories; this checkpoint
does not claim to have cloned or checked their payloads remotely.

## Commands and admission boundaries

On a separately verified, caller-owned deployment, the existing old-binary CLI
provides the maintenance operations:

```sh
canopy maintenance /path/to/exact-old-config.json status
canopy maintenance /path/to/exact-old-config.json begin <operation-uuid>
canopy maintenance /path/to/exact-old-worker-config.json recover <operation-uuid>
canopy maintenance /path/to/exact-old-config.json status
```

These commands do not replace full catalog admission. Verify the exact old
executable, descriptor and supported persisted metadata first; use a fresh
worker identity and workspace. Do not end maintenance or activate another
release until its separate admission requirements are proven. The retained
readers and wrapper are hard-scoped qualification tools, not general upgrade
commands.

## Evidence and remaining work

Closed evidence is retained at
`/Users/haipingfu/.codex/canopy-original-corpus-admission-YxGodu`.
All 73 closed files and bound inputs were copied and independently reread at
`/Volumes/Workspace/CrabData/canopy-original-recovery-closed-181o8fa6`.
This is another local filesystem, not an off-machine or provider-data backup.
The first offline-resolution failure, map-entry lint failure and output-name
collision are preserved separately; existing receipts were never overwritten.

| Artifact | SHA-256 |
| --- | --- |
| Exact old executable | `32b114119960608c0a91d1c783bb69eafec432831bfa452d54d8950b09bc0e99` |
| Pre-recovery snapshot, both copies | `3c53357ec9568ba878a7d21e347038fd09a366559b7bd06f24fc98d4ef89fc49` |
| Same-code recovery receipt | `28e6e6899b946b62bad8c4312daca884a4377d93c159aabb588f8feb8d027e38` |
| Post-recovery snapshot, both copies | `de62eebeaa609937759186126ff6540e823e3ddbb209df8becb6128a21fc96ce` |
| Post-recovery admission | `2b4b691d47d8910106593046dc6efbb3553e8496ee2c9cedfe516601db9a77bd` |
| Independent closed audit | `219331a18c0dcb0e9be65885f22f73bea610befefb66ec3d70e4b018824fd95a` |

The checks ran October 2 at 01:46–01:57 UTC, October 1 Pacific.
The selected old release at this checkpoint was
`e31bf1a951e2fa19d91e9f964b2ddeade1a81b05a20ad628362819a1487c16b1`;
maintenance operation is `74f10df1-417c-4c63-83b0-b554fdb996e4`.
The UI preview and unrelated providers were not changed or signalled.

The subsequent activation checkpoint closes rolling-descriptor and full
actual-Control admission, controlled activation and three-gateway readiness.
Its subsequent full remote Git/LFS verification passed with an independent
audit and verified evidence copy. Fresh-owner/every-ACK recovery after the
current load remains open; that checkpoint records failed diagnostic arrivals.
The [full campaign](../performance-plan.md) remains **108 windows, 114,960
arrivals and 8,640 scheduled seconds**, with independent rates/concurrency,
uniform/skewed access, higher residency profiles, concurrent critical workflows
and faults, matched comparisons, large transfers and isolated Linux capacity.
No new serving or performance result is claimed here. PR #18 remains draft.
