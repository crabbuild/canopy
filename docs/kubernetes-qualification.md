# Kubernetes qualification progress, 2026-09-30–2026-10-01

The new local evaluation server is running at <http://127.0.0.1:18082>.
The Kubernetes **tree** gate now passes, including recovery. The complete
upstream history gate is running; it is not yet a capacity qualification.
Earlier failures are retained below for comparison.

The measured predecessor release binary (small packed blobs) is
`e67bd56807df3fde4b8cd2dc1f034216ee38ff920fa8dd3ee9ad908c75779d8f`,
based on revision `9c5f1d1bf837fdc2e38ee229aa409712b9579f69`.
Its source fingerprint and provider identity are in each report. The complete
source mirror includes 1,312 branches/tags and 162,267 reachable commits,
with 1,812,898 reachable objects at HEAD
`6d805ebe018f428d503fdc01bb6d57bd9574f598`.

The implementation retains verified receive packs and indexes durably, with canonical
identities and graph certificates in SQLite. The measured tree build keeps small blobs compressed. The current PR extends
this to oversized packed blobs with bounded streaming verification, reads packs
in physical order, and increases metadata/certificate batching; these newer
changes still require the complete-history gate. Recovery restores
verified packs and uses a durable coverage proof rather than reconstructing
all previously covered objects. Existing object identities and digests are
checked before approving a pack, including thin-pack bases.

| Current fixture | Result |
| --- | --- |
| Pristine Kubernetes HEAD tree: 29,839 objects, one synthetic root | Passed: push 127.567 s; warm clones v0 9.799 s, v2 8.292 s; incremental push 1.272 s, fetch 0.709 s, pull 0.749 s; cold clone 7.293 s; all strict full `fsck` and ref/tree comparisons passed |
| Eight-push compressed history: 1,616 objects | Background repack published in 0.376 s, reducing serving-cache bytes 356,499 → 146,715; subsequent clone 0.915 s and strict `fsck` passed |
| Complete upstream Kubernetes history | Running; no passing claim yet |

The tree fixture's SQLite file is about 10.13 MiB after recovery, and sampled
node-plus-descendant RSS peaked at 120.16 MiB. Timings include competition from
other builds and repository evaluations on this host; they are not an isolated
throughput or concurrency SLA measurement.

Verification of the measured predecessor: 113 library tests passed with one test thread; the repository Cell
integration gate passed; filtered-clone tests passed (two tests); the compressed
pack backup/restore gate passed after deleting original storage. The initial
concurrent library run had one native-fence cleanup assertion fail; the serial
run passed. This does not establish cancellation behavior under arbitrary
concurrent process creation. Four local-evaluation helper tests passed.

Maintenance checks each resident repository every 60 seconds, triggers at 1,024
loose objects or eight pack files, and admits one job per process. It uses one
native compression thread, verifies a new generation, preserves active reader
ownership, and skips publication if concurrent writes change its inventory.
It has supervised shutdown cancellation and separate admission from transfers.
Native Git transfers now have a one-hour worker deadline.

Durable artifacts and indexes are included in backup verification/copy/restore.
Serving-cache repack does not delete durable packs or repoint blob locators.
Durable pack compaction/garbage collection, preview schema migration, provider
fault tests and bounded Linux concurrent-load qualification remain production
work. See [the operational recipe](../deploy/local-evaluation.md).

Current evidence:

```text
/Volumes/Workspace/crabbuild-target/canopy-scale-eval-20260930/
  packed-kubernetes-snapshot/report.json
  packed-kubernetes-full/report.json
  packed-kubernetes-full/metadata-latency.jsonl
  maintenance-proof/report.json
  packed-service/server.log
  packed-lib-serial.log
  packed-backup-test.log
  packed-partial-test.log
```

## Earlier evaluation of the original release

**Result: this build did not pass Kubernetes-sized repository hosting on the
local evaluation host.** The clean full-history push hit the native HTTP Git
worker deadline before durable object ingestion. A separate Kubernetes tree
fixture imported successfully, but its full clone failed. Neither large fixture
completed the recovery gate. These are observed failures for this environment,
not a universal maximum repository size.

## Build and environment

- Canopy revision: `9c5f1d1bf837fdc2e38ee229aa409712b9579f69`.
- Cellule pin: `a3fbfb0115a1ae2519ee8f8e0cf6b8e72fdaa303`.
- Release binary SHA-256: `e44148670da22af1a870837cdbec48fe633f2c09561b3870c20c65d828958ed0`.
- Rust/Cargo 1.97.0; Apple Git 2.50.1; 12-CPU, 32-GiB Apple Silicon macOS host.
- Workspace: external USB APFS SSD. Other repository evaluations were running
  on the host; this was not an isolated throughput benchmark.
- Provider: local ARM64 Docker RustFS image
  `ghcr.io/rustfs/rustfs@sha256:0c3c7030ffb93afde8d359fb1db957b85033ede05115518bd0dede51f4353f6a`,
  with a dedicated volume and application prefix per node. Provider limits:
  2 GiB memory, two CPUs, 256 processes, bounded logs. The Docker VM has eight
  CPUs and about 16 GiB memory.
- Each native Canopy node: three active repository slots, 64 GiB disk admission
  budget. No hard cgroup CPU/memory/filesystem containment for the host process.
- Large tests ran on separate nodes. The full-history node was stopped after
  recording its failure. The evaluation node remains at
  <http://127.0.0.1:18080>, with credentials in its private state directory.

## Measured results

| Fixture | Scope | Result |
| --- | --- | --- |
| Clean Kubernetes history | Source HEAD `08147af84478f859c2e2234d71ceace8bdb412c7`; 141,666 reachable commits, 1,663,509 reachable objects; source packed object store about 1.22 GiB | Atomic push rejected in 265.747 s; server recorded `Http(Timeout)` |
| Kubernetes tree fixture | Source HEAD `1124a801ebcedde8880b3cb9a4721745bad55c4c`; one synthetic root commit, 30,021 reachable objects | Push passed in 314.899 s; protocol-v0 mirror clone failed in 298.077 s with truncated pack/RPC failure |
| Small welcome/recovery fixture | Three initial objects, one initial commit, then one additional commit | Protocol-v0/v2 clones, strict full `fsck`, incremental push, fresh-workspace restart, cold clone and restored ref/tree comparison all passed |

The clean source contains complete master history, not the complete set of
GitHub release branches, tags or hosting metadata. Its copied fixture also
contained a redundant remote HEAD alias as a branch; both fixture refs were
rejected. The benchmark helper now drops that alias during normalization.
The tree fixture comes from an older local Kubernetes checkout with local
changes (`Superset-arm64.dmg` and `crab.toml` in its tip commit). It is a scoped
tree fixture, not pristine upstream history or a passing large-repository gate.

The full-history server logged:

```text
Git push failed before publication ... error=Http(Timeout)
```

The associated native cache cleanup reported `operation would block` and retained
disk admission until process restart. The client received an unpack rejection;
the object insertion high-water mark remained unset during the sampled run.
The native worker deadline is **120 seconds**, beginning after request spooling
and cache preparation; the 265.747-second client duration includes those other
phases. The tree clone's precise terminal server error was not logged, so its
cause remains unconfirmed. Do not label that clone failure a proven timeout.

Sampled node-plus-descendant RSS peaked at 222.5 MiB for the full-history attempt
and 153.25 MiB for the tree fixture. These samples exclude the Git client,
provider and filesystem cache, and do not capture every transient peak. The
tree repository database reached about 239 MiB; its managed local workspace
reached about 412 MiB during clone preparation. Those figures do not size the
full-history database because that import never reached durable ingestion.

While the tree clone and separate full import ran, ten sequential requests per
endpoint all returned 200:

| Endpoint | Median | Maximum |
| --- | --- | --- |
| Readiness | 0.47 ms | 28.55 ms |
| Repository metadata | 1.32 ms | 354.20 ms |
| Root tree browser | 1.92 ms | 143.80 ms |

Ten samples establish basic responsiveness in that phase, not p95/p99 service
levels or concurrent-user capacity.

## Evidence and reproduction

The raw reports and logs are retained under:

```text
/Volumes/Workspace/crabbuild-target/canopy-product-eval-9c5f1d1-20260930/
  full/report.json
  full/push.log
  full/resources.jsonl
  full-service/server.log
  snapshot-v2/report.json
  snapshot-v2/warm-clone-v0.log
  snapshot-v2/resources.jsonl
  metadata-probe.json
  welcome-proof-v2/report.json
  main-proof/report.json
```

Use [the local evaluation recipe](../deploy/local-evaluation.md) and
`scripts/benchmark_large_repository.py` with a fresh repository name and work
directory. The helper rejects shallow source repositories and records exact
refs and source trees. Reports distinguish a failed stage from a completed
clone/recovery gate; do not advertise a passing push as end-to-end support.

The passing small recovery run also exercises remote HEAD normalization. The
setup helper's restart probe initially rejected the port while accepted
connections were in `TIME_WAIT`; it now uses address reuse, and the complete
recovery run passed after that fix. Four focused helper tests passed for identity
reuse, private credentials/environment isolation, unrelated-directory rejection,
stale PID protection and changed-executable rejection.
The same complete small-repository gate also passed with a bare source on
`main` and no remote-tracking refs, exercising the helper's default-branch and
empty-normalization paths.

## Product work this result prioritizes

1. Make native worker timeout policy explicit and suitable for large pack
   indexing/generation. Preserve cancellation, descendant cleanup and resource
   admission. Merely increasing the deadline does not establish capacity.
2. Record terminal streaming failures and timings for request spooling, native
   Git, object ingestion, graph certification, cache hydration and pack output.
   The failed clone currently lacks a terminal diagnostic in the server log.
3. Measure and optimize object ingestion and cold/warm cache rebuilding.
   Ordinary objects are expanded into SQLite and reconstructed into a native
   cache; compressed pack size is a poor deployment sizing estimate. The
   preliminary all-ref local corpus had about 32 GiB of logical object bodies
   despite about 1.29 GiB packed storage; this is a different corpus from the
   clean master-history test.
4. Re-run pristine master history, release branches/tags, protocol-v0/v2,
   filtered/shallow clones, incremental pushes and fresh-workspace recovery.
   Then qualify concurrent traffic and the intended bounded Linux/provider
   configuration. Keep those claims separate.
5. Qualify collaboration traversal independently. Merge-base and ancestry
   searches currently stop at 100,000 discovered commits or 250,000 edges;
   some operations on this history can exceed those limits. No failing
   pull-request traversal was demonstrated in this evaluation.

Continue the remaining production gates in [the roadmap](../ROADMAP.md),
especially migrations, garbage collection, provider/failure qualification,
operational metrics and alerts, TLS/credential operations and release artifacts.
