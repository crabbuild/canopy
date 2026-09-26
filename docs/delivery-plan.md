# Deliver Canopy

The executable acceptance gates below define when Canopy can be called a Git
hosting service. Each gate needs a black-box client action, the durable side
effect, and the same visible result after a new node restores Cell state.
Do not infer completion from compilation or a disposable cache test.

| Gate | Deliverable | Acceptance proof | State |
| --- | --- | --- | --- |
| 0 Independent build | Pin an immutable Cellule revision; build `canopy-server` without local paths or Crab product crates | Fresh checkout builds in CI | Partial: immutable Git revision pinned and local fresh-checkout proof; hosted CI pending a Canopy remote |
| 1 Node process | `canopy` binary, validated config, CellNode lease/renewal, listener, readiness, drain | Start/stop against durable store; no worker or lease leak | Partial: S3-compatible process restart and clean drain pass; worker/lease fault matrix remains |
| 2 Repository lifecycle | Directory Cell, create/list/get/rename, account identity, token scopes, repository ACL | Two users see only authorized repositories; failed creation converges on one UUID | Partial: durable accounts and per-repository Git/LFS roles survive recovery; account lifecycle, collaborator listing and get remain |
| 3 Git object path | Bounded pack ingest, SQLite object chunks, verified external large blobs, quotas | Push delta pack; restore exact bytes and OIDs after owner loss; reject corruption | Partial: disk-accounted requests admit 512 MiB pushes; per-object buffers and 64 MiB blob ceiling remain |
| 4 Atomic push | Durable push session, graph closure proof, ACL and branch rules in finalization, recorded retry outcome | Concurrent and multi-ref pushes, ABA, owner death at every publication boundary | Partial: ref CAS, ACL, ABA protection, ordinary mixed push results, atomic rejection and exact HTTP push replay survive recovery; typed graph closure is enforced; branch rules and publication fault matrix remain |
| 5 Fetch | Bounded streaming upload-pack, snapshot refs, cold recovery | Clone/fetch after owner takeover while refs move; large corpus capacity evidence | Partial: backpressured fetch responses and v0/v2 clones above 80 MiB pass after takeover; consistent ref snapshot, cache admission and corpus capacity proof remain |
| 6 LFS | Batch/basic transfer, verified bytes, quotas and transfer admission | Stock `git-lfs` push/pull after owner loss; wrong hash/size and interruption fail closed | Partial: stock push/pull after gateway restart works |
| 7 Collaboration | Issues, comments, checks, rules, pulls, reviews, merge, releases, repository UI | Create, review, check, merge and reload across owner change | Open |
| 8 Recovery and operations | Two-node routing, backups, restore, conservative GC, audit and metrics | Kill owner, lose local disk, restore from backup, clone and inspect collaboration data | Partial: process lease takeover and cold clone pass for two repository Cells; multi-node routing/backup/GC/telemetry remain |
| 9 Public service | Public visibility, organizations/teams, search and webhooks | ACL-safe anonymous reads, revocation, index rebuild and webhook retry | Open |

The **internal preview** requires gates 0–5, including real storage and
two-node owner loss. A private beta requires gates 0–8. A public release
requires gate 9 and measured limits for repository count, hot repository
throughput, pack size, concurrent clients, restore time and storage cost.
Hosted CI runners, packages, forks and GitHub API compatibility require
separate product decisions.

## Next reviewable changes

1. Expand the S3-compatible process smoke into a node/lease fault matrix and
   test the target production object store. Run the checked-in CI workflow on
   a Canopy remote. The pinned Cellule revision currently lives on a public
   branch; [Cellule PR #5](https://github.com/crabbuild/cellule/pull/5)
   proposes the UUID partition contract. The storage capability probe also
   needs to land upstream before Canopy can pin a revision on `main`.
2. Replace the per-object subprocess path and account bare-cache disk usage.
   Requests now spool under shared disk admission and CGI read responses stream
   through a bounded queue. Add SQLite chunks
   for large trees, commits and tags, plus a real corpus benchmark and bounded
   graph certification for
   large initial pushes. Keep the bare repo disposable.
3. Qualify durable push replay at every staging/publication boundary. Exact
   HTTP replay now binds a UUID to account and request digest and atomically
   publishes the complete response with ref changes. Typed graph connectivity
   now gates both ref commands. Add branch rules; define retention and quotas
   for completed outcomes and abandoned staging chunks before persistent use.
   Keep testing distinct IDs for identical bytes after refs change: Cellule
   command deduplication alone does not identify an HTTP operation.
4. Complete account lifecycle, token rotation/revocation, collaborator-visible
   listing, and audit records. Test revocation during in-flight Git and LFS
   operations, including a node takeover.
5. Implement collaboration as vertical slices: issues; checks and branch
   rules; pull requests, reviews and merge; releases and assets; UI. Each
   slice ships with its own public action and owner-recovery proof.

Keep LFS bodies and unreferenced Git objects under conservative retention
until a fenced collector can prove the complete root set. No automatic GC
should remove bytes while fetch, backup or a pending merge can still read them.

## Transfer-size qualification

The initial 2026-09-25 streaming-response build on Darwin arm64, using Apple Git 2.50.1,
passed `smoke_s3_process.py --large-clone` against RustFS
`1.0.0-beta.8-glibc`. Two separate 40 MiB random-blob pushes
remained within the current ingress limit. After clean restart, owner death,
lease expiry and local database loss, each stock Git clone received an
83,912,143-byte pack, reproduced both SHA-256 file hashes and passed `git fsck`.

| Client protocol | Cache state | Clone plus verification time |
| --- | --- | --- |
| v0 | Cold after takeover | 33.63 seconds |
| v2 | Warm from the preceding clone | 7.24 seconds |

These times include client checks and use different cache states. They prove
transfer size and recovery behavior, not comparative protocol speed or service
capacity. The same run passed Git/LFS, ACL, mixed/atomic ref outcomes and exact
replay of a dropped push reply. Unix subprocess tests separately verify bounded
output backpressure, disconnect cleanup, and errors after HTTP headers.

The subsequent disk-backed input build passed the same qualification with both
40 MiB blobs sent in **one** push using a 1 MiB client
[`http.postBuffer`](https://git-scm.com/docs/git-config#Documentation/git-config.txt-httppostBuffer). The
push completed in 19.12 seconds. After takeover, v0 and v2 each restored an
83,912,144-byte pack with matching file hashes and clean `git fsck` results
(28.49 seconds cold and 3.89 seconds warm, including verification). The suite
also proves HTTP 507 on exhausted upload admission, successful retry after
capacity is released, and cleanup after input cancellation/disconnect.
