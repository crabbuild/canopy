# Deliver Canopy

The executable acceptance gates below define when Canopy can be called a Git
hosting service. Each gate needs a black-box client action, the durable side
effect, and the same visible result after a new node restores Cell state.
Do not infer completion from compilation or a disposable cache test.

| Gate | Deliverable | Acceptance proof | State |
| --- | --- | --- | --- |
| 0 Independent build | Pin an immutable Cellule revision; build `canopy-server` without local paths or Crab product crates | Fresh checkout builds in CI | Open: local paths remain |
| 1 Node process | `canopy` binary, validated config, CellNode lease/renewal, listener, readiness, drain | Start/stop against durable store; no worker or lease leak | Partial: S3-compatible process restart and clean drain pass; worker/lease fault matrix remains |
| 2 Repository lifecycle | Directory Cell, create/list/get/rename, account identity, token scopes, repository ACL | Two users see only authorized repositories; failed creation converges on one UUID | Open |
| 3 Git object path | Bounded pack ingest, SQLite object chunks, verified external large blobs, quotas | Push delta pack; restore exact bytes and OIDs after owner loss; reject corruption | Partial: small and 64 MiB buffered paths work |
| 4 Atomic push | Durable push session, graph closure proof, ACL and branch rules in finalization, recorded retry outcome | Concurrent and multi-ref pushes, ABA, owner death at every publication boundary | Partial: Cell ref CAS and stock push work |
| 5 Fetch | Bounded streaming upload-pack, snapshot refs, cold recovery | Clone/fetch after owner takeover while refs move; large corpus capacity evidence | Partial: stock clone after gateway restart and clean Cell owner move works |
| 6 LFS | Batch/basic transfer, verified bytes, quotas and transfer admission | Stock `git-lfs` push/pull after owner loss; wrong hash/size and interruption fail closed | Partial: stock push/pull after gateway restart works |
| 7 Collaboration | Issues, comments, checks, rules, pulls, reviews, merge, releases, repository UI | Create, review, check, merge and reload across owner change | Open |
| 8 Recovery and operations | Two-node routing, backups, restore, conservative GC, audit and metrics | Kill owner, lose local disk, restore from backup, clone and inspect collaboration data | Partial: single-repository process lease takeover and cold clone pass; routing/backup/GC/telemetry remain |
| 9 Public service | Public visibility, organizations/teams, search and webhooks | ACL-safe anonymous reads, revocation, index rebuild and webhook retry | Open |

The **internal preview** requires gates 0–5, including real storage and
two-node owner loss. A private beta requires gates 0–8. A public release
requires gate 9 and measured limits for repository count, hot repository
throughput, pack size, concurrent clients, restore time and storage cost.
Hosted CI runners, packages, forks and GitHub API compatibility require
separate product decisions.

## Next reviewable changes

1. Expand the S3-compatible process smoke into a node/lease fault matrix and
   test the target production object store. Pin an immutable public Cellule
   revision before a fresh checkout can build independently.
2. Replace the buffered CGI and per-object subprocess path with bounded
   streaming pack ingest/fetch. Add SQLite chunks for large trees, commits
   and tags, plus a real corpus benchmark. Keep the bare repo disposable.
3. Add durable push outcome records and reachable-closure verification to the
   Cell finalization contract. Make a lost response and retry deterministic.
4. Add account, directory and ACL Cells, then route multiple repositories by
   stable UUID and owner/name. Keep authorization at every read and write
   boundary, including Git and LFS.
5. Implement collaboration as vertical slices: issues; checks and branch
   rules; pull requests, reviews and merge; releases and assets; UI. Each
   slice ships with its own public action and owner-recovery proof.

Keep LFS bodies and unreferenced Git objects under conservative retention
until a fenced collector can prove the complete root set. No automatic GC
should remove bytes while fetch, backup or a pending merge can still read them.
