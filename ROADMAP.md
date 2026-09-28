# Canopy roadmap

Canopy's Git-hosting core works, but the service is not yet ready for an unattended team deployment. This roadmap tracks the remaining work from the current `main` baseline (`ab43e71`, 2026-09-27). It is a planning checklist, not a claim that every capability mentioned below is absent. See [delivery gates](docs/delivery-plan.md) for the detailed acceptance record, [Git compatibility](docs/git-compatibility.md) for protocol coverage, and [performance evidence](docs/performance-plan.md) for measured limits.

## How to use this roadmap

- Check off a task only when its acceptance proof passes against the intended release build and the result is recorded in the linked delivery or performance document. A working implementation or a local happy-path test alone does not close a gate.
- Add an issue or PR link beside a task when work starts. Keep the current status and evidence in the source documents; update this summary when a gate changes.
- Milestones are ordered by dependency, not by calendar date. The private team release includes **R01–R10**, plus its exit checks. Later features can be delivered in smaller slices.
- Preserve the current architecture: one Canopy server binary using Git, local SQLite workspace, an object store, and HTTPS ingress. Do not impose fixed repository or file-size caps to substitute for resource admission and measurement.

## Milestone 1 — single-node private team release

This is the first intended production-use milestone. The existing bounded Compose profile is a starting point, not a completed install or capacity claim.

### R01. Safe upgrades and schema migration

- [ ] Version persistent Canopy and runtime formats; add a fenced, repeatable upgrade command and migration path for a populated deployment.
- [ ] Require a verified backup and compatible release/configuration before migration; document failed-upgrade recovery and rollback limits.
- [ ] Upgrade a prior release containing Git objects, LFS, ACLs, issues, checks, and pull requests; interrupt and retry each migration phase, then clone and inspect the restored state.
- [ ] Define whether mixed-version nodes are supported. Until proven, enforce a fully drained, single-version upgrade.

**Current state:** schema changes still require a fresh storage prefix; maintenance admission and drain exist, but there is no upgrade path.

### R02. Backup, restore, and owner-recovery fault qualification

- [ ] Inject process death, lease loss, competing workers, stalled storage, and provider failures at every maintenance claim, restore, drain, backup pin, copy, and completion boundary.
- [ ] Prove same-operation retries converge, old owners cannot publish, and incomplete backup/restore destinations cannot serve traffic.
- [ ] Restore after deleting the original storage prefix; verify stock Git clone and `fsck`, LFS hashes, ACLs, issues, reviews, checks, and pull-request state.
- [ ] Add and test cross-provider export/restore so loss of a bucket or provider has a recovery path.

**Current state:** fenced maintenance recovery and same-provider backup/isolated restore exist; the full failure matrix and cross-provider copy do not.

### R03. Safe retention and garbage collection

- [ ] Define retention windows and a complete root set covering live refs, active fetches, pending pushes and merge candidates, backup pins, recovery roots, and incomplete transfers.
- [ ] Add a fenced collector for abandoned uploads, old outcomes/candidates, and unreachable Git/LFS objects, with an inspectable dry run and bounded work.
- [ ] Prove with concurrent reads, backup, publication, owner loss, and injected collector failures that required bytes are never removed; collect abandoned bytes only after their grace period.

**Current state:** no production collector; abandoned staging and outcomes can accumulate. Collection must remain conservative until its root proof passes.

### R04. Trustworthy CI and release artifacts

- [ ] Keep formatting, Clippy, ordinary tests, and smaller provider probes on standard CI.
- [ ] Run the unchanged release-mode >5 GiB Git/LFS recovery gate on a runner with at least 40 GiB of temporary space; retain its logs and exact revision. The current `ubuntu-latest` workflow cannot supply that space.
- [ ] Add required failure-path and provider gates to branch protection, then publish versioned, digest-identified images and a repeatable release checklist.
- [ ] Record toolchain, Git, Git LFS, Cellule revision, image digest, provider, and test outcomes for each release candidate.

**Current state:** `.github/workflows/verify.yml` invokes the full size gate on `ubuntu-latest`; a previous hosted run exhausted disk.

### R05. Production object-store qualification

- [ ] Select the intended S3-compatible service and test conditional writes/copies, multipart behavior, latency, throttling, credential rotation, timeouts, and outages.
- [ ] Run stock Git/LFS transfer, owner takeover, backup, restore, and the size gate against that service; record the supported configuration and failure behavior.
- [ ] Make separate compatibility claims and test plans for GCS or Azure if either is adopted.

**Current state:** isolated RustFS tests, including a release-mode >5 GiB recovery run, prove important paths locally; they do not qualify a production provider.

### R06. Operational visibility and runbooks

- [ ] Expose low-cardinality request/transfer latency and error metrics, active and pending Cells, lease and renewal health, queues, object-store calls, local disk, and admission failures.
- [ ] Set alert thresholds from measured limits and test that alerts fire during injected faults.
- [ ] Write and drill runbooks for 503/507, exhausted resources, lost ownership/fencing, failed upgrades, backup verification, and restore.

**Current state:** health endpoints and tracing logs exist; metrics, alerting, and a complete operator contract do not.

### R07. Repeatable single-node deployment

- [ ] Turn [`deploy/README.md`](deploy/README.md) and the bounded Compose profile into a documented install with TLS ingress, SSH host-key handling, secret rotation, bootstrap, health checks, backup schedule, upgrade, and restore drill.
- [ ] Verify the actual cgroup, descriptor, scratch, and Git-worker limits in the install; publish a measured resource envelope and operator checklist.
- [ ] Add a two-node recipe after R02 and R05 establish failover behavior on the selected provider.

**Current state:** the checked-in profile bounds one Linux node; TLS and operational procedures remain deployment work.

### R08. Organizations, teams, and account lifecycle

- [ ] Add shared repository ownership, organizations/teams, invitations, team grants, and grant management in the UI.
- [ ] Define and implement repository transfer, account reactivation/deletion, and the effect of those actions on ACLs, audit history, authored content, and active credentials.
- [ ] Test permission changes during Git/LFS operations and after owner takeover.

**Current state:** repositories have one owner and individual grants; account disablement exists, but reactivation/deletion and team ownership do not.

### R09. External CI integration

- [ ] Add signed webhooks with a durable outbox, bounded retries, delivery history, and replay controls for repository and pull-request events.
- [ ] Document and test an external check-reporting contract that existing CI can use with required branch checks.
- [ ] Prove events are durably retried after worker death and owner recovery; give consumers stable delivery IDs so retries can be deduplicated, and ensure check-report retries do not create duplicate results.

**Current state:** Canopy stores commit checks and can require them for merges; it does not yet notify external CI or provide a complete integration workflow.

### R10. Administration, security audit, and identity

- [ ] Record actor-attributed repository grant, visibility, branch policy, and security-sensitive changes; include denied operations and retention/export decisions where useful for incident review.
- [ ] Provide audit export, retention policy, and documented credential and access revocation behavior.
- [ ] Add optional OIDC/SSO when required by the target team, while preserving PAT and SSH-key access for Git.

**Current state:** account/token changes have durable audit history; repository/security coverage, export, and SSO remain open.

### Private team release exit checks

- [ ] R01–R10 acceptance proof is recorded for the exact release candidate and selected object store.
- [ ] A clean install, upgrade of a populated prior release, backup, source-prefix loss, and restore all pass the operator runbook.
- [ ] Stock Git and Git LFS work after recovery; permissions, issues, reviews, checks, and pull requests retain their state.
- [ ] The release notes state measured limits for active repositories, concurrent transfers, local disk, memory, restore time, and object-store cost. Unmeasured capacity is not advertised.

## Milestone 2 — complete daily collaboration and repository lifecycle

These features can land individually after the private release gates, with their own authorization, retry, and owner-recovery tests.

### R11. Collaboration and discovery

- [ ] Browser conflict resolution; discussion editing and moderation; issue labels and assignees.
- [ ] Releases and assets; notifications; ACL-safe repository and code search with index rebuild after recovery.
- [ ] Forks and pull-request retargeting, if included in the intended team workflow.

### R12. Import, export, archive, and delete

- [ ] Guided mirror import covering refs, LFS, and submodules, with resumable or clearly reversible partial-failure behavior.
- [ ] Repository archive/delete with a recovery window, verified export, and documented retention of audit and backup data.
- [ ] Test import/export against stock clients and restore after interrupted lifecycle operations.

## Milestone 3 — broader service and measured scale

### R13. Realistic capacity and reliability envelope

- [ ] Benchmark populated repositories under mixed Git, LFS, browsing, push, collaboration, and cold-restore traffic on the intended Linux/provider configuration.
- [ ] Report repository identities, active Cells, concurrent transfers, p50/p95/p99 latency, errors, memory, descriptors, scratch, recovery time, provider calls, and cost.
- [ ] Resolve warm metadata latency misses and the observed 1,000-active-Cell renewal coverage failure before raising active-density claims.
- [ ] Test OOM, descriptor/process exhaustion, CPU throttling, and provider latency with explicit admission and recovery behavior.

**Current state:** a 1,000-repository Linux run recovered correctly, but 997 repositories were empty and some latency targets were missed. The 10,000-repository reference target in [the performance plan](docs/performance-plan.md) is unmeasured.

### R14. Cold and warm Git fetch efficiency

- [ ] Finish filter-specific hydration and measure bytes, object-store requests, and first-byte latency for cold HTTP and SSH fetches.
- [ ] Evaluate verified warm disk/pack reuse and cache eviction while preserving ref snapshots, authorization, and fetch correctness during concurrent pushes.

### R15. Protocol and provider failure qualification

- [ ] Complete malformed-filter, incomplete-push, owner-loss-during-publication, SSH interruption, signed-push, bulk-ref, and SHA-256 negative/positive matrices against real providers and multiple operating systems.
- [ ] Bring the malformed-filter HTTP 400 fix through its post-rebase tests and PR if it remains only on the local `codex/filter-syntax-rejection` branch.
- [ ] Decide advanced LFS resumable upload, pure SSH LFS, dumb HTTP, and remote archive from actual customer needs; add acceptance gates before claiming support.

## Separate objective — full repository Cell capabilities

### R16. SQL, KV, queue, workflow, and scheduled work in one Cell

- [ ] Agree on the product need and ownership for the full [repository Cell capability proposal](docs/repository-cell-primitives.md).
- [ ] Extend Cellule's catalog, typed clients, lifecycle inventory, and bounded background runners so SQL, KV, queue, workflow, Blob, Cron, Timer, effects, and projections share one repository identity and recovery root.
- [ ] Integrate real Canopy operations and prove atomic composition, autonomous progress after eviction, owner-takeover safety, backup/restore, and mixed-workload density.

**Current state:** Canopy uses SQL repository Cells. The full primitive objective is substantial runtime and product work, with its own acceptance gates; it is not required for the first private Git-hosting release.
