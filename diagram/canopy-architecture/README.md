# Canopy architecture and Cellule integration

Canopy is a Git hosting service embedded in the Cellule framework. The Directory Cell owns names and account identity; each repository UUID selects its own Repository Cell containing Git authority, access policy and collaboration state. Native Git prepares and serves wire protocol data through a disposable cache. A successful write depends on durable Cell publication, so rebuilding that cache does not discard acknowledged repository state.

This atlas explains components, request and control flow, persistence, recovery and the newer packed-storage work. It describes the inspected Canopy revision `9438bb865959fb975d5349ba8b9908b461653821` and the Cellule dependency pinned by Cargo to `161067f5a21703b3e257024bcb64e565fd9657b4`, as of October 4, 2026. Several older documents record different pins; this guide uses the current declarations and executable registration. Diagrams show implementation boundaries, not measured production capacity.

Open the [browsable gallery](index.html) for all diagrams and explanations. Every diagram is a standalone SVG with a matching PNG at twice its logical resolution.

| Diagram | What it explains | SVG | PNG |
| --- | --- | --- | --- |
| 01 | Public clients, node components and storage authority | [System overview](01-system-overview.svg) | [PNG](01-system-overview@2x.png) |
| 02 | How Canopy models Directory and Repository Cells on Cellule | [Framework mapping](02-canopy-on-cellule.svg) | [PNG](02-canopy-on-cellule@2x.png) |
| 03 | Commands, SQLite, LTX and the durable acknowledgement boundary | [Durable command](03-durable-command.svg) | [PNG](03-durable-command@2x.png) |
| 04 | Authentication, residency, local routing and signed peer routing | [Request routing](04-request-routing.svg) | [PNG](04-request-routing@2x.png) |
| 05 | Push preparation, object ingestion, policy checks and exact replay | [Git push](05-git-push.svg) | [PNG](05-git-push@2x.png) |
| 06 | Fetch, browse, LFS and current physical storage choices | [Reads and LFS](06-read-and-lfs.svg) | [PNG](06-read-and-lfs@2x.png) |
| 07 | Startup, leases, takeover, exact restore, drain and backup | [Owner recovery](07-owner-recovery.svg) | [PNG](07-owner-recovery@2x.png) |
| 08 | Repository features and merge publication controls | [Domain and policy](08-domain-and-policy.svg) | [PNG](08-domain-and-policy@2x.png) |
| 09 | Packed catalog primitives, remaining integration and capability proposals | [Storage evolution](09-packed-storage-evolution.svg) | [PNG](09-packed-storage-evolution@2x.png) |

## System components

![Canopy components and storage boundaries](01-system-overview.svg)

The `canopy` binary runs one Rust service. Axum handles HTTP, JSON APIs, the embedded browser and Git LFS; an optional russh listener handles SSH Git operations. `RepositoryManager` resolves identities, admits repository transitions, binds local or remote Cell clients and retains request pins. `GitGateway` turns repository state into a native Git workspace and translates native results into authoritative Cell operations. `LfsService` verifies and publishes LFS bodies independently of native Git.

The Rust workspace has three crates. `canopy-git-format` supplies object kinds, SHA-1/SHA-256 identities and canonical hashing. `canopy-object-storage` owns immutable body and artifact operations. `canopy-server` composes those crates with Cellule and owns product schemas, protocols, authorization and deployment lifecycle. These are linked components of the service, not separately deployed microservices. See the [workspace map](../../docs/workspace.md) and [server assembly](../../crates/canopy-server/src/server/mod.rs).

## Modeling Canopy on Cellule

![Domain modules, Cell targets and framework components](02-canopy-on-cellule.svg)

`CanopyApplication::register` installs `DirectoryModule` and `RepositoryModule`, then declares their SQL Cell types. Modules describe schema migrations, operation IDs, codecs, code digests and limits. A compiled registry binds these contracts to the application and selected release. Product wrappers invoke registered commands and queries through typed handles. See [application and repository registration](../../crates/canopy-server/src/lib.rs) and [Directory registration](../../crates/canopy-server/src/directory/mod.rs).

| Canopy concept | Cellule representation | Consequence |
| --- | --- | --- |
| Deployment identity | Tenant and application IDs | Scope every durable target |
| Directory | SQL namespace `[0x48; 16]`, fixed shard zero | One shared name and identity authority per tenant/application |
| Repository | SQL namespace `[0x47; 16]`, entity partition derived from UUID | One independently owned Cell for each repository |
| Directory operations | `SqlCell<DirectoryModule>` plus credential commands | Account state and name changes stay inside the Directory boundary |
| Repository operations | `SqlCell<RepositoryModule>` plus typed domain commands | Ref publication can check ACL, graph and policy in one transaction |
| Mutation | `MutationIdentity` and recorded outcome | Retries preserve one logical command identity |
| Observed write position | `Receipt` | A later read can require that per-Cell position |
| Node lifecycle | `CellNode` and installed runtime/replica facilities | Readiness and shutdown depend on supervised framework state |

The repository partition is a prefix byte followed by a domain-separated 32-byte digest; `RepositoryCell::new` verifies that the supplied UUID derives the supplied target. A rename changes the Directory mapping while retaining the repository UUID and Cell identity. Creating a repository uses a durable pending name reservation, initializes the Cell and then marks the name ready. Those steps coordinate separate Cells; they do not form a distributed SQL transaction.

`cellule-app` compiles topology and exposes handles. `cellule-host` supervises the node. `cellule-runtime` manages actors, SQL execution, request outcomes and fenced authority. `cellule-ltx` captures SQLite changes and verifies restoration; `cellule-store` supplies bounded object operations and conditional updates. Canopy supplies public ingress, user authorization, credentials and deployment policy. The [Cellule framework architecture at the pinned revision](https://github.com/crabbuild/cellule/blob/161067f5a21703b3e257024bcb64e565fd9657b4/docs/architecture.md) documents these boundaries.

## Durable command execution

![Command execution and the durability gate](03-durable-command.svg)

A command executes at the current fenced owner of one Cell. Its SQLite transaction records the domain change and the request outcome together. On the object-store path assembled here, LTX captures the committed WAL boundary, verifies and publishes immutable recovery bytes, and supplies a proposed root. A conditional authority update publishes that exact root under the owner fence. The caller receives `Committed<Output>` and a receipt after this durability gate.

Authority fencing prevents a former owner from publishing new authoritative state after ownership changes. A transport timeout can occur after a command committed; resolving its original identity distinguishes a completed outcome from an unstarted operation. A receipt is scoped to a Cell, owner incarnation and commit sequence. It can constrain a subsequent query, but it cannot create an atomic transaction across Directory and Repository Cells. Cellule also documents an optional follower-log durability path; the diagram describes Canopy's inspected object-store setup.

## Request routing and residency

![Routing through authentication, residency and owner resolution](04-request-routing.svg)

HTTP credentials, SSH keys and LFS grants are checked using Directory state. Ready owner/name entries resolve to stable repository UUIDs. Repository routing then binds a `CellClient::local` to a resident actor or a `CellClient::peer` to the current remote owner. Canopy's peer transport signs requests to `POST /internal/cell` and verifies the enrolled sender, signature, release, expiry and principal over HTTPS. It is Canopy's own transport over runtime peer contracts; the optional Cellule peer adapter is not a Canopy dependency. See [peer routing](../../crates/canopy-server/src/server/peer.rs).

The receiving node can run native Git and stream product bodies while Cell calls go to another node. Public requests therefore do not require sticky load-balancer sessions. Node advertisement and Cell ownership are separate controls: the advertisement proves a node session is live; the Cell control record identifies authority for a particular Cell.

Cold and remote transitions use bounded account admission and a lock for that repository. The configured residency limit counts locally bound repository gateways, including remote bindings. When no slot is free, an inactive gateway can be evicted; local ownership must be released before its slot is reused. Requests and response streams pin their route. Supervised activation and cleanup continue after client cancellation. Full admission or ownership movement can return a retryable 503. See [residency](../../crates/canopy-server/src/server/residency/mod.rs) and [admission](../../crates/canopy-server/src/admission.rs).

## Git push and replay

![Push from encoded input to saved durable response](05-git-push.svg)

The gateway spools input to admitted scratch storage and binds the logical push UUID to the account, repository context and encoded request digest. `begin_push` detects completed operations before gzip decoding and native preparation. Reusing an ID with different bytes or an actor produces a conflict. Upload reception can overlap, but the current gateway serializes its ID check, native push work and publication phase through a mutex. See [gateway control flow](../../crates/canopy-server/src/git_gateway/mod.rs) and [owned preflight](../../crates/canopy-server/src/git_gateway/preflight.rs).

New attempts capture consistent refs and policy, run native `receive-pack` against private refs and derive the actual accepted changes. Ingestion verifies canonical object identities, publishes required external bytes, persists bounded object batches and certifies typed graph closure. The gateway stages the response and ref plan. `CompletePush` rechecks current permissions, branch rules, expected OIDs and monotonic ref versions, then commits accepted refs, the generation change and the canonical response pointer together. Cellule publishes the durable result before the client sees success. See [push execution](../../crates/canopy-server/src/git_gateway/push.rs), [saved outcomes](../../crates/canopy-server/src/push/mod.rs) and [shared ref guards](../../crates/canopy-server/src/refs.rs).

There are two identity layers: the push UUID identifies the complete wire operation; Cellule mutation identities identify its individual durable commands. A retry of a completed push replays the saved result and does not reapply old refs over later repository changes. Native partial acceptance is preserved, while stock Git `--atomic` requests atomic validation. Preparation failure cannot publish private refs, although verified unreferenced objects can remain. An uncertain final publication must be resolved rather than reported as a definite refusal.

## Fetch browse and Git LFS

![Read and storage paths](06-read-and-lfs.svg)

Fetch reads generation-consistent refs and HEAD, validates wants against current repository reachability, hydrates selected verified bodies and lets native `upload-pack` generate the response. Warm verified object files are shared across private ref snapshots. Git v2 capability discovery avoids object hydration; ref discovery prepares ref targets and tag chains. Blobless fetch omits ordinary blobs, and supported filters guide further body selection. Browse APIs use repository metadata and verified object readers for trees, blobs, history and comparisons. See [fetch](../../crates/canopy-server/src/git_gateway/fetch.rs), [hydration](../../crates/canopy-server/src/git_gateway/hydration.rs) and [Git reads](../../crates/canopy-server/src/git_read/mod.rs).

The active [repository schema](../../crates/canopy-server/src/schema.sql) stores per-object kind, size, digest and storage choice. Inline objects are bounded at 768 KiB; larger structural objects use SQL chunks. Large loose blobs use immutable external bodies. The serving code also archives pack/index bodies and can store packed-blob references while retaining per-object SQL authority. This active optimization is distinct from the future immutable catalog design.

LFS transfers use HTTP even when SSH issues the authorization grant. Upload hashes and publishes bounded immutable parts, verifies the complete body and then commits authorized metadata. Download uses the manifest digest pinned in SQLite and verifies requested parts, including tail range responses. Locks are advisory Repository Cell records. See [LFS upload](../../crates/canopy-server/src/lfs/upload.rs) and [LFS read](../../crates/canopy-server/src/lfs/read.rs). Canopy's immutable body store is product code; it is not a registered Cellule Blob capability.

## Owner recovery and operational control

![Node control and recovery](07-owner-recovery.svg)

Startup locks the managed workspace, probes provider behavior, compiles the application, checks release admission and enrolls a signed node lease. Readiness depends on valid framework state and leases. A new Cell bootstraps; an idle Cell restores under acquired authority; a dead owner requires lease expiry and a fenced takeover. Recovery restores the root pinned by Cell authority, verifies required bytes and restores the outcome ledger before activation. Local databases and object listings cannot select a newer-looking root. Native caches rebuild afterward. See [acquisition and startup](../../crates/canopy-server/src/server/mod.rs) and [workspace management](../../crates/canopy-server/src/server/workspace/mod.rs).

Graceful shutdown stops ingress, finishes admitted work, drains Cells and withdraws the node advertisement. Deployment maintenance closes release admission and uses an operation UUID until drain and recovery are proven complete; an explicit end reopens the release. Backup copies pinned roots and referenced external bodies into a disjoint prefix, verifies that independent copy and restores into an unused reserved destination. The current path is a same-provider copy. These controls do not complete schema migration, cross-provider export or collection. See the [operations runbook](../../docs/operations.md).

## Collaboration and final policy checks

![Repository components and merge control](08-domain-and-policy.svg)

The Repository Cell stores ACLs and visibility alongside issues, PRs, reviews, line discussions, commit checks and branch rules. This placement lets final commands check current policy against the same state that they mutate. Directory listings only suggest candidate repositories; the product rechecks actual Repository Cell access before exposing them.

For a merge, preparation captures exact base/head revisions and may use native `merge-tree` or `commit-tree` to construct candidate objects. `MergePull` checks the actor, revisions, current branch state, required checks, reviews and unresolved discussions before changing the branch and PR state. The candidate remains provisional until that command publishes. Check results are API records tied to commits and attempts; they do not imply that Canopy currently runs CI through a Cellule Workflow capability. See [merge command](../../crates/canopy-server/src/pulls/merge/command.rs), [candidates](../../crates/canopy-server/src/pulls/candidates/mod.rs) and [branch rules](../../crates/canopy-server/src/branch_rules/command.rs).

## Packed storage and framework capability evolution

![Active model, new primitives and remaining work](09-packed-storage-evolution.svg)

The newer `packs` subsystem implements immutable native-pack metadata, canonical directory runs, leveled indexes, source roots, catalogs, ref-state roots and exact outcome artifacts. Staging and preparation retain attempts and input custody; trusted certificates bind verified work to the repository, owner fence and base. A bounded publication coordinator dispatches registered commands by class and account. Short final commands publish catalog/ref state and outcomes while rechecking authority and policy. Uploaded artifacts remain preparation until authorized publication selects them.

The [publication module](../../crates/canopy-server/src/packs/publication/mod.rs) explicitly states that its commands are not registered on the legacy serving path. The [implementation status](../../docs/large-repository-implementation-status.md) lists production startup, HTTP, SSH, generated producers/readers and the fresh-schema hard cutover as open work, along with recovery, collection, backup and capacity qualification. The atlas therefore separates implemented primitives from a complete replacement serving system.

A separate [repository capability proposal](../../docs/repository-cell-primitives.md) aims to compose SQL, KV, Queue, Workflow, Blob, Cron, Timer and Effects within one repository identity, fence and recovery boundary. Current `RepositoryModule` declares `CatalogRole::Sql` and has empty workflow/activity inventories. Autonomous per-repository work and composed capabilities remain acceptance goals. Framework support for a primitive does not mean Canopy has wired that primitive into its Repository Cells.

## Regenerate and verify the diagrams

The [generator](generate.py) contains the SVG source layouts, labels and source map. It uses embedded styles and system monospace fonts, with no external rendering dependency in the SVG files.

```sh
python3 diagram/canopy-architecture/generate.py
python3 diagram/canopy-architecture/render_pngs.py
python3 diagram/canopy-architecture/build_gallery.py
```

The PNG export helper requires `rsvg-convert` on `PATH` and renders at twice the SVG viewBox dimensions. The gallery embeds the SVGs directly and works offline. Sequence diagrams draw activation bars behind dashed lifelines, with message arrows on top. The atlas is documentation only; repository runtime behavior is unchanged.
