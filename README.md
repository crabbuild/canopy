# Canopy

Canopy is an independent Git hosting service built on Cellule. The dedicated
`canopy-server` crate owns its product schema, Git gateway and HTTP API. A
Directory Cell maps an owner and repository name to a stable UUID. Each UUID
identifies its own SQLite Repository Cell. Ordinary Git objects, refs and LFS
metadata live in that Cell; large Git blob and LFS bytes use immutable
repository-scoped external objects. Canopy has no Crab product or Xet
dependency.

## Current implementation

This repository is an implementation under construction. The `canopy` binary
starts one leased Cellule node and serves repositories created through its API.
It probes the object store's fencing capabilities, publishes and renews a signed node
advertisement, and restores the repository Cell from object storage when its
local SQLite file is lost. `git-http-backend` supplies Git smart HTTP wire
handling, including protocol v2 negotiation. The SQLite Cell is the durable
authority, and a bare Git repository is only a rebuildable cache. Integration
tests use stock `git` and `git-lfs` clients to push and clone, including a
restart with a fresh local SQLite file.

### Repository browser

Open `/` on the Canopy HTTP listener and connect using your existing access
token. The embedded interface lists authorized repositories, creates repositories
with an owner token, selects branches/tags, browses directories, previews or
downloads small files, and follows first-parent commit history. Merge commits
link to each parent. No separate frontend build or asset service is required.
The token remains in tab memory and clears on disconnect or reload. Use HTTPS
at the deployment ingress. Repository text is displayed literally; HTML,
Markdown, symlinks, submodules and LFS pointers are never executed or followed.

Views pin an immutable object ID after selecting a reference. Use **Refresh
branch** to resolve its latest tip. Directory/history pages contain up to 32
entries; previews/downloads contain at most 256 KiB. Larger files require Git.
The **Issues** tab supports open/closed filters, paged discussions, issue/comment
creation, edits and close/reopen. Write-scoped tokens may participate; authors
and repository writers can edit. Discussion text is displayed literally.
If a submission reply is lost, keep the page open and use **Retry submission**
to recover the same post. Edit conflicts preserve your draft for copying and
require **Reload current version** before another edit. Drafts are not retained
across navigation, disconnect or reload.

The **Pull requests** tab opens, edits, closes and reopens requests between local
branches. It includes draft state, paged reviews, unified text diffs, current
approval requirements and commit check results. Reviews bind the displayed pull
version and both branch tips. Writers can fast-forward or prepare a merge/squash
candidate, inspect its files, fetch it for testing, then explicitly publish it.
Publication rechecks the revision, permissions, reviews and required checks.
Stale or conflicting candidates cannot be published. Check reporting, branch
policy configuration and access management remain API operations. Line discussions can be opened from diff line numbers, replied to and
resolved/reopened. Their original file/line snapshots remain available after
branch changes. Conflict resolution is pending. Merged requests retain
their pre-merge comparison, and **View reviewed changes** opens the exact version
bound to a review, including after branch movement or deletion.
See [browser API contracts](docs/contracts.md#repository-browser) for raw-byte
paths, pagination, limits and authorization behavior.

### Repository API

`POST /api/repositories` with `{"name":"example"}` creates a repository for the
configured owner and returns its UUID and clone URL. `GET /api/repositories`
lists ready repositories that the authenticated account can access. Pass the
returned `next_cursor` as `after` until it is null. Pages inspect at most 32
UUID-ordered candidates and can be short or empty with a non-null cursor,
including when access was revoked or Cell movement capacity runs out.
A page that cannot make progress returns 503 with `Retry-After: 1`.
`GET /api/repositories/<name>` returns the UUID, clone URL, repository role,
default branch and ref generation, plus `viewer.account` and `viewer.token_scope`
for the authenticated request; missing or inaccessible names return 404.
`PATCH /api/repositories/<old_name>` with
`{"name":"new_name","repository_id":"<returned UUID>"}` atomically renames a ready
repository. The UUID is a precondition and remains unchanged; a retry with
the same UUID and new name returns the renamed repository. Create and rename
require the configured owner's token; list and get also admit collaborators.
Git and LFS use
`/<owner>/<repository_name>.git`. Repository creation reserves a UUID in the
Directory Cell, provisions its own Repository Cell, then marks the name ready.
Later requests recover that Cell on demand from the directory.

`GET /api/repositories/<name>/default-branch` returns `repository_id`, the fully
qualified `reference` (initially `refs/heads/main`), and `generation`.
Repository readers can inspect it. An owner with an admin-scoped token can
`PUT` the same URL with
`{"repository_id":"<UUID>","reference":"refs/heads/trunk","expected_generation":7}`.
Use the UUID and generation returned by GET. A concurrent ref or HEAD change
returns 409; read current state before retrying. The target must exist unless
there are no live branches. Stock Git discovery and clone use this durable HEAD,
including after restart. Protocol v2 also reports an unborn target. Branch
deletion preserves HEAD's name; recreating the branch makes it live again.

The configured token bootstraps a durable owner account in the Directory Cell.
`POST /api/accounts` creates another account with a client-generated `cnp_`
token followed by 64 random hexadecimal digits and a `read`, `write`, or
`admin` token scope. Its JSON fields are `name`, `token`, and `scope`.
Only the configured owner can create accounts and change collaborators.
`PUT /api/repositories/<name>/collaborators/<account>` with
`{"role":"read"}` or `{"role":"write"}` grants access to one repository;
`DELETE` on the same URL revokes it. The owner retains access. Git smart HTTP
and LFS require both sufficient token scope and repository role. A ref update
rechecks the writer in the ref transaction; an LFS upload rechecks the writer
when publishing metadata.

`GET /api/repositories/<name>/collaborators` lets an admin-scoped owner inspect
access. It returns `repository_id`, `owner`, `collaborators` (account and role),
and `next_after`. The owner is separate from explicit collaborator grants.
Pages contain up to 32 grants ordered by account name; pass `?after=<next_after>`
until the cursor is null. Each page observes current membership independently;
changes before the cursor require a fresh scan.

Issues and comments live in the same Repository Cell as Git and its ACL:

| Method | Path | Action |
| --- | --- | --- |
| GET / POST | `/api/repositories/<name>/issues` | List summaries / create an issue |
| GET / PUT | `/api/repositories/<name>/issues/<number>` | Read / replace title, body and state |
| GET / POST | `/api/repositories/<name>/issues/<number>/comments` | List / add comments |
| PUT | `/api/repositories/<name>/issues/<number>/comments/<comment>` | Replace comment text |

Reads require repository access and a read-scoped token. Mutations require a
write-scoped token. Repository readers can create issues and comments; authors
and repository writers can edit. Revocation is checked again in the Cell write.

All mutation bodies include `repository_id`, obtained from repository discovery.
Create an issue with `id` (a fresh canonical UUID), `title`, and `body`; create a
comment with `id` and `body`. Both return `{"number":1}` with HTTP 200. Retrying
the original payload returns the same number, including after edits or recovery.
Reusing its UUID with different original content or author returns 409.

Issue PUT supplies `expected_version`, `title`, `body`, and `state` (`open` or
`closed`). Comment PUT supplies `expected_version` and `body`. Success returns
204; stale versions return 409. Read current state after an ambiguous edit reply.
Titles allow 256 UTF-8 bytes, bodies 16 KiB; comments cannot be empty. Request
bodies admit 128 KiB of JSON with a 30-second reception deadline.

Issue lists omit bodies and return up to 32 summaries; comment pages contain up
to 16 full comments. Use numeric `after` / `next_after` cursors until null. Issue
lists optionally accept `state=open` or `state=closed`; the default includes both.
Pages observe current state independently. Bodies are returned as raw text; no
Markdown or HTML rendering, attachments, labels, assignees, or deletion API yet.

Pull requests and reviews are repository-local SQLite records:

| Method | Path | Action |
| --- | --- | --- |
| GET / POST | `/api/repositories/<name>/pulls` | List summaries / open a pull |
| GET / PUT | `/api/repositories/<name>/pulls/<number>` | Read / edit, close or reopen |
| GET / POST | `/api/repositories/<name>/pulls/<number>/reviews` | Read history / submit a review |
| POST | `/api/repositories/<name>/pulls/<number>/comparison` | Read exact-revision changed files, patches or file bytes |
| GET / POST | `/api/repositories/<name>/pulls/<number>/threads` | List / start anchored line discussions |
| GET / PUT | `/api/repositories/<name>/pulls/<number>/threads/<thread>` | Read / resolve or reopen at an expected version |
| GET / POST | `/api/repositories/<name>/pulls/<number>/threads/<thread>/comments` | List / append discussion replies |
| GET | `/api/repositories/<name>/pulls/<number>/review-policy` | Read current review requirements and counts |
| POST | `/api/repositories/<name>/pulls/<number>/merge` | Publish a reviewed fast-forward, merge commit or squash |
| POST | `/api/repositories/<name>/pulls/<number>/merge-candidates` | Prepare a merge commit or squash |
| GET | `/api/repositories/<name>/pulls/<number>/merge-candidates/<id>` | Read the frozen candidate and fetch ref |

To open a pull, POST `repository_id`, a fresh UUID `id`, `title`, `body`, `draft`,
`source_ref`, `source_oid`, `base_ref`, and `base_oid`. Use fully qualified branch
names and current lowercase SHA-1 tips from Git. Both branches must exist in this
repository and point to different commits. Creation returns `{"number":1}`.
Exact UUID/payload retries preserve the original number and later edits.

GET returns editorial `version`, source/base objects with `reference`, current
`oid` and ref `version`, and the original commit identities. A deleted branch has
`oid: null`; deletion retains its ref version. PUT supplies `repository_id`,
`expected_version`, `title`, `body`, `state` (`open` or `closed`), and `draft`.
The author or a repository writer may edit; branch names remain fixed. PUT
returns 204, or 409 for a stale version. Closing a pull never changes Git refs.

Review POST supplies `repository_id`, a fresh UUID `id`, `kind` (`comment`,
`approve`, or `request_changes`), `body`, and this revision copied from GET:

```json
{
  "pull_version": 1,
  "source_oid": "<source SHA-1>",
  "source_version": 1,
  "base_oid": "<base SHA-1>",
  "base_version": 1
}
```

Reviews are immutable; new decisions use new UUIDs. Review creation returns its
`number`. An exact retry preserves that number and ordering. Members may open
pulls and comment using write-scoped tokens. Approval or requested changes require
a repository writer other than the author, on an open, ready pull. All reviews
require the exact current revision; stale, deleted or equal tips return 409.

History marks a decision `applicable` only for the current pull/ref versions and
reviewer's current grant, and only for that reviewer's newest decision. Comments
do not replace decisions. Branch movement, editorial edits, close/reopen and
revoke/regrant invalidate earlier decisions. Repeating an unchanged membership
grant does not. Lists return up to 32 pull summaries or 16 reviews with numeric
`after` / `next_after`; pulls support an optional `state` filter. Text limits match
issues: 256-byte titles, 16 KiB bodies; review comments must be nonblank.

Comparison POSTs require a read-scoped token, current repository membership,
`repository_id`, a tagged `target`, and one of the queries below. Use
`{"kind":"current","revision":<the revision object above>}` for a live view,
`{"kind":"review","number":<review number>}` for a saved review, or
`{"kind":"merged"}` for the published request's pre-merge revision. Historical
selectors resolve immutable records belonging to this pull; callers cannot
substitute arbitrary historical OIDs. `{"kind":"thread","number":<thread number>}`
reads the snapshot retained by a line discussion.

```json
{"kind":"files","after":null}
```

```json
{"kind":"file","path_base64":"UkVBRE1FLm1k","side":"after"}
```

```json
{"kind":"patch","path_base64":"UkVBRE1FLm1k"}
```

Changed files compare the unique merge-base tree to the source tree. Responses
include `comparison` with `merge_base`, `revision`, up to 32 `files`, and
`next_after`. Each file has a byte-preserving `path_base64`, optional UTF-8 `path`,
and nullable `before`/`after` entries containing six-digit octal `mode` and `oid`.
Pass `next_after` as `query.after` with the same target for the next page.
Renames appear as deletion plus addition. Unrelated or ambiguous histories and
moved revisions return 409; traversal limits return 413 without a partial list.

A file query uses `side: "before"` for the merge base or `"after"` for the source.
The `file` response includes the entry, size, and `content_status`: `included`,
`too_large`, or `gitlink`. `content_base64` contains at most 256 KiB of raw blob
bytes when included; otherwise it is null. All base64 uses the URL-safe alphabet
without padding. Symlinks return target text; Gitlinks and LFS pointers are never
followed. Large blobs return metadata without fetching external content.

A patch query returns `patch` with the selected `revision`, `merge_base`, path,
nullable `before`/`after` entries, `status` and `hunks`. Text hunks use three context
lines and Git-compatible `old_start`, `old_lines`, `new_start`, `new_lines`.
Each line has `kind` (`context`, `delete`, `add`), `text` without its final LF,
and `no_newline`. CR bytes are preserved. Non-text states are `binary`,
`too_large` (either side exceeds 256 KiB) and `gitlink`, with empty hunks.
Mode-only changes and empty files may also have no hunks. Text work/output budgets
return 413 without a partial diff; see [limits](docs/contracts.md#unified-text-patches).
The web view displays hunks literally, marks missing final newlines and CRs,
and links both immutable files. It never executes repository content.

Start a line discussion by POSTing to `.../pulls/<number>/threads`:

```json
{
  "repository_id": "<repository UUID>",
  "id": "<new discussion UUID>",
  "target": {"kind":"review","number":1},
  "path_base64": "UkVBRE1FLm1k",
  "side": "after",
  "line": 12,
  "body": "Could we explain this change?"
}
```

Creation accepts current, review or merged targets. The side/line must occur in a
text hunk returned by the patch query, including context lines. The server verifies
the blob and coordinates; clients cannot supply an anchor OID. A thread keeps its
original revision, merge base, byte path, side, line and blob identity.
Current members with write-scoped tokens may start/reply; replies use a new UUID
and `body`. Exact retries return the original number. Discussion text and replies
are immutable; corrections can be added as replies. Lists page at 16 records with
numeric `after`/`next_after`.

Resolve/reopen with `repository_id`, `expected_version` and `resolved: true|false`.
The thread author, pull author or repository writer may do so with a write-scoped
token. Stale versions return 409. Resolution is informational and does not replace
required approvals or checks. See [line discussion contracts](docs/contracts.md#line-discussions).
This unreleased schema adds discussion tables and requires a fresh development
storage prefix; there is no upgrade migration yet.

Merge POSTs require a write-scoped token and current repository write access:

```json
{
  "repository_id": "<repository UUID>",
  "id": "<new merge request UUID>",
  "revision": {
    "pull_version": 1,
    "source_oid": "<source SHA-1>",
    "source_version": 1,
    "base_oid": "<base SHA-1>",
    "base_version": 1
  },
  "strategy": "fast_forward"
}
```

For `fast_forward`, the source must descend from the current base. Current reviews, required checks
and exact ref versions are checked in the transaction that advances the base and
marks the pull `merged`. The response contains `merge` with `id`, `number`, `oid`
and `merged_at_ms`, plus the exact pre-merge `revision`; pull details retain that
record. Exact retries with the same
UUID and payload return the original result, including after a lost reply or
later branch movement. Changed payloads or actors conflict. Retry an uncertain
result with the original request ID and revision. Merged pulls cannot be reopened
or edited; `state=merged` is available on the list endpoint.

The review-policy response reports its observed `revision`, `ready`, rule version,
required approvals, eligible approval count, outstanding requested changes and
`reviews_satisfied`. It does not claim checks have passed or history can merge.
For `merge_commit` or `squash`, first POST `/merge-candidates` with the same
`repository_id` and `revision`, a new UUID `id`, the selected `strategy`, and a
nonblank `message` (up to 16 KiB UTF-8). It returns `repository_id`, `candidate` and `fetch_ref`.
The candidate result is `ready` with `oid` and `tree_oid`, `conflicted` with
URL-safe unpadded `paths_base64`, or `unrelated`. GET may also show `pending`
after interrupted preparation. Exact preparation retries return the same result.
Only a ready candidate advertises a fetch ref:

```sh
git fetch origin refs/canopy/merge-candidates/<candidate-UUID>
git checkout --detach FETCH_HEAD
```

Run checks on that candidate OID. Then POST `/merge` with a new merge UUID,
original `revision`, the same `strategy`, and `candidate_id`. A merge commit
has ordered base/source parents; a squash has only the base parent. Publication
rechecks current reviews and required checks against the candidate commit.
Source or base movement invalidates publication, including movement away and
back. Conflicted candidates cannot publish. The `refs/canopy` namespace is
server-owned and rejects every push update, including owner pushes.

Native Git handles three-way content merges, renames and multiple merge bases.
Merge drivers and signing commands from host Git configuration are disabled.
Rebase, conflict resolution, forks and retargeting remain
to be delivered.

Commit checks record results from a configured reporter; they do not execute CI
jobs. Exact-branch rules can require successful results.

| Method | Path | Action |
| --- | --- | --- |
| GET | `/api/repositories/<name>/check-contexts` | List contexts, including disabled ones |
| PUT | `/api/repositories/<name>/check-contexts/<context>` | Owner configures reporter and enablement |
| GET / POST | `/api/repositories/<name>/commits/<oid>/checks` | Read latest attempts / start an attempt |
| GET / PUT | `/api/repositories/<name>/checks/<id>` | Read an attempt / report progress or result |

Context PUT requires an admin-scoped owner token and `repository_id`,
`expected_version` (zero for creation), `reporter` (a repository member), and
`enabled`. Context names use lowercase components up to 64 bytes. Each policy
change advances its version and invalidates earlier-version results.

The reporter uses a write-scoped token to POST `repository_id`, a fresh UUID `id`,
`context`, and `context_version`. The Git commit must already be stored. The
attempt starts `queued`; POST returns its `id`. An exact retry preserves that
attempt. PUT supplies `repository_id`, `expected_version`, `state`, and `summary`
(up to 4 KiB). States advance to `in_progress`, `success`, `failure`, or `cancelled`.
Terminal results are immutable; reruns need a new UUID. PUT returns 204; stale
versions or changed context policy return 409.

Repository readers can inspect checks. Commit views select the newest-created
attempt for each enabled context at its current version; a late result or retry
from an older attempt cannot replace it. A missing current attempt appears as
`run: null`. Context/commit pages contain at most 32 entries with name-based
`after` / `next_after` cursors. Historical attempts remain readable by UUID.

Branch protection uses `GET /api/repositories/<name>/branch-rules` and an
admin-scoped owner `PUT` to that URL:

```json
{
  "repository_id": "<repository UUID>",
  "rule": {
    "reference": "refs/heads/main",
    "expected_version": 0,
    "enabled": true,
    "deny_deletions": true,
    "fast_forward_only": true,
    "required_checks": ["unit-tests"],
    "require_pull_request": true,
    "required_approvals": 1
  }
}
```

Rules name exact branch refs and apply to every writer, including the owner.
Configure required check contexts first. The newest attempt for each required
context at its current version must be `success`; missing, pending, failed, or
stale results reject the update. Push a candidate to an unprotected branch, run
and report its checks, then merge its reviewed pull into the protected branch.
Previously accepted results remain trusted after reporter access is revoked;
disable or update the context to invalidate them.

PUT returns 204; stale rule versions or unavailable required contexts return 409.
Use `expected_version: 0` for creation and the returned version for later changes.
Disable with `enabled: false`; the name/version remains reserved. GET is available
to repository readers and returns `repository_id`, `rules`, and `next_after` with
up to 32 ref-ordered rules per page, including disabled ones. Each rule lists its
version and complete policy. Up to 16 unique contexts are permitted per rule.

Every rule replacement supplies `require_pull_request` and `required_approvals`.
An enabled required-PR rule rejects direct pushes, deletion and recreation,
including owner pushes; a merge must have enough eligible approvals and no
applicable requested changes. Counts range from 0 through 16. Zero still requires
a pull when `require_pull_request` is true. Set both false/zero to permit direct
pushes under the other branch checks. Establish the base branch before enabling
a required-PR rule. Current reviewers must still have the same write grant;
old approval retries, new comments and revoke/regrant cannot restore eligibility.

Ordinary pushes retain allowed sibling refs when another ref is rejected;
`git push --atomic` rejects the group. The final Cell transaction rechecks policy,
so a concurrent rule/check change can return HTTP 409 for the accepted group
before any success report is sent. Ref names are at most 255 ASCII bytes. When
rules are enabled, command preflight accepts at most 64 updates within a 256 KiB
prefix. The Git response is recorded only after durable ref publication; an exact
completed retry returns that saved response without reapplying refs.

Accounts can hold multiple scoped tokens. An admin-scoped token can manage its
own account's tokens; the configured owner can manage any account's tokens:

| Method | Path | Result |
| --- | --- | --- |
| GET | `/api/accounts/<account>/tokens?after=<UUID>` | Up to 32 metadata records and `next_after`; omit `after` for the first page |
| POST | `/api/accounts/<account>/tokens` | Issue with `{"id":"<UUID>","token":"cnp_<64 random hex digits>","scope":"read"}`; returns 204 |
| DELETE | `/api/accounts/<account>/tokens/<UUID>` | Revoke one token; returns 204 |

Choose a new canonical UUID and random secret for each issuance. An exact retry
with the same active ID, secret and scope succeeds; conflicting or revoked
identities return 409. Listing returns only ID, scope, enabled state and creation
time. Revoked IDs and secrets stay reserved. Revoking the site's last admin token
returns 409, including under concurrent requests.

For rotation, issue a replacement, verify it, update clients, then revoke the old
token. For the site owner, also update `CANOPY_GIT_TOKEN` in the deployment before
retiring its configured credential: startup requires an active owner admin token.
Revocation blocks subsequent API, Git and LFS authentication. Already admitted
Git/LFS operations may finish; token issuance and account creation recheck the
authorizing credential in their mutation transaction.

The current service supports one repository owner.
Incoming Git requests stream to temporary files charged to the same disk budget
as the node's SQLite files. Push requests admit up to 512 MiB; fetch requests
up to 64 MiB. Push replies remain buffered and capped at 64 MiB. Clone and fetch
responses stream with backpressure and have no 64 MiB response ceiling. LFS
transfers and individual external Git blobs remain capped at 64 MiB.
Trees, commits and tags above 768 KiB use 512 KiB SQLite chunks, up to 64 MiB
per object. Publication verifies every chunk and the complete object identity;
partial uploads stay invisible to Git.
Each node admits eight Git/LFS transfers across all repositories. Overload
returns 503 with `Retry-After: 1`; retry after capacity is available. Health,
readiness and management routes remain outside this transfer limit. LFS body
reception has a 120-second deadline (408 on timeout). Eight is an initial
operational bound, not a measured production capacity target.
Ref advertisements use generation-checked pagination; sustained concurrent
changes return a retryable 503. Gzip-compressed Git requests are supported.
Gzip is fully validated before Git runs; decoded bytes have the same request
size limits and share disk admission with the encoded upload.
Disposable Git caches retain shared disk reservations. Hydration admits bytes
before writing; native Git writes are measured before durable ref publication.
Exhaustion returns 507. Native Git's peak scratch usage is not yet hard bounded.
Local recovery currently admits a 512 MiB SQLite database. The node keeps the
Directory Cell and up to three Repository Cells resident. Additional repositories
evict an inactive repository and restore from durable state when accessed again.
Requests and streamed responses pin their repository; admission returns 503 when
no repository can be safely released. A terminal ownership-release failure leaves
that repository unavailable until node restart; confirmed-release cleanup errors
are retried on later admission. There is no
account disable/delete API, organization model, multi-node routing, backup
or production capacity evidence.
`Cargo.toml` pins Cellule to a specific Git revision, so a fresh Canopy checkout
builds without a local Cellule checkout.

Before ref publication, bounded certificate batches verify the durable Git
graph: commit trees and parents, tree entries and tag targets must exist with
the correct object type. Each batch covers at most 128 objects and 64 MiB of
SQLite object bytes. Ref publication checks certified tips atomically with
permissions and ref versions; branch tips must be commits. Submodule gitlinks may name commits in another repository.
SQLite certificates let later pushes reuse validated history. Push ingestion streams
candidates from accepted ref tips, excludes previously published history, and
reads missing objects through one persistent Git batch process. Object sizes
are checked before allocation and canonical OIDs before storage. SQLite lookups
group up to 128 candidate IDs; object publication groups up to 128 records and
768 KiB of inline bytes in one Cell transaction, with at most 64 MiB of SQLite
object bytes verified per batch. A conflicting record rejects
the whole batch. Recovery tests include annotated tags, submodules and
`git fsck` on the restored clone.

Cold cache hydration reads OID-ordered pages of at most 128 records and 768 KiB
of inline bodies. It verifies inline identities on a blocking worker; chunked
and external bodies retain their own verification before cache writes. Pages
reduce SQLite query overhead, but recovery still rebuilds the complete cache.

## Recover a lost push reply

For a receive-pack POST, a client or proxy can supply `Idempotency-Key` as one
canonical lowercase, hyphenated UUID. Use one ID per logical operation. Canopy
binds it to the repository, authenticated account and request digest. Repeating
the same request returns the recorded status, headers and per-ref report without
applying the refs again, including after owner takeover. Reusing an ID with
different request bytes or another account returns HTTP 409. Current token
scope and repository access are checked on every replay.

Replay requires the same body, content type and Git protocol setting. A new
`git push` invocation may generate different pack bytes; reusing its header does
not guarantee replay. An HTTP client or proxy must retain the original request.
Recorded replies include `X-Canopy-Push-Id`. If no ID was supplied, the server
generates one; a client that loses that reply cannot discover the generated ID.
Advertisements and fetches ignore this header.

The Repository Cell stages the reply in SQLite chunks, then publishes its
pointer and accepted ref updates in one transaction. Git rejection and no-op
reports are recorded too. Failures before publication remain pending and can be
retried. Response bodies are limited to 64 MiB and serialized response headers
to 64 KiB. Completed records and abandoned staging chunks currently have no
expiry or collector and consume the repository database allowance.

## Run the current service

The server requires Git with `http-backend`, modern `merge-tree --write-tree`
(`-z --name-only --no-messages`) and `commit-tree` on `PATH`. This slice was
qualified with Git 2.50.1; an unsupported native command fails preparation.

Copy [config.example.json](config.example.json) and set the object storage URL,
tenant and application IDs, owner name, network addresses and data
directory. The object store must support
conditional create/update and ranged reads; startup probes these operations.
Configure credentials through the provider's environment variables. Set
`CANOPY_GIT_TOKEN` and `CANOPY_NODE_SIGNING_KEY_HEX` (a 32-byte key encoded as
64 hex characters) in the process environment. Then run:

```sh
CARGO_TARGET_DIR=$HOME/Workspace/crabbuild-target/canopy-local cargo run --release --locked --bin canopy -- config.json
```

`GET /healthz` reports process liveness and `GET /readyz` reports Cell
readiness. Git and LFS requests require `Authorization: Bearer <token>` or
HTTP Basic credentials using the matching account name and token. Stop with
SIGINT or SIGTERM to drain requests and withdraw the node advertisement.

## Verify the current slice

Use a checkout-specific target directory on the mounted Workspace volume:

```sh
CARGO_TARGET_DIR=$HOME/Workspace/crabbuild-target/canopy-local cargo test --locked
CARGO_TARGET_DIR=$HOME/Workspace/crabbuild-target/canopy-local cargo clippy --all-targets --locked -- -D warnings
cargo fmt --all -- --check
```

The integration tests need `git` and `git-lfs` on `PATH`. See
[delivery plan](docs/delivery-plan.md) for the remaining release gates and
[contracts](docs/contracts.md) for persisted identities and storage rules.

For a black-box process smoke, build the optimized `canopy` binary, provide a
test S3-compatible bucket and credentials through the provider's environment
variables, and run:

```sh
CARGO_TARGET_DIR=$HOME/Workspace/crabbuild-target/canopy-local cargo build --release --locked --bin canopy
python3 scripts/smoke_s3_process.py \
  --binary "$HOME/Workspace/crabbuild-target/canopy-local/release/canopy" \
  --storage-url s3://your-test-bucket \
  --work-parent "$HOME/Workspace/crabbuild-target/canopy-local"
```

The script requires `CANOPY_NODE_SIGNING_KEY_HEX` and provider credentials in
the environment. It pushes two repositories with stock Git and LFS, grants a
collaborator access, renames one repository, restarts with fresh local databases,
kills the new owner, waits for lease expiry, and clones from a third process.
It verifies collaborator access and the owner roster after takeover, then denial
and roster removal after revocation. Edited issues/comments and original-create
retries are checked before shutdown, after restart and after forced takeover.
Check policies/results retain the newest attempt even after an old start retry.
It rotates a collaborator token before restart, then checks the retired token
remains denied for API, Git and LFS after restart and owner takeover.
It also verifies that a deleted branch stays absent through takeover and can
then be recreated through stock Git.
Mixed push checks prove accepted refs survive recovery, rejected refs stay
absent, and `git push --atomic` rejects the entire mixed update.
A proxy drops a successful push reply; replay after takeover returns the original
report without undoing a later branch deletion.
It writes under a unique prefix in the supplied bucket.

Add `--large-clone` to send two 40 MiB random blobs in a single push, then clone the
repository using protocol v0 and v2 after takeover. Each clone must receive a
pack larger than 64 MiB, reproduce both file hashes and pass `git fsck`. This is
a transfer-size qualification; it does not establish production capacity.

Add `--many-objects 256` to qualify a 256-file initial push, a one-file update
with an annotated tag, and a verified clone after takeover. Combine it with
`--large-clone` to exercise four repositories through resident eviction and
verify Git/LFS recovery on the same node before restart. For local container
stores, place data and logs on the mounted workspace and verify free
inodes as well as bytes before qualification. A full container filesystem can
turn storage publications into unresolved mutations even with free byte space.


Add `--sqlite-chunks` to push a 32,000-entry tree and commit/tag messages above
1 MiB, then verify exact raw bytes and OIDs after takeover with a strict fsck.
This exercises SQLite chunk storage independently of external large blobs.
Add `--corpus-repository /path/to/existing/repository` to qualify that checkout's
HEAD history. The script only reads the source, creates a bundle and temporary
fixtures under `--work-parent`, then verifies every reachable object's type,
size and bytes in protocol v0/v2 clones after takeover, plus strict `git fsck`.
Other source branches and tags are outside this qualification.

To measure cold recovery, set
`RUST_LOG=warn,canopy_server::git_gateway=debug,canopy_server::server::residency=debug`.
The node reports Cell acquisition time and cache hydration time, with object
count, raw/cache bytes and time spent in page reads, body retrieval and cache
writes. Page time includes inline integrity verification; cache time includes
worker scheduling, OID verification, compression and admitted disk writes.
Use release builds for performance measurements.

The chunk, default-branch, repository-discovery, token-metadata, issue and check layouts
change the unreleased schema; use a fresh development storage prefix when moving
from older builds.
