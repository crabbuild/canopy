# Canopy API reference

This reference preserves the complete HTTP and SSH-facing behavior documented in the project README. It covers repository lifecycle, collaboration, merge, checks, credentials, and SSH transport.

> **Document type:** Reference. **Goal:** automate Canopy operations with the correct request identity, authorization scope, preconditions, and retry behavior.

## Endpoint reference

The routes below are a behavior reference. Replace `<name>`, `<account>`, UUIDs,
object IDs and generations with values returned by your deployment. JSON
examples containing angle-bracketed strings illustrate the required shape;
those strings are not usable IDs. Read [persisted contracts](contracts.md)
for exact limits and failure semantics.

### API map

| Surface | Reference section |
| --- | --- |
| Repository lifecycle and default branch | [Repository creation, discovery, and rename](#repository-creation-discovery-and-rename), [Default branch](#default-branch) |
| Accounts, grants, and tokens | [Accounts and repository access](#accounts-and-repository-access), [Token lifecycle](#token-lifecycle) |
| Issues, pull requests, reviews, and line discussions | [Issues and comments](#issues-and-comments), [Pull requests and reviews](#pull-requests-and-reviews), [Compare revisions and discuss lines](#compare-revisions-and-discuss-lines) |
| Merge, checks, and branch rules | [Merge and rebase](#merge-and-rebase), [Checks and branch rules](#checks-and-branch-rules) |
| SSH transport and keys | [SSH keys and transport](#ssh-keys-and-transport) |

The [persisted contracts](contracts.md) remain the canonical reference for byte limits, authorization timing, replay semantics, and recovery guarantees.

### Repository creation, discovery, and rename

`POST /api/repositories` with `{"name":"example"}` creates a repository for the
configured owner and returns its UUID and clone URL. `GET /api/repositories`
lists ready repositories that the viewer can access, including anonymous public
reads. Pass the returned `next_cursor` as `after` until it is null. Pages inspect at most 32
UUID-ordered candidates and can be short or empty with a non-null cursor,
including when access was revoked or Cell movement capacity runs out.
A page that cannot make progress returns 503 with `Retry-After: 1`.
`GET /api/repositories/<name>` returns the UUID, clone URL, repository role,
default branch, ref generation and visibility, plus `viewer.account` and
`viewer.token_scope` for an authenticated request (`viewer: null` anonymously).
Missing or inaccessible names return 404.
`PATCH /api/repositories/<old_name>` with
`{"name":"new_name","repository_id":"<returned UUID>"}` atomically renames a ready
repository. The UUID is a precondition and remains unchanged; a retry with
the same UUID and new name returns the renamed repository. Create and rename
require the configured owner's token; list and get also admit collaborators.
Git and LFS use
`/<owner>/<repository_name>.git`. Repository creation reserves a UUID in the
Directory Cell, provisions its own Repository Cell, then marks the name ready.
Later requests recover that Cell on demand from the directory.

### Default branch

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

### Accounts and repository access

The configured token bootstraps a durable owner account in the Directory Cell.
`POST /api/accounts` creates another account with a client-generated `cnp_`
token followed by 64 random hexadecimal digits and a `read`, `write`, or
`admin` token scope. Its JSON fields are `name`, `token`, and `scope`.
Only the configured owner can create accounts and change collaborators.
`POST /api/accounts/<account>/disable` with an admin-scoped owner credential
disables an account and returns 204, including on repeat requests. The site owner
cannot be disabled (409). All of the account's credentials then fail new API,
Git and LFS authentication. Its name, repository grants and authored data remain
reserved; account creation cannot reactivate it. There is no re-enable or account
deletion endpoint yet. Requests authenticated before disablement may finish.
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

### Issues and comments

Issues and comments live in the same Repository Cell as Git and its ACL:

| Method | Path | Action |
| --- | --- | --- |
| GET / POST | `/api/repositories/<name>/issues` | List summaries / create an issue |
| GET / PUT | `/api/repositories/<name>/issues/<number>` | Read / replace title, body and state |
| GET / POST | `/api/repositories/<name>/issues/<number>/comments` | List / add comments |
| PUT | `/api/repositories/<name>/issues/<number>/comments/<comment>` | Replace comment text |

Reads require repository read access, including anonymous public access. Mutations
require a write-scoped token. Repository readers can create issues and comments; authors
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

### Pull requests and reviews

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
| POST | `/api/repositories/<name>/pulls/<number>/merge` | Publish a reviewed fast-forward, merge commit, squash or rebase |
| POST | `/api/repositories/<name>/pulls/<number>/merge-candidates` | Prepare a merge commit, squash or rebase |
| GET | `/api/repositories/<name>/pulls/<number>/merge-candidates/<id>` | Read the frozen candidate and fetch ref |

To open a pull, POST `repository_id`, a fresh UUID `id`, `title`, `body`, `draft`,
`source_ref`, `source_oid`, `base_ref`, and `base_oid`. Use fully qualified branch
names and current lowercase object IDs from Git in the repository's format.
Both branches must exist in this repository and point to different commits.
Creation returns `{"number":1}`.
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
  "source_oid": "<source object ID>",
  "source_version": 1,
  "base_oid": "<base object ID>",
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

### Compare revisions and discuss lines

Comparison POSTs are read operations and require current repository read access,
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
return 413 without a partial diff; see [limits](contracts.md#unified-text-patches).
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
Current readers with write-scoped tokens may start/reply; replies use a new UUID
and `body`. Exact retries return the original number. Discussion text and replies
are immutable; corrections can be added as replies. Lists page at 16 records with
numeric `after`/`next_after`.

Resolve/reopen with `repository_id`, `expected_version` and `resolved: true|false`.
The thread author, pull author or repository writer may do so with a write-scoped
token. Stale versions return 409. Resolution is informational and does not replace
required approvals or checks. See [line discussion contracts](contracts.md#line-discussions).
This unreleased schema adds discussion tables and requires a fresh development
storage prefix; there is no upgrade migration yet.

### Merge and rebase

Merge POSTs require a write-scoped token and current repository write access:

```json
{
  "repository_id": "<repository UUID>",
  "id": "<new merge request UUID>",
  "revision": {
    "pull_version": 1,
    "source_oid": "<source object ID>",
    "source_version": 1,
    "base_oid": "<base object ID>",
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
Only a ready candidate advertises a fetch ref. Set `fetch_ref` to the
`fetch_ref` value returned by preparation, then inspect that exact candidate:

```bash
git fetch origin "$fetch_ref"
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
For `rebase`, prepare with `message: ""`: original authors, author dates,
message bytes, encodings and extra headers are preserved. Each source-only
commit is replayed onto the base in order; empty commits are retained. The
committer becomes the preparing account at the reserved time; original commit
and ancestry signatures are removed. The source branch is unchanged.

Rebase preparation accepts a linear history of at most 128 commits, each at most
64 KiB before and after rewriting. It returns `rebase_unavailable` with reason
`merge_history`, `no_commits`, `limit` or `commit_format` when it cannot prepare;
these results cannot publish. Resolve conflicts or rewrite unsupported history
locally and push, then prepare a new candidate. A conflict at any intermediate
commit stops replay even when the final source tree would merge cleanly.
Conflict resolution in the browser, forks and retargeting remain to be delivered.

### Checks and branch rules

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
so a concurrent rule/check change rejects the accepted group through Git
report-status before any success report is sent. Ref names must be UTF-8;
native Git and the host filesystem still impose path constraints. Push
preflight accepts at most 100,000 updates within 40 MiB of packet-line commands.
Plans are staged in bounded SQLite chunks and published in one transaction;
ref format and command limits receive native Git rejection reports.
The Cell records ref publication or rejection atomically with the response
selection; an exact completed retry returns the same report without reapplying
refs. Clients that decline report-status receive HTTP 409 on a late rejection.

Signed pushes use `git push --signed=true` with `gpg.format=ssh` and a
`user.signingkey` whose public key is registered with write scope on the
authenticated Canopy account. Git verifies the signature and nonce; the Repository Cell
records certificate bytes and prevents replay under a different push ID.

### Token lifecycle

Accounts can hold multiple scoped tokens. An admin-scoped token can manage its
own account's tokens; the configured owner can manage any account's tokens:

| Method | Path | Result |
| --- | --- | --- |
| GET | `/api/accounts/<account>/tokens?after=<UUID>` | Up to 32 metadata records and `next_after`; omit `after` for the first page |
| POST | `/api/accounts/<account>/tokens` | Issue with `{"id":"<UUID>","token":"cnp_<64 random hex digits>","scope":"read"}`; returns 204 |
| DELETE | `/api/accounts/<account>/tokens/<UUID>` | Revoke one token; returns 204 |

Choose a new canonical UUID and random secret for each issuance. An exact retry
with the same active ID, secret, scope and expiry succeeds; conflicting, expired
or revoked identities return 409. Optional `expires_at_ms` is an absolute Unix
millisecond timestamp strictly in the future; omitted/null means no expiry.
Listing includes that timestamp (or null), ID, scope, enabled state and creation
time. `enabled` records revocation state: an enabled token can still be expired.
Expired/revoked IDs and secrets stay reserved. Revoking the site's last
non-expiring admin token returns 409, including under concurrent requests.
Expiring admins do not satisfy that recovery guard.

Each account admits at most 64 active tokens and 256 new tokens per
rolling 24 hours, including its initial credential. HTTP 429 distinguishes an
active-capacity limit from the issuance window. Revoke a credential or wait for
its expiry to free active capacity; revocation and expiry do not erase issuance
history. Exact active-record retries do not consume another slot. Limits apply
to the target account, including issuance by the site owner, and survive recovery.

For rotation, issue a replacement, verify it, update clients, then revoke the old
token. For the site owner, also update `CANOPY_GIT_TOKEN` in the deployment before
retiring its configured credential: startup requires an active owner admin token.
Revocation and expiry block subsequent API, Git and LFS authentication.
Already admitted Git/LFS operations may finish; token issuance, account creation and disablement
recheck the authorizing credential and expiry in their mutation transaction.
Expiry uses the Directory owner's clock at execution; keep fleet clocks
synchronized.
Bootstrap and initial account credentials are non-expiring.

### SSH keys and transport

SSH public keys have a separate durable registry, managed by the same account/site
admin tokens. The optional SSH listener supports stock Git clone, push and fetch.
Key management currently uses the API:

| Method | Path | Result |
| --- | --- | --- |
| GET | `/api/accounts/<account>/ssh-keys?after=<UUID>` | Up to 32 key records and `next_after` |
| POST | `/api/accounts/<account>/ssh-keys` | Register with `{"id":"<UUID>","public_key":"ssh-ed25519 AAAA...","scope":"write"}`; returns 204 |
| DELETE | `/api/accounts/<account>/ssh-keys/<UUID>` | Revoke one key; returns 204 |

Keys may have `read` or `write` scope. Accepted key formats are plain OpenSSH
Ed25519, ECDSA and RSA (2048–8192 bits); authorized_keys options, certificates,
DSA and security-key formats are rejected. Comments are discarded. Listing
returns the canonical public key, OpenSSH SHA-256 fingerprint, ID, scope,
creation time and enabled state. No private key is accepted or stored.
An exact active registration retry returns 204. Conflicting IDs, ownership,
scopes or revoked key material return 409, including when the comment changes.
Revoked keys remain reserved; register a fresh key to rotate. Disabled accounts
cannot resolve to an SSH identity. Each account has separate limits of 64 active
SSH keys and 256 registrations per rolling 24 hours (429 on either limit).
Registration/revocation recheck the exact admin token in their Directory
transaction and append an account-history event atomically. Events use
`ssh_key_id` for the affected key, with `token_id` null.

Enable SSH by adding a listener and a stable OpenSSH private host key to the
server configuration:

```json
{
  "ssh": {
    "listen": "0.0.0.0:2222",
    "host_key": "/run/secrets/canopy_ssh_host_ed25519_key"
  }
}
```

Generate the host key with `ssh-keygen -t ed25519 -N '' -f /path/to/host_key`
and preserve it across restarts. The configured key must be decrypted and
readable by the server.
Publish its fingerprint to clients through a trusted channel. Git URLs use the
SSH user `git`, for example `ssh://git@example.com:2222/canopy/project.git`.
The registered client key identifies the account; repository permissions and key
scope both apply. New commands on existing connections recheck revocation and
account status. Shell, SFTP, forwarding and arbitrary environment requests are
denied. HTTP and SSH share the same node/account transfer limits and durable
push publication path.

SSH prepares advertised ref/tag targets before negotiation and hydrates non-blob
history reachable from each request's wants before forwarding them to Git.
Unfiltered requests hydrate reachable blobs in the same certified Cell graph
walk. Exact `blob:none` requests hydrate only explicit missing blobs. Other
requests apply the native filter while selecting missing reachable blobs, before
forwarding wants to native Git. Tree and object-type constraints skip omitted
bodies; size filters still load missing bodies because native Git needs their sizes.
Git LFS can use the same SSH key: `git-lfs-authenticate` returns the configured
HTTP LFS endpoint and a five-minute credential for that repository and operation.
No separate HTTP credential helper or `lfs.url` is required. File bytes still use
HTTP basic transfers to Canopy's object-store-backed LFS service. Each request
checks grant expiry, parent SSH key/account status and current repository access.
Grants cannot authorize Git or management API requests. Pure SSH LFS transfers
remain unsupported.
The local compatibility suite covers stock Git transfers and fresh-disk recovery;
late policy/access refusal and disconnected-push drain are also covered locally.
Provider failure, owner-loss and capacity qualification remain open.
