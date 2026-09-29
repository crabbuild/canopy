# Use Canopy

This guide covers the embedded browser, public repositories, Git LFS, Git LFS locks, and account administration. Use the [API reference](api-reference.md) for automation and the [persisted contracts](contracts.md) for exact limits and failure semantics.

> **Document type:** How-to and product reference. **Goal:** use Canopy's browser and collaboration features while keeping authorization, limits, and recovery behavior visible.

## Browse and collaborate

The embedded browser covers common tasks; the API supports automation and
operations that have no browser control yet. Git clients use
`/<owner>/<repository>.git` over HTTP, or the optional SSH listener. Private
Git and LFS requests use an active account token plus a repository grant.

### Common paths

| Task | Where to begin |
| --- | --- |
| Browse code, issues and pull requests | Open `/` on the Canopy listener; connect with an existing token for private repositories. |
| Change visibility or collaborators | Use the repository browser for visibility; use the [access API](api-reference.md#accounts-and-repository-access) for grants. |
| Manage account tokens | Open **Account**; use the [token API](api-reference.md#token-lifecycle) for automation. |
| Add Git LFS | Install `git-lfs`, run `git lfs install` in your client, then track and push files through the normal Git remote. |
| Protect a branch | Configure a check reporter and [branch rules](api-reference.md#checks-and-branch-rules), then use a reviewed pull request. |
| Diagnose a refused request | Inspect `401` credentials, `403` scope/grant, `409` version or policy conflict, `503` admission/retry, or `507` local disk budget in the relevant section below. |

### Repository browser

Open `/` on the Canopy HTTP listener to browse public repositories or connect
using your existing access token. The embedded interface lists authorized
repositories and creates repositories with an owner token. It selects branches
and tags, browses directories, previews or downloads small files, and follows
first-parent commit history. Merge commits
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
version and both branch tips. Writers can fast-forward or prepare a merge/squash/rebase
candidate, inspect its files, fetch it for testing, then explicitly publish it.
Publication rechecks the revision, permissions, reviews and required checks.
Stale or conflicting candidates cannot be published. Check reporting, branch
policy configuration and access management remain API operations. Line
discussions can be opened from diff line numbers, replied to, and resolved or
reopened. Their original file/line snapshots remain available after
branch changes. Conflict resolution is pending. Merged requests retain
their pre-merge comparison, and **View reviewed changes** opens the exact version
bound to a review, including after branch movement or deletion.
See [browser API contracts](contracts.md#repository-browser) for raw-byte
paths, pagination, limits and authorization behavior.

### Public repositories

Repositories start private. An owner with an admin-scoped token can use **Change
visibility** in the repository browser, or `GET` then
`PUT /api/repositories/<name>/visibility`. The PUT body contains `repository_id`,
`expected_generation` from the GET, and `visibility` (`private` or `public`).
A stale generation returns 409; after an uncertain response, read current state
before retrying. Visibility shares the repository ref generation, so concurrent
pushes or default-branch edits can require a refresh too.

Public repositories allow anonymous discovery, stock Git clone/fetch, LFS
batch/download, code browsing, issues, pull requests, reviews and check reads.
Authenticated readers with write-scoped tokens can participate in discussions;
Git/LFS writes, approvals and merges still require explicit repository write
access. Anonymous mutations are denied. Supplied invalid credentials return 401,
even on public repositories. Collaborator rosters remain owner-only.

Making a repository private blocks newly authorized anonymous reads. Requests
already admitted can finish, and downloaded copies cannot be recalled. Successful
data responses and Git/LFS responses use `Cache-Control: no-store`. Public
discovery candidates are retained and rechecked against the Repository Cell on
every listing; a stale index entry
never grants access.

### Git LFS storage

Canopy provides the Git LFS batch/basic HTTP API itself. Stock `git-lfs` clients
upload and download through Canopy; bytes live in the configured object store at
`repos/<repository-uuid>/lfs/<sha256>`, while the repository's SQLite Cell stores
their size and verified hashes. No separate LFS server is needed. Uploads verify
SHA-256 and store immutable bytes before publishing the SQLite reference.

Transfers pass through Canopy with 8 MiB immutable parts and no fixed file-size
quota. Uploads hash bytes incrementally in temporary multipart storage, then
conditionally copy each verified part and publish a small immutable manifest.
The logical key holds the manifest; `<key>.parts/<hex-index>` holds its bytes.
Downloads verify both hashes before delivering their final range. The node's
eight-transfer admission covers these operations, cleanup and outstanding output.
Presigned direct-to-storage transfers are not implemented. Git LFS also supports
[client configuration for a separate LFS server](https://github.com/git-lfs/git-lfs/blob/main/docs/api/server-discovery.md#custom-configuration)
using `lfs.url`; that server manages its own access and backups. Canopy does not
proxy external LFS servers or include their bodies in its backup.

### Git LFS locks

`git lfs lock`, `git lfs locks` and `git lfs unlock` use repository-scoped
SQLite locks. The standard `/info/lfs/locks/verify` endpoint lets stock Git LFS
pre-push checks reject changes to another account's locked paths. Locks apply
across branches. Listing requires read access; creation, verification and
unlocking require write access. A writer can explicitly use `--force` to remove
another account's lock. Locks survive node restart and fresh-disk recovery.

Locking is advisory: a client can bypass Git LFS verification. It is not a
server-side branch protection rule. HTTP credentials and SSH-issued LFS
grants both support these lock operations. Lock pages contain
at most 100 entries, with continuation cursors; paths are canonical relative
UTF-8 strings up to 4096 bytes. This adds an unreleased SQLite table and requires
a fresh development storage prefix when switching from older builds.

### Account administration

After connecting, choose **Account**. Admin-scoped credentials can list, issue
and revoke their own account's tokens. Site-owner admins can also page through
accounts, create accounts, manage other accounts' tokens and disable accounts.
Read/write credentials show their identity and explain the required admin scope.
Repository grants remain separate from token scopes and are managed through the API.

Token rows show scope, ID, creation time, expiry, revocation and **This session**.
Times use the browser's local time; refresh to update status. Issuance defaults
to 30 days, with 7-day, 90-day and non-expiring choices. Account creation issues
one non-expiring initial token. The browser generates secrets using Web Crypto
and shows them only in the issuance dialog. Save the secret before closing;
Canopy stores only its digest and cannot retrieve it later. Nothing is placed in
URLs or browser storage. A lost response preserves the exact request and secret
for **Retry token issuance** or **Retry account creation**. Reloading or closing
an unconfirmed issuance can lose a credential that the server already accepted.

Revocation and disablement have explicit confirmations. Confirmed revocation of
the current credential disconnects the tab. The server protects the site's last
non-expiring admin credential. Disabled accounts remain listed with their name
reserved; re-enable and account deletion are not implemented.

`GET /api/session` requires an active credential of any scope and returns
`{account, token_scope, token_id, site_admin}` without the secret or digest.
`GET /api/accounts?after=<name>` requires a site-owner admin credential and
returns `{accounts: [{name, enabled}], next_after}`. Pages contain up to 32
accounts, including disabled identities, ordered by name. Continue with
`next_after` until null; a full final page may require one empty request. Each
page observes current state independently. Account listing rechecks the exact
credential and its expiry in the Directory query at owner execution time.
Both endpoints use `Cache-Control: no-store`.

Site-owner admins can open **Account history** for committed account creation,
disablement, token issuance and revocation. Entries identify the actor and public
credential IDs, target account, scope, expiry and execution time. History is
newest first with **Older changes** pagination; retries that change nothing add
no entries. The initial trusted bootstrap is labeled **System bootstrap**.
`GET /api/audit/accounts?before=<event-id>` exposes the same 32-entry pages and
returns `{events, next_before}`. IDs/cursors are decimal strings. Secrets and
credential digests are excluded. History and the change commit atomically in the
Directory Cell and restore together. Repository-policy changes, denied attempts,
retention/export and account deletion remain separate work.
