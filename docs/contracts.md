# Canopy persisted contracts

These are the current development contracts. Freeze them and add migrations
before admitting persistent customer repositories.

| Surface | Current value | Owner |
| --- | --- | --- |
| Application | `canopy` | `CanopyApplication` |
| Repository namespace | sixteen bytes of value `0x47` | `REPOSITORIES` |
| Directory namespace | sixteen bytes of value `0x48`, one fixed SQL shard per tenant | `DIRECTORY` |
| Repository name | lowercase ASCII owner/name components, each at most 64 bytes | Directory Cell |
| Name reservation | owner/name row stores one canonical UUID and pending/ready state | Directory Cell |
| Repository rename | compare expected UUID, move one ready name atomically, keep Cell identity | Directory Cell |
| Account | lowercase ASCII name, enabled flag | Directory Cell |
| Access token | SHA-256 digest of bearer secret, account, scope (`read`, `write`, `admin`), enabled flag | Directory Cell |
| Repository access | immutable owner identity plus collaborator role (`read`, `write`); only owner has repository admin access | Repository Cell |
| Repository partition | canonical 16-byte UUID, versions 1–8, RFC 4122 variant | `repository_target`, `CellType::entity_uuid` |
| Repository Cell | one SQL Cell per repository UUID | Cellule catalog and authority |
| Git object format | SHA-1 object IDs from canonical Git type, decimal length, NUL and body | `object_id` |
| Small Git objects | SQLite `objects.body`, maximum 768 KiB | Repository Cell |
| Large Git blobs | immutable `repos/<uuid>/git-blobs/<sha256>` body, SQLite digest/size/reference | `LargeBlobStore` |
| LFS objects | immutable `repos/<uuid>/lfs/<sha256>` body, SQLite digest/size/reference | `LfsService` |
| External byte ceiling | 64 MiB per Git blob or LFS object | current buffered ingress |
| Ref mutation | check actor's write role, compare expected optional OID and monotonic version; retain deletion records and apply all updates in one Cell transaction | `FinalizePush` |
| HTTP push identity | repository-local UUID bound to account and BLAKE3 request digest; different IDs identify independent operations | `pushes` |
| HTTP push outcome | status, headers and BLAKE3-verified body in SQLite chunks; publish response pointer atomically with accepted refs | `CompletePush`, codec 1 |
| LFS metadata publication | check actor's write role in the SQLite insert transaction | `record_lfs_object` |

The `objects` table stores one verified kind, size and independent BLAKE3
digest per Git OID. External objects also store SHA-256. Readers verify the
bytes against the SQLite record and recompute the Git OID or LFS SHA-256.
The `refs` table stores name, optional OID and version. Deletion sets the OID
to NULL and advances the version; recreation advances it again. Never-seen
names and deleted names have different expectations, so stale plans fail even
after a ref returns to the same OID or to a deleted state. Deleted rows remain
in SQLite and are excluded from Git advertisements and namespace conflicts.
They must not be collected without a replacement mechanism for fencing stale
ref plans. `FinalizePush` uses codec version 3 for this expectation shape.
Before reporting accepted refs, a push persists all new objects and publishes
the accepted ref changes and replayable response through `CompletePush`. Both
`CompletePush` and the direct `FinalizePush` command share the same ref checks.
Rejected or interrupted pushes may leave unreferenced objects; collection is not
implemented yet.

Git owns the [per-ref report and atomic capability](https://git-scm.com/docs/protocol-capabilities#_report_status).
An ordinary push may accept some refs and reject others. The gateway publishes
the actual accepted changes in one Cell transaction before forwarding Git's
report unchanged. A rejected atomic push changes no refs. Malformed packs
retain Git's unpack failure report and publish no refs. The gateway does not
infer transaction success by searching diagnostic text for status fragments.

For receive-pack POSTs, `Idempotency-Key` must be one canonical lowercase,
hyphenated UUID. Missing IDs are generated, and recorded responses include
`X-Canopy-Push-Id`. The request digest covers a versioned domain, protocol flag,
content-type presence, and length-prefixed method, internal repository path,
query, content type and body. Account identity is bound separately. The
repository UUID scopes the record, so a name change preserves its identity.
Authorization secrets and host addresses are not part of the digest.

A reservation binds the ID before running Git. Response metadata and 512 KiB
body chunks are durable before finalization. `CompletePush` checks the binding,
chunk completeness and current write role, then applies the ref plan and
publishes the response pointer in one Cell transaction. A completed ID returns
its existing outcome. Only a published pointer can expose a staged response;
replay verifies the stored body length and BLAKE3 digest. The HTTP boundary
checks current token scope and repository access even for completed requests.

No-op, rejected and non-200 Git backend responses are also terminal outcomes.
Infrastructure or ref-CAS failures before publication leave the ID pending;
an exact retry can rerun against current refs. A lost reply after publication
returns the original report without changing refs, even when later operations
have changed them. Retrying a new Git invocation is not necessarily an exact
HTTP replay: its body can differ and then the reused ID conflicts. A distinct
ID permits identical bytes to represent a new operation. There is no expiry or
GC for reservations, completed outcomes or abandoned response attempts yet.

All branches currently permit deletion by an authorized writer. The gateway
sets `receive.denyDeleteCurrent=ignore` because its synthetic HEAD must not
create a branch protection policy. Git's
[receive-pack implementation](https://github.com/git/git/blob/v2.50.1/builtin/receive-pack.c#L1428-L1454)
otherwise rejects deletion of the branch named by HEAD even in this bare
cache. Future branch rules belong in the Repository Cell transaction.

A name reservation commits before its Repository Cell is provisioned. A retry
reads the previously reserved UUID and completes the same Cell instead of
assigning another identity. The server marks the row ready only after it has
acquired that Cell. Rename updates only the ready Directory Cell row, so the
same UUID and repository contents survive a URL change. Ready names route
through their UUID on demand; one node can serve multiple repository Cells.
The Directory Cell authenticates token digests; each Repository Cell authorizes
its own Git and LFS access. Repeated bootstrap requires the same owner token.
Account disablement, token rotation/revocation, collaborator-visible listing
and audit records remain open.

Token scope is checked at request admission. Repository write access is also
checked in the ref or LFS metadata transaction, so an intervening collaborator
revocation blocks publication. Previously admitted reads may finish after
revocation. A denied upload can leave an unreferenced immutable body; no
collector removes those bodies yet. Account creation is idempotent only for
the same account, token digest and scope; changing bootstrap credentials fails
startup instead of replacing the stored account.

Git packs and the bare repository cache are transport and acceleration
artifacts. Neither is authoritative. The gateway can reconstruct cache objects
from the Cell and external store. Integration tests prove exact-root restore
after clean node shutdown and local SQLite loss. The process smoke also proves
Git and LFS fetch after an unclean owner exit, lease expiry and a third process
claiming the Cell from an S3-compatible object store. A proxy also drops a
successful push reply; exact replay after takeover returns its recorded report
and preserves a later ref deletion. Crashes during
individual staging/publication boundaries, simultaneous multi-node routing and
backup restore still need proof before service readiness.

Schema version 1 is still changing in this unreleased repository. The module
descriptor and object paths will become compatibility boundaries at the first
persistent preview. The current build pins an immutable public Cellule
revision; its UUID partition contract is proposed in
[Cellule PR #5](https://github.com/crabbuild/cellule/pull/5).
