# Canopy persisted contracts

These are the current development contracts. Freeze them and add migrations
before admitting persistent customer repositories.

| Surface | Current value | Owner |
| --- | --- | --- |
| Application | `canopy` | `CanopyApplication` |
| Repository namespace | sixteen bytes of value `0x47` | `REPOSITORIES` |
| Repository partition | canonical 16-byte UUID, versions 1–8, RFC 4122 variant | `repository_target`, `CellType::entity_uuid` |
| Repository Cell | one SQL Cell per repository UUID | Cellule catalog and authority |
| Git object format | SHA-1 object IDs from canonical Git type, decimal length, NUL and body | `object_id` |
| Small Git objects | SQLite `objects.body`, maximum 768 KiB | Repository Cell |
| Large Git blobs | immutable `repos/<uuid>/git-blobs/<sha256>` body, SQLite digest/size/reference | `LargeBlobStore` |
| LFS objects | immutable `repos/<uuid>/lfs/<sha256>` body, SQLite digest/size/reference | `LfsService` |
| External byte ceiling | 64 MiB per Git blob or LFS object | current buffered ingress |
| Ref mutation | compare expected OID and monotonic version; apply all updates in one Cell transaction | `FinalizePush` |

The `objects` table stores one verified kind, size and independent BLAKE3
digest per Git OID. External objects also store SHA-256. Readers verify the
bytes against the SQLite record and recompute the Git OID or LFS SHA-256.
The `refs` table stores name, OID and version. A successful push persists all
new objects before `FinalizePush` publishes any ref. Rejected or interrupted
pushes may leave unreferenced objects; collection is not implemented yet.

Git packs and the bare repository cache are transport and acceleration
artifacts. Neither is authoritative. The gateway can reconstruct cache objects
from the Cell and external store. One test proves gateway restart in one live
runtime. Another proves exact-root restore after clean node shutdown and local
SQLite loss using a shared in-memory object store. Unexpected owner loss,
ambiguous push result resolution and multi-node takeover still need proof
before service readiness.

Schema version 1 is still changing in this unreleased repository. The module
descriptor and object paths will become compatibility boundaries at the first
persistent preview. A release must pin an immutable Cellule revision rather
than the current absolute local dependencies.
