# Staged input retention and late catalog binding

Long imports now acquire artifact custody without retaining every catalog generation published during upload and physical verification. This uses the existing preparation token, namespace allocator, operation rows and independent lease rows. A one-way bind adds the current catalog floor only when catalog-dependent preparation starts. Production producer wiring, automatic renewal, authenticated takeover reconstruction and full-history qualification remain required.

## Stored phases and exact identity

`catalog_operations.generation` and `catalog_leases.generation` are nullable in the fresh schema. NULL means input staging; zero still means a bound, certified empty catalog. A staging pin retains its complete creating namespace until its independently recorded expiry and reaping. It does not retain historical catalog facts. No new per-object table or separate input ownership service is introduced.

Both tables store a generated `binding_generation = coalesce(generation, -1)`. The deferred composite foreign key uses that non-null value alongside incarnation, admission sequence, logical operation, owner epoch, creating namespace and expiry. Using a nullable generation directly in the composite foreign key would allow SQLite to skip all the other binding checks. The generated value prevents that escape while retaining the existing exact binding structure.

Pin identity stays immutable. Its generation can change once from NULL to a non-null floor, with no attestation present. A bound floor cannot advance, reset to NULL or change to another generation. A staging operation or pin cannot contain a publication attestation. Both sides of the binding transition commit atomically; a pin-only update fails the deferred foreign key.

## Typed protocol

| Operation | ID | Result and authority |
| --- | --- | --- |
| BeginStaging | 24 | Allocates one current-owner attempt and creating namespace, without reading a catalog floor |
| RenewStaging | 25 | Extends an unexpired matching staging lease; preserves namespace and NULL generation |
| BindStaging | 26 | Reads the current certified generation and binds it once; preserves token, namespace and expiry |
| CheckStaging | 27 query | Reads an exact authorized unexpired staging lease; gives no catalog or publication proof |
| ClaimStaging | 28 | Allocates a new admitted attempt and namespace, preserving the previous independent pin |

Begin and Claim reuse `BeginRequest` and `LeaseRequest`. Check and Bind reuse `LeaseCheck`. A distinct bounded `StagingLease` carries the token, object format and observed/expiry times, with no base fact. Decoded lease data cannot construct `PreparationBaseResolver`. The existing Abort and bounded Reap commands serve both phases.

Repeated Begin with the same actor and request digest returns the same live staging identity; a conflicting or completed request is refused. Exact Cellule replay returns the original result and receipt. Repeated Bind returns its original floor even after the current catalog advances. It does not allocate another namespace or pin and can succeed when the pin quota is full. Read-only frontier refresh remains query 21 after binding.

Regular Begin, Renew and Claim preparation paths reject staging rows. Staging Begin, Renew and Claim reject bound rows. CheckPreparation and CheckPreparationFrontier return no base for staging. Final publication and attestation validation explicitly reject an absent floor. Bind grants no canonical, dependency, policy or physical-verification authority; the existing private preparation and certificate factories remain mandatory.

## Producer sequence

1. Obtain durable BeginStaging acceptance before external upload. Keep its exact SDK mutation identity and receipt for uncertain-outcome recovery. For ordinary short pushes, the direct BeginPreparation path remains available.
2. Upload, normalize and physically verify inputs under the returned creating namespace. Apply existing process, reader, disk and artifact admission. RenewStaging before expiry using current authority; renewal never resurrects expiry or acquires a catalog floor. A thin input needing published bases must use a separate admitted base reader and retain those exact bases during normalization. Staging itself supplies no base reader.
3. Finish the input phase, preserving private physical witnesses, canonical metadata segments and admitted workspace ownership. Physical verification does not prove dependency closure or grant publication.
4. BindStaging once, then open PreparationBaseResolver through the authoritative query at the binding receipt. The creating namespace is unchanged, so the existing CatalogPreparation accepts its verified physical witnesses and segments without decoding the pack again. Resolve closure and canonical overlaps against this late selected base.
5. Complete private verification and certification, enqueue the exact ready command, and retain inputs through ambiguous outcomes. Reconcile through the existing frontier protocol; never rebind a live floor or substitute another mutation identity while acceptance is unknown.
6. On owner loss, resolve exact and logical outcomes first. ClaimStaging stamps the new admitted owner and a fresh namespace. The old pin remains intact. Reusing old physical inputs under the new attempt requires authenticated adoption/reconstruction work that is still open; raw descriptors cannot bypass the assembler's creating-namespace check.

The input phase must be service-owned and renewed while workers or detached readers remain active. A DTO or canceled observer is not sufficient lifecycle management. Such producer orchestration is not selected in production yet.

## Expiry and capacity

Binding neither renews nor shortens artifact custody. Shortening the existing lease would invalidate previously admitted uploads or physical readers that borrowed its deadline. The bound floor therefore lasts for the remaining input lease, normally at most the default 60-second renewal interval. Configurations allowing five-minute staging renewals must account for a five-minute remaining floor when binding. This change separates an hours-long import from an hours-long catalog floor; it does not eliminate bound-floor capacity limits.

Both phases share the 1,024-operation and 4,096-independent-pin caps. Expired and detached pins count until reaped. Claim consumes another pin; Bind does not. The existing indexed minimum ignores NULL staging generations and retains all facts at or above the oldest bound floor, including expired floors until their pins are reaped. Generation zero and the current root remain protected. Reaping removes at most 512 rows from each class in one admitted command.

A bulk operation adds BeginStaging and BindStaging before final publication, plus renewals and any recovery claims. Account for this measured durable-command load separately from the ordinary two-command path. At the provisional 35 publications/s, a remaining 60-second floor contributes roughly 2,100 generations plus maintenance and reaping lag; an indefinitely renewed bound floor still exhausts the 8,192-fact cap. The 70/s headroom profile still requires qualified lease/pin/admission configuration.

A staging pin must be included as namespace custody in any future complete retained-root collector even though it has no catalog fact. SQL reaping does not authorize remote deletion. Recovery roots, backups, active readers, unexpired custody and uncertain publication outcomes remain separate retention obligations.

## Evidence and remaining release work

Nine tests cover normalized nullable-phase binding and immutable floors, bounded codecs, exact replay and one-way phase transition, revocation/exact identity/expiry, namespace separation after Claim/Abort, actual owner restoration and original receipts, shared operation/pin quotas and no-allocation Bind, and pre-bind native physical verification feeding the existing private catalog proof and durable attestation for SHA-1/SHA-256.

The retention fixture injects 10,240 intervening immutable generation facts in bounded batches with the production reaper between batches, keeping only generation zero and current. It uses trusted tiny catalog facts to test retention independently of publication and throughput. It does not simulate an hours-long import, prove provider durability or qualify full-history memory/CPU/I/O.

Required release work includes production HTTP/SSH/mirror/generated producers; service-owned renewal/drain and uncertainty handling; durable authenticated input inventories and takeover adoption; larger normalized input/metadata limits; capacity-aware scheduling of the remaining floor; complete retained-root collection and isolated restore; and the mandatory full-history hot-repository mixed-load campaigns. The full implementation goal remains open.
