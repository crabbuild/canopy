# Native generated candidate publication

This hard-cutover path reuses the existing candidate, merge, catalog, ref snapshot,
input checkpoint and exact-command recovery structures. It creates no SQL Git
object/ancestry mirror and no second persistent candidate UUID table.

## Frozen intent and resident admission

Operation 10 reserves the existing `merge_candidates` row with the first actor,
pull number, full source/base revision, strategy, message and creating timestamp.
The intent is immutable. Completed results are immutable and duplicate inserts
and deletes are refused by the native schema. A completed UUID returns its
original result under current authorization.

A pending intent enters resident staging with a domain-separated digest of its
canonical candidate value and repository. Admission briefly holds the existing
gateway admission mutex. It searches only the staging service's bounded admitted
job map for the same actor and digest. Concurrent observers share the original
controller, including uncertain original custody or publication commands. A new
operation and creating namespace can be admitted after a known attempt has fully
drained. No observer owns the native process or final recovery command.

Git `merge-base`, `merge-tree`, `commit-tree` and the bounded rebase producer run
with the staging physical owner retained by their native process owner. Rebase
continues to support at most 128 linear commits, preserving original author,
message and encoding while removing stale signatures. The private catalog
verifier independently checks the original boundary and exact rewritten chain.
Only this native producer constructs `ProducedCandidate`; a client Ready DTO
cannot construct the production witness.

## Generated inputs

The accepted native base consists of immutable catalog packs. The producer
streams newly written loose OIDs into a disk-admitted spool with constant
iteration memory and a one-million-object request ceiling. `pack-objects` reads
that explicit spool, disables object/delta reuse and emits a non-thin pair.
Existing base packs are not recaptured as new input.

Capture reconciles disk usage, fences the private cache and inspects only that
exact generated pack/index pair in the admitted creating namespace. The fence,
cache and physical owner remain pinned through authenticated hash/upload work.
The existing native input checkpoint retains the exact pair inventory.
Independent physical verification downloads it without alternates, checks the
physical partition and canonical identities, then stages bounded metadata.
Catalog preparation checks typed closure against the certified bound base.
The candidate verifier checks the exact generated commit semantics before the
private publication factory can issue a MAC. A generated commit already present
in the certified base can require no new pair.

## Atomic publication and compact recovery

Operation 55, codec 1, authenticates the candidate-purpose binding of the frozen
result, exact held-base native ref observations, proposed snapshot, its certified
ref generation, and typed permanent audit. Ready creates the one server-owned
candidate fetch ref with an absent expectation; the ordinary push factory still
rejects server-owned ref updates.

The final owner transaction selects the immutable intent and any first completed
result, then checks current write authority, actual owner fence, exact operation
and independent pin, lease expiry, retention, current joint roots and full pull
revision. Ready additionally checks generation capacity. It atomically commits
the catalog/ref roots, existing constant-size ref summary (preserving HEAD),
candidate result/OID/audit, attestation checkpoint, original recovery phase and
SDK acceptance. Late SQL failure rolls all of those back. Negative results retain
the frozen editorial result without advancing roots.

`CandidatePublicationReply` carries the UUID, canonical result digest and optional
`PublishedRefs`; it fits the existing 512-byte recovery contract even when a
conflict result approaches the existing 256-KiB limit. The larger result remains
in the immutable candidate row. Ready additionally stores a typed input-root
audit referencing the accepted catalog and ref snapshot. Recovery kind 6 routes
the exact originally registered operation 55 command. Typed retirement selects
the permanent row before traversing its audit and preserves the original SDK
receipt before releasing the transient pin. Missing audit metadata prevents
retirement. This path performs no provider deletion.

## Reviewed generated merges

Operation 9, codec 8, supports fast-forward and generated strategies. The private
factory selects the immutable Ready candidate matching the requested UUID,
strategy, pull number and complete revision. It verifies that candidate's native
audit and commit semantics in the accepted catalog, observes the exact reserved
ref and prepares the ordinary base-branch update to the generated target. Native
ancestry evidence proves the target descends from the requested base.

The final transaction reselects the candidate result and audit, checks the held
reserved ref, and rechecks current authorization, pull revision, approvals,
changes requested, required checks on the generated target, branch policy,
owner/pin/expiry/retention and joint-root CAS. The reviewed capability authorizes
only that exact update. Existing `pull_merges` now retains strategy and candidate
UUID; its immutable audit retains the actual target and candidate audit root.
Replay reconstructs the original binding from those fields rather than assuming
fast-forward. Merge results retain their existing wire shape and original receipt.

## Qualification boundaries

The evidence record distinguishes focused tests, the full workspace diagnostic
and exact-head Linux CI. This work does not qualify full-history cold-base cost,
10,000-engineer throughput, automatic reconstruction of generated work before
final command registration, remote-provider interruption/adoption, continuous
retention/collection, backup export of the complete typed graph, or native
acceleration. Those remain required delivery gates. Passing the generated
workflow does not make the full cutover release qualified.
