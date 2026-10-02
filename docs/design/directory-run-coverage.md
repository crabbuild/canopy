# Immutable directory run coverage

This is the persisted contract for directory-root v3 and range-index v2. It supports bounded compaction windows while retaining the existing native files, canonical headers, source identities, placement versions and persistent range tree. The deployment is a hard cutover: previous directory-root and directory-leaf domains are rejected. Production schema/handler selection and deployment-format installation remain pending.

## One representation for full files and projections

Every `StoredRun` contains a complete physical `RunDescriptor`, an authenticated `ArtifactDescriptor`, and a logical `RunCoverage`. A whole-file record uses the physical descriptor's count, endpoints and canonical inventory as its coverage. A projection uses inclusive OID endpoints and the exact count and canonical inventory of the entries within those endpoints. There is no alternate legacy representation or optional projection flag.

Physical facts continue to identify the entire immutable file: repository, object format, creating operation, count, first/last OID, canonical inventory, byte size and file digest. The artifact additionally binds its authenticated manifest. Coverage never changes these facts or the file's bytes.

Coverage uses the existing `inventory_seed` and `fold_header` encoding over strictly increasing OIDs. The fold excludes physical placement. A projection starts a fresh canonical fold at ordinal zero; its digest is not a byte-range hash or a substring of the parent's digest. Source and location versions remain in the entries and merge with the existing canonical-conflict and preferred-placement rules.

Structural validation rejects zero counts, counts larger than the physical count, wrong OID widths, reversed or out-of-file endpoints, and inconsistent singleton endpoints. Full count or both full endpoints require the exact complete physical coverage. Structural checks do not prove a partial inventory; trusted preparation must fold its actual entries before certifying it.

## Persisted bytes

Directory snapshots use `canopy.directory-root.v3\0`. Directory range nodes use `canopy.range-index.v2\0`. Source leaves retain their separate existing domain. Native pack/index formats and the canonical header fold are unchanged.

All integers below use the existing bounded codec's big-endian encoding. Byte arrays are prefixed by a big-endian u32 length. A directory leaf record serializes these fields in order:

| Field | Encoding |
| --- | --- |
| Physical creating operation | bytes, length 16 |
| Physical object count | u64 |
| Physical first OID | bytes, length 20 or 32 |
| Physical last OID | bytes, length 20 or 32 |
| Physical canonical inventory | bytes, length 32 |
| Artifact byte size | u64 |
| Artifact file digest | bytes, length 32 |
| Artifact manifest digest | bytes, length 32 |
| Coverage object count | u64 |
| Coverage first OID | bytes, same object format |
| Coverage last OID | bytes, same object format |
| Coverage canonical inventory | bytes, length 32 |

The node context carries repository and object format. Physical descriptor size/digest are reconstructed from the artifact and must agree. Decoder bounds, exact field widths, structural validation and trailing-byte rejection still apply.

Independent fixed-byte record vectors are [SHA-1](directory-run-v2-sha1.hex) (284 bytes) and [SHA-256](directory-run-v2-sha256.hex) (332 bytes). They are codec fixtures with deliberately synthetic inventory facts; they grant no closure or publication authority. Tests compare exact encoder bytes and decode them independently.

Directory node fanout is 128, encoded node size is at most 64 KiB, and height is at most seven. Index keys, endpoints and represented object counts use logical coverage. Generic node counts may include overlaps across roots or source shards; they are never a unique canonical-object proof or deletion authority. Snapshot point selection remains bounded by 32 ingress roots plus 16 levels.

## Verified bounded replacement

1. Select one catalog-derived source projection and a consecutive prefix of complete target records within the input record/physical-byte budget. Charge a shared physical file once and reject conflicting physical facts under the same identity.
2. Scan the complete source projection in indexed pages of at most 512 entries. Fold parent, moved prefix and retained suffix in the same pass. Reject before either fragment escapes if the parent's count, endpoints or canonical inventory differ. Empty fragments are absent.
3. Merge the prefix and selected targets through the existing admitted builder and partitioner. For disjoint promotion within the output file limit, reuse the verified original physical artifact. The suffix always retains its exact physical descriptor and artifact identity.
4. Revalidate the exact source incarnation and target interval against the selected publication base. Path-copy their replacements, retaining unrelated current files. A replaced source or newly overlapping, missing or changed target rejects reuse. Bind the original source, prefix, suffix and selected targets into the range-compaction v2 input digest.
5. Issue the existing purpose-bound maintenance certificate and publish through admin command 22. The catalog update and immutable outcome are atomic; refs and source roots remain unchanged. Query 23 and existing lease, owner-fence, checkpoint and replay rules continue to apply.

If an unaffordable first target starts after the source's first OID, the preceding disjoint source prefix may progress. Otherwise a required physical input larger than the job budget rejects. A source spanning many affordable target files progresses through repeated jobs; the scheduler must revisit the retained suffix rather than skip to the original source's last OID.

## File sharing, retention and cost

The catalog file cache validates a requested projection, then normalizes its cache descriptor to the complete physical coverage. Distinct projections share one authenticated file, reader admission and disk charge. Different physical descriptors or manifests under the same physical key fail even on a cache hit. Snapshot lookup selects requests by logical coverage before grouping by physical file, so a projection cannot expose entries outside its endpoints.

Raw descriptors and range-node summaries provide no authorization or closure proof. Only a trusted catalog/preparation context certifies coverage. Reclamation must inventory all retained roots and valid pins, deduplicate physical artifact incarnations, and fence deletion; removal of one projection cannot authorize deleting a file retained by another. Online reclamation is not implemented by this increment.

Each window scans its complete parent logical projection, including the suffix, to preserve exact canonical proofs. Output runs are bounded at 64 MiB by the current partitioner, but repeated windows can amplify read/CPU work. Geometric selection, continuous fair scheduling, whole-process resource admission and full-history mixed-load measurements must qualify that cost. Current defaults retain 128 input records/256 MiB physical input, a 256 MiB spool with a 768 MiB reservation, and at most 64 MiB output files. These limits and small native correctness fixtures do not establish capacity for 10,000 engineers.
