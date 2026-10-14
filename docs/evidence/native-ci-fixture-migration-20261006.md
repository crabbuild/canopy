# Native CI fixture migration

The repository cutover registers native catalog publication, staging custody,
and serving owners. A detached `RepositoryCell` is not a serving owner, and the
old `PutObjects` command (5) is deliberately not registered. Standalone tests
must create repositories through a leased `CanopyServer`, just as production
requests do. The workflow continues to run every integration test target.

The standalone entry points now qualify these production paths:

- `owner_restart`: publish commit/tree/blob/tag and gitlink graphs; stop the
  first owner, delete its entire data directory, restore on a new node, clone
  and run strict Git fsck, and delete/recreate a branch after restoration.
- `repository_cell`: require the retired ingestion command to stay absent;
  publish and clone 600 delta candidates across selection-page boundaries in
  SHA-1 and SHA-256 repositories; reject an atomic two-ref update after a policy
  change and prove that neither ref changes.
- `smart_http`: preserve unsupported-media handling; exercise stock push
  options, both Git protocol versions, blobless clone and explicit lazy fetch,
  reject a guessed unreachable object, and publish a delete-only push.

The old helper modules under `tests/repository_cell` and `tests/smart_http`
remain available as historical fixture references. Their loose-object SQL,
chunk uploads, detached gateway and graph-certificate setup are not fixtures
for the supported storage model. Native coverage lives at the following seams:

| Former fixture area | Active coverage |
| --- | --- |
| Object batch/page/chunk bounds and rollback | `src/packs/metadata/tests.rs`, `src/packs/catalog/graph_spool/tests.rs`, `src/packs/verification/spool/tests.rs`, and the 600-object standalone case |
| Native object format, index binding and canonical bytes | `canopy-git-format/src/pack_index/tests.rs`, `src/packs/catalog/native/tests.rs`, `src/git_cache/tests.rs`, `src/packs/verification/tests.rs` |
| Graph/frontier preparation and all-or-none refs | `src/packs/publication/tests/frontier.rs`, `refs.rs`, `ref_policy`, and standalone atomic publication |
| Cache reuse, pressure and physical ownership | `src/packs/catalog/native/tests.rs`, `src/server/residency/tests`, `tests/multi_server/partial_clone.rs`, `ssh/fetch.rs`, `ssh/filtered_preparation.rs` |
| Encoded input, uploads and completion | `src/git_input/tests.rs`, `src/packs/publication/tests/native_capture.rs`, `tests/multi_server/push_options.rs`, `ssh/publication.rs` |
| Issues, checks, pulls, reviews, merge, rebase, visibility, default branch | Corresponding `tests/multi_server` modules and native publication unit tests |

Selective reads verify each original provider part against its authenticated
manifest and verify the extracted canonical object against its certified kind,
size, Git OID and BLAKE3 digest. Their private sparse pack is an input to native
Git decoding; it is not represented as a newly verified complete pack. Incoming
pack verification and writer preparation retain their complete-pack checks.

Size filters inspect bodies in a disposable workspace. The persistent cache
retains size-inspection candidates permitted by the other combined filters;
Git still applies the original size threshold to the response. Omitted bodies
outside the permitted tree/type selection are not retained.

A late SSH access downgrade can emit the original per-ref failure report only
when the staging owner reports a known inactive terminal attempt and current
access is below write. This is a wire rejection, not a durable publication or
success acknowledgement. Uncertain mutations and lost replies keep their
original error/recovery paths.

Explicit producer-root workspaces install each certified complete pack/index
pair once and read selected bodies from that workspace. This keeps native
baselines packed, avoids per-object child processes during construction, and
preserves the original cancellation/expiry/revocation file-admission tests.
Ref-based fetch workspaces continue to use selective extraction. The producer
correction changes no test assertions or admission limits; all 11 workspace
unit tests pass at this revision.
