# Owned push request preflight

The production Git gateway now retains one owned handoff from authenticated encoded input to normalized native input and parsed command intent. It reuses GitInput's disk-accounted spool, the existing BeginRequest identity and the existing command parser. The push handler consumes PushPreflight rather than accepting independent input, actor, operation and digest arguments. This is a prerequisite for routing production receive-pack into the packed lifecycle; the selected production schema and publication path still require conversion.

## Authentication and identity

The gateway checks current repository access, an account identity and transport authentication before spooling a push. EncodedPush then validates POST receive-pack, the actor component and the canonical target derived from tenant, application and repository UUID. Its authentication flag is an internal transport assertion after those gateway checks, not an independently verifiable authorization proof. Authorization must still be rechecked at final publication.

The request digest is BLAKE3 with domain `canopy.git.push-request.v3\0`. Fields appear in this order:

1. Tenant, application, namespace, partition, repository UUID, logical operation ID and actor, each prefixed with its byte length as a little-endian u64.
2. Four one-byte values: object-format byte width (20 or 32), protocol-v2 flag, content-type-presence flag and gzip flag.
3. Method, path, query and content type (empty if absent), each prefixed with a little-endian u64 byte length.
4. The little-endian u64 encoded body length followed by the exact encoded body bytes.

GitInput hashes the body with a fixed 64 KiB buffer and rewinds the spool. It computes the unkeyed artifact digest in that same scan for [durable request retention](durable-push-request.md); the authenticated uploader independently verifies the bytes. Hashing precedes decoding and is independent of upload chunk boundaries. Distinct gzip representations have distinct identities even when they expand to identical commands. This replaces the earlier v2 request domain directly; there is no legacy-digest replay adapter. Old persisted identities are not supported across the required fresh-data hard cutover.

The immutable BeginRequest is available before decoding so completed replay can return the original saved response without allocating another decoded spool or invoking native Git. Normalization consumes EncodedPush once and carries the same identity into PushPreflight. BeginRequest's lease duration remains a preparation policy value, outside request identity.

## Native input and intent

Gzip decoding reuses GitInput's admitted multi-member decoder and cancellation ownership. Blocking work retains its input/output files, disk reservations and transfer admission through drain. The command parser inspects only the bounded packet prefix and optional push-option group, preserving the entire normalized spool and rewinding it for native Git. It retains the existing 40 MiB command-prefix, 32 KiB option-prefix and 100,000-update ceilings. These bounds do not establish a whole-operation RSS or full-history latency qualification.

Old and new OIDs must match the repository format before zero IDs become create/delete intent. The same format check applies to signed certificate commands and shallow OIDs. Parsed ref versions remain placeholders for intent; current durable ref versions must supply the final CAS plan. Parsed certificate bytes are not a signature witness. Existing native signature/nonce verification and registered-key authorization still produce the private verified certificate. Signed options must match the separate option group; unsupported options retain the existing durable refusal path.

SSH's boundary detector still parses enough packets to locate a complete request before entering the common gateway. The common preflight performs normalization and policy parsing once per unreplayed request. Other media types and command limits continue through the existing native refusal path.

## Evidence and required continuation

Five focused tests cover context/actor/operation/format/metadata/body identity changes, both-format gzip normalization with immutable identity and spool rewind, mismatched signed/unsigned/shallow formats, signed option preservation/mismatch, invalid scope/authentication and invalid or oversized gzip with admission release. The production smart-HTTP replay regression reserves all disk except the encoded request size, proving a completed gzip retry can return its original response without expansion. Changing only gzip header metadata under the completed operation ID must conflict without changing refs.

The [durable request API](durable-push-request.md) now checkpoints the original encoded input before native receive and appends native inventory under exact predecessor CAS. Reopening reconstructs normalized input and parsed intent with the same identity. The final versioned ref-CAS plan and exact native response/signature evidence still require durable roots and process/owner-loss reconstruction. Plans exceeding the 4 MiB final-command envelope need a bounded root reference rather than copied inline ref vectors. HTTP/SSH packed producer orchestration, every other producer and canonical reader, fresh-schema selection, containment and full mixed-load qualification remain mandatory. This preflight adds no alternate publication format or remote deletion authority.
