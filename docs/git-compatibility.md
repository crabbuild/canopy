# Git compatibility

Canopy supports ordinary SHA-1 and SHA-256 Git repositories over smart HTTP. It is not yet
a fully compatible replacement for every Git server capability. The native
`git-http-backend` owns protocol negotiation and pack processing; Canopy must
also persist the accepted objects and refs before returning success.

## Verified operations

| Surface | Current evidence |
| --- | --- |
| Clone, push, incremental fetch and fast-forward pull | Stock Git through the production HTTP server and Repository Cells |
| Protocol v0 and v2 | Native discovery and transfer; a client requesting v1 also works by falling back to v0 |
| Lightweight and annotated tags, notes, ordinary custom refs | Mirror round trip, exact ref/OID comparison and strict full `git fsck` |
| Unicode UTF-8 branches and tags | Push, protected branch preflight, default branch selection, clone and fresh-disk recovery |
| Partial clone and lazy fetch | Blobless v0/v2, treeless, blob-size, object-type and combined filters; omitted objects fetched on demand; unreachable commit/tree/blob wants rejected |
| Shallow clone, deepen, unshallow | Verified history counts and subsequent incremental pull |
| Branch deletion and fetch pruning | Deleted refs disappear from remote tracking refs |
| Mirror clone and mirror push | Exact refs/tags/notes in a second repository; 4,096 long refs published in one generation and recovered after fresh-disk restart |
| Force-with-lease, mixed rejection and atomic push | Existing owner-race and protected-branch integration tests |
| Push option notes | Stock HTTP/SSH `git push -o canopy.note=...`, ordered durable receipts, unsupported-option rejection, delete-only push and fresh-disk recovery |
| Git LFS basic upload/download | Stock `git-lfs`, SQLite metadata, immutable object-store bodies and restart/backup tests |
| Git LFS locking | Stock lock/list/unlock, forced unlock, pre-push conflict checks, paginated verification, ACLs and fresh-disk recovery |
| SHA-256 Git repositories | Repository format selected at creation; stock HTTP/SSH push and clone, annotated tag, external blob, LFS pull, browser resolve, filtered clone, incremental fetch, reviewed pull request and required check merge, native merge/squash/rebase candidates, and fresh-disk restore; strict full `git fsck`; Git/LFS and candidate recovery also pass against isolated RustFS |
| Recovery without original local disk | Rebuild from durable Cell state, compare every live ref/OID and run strict full fsck |

Local operations such as commit, diff, merge, rebase, stash and cherry-pick run
in the client's Git checkout. They require the host to preserve Git objects
and refs, rather than separate server implementations of those commands.
Submodule targets require their own accessible repository URLs; recursively
cloning mixed Git/LFS submodules still needs an explicit compatibility gate.

## Missing or restricted

| Surface | Current behavior | Acceptance gate |
| --- | --- | --- |
| Bulk refs | Up to 100,000 updates staged in SQLite; 4,096-ref mirror import/delete and atomic generation qualified | Full-capacity and real-provider scale qualification remain |
| Rejection reporting | Branch policy, unsupported names, command limits, late Cell refusals, ingestion and preparation failures produce durable Git reports when commands are decoded and completion is available; clients declining reports receive HTTP 409 | Incomplete uploads, unparsed commands and unavailable/uncertain response publication can still return transport errors; qualify the remaining failure matrix |
| Ref names | UTF-8 only, at most 255 bytes total; filesystem ref caches add host filesystem constraints | Declare raw-byte, long-name and filesystem-equivalent-name scope and test accepted names end to end |
| Partial clone | `blob:none`, `blob:limit`, `tree`, `object:type` and `combine` filters enabled; `sparse:oid` disabled | Qualify supported filters with real providers and large histories; select sparse-pattern scope explicitly |
| Cold fetch | HTTP and SSH hydrate ref/tag targets and non-blob history reachable from requested wants; exact `blob:none` adds explicit blob wants only; native tree/type/combined filters select missing reachable blobs before hydration | Narrow size-filter hydration; measure cold/warm bytes and latency at scale |
| SSH | Optional listener, signature authentication, key scope and repository ACLs; stock transfers, recovery, shared admission, fetch cancellation and disconnected push drain tested | Storage failures, owner-loss, real-provider and capacity qualification |
| Push options | Advertised over HTTP/SSH; up to 16 ordered printable-ASCII `canopy.note=<text>` values, each at most 1,024 bytes, are saved with the completed push; other options receive a Git rejection | Additional option names and their effects require explicit product contracts; no CI or user-hook behavior is implied |
| Signed pushes | Push certificates are not advertised; `git push --signed=true` fails. The request parser rejects mixed command lists and mismatched signed/outer push options | Certificate verification, signer identity, nonce/replay handling and durable audit record |
| SHA-256 Git repositories | Repository identity, object IDs, graph/ref storage and native Git negotiation support SHA-256; mixed format objects and refs are rejected | Cloud S3/GCS/Azure and cross-platform qualification remain |
| Advanced LFS | HTTP basic transfers, verified tail-range download resume, advisory locks and SSH authentication for repository/operation-scoped HTTP grants; no pure SSH, resumable upload, custom transfer or external-LFS federation | Complete selected transfer capabilities, optimize large-offset resume reads and qualify real providers |
| Other transports/services | No dumb HTTP, Git daemon or remote archive endpoint | Explicitly select supported services and add stock-client tests before claiming support |

Signed commits and signed tags are ordinary stored Git objects; **signed push
certificates are a different feature**. LFS uses SHA-256 content IDs independently of each repository's Git object format. Xet is outside Canopy's scope.

Canopy imposes no fixed product byte quota on push bodies, Repository Cell
SQLite databases, Git blobs, LFS objects, or individual SQLite Git objects.
External bodies use immutable 8 MiB parts and a 16-byte manifest, so the S3
adapter's single-part copy restriction does not bound logical file size. SQLite
integer/page formats and available storage still impose physical limits.
Node resource admission, bounded batch/chunk sizes, protocol validation, the
64 MiB fetch-request and push-report bounds, ref-count/name constraints, and
native-worker deadlines remain. These are not an unlimited-capacity claim.


## Size qualification

`python3 scripts/qualify_size.py` starts an isolated RustFS container and runs
`tests/multi_server/size.rs` plus the ignored SHA-256 provider tests in
`tests/multi_server/sha256.rs`. It requires Docker, the AWS CLI, Git, and a temporary
directory with at least 40 GiB free. Set `TMPDIR` to the dedicated test volume.
Docker must mount that host volume; the script verifies visibility before writes.
Use `DOCKER_CONTEXT` to select an isolated Docker environment if needed.
The fixture uses public test credentials and removes its own container and data.
The Verify workflow explicitly invokes this gate; ordinary `cargo test` skips
the resource-intensive provider tests. `--sha256-only` runs the smaller SHA-256
probes against an isolated Docker volume when the host's large test volume is
unavailable.

The gate pushes an incompressible pack above 512 MiB, checks the SQLite database
exceeds 512 MiB, stores a Git blob and LFS object above 5 GiB, restarts on fresh
local storage, then checks clone bytes, strict fsck, and the LFS download hash.
The ordinary large-object integration test covers a 65 MiB commit and recovery.
These are qualification points, not configured maxima or performance targets.

The pinned runtime retains its five-second SQL command deadline. Very large
non-blob verification still materializes a complete object inside a transaction;
progressive verification is needed before claiming arbitrary structural-object
capacity. LTX artifact promotion and backup copies still use the configured
provider's conditional-copy implementation; very large LTX artifacts need
separate provider qualification. External Git/LFS parts avoid that issue for
file bodies. Neither a database format ceiling nor quota removal constitutes
proof of unlimited capacity.


## Permanent gates and remaining work

`tests/multi_server/compatibility.rs` exercises the working transport matrix
above through real Git processes, HTTP and Cell persistence. The existing
default-branch test now uses a Unicode branch. `src/refs.rs` compares the shared
name validator against `git check-ref-format` for ASCII restrictions and Unicode.
The existing `Verify` workflow runs these tests through `cargo test --locked`.
Bulk tests additionally cover 1,001 distinct graph roots, namespace conflicts
beyond several SQL pages, long plans exceeding the command wire ceiling, mixed
and atomic rejection, bulk deletion and command-limit rejection/replay.
`tests/multi_server/partial_clone.rs` verifies actual client omissions, on-demand
bytes, fresh-disk blob omission and rejected unreachable wants before and after
full cache hydration. SQL work-bound tests cover indexed structural reads and
reachability short-circuiting with 10,000 stored objects.
`tests/smart_http/publication.rs` pauses large-blob ingestion after native Git
acceptance, then changes refs, policy or write permission. It verifies Git
rejection, no sibling/generation publication and exact replay from a new gateway
after restoring policy/permission. Injected ingestion failures cover stock Git,
plain/sideband/no-report clients, exact rejection replay and a new successful
operation after recovery. A concurrent same-ID success wins over a losing
attempt's ingestion failure. SSH exercises the same durable refusal path and
fresh-disk recovery. The fragmented-report unit test covers 4,096
Unicode refs and preserves existing native rejections.
Hosted CI has not been run for these changes.

`tests/multi_server/lfs_locks.rs` uses stock Git LFS to create/list locks,
reject a conflicting push before refs change, force-unlock and retry, then clone
and verify the resulting bytes. API coverage includes concurrent lock creation,
repository isolation, branch-independent exclusivity, list/verify pagination,
owner partitions, token scope, ACL downgrade, public reads and fresh-disk restore.
These locks are advisory: clients can bypass pre-push verification.

`tests/multi_server/ssh.rs` exercises stock Git SSH mirror push, protocol v0/v2
clone (v1 requests fall back to v0), shallow/unshallow, filtered lazy fetch,
incremental pull, deletion and fresh-disk mirror recovery. Wire-level probes
check unreachable commit/tree/blob wants, command and environment restrictions,
forwarding denial, key scope, repository ACLs and revocation on an authenticated
connection. Held fetches consume HTTP account capacity, release it on channel
close, and cancel during node shutdown.
Stock OpenSSH authentication is tested with Ed25519, RSA and ECDSA P-256,
P-384 and P-521 keys.

`tests/multi_server/filtered_preparation.rs` covers cold HTTP/SSH v0/v2 with
tree depths, object types, blobless and escaped/nested combined filters. It checks
client object inventory and server cache presence separately, then exercises lazy
fetch and strict fsck. Size filters still hydrate unknown-size missing blobs;
Git's native filter needs their headers before it can exclude them.

`tests/multi_server/ssh_fetch.rs` verifies cold v0/v2 blobless clones, explicit
lazy blob hydration, blob-tag advertisements and subsequent full clones. Its
full-fetch regression covers HTTP and SSH v0/v2: a cold single-branch clone
omits unrelated/deleted branch blobs; a later fetch of another branch loads
that branch's bytes without loading the deleted branch. A fresh server also
handles negotiation from a client with unrelated history without hydrating its
blobs. Strict fsck verifies the resulting clones.
`tests/multi_server/ssh_publication.rs` pauses external ingestion after native
acceptance: late ACL/policy refusals preserve both refs and generation, while an
accepted push survives client disconnect and completes before shutdown releases
Cells. Fresh-disk clones and strict fsck verify both outcomes.

The baseline real-provider probe used Apple Git 2.50.1 and disposable RustFS
`1.0.0-beta.8-glibc` at Canopy commit `48dfc21`; it confirmed the bulk, filter,
push-option, signed-push and SHA-256 gaps above. Unicode failed in that baseline
and is corrected by the Unicode validator change and permanent integration tests.
The former 64-ref cap is also removed, and filtered clones now pass local
integration tests. Current bulk and filter qualification uses the in-memory
provider and does not replace the earlier real-provider baseline.
Tests using the in-memory provider do not qualify provider outages or performance.

Implementation order:

1. Qualify staged bulk publication at scale and finish resource/infrastructure
   error reporting. Bounded plan chunks now feed one final Cell transaction; no
   independently committed ref batches or old inline HTTP completion remain.
2. Add positive and negative CI gates alongside each capability, including a
   real-provider process/restart suite and cross-platform cache qualification.
3. Further reduce cold preparation beyond blobless fetch; measure transferred
   and hydrated objects separately and qualify filter negotiation at scale.
4. Qualify SSH interruption and publication failures using the shared authorization,
   object ingestion and publication path.
5. Complete the selected push, LFS and SHA-256 capabilities; keep unsupported
   services explicit until their acceptance gates pass.
