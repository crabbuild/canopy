# Git compatibility

Canopy supports ordinary SHA-1 Git repositories over smart HTTP. It is not yet
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
| Git LFS basic upload/download | Stock `git-lfs`, SQLite metadata, immutable object-store bodies and restart/backup tests |
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
| Rejection reporting | Branch policy, unsupported names, command limits and late Cell refusals produce Git reports; clients declining reports receive HTTP 409, and infrastructure failures can still return HTTP 500 | Finish pre-publication resource/infrastructure reporting without mislabeling uncertain outcomes |
| Ref names | UTF-8 only, at most 255 bytes total; filesystem ref caches add host filesystem constraints | Declare raw-byte, long-name and filesystem-equivalent-name scope and test accepted names end to end |
| Partial clone | `blob:none`, `blob:limit`, `tree`, `object:type` and `combine` filters enabled; `sparse:oid` disabled | Qualify supported filters with real providers and large histories; select sparse-pattern scope explicitly |
| Cold fetch | Exact `blob:none` hydrates non-blob history, ref/tag targets and explicit wants; other filters and full fetch still hydrate all stored objects | Bound preparation to the requested reachable object set and measure bytes/time for cold and warm requests |
| SSH | No SSH listener or Git command endpoint | Key ownership/revocation, repository ACLs, clone/push/fetch, cancellation and durable publication |
| Push options | Not advertised; `git push -o` fails | Define supported option semantics, validate before publication and persist outcomes |
| Signed pushes | Push certificates are not advertised; `git push --signed=true` fails | Certificate verification, signer identity, nonce/replay handling and durable audit record |
| SHA-256 Git repositories | Rejected; object IDs and graph formats are SHA-1 throughout | Repository-level format identity, 32-byte graph/ref storage, negotiation, restore and mixed-format rejection |
| Advanced LFS | Basic transfer only; no lock API, resumable/custom transfer or external-LFS federation | Define scope; stock-client locking and conflict tests, recovery and ACL coverage for each added endpoint |
| Other transports/services | No dumb HTTP, Git daemon or remote archive endpoint | Explicitly select supported services and add stock-client tests before claiming support |

Signed commits and signed tags are ordinary stored Git objects; **signed push
certificates are a different feature**. LFS uses SHA-256 content IDs already;
that does not imply SHA-256 Git repository support. Xet is outside Canopy's scope.

Current resource bounds also include a 512 MiB push body, 64 MiB fetch request,
64 MiB individual non-blob object, 5 GiB external blob/LFS object, 512 MiB
Repository Cell database and 120-second native Git process deadline. Full Git
compatibility does not mean unlimited repository or request size; limits and
their rejection behavior are part of the supported contract.

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
after restoring policy/permission. The fragmented-report unit test covers 4,096
Unicode refs and preserves existing native rejections.
Hosted CI has not been run for these changes.

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
4. Add SSH using the same authorization, object ingestion and publication path.
5. Complete the selected push, LFS and SHA-256 capabilities; keep unsupported
   services explicit until their acceptance gates pass.
