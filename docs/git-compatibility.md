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
| Shallow clone, deepen, unshallow | Verified history counts and subsequent incremental pull |
| Branch deletion and fetch pruning | Deleted refs disappear from remote tracking refs |
| Small mirror clone and mirror push | Exact refs, tags and notes recovered in a second repository |
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
| Bulk refs | At most 64 updates per publication; a 65-ref unprotected push currently fails with HTTP 500 and publishes no refs | Large mirror import and deletion, one atomic Cell publication, partial native rejection, replay and restart |
| Rejection reporting | Branch policy uses native per-ref reports; late Cell conflicts return HTTP 409 and some capacity failures return HTTP 500 | Resource/policy refusals produce clear Git reports without reporting uncommitted refs as accepted |
| Ref names | UTF-8 only, at most 255 bytes total; filesystem ref caches add host filesystem constraints | Declare raw-byte, long-name and filesystem-equivalent-name scope and test accepted names end to end |
| Partial clone | Filters are not advertised; Git warns and downloads the full object set | Blobless/treeless clones actually omit objects, authorized lazy fetch succeeds, hidden/unreachable objects stay inaccessible |
| Cold fetch | Transfer preparation hydrates all stored objects; discovery already prepares only ref tips and tag chains | Bound preparation to the requested reachable object set and measure bytes/time for cold and warm requests |
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
Hosted CI has not been run for these changes.

The baseline real-provider probe used Apple Git 2.50.1 and disposable RustFS
`1.0.0-beta.8-glibc` at Canopy commit `48dfc21`; it confirmed the bulk, filter,
push-option, signed-push and SHA-256 gaps above. Unicode failed in that baseline
and is corrected by the current validator change and permanent integration tests.
Tests using the in-memory provider do not qualify provider outages or performance.

Implementation order:

1. Finish bulk ref publication and Git rejection reporting. Stage bounded plan
   chunks, then validate and publish the complete plan with its replayable
   response in one Cell transaction. Do not publish each chunk independently.
2. Add positive and negative CI gates alongside each capability, including a
   real-provider process/restart suite and cross-platform cache qualification.
3. Add filter negotiation and lazy fetch while reducing cold preparation;
   measure transferred and hydrated objects separately.
4. Add SSH using the same authorization, object ingestion and publication path.
5. Complete the selected push, LFS and SHA-256 capabilities; keep unsupported
   services explicit until their acceptance gates pass.
