# Directory entry file attribution

Canopy should show the last commit that changed each directory entry at the selected revision. Return the directory page immediately and fill attribution asynchronously from an immutable, commit-specific cache. Use bounded native Git history queries for cold misses; add a persistent index to share unchanged attribution between commits as measured demand warrants. This is a proposed browser implementation, not an implemented endpoint or a production performance result.

## Current behavior and exact meaning

`git_read/browse.rs::browser_tree` returns up to 32 entries containing raw names and paths, kind, mode and OID, plus the selected commit. The selected commit is not each entry's last-changing commit. `browser_history` follows first parents and cannot serve as the attribution oracle without changing the semantics below.

Define the initial algorithm version by the result of this invocation against an authorized, certified snapshot:

```text
git --no-replace-objects --literal-pathspecs log -1 --format=%H <commit> -- <raw-path>
```

Pass arguments directly, preserving raw path bytes; never concatenate a shell command. Disable ambient Git configuration and graft/replacement behavior in the managed native workspace. This is path history without rename following. An entry changes when its kind, mode or object ID changes; a directory changes when its subtree changes. Renaming creates attribution at the new path. Deleting and later adding identical bytes counts as a new change. Author timestamps do not define the last change.

Git's default path history uses parent comparisons and history simplification at merges. A merge identical to a parent can inherit that parent's history; a merge resolution different from every parent is itself a change. Preserve ordered parents. Never silently substitute first-parent integration history. See [Git history simplification](https://git-scm.com/docs/git-log#_history_simplification).

Display the commit's author, subject, time and a link to that commit. A raw Git author identity is not proof of a Canopy account or the authenticated actor that pushed it. Author-to-account decoration must preserve that distinction.

## Read protocol

Resolve the requested ref once to commit C and a certified read generation. Every directory and attribution result carries C. A moving ref must not mix attribution from another revision into the same page.

Keep the existing directory pagination and expose a batch attribution request for at most one page of paths. Use repository identity, object format, C, literal raw path, and algorithm version as the logical cache key. A blob OID or Git tree OID alone is insufficient: identical content can occur in different histories.

The response contains the selected commit, one state per requested path, and a dictionary of commit summaries keyed by commit OID. States are `ready`, `pending`, or an explicit unavailable/error disposition. Never fill a missing result with the selected commit. A response can include ready entries while others remain pending. Use a bounded request deadline and an existing bounded polling mechanism or bounded retry token; do not introduce an unbounded job registry or a stream held indefinitely.

The UI renders names and file actions immediately, reserves space for attribution, then fills author/subject/time together. Discard responses whose commit or page no longer matches the visible page. Coalesce requests for the same commit and directory so many users do not launch duplicate history walks.

Authorize every request, including cache hits. Acquire the selected certified generation and retain its objects/workspace pins until native work and response ownership finish. Caching attribution neither grants Read nor proves object reachability. A detached observer cannot release resources still owned by a worker. Recheck current access according to the browser read contract before returning results.

## Cold computation and acceleration

Initially, use bounded native per-path queries inside the admitted repository read service. Bound workers, queued paths, output bytes, scratch and subprocess lifetime; schedule fairly between repositories and accounts. Do not spawn 32 unconstrained processes for a directory. Cache successful immutable answers and coalesce concurrent misses. Permission failures and budget exhaustion are not history results.

Generate verified commit graphs with changed-path Bloom filters in background maintenance of certified native workspaces. Bloom filters help history traversal reject commits that did not touch a path; a positive filter result is not proof of a change. They do not guarantee constant-time cold answers. See [Git commit graph maintenance](https://git-scm.com/docs/git-commit-graph).

Do not replace per-path queries with one naive `git log -- pathA pathB ...` and assign commits from that stream. History simplification for a union of paths can differ from simplification for each individual path. A shared custom walker requires differential qualification per path, including merges.

Prewarm the default branch's root page and recently viewed directories after publication. Full imports and pushes must not wait for attribution of every file or every historical commit. Under overload, retain immediate directory browsing and return pending attribution.

## Persistent index and shared data structures

For large sustained workloads, add an immutable derived index. Its commit binding identifies repository, format, algorithm version and C. An entry contains the exact path, kind/mode/OID and last-changing commit OID. Store commit summaries separately to avoid repeating author and message bytes for every file.

Reuse the packed architecture's `IndexKey`, `IndexRecord`, `RangeIndex`, `NodeRef`, bounded codecs, verified artifact transport and path-copy updates. Add distinct attribution codec domains and byte-ordered path keys; do not manufacture object IDs from paths. Qualify variable-length keys, node byte limits and wide/deep directories before adopting the generic index. Long keys may require different fanout bounds from object-directory records. A tree node's integrity does not establish that its attribution is semantically correct.

The proposed recurrence for a present path is: if its exact entry matches a parent, inherit the first matching ordered parent's last-change record; otherwise record C. A root records itself. For directories, compare the subtree entry. This recurrence matched a finite native Git experiment, but remains subject to broader differential qualification before becoming the serving algorithm. Parent order, unusual histories and supported Git versions are part of that qualification.

Share nodes only when their attribution contents match. Matching Git content trees alone cannot justify sharing history-dependent attribution. An incremental single-parent update should write changed records and their ancestor index nodes, rather than copy all repository paths. Merges require comparison with all relevant parents; their work is not necessarily proportional only to a first-parent diff. Budget and measure merge work independently.

Use bounded commit-to-attribution-root indexes in immutable artifacts rather than a Cellule SQL row for every file at every commit. Cellule coordinates authoritative repository/catalog facts and, if needed, a bounded descriptor for a published derived index. It does not compute history inside a transaction. Keep attribution replaceable and disposable; it must not delay ref acceptance.

Represent incomplete coverage explicitly. A missing parent index triggers a bounded history fallback or deferred computation, never inheritance from an incomplete record. Start with per-directory cache coverage and measured hot revisions. Add complete historical roots only through admitted backfill. Avoid accumulating a chain of deltas that every directory request must replay.

Derived cache retention has explicit quotas and eviction. It must not keep all old commits alive by accident. If attribution artifacts become durable/shared, register their typed storage ownership and retention under the final artifact/GC design; do not use an unrelated staging namespace or historical receipt as a GC root. Rebuilding from certified Git history remains possible after cache loss.

## Implementation sequence and acceptance

1. Add a typed attribution record and native history helper, with the exact algorithm version above. Differential tests cover both object formats, empty commits, ordered merges, identical parents with different histories, conflict resolution, octopus merges, modes, symlinks, gitlinks, renames, reverts, delete/re-add, raw non-UTF-8 paths and literal pathspec characters. Include generated DAGs and skewed clocks.
2. Add the commit-pinned batch endpoint, byte/count limits, fair read admission, coalesced cache fills and certified-generation ownership. Test access revocation, force pushes, concurrent pagination, cancellation, timeout, worker death and cache eviction. HTTP history work must stay outside Cellule commands.
3. Add asynchronous directory row decoration. Test navigation during pending requests and partial batch completion. Confirm file browsing works while attribution is backlogged.
4. Measure native fallback and warm cache behavior on Tokio, then full Linux, Kubernetes and Chromium histories. Record cold versus warm storage, path count, history depth, merge shape, native CPU/RSS, queue latency and artifact I/O. Do not reuse line-blame benchmarks as evidence for this feature.
5. If native fallback and page caching miss the workload targets, implement and differentially qualify the shared persistent index. Verify bounded update/read amplification, incomplete backfill, process loss, integrity rejection and rebuild after deleting the cache. Persist only validated answers.

The proposed warm attribution batch target is p95 below 100 ms. It is an engineering target, not a current guarantee. Keep the directory listing latency independent of attribution history depth. Report cache hit rate, oldest queued job, cache-fill latency, budget refusals and index lag alongside request percentiles.

Capacity qualification must include 10,000 engineers making 10 commits each in an eight-hour day: 100,000 commits/day, about 3.47 commits/second on average, plus measured bursts. This is not the attribution read rate; model concurrent browsers, pages per session, cache locality and cold misses separately. Require bounded backlog, fair service, stable memory/disk use and foreground push/clone performance while attribution and maintenance run.

## Evidence and remaining work

The local design experiment used Git 2.50.1 and compared 101 present file/directory paths across 17 commit states. It passed for its tested root, empty commit, merge, mode, revert, delete/re-add, rename, octopus and literal-path scenarios. The experiment is finite evidence for the proposed recurrence, not a proof for all Git histories or a Canopy API benchmark.

A separate local Tokio measurement queried 25 root-directory entries at commit `5d5cd8b5b896796445920b3b78c1ad5f9b853fc6` using four bounded native workers with a verified commit graph enabled. Three batch runs took 233.118, 146.671 and 140.715 ms; the median was 146.671 ms. The OS caches were not flushed and the host was shared. These are native fallback measurements, not cache-hit, HTTP, cold-storage or large-team results.

The production helper, endpoint, UI, cache/index and large-team qualification remain to be implemented. The storage cutover's certified readers, ownership and final retention model are prerequisites for serving this feature through the new architecture.
