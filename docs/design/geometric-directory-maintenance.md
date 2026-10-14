# Geometric directory maintenance

`CompactionPlanner::prepare_next` selects one bounded verified directory replacement from a query-derived preparation base. It reuses `DirectorySnapshot`, `NodeRef` summaries, the logical/physical [coverage contract](directory-run-coverage.md), the range builder/partitioner, and command 22/query 23. It adds no authoritative tables, per-object state or compatibility layer. Physical native pack rewriting and production continuous scheduling remain separate required work.

## Policy and pressure

The default policy starts level 0 at 262,144 logical objects and multiplies its target by four for each successive nonoverlapping level. Target arithmetic saturates at SQLite's maximum signed 64-bit count, preventing overflow from hiding debt. Object counts are exact within a certified disjoint level; overlapping ingress/levels are not summed into a global unique inventory.

`CompactionPolicy::pressure` exposes at most 32 ingress roots and two fixed arrays of 16 level counts/targets. Reading this pressure examines only bounded snapshot summaries. Selecting and preparing a job then authenticates indexed paths and verifies exact inventories. Pressure is advisory and cannot authorize publication or deletion.

Any ingress root remains eligible, including a tail below the urgent watermark. A level is eligible when its logical count exceeds its target; exact equality needs no promotion. The final level has no successor. Pressure above its target returns a capacity error rather than selecting a nonexistent level or reporting a drained backlog. Configure a qualified larger profile or architecture before exceeding that terminal capacity; keep admission bounded.

Profiles require a positive base count within the SQL count range, ratio 2–16, ingress high water 1–32 and urgent burst 1–32. Defaults use eight roots as the urgent watermark and three as the maximum urgent burst. These values are initial policy, not measured optimal settings.

## Fair local selection

There are 16 rotation classes: ingress plus 15 promotable levels. With urgent ingress, dispatch at most three ingress preparations before giving one eligible higher level a turn. Urgent jobs preserve the higher-level rotation position; restarting it at level 0 would starve deeper levels under continuous arrivals. With all 15 levels continuously eligible and successful preparations, every level receives a turn within 60 preparations under the default burst. This is a job-selection bound, not a time, I/O, throughput or publication-progress guarantee.

Ingress selection rotates through the current bounded root slots. Each promotable level keeps one exclusive last-moved OID. Seek after that OID using the existing range cursor, then wrap to the beginning only upon indexed exhaustion. This revisits lower ranges introduced by intervening publications and follows retained suffixes using the moved prefix's last OID, rather than the original physical file's endpoint. Selection retains no full level inventory or history-sized queue.

The planner binds repository/object format after its first successful preparation and rejects reuse in another context. Its fixed-size local cursor is advisory. Resetting it after restart changes traversal order but cannot drop work from the authoritative catalog. Failed preparation and cancellation before completion leave rotation unchanged. Successful private preparation advances it without claiming a durable acknowledgement. The caller must retain the prepared inputs and recover any uncertain publication through the existing exact command/outcome rules.

## Execution contract

1. Begin an admitted maintenance preparation under the current owner fence and open its query-derived base using existing lease/retention APIs.
2. Call `prepare_next` with a service-owned planner, private workspace, shared disk budget and qualified compaction limits. The API checks live deadlines and current admin access, including when it reports no eligible work.
3. The selected source and target window use the existing `PreparedCompaction::prepare_range`. Verify complete source/projection folds, output inventory and exact path replacement. A required physical input outside the resource profile fails admission; selection never expands the configured limit.
4. Use `ready_compaction` to issue the purpose-bound maintenance certificate and retain exact command 22, then submit through the [shared foreground/maintenance dispatcher](shared-publication-dispatch.md) under the selected catalog CAS. On a changed frontier, reuse `reconcile` only while its exact selected source/target incarnation checks hold. Replaced inputs require a new preparation.
5. Keep unknown outcomes and their inputs admitted until authoritative recovery. After a known outcome, release private scratch and obtain a new queried base before the next job. Do not use a local rotation advancement as evidence that bytes were published or can be collected.

The selection API does not run a timer, choose maintenance/foreground CPU/I/O resource shares, provide a durable service outbox, or replace owner-loss reconstruction. Those components must integrate it before production schema selection. An unadmittable required file needs a qualified resource profile; repeatedly selecting it cannot prove service progress.

## Performance and acceptance

Pressure selection is O(16 + 32) time and fixed space. Range seeking uses bounded-height authenticated paths. Per-job input/run/output bounds and the 48-candidate lookup bound remain unchanged. Every source window folds its complete parent projection, so repeated windows can amplify I/O/CPU. Geometric targets organize debt but do not remove that cost.

Tests cover both object formats, invalid/overflowing profiles, final-level capacity, exact-threshold idleness, urgent ingress with every higher level continuously pressured, root rotation, tail ingress, and repository/format separation. Native fixtures repeatedly query, prepare and publish until ingress and geometric debt drain, comparing complete effective canonical/source/version entries, unchanged refs and pinned old-root reads after every job. Admission failure retries the same ingress selection, and an empty selection still rechecks current admin access.

Full-history Kubernetes/Linux/Chromium imports, 100,000 incremental commits, amplification/retention breakdowns, maintenance service exceeding arrivals under foreground load, owner-loss recovery, isolated restore and the large-team working-day/peak/headroom campaigns remain mandatory unpassed gates. This increment is not evidence of capacity for 10,000 engineers.
