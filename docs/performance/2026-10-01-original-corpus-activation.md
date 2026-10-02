# Original corpus release activation

The original RustFS corpus passed full new-release admission and controlled
activation without changing any of its **10,003 Cell Controls, catalog entries
or published roots**. The qualified executable uses Cellule `0dc04a6`; its
selected release reached **Ready revision 9**. This closes the release-admission
gate after [same-code maintenance recovery](2026-10-01-original-corpus-recovery.md).
It does not establish remote Git/LFS content recovery, throughput or capacity.

## Activation sequence

```text
Old Maintenance 5: all 10,003 Cells idle and unowned
  |
  +-- read-only admission: actual old descriptor + new rolling compatibility
  |                       all 256 shards and actual Control code/schema
  |                       two unchanged complete snapshots
  |
  +-- exact old CLI ends maintenance --> Old Ready 6
  |
  +-- new SDK prepares release ------> Prepared 7
  +-- new SDK begins activation -----> Activating 8
  +-- new SDK completes activation --> New Ready 9
                                       |
                                       +-- preserve and reread closed evidence
                                       +-- three fresh gateways and proxy ready
                                       +-- remote Git/LFS verification still open
```

The activation controller repeats two complete scans at each of Ready 6,
Prepared 7, Activating 8 and Ready 9. Every snapshot matches the admitted catalog,
canonical Controls, ETags and service root. No serving owner is admitted during
these scans. The later gateway launch is a separate step, not part of this
unchanged-metadata claim.

## Write boundary

The fixture-scoped controller uses the existing Cellule release APIs, wrapped
by a transport gate. It arms only after checking the exact old Ready record and
two stable complete snapshots. While armed, it permits only:

- Creation of the exact new descriptor with its canonical bytes.
- Conditional updates of the exact release key to the three expected records:
  Prepared 7, Activating 8 and Ready 9.

All Control, catalog, node, data, delete, copy and multipart writes remain
refused. The gate disarms after activation. It forwarded **four writes**, with
zero refused attempts during activation. The exact old CLI's preceding
maintenance-end transition is separate from that four-write count.

Three unit tests cover read-only refusal, exact armed paths/bytes/write modes,
and real ReleaseStore transitions with canonical readback. Locked offline
metadata, release build and all-target Clippy passed. The earlier test attempt
that incorrectly listed an exact leaf as a directory prefix is preserved; its
SDK transitions had passed, and the corrected test verifies the exact record.
The controller is a retained qualification tool, not a general upgrade command.

## Closed results

| Check | Result |
| --- | --- |
| Corpus admission | All 256 shards and 10,003 Cells; actual predecessor descriptor and every catalog/Control code/schema supported |
| Admission snapshots | Two identical scans; zero owners, advertisements, unsettled Cells or attempted provider writes |
| Old maintenance end | Exact retained executable exited zero; old Ready revision 6, zero advertisements and unsettled Cells |
| Activation | SDK transitions 7 → 8 → 9; exact new descriptor and selected image verified |
| Activation snapshots | Eight complete scans; all canonical Controls, ETags, catalog and published roots unchanged |
| Provider | Container, volume, image, configuration, start time, resource envelope and restart count unchanged |
| Evidence | 369 closed files and exact inputs copied and independently reread on another local filesystem |

Admission closed October 2 at 02:25:04 UTC; activation closed at 02:32:05 UTC
(October 1 Pacific). The controller took 225.749 seconds including complete
rescans. That is activation wall time, not Git request latency.

## Artifact bindings

| Artifact | SHA-256 |
| --- | --- |
| Qualified Canopy executable | `a61ef0f2cb977348e4e4fc45330334a1f8fd68bf8c44146cd9343b0e6676a6c8` |
| Scoped activation controller | `d5ebef17d975a349bb6db250e2154198dbda53a273ad1078db1958e8b819373f` |
| Admission receipt | `39cdcfce11fdc4e2261beaa51b4640a2321756b014d04887082d2a57ca614fe5` |
| Activation receipt | `316ef6b8449b3a41c09d41dd2578474ad77b1f17147d06ab14be45f035d66981` |
| Backup manifest | `03ca94a54b7683b32c717bc0fbd9b5839f1d0f9f705e696431b79c7d387294e8` |

The Canopy source is `3dda2b47b1cba105642a62b4ad27d7c84d0940d5`, independently
qualified in the [listener handoff checkpoint](2026-10-01-listener-handoff.md).
The selected release is
`9a8df7ae5feba1f1760a843bc88d7870af4ebf515b8bc48e45bb7433569cd5d0`;
activation operation is `d36626b9-361b-47dc-a43a-3e14f9bd1a7d`.

Receipts and exact tools are retained at
`/Users/haipingfu/.codex/canopy-original-corpus-admission-YxGodu`.
The verified copy is
`/Volumes/Workspace/CrabData/canopy-original-upgrade-activation-7ze24cer`.
This is not an off-machine or provider-data backup. No runtime budget was raised,
owner erased, Control force-CAS applied, catalog rewritten, corpus reseeded or
provider restarted. The UI preview and unrelated providers remain unchanged.

## Remaining verification

Three fresh gateways reported ready behind the proxy at 02:35:59 UTC with the
exact qualified executable and 100 active repositories per node. Readiness is
not a full remote-content result. Full verification of 10,000 identities,
100 populated Git/LFS fixtures and the two critical repositories remains open,
as does fresh-owner recovery of every acknowledged write.

Cellule upstream subsequently advanced by two commits to
`191409685b001a82bd02780def45102b4fc2f164`, observed when publishing this checkpoint.
Those commits change runtime forwarding/compaction and peer HTTP CI gates.
They are **not** the dependency revision tested here and require independent
qualification; no performance result transfers to them.

PR #18 remains draft. The [full campaign](../performance-plan.md) still requires
108 windows, 114,960 arrivals and 8,640 scheduled seconds, concurrent critical
operations and faults, higher admission profiles, matched comparisons, large
transfers and isolated Linux capacity. The earlier diagnostic lease-fencing
failure remains unexplained; successful activation does not establish its cause.
