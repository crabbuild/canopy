# Canopy architecture diagrams

Read the root [Canopy design document](../../DESIGN.md) for system goals, Cellule integration, authority boundaries, request and control flows, resource ownership, security, failure handling and design tradeoffs. It is the source of the gallery's detailed explanations.

Open the [browsable gallery](index.html) to inspect the nine diagrams. Each diagram has a standalone SVG and a PNG at twice its logical resolution. Diagrams 01–08 describe the inspected serving architecture; diagram 09 distinguishes implemented packed-storage primitives from incomplete production integration and proposed repository capabilities. See [design scope and implementation status](../../DESIGN.md#design-scope-and-implementation-status).

| Diagram | What it explains | SVG | PNG |
| --- | --- | --- | --- |
| 01 | Public clients, node components and storage authority | [System overview](01-system-overview.svg) | [PNG](01-system-overview@2x.png) |
| 02 | How Canopy models Directory and Repository Cells on Cellule | [Framework mapping](02-canopy-on-cellule.svg) | [PNG](02-canopy-on-cellule@2x.png) |
| 03 | Commands, SQLite, LTX and the durable acknowledgement boundary | [Durable command](03-durable-command.svg) | [PNG](03-durable-command@2x.png) |
| 04 | Authentication, residency, local routing and signed peer routing | [Request routing](04-request-routing.svg) | [PNG](04-request-routing@2x.png) |
| 05 | Push preparation, object ingestion, policy checks and exact replay | [Git push](05-git-push.svg) | [PNG](05-git-push@2x.png) |
| 06 | Fetch, browse, LFS and current physical storage choices | [Reads and LFS](06-read-and-lfs.svg) | [PNG](06-read-and-lfs@2x.png) |
| 07 | Startup, leases, takeover, exact restore, drain and backup | [Owner recovery](07-owner-recovery.svg) | [PNG](07-owner-recovery@2x.png) |
| 08 | Repository features and merge publication controls | [Domain and policy](08-domain-and-policy.svg) | [PNG](08-domain-and-policy@2x.png) |
| 09 | Packed catalog primitives, remaining integration and capability proposals | [Storage evolution](09-packed-storage-evolution.svg) | [PNG](09-packed-storage-evolution@2x.png) |

## Regenerate the diagrams and gallery

Run from the repository root:

```sh
python3 diagram/canopy-architecture/generate.py
python3 diagram/canopy-architecture/render_pngs.py
python3 diagram/canopy-architecture/build_gallery.py
```

[generate.py](generate.py) owns SVG layouts, labels and source maps. [render_pngs.py](render_pngs.py) requires `rsvg-convert` on `PATH`. [build_gallery.py](build_gallery.py) embeds the SVGs and matching sections from [DESIGN.md](../../DESIGN.md); it links repository files relative to the gallery. The generated gallery works offline, with network access needed only for external source links.

Sequence diagrams draw activation bars behind dashed lifelines and message arrows. When updating the design, keep the diagram labels, source snapshot, implementation boundaries and generated gallery consistent. See [verification and change obligations](../../DESIGN.md#verification-and-change-obligations).
