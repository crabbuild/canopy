# Recover supported retained contracts during maintenance

Maintenance recovery now accepts a Cell contract explicitly supported by the
compiled registry, including a declared predecessor. It no longer requires that
an unsettled catalog entry name the current module contract.

This is a recovery admission correction, not a migration or performance result.
The follow-up starts from main `877dc33` and keeps the existing Cellule
`0f4ca0919b0dfe20a3dcd964d21da03135e42eed` dependency and lockfile unchanged.

## What changes

The previous `is_current_cell` check rejected a supported predecessor before
the maintenance worker could restore it. `supports_cell` checks the complete
namespace, role, code and schema contract against the registry's declarations.

```mermaid
sequenceDiagram
    participant M as Maintenance worker
    participant C as Catalog
    participant R as Compiled registry
    participant W as Cell acquisition
    M->>C: Read existing unsettled catalog proof
    M->>R: Is this namespace/role/code/schema supported?
    alt Declared current or retained contract
        R-->>M: Supported
        M->>W: Acquire using the existing proof
        W->>W: Validate Control and verify published root
        W-->>M: Restored handle
        M->>W: Drain before removing local scratch
    else Unknown contract
        R-->>M: Refuse
        M->>M: Return error; remain in Maintenance
    end
```

No lease, ownership, resource limit or publication rule changes. Recovery still
requires the exact active maintenance operation and selected executable image.
It does not finish maintenance or enable user traffic automatically.

## Regression coverage

| Fixture | Required result |
| --- | --- |
| Unpublished retained Directory | Recovery publishes an idle root without changing the catalog contract |
| Published retained Directory | The published root and independently restored marker bytes survive recovery |
| Same descriptor, different selected image | The old image is refused; the selected image recovers the retained contract |
| Unknown namespace, role, code or schema | Refusal before Control initialization; catalog remains unchanged and maintenance stays unfinished |

These are owned in-memory fixtures exercising the real maintenance worker.
They are not old-binary RustFS recovery or proof that the deleted original
10,000-repository corpus and its acknowledged writes survived.

Run the focused release regressions and the complete correctness gates:

```sh
cargo test --release --locked -p canopy-server --lib deployment::tests::retained_maintenance
cargo fmt --all -- --check
cargo clippy --release --workspace --all-targets --locked -- -D warnings
cargo test --release --workspace --locked -- --test-threads=4
python3 -B -m unittest discover -s scripts -p 'test_*.py'
python3 scripts/qualify_size.py --provider-only --release
```

## Separate corpus rebuild remains incomplete

After disk reclamation, a fresh 10K/100 corpus rebuild was authorized. Its
pre-seed native qualification used Canopy `64db462` and Cellule `0f4ca09`,
not this follow-up or the later Git-pack merge.

Formatting, locked dependency identity and release lints passed. The
multi-server release suite closed with **103 passed, 1 failed, 9 ignored**.
`bulk_mirror_publication_is_atomic_and_survives_restart` failed during a
4096-ref mirror push after the server logged `Http(Timeout)` before publication.
This is the observed failure, not a confirmed root-cause diagnosis.

The qualification exited with code 101. Both continuation helpers refused to
advance: no new corpus provider, fleet, seed or performance windows were started.
The captured failure is retained; the rebuild is not a successful recovery,
throughput measurement or capacity qualification.

A separate helper-only regression corrected an outdated Cargo test-log path
matcher (`tests/multi_server.rs` versus `tests/multi_server/main.rs`). Its offline
checks passed, but that correction does not resolve the failed bulk-mirror gate.
