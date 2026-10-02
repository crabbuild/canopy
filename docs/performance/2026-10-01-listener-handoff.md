# HTTP listener handoff verification

The SHA-256 verification tests now transfer an already-bound HTTP listener into
Canopy startup. This removes their port reservation gap without retries,
serialized tests, relaxed Git assertions or changed runtime budgets. Release
correctness and all eight fresh-RustFS gates passed on the separately frozen
source. This is not a throughput improvement or an upgrade of the
original 10,000-repository corpus.

The [earlier three-node measurements](2026-10-01-old-binary-rustfs-upgrade.md)
remain bound to their original executable. They are not performance results for
this new build.

## Reproduction and correction

At PR head `240203a7`, one Linux Rust job failed with `AddrInUse` in
`sha256_native_merge_candidates_survive_restore_and_publish`; the parallel job
passed. Both later Rust jobs at `0498e22` passed before this correction.
Those passes do not erase the original failure.

The affected helper returned the address of a temporary listener and dropped
the listener before Canopy bound that address. A competing binder inserted in
that gap reproduces the same error. A regression that retains the reservation
failed in all three pre-fix runs. The exact competing binder in the original
Linux job was not captured.

```text
Before: select port -> drop listener -> unreserved gap -> bind again
                                      another test may take the port

After:  bind listener -> transfer ownership -> supervised startup -> serve
                           listener remains bound throughout
```

`CanopyServer::start_with_listener` uses the existing startup and shutdown
supervisor. It requires the listener's actual address to equal `config.listen`
before creating a workspace or writing storage. The public URL can still name
a proxy. Ordinary `CanopyServer::start` retains its existing bind path, readiness
checks and drain behavior; SSH configuration is unchanged.

An embedding caller can reserve its HTTP address as follows:

```rust
let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
config.listen = listener.local_addr()?;
config.public_url = format!("http://{}", config.listen);
let server = CanopyServer::start_with_listener(config, store, listener).await?;
// Use the advertised clone URL only after startup returns readiness.
server.shutdown().await?;
```

All six startup addresses in the SHA-256 integration tests use owned listeners,
including both provider tests. Other tests still use the legacy address helper;
this correction does not claim to eliminate every test-suite port race.

## Closed checks

The tested source is `3dda2b47b1cba105642a62b4ad27d7c84d0940d5`, with the same
Cellule revision `0dc04a658bd99668936f7ec58032d054f6fbc141` and unchanged lockfile.

| Check | Result and boundary |
| --- | --- |
| Pre-fix reservation regression | Three failures with `AddrInUse`; failed executable and exact source retained |
| Handoff regressions | All four passed: real advertised clone URL, competing binder refusal, mismatch before writes and unpolled cancellation |
| Admitted startup cancellation | Workspace and listener remain held until cleanup settles; subsequent rebind and repository creation passed |
| Repetitions | All four handoff tests and admitted cancellation passed in each of 20 independent runs; 100 checks, not throughput samples |
| Lifecycle suite | All 14 passed, including both bind paths rejecting ignored conditional writes before enrollment |
| SHA-256 suite | All three ordinary tests passed; includes all three native merge strategies, exact candidate restoration, stock Git clones and strict fsck |
| Release workspace | 244 top-level tests passed, zero failed, nine ignored; nested subprocess tests counted once |
| Release build and lints | Locked release executable built; all-target Clippy passed with warnings denied |
| Python harness | All 84 tests passed on the same frozen source |
| Fresh RustFS compatibility | All eight exact gates passed; disposable fresh fixture, not a retained-store or five-GiB gate |

The full workspace preserved the SSH-LFS grant's real 300-second expiry check.
Release verification closed October 2 at 01:15:37 UTC, October 1 in the local
Pacific timezone. No runtime diagnostic logging was added.
Provider verification closed at 01:18:09 UTC. The original performance
provider's container, volume, image, configuration, start time and restart count
were identical before and after these gates. The disposable gate fixture was
removed by its normal cleanup.

Reproduce the local checks:

```sh
cargo test --release --locked --test multi_server listener_handoff:: -- --test-threads=4
cargo test --release --locked --test multi_server lifecycle:: -- --test-threads=4
cargo test --release --locked --test multi_server sha256:: -- --test-threads=4
cargo test --release --locked --workspace -- --test-threads=4
cargo clippy --release --locked --workspace --all-targets -- -D warnings
python3 -B -m unittest discover -s scripts -p 'test_*.py'
python3 -B scripts/qualify_size.py --provider-only --release
```

## Artifact bindings and remaining work

Evidence is retained at
`/Users/haipingfu/.codex/canopy-listener-handoff-I4OJLd`. The failed source,
executable and original CI log were copied and independently reread at
`/Volumes/Workspace/CrabData/canopy-listener-red-8_2n504v`; the 330-file release
copy is at `/Volumes/Workspace/CrabData/canopy-listener-green-hreldv3h`.
All 695 closed files, including provider, Python and repetition receipts, were
copied and independently reread at
`/Volumes/Workspace/CrabData/canopy-listener-closed-omgsp1wu` before publication.
These are different local filesystems, not off-machine backups.

| Artifact | SHA-256 |
| --- | --- |
| Pre-fix receipt | `fe23bd6d351a847f29b43ff8ed00c6902b86374b976c8cda52e72c31482f0f76` |
| New release executable | `a61ef0f2cb977348e4e4fc45330334a1f8fd68bf8c44146cd9343b0e6676a6c8` |
| Release verification receipt | `85845cf11b2977136904b6095d5be55ec628818ab220e5386e69efc1de26470b` |
| Release workspace log | `6b78ae474033ba44a7aa9a8662662364ca9ca1733bd9af2b3de7dd01959299dc` |
| Repetition receipt | `72b82bf3aaf667c7206c013fe8252f27941c0b1f5c0c60c66c867eee56e2dfb7` |
| Python receipt | `2f519343688f3e9055ad48bc0060d7e2088551935ef706e22b287617a397a23e` |
| RustFS receipt | `b5a1baa082d8178a2ceedcb589edb0c2aa542d0cea95ea550989549ab23216b2` |
| RustFS log | `3ed9009bb25bd7fb348eec0bec10c729cae3722976479cac24329f5ae0c2ba3b` |

Both [Linux PR CI](https://github.com/crabbuild/canopy/actions/runs/36950769630)
and [push CI](https://github.com/crabbuild/canopy/actions/runs/36950765768) at
`eaebfc4cdd7ead54a8d62978aebcd8656f0de0b8` passed Rust, real RustFS compatibility,
server build and Python harness checks. PR #18 remains draft. The
[full campaign](../performance-plan.md) still requires original-corpus new-release
admission, full remote Git/LFS verification, all 108 load
windows, every acknowledged write after owner loss, higher admission profiles,
matched comparisons, non-sparse five-GiB transfers and isolated Linux capacity.
The original provider's read-only status at this qualification had zero advertised
writers and 301 unsettled Cells; neither its release nor its data was changed here.
The later [maintenance recovery](2026-10-01-original-corpus-recovery.md) settled
those Cells without changing catalog identities or published roots. It remains
in old Maintenance; that later action is not a new listener-build performance result.
