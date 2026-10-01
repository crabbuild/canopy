# Local server and Kubernetes repository qualification

Use this profile to evaluate the product locally with real Git clients and an
isolated S3-compatible store. The server and provider bind to loopback. It keeps
its credentials, deployment identities, provider volume and reports across
restarts. It is a development install; the native Canopy process does not inherit
the Linux containment ceilings in [the bounded profile](README.md).

## Start a server

Prerequisites: Rust 1.97+, Git 2.50.1 or another qualified version, Python 3,
Docker and the AWS CLI. Select a writable volume with enough free space. The
large-repository fixture needs room for compressed durable packs, a native Git
cache, SQLite metadata and graph indexes, and client clones. Blob bodies
from verified receive packs retain Git compression in object storage. Structural
objects remain in SQLite; pushes without a retained native pack still use the
separate streaming store for oversized blobs. Reserve extra disk for ingestion and overlapping cache generations.

```sh
export CARGO_TARGET_DIR="$PWD/target"
CARGO_INCREMENTAL=0 cargo build --release --locked --bin canopy
python3 scripts/local_eval.py start \
  --binary "$CARGO_TARGET_DIR/release/canopy" \
  --state-dir "$HOME/canopy-evaluation" \
  --disk-gib 64
```

Open <http://127.0.0.1:18080>. The configured owner is `canopy`. Its bootstrap
credential is `CANOPY_GIT_TOKEN` in the mode-0600 `secrets.json` in the state
directory. The helper never prints credentials. Use that token to sign into the
browser or authenticate Git. The separate signing key and provider credentials
are in the same private file. Keep the state directory outside version control.

The helper pins its RustFS image by digest and creates an isolated Docker volume,
bucket and application prefix. RustFS has a 2 GiB memory ceiling, a two-CPU
bandwidth ceiling and bounded logs. The native Canopy process has three active
repository slots and a 64 GiB disk admission budget by default; those settings
are evaluation choices, not measured production limits. Neither the Docker
volume nor the host process has a hard filesystem limit in this profile.

```sh
python3 scripts/local_eval.py status --state-dir "$HOME/canopy-evaluation"
python3 scripts/local_eval.py stop --state-dir "$HOME/canopy-evaluation"
python3 scripts/local_eval.py start --state-dir "$HOME/canopy-evaluation"
```

Stop drains the node before stopping its provider. Start reuses the same secrets
and deployment identity. Node startup discards its managed runtime workspace and
restores published state from object storage. The helper checks the process
identity before signaling it and rejects a changed executable: preview schema
and release changes require a new state directory and storage prefix. Do not
replace a running executable or edit release identities to bypass that check.
The native node runs independently of the invoking terminal; run `start` after
a host reboot. Only the provider has automatic Docker restart configured.

Inspect `server.log` for Canopy diagnostics. `deployment.json` records the exact
executable hash, provider image, container and volume names. The provider volume
is retained by `stop`; removal is a separate, destructive operation.

## Measure a real repository

Use a clean, complete source checkout or mirror. The benchmark copies objects
without hard links and changes refs only in its independent fixture. Remote
tracking branches become regular branches; a divergent local branch is retained
alongside an `upstream/` branch. It imports only branches and tags, not GitHub's
issues, reviews, CI state or other hosting metadata. Record which refs the source
actually includes: one complete master history does not cover every release
branch and tag.

Use a dedicated node for each concurrent benchmark, with a distinct state
directory and port. The recovery stage stops and restarts that node. Start with
the current tree, then test full history:

```sh
python3 scripts/benchmark_large_repository.py \
  --state-dir "$HOME/canopy-evaluation" \
  --source /path/to/kubernetes \
  --work-dir /path/to/evidence/snapshot \
  --name kubernetes-snapshot --mode snapshot

python3 scripts/benchmark_large_repository.py \
  --state-dir "$HOME/canopy-evaluation" \
  --source /path/to/kubernetes \
  --work-dir /path/to/evidence/full \
  --name kubernetes --mode full
```

Both the work directory and repository name must be new. Snapshot mode creates
one root commit with the source HEAD tree; it cannot establish history capacity.
Full mode preserves reachable history, branches and tags in the source. The
script rejects shallow sources. It imports through stock Git smart HTTP with an
atomic ref update, verifies protocol-v0/v2 mirror clones and strict full `fsck`,
pushes one additional commit, checks incremental fetch and fast-forward pull,
restarts with a fresh managed local workspace,
then compares restored refs and HEAD tree and runs another `fsck`.

`report.json` is checkpointed at stage boundaries, including on failure. It
records revision, binary/provider identity, source tree, reachable object and
commit counts, stage durations and sampled resource peaks. Operation logs and
`resources.jsonl` remain beside it. Process-tree RSS is sampled every five
seconds and excludes the provider, client processes and filesystem cache; it is
not cgroup memory or a complete peak-memory bound. New runs also record SQLite
and WAL sizes and the indexed object insertion high-water mark. Client timeout
is configurable with `--timeout`; it does not change native server deadlines.

Treat a failed or timed-out stage as an incomplete gate. Client cancellation
does not prove that an already admitted durable operation has stopped. Inspect
the server and ref state before stopping it or retrying; partially staged objects
may remain in the provider. A passed run establishes correctness only for the
recorded fixture, host and provider. Add filtered/shallow clones, browsing, pulls,
concurrent users, provider faults and bounded Linux measurements before making a
production capacity claim.

For production work, retain the release gates in [the roadmap](../ROADMAP.md):
schema upgrades, recovery faults, garbage collection, provider qualification,
metrics and alerts, TLS/secret operations and the release artifact pipeline.

## Background Git maintenance

Each resident repository checks its serving cache every 60 seconds. It repacks
when the cache has at least 1,024 loose objects or eight pack files. One maintenance
job runs per process, with one native compression thread and separate admission
from user transfers. It writes and verifies a complete new cache generation,
then publishes it only if the object inventory is still current. Existing clones
and fetches retain their original generation until their streams finish.

The job skips active hydration and retries after concurrent writes. Failure keeps
the old generation available. Shutdown cancels the supervised job and kills its
native process group before repository drain. Server logs record successful
publication, object count, bytes before and after, and elapsed time. Native Git
transfers have a one-hour worker deadline; client disconnection still cancels
streaming workers. Individual provider operations retain their bounded deadlines.

Durable pack/index files are immutable and backed up alongside external Git/LFS
bodies. Recovery verifies artifact hashes and restores complete packs directly.
The serving-cache repack does not delete durable artifacts or repoint SQLite blob
locators. Durable object-store compaction, orphan collection, and schema upgrades
remain separate production work. Preview schema changes require a fresh prefix.
