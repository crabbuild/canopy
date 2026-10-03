# Bounded Linux deployment

This profile runs one Canopy node with its own Repository Cells and native Git
workers inside one Linux cgroup. The object store remains the durable authority.
It is a deployment boundary; running the binary directly does not inherit it.
Use a cgroup v2 Docker Engine. Check the actual limits before admitting traffic.

| Resource | Enforced ceiling |
| --- | --- |
| Entire node, Git descendants and tmpfs memory | 4 GiB, no swap |
| CPU bandwidth | Two CPUs per scheduling period |
| Processes and threads | 256 |
| File descriptors per process | 16,384 soft and hard |
| Local SQLite, packs, loose objects, request spools and native temporary files | One 2 GiB tmpfs |
| Shared memory | 16 MiB, also charged to the memory cgroup |
| Container logs | Local driver, three 10 MiB files |

The root filesystem is read-only and the service has no writable host volume.
All application temporary files use `/var/lib/canopy`; native Git further narrows
temporary paths to its disposable cache. The state mount must permit execution
because Canopy generates the receive update hook there. Repository-supplied hooks
and host Git configuration are never installed in the cache.

These are hard capacity ceilings, not throughput promises. tmpfs pages consume
that same 4 GiB memory allowance. Full scratch space returns an I/O failure;
exhausted memory can kill a Git worker or the node. A killed node must recover
through the existing Cell lease/fencing protocol. Reads or writes may fail while
capacity is exhausted. Do not disable the OOM killer or mount an unbounded
writable directory to work around these limits.

## Build the image

Build a Linux binary with Rust 1.97 or newer, preserving the pinned Cargo.lock.
Verify the workspace volume is mounted and writable before creating the target
directory. Keep Cargo output in a dedicated directory on that volume.
On a Linux build host (adjust the checkout-specific suffix):

```sh
export CARGO_TARGET_DIR="$HOME/Workspace/crabbuild-target/canopy-container-main"
mkdir -p "$CARGO_TARGET_DIR"
CARGO_INCREMENTAL=0 cargo build --release --locked --bin canopy
# Send only the runtime Dockerfile and executable to the image builder.
tar -C deploy -cf - Dockerfile -C "$CARGO_TARGET_DIR/release" canopy |
  docker build --tag canopy:local -
```

The binary architecture must match the image platform. On a cross-build host,
use the corresponding Rust target and C compiler; package the executable from
`$CARGO_TARGET_DIR/<target>/release`. The Debian base image is pinned by digest;
Git, curl and CA certificates use its supported package repositories. Record the
resulting image digest and resolved package versions for each release. Rebuilding
later can incorporate distro package updates; the Dockerfile alone is not a
bit-for-bit reproducibility guarantee.

## Configure and start

Create `deploy/config.json` from the root `config.example.json`. Choose the
storage prefix, tenant/application/node identities, release image identity and
public/peer URLs for this deployment. Set these container-specific values:

```json
{
  "listen": "0.0.0.0:8080",
  "data_dir": "/var/lib/canopy",
  "native_limits": {
    "total": {
      "processes": 16,
      "cpu_units": 16,
      "memory_bytes": 2147483648,
      "descriptors": 1024
    },
    "maintenance_reserved": {
      "processes": 2,
      "cpu_units": 5,
      "memory_bytes": 805306368,
      "descriptors": 128
    },
    "read": {
      "processes": 1,
      "cpu_units": 1,
      "memory_bytes": 134217728,
      "descriptors": 32
    },
    "pack": {
      "processes": 1,
      "cpu_units": 4,
      "memory_bytes": 536870912,
      "descriptors": 64
    }
  },
  "local_disk_limit_bytes": 1610612736,
  "max_active_repositories": 3
}
```

The three-repository active limit preserves this profile's existing qualification
scope. Larger active sets need their own memory, descriptor and disk measurements;
raise `max_active_repositories` only within that measured deployment envelope.
Stored repository count can exceed this limit through eviction and restore.
The checker budgets eight descriptors per active Cell (including Directory),
plus 1,024 for sockets, Git and other I/O, against the explicit process limit.
This reservation is a sizing floor; actual descriptor demand still needs sampling.
The limit applies to each process, not to aggregate descriptors across the cgroup.
Do not depend on Docker's inherited default: a 1,024-descriptor limit exhausted
during the initial Linux density seed, long before memory was full.

The 1.5 GiB application admission limit leaves filesystem headroom for native
writes; the 2 GiB filesystem, including transient native writes, is the final
boundary. Provision storage and identities as described in the root README.
Each node needs a distinct node identity/signing key and its own tmpfs. Configure
a TLS ingress before exposing public Git endpoints; the example publishes only
on host loopback. Multi-node peer RPC also requires the documented HTTPS ingress.

Create a private, gitignored `deploy/.env` containing `CANOPY_GIT_TOKEN`,
`CANOPY_NODE_SIGNING_KEY_HEX` and the provider credentials/environment required by
`storage_url`. Do not put provider credentials in image build arguments or layers.
The native Git environment strips them before spawning workers.

```sh
docker compose -f deploy/compose.yaml up -d
python3 scripts/check_container.py "$(docker compose -f deploy/compose.yaml ps -q canopy)"
curl --fail http://127.0.0.1:8080/readyz
```

The checker reads effective kernel cgroup values, actual tmpfs capacity, mounts
and application configuration; it prints only resource limits. Failure means
this deployment has not established the documented containment boundary.
Container health is separate from containment and from full service readiness.

`restart: on-failure` restarts a crashed node. Graceful maintenance exits cleanly
and stays stopped. tmpfs is discarded on container stop/restart; the next process
restores published Cell state from object storage. Keep the configuration and
signing key stable; replacing a container never authorizes deletion of leases,
control objects or provider data. All shutdown, takeover and backup rules in the
root README still apply. A 2-minute stop grace allows normal drain; if exceeded,
Docker kills the process and the ordinary crash-recovery path applies.

## Run the process qualification

Use an existing disposable S3-compatible bucket and credentials in the caller's
environment. `AWS_ENDPOINT`, when supplied, must resolve from inside the Canopy
container. An existing Docker network can connect Canopy to a local test provider.
The script creates a unique child of the supplied prefix; object-store test data
remains there for inspection and caller-controlled cleanup.

```sh
CARGO_TARGET_DIR="$HOME/Workspace/crabbuild-target/canopy-container-main" \
  python3 -B scripts/smoke_container.py \
  --image canopy:local \
  --storage-url s3://disposable-bucket/container-proof \
  --network test-provider-network
```

Supply `CANOPY_NODE_SIGNING_KEY_HEX` and provider environment as for the service.
Omit `--network` when the provider is reachable through the default Docker
network. The script materializes the checked-in Compose profile, verifies its
kernel limits, tests stock Git and LFS, induces native unpack disk pressure,
then kills/recreates the node and retries against recovered Cells. Only its own
containers/network and temporary client/configuration files are removed. The
smaller pressure profile is a test fixture, not a recommended deployment size.

## Scope

The profile bounds one Linux node and descendants, including their aggregate
native scratch writes. It does not bound the object-store provider, network
transfer charges, the Docker daemon, or the sum of several nodes. Size host
capacity for all scheduled nodes. Docker/cgroup v1, other operating systems,
per-account durable quotas and the complete crash/fault matrix remain separate
delivery gates.

## Measure repository density under these limits

`scripts/benchmark_container.py` uses this same profile with an explicit active
repository count. It disables automatic restart so an unexpected exit remains a
failed measurement. Use an image built from the intended source revision; add
`--label org.opencontainers.image.revision=<commit>` to the image build command.
The report records that label, image ID and the executable's SHA-256 separately.

```sh
CARGO_TARGET_DIR="$HOME/Workspace/crabbuild-target/canopy-container-main" \
  python3 -B scripts/benchmark_container.py \
  --image canopy:local --network test-provider-network \
  --storage-url s3://disposable-bucket/density-proof \
  --provider-description 'Provider version and network placement' \
  --output "$HOME/Workspace/crabbuild-target/canopy-container-main/density-run-1" \
  --repositories 1000 --active-repositories 1000 --recover
```

Supply the same signing key and provider environment as the process smoke.
The output directory must be new and inside the checkout's existing external
target directory. The caller retains responsibility for cleaning the unique
object-store prefix recorded in `environment.json`. Client repositories, logs,
the corpus and reports remain available after success or failure.

The default corpus has three one-commit repositories and 997 empty repositories.
After seeding and a 15-second idle interval, the driver measures metadata for
three prewarmed repositories, Git v2 discovery, then uniform metadata arrivals
across the entire corpus. `--rate` and `--duration` control that last workload;
defaults are 10 requests/s for 120 seconds. A repository count larger than the
active limit deliberately measures activation/eviction pressure. Git caches are
not explicitly prewarmed. No failed read is retried.

Every five seconds the sampler records cgroup memory, CPU usage/throttling,
process/thread count, aggregate process descriptors and tmpfs usage. The kernel's
`memory.peak` covers missed memory peaks; descriptor and scratch peaks are only
sampled. Sampling spawns a small process inside the measured cgroup and adds
overhead. Memory includes Git descendants and tmpfs, unlike parent RSS. The
sampler stops before crash recovery; recovery has a separate final snapshot.
The idle interval measures resource use, not provider request counts.

`--recover` kills the owner, waits for lease expiry, recreates the container
with fresh tmpfs, verifies every repository identity, and clones all populated
samples with stock Git v0/v2, exact hashes and strict fsck. Read failures do not
skip this proof or graceful shutdown. `outcome.json` reports these results
independently. Sampling errors, OOM evidence, failed reads, failed recovery or
unclean shutdown fail the harness. Latency percentiles remain reported values;
the harness does not turn a zero-error run into an SLO or throughput claim.

This is a bounded Linux tmpfs baseline. It does not qualify the proposed
NVMe-backed reference node, large histories, LFS throughput, all runtime
primitives, a separate load-generator host, or production object-store latency.
