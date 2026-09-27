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
  "local_disk_limit_bytes": 1610612736,
  "max_active_repositories": 3
}
```

The three-repository active limit preserves this profile's existing qualification
scope. Larger active sets need their own memory, descriptor and disk measurements;
raise `max_active_repositories` only within that measured deployment envelope.
Stored repository count can exceed this limit through eviction and restore.

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
