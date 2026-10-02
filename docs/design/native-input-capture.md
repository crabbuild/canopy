# Native request input capture

GitHttpBackend::run_native_receive configures receive.unpackLimit=0 so even small staged receives leave native pack/index pairs. It reuses the existing bounded CGI response collector and native process owner. Production producer conversion to this API remains open. Git remains responsible for receive protocol, thin-pack completion and private ref updates. Receiving successfully is preparation; the durable catalog/ref/response transaction remains the acknowledgement boundary.

## Admitted capture

GitHttpBackend::stage_native_packs runs inside a StagingTicket producer after the native receive has drained. It takes the service's StagingContext, ArtifactStore and existing PhysicalLimits. A fresh context token supplies the repository, creating artifact namespace and object format. Repository/format mismatches reject before provider access. The creating namespace is never chosen by a client filename or content digest.

Capture reuses the private GitCache and its disk reservation. It serializes cache selection, reconciles completed native disk writes, then takes an exclusive lock on the same native worker fence used by every Git command. An active native worker causes a refusal rather than concurrent file mutation. The blocking scan retains both the cache and a node read claim; hashing uses fixed 64 KiB buffers.

Only ordinary pack/info object directories are permitted. Loose or quarantine object directories, nonregular pack/index files, inconsistent names, corrupt index/pack hashes and configured size excess reject. At most 32 input pairs may be captured from one request workspace; this is a request inventory bound, not a repository pack-count limit. PhysicalLimits supplies pack/index byte ceilings, and the node DiskBudget accounts for the existing private files. Capture builds no decoded-body vector or complete OID inventory.

NativePackDescriptor's local inspector reuses the same streaming pack checksum/BLAKE3 scan as verify_files. Local descriptors remain inside the capture service with unset manifest digests. Each authenticated upload fills the actual manifest descriptor before the input can escape. Pack/index keys reuse the repository/creating-operation/pack-digest layout. All pairs are checked locally before transfer; a descriptor vector is returned only after every pair upload and a final live-custody check. A failed later upload can leave private artifacts in the retained creating namespace, but cannot return a partially complete input set or publish refs.

## Cancellation and verification

The existing pinned-file uploader now serves native files as well as immutable metadata. Every queued open/read owns an InputFile, which retains the cache and exclusive fence. Canceling an observer or upload cannot release the cache reservation while its blocking file work still runs. The fence closes before the final cache owner drops, allowing normal cleanup after drain. The staging coordinator separately owns producer execution and completed typed results through single handoff.

Capture establishes authenticated physical bytes, native index validity, header/count agreement and whole pack/index checksums. It does not establish decoded-object CRC validity, self-contained delta decoding, canonical metadata, graph closure or ref authority. PhysicalVerifier must independently reopen the downloaded pair without alternates, decode every physical ordinal and finish its complete metadata partition. CatalogPreparation then checks canonical identity and typed closure against the bound certified base. CompleteCatalogPush rechecks current authority, policy, refs and catalog CAS while saving the exact native response in that same transaction.

## Executable evidence and remaining scope

The composition test submits an actual receive-pack CGI request for both OID formats, captures its resulting native pair through StagingCoordinator, persists/reads its authenticated input checkpoint, deletes the receive/source caches, independently downloads/verifies it, binds staging, assembles the catalog and invokes CompleteCatalogPush. A fresh CatalogReader selects the source from the committed root after preparation scratch has drained. A new native cache downloads that source, and stock Git clones and fscks it. The fresh Cell has no legacy objects/object_edges/object_closure/git_packs tables. The test supplies an authorized fixture owner and orchestrates the steps directly; production HTTP/SSH authentication and orchestration are not implemented by this test.

Failure checks exercise wrong repository, configured size excess, loose inputs, physical byte corruption, active-worker exclusion and exact repeated upload. A queued-upload cancellation test verifies retained disk/cache ownership and exclusion until the queued worker releases its fence.

Production producer/reader conversion, request preflight, gzip/signed/options bindings, production integration of [input checkpoints/adoption](native-input-checkpoint.md) and durable wire-plan/response recovery, continuous frontier/maintenance orchestration, whole-operation file/I/O/OS containment and full-history capacity campaigns remain required. No online provider deletion is authorized by this capture API. The fixture's memory provider and small history do not qualify remote durability or 10,000-engineer throughput.
