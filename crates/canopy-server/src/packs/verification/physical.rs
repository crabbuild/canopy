//! Isolated verification of authenticated native bytes and exact decoded shard
//! coverage. Global typed closure and fenced publication are separate stages.
use super::super::{
    directory::index::IndexError,
    metadata::{
        MetadataBuilder, MetadataError, MetadataLimits, MetadataSegment, PAGE_OBJECTS,
        SegmentDescriptor, SegmentIdentity,
    },
    sources::{NativePackDescriptor, VerifiedPackBinding},
};
use super::*;
use crate::{
    git_cache::{CacheError, GitCache},
    git_http::{GitHttpError, GitProcess, WORKER_DEADLINE, read_bounded},
};
use canopy_object_storage::{artifact::ArtifactStore, external::MAX_ARTIFACT_BYTES};
use cellule_ltx::DiskBudget;
use std::{path::PathBuf, process::Stdio, sync::Arc, time::Duration};

mod partition;
pub use partition::PhysicalPartition;

#[derive(Clone, Copy, Debug)]
pub struct PhysicalLimits {
    pub max_pack_bytes: u64,
    pub max_index_bytes: u64,
    pub max_edge_bytes: u64,
    pub metadata: MetadataLimits,
    /// Deadline for native index validation and actor shutdown. The service
    /// separately owns the whole-operation deadline and native process quotas.
    pub native_timeout: Duration,
}
impl Default for PhysicalLimits {
    fn default() -> Self {
        Self {
            max_pack_bytes: MAX_ARTIFACT_BYTES,
            max_index_bytes: MAX_ARTIFACT_BYTES,
            max_edge_bytes: 256 << 20,
            metadata: MetadataLimits::default(),
            native_timeout: WORKER_DEADLINE,
        }
    }
}
#[derive(Debug, thiserror::Error)]
pub enum PhysicalError {
    #[error("isolated native workspace failed")]
    Cache(#[from] CacheError),
    #[error("physical artifact binding failed")]
    Binding(#[from] IndexError),
    #[error("physical metadata assembly failed")]
    Metadata(#[from] MetadataError),
    #[error("canonical decoded inspection failed")]
    Object(#[from] ObjectReadError),
    #[error("native physical validation failed")]
    Native(#[from] GitHttpError),
    #[error("physical verification I/O failed")]
    Io(#[from] std::io::Error),
    #[error("physical verification task failed")]
    Task(#[from] tokio::task::JoinError),
    #[error("physical verification is incomplete, canceled or inconsistent")]
    Integrity,
    #[error("physical verification exceeds its admitted limits")]
    Limit,
}

/// Only successful complete isolated verification constructs this value. It
/// binds every decoded native ordinal to an exact sealed metadata partition.
/// It is an in-process witness, not a persisted publication certificate.
pub struct PhysicalPackWitness {
    store: ArtifactStore,
    native: NativePackDescriptor,
    shard_count: u32,
    metadata_digest: [u8; 32],
}
impl PhysicalPackWitness {
    pub fn verify_store(&self, store: &ArtifactStore) -> Result<(), PhysicalError> {
        if self.store.same_binding(store) {
            Ok(())
        } else {
            Err(PhysicalError::Integrity)
        }
    }
    pub fn native(&self) -> NativePackDescriptor {
        self.native
    }
    pub fn shard_count(&self) -> u32 {
        self.shard_count
    }
    pub fn metadata_digest(&self) -> [u8; 32] {
        self.metadata_digest
    }
    /// A bounded checker for metadata loading/copying one shard at a time.
    pub fn partition(&self) -> PhysicalPartition {
        PhysicalPartition::new(self.native, self.shard_count, self.metadata_digest)
    }
    /// Compare the exact ordered metadata descriptors when assembling the
    /// catalog. Merely matching a pack's object count is insufficient.
    pub fn verify_segments(
        &self,
        segments: impl IntoIterator<Item = SegmentDescriptor>,
    ) -> Result<(), PhysicalError> {
        let mut partition = self.partition();
        for segment in segments {
            partition.add(segment)?;
        }
        partition.finish()
    }
}

/// Owns a fresh private workspace with exactly one pack/index pair, no loose
/// objects, alternates, refs, replacement objects or host configuration. Native
/// cache cleanup retains admission until all inherited worker fences clear.
/// The service must also retain its native-process admission for this lifetime.
pub struct PhysicalVerifier {
    store: ArtifactStore,
    // Drop the native actor/index before releasing the fenced workspace.
    native: Option<CanonicalVerifier>,
    binding: Arc<VerifiedPackBinding>,
    cache: Arc<GitCache>,
    descriptor: NativePackDescriptor,
    root: PathBuf,
    budget: DiskBudget,
    limits: PhysicalLimits,
    next_ordinal: u32,
    shards: u32,
    chain: [u8; 32],
    failed: bool,
}
impl PhysicalVerifier {
    pub async fn download(
        root: &Path,
        budget: DiskBudget,
        store: &ArtifactStore,
        descriptor: NativePackDescriptor,
        limits: PhysicalLimits,
        native: crate::native_resources::NativeScope,
    ) -> Result<Self, PhysicalError> {
        descriptor.validate(store.repository(), descriptor.format)?;
        if limits.max_pack_bytes > MAX_ARTIFACT_BYTES
            || limits.max_index_bytes > MAX_ARTIFACT_BYTES
            || descriptor.pack.size > limits.max_pack_bytes
            || descriptor.index.size > limits.max_index_bytes
            || limits.native_timeout.is_zero()
        {
            return Err(PhysicalError::Limit);
        }
        let root = root.to_owned();
        let root = tokio::task::spawn_blocking(move || std::fs::canonicalize(root)).await??;
        let cache = GitCache::create(
            root.clone(),
            budget.clone(),
            "refs/heads/main",
            descriptor.format,
            native,
        )
        .await?;
        cache.download_native(store, descriptor).await?;
        let pinned = Arc::clone(&cache);
        let input_claim = cache
            .native
            .try_admit(crate::native_resources::NativeWork::Read)?;
        let binding = tokio::task::spawn_blocking(move || {
            let _claim = input_claim;
            let path = pack_path(&pinned, descriptor);
            descriptor.verify_files(&path, &path.with_extension("idx"))
        })
        .await??;
        validate_native(Arc::clone(&cache), descriptor, limits.native_timeout).await?;
        let native = CanonicalVerifier::new(&cache.git_dir(), descriptor.format, &cache.native)?;
        Ok(Self {
            store: store.clone(),
            native: Some(native),
            binding: Arc::new(binding),
            cache,
            descriptor,
            root,
            budget,
            limits,
            next_ordinal: 0,
            shards: 0,
            chain: seed(descriptor),
            failed: false,
        })
    }

    /// Produce the next exact ordinal interval. Returned files remain private
    /// staging until finish returns the whole-pack witness and closure succeeds.
    /// Any error or cancellation permanently prevents reuse/successful finish.
    pub async fn inspect_next_shard(
        &mut self,
        object_count: u32,
    ) -> Result<Arc<MetadataSegment>, PhysicalError> {
        if self.failed {
            return Err(PhysicalError::Integrity);
        }
        self.failed = true;
        let end = self
            .next_ordinal
            .checked_add(object_count)
            .filter(|end| *end <= self.descriptor.object_count && object_count != 0)
            .ok_or(PhysicalError::Integrity)?;
        let identity = shard_identity(self.descriptor, self.next_ordinal, object_count);
        let root = self.root.clone();
        let budget = self.budget.clone();
        let limits = self.limits.metadata;
        let mut builder = tokio::task::spawn_blocking(move || {
            MetadataBuilder::new(&root, budget, identity, limits)
        })
        .await??;
        let mut ordinal = self.next_ordinal;
        while ordinal < end {
            let count = (end - ordinal).min(PAGE_OBJECTS as u32);
            let binding = Arc::clone(&self.binding);
            let cache = Arc::clone(&self.cache);
            let ids = tokio::task::spawn_blocking(move || {
                let _pin = cache;
                binding
                    .index()
                    .ids_from(ordinal)?
                    .take(count as usize)
                    .collect::<std::io::Result<Vec<_>>>()
            })
            .await??;
            if ids.len() != count as usize {
                return Err(PhysicalError::Integrity);
            }
            let mut witnesses = Vec::with_capacity(ids.len());
            let edges = spool::EdgeSpool::new(&self.root, self.budget.clone());
            for oid in ids {
                let witness = self
                    .native
                    .as_mut()
                    .ok_or(PhysicalError::Integrity)?
                    .inspect_to_spool(oid, &edges, self.limits.max_edge_bytes)
                    .await?;
                witnesses.push(witness);
            }
            // Witnesses and detached SQL workers own the file through every
            // replay. Drop the producer handle before handing off this page.
            drop(edges);
            let cache = Arc::clone(&self.cache);
            builder = tokio::task::spawn_blocking(move || {
                let _pin = cache;
                builder.put_verified_batch(witnesses)?;
                Ok::<_, MetadataError>(builder)
            })
            .await??;
            ordinal += count;
        }
        let binding = Arc::clone(&self.binding);
        let cache = Arc::clone(&self.cache);
        let segment = tokio::task::spawn_blocking(move || {
            let _pin = cache;
            builder.seal(binding.index()).map(Arc::new)
        })
        .await??;
        self.chain = fold_shard(self.chain, self.shards, segment.descriptor());
        self.shards = self.shards.checked_add(1).ok_or(PhysicalError::Integrity)?;
        self.next_ordinal = end;
        self.failed = false;
        Ok(segment)
    }

    pub async fn finish(mut self) -> Result<PhysicalPackWitness, PhysicalError> {
        if self.failed || self.next_ordinal != self.descriptor.object_count || self.shards == 0 {
            return Err(PhysicalError::Integrity);
        }
        let native = self.native.take().ok_or(PhysicalError::Integrity)?;
        tokio::time::timeout(self.limits.native_timeout, native.finish())
            .await
            .map_err(|_| GitHttpError::Timeout)??;
        Ok(PhysicalPackWitness {
            store: self.store,
            native: self.descriptor,
            shard_count: self.shards,
            metadata_digest: self.chain,
        })
    }
}

fn pack_path(cache: &GitCache, descriptor: NativePackDescriptor) -> PathBuf {
    cache.git_dir().join(format!(
        "objects/pack/pack-{}.pack",
        hex::encode(descriptor.git_checksum)
    ))
}
async fn validate_native(
    cache: Arc<GitCache>,
    descriptor: NativePackDescriptor,
    deadline: Duration,
) -> Result<(), GitHttpError> {
    let mut command = crate::native_git::command(&cache.git_dir())?;
    command
        .args(["index-pack", "--threads=2", "--verify"])
        .arg(pack_path(&cache, descriptor))
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let native = cache
        .native
        .try_admit(crate::native_resources::NativeWork::Pack)?;
    let mut process = GitProcess::spawn(command, cache, native)?;
    let run = async {
        let (_, stderr) = tokio::try_join!(
            read_bounded(
                process
                    .child
                    .stdout
                    .take()
                    .ok_or(GitHttpError::Interrupted)?,
                128
            ),
            read_bounded(
                process
                    .child
                    .stderr
                    .take()
                    .ok_or(GitHttpError::Interrupted)?,
                64 << 10
            ),
        )?;
        let status = process.wait().await?;
        if !status.success() {
            return Err(GitHttpError::GitExit {
                status,
                stderr: String::from_utf8_lossy(&stderr).into_owned(),
            });
        }
        Ok(())
    };
    tokio::time::timeout(deadline, run)
        .await
        .map_err(|_| GitHttpError::Timeout)?
}
fn shard_identity(
    native: NativePackDescriptor,
    first_ordinal: u32,
    object_count: u32,
) -> SegmentIdentity {
    SegmentIdentity {
        repository: native.repository,
        operation: native.operation,
        format: native.format,
        pack_digest: native.pack.digest,
        git_checksum: native.git_checksum,
        first_ordinal,
        object_count,
    }
}
fn seed(native: NativePackDescriptor) -> [u8; 32] {
    let mut hash = blake3::Hasher::new();
    hash.update(b"canopy.physical-pack-witness.v1\0");
    hash.update(&native.repository);
    hash.update(&native.operation);
    hash.update(&[native.format.bytes() as u8]);
    hash.update(&native.git_checksum);
    hash.update(&native.object_count.to_le_bytes());
    for artifact in [native.pack, native.index] {
        hash.update(&artifact.size.to_le_bytes());
        hash.update(&artifact.digest);
        hash.update(&artifact.manifest_digest);
    }
    *hash.finalize().as_bytes()
}
fn fold_shard(previous: [u8; 32], ordinal: u32, segment: SegmentDescriptor) -> [u8; 32] {
    let mut record = Vec::with_capacity(160);
    record.extend_from_slice(&segment.identity.first_ordinal.to_le_bytes());
    record.extend_from_slice(&segment.identity.object_count.to_le_bytes());
    record.extend_from_slice(&segment.first_oid);
    record.extend_from_slice(&segment.last_oid);
    record.extend_from_slice(&segment.edge_count.to_le_bytes());
    record.extend_from_slice(&segment.inventory_digest);
    record.extend_from_slice(&segment.size.to_le_bytes());
    record.extend_from_slice(&segment.digest);
    super::super::metadata::fold(previous, u64::from(ordinal), &record)
}

#[cfg(test)]
pub(in crate::packs) mod tests;
