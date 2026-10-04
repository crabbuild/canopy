use super::*;
use crate::packs::{
    metadata::tests::{Fixture, fixture, limits},
    sources::{PackCoverage, SourceRecord},
};

pub(in crate::packs) mod independence;
use canopy_object_storage::artifact::{ArtifactDescriptor, ArtifactKey, ArtifactKind};
use cellule_ltx::DiskBudget;
use object_store::{ObjectStore, ObjectStoreExt, memory::InMemory};
use std::{future::Future, path::Path};

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;
pub(in crate::packs) struct Prepared {
    pub(in crate::packs) fixture: Fixture,
    provider: Arc<dyn ObjectStore>,
    pub(in crate::packs) store: Arc<ArtifactStore>,
    pub(in crate::packs) descriptor: NativePackDescriptor,
}
pub(in crate::packs) fn physical_limits() -> PhysicalLimits {
    PhysicalLimits {
        metadata: limits(),
        native_timeout: Duration::from_secs(30),
        ..PhysicalLimits::default()
    }
}
async fn upload_bytes(
    store: &ArtifactStore,
    key: ArtifactKey,
    bytes: &[u8],
) -> Result<ArtifactDescriptor> {
    Ok(store
        .put(
            key,
            bytes.len() as u64,
            *blake3::hash(bytes).as_bytes(),
            &mut &bytes[..],
        )
        .await?)
}
pub(in crate::packs) async fn prepared(format: ObjectFormat, blobs: usize) -> Result<Prepared> {
    prepared_for_context(format, blobs, [1; 16], [2; 16]).await
}
pub(in crate::packs) async fn prepared_for_context(
    format: ObjectFormat,
    blobs: usize,
    repository: [u8; 16],
    operation: [u8; 16],
) -> Result<Prepared> {
    let provider: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let store = Arc::new(ArtifactStore::new(provider.clone(), repository));
    prepared_for_store(format, blobs, operation, provider, store).await
}
pub(in crate::packs) async fn prepared_for_store(
    format: ObjectFormat,
    blobs: usize,
    operation: [u8; 16],
    provider: Arc<dyn ObjectStore>,
    store: Arc<ArtifactStore>,
) -> Result<Prepared> {
    let fixture = fixture(format, blobs).await?;
    upload_fixture(fixture, operation, provider, store).await
}
/// Upload the exact native fixture after admission assigns its namespace.
pub(in crate::packs) async fn upload_fixture(
    mut fixture: Fixture,
    operation: [u8; 16],
    provider: Arc<dyn ObjectStore>,
    store: Arc<ArtifactStore>,
) -> Result<Prepared> {
    let format = fixture.identity.format;
    fixture.identity.repository = store.repository();
    fixture.identity.operation = operation;
    let path = std::fs::read_dir(fixture.root.path().join("objects/pack"))?
        .find_map(|entry| {
            entry
                .ok()
                .map(|entry| entry.path())
                .filter(|path| path.extension().is_some_and(|ext| ext == "idx"))
        })
        .ok_or("index")?;
    let key = |kind| ArtifactKey {
        operation: fixture.identity.operation,
        binding_digest: fixture.identity.pack_digest,
        kind,
    };
    let pack = upload_bytes(
        &store,
        key(ArtifactKind::Pack),
        &std::fs::read(path.with_extension("pack"))?,
    )
    .await?;
    let index = upload_bytes(&store, key(ArtifactKind::Index), &std::fs::read(path)?).await?;
    let descriptor = NativePackDescriptor {
        repository: fixture.identity.repository,
        operation: fixture.identity.operation,
        format,
        git_checksum: fixture.identity.git_checksum,
        object_count: fixture.index.len(),
        pack,
        index,
    };
    Ok(Prepared {
        fixture,
        provider,
        store,
        descriptor,
    })
}
async fn drained(root: &Path, budget: &DiskBudget, expected: u64) -> Result {
    tokio::time::timeout(Duration::from_secs(5), async {
        while budget.used() != expected {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;
    if expected == 0 {
        assert_eq!(std::fs::read_dir(root)?.count(), 0);
    }
    Ok(())
}

#[tokio::test]
async fn isolated_physical_verification_covers_exact_shards_and_binds_uploaded_sources() -> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let prepared = prepared(format, 1600).await?;
        let root = tempfile::TempDir::new()?;
        let budget = DiskBudget::new(128 << 20);
        let mut verifier = PhysicalVerifier::download(
            root.path(),
            budget.clone(),
            &prepared.store,
            prepared.descriptor,
            physical_limits(),
            crate::native_resources::NativeResources::default()
                .scope(crate::native_resources::NativeClass::Foreground),
        )
        .await?;
        let mut segments = Vec::new();
        let halfway = prepared.descriptor.object_count / 2;
        for count in [1, halfway, prepared.descriptor.object_count - halfway - 1] {
            segments.push(verifier.inspect_next_shard(count).await?);
        }
        let witness = verifier.finish().await?;
        assert_eq!(witness.native(), prepared.descriptor);
        assert_eq!(witness.shard_count(), 3);
        let descriptors: Vec<_> = segments
            .iter()
            .map(|segment| segment.descriptor())
            .collect();
        witness.verify_segments(descriptors.iter().copied())?;
        let retained = segments
            .iter()
            .map(|segment| segment.descriptor().size)
            .sum();
        drained(root.path(), &budget, retained).await?;
        let mut sources = Vec::new();
        for segment in &segments {
            let mut after = None;
            let mut count = 0;
            loop {
                let page = segment.headers_after(after)?;
                if page.is_empty() {
                    break;
                }
                assert!(page.len() <= PAGE_OBJECTS);
                for header in page {
                    assert_eq!(
                        header.object,
                        prepared.fixture.objects[&header.object.oid].0
                    );
                    after = Some(header.object.oid);
                    count += 1;
                }
            }
            assert_eq!(count, segment.descriptor().identity.object_count);
            let metadata = segment.clone().upload(&prepared.store).await?;
            let source = SourceRecord {
                metadata,
                pack: prepared.descriptor.pack,
                index: prepared.descriptor.index,
                pack_object_count: prepared.descriptor.object_count,
            };
            source.validate(prepared.descriptor.repository, format)?;
            assert_eq!(source.native(), witness.native());
            sources.push(source);
        }
        let mut coverage = PackCoverage::new(sources[0])?;
        for source in sources {
            coverage.add(source)?;
        }
        coverage.finish()?;
        for bad in 0..5 {
            let mut forged = descriptors.clone();
            match bad {
                0 => {
                    forged.swap(0, 1);
                }
                1 => {
                    forged.pop();
                }
                2 => {
                    forged[1].digest[0] ^= 1;
                }
                3 => {
                    forged[1].inventory_digest[0] ^= 1;
                }
                _ => {
                    forged[1].identity.operation[0] ^= 1;
                }
            }
            assert!(matches!(
                witness.verify_segments(forged),
                Err(PhysicalError::Integrity)
            ));
        }
        drop(segments);
        drained(root.path(), &budget, 0).await?;
    }
    Ok(())
}

#[tokio::test]
async fn incomplete_or_failed_physical_inspection_cannot_finish_or_resume() -> Result {
    let prepared = prepared(ObjectFormat::Sha256, 700).await?;
    for fail in [false, true] {
        let root = tempfile::TempDir::new()?;
        let budget = DiskBudget::new(128 << 20);
        let mut limits = physical_limits();
        if fail {
            limits.max_edge_bytes = 0;
        }
        let mut verifier = PhysicalVerifier::download(
            root.path(),
            budget.clone(),
            &prepared.store,
            prepared.descriptor,
            limits,
            crate::native_resources::NativeResources::default()
                .scope(crate::native_resources::NativeClass::Foreground),
        )
        .await?;
        if fail {
            assert!(
                verifier
                    .inspect_next_shard(prepared.descriptor.object_count)
                    .await
                    .is_err()
            );
            assert!(matches!(
                verifier.inspect_next_shard(1).await,
                Err(PhysicalError::Integrity)
            ));
        } else {
            let segment = verifier.inspect_next_shard(1).await?;
            drop(segment);
        }
        assert!(matches!(
            verifier.finish().await,
            Err(PhysicalError::Integrity)
        ));
        drained(root.path(), &budget, 0).await?;
    }
    Ok(())
}

#[tokio::test]
async fn native_index_verification_rejects_forged_crc_despite_valid_artifact_and_index_hashes()
-> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let prepared = prepared(format, 16).await?;
        let path = std::fs::read_dir(prepared.fixture.root.path().join("objects/pack"))?
            .find_map(|entry| {
                entry
                    .ok()
                    .map(|entry| entry.path())
                    .filter(|path| path.extension().is_some_and(|ext| ext == "idx"))
            })
            .ok_or("index")?;
        let mut bytes = std::fs::read(&path)?;
        let crc_at = 1032 + prepared.descriptor.object_count as usize * format.bytes();
        bytes[crc_at] ^= 1;
        let payload = bytes.len() - format.bytes();
        let mut hash = crate::git_format::ObjectHasher::raw(format);
        hash.update(&bytes[..payload]);
        bytes[payload..].copy_from_slice(&hash.finalize());
        let candidate = tempfile::NamedTempFile::new()?;
        std::fs::write(candidate.path(), &bytes)?;
        // The bounded native index checker accepts a self-consistent index;
        // only the isolated native pack/index verification catches the CRC lie.
        crate::git_format::pack_index::PackIndex::open(candidate.path(), format)?;
        let index = upload_bytes(
            &prepared.store,
            prepared.descriptor.key(ArtifactKind::Index)?,
            &bytes,
        )
        .await?;
        let descriptor = NativePackDescriptor {
            index,
            ..prepared.descriptor
        };
        let root = tempfile::TempDir::new()?;
        let budget = DiskBudget::new(128 << 20);
        assert!(matches!(
            PhysicalVerifier::download(
                root.path(),
                budget.clone(),
                &prepared.store,
                descriptor,
                physical_limits(),
                crate::native_resources::NativeResources::default()
                    .scope(crate::native_resources::NativeClass::Foreground)
            )
            .await,
            Err(PhysicalError::Native(GitHttpError::GitExit { .. }))
        ));
        drained(root.path(), &budget, 0).await?;
    }
    Ok(())
}

#[tokio::test]
async fn artifact_corruption_and_admission_limits_never_return_a_physical_verifier() -> Result {
    let prepared = prepared(ObjectFormat::Sha256, 16).await?;
    let root = tempfile::TempDir::new()?;
    let budget = DiskBudget::new(128 << 20);
    let mut limits = physical_limits();
    limits.max_pack_bytes = prepared.descriptor.pack.size - 1;
    assert!(matches!(
        PhysicalVerifier::download(
            root.path(),
            budget.clone(),
            &prepared.store,
            prepared.descriptor,
            limits,
            crate::native_resources::NativeResources::default()
                .scope(crate::native_resources::NativeClass::Foreground)
        )
        .await,
        Err(PhysicalError::Limit)
    ));
    drained(root.path(), &budget, 0).await?;
    let mut foreign = prepared.descriptor;
    foreign.repository[0] ^= 1;
    assert!(matches!(
        PhysicalVerifier::download(
            root.path(),
            budget.clone(),
            &prepared.store,
            foreign,
            physical_limits(),
            crate::native_resources::NativeResources::default()
                .scope(crate::native_resources::NativeClass::Foreground)
        )
        .await,
        Err(PhysicalError::Binding(_))
    ));
    drained(root.path(), &budget, 0).await?;
    let small_budget =
        DiskBudget::new(prepared.descriptor.pack.size + prepared.descriptor.index.size - 1);
    assert!(matches!(
        PhysicalVerifier::download(
            root.path(),
            small_budget.clone(),
            &prepared.store,
            prepared.descriptor,
            physical_limits(),
            crate::native_resources::NativeResources::default()
                .scope(crate::native_resources::NativeClass::Foreground)
        )
        .await,
        Err(PhysicalError::Metadata(MetadataError::Budget(_)))
    ));
    drained(root.path(), &small_budget, 0).await?;
    let path = prepared.store.path(
        prepared.descriptor.key(ArtifactKind::Pack)?,
        prepared.descriptor.pack.digest,
    )?;
    prepared
        .provider
        .put(
            &canopy_object_storage::external::part(&path, 0),
            bytes::Bytes::from(vec![0; prepared.descriptor.pack.size as usize]).into(),
        )
        .await?;
    assert!(matches!(
        PhysicalVerifier::download(
            root.path(),
            budget.clone(),
            &prepared.store,
            prepared.descriptor,
            physical_limits(),
            crate::native_resources::NativeResources::default()
                .scope(crate::native_resources::NativeClass::Foreground)
        )
        .await,
        Err(PhysicalError::Metadata(MetadataError::Artifact(_)))
    ));
    drained(root.path(), &budget, 0).await?;
    Ok(())
}

#[test]
fn canceling_confirmed_queued_shard_assembly_poisons_the_complete_pack() -> Result {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .max_blocking_threads(1)
        .enable_all()
        .build()?;
    let prepared = runtime.block_on(prepared(ObjectFormat::Sha256, 16))?;
    let root = tempfile::TempDir::new()?;
    let budget = DiskBudget::new(128 << 20);
    let mut verifier = runtime.block_on(PhysicalVerifier::download(
        root.path(),
        budget.clone(),
        &prepared.store,
        prepared.descriptor,
        physical_limits(),
        crate::native_resources::NativeResources::default()
            .scope(crate::native_resources::NativeClass::Foreground),
    ))?;
    let entered = Arc::new(tokio::sync::Notify::new());
    let worker_entered = entered.clone();
    let (release, wait) = std::sync::mpsc::channel();
    let _blocker = runtime.spawn_blocking(move || {
        worker_entered.notify_one();
        wait.recv().expect("release blocker");
    });
    runtime.block_on(entered.notified());
    runtime.block_on(async {
        let mut pending = std::pin::pin!(verifier.inspect_next_shard(1));
        std::future::poll_fn(|context| {
            assert!(pending.as_mut().poll(context).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
    });
    assert!(runtime.block_on(verifier.inspect_next_shard(1)).is_err());
    assert!(runtime.block_on(verifier.finish()).is_err());
    release.send(())?;
    runtime.block_on(runtime.spawn_blocking(|| ()))?;
    runtime.block_on(drained(root.path(), &budget, 0))?;
    Ok(())
}
