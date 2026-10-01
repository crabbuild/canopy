use super::*;
use crate::packs::{
    directory::DirectoryBuilder,
    metadata::{CanonicalObject, TypedEdge, tests::limits},
    verification::{
        PhysicalVerifier,
        physical::tests::{
            independence::{git_input, upload_pair},
            physical_limits, prepared,
        },
    },
};
use canopy_object_storage::artifact::ArtifactDescriptor;
use std::{collections::BTreeMap, time::Duration};

mod graph;
mod resources;

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;
fn context(format: ObjectFormat) -> ClosureContext {
    ClosureContext {
        repository: [1; 16],
        operation: [2; 16],
        format,
        base: None,
    }
}
fn base(context: ClosureContext) -> ClosureBase {
    // Test-only catalog identity; production derives this from leased Cell facts.
    ClosureBase {
        catalog: StoredCatalog {
            repository: context.repository,
            operation: [8; 16],
            format: context.format,
            artifact: ArtifactDescriptor {
                size: 1,
                digest: [3; 32],
                manifest_digest: [4; 32],
            },
        },
        generation: 7,
    }
}
fn header(object: CanonicalObject, edges: &[TypedEdge]) -> ObjectHeader {
    let edges: BTreeMap<_, _> = edges.iter().map(|e| (e.child, e.expected_kind)).collect();
    let mut chain = metadata::edge_seed(object.oid);
    for (ordinal, (oid, kind)) in edges.iter().enumerate() {
        let mut record = oid.to_vec();
        record.push(metadata::kind_code(*kind));
        chain = metadata::fold(chain, ordinal as u64, &record);
    }
    ObjectHeader {
        object,
        edge_count: edges.len() as u64,
        edge_digest: chain,
    }
}
struct Resolver {
    headers: BTreeMap<ObjectId, BaseObject>,
    requests: Mutex<Vec<ObjectId>>,
}
impl Resolver {
    fn empty() -> Self {
        Self {
            headers: BTreeMap::new(),
            requests: Mutex::new(Vec::new()),
        }
    }
}
impl BaseResolver for Resolver {
    async fn resolve(
        &self,
        base: ClosureBase,
        ids: &[ObjectId],
    ) -> std::result::Result<BaseBatch, ClosureError> {
        assert!(!ids.is_empty() && ids.len() <= PAGE_OBJECTS);
        assert!(ids.windows(2).all(|pair| pair[0] < pair[1]));
        self.requests.lock().unwrap().extend_from_slice(ids);
        Ok(BaseBatch {
            base,
            objects: ids
                .iter()
                .map(|oid| self.headers.get(oid).copied())
                .collect(),
        })
    }
}
async fn cleanup(root: &Path, budget: &DiskBudget) -> Result {
    tokio::time::timeout(Duration::from_secs(5), async {
        while budget.used() != 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;
    assert_eq!(std::fs::read_dir(root)?.count(), 0);
    Ok(())
}

#[tokio::test]
async fn physical_shards_close_and_bind_the_reused_directory_inventory() -> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let prepared = prepared(format, 1600).await?;
        let root = tempfile::TempDir::new()?;
        let budget = DiskBudget::new(256 << 20);
        let mut ctx = context(format);
        ctx.base = Some(base(ctx));
        let mut closure = ClosureVerifier::new(root.path(), budget.clone(), ctx, limits()).await?;
        let mut directory = DirectoryBuilder::new(
            root.path(),
            budget.clone(),
            ctx.repository,
            ctx.operation,
            format,
            limits(),
        )?;
        // Repeated exact physical inputs deduplicate objects, edges and input proofs.
        for _ in 0..2 {
            let mut physical = PhysicalVerifier::download(
                root.path(),
                budget.clone(),
                &prepared.store,
                prepared.descriptor,
                physical_limits(),
            )
            .await?;
            let mut segments = Vec::new();
            for count in [1, 800, prepared.descriptor.object_count - 801] {
                segments.push(physical.inspect_next_shard(count).await?);
            }
            closure.begin_pack(physical.finish().await?)?;
            for segment in segments {
                directory.add_segment(&segment)?;
                closure.add_segment(segment).await?;
            }
            closure.finish_pack().await?;
        }
        let mut resolver = Resolver::empty();
        let expected: Vec<_> = prepared
            .fixture
            .objects
            .values()
            .map(|(o, e)| header(*o, e))
            .collect();
        // An overlap from the base must be compared even though all children are local.
        let overlapping = expected[0];
        resolver.headers.insert(
            overlapping.object.oid,
            BaseObject {
                header: overlapping,
                certified: true,
            },
        );
        let witness = closure.finish(Some(&resolver)).await?;
        assert_eq!(witness.context(), ctx);
        assert_eq!(witness.input_count(), 1);
        assert_eq!(witness.object_count(), expected.len() as u64);
        assert_eq!(
            witness.edge_count(),
            expected.iter().map(|h| h.edge_count).sum::<u64>()
        );
        witness.verify_headers(expected.iter().copied())?;
        {
            let requested = resolver.requests.lock().unwrap();
            assert_eq!(
                *requested,
                expected.iter().map(|h| h.object.oid).collect::<Vec<_>>()
            );
        }
        let run = directory.seal()?;
        witness.verify_run(run.descriptor())?;
        let mut forged = run.descriptor();
        forged.inventory_digest[0] ^= 1;
        assert!(matches!(
            witness.verify_run(forged),
            Err(ClosureError::Integrity)
        ));
        assert!(matches!(
            witness.verify_headers(expected.iter().copied().skip(1)),
            Err(ClosureError::Integrity)
        ));
        let mut conflicting = expected.clone();
        conflicting[0].object.digest[0] ^= 1;
        assert!(matches!(
            witness.verify_headers(conflicting),
            Err(ClosureError::Integrity)
        ));
        drop(run);
        cleanup(root.path(), &budget).await?;
    }
    Ok(())
}

#[tokio::test]
async fn commit_in_a_separate_pack_requires_the_exact_certified_base_dependency() -> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let prepared = prepared(format, 16).await?;
        let (commit, edges) = prepared
            .fixture
            .objects
            .values()
            .find(|(o, _)| o.kind == ObjectKind::Commit)
            .ok_or("commit")?;
        let tree = edges
            .iter()
            .find(|e| e.expected_kind == ObjectKind::Tree)
            .ok_or("tree")?
            .child;
        let (tree_object, tree_edges) = &prepared.fixture.objects[&tree];
        let tree_header = header(*tree_object, tree_edges);
        let pack = git_input(
            prepared.fixture.root.path(),
            &["pack-objects", "--stdout", "--no-reuse-delta"],
            format!("{}\n", hex::encode(commit.oid)).as_bytes(),
        )
        .await?;
        let descriptor = upload_pair(&prepared, commit.oid, &pack).await?;
        // Success, missing base, uncertified base, and the wrong typed child.
        for case in 0..4 {
            let root = tempfile::TempDir::new()?;
            let budget = DiskBudget::new(128 << 20);
            let mut physical = PhysicalVerifier::download(
                root.path(),
                budget.clone(),
                &prepared.store,
                descriptor,
                physical_limits(),
            )
            .await?;
            let segment = physical.inspect_next_shard(1).await?;
            let mut ctx = context(format);
            ctx.operation = descriptor.operation;
            ctx.base = Some(base(ctx));
            let mut closure =
                ClosureVerifier::new(root.path(), budget.clone(), ctx, limits()).await?;
            closure.begin_pack(physical.finish().await?)?;
            closure.add_segment(segment).await?;
            closure.finish_pack().await?;
            let mut resolver = Resolver::empty();
            if case != 1 {
                let mut h = tree_header;
                if case == 3 {
                    h.object.kind = ObjectKind::Tag;
                }
                resolver.headers.insert(
                    tree,
                    BaseObject {
                        header: h,
                        certified: case != 2,
                    },
                );
            }
            let result = closure.finish(Some(&resolver)).await;
            match case {
                0 => {
                    let witness = result?;
                    assert_eq!(witness.object_count(), 1);
                    assert_eq!(witness.edge_count(), 1);
                    witness.verify_headers([header(*commit, edges)])?;
                }
                1 => assert!(matches!(result,Err(ClosureError::Missing(oid)) if oid==tree)),
                2 => assert!(matches!(result,Err(ClosureError::Uncertified(oid)) if oid==tree)),
                _ => assert!(
                    matches!(result,Err(ClosureError::Kind { oid,expected:ObjectKind::Tree,actual:ObjectKind::Tag }) if oid==tree)
                ),
            }
            assert_eq!(resolver.requests.lock().unwrap().len(), 2);
            cleanup(root.path(), &budget).await?;
        }
    }
    Ok(())
}

#[tokio::test]
async fn incomplete_mismatched_or_interrupted_physical_partitions_poison_closure() -> Result {
    let prepared = prepared(ObjectFormat::Sha256, 4).await?;
    for case in 0..4 {
        let root = tempfile::TempDir::new()?;
        let budget = DiskBudget::new(128 << 20);
        let mut physical = PhysicalVerifier::download(
            root.path(),
            budget.clone(),
            &prepared.store,
            prepared.descriptor,
            physical_limits(),
        )
        .await?;
        let first = physical.inspect_next_shard(1).await?;
        let second = physical
            .inspect_next_shard(prepared.descriptor.object_count - 1)
            .await?;
        let proof = physical.finish().await?;
        let mut ctx = context(ObjectFormat::Sha256);
        if case == 0 {
            ctx.operation[0] ^= 1;
        }
        let mut closure = ClosureVerifier::new(root.path(), budget.clone(), ctx, limits()).await?;
        if case == 0 {
            assert!(matches!(
                closure.begin_pack(proof),
                Err(ClosureError::Integrity)
            ));
        } else {
            closure.begin_pack(proof)?;
            if case == 1 {
                assert!(closure.add_segment(second.clone()).await.is_err());
            }
            if case == 2 {
                closure.add_segment(first.clone()).await?;
                assert!(closure.finish_pack().await.is_err());
            }
            if case == 3 {
                closure.add_segment(first.clone()).await?;
            }
        }
        assert!(closure.finish(None::<&Resolver>).await.is_err());
        drop((first, second));
        cleanup(root.path(), &budget).await?;
    }
    // Empty operations are allowed; they certify no pack or incoming object.
    let root = tempfile::TempDir::new()?;
    let budget = DiskBudget::new(64 << 20);
    let closure = ClosureVerifier::new(
        root.path(),
        budget.clone(),
        context(ObjectFormat::Sha256),
        limits(),
    )
    .await?;
    let witness = closure.finish(None::<&Resolver>).await?;
    assert_eq!(
        (
            witness.object_count(),
            witness.input_count(),
            witness.edge_count()
        ),
        (0, 0, 0)
    );
    witness.verify_headers([])?;
    cleanup(root.path(), &budget).await?;
    Ok(())
}

#[tokio::test]
async fn changed_sealed_metadata_bytes_cannot_enter_closure_or_be_retried() -> Result {
    let prepared = prepared(ObjectFormat::Sha256, 4).await?;
    let root = tempfile::TempDir::new()?;
    let budget = DiskBudget::new(128 << 20);
    let mut physical = PhysicalVerifier::download(
        root.path(),
        budget.clone(),
        &prepared.store,
        prepared.descriptor,
        physical_limits(),
    )
    .await?;
    let segment = physical
        .inspect_next_shard(prepared.descriptor.object_count)
        .await?;
    let proof = physical.finish().await?;
    let mut closure = ClosureVerifier::new(
        root.path(),
        budget.clone(),
        context(ObjectFormat::Sha256),
        limits(),
    )
    .await?;
    closure.begin_pack(proof)?;
    // Mutate an unused tail byte: cached SQLite headers might remain readable,
    // but copying must check all sealed bytes before trusting any cached row.
    let mut bytes = std::fs::read(segment.path())?;
    *bytes.last_mut().ok_or("metadata bytes")? ^= 1;
    std::fs::write(segment.path(), bytes)?;
    assert!(matches!(
        closure.add_segment(segment.clone()).await,
        Err(ClosureError::Integrity)
    ));
    assert!(matches!(
        closure.add_segment(segment.clone()).await,
        Err(ClosureError::Integrity)
    ));
    assert!(matches!(
        closure.finish(None::<&Resolver>).await,
        Err(ClosureError::Integrity)
    ));
    drop(segment);
    cleanup(root.path(), &budget).await?;
    Ok(())
}

#[tokio::test]
async fn separate_native_packs_merge_their_graphs_and_identical_canonical_overlaps() -> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let root = tempfile::TempDir::new()?;
        let budget = DiskBudget::new(256 << 20);
        let ctx = context(format);
        let mut closure = ClosureVerifier::new(root.path(), budget.clone(), ctx, limits()).await?;
        let mut directory = DirectoryBuilder::new(
            root.path(),
            budget.clone(),
            ctx.repository,
            ctx.operation,
            format,
            limits(),
        )?;
        let mut expected = BTreeMap::new();
        let mut physical_count = 0;
        for blobs in [16, 8] {
            let prepared = prepared(format, blobs).await?;
            for (object, edges) in prepared.fixture.objects.values() {
                let h = header(*object, edges);
                if let Some(prior) = expected.insert(object.oid, h) {
                    assert_eq!(prior, h);
                }
            }
            let mut physical = PhysicalVerifier::download(
                root.path(),
                budget.clone(),
                &prepared.store,
                prepared.descriptor,
                physical_limits(),
            )
            .await?;
            physical_count += prepared.descriptor.object_count as u64;
            let segment = physical
                .inspect_next_shard(prepared.descriptor.object_count)
                .await?;
            closure.begin_pack(physical.finish().await?)?;
            directory.add_segment(&segment)?;
            closure.add_segment(segment).await?;
            closure.finish_pack().await?;
        }
        let witness = closure.finish(None::<&Resolver>).await?;
        assert_eq!(witness.input_count(), 2);
        assert_eq!(witness.object_count(), expected.len() as u64);
        assert!(physical_count > witness.object_count());
        witness.verify_headers(expected.values().copied())?;
        let run = directory.seal()?;
        witness.verify_run(run.descriptor())?;
        drop(run);
        cleanup(root.path(), &budget).await?;
    }
    Ok(())
}
