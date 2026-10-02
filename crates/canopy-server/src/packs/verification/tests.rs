use super::*;
use crate::{
    ObjectKind,
    packs::metadata::{PAGE_OBJECTS, TypedEdge, tests::fixture},
};
use std::{sync::Arc, time::Duration};
use tokio::{io::AsyncWriteExt, process::Command};

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

#[tokio::test]
async fn admitted_native_witnesses_assemble_exact_metadata_and_release_scratch() -> Result {
    use crate::packs::metadata::{MetadataBuilder, tests::limits};
    use cellule_ltx::DiskBudget;
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let fixture = fixture(format, 1600).await?;
        let scratch = tempfile::TempDir::new()?;
        // A 16 MiB shard ceiling must not require a 48 MiB reservation before
        // verification. This budget admits the real database and edge spools.
        let budget = DiskBudget::new(2 << 20);
        let mut builder =
            MetadataBuilder::new(scratch.path(), budget.clone(), fixture.identity, limits())?;
        let baseline = budget.used();
        let mut verifier = CanonicalVerifier::new(fixture.root.path(), format)?;
        let mut witnesses = Vec::with_capacity(PAGE_OBJECTS);
        let mut spool = spool::EdgeSpool::new(scratch.path(), budget.clone());
        for oid in fixture.index.ids() {
            let witness = verifier.inspect_to_spool(oid?, &spool, 1 << 20).await?;
            assert_eq!(witness.object(), fixture.objects[&witness.object().oid].0);
            witnesses.push(witness);
            if witnesses.len() == PAGE_OBJECTS {
                drop(spool);
                builder.put_verified_batch(std::mem::take(&mut witnesses))?;
                assert_eq!(budget.used(), builder.admitted_bytes());
                spool = spool::EdgeSpool::new(scratch.path(), budget.clone());
            }
        }
        drop(spool);
        if !witnesses.is_empty() {
            builder.put_verified_batch(witnesses)?;
        }
        // Repeated complete witnesses are canonical overlap, not extra objects
        // or dependencies. Exercise the wide tree's multi-page replay twice.
        let tree = fixture
            .objects
            .values()
            .find(|(object, _)| object.kind == ObjectKind::Tree)
            .ok_or("tree")?
            .0
            .oid;
        for _ in 0..2 {
            let repeated = verifier
                .inspect_to_disk(tree, scratch.path(), budget.clone(), 1 << 20)
                .await?;
            builder.put_verified(repeated)?;
        }
        verifier.finish().await?;
        assert_eq!(budget.used(), builder.admitted_bytes());
        assert!(budget.used() > baseline);
        let segment = builder.seal(&fixture.index)?;
        for (oid, (expected, edges)) in &fixture.objects {
            let header = segment.header(*oid)?.ok_or("header")?;
            assert_eq!(header.object, *expected);
            assert_eq!(header.edge_count, edges.len() as u64);
            let mut actual = Vec::new();
            loop {
                let page =
                    segment.edges_after(*oid, actual.last().map(|edge: &TypedEdge| edge.child))?;
                if page.is_empty() {
                    break;
                }
                assert!(page.len() <= PAGE_OBJECTS);
                actual.extend(page);
            }
            let mut expected = edges.clone();
            expected.sort_unstable_by_key(|edge| edge.child);
            assert_eq!(actual, expected);
        }
        drop(segment);
        assert_eq!(budget.used(), 0);
        assert_eq!(std::fs::read_dir(scratch.path())?.count(), 0);
    }
    Ok(())
}

#[tokio::test]
async fn canonical_and_typed_overlap_conflicts_poison_verified_assembly() -> Result {
    use crate::packs::metadata::{MetadataBuilder, MetadataError, tests::limits};
    use cellule_ltx::DiskBudget;
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let fixture = fixture(format, 700).await?;
        let (tree, edges) = fixture
            .objects
            .values()
            .find(|(object, _)| object.kind == ObjectKind::Tree)
            .ok_or("tree")?;
        for conflict_in_edges in [false, true] {
            let scratch = tempfile::TempDir::new()?;
            let budget = DiskBudget::new(128 << 20);
            let mut builder =
                MetadataBuilder::new(scratch.path(), budget.clone(), fixture.identity, limits())?;
            let mut previous = *tree;
            if !conflict_in_edges {
                previous.digest[0] ^= 1;
            }
            builder.put_objects(&[previous])?;
            if conflict_in_edges {
                let edge = edges
                    .iter()
                    .find(|edge| edge.expected_kind == ObjectKind::Blob)
                    .ok_or("blob edge")?;
                builder.put_edges(
                    tree.oid,
                    &[TypedEdge {
                        child: edge.child,
                        expected_kind: ObjectKind::Tree,
                    }],
                )?;
            }
            let mut verifier = CanonicalVerifier::new(fixture.root.path(), format)?;
            let witness = verifier
                .inspect_to_disk(tree.oid, scratch.path(), budget.clone(), 1 << 20)
                .await?;
            verifier.finish().await?;
            assert!(matches!(
                builder.put_verified(witness),
                Err(MetadataError::IdentityConflict)
            ));
            let path = std::fs::read_dir(scratch.path())?
                .find_map(|entry| {
                    entry
                        .ok()
                        .filter(|entry| {
                            entry
                                .file_name()
                                .to_string_lossy()
                                .starts_with("canopy-metadata-")
                        })
                        .map(|entry| entry.path())
                })
                .ok_or("metadata")?;
            let database = rusqlite::Connection::open_with_flags(
                path,
                rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
            )?;
            let objects: u64 =
                database.query_row("SELECT count(*) FROM objects", [], |row| row.get(0))?;
            let edges: u64 =
                database.query_row("SELECT count(*) FROM object_edges", [], |row| row.get(0))?;
            assert_eq!(objects, 1);
            assert_eq!(edges, u64::from(conflict_in_edges));
            drop(database);
            assert!(matches!(
                builder.seal(&fixture.index),
                Err(MetadataError::Integrity)
            ));
            assert_eq!(budget.used(), 0);
        }
    }
    Ok(())
}

#[tokio::test]
async fn edge_spool_quota_failure_prevents_witness_and_actor_reuse() -> Result {
    use cellule_ltx::DiskBudget;
    let fixture = fixture(ObjectFormat::Sha256, 1600).await?;
    let tree = fixture
        .objects
        .values()
        .find(|(object, _)| object.kind == ObjectKind::Tree)
        .ok_or("tree")?
        .0
        .oid;
    for (capacity, limit) in [(512 * 33, 1 << 20), (1 << 20, 512 * 33)] {
        let scratch = tempfile::TempDir::new()?;
        let budget = DiskBudget::new(capacity);
        let mut verifier = CanonicalVerifier::new(fixture.root.path(), ObjectFormat::Sha256)?;
        assert!(
            verifier
                .inspect_to_disk(tree, scratch.path(), budget.clone(), limit)
                .await
                .is_err()
        );
        assert!(
            verifier
                .inspect(tree, &mut Collector::default())
                .await
                .is_err()
        );
        assert!(verifier.finish().await.is_err());
        assert_eq!(budget.used(), 0);
        assert_eq!(std::fs::read_dir(scratch.path())?.count(), 0);
    }
    Ok(())
}
#[derive(Default)]
struct Collector {
    edges: Vec<TypedEdge>,
    parent: Option<ObjectId>,
    largest: usize,
}
impl EdgeSink for Collector {
    async fn append(
        &mut self,
        parent: ObjectId,
        edges: &[TypedEdge],
    ) -> std::result::Result<(), ObjectReadError> {
        assert!(!edges.is_empty() && edges.len() <= PAGE_OBJECTS);
        assert!(self.parent.is_none_or(|expected| expected == parent));
        self.parent = Some(parent);
        self.largest = self.largest.max(edges.len());
        self.edges.extend_from_slice(edges);
        Ok(())
    }
}
#[tokio::test]
async fn native_streamed_witnesses_match_headers_and_typed_edges_with_bounded_batches() -> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let fixture = fixture(format, 700).await?;
        let mut verifier = CanonicalVerifier::new(fixture.root.path(), format)?;
        // File-backed index iteration replaces a heap inventory for this path.
        for oid in fixture.index.ids() {
            let oid = oid?;
            let mut sink = Collector::default();
            let canonical = verifier.inspect(oid, &mut sink).await?;
            let (expected, edges) = fixture.objects.get(&oid).ok_or("object")?;
            assert_eq!(canonical, *expected);
            sink.edges
                .sort_unstable_by_key(|edge| (edge.child, edge.expected_kind.git_name()));
            sink.edges.dedup();
            let mut expected = edges.clone();
            expected.sort_unstable_by_key(|edge| (edge.child, edge.expected_kind.git_name()));
            assert_eq!(sink.edges, expected);
            assert!(sink.largest <= PAGE_OBJECTS);
        }
        verifier.finish().await?;
    }
    Ok(())
}

#[tokio::test]
async fn large_native_commit_messages_are_hashed_without_entering_the_edge_inventory() -> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let fixture = fixture(format, 4).await?;
        let tree = fixture
            .objects
            .values()
            .find(|(header, _)| header.kind == ObjectKind::Tree)
            .ok_or("tree")?
            .0
            .oid;
        let mut body = format!("tree {}\nauthor Verifier <test@example.invalid> 1 +0000\ncommitter Verifier <test@example.invalid> 1 +0000\n\n", hex::encode(tree)).into_bytes();
        body.extend(std::iter::repeat_n(b'x', 8 << 20));
        let expected = crate::object_id(format, ObjectKind::Commit, &body);
        let mut command: Command = crate::native_git::command(fixture.root.path())?;
        let mut child = command
            .arg("--git-dir")
            .arg(fixture.root.path())
            .args(["hash-object", "-t", "commit", "-w", "--stdin"])
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true)
            .spawn()?;
        child.stdin.take().ok_or("stdin")?.write_all(&body).await?;
        let output = child.wait_with_output().await?;
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(ObjectId::from_hex(output.stdout.trim_ascii())?, expected);
        let mut verifier = CanonicalVerifier::new(fixture.root.path(), format)?;
        let mut sink = Collector::default();
        let canonical = verifier.inspect(expected, &mut sink).await?;
        assert_eq!(canonical.size, body.len() as u64);
        assert_eq!(canonical.digest, *blake3::hash(&body).as_bytes());
        assert_eq!(
            sink.edges,
            vec![TypedEdge {
                child: tree,
                expected_kind: ObjectKind::Tree
            }]
        );
        verifier.finish().await?;
    }
    Ok(())
}
struct FailedSink;
impl EdgeSink for FailedSink {
    async fn append(
        &mut self,
        _: ObjectId,
        _: &[TypedEdge],
    ) -> std::result::Result<(), ObjectReadError> {
        Err(std::io::Error::other("injected spool failure").into())
    }
}
#[tokio::test]
async fn sink_failure_poisoning_prevents_another_inspection_or_successful_finish() -> Result {
    let fixture = fixture(ObjectFormat::Sha256, 4).await?;
    let tree = fixture
        .objects
        .values()
        .find(|(header, _)| header.kind == ObjectKind::Tree)
        .ok_or("tree")?
        .0
        .oid;
    let mut verifier = CanonicalVerifier::new(fixture.root.path(), ObjectFormat::Sha256)?;
    assert!(matches!(
        verifier.inspect(tree, &mut FailedSink).await,
        Err(ObjectReadError::Io(_))
    ));
    assert!(matches!(
        verifier.inspect(tree, &mut Collector::default()).await,
        Err(ObjectReadError::Malformed)
    ));
    assert!(matches!(
        verifier.finish().await,
        Err(ObjectReadError::Malformed)
    ));
    Ok(())
}
struct PausedSink {
    entered: Arc<tokio::sync::Notify>,
}
impl EdgeSink for PausedSink {
    async fn append(
        &mut self,
        _: ObjectId,
        _: &[TypedEdge],
    ) -> std::result::Result<(), ObjectReadError> {
        self.entered.notify_one();
        std::future::pending().await
    }
}
#[tokio::test]
async fn canceling_a_confirmed_pending_sink_write_poisons_native_frame_reuse() -> Result {
    let fixture = fixture(ObjectFormat::Sha256, 700).await?;
    let tree = fixture
        .objects
        .values()
        .find(|(header, _)| header.kind == ObjectKind::Tree)
        .ok_or("tree")?
        .0
        .oid;
    let mut verifier = CanonicalVerifier::new(fixture.root.path(), ObjectFormat::Sha256)?;
    let entered = Arc::new(tokio::sync::Notify::new());
    let mut sink = PausedSink {
        entered: Arc::clone(&entered),
    };
    let mut inspection = Box::pin(verifier.inspect(tree, &mut sink));
    tokio::time::timeout(Duration::from_secs(5), async {
        tokio::select! {
            _ = entered.notified() => Ok::<_, Box<dyn std::error::Error>>(()),
            result = inspection.as_mut() => Err(format!("inspection unexpectedly finished: {result:?}").into()),
        }
    }).await??;
    drop(inspection);
    assert!(matches!(
        verifier.inspect(tree, &mut Collector::default()).await,
        Err(ObjectReadError::Malformed)
    ));
    assert!(matches!(
        verifier.finish().await,
        Err(ObjectReadError::Malformed)
    ));
    Ok(())
}
