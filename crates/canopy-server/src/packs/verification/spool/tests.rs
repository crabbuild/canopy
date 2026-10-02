use super::*;
use crate::{
    ObjectFormat,
    packs::metadata::{
        MetadataBuilder,
        tests::{fixture, limits},
    },
};
use std::future::Future;
type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

#[test]
fn canceled_queued_write_retains_file_admission_and_cannot_complete() -> Result {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .max_blocking_threads(1)
        .enable_all()
        .build()?;
    let root = tempfile::TempDir::new()?;
    let budget = DiskBudget::new(100);
    let parent = crate::object_id(ObjectFormat::Sha256, ObjectKind::Tree, b"tree");
    let child = crate::object_id(ObjectFormat::Sha256, ObjectKind::Blob, b"blob");
    let edge = [TypedEdge {
        child,
        expected_kind: ObjectKind::Blob,
    }];
    let mut sink = DiskSink::new(parent, root.path(), budget.clone(), 100);
    runtime.block_on(sink.append(parent, &edge))?;
    assert_eq!(budget.used(), 33);
    let entered = Arc::new(tokio::sync::Notify::new());
    let worker_entered = entered.clone();
    let (release, wait) = std::sync::mpsc::channel();
    let _blocker = runtime.spawn_blocking(move || {
        worker_entered.notify_one();
        wait.recv().expect("release blocker");
    });
    runtime.block_on(entered.notified());
    runtime.block_on(async {
        let mut pending = std::pin::pin!(sink.append(parent, &edge));
        std::future::poll_fn(|context| {
            assert!(pending.as_mut().poll(context).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
        // Drop the confirmed queued append here, before releasing its worker.
    });
    assert!(
        sink.complete(CanonicalObject {
            oid: parent,
            kind: ObjectKind::Tree,
            size: 4,
            digest: [0; 32]
        })
        .is_err()
    );
    assert_eq!(budget.used(), 33);
    assert_eq!(std::fs::read_dir(root.path())?.count(), 1);
    release.send(())?;
    runtime.block_on(runtime.spawn_blocking(|| ()))?;
    assert_eq!(budget.used(), 0);
    assert_eq!(std::fs::read_dir(root.path())?.count(), 0);
    Ok(())
}

#[tokio::test]
async fn late_scratch_corruption_rolls_back_entire_witness_batch_and_poison_sealing() -> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let fixture = fixture(format, 1600).await?;
        let root = tempfile::TempDir::new()?;
        let budget = DiskBudget::new(128 << 20);
        let mut builder =
            MetadataBuilder::new(root.path(), budget.clone(), fixture.identity, limits())?;
        let tree = fixture
            .objects
            .values()
            .find(|(object, _)| object.kind == ObjectKind::Tree)
            .ok_or("tree")?
            .0
            .oid;
        let commit = fixture
            .objects
            .values()
            .find(|(object, _)| object.kind == ObjectKind::Commit)
            .ok_or("commit")?
            .0
            .oid;
        let mut verifier = super::super::CanonicalVerifier::new(
            fixture.root.path(),
            format,
            &crate::native_resources::NativeResources::default()
                .scope(crate::native_resources::NativeClass::Foreground),
        )?;
        let spool = EdgeSpool::new(root.path(), budget.clone());
        let first = verifier.inspect_to_spool(tree, &spool, 1 << 20).await?;
        let corrupt = verifier.inspect_to_spool(commit, &spool, 1 << 20).await?;
        assert!(Arc::ptr_eq(
            &first.range.as_ref().ok_or("first range")?.storage,
            &corrupt.range.as_ref().ok_or("second range")?.storage,
        ));
        assert!(corrupt.range.as_ref().ok_or("second range")?.offset > 0);
        drop(spool);
        {
            let range = corrupt.range.as_ref().ok_or("spool")?;
            let mut storage = range.storage.lock().map_err(|_| "spool lock")?;
            let file = storage
                .file
                .as_mut()
                .ok_or("spool file")?
                .file_mut()
                .as_file_mut();
            file.seek(SeekFrom::Start(range.offset))?;
            let mut byte = [0];
            file.read_exact(&mut byte)?;
            byte[0] ^= 1; // valid OID bytes; detected after all replay pages.
            file.seek(SeekFrom::Start(range.offset))?;
            file.write_all(&byte)?;
        }
        verifier.finish().await?;
        assert!(matches!(
            builder.put_verified_batch(vec![first, corrupt]),
            Err(MetadataError::Integrity)
        ));
        let path = std::fs::read_dir(root.path())?
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
        for table in ["objects", "object_edges"] {
            let count: u64 =
                database.query_row(&format!("SELECT count(*) FROM {table}"), [], |row| {
                    row.get(0)
                })?;
            assert_eq!(count, 0);
        }
        drop(database);
        assert!(matches!(
            builder.put_objects(&[fixture.objects[&commit].0]),
            Err(MetadataError::Integrity)
        ));
        assert!(matches!(
            builder.seal(&fixture.index),
            Err(MetadataError::Integrity)
        ));
        assert_eq!(budget.used(), 0);
        assert_eq!(std::fs::read_dir(root.path())?.count(), 0);
    }
    Ok(())
}

#[tokio::test]
async fn native_structural_page_uses_one_file_with_exact_independent_ranges() -> Result {
    use crate::packs::verification::physical::tests::independence::git_input;
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let fixture = fixture(format, 1).await?;
        let initial = fixture
            .objects
            .values()
            .find(|(object, _)| object.kind == ObjectKind::Commit)
            .ok_or("initial commit")?
            .0
            .oid;
        let tree = fixture
            .objects
            .values()
            .find(|(object, _)| object.kind == ObjectKind::Tree)
            .ok_or("tree")?
            .0
            .oid;
        let mut input = Vec::new();
        for n in 1..=PAGE_OBJECTS {
            let parent = if n == 1 {
                hex::encode(initial)
            } else {
                format!(":{}", n - 1)
            };
            input.extend_from_slice(format!("commit refs/heads/chain\nmark :{n}\ncommitter Test <test@example.invalid> {} +0000\ndata 1\nx\nfrom {parent}\n\n", n + 1).as_bytes());
        }
        git_input(fixture.root.path(), &["fast-import", "--quiet"], &input).await?;
        let output = git_input(
            fixture.root.path(),
            &["rev-list", "--max-count=512", "refs/heads/chain"],
            b"",
        )
        .await?;
        let ids = String::from_utf8(output)?
            .lines()
            .map(|line| {
                ObjectId::try_from(hex::decode(line)?.as_slice()).map_err(|error| error.into())
            })
            .collect::<Result<Vec<_>>>()?;
        assert_eq!(ids.len(), PAGE_OBJECTS);
        let root = tempfile::TempDir::new()?;
        let budget = DiskBudget::new(64 << 10);
        let spool = EdgeSpool::new(root.path(), budget.clone());
        let mut verifier = super::super::CanonicalVerifier::new(
            fixture.root.path(),
            format,
            &crate::native_resources::NativeResources::default()
                .scope(crate::native_resources::NativeClass::Foreground),
        )?;
        let mut witnesses: Vec<VerifiedObject> = Vec::with_capacity(PAGE_OBJECTS);
        for (n, oid) in ids.iter().copied().enumerate() {
            // Replaying an earlier range moves the shared file cursor. A later
            // append must explicitly seek to the admitted end, never overwrite.
            if let Some(previous) = witnesses.last_mut() {
                previous.replay(|_| Ok(()))?;
            }
            let witness = verifier.inspect_to_spool(oid, &spool, 100).await?;
            assert_eq!(witness.object().oid, oid);
            assert_eq!(witness.object().kind, ObjectKind::Commit);
            assert_eq!(witness.bytes, (2 * (format.bytes() + 1)) as u64);
            assert_eq!(
                witness.range.as_ref().ok_or("range")?.offset,
                (n * 2 * (format.bytes() + 1)) as u64
            );
            witnesses.push(witness);
            assert_eq!(std::fs::read_dir(root.path())?.count(), 1);
        }
        verifier.finish().await?;
        drop(spool);
        let admitted = (PAGE_OBJECTS * 2 * (format.bytes() + 1)) as u64;
        assert_eq!(budget.used(), admitted);
        // Reverse and repeat replay to exercise exact offsets and complete
        // digest rechecking, including the behavior needed by SQLite retries.
        for n in (0..PAGE_OBJECTS).rev() {
            let expected = [
                TypedEdge {
                    child: tree,
                    expected_kind: ObjectKind::Tree,
                },
                TypedEdge {
                    child: ids.get(n + 1).copied().unwrap_or(initial),
                    expected_kind: ObjectKind::Commit,
                },
            ];
            for _ in 0..2 {
                let mut actual = Vec::new();
                witnesses[n].replay(|page| {
                    actual.extend_from_slice(page);
                    Ok(())
                })?;
                assert_eq!(actual, expected);
            }
        }
        while witnesses.len() > 1 {
            drop(witnesses.pop());
        }
        assert_eq!(budget.used(), admitted);
        assert_eq!(std::fs::read_dir(root.path())?.count(), 1);
        drop(witnesses);
        assert_eq!(budget.used(), 0);
        assert_eq!(std::fs::read_dir(root.path())?.count(), 0);
    }
    Ok(())
}

#[test]
fn canceled_shared_writer_blocks_reuse_until_queued_work_drains() -> Result {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .max_blocking_threads(1)
        .enable_all()
        .build()?;
    let root = tempfile::TempDir::new()?;
    let budget = DiskBudget::new(100);
    let parent = crate::object_id(ObjectFormat::Sha256, ObjectKind::Tree, b"tree");
    let child = crate::object_id(ObjectFormat::Sha256, ObjectKind::Blob, b"blob");
    let edge = [TypedEdge {
        child,
        expected_kind: ObjectKind::Blob,
    }];
    let spool = EdgeSpool::new(root.path(), budget.clone());
    let mut sink = spool.sink(parent, 100)?;
    runtime.block_on(sink.append(parent, &edge))?;
    assert!(spool.sink(parent, 100).is_err());
    let (entered, wait_entered) = tokio::sync::oneshot::channel();
    let (release, wait) = std::sync::mpsc::channel();
    let blocker = runtime.spawn_blocking(move || {
        let _ = entered.send(());
        wait.recv().expect("release");
    });
    runtime.block_on(wait_entered)?;
    runtime.block_on(async {
        let mut pending = std::pin::pin!(sink.append(parent, &edge));
        std::future::poll_fn(|cx| {
            assert!(pending.as_mut().poll(cx).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
    });
    let object = CanonicalObject {
        oid: parent,
        kind: ObjectKind::Tree,
        size: 4,
        digest: [0; 32],
    };
    assert!(sink.complete(object).is_err());
    assert!(spool.sink(parent, 100).is_err());
    assert_eq!(budget.used(), 33);
    assert_eq!(std::fs::read_dir(root.path())?.count(), 1);
    release.send(())?;
    runtime.block_on(blocker)?;
    runtime.block_on(runtime.spawn_blocking(|| ()))?;
    assert_eq!(budget.used(), 66);
    // The canceled object's bytes remain admitted but cannot enter a witness.
    // A later writer starts after that orphan range, with its own exact digest.
    let next_child = crate::object_id(ObjectFormat::Sha256, ObjectKind::Blob, b"next");
    let next_edge = [TypedEdge {
        child: next_child,
        expected_kind: ObjectKind::Blob,
    }];
    let mut next = spool.sink(parent, 100)?;
    runtime.block_on(next.append(parent, &next_edge))?;
    let mut witness = next.complete(object)?;
    assert_eq!(witness.range.as_ref().ok_or("range")?.offset, 66);
    let mut actual = Vec::new();
    witness.replay(|edges| {
        actual.extend_from_slice(edges);
        Ok(())
    })?;
    assert_eq!(actual, next_edge);
    drop(spool);
    assert_eq!(budget.used(), 99);
    drop(witness);
    assert_eq!(budget.used(), 0);
    assert_eq!(std::fs::read_dir(root.path())?.count(), 0);
    Ok(())
}

#[tokio::test]
async fn failed_shared_growth_and_file_length_changes_reject_retained_ranges() -> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let parent = crate::object_id(format, ObjectKind::Tree, b"tree");
        let child = crate::object_id(format, ObjectKind::Blob, b"blob");
        let edge = [TypedEdge {
            child,
            expected_kind: ObjectKind::Blob,
        }];
        let object = CanonicalObject {
            oid: parent,
            kind: ObjectKind::Tree,
            size: 4,
            digest: [0; 32],
        };
        for failure in ["admission", "write", "append", "truncate"] {
            let root = tempfile::TempDir::new()?;
            let stride = (format.bytes() + 1) as u64;
            let budget = DiskBudget::new(if failure == "admission" { stride } else { 100 });
            let spool = EdgeSpool::new(root.path(), budget.clone());
            let mut sink = spool.sink(parent, 100)?;
            sink.append(parent, &edge).await?;
            let mut first = sink.complete(object)?;
            let mut retained = stride;
            if failure == "admission" {
                let mut denied = spool.sink(parent, 100)?;
                assert!(denied.append(parent, &edge).await.is_err());
                assert!(denied.complete(object).is_err());
                assert_eq!(budget.used(), stride);
            } else if failure == "write" {
                {
                    let range = first.range.as_ref().ok_or("range")?;
                    let mut storage = range.storage.lock().map_err(|_| "lock")?;
                    let file = storage.file.as_mut().ok_or("file")?.file_mut();
                    // Keep the same admitted inode but make writes fail after
                    // credit growth and a successful end seek.
                    let read_only = std::fs::File::open(file.path())?;
                    *file.as_file_mut() = read_only;
                }
                let mut broken = spool.sink(parent, 100)?;
                assert!(matches!(
                    broken.append(parent, &edge).await,
                    Err(ObjectReadError::Io(_))
                ));
                assert!(broken.complete(object).is_err());
                retained = stride * 2;
                assert_eq!(budget.used(), retained);
                let mut later = spool.sink(parent, 100)?;
                assert!(matches!(
                    later.append(parent, &edge).await,
                    Err(ObjectReadError::Malformed)
                ));
                assert!(later.complete(object).is_err());
            } else {
                let range = first.range.as_ref().ok_or("range")?;
                let storage = range.storage.lock().map_err(|_| "lock")?;
                let file = storage.file.as_ref().ok_or("file")?.file().as_file();
                file.set_len(if failure == "append" {
                    stride + 1
                } else {
                    stride - 1
                })?;
            }
            assert!(matches!(
                first.replay(|_| Ok(())),
                Err(MetadataError::Integrity)
            ));
            drop(spool);
            assert_eq!(budget.used(), retained);
            drop(first);
            assert_eq!(budget.used(), 0);
            assert_eq!(std::fs::read_dir(root.path())?.count(), 0);
        }
    }
    Ok(())
}
