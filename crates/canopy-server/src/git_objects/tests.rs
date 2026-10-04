use std::collections::BTreeMap;
use tokio::process::Command;

use super::*;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

async fn git(path: &Path, args: &[&str]) -> TestResult<Vec<u8>> {
    let output = Command::new("git")
        .arg("-C")
        .arg(path)
        .args(args)
        .output()
        .await?;
    if !output.status.success() {
        return Err(String::from_utf8_lossy(&output.stderr).into_owned().into());
    }
    Ok(output.stdout)
}

async fn oid(path: &Path, name: &str) -> TestResult<crate::ObjectId> {
    let output = git(path, &["rev-parse", name]).await?;
    Ok(parse_oid(output.trim_ascii())?)
}

async fn fixture() -> TestResult<tempfile::TempDir> {
    let directory = tempfile::TempDir::new()?;
    let path = directory.path();
    git(path, &["init", "-b", "main"]).await?;
    git(path, &["config", "user.name", "Canopy Test"]).await?;
    git(path, &["config", "user.email", "canopy@example.invalid"]).await?;
    for number in 0..256 {
        tokio::fs::write(path.join(format!("file-{number}")), format!("{number}\0\n")).await?;
    }
    git(path, &["add", "."]).await?;
    git(path, &["commit", "-m", "base"]).await?;
    Ok(directory)
}

async fn collect(
    path: &Path,
    included: Vec<crate::ObjectId>,
    excluded: Vec<crate::ObjectId>,
) -> TestResult<BTreeMap<crate::ObjectId, (ObjectKind, Vec<u8>)>> {
    let mut objects = GitObjects::start(
        &path.join(".git"),
        included,
        excluded,
        &crate::native_resources::NativeResources::default()
            .scope(crate::native_resources::NativeClass::Foreground),
    )?;
    let mut result = BTreeMap::new();
    while let Some(oid) = objects.next().await? {
        let object = objects.read(oid).await?.body().await?;
        assert!(result.insert(oid, object).is_none(), "duplicate object");
    }
    objects.finish().await?;
    Ok(result)
}

#[tokio::test]
async fn incremental_walk_omits_old_history_and_ignores_replacements() -> TestResult {
    let directory = fixture().await?;
    let path = directory.path();
    let old = oid(path, "HEAD").await?;
    let original_blob = oid(path, "HEAD:file-1").await?;
    let body = b"new\0binary\ncontents\xff";
    tokio::fs::write(path.join("new\nfile"), body).await?;
    git(path, &["add", "."]).await?;
    git(path, &["commit", "-m", "incremental"]).await?;
    git(path, &["tag", "-a", "inner", "-m", "inner"]).await?;
    git(path, &["tag", "-a", "outer", "inner", "-m", "outer"]).await?;
    let blob = oid(path, "HEAD:new\nfile").await?;
    git(
        path,
        &["replace", &hex::encode(original_blob), &hex::encode(blob)],
    )
    .await?;
    let tip = oid(path, "HEAD").await?;
    let tree = oid(path, "HEAD^{tree}").await?;
    let inner = oid(path, "inner").await?;
    let outer = oid(path, "outer").await?;
    let objects = collect(path, vec![tip, outer], vec![old]).await?;
    assert_eq!(
        objects
            .keys()
            .copied()
            .collect::<std::collections::BTreeSet<_>>(),
        [tip, tree, blob, inner, outer].into_iter().collect()
    );
    assert_eq!(objects[&blob], (ObjectKind::Blob, body.to_vec()));
    // Reading a replaced OID must return its original canonical bytes.
    let objects = collect(path, vec![original_blob], vec![]).await?;
    assert_eq!(
        objects[&original_blob],
        (ObjectKind::Blob, b"1\0\n".to_vec())
    );
    Ok(())
}

#[tokio::test]
async fn direct_tree_blob_and_tag_roots_recover_all_required_objects() -> TestResult {
    let directory = fixture().await?;
    let path = directory.path();
    let tree = oid(path, "HEAD^{tree}").await?;
    let blob = oid(path, "HEAD:file-0").await?;
    git(
        path,
        &["tag", "-a", "tree-tag", &hex::encode(tree), "-m", "tree"],
    )
    .await?;
    let tag = oid(path, "tree-tag").await?;
    let objects = collect(path, vec![tree, blob, tag], vec![]).await?;
    assert_eq!(objects.len(), 258); // 256 blobs, one tree and its annotated tag.
    assert_eq!(objects[&blob], (ObjectKind::Blob, b"0\0\n".to_vec()));
    assert_eq!(objects[&tag].0, ObjectKind::Tag);
    assert!(collect(path, vec![tag], vec![tree, tag]).await?.is_empty());
    Ok(())
}

#[tokio::test]
async fn missing_walk_root_cannot_finish_successfully() -> TestResult {
    let directory = fixture().await?;
    let mut objects = GitObjects::start(
        &directory.path().join(".git"),
        vec![crate::ObjectId::Sha1([42; 20])],
        vec![],
        &crate::native_resources::NativeResources::default()
            .scope(crate::native_resources::NativeClass::Foreground),
    )?;
    assert_eq!(objects.next().await?, None);
    assert!(matches!(
        objects.finish().await,
        Err(ObjectReadError::Git(_))
    ));
    Ok(())
}

#[tokio::test]
async fn malformed_and_oversized_batches_fail_before_publication() {
    let oid = object_id(crate::ObjectFormat::Sha1, ObjectKind::Blob, b"abc");
    let hex = hex::encode(oid);
    for data in [
        format!("{hex} missing\n"),
        format!("{hex} blob 3\nab"),
        format!("{hex} blob 3\nabcX"),
        format!("{hex} blob 3\nxyz\n"),
        format!("{} blob 3\nabc\n", "0".repeat(40)),
        "x".repeat(HEADER_LIMIT + 1),
    ] {
        let mut input = data.as_bytes();
        if let Ok(object) = open_object(&mut input, oid).await {
            assert!(object.body().await.is_err());
        }
    }
    for (kind, limit) in [
        ("blob", i64::MAX as u64),
        ("tree", isize::MAX as u64),
        ("commit", isize::MAX as u64),
        ("tag", isize::MAX as u64),
    ] {
        // No body supplied: the size must be rejected before attempting a body read.
        let data = format!("{hex} {kind} {}\n", limit + 1);
        assert!(matches!(
            open_object(&mut data.as_bytes(), oid).await,
            Err(ObjectReadError::TooLarge)
        ));
    }
}

#[cfg(unix)]
#[tokio::test]
async fn dropping_reader_kills_both_children() -> TestResult {
    let directory = fixture().await?;
    let tip = oid(directory.path(), "HEAD").await?;
    let objects = GitObjects::start(
        &directory.path().join(".git"),
        vec![tip],
        vec![],
        &crate::native_resources::NativeResources::default()
            .scope(crate::native_resources::NativeClass::Foreground),
    )?;
    let pids = [
        objects
            .walk
            .as_ref()
            .unwrap()
            .process
            .worker
            .child
            .id()
            .unwrap(),
        objects.batch.worker.child.id().unwrap(),
    ];
    drop(objects);
    timeout(Duration::from_secs(5), async {
        for pid in pids {
            loop {
                // SAFETY: a positive PID and signal zero only query process existence.
                if unsafe { libc::kill(pid as i32, 0) } == -1 {
                    assert_eq!(io::Error::last_os_error().raw_os_error(), Some(libc::ESRCH));
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
    })
    .await?;
    Ok(())
}

#[tokio::test]
async fn batch_reads_large_blob_across_pipe_buffers() -> TestResult {
    let directory = fixture().await?;
    let body = vec![31; 40 * 1024 * 1024];
    tokio::fs::write(directory.path().join("large"), &body).await?;
    let output = git(directory.path(), &["hash-object", "-w", "large"]).await?;
    let oid = parse_oid(output.trim_ascii())?;
    let mut objects = GitObjects::start(
        &directory.path().join(".git"),
        vec![oid],
        vec![],
        &crate::native_resources::NativeResources::default()
            .scope(crate::native_resources::NativeClass::Foreground),
    )?;
    assert_eq!(objects.next().await?, Some(oid));
    let mut object = objects.read(oid).await?;
    let store = crate::blob::LargeBlobStore::new(
        std::sync::Arc::new(object_store::memory::InMemory::new()),
        [1; 16],
    );
    let reference = store.put(oid, object.size, &mut object.reader).await?;
    object.finish().await?;
    assert_eq!(objects.next().await?, None);
    objects.finish().await?;
    let mut reader = store.read(&reference).await?;
    let mut offset = 0;
    while let Some(bytes) = reader.next().await? {
        assert_eq!(bytes.as_ref(), &body[offset..offset + bytes.len()]);
        offset += bytes.len();
    }
    assert_eq!(offset, body.len());
    struct BlobSink;
    impl EdgeSink for BlobSink {
        async fn append(
            &mut self,
            _: crate::ObjectId,
            _: &[crate::packs::metadata::TypedEdge],
        ) -> Result<(), ObjectReadError> {
            Err(ObjectReadError::Malformed)
        }
    }
    let mut verifier = crate::packs::verification::CanonicalVerifier::new(
        &directory.path().join(".git"),
        oid.format(),
        &crate::native_resources::NativeResources::default()
            .scope(crate::native_resources::NativeClass::Foreground),
    )?;
    let canonical = verifier.inspect(oid, &mut BlobSink).await?;
    assert_eq!(canonical.size, body.len() as u64);
    assert_eq!(canonical.kind, ObjectKind::Blob);
    assert_eq!(canonical.digest, *blake3::hash(&body).as_bytes());
    verifier.finish().await?;
    Ok(())
}

#[tokio::test]
async fn missing_walk_streams_requested_history_without_unrelated_blobs() -> TestResult {
    let directory = fixture().await?;
    let path = directory.path();
    let root = oid(path, "HEAD").await?;
    let output = git(path, &["ls-tree", "-r", "--format=%(objectname)", "HEAD"]).await?;
    let expected = output
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
        .map(parse_oid)
        .collect::<Result<std::collections::BTreeSet<_>, _>>()?;
    tokio::fs::write(path.join("unrelated"), b"not in requested history\n").await?;
    git(path, &["add", "unrelated"]).await?;
    git(path, &["commit", "-m", "Unrequested descendant"]).await?;
    let unrelated = oid(path, "HEAD:unrelated").await?;
    for oid in expected.iter().chain(std::iter::once(&unrelated)) {
        let hex = hex::encode(oid);
        tokio::fs::remove_file(path.join(".git/objects").join(&hex[..2]).join(&hex[2..])).await?;
    }
    let mut walk = GitObjectWalk::missing(
        &path.join(".git"),
        vec![root],
        None,
        &crate::native_resources::NativeResources::default()
            .scope(crate::native_resources::NativeClass::Foreground),
    )?;
    let mut found = std::collections::BTreeSet::new();
    while let Some(oid) = walk.next().await? {
        assert!(found.insert(oid), "duplicate missing object");
    }
    walk.finish().await?;
    assert_eq!(found, expected);
    Ok(())
}

#[tokio::test]
async fn streamed_inspection_rejects_hash_mismatch_partial_bodies_bad_separators_and_invalid_graphs()
-> TestResult {
    struct NoEdges;
    impl EdgeSink for NoEdges {
        async fn append(
            &mut self,
            _: crate::ObjectId,
            _: &[crate::packs::metadata::TypedEdge],
        ) -> Result<(), ObjectReadError> {
            Ok(())
        }
    }
    for format in [crate::ObjectFormat::Sha1, crate::ObjectFormat::Sha256] {
        let body = b"canonical bytes";
        let oid = crate::object_id(format, ObjectKind::Blob, body);
        for (bytes, size, separator) in [
            (&b"incorrect bytes"[..], body.len(), b'\n'),
            (&body[..2], body.len(), b'\n'),
            (&body[..], body.len(), b'X'),
            (&body[..], 1, b'\n'),
        ] {
            let mut frame = format!("{} blob {size}\n", hex::encode(oid)).into_bytes();
            frame.extend_from_slice(bytes);
            frame.push(separator);
            let mut input = frame.as_slice();
            let object = open_object(&mut input, oid).await?;
            assert!(object.inspect_graph(&mut NoEdges).await.is_err());
        }
        let body = b"100644 unfinished";
        let oid = crate::object_id(format, ObjectKind::Tree, body);
        let mut frame = format!("{} tree {}\n", hex::encode(oid), body.len()).into_bytes();
        frame.extend_from_slice(body);
        frame.push(b'\n');
        let mut input = frame.as_slice();
        let object = open_object(&mut input, oid).await?;
        assert!(matches!(
            object.inspect_graph(&mut NoEdges).await,
            Err(ObjectReadError::Malformed)
        ));
    }
    Ok(())
}

#[tokio::test]
async fn verified_body_checks_catalog_fingerprint_size_kind_and_limit_for_both_formats()
-> TestResult {
    for format in [crate::ObjectFormat::Sha1, crate::ObjectFormat::Sha256] {
        let body = b"bounded canonical body";
        let expected = crate::packs::metadata::CanonicalObject {
            oid: object_id(format, ObjectKind::Blob, body),
            kind: ObjectKind::Blob,
            size: body.len() as u64,
            digest: *blake3::hash(body).as_bytes(),
        };
        let frame = format!("{} blob {}\n", hex::encode(expected.oid), expected.size)
            .into_bytes()
            .into_iter()
            .chain(body.iter().copied())
            .chain(*b"\n")
            .collect::<Vec<_>>();
        let mut input = frame.as_slice();
        let object = open_object(&mut input, expected.oid).await?;
        assert_eq!(
            object
                .body_verified(expected, body.len(), std::sync::Arc::new(()))
                .await?,
            body
        );
        for variant in 0..4 {
            let mut metadata = expected;
            let mut limit = body.len();
            match variant {
                0 => metadata.digest[0] ^= 1,
                1 => metadata.kind = ObjectKind::Tree,
                2 => metadata.size += 1,
                _ => limit -= 1,
            }
            let mut input = frame.as_slice();
            let object = open_object(&mut input, expected.oid).await?;
            assert!(
                object
                    .body_verified(metadata, limit, std::sync::Arc::new(()))
                    .await
                    .is_err()
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn verified_batch_reuses_only_complete_verified_frames_and_poison_refuses_finish()
-> TestResult {
    let directory = fixture().await?;
    let blob = oid(directory.path(), "HEAD:file-0").await?;
    let expected = crate::packs::metadata::CanonicalObject {
        oid: blob,
        kind: ObjectKind::Blob,
        size: 3,
        digest: *blake3::hash(b"0\0\n").as_bytes(),
    };
    let resources = crate::native_resources::NativeResources::default();
    let scope = resources.scope(crate::native_resources::NativeClass::Foreground);
    let mut reader = GitObjects::batch(&directory.path().join(".git"), &scope)?;
    assert!(matches!(
        reader.read_verified(expected, 2).await,
        Err(ObjectReadError::TooLarge)
    ));
    assert_eq!(reader.read_verified(expected, 3).await?, b"0\0\n");
    assert_eq!(reader.read_verified(expected, 3).await?, b"0\0\n");
    reader.finish().await?;
    let mut reader = GitObjects::batch(&directory.path().join(".git"), &scope)?;
    let mut corrupt = expected;
    corrupt.digest[0] ^= 1;
    assert!(matches!(
        reader.read_verified(corrupt, 3).await,
        Err(ObjectReadError::Malformed)
    ));
    assert!(matches!(
        reader.read_verified(expected, 3).await,
        Err(ObjectReadError::Malformed)
    ));
    assert!(matches!(
        reader.finish().await,
        Err(ObjectReadError::Malformed)
    ));
    Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn owned_batch_cancellation_releases_owner_only_after_native_reaping() -> TestResult {
    use std::sync::{
        Arc,
        atomic::{AtomicU32, Ordering},
    };
    struct Owner {
        pid: Arc<AtomicU32>,
        released: tokio::sync::oneshot::Sender<bool>,
    }
    impl Drop for Owner {
        fn drop(&mut self) {
            let pid = self.pid.load(Ordering::Acquire);
            // SAFETY: signal zero only queries a PID assigned by this fixture.
            let gone = unsafe { libc::kill(pid as i32, 0) } == -1
                && io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH);
            let (unused, _) = tokio::sync::oneshot::channel();
            let released = std::mem::replace(&mut self.released, unused);
            let _ = released.send(gone);
        }
    }
    let directory = fixture().await?;
    let pid = Arc::new(AtomicU32::new(0));
    let (released, observed) = tokio::sync::oneshot::channel();
    let owner = Arc::new(Owner {
        pid: Arc::clone(&pid),
        released,
    });
    let reader = GitObjects::batch_owned(
        &directory.path().join(".git"),
        &crate::native_resources::NativeResources::default()
            .scope(crate::native_resources::NativeClass::Foreground),
        owner,
    )?;
    pid.store(
        reader.batch.worker.child.id().ok_or("native PID")?,
        Ordering::Release,
    );
    drop(reader);
    assert!(timeout(Duration::from_secs(5), observed).await??);
    Ok(())
}
