use super::*;
use crate::git_objects::GitObjects;
use std::{collections::BTreeMap, process::Stdio};
use tokio::{io::AsyncWriteExt, process::Command};

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

pub(in crate::packs) async fn git(
    path: &Path,
    args: &[&str],
    input: Option<Vec<u8>>,
) -> Result<Vec<u8>> {
    let mut command = Command::new("git");
    command
        .env_clear()
        .env("PATH", std::env::var_os("PATH").ok_or("PATH")?)
        .env("HOME", path)
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env(
            "GIT_CONFIG_GLOBAL",
            if cfg!(windows) { "NUL" } else { "/dev/null" },
        )
        .env("LC_ALL", "C")
        .arg("-C")
        .arg(path)
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = command.spawn()?;
    if let Some(input) = input {
        child.stdin.take().ok_or("stdin")?.write_all(&input).await?;
    } else {
        drop(child.stdin.take());
    }
    let output = child.wait_with_output().await?;
    if !output.status.success() {
        return Err(String::from_utf8_lossy(&output.stderr).into_owned().into());
    }
    Ok(output.stdout)
}

pub(in crate::packs) struct Fixture {
    pub(in crate::packs) root: tempfile::TempDir,
    pub(in crate::packs) index: PackIndex,
    pub(in crate::packs) identity: SegmentIdentity,
    pub(in crate::packs) objects: BTreeMap<ObjectId, (CanonicalObject, Vec<TypedEdge>)>,
}
pub(in crate::packs) async fn fixture(format: ObjectFormat, blobs: usize) -> Result<Fixture> {
    // fast-import avoids one process per fixture object. The input is a test
    // fixture; production verification streams native bodies into bounded SQL.
    let mut input = b"commit refs/heads/main\ncommitter Metadata Test <test@example.invalid> 1 +0000\ndata 7\nfixture\n".to_vec();
    for n in 0..blobs {
        let body = format!("fixture body {n}\n");
        input.extend_from_slice(
            format!("M 100644 inline file-{n}\ndata {}\n{body}", body.len()).as_bytes(),
        );
    }
    input.extend_from_slice(b"\n");
    fixture_with_input(format, input).await
}

pub(in crate::packs) async fn fixture_with_input(
    format: ObjectFormat,
    input: Vec<u8>,
) -> Result<Fixture> {
    let root = tempfile::TempDir::new()?;
    git(
        root.path(),
        &[
            "init",
            "--bare",
            &format!("--object-format={}", format.as_str()),
        ],
        None,
    )
    .await?;
    git(root.path(), &["fast-import", "--quiet"], Some(input)).await?;
    git(
        root.path(),
        &[
            "-c",
            "user.name=Test",
            "-c",
            "user.email=test@example.invalid",
            "tag",
            "-a",
            "metadata",
            "-m",
            "annotation",
            "refs/heads/main",
        ],
        None,
    )
    .await?;
    git(root.path(), &["repack", "-ad"], None).await?;
    let index_path = std::fs::read_dir(root.path().join("objects/pack"))?
        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
        .find(|path| path.extension().is_some_and(|ext| ext == "idx"))
        .ok_or("index")?;
    let index = PackIndex::open(&index_path, format)?;
    let pack = std::fs::read(index_path.with_extension("pack"))?;
    let identity = SegmentIdentity {
        repository: [1; 16],
        operation: [2; 16],
        format,
        pack_digest: *blake3::hash(&pack).as_bytes(),
        git_checksum: index.pack_checksum(),
        first_ordinal: 0,
        object_count: index.len(),
    };
    let ids = index.ids().collect::<std::io::Result<Vec<_>>>()?;
    let mut reader = GitObjects::packed(
        root.path(),
        ids,
        &crate::native_resources::NativeResources::default()
            .scope(crate::native_resources::NativeClass::Foreground),
    )?;
    let mut objects = BTreeMap::new();
    while let Some(oid) = reader.next().await? {
        let (kind, body) = reader.read(oid).await?.body().await?;
        let edges = crate::graph::edges(format, kind, &body)
            .ok_or("graph")?
            .into_iter()
            .map(|(child, expected_kind)| {
                Ok(TypedEdge {
                    child,
                    expected_kind: expected_kind.ok_or("typed edge")?,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        objects.insert(
            oid,
            (
                CanonicalObject {
                    oid,
                    kind,
                    size: body.len() as u64,
                    digest: *blake3::hash(&body).as_bytes(),
                },
                edges,
            ),
        );
    }
    reader.finish().await?;
    Ok(Fixture {
        root,
        index,
        identity,
        objects,
    })
}
pub(in crate::packs) fn limits() -> MetadataLimits {
    MetadataLimits {
        max_file_bytes: 16 << 20,
        cache_kib: 64,
    }
}
pub(in crate::packs) fn builder(
    fixture: &Fixture,
    budget: DiskBudget,
    identity: SegmentIdentity,
) -> Result<MetadataBuilder> {
    Ok(MetadataBuilder::new(
        fixture.root.path(),
        budget,
        identity,
        limits(),
    )?)
}
pub(in crate::packs) fn fill(
    builder: &mut MetadataBuilder,
    objects: &[(CanonicalObject, Vec<TypedEdge>)],
) -> Result {
    for objects in objects.chunks(PAGE_OBJECTS) {
        builder.put_objects(
            &objects
                .iter()
                .map(|(object, _)| *object)
                .collect::<Vec<_>>(),
        )?;
    }
    for (object, edges) in objects {
        for edges in edges.chunks(PAGE_OBJECTS) {
            builder.put_edges(object.oid, edges)?;
        }
    }
    Ok(())
}

pub(in crate::packs) async fn prepared_segment() -> Result<(
    tempfile::TempDir,
    std::sync::Arc<MetadataSegment>,
    DiskBudget,
)> {
    let fixture = fixture(ObjectFormat::Sha256, 4).await?;
    let budget = DiskBudget::new(128 << 20);
    let mut writer = builder(&fixture, budget.clone(), fixture.identity)?;
    fill(
        &mut writer,
        &fixture.objects.values().cloned().collect::<Vec<_>>(),
    )?;
    let segment = std::sync::Arc::new(writer.seal(&fixture.index)?);
    Ok((fixture.root, segment, budget))
}

#[tokio::test]
async fn native_objects_roundtrip_with_deterministic_inventory_and_duplicate_replay() -> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let fixture = fixture(format, 4).await?;
        let budget = DiskBudget::new(128 << 20);
        let objects = fixture.objects.values().cloned().collect::<Vec<_>>();
        let mut first = builder(&fixture, budget.clone(), fixture.identity)?;
        fill(&mut first, &objects)?;
        fill(&mut first, &objects)?;
        let first = first.seal(&fixture.index)?;
        let mut second = builder(&fixture, budget.clone(), fixture.identity)?;
        fill(&mut second, &objects.into_iter().rev().collect::<Vec<_>>())?;
        let second = second.seal(&fixture.index)?;
        assert_eq!(
            first.descriptor().inventory_digest,
            second.descriptor().inventory_digest
        );
        assert_eq!(
            first.descriptor().identity.object_count,
            fixture.objects.len() as u32
        );
        for (oid, (object, expected_edges)) in &fixture.objects {
            let header = first.header(*oid)?.ok_or("header")?;
            assert_eq!(header.object, *object);
            assert_eq!(header.edge_count, expected_edges.len() as u64);
            assert_eq!(first.edges_after(*oid, None)?, *expected_edges);
        }
        assert!(
            first
                .header(if format == ObjectFormat::Sha1 {
                    ObjectId::Sha256([3; 32])
                } else {
                    ObjectId::Sha1([3; 20])
                })?
                .is_none()
        );
        drop(first);
        drop(second);
        assert_eq!(budget.used(), 0);
    }
    Ok(())
}

#[tokio::test]
async fn shard_page_limit_rolls_back_then_allows_a_valid_smaller_inventory() -> Result {
    let fixture = fixture(ObjectFormat::Sha256, 4).await?;
    let budget = DiskBudget::new(1 << 20);
    let mut writer = MetadataBuilder::new(
        fixture.root.path(),
        budget.clone(),
        fixture.identity,
        MetadataLimits {
            max_file_bytes: 32 << 10,
            cache_kib: 64,
        },
    )?;
    let fake = (1_u32..=512)
        .map(|n| {
            let mut oid = [0; 32];
            oid[..4].copy_from_slice(&n.to_be_bytes());
            CanonicalObject {
                oid: ObjectId::Sha256(oid),
                kind: ObjectKind::Blob,
                size: 0,
                digest: *blake3::hash(b"").as_bytes(),
            }
        })
        .collect::<Vec<_>>();
    assert!(matches!(
        writer.put_objects(&fake),
        Err(MetadataError::Limit)
    ));
    fill(
        &mut writer,
        &fixture.objects.values().cloned().collect::<Vec<_>>(),
    )?;
    let segment = writer.seal(&fixture.index)?;
    assert!(segment.descriptor().size <= 32 << 10);
    assert_eq!(budget.used(), segment.descriptor().size);
    drop(segment);
    assert_eq!(budget.used(), 0);
    Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn failed_file_cleanup_retains_disk_admission_for_workspace_recovery() -> Result {
    let (root, segment, budget) = prepared_segment().await?;
    let charged = budget.used();
    let old = segment.path().to_owned();
    let retained = root.path().join("retained-metadata");
    std::fs::rename(&old, &retained)?;
    std::fs::create_dir(&old)?;
    drop(segment);
    assert!(retained.exists());
    assert_eq!(budget.used(), charged);
    std::fs::remove_dir(&old)?;
    std::fs::remove_file(&retained)?;
    // A failed cleanup conservatively retains its original reservation. A
    // fresh workspace startup reclaims files before allocating a fresh budget.
    assert_eq!(budget.used(), charged);
    Ok(())
}

#[tokio::test]
async fn sealed_segments_roundtrip_through_authenticated_storage_and_reject_bad_bindings() -> Result
{
    use canopy_object_storage::artifact::{ArtifactKind, ArtifactStore};
    use object_store::{ObjectStore, ObjectStoreExt, memory::InMemory};
    use std::sync::Arc;

    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let fixture = fixture(format, 20).await?;
        let budget = DiskBudget::new(128 << 20);
        let mut writer = builder(&fixture, budget.clone(), fixture.identity)?;
        fill(
            &mut writer,
            &fixture.objects.values().cloned().collect::<Vec<_>>(),
        )?;
        let segment = Arc::new(writer.seal(&fixture.index)?);
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let artifacts = ArtifactStore::new(Arc::clone(&store), fixture.identity.repository);
        let (first, second) = tokio::join!(
            Arc::clone(&segment).upload(&artifacts),
            Arc::clone(&segment).upload(&artifacts)
        );
        let stored = first?;
        assert_eq!(stored, second?);
        let download_root = tempfile::TempDir::new()?;
        let downloaded = MetadataSegment::download(
            download_root.path(),
            budget.clone(),
            &artifacts,
            stored,
            limits(),
        )
        .await?;
        assert_eq!(downloaded.descriptor(), segment.descriptor());
        for oid in fixture.objects.keys() {
            assert_eq!(downloaded.header(*oid)?, segment.header(*oid)?);
            assert_eq!(
                downloaded.edges_after(*oid, None)?,
                segment.edges_after(*oid, None)?
            );
        }
        drop(downloaded);
        assert_eq!(budget.used(), stored.segment.size);

        let other = ArtifactStore::new(Arc::clone(&store), [99; 16]);
        assert!(matches!(
            Arc::clone(&segment).upload(&other).await,
            Err(MetadataError::Integrity)
        ));
        assert!(matches!(
            MetadataSegment::download(
                download_root.path(),
                budget.clone(),
                &other,
                stored,
                limits()
            )
            .await,
            Err(MetadataError::Integrity)
        ));
        let mut wrong = stored;
        wrong.artifact.size += 1;
        assert!(matches!(
            MetadataSegment::download(
                download_root.path(),
                budget.clone(),
                &artifacts,
                wrong,
                limits()
            )
            .await,
            Err(MetadataError::Integrity)
        ));
        wrong = stored;
        wrong.artifact.digest[0] ^= 1;
        assert!(matches!(
            MetadataSegment::download(
                download_root.path(),
                budget.clone(),
                &artifacts,
                wrong,
                limits()
            )
            .await,
            Err(MetadataError::Integrity)
        ));
        assert!(matches!(
            MetadataSegment::download(
                download_root.path(),
                DiskBudget::new(stored.segment.size - 1),
                &artifacts,
                stored,
                limits()
            )
            .await,
            Err(MetadataError::Budget(_))
        ));

        let path = artifacts.path(
            canopy_object_storage::artifact::ArtifactKey {
                operation: fixture.identity.operation,
                binding_digest: fixture.identity.pack_digest,
                kind: ArtifactKind::Metadata,
            },
            stored.artifact.digest,
        )?;
        assert!(path.as_ref().contains("/metadata/"));
        let part_path = canopy_object_storage::external::part(&path, 0);
        store
            .put(
                &part_path,
                bytes::Bytes::from(vec![0; stored.segment.size as usize]).into(),
            )
            .await?;
        assert!(
            MetadataSegment::download(
                download_root.path(),
                budget.clone(),
                &artifacts,
                stored,
                limits()
            )
            .await
            .is_err()
        );
        assert_eq!(budget.used(), stored.segment.size);
        assert_eq!(std::fs::read_dir(download_root.path())?.count(), 0);
        drop(segment);
        assert_eq!(budget.used(), 0);
    }
    Ok(())
}

#[tokio::test]
async fn header_and_typed_edge_conflicts_roll_back_entire_batches() -> Result {
    let fixture = fixture(ObjectFormat::Sha1, 4).await?;
    let budget = DiskBudget::new(128 << 20);
    let mut writer = builder(&fixture, budget.clone(), fixture.identity)?;
    fill(
        &mut writer,
        &fixture.objects.values().cloned().collect::<Vec<_>>(),
    )?;
    let original = fixture
        .objects
        .values()
        .find(|(object, _)| object.kind == ObjectKind::Blob)
        .ok_or("blob")?
        .0;
    let mut extra = original;
    extra.oid = ObjectId::Sha1([37; 20]);
    assert!(!fixture.objects.contains_key(&extra.oid));
    let mut conflict = original;
    conflict.digest = [38; 32];
    assert!(matches!(
        writer.put_objects(&[extra, conflict]),
        Err(MetadataError::IdentityConflict)
    ));
    let (tree, edges) = fixture
        .objects
        .values()
        .find(|(object, _)| object.kind == ObjectKind::Tree)
        .ok_or("tree")?;
    assert!(matches!(
        writer.put_edges(
            tree.oid,
            &[
                TypedEdge {
                    child: extra.oid,
                    expected_kind: ObjectKind::Blob
                },
                TypedEdge {
                    child: edges[0].child,
                    expected_kind: ObjectKind::Tree
                }
            ]
        ),
        Err(MetadataError::IdentityConflict)
    ));
    let segment = writer.seal(&fixture.index)?;
    assert!(segment.header(extra.oid)?.is_none());
    assert_eq!(segment.edges_after(tree.oid, None)?, *edges);
    drop(segment);
    assert_eq!(budget.used(), 0);
    Ok(())
}

#[tokio::test]
async fn exact_native_ordinal_shards_cover_a_pack_without_rescanning_prefixes() -> Result {
    let fixture = fixture(ObjectFormat::Sha256, 9).await?;
    let budget = DiskBudget::new(128 << 20);
    let split = fixture.index.len() / 2;
    let objects = fixture.objects.values().cloned().collect::<Vec<_>>();
    let mut descriptors = Vec::new();
    for (first, count) in [(0, split), (split, fixture.index.len() - split)] {
        let identity = SegmentIdentity {
            first_ordinal: first,
            object_count: count,
            ..fixture.identity
        };
        let mut writer = builder(&fixture, budget.clone(), identity)?;
        fill(
            &mut writer,
            &objects[first as usize..(first + count) as usize],
        )?;
        let segment = writer.seal(&fixture.index)?;
        descriptors.push(segment.descriptor());
        assert_eq!(segment.headers_after(None)?.len(), count as usize);
    }
    assert!(descriptors[0].last_oid < descriptors[1].first_oid);
    assert_eq!(
        descriptors[0].identity.object_count + descriptors[1].identity.object_count,
        fixture.index.len()
    );
    assert_eq!(budget.used(), 0);
    Ok(())
}

#[tokio::test]
async fn wrong_index_inventory_and_missing_structural_edges_cannot_seal() -> Result {
    let fixture = fixture(ObjectFormat::Sha1, 2).await?;
    let budget = DiskBudget::new(128 << 20);
    let mut writer = builder(&fixture, budget.clone(), fixture.identity)?;
    let objects = fixture
        .objects
        .values()
        .map(|(object, _)| *object)
        .collect::<Vec<_>>();
    writer.put_objects(&objects)?;
    assert!(matches!(
        writer.seal(&fixture.index),
        Err(MetadataError::Integrity)
    ));
    let identity = SegmentIdentity {
        first_ordinal: 1,
        ..fixture.identity
    };
    let mut writer = builder(&fixture, budget.clone(), identity)?;
    fill(
        &mut writer,
        &fixture.objects.values().cloned().collect::<Vec<_>>(),
    )?;
    assert!(matches!(
        writer.seal(&fixture.index),
        Err(MetadataError::Integrity)
    ));
    assert_eq!(budget.used(), 0);
    Ok(())
}

#[tokio::test]
async fn stored_child_kind_must_match_the_typed_dependency() -> Result {
    let fixture = fixture(ObjectFormat::Sha1, 2).await?;
    let budget = DiskBudget::new(128 << 20);
    let mut writer = builder(&fixture, budget.clone(), fixture.identity)?;
    let mut objects = fixture.objects.values().cloned().collect::<Vec<_>>();
    let tree = objects
        .iter_mut()
        .find(|(object, _)| object.kind == ObjectKind::Tree)
        .ok_or("tree")?;
    tree.1[0].expected_kind = ObjectKind::Tree;
    fill(&mut writer, &objects)?;
    assert!(matches!(
        writer.seal(&fixture.index),
        Err(MetadataError::Integrity)
    ));
    assert_eq!(budget.used(), 0);
    Ok(())
}

#[tokio::test]
async fn artifact_tampering_and_wrong_repository_are_rejected_before_reading_rows() -> Result {
    let fixture = fixture(ObjectFormat::Sha1, 2).await?;
    let budget = DiskBudget::new(128 << 20);
    let mut writer = builder(&fixture, budget.clone(), fixture.identity)?;
    fill(
        &mut writer,
        &fixture.objects.values().cloned().collect::<Vec<_>>(),
    )?;
    let segment = writer.seal(&fixture.index)?;
    for mode in ["bytes", "identity", "size"] {
        let file = tempfile::NamedTempFile::new_in(fixture.root.path())?;
        std::fs::copy(segment.path(), file.path())?;
        let mut descriptor = segment.descriptor();
        match mode {
            "bytes" => {
                use std::io::{Seek, SeekFrom, Write};
                let mut handle = file.reopen()?;
                handle.seek(SeekFrom::Start(100))?;
                handle.write_all(&[255])?;
            }
            "identity" => descriptor.identity.repository = [39; 16],
            "size" => descriptor.size += 1,
            _ => unreachable!(),
        }
        let reservation = budget.try_reserve(descriptor.size)?;
        assert!(matches!(
            MetadataSegment::open(file, reservation, descriptor, 64),
            Err(MetadataError::Integrity)
        ));
    }
    drop(segment);
    assert_eq!(budget.used(), 0);
    Ok(())
}

#[tokio::test]
async fn large_inventory_and_wide_tree_use_bounded_pages_and_disk_admission() -> Result {
    let fixture = fixture(ObjectFormat::Sha1, 1600).await?;
    let budget = DiskBudget::new(128 << 20);
    let mut writer = builder(&fixture, budget.clone(), fixture.identity)?;
    fill(
        &mut writer,
        &fixture.objects.values().cloned().collect::<Vec<_>>(),
    )?;
    let segment = writer.seal(&fixture.index)?;
    assert_eq!(budget.used(), segment.descriptor().size);
    let mut after = None;
    let mut count = 0;
    loop {
        let page = segment.headers_after(after)?;
        if page.is_empty() {
            break;
        }
        assert!(page.len() <= PAGE_OBJECTS);
        assert!(
            page.windows(2)
                .all(|pair| pair[0].object.oid < pair[1].object.oid)
        );
        count += page.len();
        after = page.last().map(|header| header.object.oid);
    }
    assert_eq!(count, fixture.objects.len());
    let tree = fixture
        .objects
        .values()
        .find(|(object, _)| object.kind == ObjectKind::Tree)
        .ok_or("tree")?
        .0;
    let mut after = None;
    let mut edges = 0;
    loop {
        let page = segment.edges_after(tree.oid, after)?;
        if page.is_empty() {
            break;
        }
        assert!(page.len() <= PAGE_OBJECTS);
        edges += page.len();
        after = page.last().map(|edge| edge.child);
    }
    assert_eq!(edges, 1600);
    drop(segment);
    assert_eq!(budget.used(), 0);
    assert!(matches!(
        MetadataBuilder::new(
            fixture.root.path(),
            DiskBudget::new(growth::INITIAL_BYTES * 3 - 1),
            fixture.identity,
            limits()
        ),
        Err(MetadataError::Budget(_))
    ));
    Ok(())
}
