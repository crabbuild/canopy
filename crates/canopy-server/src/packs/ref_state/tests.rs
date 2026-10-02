use super::*;
use crate::{ObjectId, RefUpdate};
use cellule_runtime::codec::WireValue;
use futures_core::Stream;
use object_store::{ObjectStore, memory::InMemory};
use std::{future::poll_fn, pin::Pin};

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;
fn operation(n: u64) -> [u8; 16] {
    let mut op = *b"CANOPY0100000000";
    op[8..].copy_from_slice(&n.to_be_bytes());
    op
}
fn repository() -> [u8; 16] {
    let mut repo = [1; 16];
    repo[6] = 0x41;
    repo[8] = 0x81;
    repo
}
fn oid(n: u64, format: ObjectFormat) -> ObjectId {
    let mut bytes = vec![0; format.bytes()];
    bytes[..8].copy_from_slice(&n.to_be_bytes());
    bytes.try_into().unwrap()
}
fn state(n: Option<u64>, version: i64, format: ObjectFormat) -> RefExpectation {
    RefExpectation {
        oid: n.map(|n| oid(n, format)),
        version,
    }
}
fn index(format: ObjectFormat) -> (RefStateIndex, Arc<ArtifactStore>, Arc<dyn ObjectStore>) {
    let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let store = Arc::new(ArtifactStore::new(objects.clone(), repository()));
    (RefStateIndex::new(store.clone(), format), store, objects)
}
fn plan(updates: Vec<RefUpdate>) -> PushPlan {
    PushPlan {
        actor: "alice".into(),
        updates,
    }
}
fn update(name: &str, expected: Option<RefExpectation>, new_oid: Option<ObjectId>) -> RefUpdate {
    RefUpdate {
        name: name.into(),
        expected,
        new_oid,
    }
}
async fn listed(objects: &dyn ObjectStore) -> Result<Vec<object_store::ObjectMeta>> {
    let mut stream = objects.list(None);
    let mut metas = Vec::new();
    while let Some(meta) = poll_fn(|cx| Stream::poll_next(Pin::new(&mut stream), cx)).await {
        metas.push(meta?);
    }
    Ok(metas)
}
async fn object_count(objects: &dyn ObjectStore) -> Result<usize> {
    Ok(listed(objects).await?.len())
}
#[tokio::test]
async fn immutable_versions_tombstones_and_expectations_for_both_formats() -> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let (index, _, objects) = index(format);
        let name = "refs/heads/main";
        let first = index
            .prepare(
                None,
                operation(1),
                &plan(vec![update(name, None, Some(oid(1, format)))]),
            )
            .await?
            .root();
        let same = index
            .prepare(
                Some(first.clone()),
                operation(2),
                &plan(vec![update(
                    name,
                    Some(state(Some(1), 1, format)),
                    Some(oid(1, format)),
                )]),
            )
            .await?
            .root();
        assert_eq!(
            index.read(Some(same.clone()), name).await?,
            Some(state(Some(1), 2, format))
        );
        let deleted = index
            .prepare(
                Some(same),
                operation(3),
                &plan(vec![update(name, Some(state(Some(1), 2, format)), None)]),
            )
            .await?
            .root();
        assert_eq!((deleted.record_count, deleted.object_count), (1, 0));
        assert_eq!(
            index.read(Some(deleted.clone()), name).await?,
            Some(state(None, 3, format))
        );
        let before = object_count(&*objects).await?;
        for expected in [
            None,
            Some(state(Some(1), 1, format)),
            Some(state(Some(1), 3, format)),
        ] {
            assert!(matches!(
                index
                    .prepare(
                        Some(deleted.clone()),
                        operation(4),
                        &plan(vec![update(name, expected, Some(oid(2, format)))])
                    )
                    .await,
                Err(RefStateError::Changed)
            ));
        }
        assert_eq!(object_count(&*objects).await?, before);
        let recreated = index
            .prepare(
                Some(deleted.clone()),
                operation(4),
                &plan(vec![update(
                    name,
                    Some(state(None, 3, format)),
                    Some(oid(2, format)),
                )]),
            )
            .await?;
        assert_eq!(recreated.base(), Some(deleted));
        assert_eq!(
            index.read(Some(recreated.root()), name).await?,
            Some(state(Some(2), 4, format))
        );
        index.clear_cache()?;
        assert_eq!(
            index.read(Some(first), name).await?,
            Some(state(Some(1), 1, format))
        );
        let terminal = index
            .tree
            .build_sorted(
                operation(5),
                [RefStateRecord::new(
                    name,
                    state(Some(1), i64::MAX, format),
                    format,
                )],
            )
            .await?
            .ok_or("terminal")?;
        assert!(matches!(
            index
                .prepare(
                    Some(terminal),
                    operation(6),
                    &plan(vec![update(
                        name,
                        Some(state(Some(1), i64::MAX, format)),
                        None
                    )])
                )
                .await,
            Err(RefStateError::Shape(_))
        ));
    }
    Ok(())
}
#[tokio::test]
async fn atomic_namespace_swaps_and_unicode_are_validated_before_uploads() -> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let (index, _, objects) = index(format);
        let parent = "refs/heads/équipe";
        let child = "refs/heads/équipe/東京";
        let root = index
            .prepare(
                None,
                operation(1),
                &plan(vec![update(parent, None, Some(oid(1, format)))]),
            )
            .await?
            .root();
        let before = object_count(&*objects).await?;
        assert!(matches!(
            index
                .prepare(
                    Some(root.clone()),
                    operation(2),
                    &plan(vec![update(child, None, Some(oid(2, format)))])
                )
                .await,
            Err(RefStateError::Namespace)
        ));
        assert!(matches!(
            index
                .prepare(
                    None,
                    operation(2),
                    &plan(vec![
                        update(parent, None, Some(oid(1, format))),
                        update(child, None, Some(oid(2, format)))
                    ])
                )
                .await,
            Err(RefStateError::Namespace)
        ));
        assert_eq!(object_count(&*objects).await?, before);
        let swapped = index
            .prepare(
                Some(root),
                operation(2),
                &plan(vec![
                    update(child, None, Some(oid(2, format))),
                    update(parent, Some(state(Some(1), 1, format)), None),
                ]),
            )
            .await?
            .root();
        assert_eq!((swapped.record_count, swapped.object_count), (2, 1));
        let before = object_count(&*objects).await?;
        assert!(matches!(
            index
                .prepare(
                    Some(swapped.clone()),
                    operation(3),
                    &plan(vec![update(
                        parent,
                        Some(state(None, 2, format)),
                        Some(oid(3, format))
                    )])
                )
                .await,
            Err(RefStateError::Namespace)
        ));
        assert_eq!(object_count(&*objects).await?, before);
        let restored = index
            .prepare(
                Some(swapped),
                operation(3),
                &plan(vec![
                    update(parent, Some(state(None, 2, format)), Some(oid(3, format))),
                    update(child, Some(state(Some(2), 1, format)), None),
                ]),
            )
            .await?
            .root();
        let mut live = index.cursor(Some(restored), None, true)?;
        assert_eq!(live.next().await?.ok_or("live")?.name(), parent);
        assert!(live.next().await?.is_none());
    }
    Ok(())
}
#[tokio::test]
async fn initial_twenty_thousand_ref_plan_builds_by_nodes_and_seeks_one_path() -> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let (index, _, objects) = index(format);
        let names: Vec<_> = (0..20_000)
            .map(|n| format!("refs/heads/{n:05}/{}", "x".repeat(200)))
            .collect();
        let initial = plan(
            names
                .iter()
                .map(|name| update(name, None, Some(oid(1, format))))
                .collect(),
        );
        let mut encoded_bytes = 0;
        for start in (0..initial.updates.len()).step_by(32) {
            let mut encoded = BoundedEncoder::new(64 << 10)?;
            initial.encode_range(start..(start + 32).min(initial.updates.len()), &mut encoded)?;
            encoded_bytes += encoded.finish().len();
        }
        assert!(encoded_bytes > 4 << 20);
        let transition = index.prepare(None, operation(1), &initial).await?;
        assert_eq!(
            transition.plan_digest(),
            crate::packs::publication::ref_proof::plan_digest(&initial)?
        );
        let root = transition.root();
        assert_eq!(
            (root.record_count, root.object_count, root.height),
            (20_000, 20_000, 2)
        );
        assert!(
            object_count(&*objects).await? <= 500,
            "construction must scale with nodes, not members"
        );
        index.clear_cache()?;
        let before = index.stats();
        assert_eq!(
            index.read(Some(root.clone()), &names[17_123]).await?,
            Some(state(Some(1), 1, format))
        );
        assert_eq!(
            index.stats().loaded_nodes - before.loaded_nodes,
            u64::from(root.height) + 1
        );
        index.clear_cache()?;
        let before = index.stats();
        let mut cursor = index.cursor(
            Some(root.clone()),
            Some(RefNameKey::new(&names[17_122])?),
            true,
        )?;
        assert_eq!(cursor.next().await?.ok_or("seek")?.name(), names[17_123]);
        assert_eq!(
            index.stats().loaded_nodes - before.loaded_nodes,
            u64::from(root.height) + 1
        );
        drop(cursor);
        let before = object_count(&*objects).await?;
        let changed = index
            .prepare(
                Some(root.clone()),
                operation(2),
                &plan(vec![update(
                    &names[17_123],
                    Some(state(Some(1), 1, format)),
                    Some(oid(2, format)),
                )]),
            )
            .await?
            .root();
        assert!(object_count(&*objects).await? - before <= 2 * (usize::from(root.height) + 1));
        index.clear_cache()?;
        assert_eq!(
            index.read(Some(root), &names[17_123]).await?,
            Some(state(Some(1), 1, format))
        );
        assert_eq!(
            index.read(Some(changed), &names[17_123]).await?,
            Some(state(Some(2), 2, format))
        );
    }
    Ok(())
}
#[tokio::test]
async fn live_seek_skips_dead_subtrees_without_losing_later_siblings() -> Result {
    let format = ObjectFormat::Sha256;
    let (index, _, _) = index(format);
    let name = |n| format!("refs/heads/{n:05}");
    let root = index
        .tree
        .build_sorted(
            operation(1),
            (0..40_000).map(|n| {
                RefStateRecord::new(
                    &name(n),
                    state(
                        if n == 0 || n == 39_999 { Some(1) } else { None },
                        1,
                        format,
                    ),
                    format,
                )
            }),
        )
        .await?
        .ok_or("root")?;
    assert_eq!((root.height, root.object_count), (2, 2));
    for seek in [1, 200, 16_000, 16_384, 20_000, 39_998] {
        index.clear_cache()?;
        let before = index.stats();
        let mut cursor = index.cursor(
            Some(root.clone()),
            Some(RefNameKey::new(&name(seek))?),
            true,
        )?;
        assert_eq!(
            cursor
                .next()
                .await?
                .ok_or("later live sibling lost")?
                .name(),
            name(39_999)
        );
        assert!(cursor.next().await?.is_none());
        assert!(
            index.stats().loaded_nodes - before.loaded_nodes <= 2 * (u64::from(root.height) + 1)
        );
    }
    let mut forged = root.clone();
    forged.object_count = 0;
    index.clear_cache()?;
    assert!(
        index
            .cursor(Some(forged), None, true)?
            .next()
            .await
            .is_err()
    );
    let mut cursor = index.cursor(Some(root), Some(RefNameKey::new(&name(39_999))?), true)?;
    assert!(cursor.next().await?.is_none());
    Ok(())
}
#[tokio::test]
async fn long_names_split_by_bytes_and_snapshot_roundtrips_large_fences() -> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let (index, store, objects) = index(format);
        let names: Vec<_> = (0..40)
            .map(|n| {
                let prefix = format!("refs/heads/{n:03}/");
                format!("{prefix}{}", "x".repeat(MAX_NAME_BYTES - prefix.len()))
            })
            .collect();
        let mut root = None;
        for name in &names {
            root = Some(
                index
                    .prepare(
                        root,
                        operation(1),
                        &plan(vec![update(name, None, Some(oid(1, format)))]),
                    )
                    .await?
                    .root(),
            );
        }
        let root = root.ok_or("root")?;
        assert!(root.height >= 2);
        let mut cursor = index.cursor(Some(root.clone()), None, false)?;
        for name in &names {
            assert_eq!(cursor.next().await?.ok_or("name")?.name(), name);
        }
        assert!(cursor.next().await?.is_none());
        for object in listed(&*objects).await? {
            assert!(object.size <= 512 << 10);
        }
        let snapshot = RefStateSnapshot {
            repository: repository(),
            format,
            generation: 1,
            default_branch: names[0].clone(),
            root: Some(root),
        };
        let saved = RefStateSnapshotRoot::upload(&store, operation(2), snapshot.clone()).await?;
        assert!(saved.artifact().size > 128 << 10);
        assert!(saved.artifact().size <= 256 << 10);
        let mut e = BoundedEncoder::new(128)?;
        saved.encode(&mut e)?;
        let bytes = e.finish();
        let mut d = BoundedDecoder::new(&bytes, 128)?;
        let decoded = RefStateSnapshotRoot::decode(&mut d)?;
        d.finish()?;
        let mut wire = BoundedDecoder::new(&bytes, 128)?;
        assert!(crate::packs::wire_request::WireRequestRoot::decode(&mut wire).is_err());
        let mut native = BoundedDecoder::new(&bytes, 128)?;
        assert!(crate::packs::publication::NativeResultRoot::decode(&mut native).is_err());
        assert_eq!(decoded.read(&store).await?, snapshot);
        let wrong = ArtifactStore::new(objects.clone(), [2; 16]);
        assert!(decoded.read(&wrong).await.is_err());
        let mut forged_bytes = bytes.clone();
        forged_bytes[32] ^= 1;
        let mut d = BoundedDecoder::new(&forged_bytes, 128)?;
        let forged = RefStateSnapshotRoot::decode(&mut d)?;
        d.finish()?;
        assert!(forged.read(&store).await.is_err());
        let too_long = format!("{}x", names[0]);
        assert!(RefNameKey::new(&too_long).is_err());
        let mut wrong_generation = snapshot.clone();
        wrong_generation.generation = 0;
        assert!(
            RefStateSnapshotRoot::upload(&store, operation(2), wrong_generation)
                .await
                .is_err()
        );
    }
    Ok(())
}
#[tokio::test]
async fn sorted_builder_rejects_duplicates_disorder_and_zero_live_cursor_authenticates() -> Result {
    let format = ObjectFormat::Sha1;
    let (index, _, _) = index(format);
    let a = RefStateRecord::new("refs/heads/a", state(None, 1, format), format)?;
    let b = RefStateRecord::new("refs/heads/b", state(None, 2, format), format)?;
    for records in [vec![a.clone(), a.clone()], vec![b.clone(), a.clone()]] {
        assert!(matches!(
            index
                .tree
                .build_sorted(operation(1), records.into_iter().map(Ok))
                .await,
            Err(IndexError::RangeOverlap)
        ));
    }
    assert!(
        index
            .tree
            .build_sorted(operation(1), std::iter::empty())
            .await?
            .is_none()
    );
    let root = index
        .tree
        .build_sorted(operation(1), [Ok(a.clone()), Ok(b)])
        .await?
        .ok_or("root")?;
    assert_eq!((root.record_count, root.object_count), (2, 0));
    index.clear_cache()?;
    let before = index.stats();
    assert!(
        index
            .cursor(Some(root.clone()), None, true)?
            .next()
            .await?
            .is_none()
    );
    assert_eq!(index.stats().loaded_nodes - before.loaded_nodes, 1);
    assert_eq!(
        index.cursor(Some(root), None, false)?.next().await?,
        Some(a)
    );
    Ok(())
}

#[tokio::test]
async fn namespace_check_finds_conflict_after_exhausted_positive_branch() -> Result {
    let format = ObjectFormat::Sha256;
    let (index, _, objects) = index(format);
    let records = (0..40_000).map(|n| {
        let name = format!("refs/heads/{}/{n:05}", if n < 200 { "aa" } else { "zz" });
        RefStateRecord::new(
            &name,
            state(
                if n == 0 || n == 39_999 { Some(1) } else { None },
                1,
                format,
            ),
            format,
        )
    });
    let root = index
        .tree
        .build_sorted(operation(1), records)
        .await?
        .ok_or("root")?;
    index.clear_cache()?;
    let before = object_count(&*objects).await?;
    assert!(matches!(
        index
            .prepare(
                Some(root),
                operation(2),
                &plan(vec![update("refs/heads/zz", None, Some(oid(1, format)))])
            )
            .await,
        Err(RefStateError::Namespace)
    ));
    assert_eq!(object_count(&*objects).await?, before);
    Ok(())
}
#[tokio::test]
async fn snapshot_purpose_and_tree_format_are_authenticated() -> Result {
    let format = ObjectFormat::Sha256;
    let (index, store, _) = index(format);
    let root = index
        .prepare(
            None,
            operation(1),
            &plan(vec![update("refs/heads/main", None, Some(oid(1, format)))]),
        )
        .await?
        .root();
    let foreign_format = RefStateIndex::new(store.clone(), ObjectFormat::Sha1);
    assert!(
        foreign_format
            .read(Some(root.clone()), "refs/heads/main")
            .await
            .is_err()
    );
    let snapshot = RefStateSnapshot {
        repository: repository(),
        format,
        generation: 1,
        default_branch: "refs/heads/main".into(),
        root: Some(root),
    };
    let saved = RefStateSnapshotRoot::upload(&store, operation(2), snapshot).await?;
    let mut e = BoundedEncoder::new(128)?;
    saved.encode(&mut e)?;
    let bytes = e.finish();
    let mut d = BoundedDecoder::new(&bytes, 128)?;
    let wrong_purpose = crate::packs::wire_request::WireRequestRoot::decode(&mut d)?;
    d.finish()?;
    assert!(wrong_purpose.read(&store).await.is_err());
    Ok(())
}
