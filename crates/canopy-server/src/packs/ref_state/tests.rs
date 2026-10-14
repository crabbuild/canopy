use super::*;
use crate::{ObjectId, RefUpdate};
use cellule_runtime::codec::WireValue;
use futures_core::Stream;
use object_store::{ObjectStore, memory::InMemory};
use std::{future::poll_fn, pin::Pin};

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;
fn send<T: Send>(value: T) -> T {
    value
}
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
            index.read(Some(changed.clone()), &names[17_123]).await?,
            Some(state(Some(2), 2, format))
        );
        let bulk = plan(
            names
                .iter()
                .enumerate()
                .map(|(n, name)| {
                    update(
                        name,
                        Some(state(
                            Some(if n == 17_123 { 2 } else { 1 }),
                            if n == 17_123 { 2 } else { 1 },
                            format,
                        )),
                        Some(oid(3, format)),
                    )
                })
                .collect(),
        );
        index.clear_cache()?;
        let reads = index.stats().loaded_nodes;
        let writes = object_count(&*objects).await?;
        let bulk_root = index
            .prepare(Some(changed), operation(3), &bulk)
            .await?
            .root();
        assert_eq!(
            (bulk_root.record_count, bulk_root.object_count),
            (20_000, 20_000)
        );
        assert!(
            index.stats().loaded_nodes - reads <= 600,
            "validate and rewrite by nodes, not by update paths"
        );
        assert!(
            object_count(&*objects).await? - writes <= 500,
            "rewrite the complete existing large plan in bounded groups"
        );
        let mut cursor = index.cursor(Some(bulk_root), None, false)?;
        for (n, name) in names.iter().enumerate() {
            let actual = cursor.next().await?.ok_or("bulk ref")?;
            assert_eq!(actual.name(), name);
            assert_eq!(
                actual.state(),
                &state(Some(3), if n == 17_123 { 3 } else { 2 }, format)
            );
        }
        assert!(cursor.next().await?.is_none());
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

#[tokio::test]
async fn existing_batch_copies_changed_subtrees_once_and_preserves_all_versions() -> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let (index, _, objects) = index(format);
        let name = |n| format!("refs/heads/{n:05}");
        let original = index
            .tree
            .build_sorted(
                operation(1),
                (0..20_000)
                    .map(|n| RefStateRecord::new(&name(n), state(Some(1), 1, format), format)),
            )
            .await?
            .ok_or("original")?;
        index.clear_cache()?;
        let reads = index.stats().loaded_nodes;
        let writes = object_count(&*objects).await?;
        // Reversed intent exercises sorting without changing the canonical digest.
        let changes = plan(
            (10_000..10_512)
                .rev()
                .map(|n| {
                    update(
                        &name(n),
                        Some(state(Some(1), 1, format)),
                        if n % 2 == 0 {
                            None
                        } else {
                            Some(oid(2, format))
                        },
                    )
                })
                .collect(),
        );
        let transition = index
            .prepare(Some(original.clone()), operation(2), &changes)
            .await?;
        assert_eq!(transition.base(), Some(original.clone()));
        assert_eq!(
            transition.plan_digest(),
            crate::packs::publication::ref_proof::plan_digest(&changes)?
        );
        let current = transition.root();
        assert_eq!(
            (current.record_count, current.object_count),
            (20_000, 19_744)
        );
        assert!(
            object_count(&*objects).await? - writes <= 40,
            "one node group per changed subtree, not one path per update"
        );
        assert!(
            index.stats().loaded_nodes - reads <= 40,
            "sorted validation and rewriting must not reload history"
        );
        for root in [original, current] {
            let current = root.operation == operation(2);
            let mut cursor = index.cursor(Some(root), None, false)?;
            for n in 0..20_000 {
                let actual = cursor.next().await?.ok_or("missing ref")?;
                assert_eq!(actual.name(), name(n));
                let changed = current && (10_000..10_512).contains(&n);
                assert_eq!(
                    actual.state(),
                    &state(
                        if changed && n % 2 == 0 {
                            None
                        } else if changed {
                            Some(2)
                        } else {
                            Some(1)
                        },
                        if changed { 2 } else { 1 },
                        format
                    )
                );
            }
            assert!(cursor.next().await?.is_none());
        }
    }
    Ok(())
}

#[tokio::test]
async fn sparse_batch_reuses_higher_subtrees_and_inserts_before_between_and_after() -> Result {
    let format = ObjectFormat::Sha256;
    let (index, _, objects) = index(format);
    let name = |n| format!("refs/heads/m/{n:06}");
    let old = index
        .tree
        .build_sorted(
            operation(1),
            (0..100_000).map(|n| RefStateRecord::new(&name(n), state(Some(1), 1, format), format)),
        )
        .await?
        .ok_or("old")?;
    index.clear_cache()?;
    let reads = index.stats().loaded_nodes;
    let writes = object_count(&*objects).await?;
    let changes = plan(vec![
        update("refs/heads/z", None, Some(oid(3, format))),
        update(&name(40_000), Some(state(Some(1), 1, format)), None),
        update("refs/heads/m/040000/topic", None, Some(oid(4, format))),
        update(
            &name(50_000),
            Some(state(Some(1), 1, format)),
            Some(oid(2, format)),
        ),
        update("refs/heads/m/050000x", None, Some(oid(5, format))),
        update("refs/heads/a", None, Some(oid(6, format))),
    ]);
    let current = send(index.prepare(Some(old.clone()), operation(2), &changes))
        .await?
        .root();
    assert_eq!(
        (current.record_count, current.object_count),
        (100_004, 100_003)
    );
    assert!(
        index.stats().loaded_nodes - reads <= 64,
        "sparse preparation must not read 100,000 records"
    );
    assert!(
        object_count(&*objects).await? - writes <= 100,
        "reuse unchanged higher subtrees"
    );
    let mut expected: std::collections::BTreeMap<_, _> = (0..100_000)
        .map(|n| (name(n), state(Some(1), 1, format)))
        .collect();
    for update in &changes.updates {
        expected.insert(
            update.name.clone(),
            RefExpectation {
                oid: update.new_oid,
                version: if update.expected.is_some() { 2 } else { 1 },
            },
        );
    }
    let mut cursor = index.cursor(Some(current), None, false)?;
    for (name, expected) in expected {
        let actual = cursor.next().await?.ok_or("actual inventory exhausted")?;
        assert_eq!(actual.name(), name);
        assert_eq!(actual.state(), &expected);
    }
    assert!(cursor.next().await?.is_none());
    assert_eq!(
        index.read(Some(old), &name(40_000)).await?,
        Some(state(Some(1), 1, format))
    );
    Ok(())
}

#[tokio::test]
async fn sorted_rewrite_rejects_late_input_errors_and_retries_immutable_artifacts() -> Result {
    let format = ObjectFormat::Sha1;
    let (index, _, objects) = index(format);
    let name = |n| format!("refs/heads/{n:05}");
    let old = index
        .tree
        .build_sorted(
            operation(1),
            (0..1_000)
                .map(|n| RefStateRecord::new(&name(2 * n), state(Some(1), 1, format), format)),
        )
        .await?
        .ok_or("old")?;
    let record = |n| RefStateRecord::new(&name(2 * n + 1), state(Some(2), 1, format), format);
    let before = object_count(&*objects).await?;
    let broken = (0..500)
        .map(record)
        .chain(std::iter::once(Err(IndexError::Integrity)));
    assert!(matches!(
        index
            .tree
            .upsert_sorted(Some(old.clone()), operation(2), broken)
            .await,
        Err(IndexError::Integrity)
    ));
    assert!(
        object_count(&*objects).await? > before,
        "late failure exercises already emitted immutable nodes"
    );
    let root = index
        .tree
        .upsert_sorted(Some(old.clone()), operation(2), (0..1_000).map(record))
        .await?
        .ok_or("merged")?;
    let complete = object_count(&*objects).await?;
    assert_eq!(
        index
            .tree
            .upsert_sorted(Some(old.clone()), operation(2), (0..1_000).map(record))
            .await?,
        Some(root.clone())
    );
    assert_eq!(object_count(&*objects).await?, complete);
    let mut cursor = index.cursor(Some(root), None, false)?;
    for n in 0..2_000 {
        let actual = cursor.next().await?.ok_or("merged ref")?;
        assert_eq!(actual.name(), name(n));
        assert_eq!(
            actual.state(),
            &state(Some(if n % 2 == 0 { 1 } else { 2 }), 1, format)
        );
    }
    assert!(cursor.next().await?.is_none());
    assert_eq!(
        index
            .tree
            .upsert_sorted(Some(old.clone()), operation(3), std::iter::empty())
            .await?,
        Some(old.clone())
    );
    let unsorted = [record(900), record(1)];
    assert!(matches!(
        index
            .tree
            .upsert_sorted(Some(old), operation(4), unsorted)
            .await,
        Err(IndexError::RangeOverlap)
    ));
    Ok(())
}

#[tokio::test]
async fn long_name_batch_preserves_byte_bounds_for_changed_and_reused_levels() -> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let (index, _, objects) = index(format);
        let name = |n| {
            let prefix = format!("refs/heads/{n:03}/");
            format!("{prefix}{}", "x".repeat(MAX_NAME_BYTES - prefix.len()))
        };
        let old = index
            .tree
            .build_sorted(
                operation(1),
                (0..64)
                    .map(|n| RefStateRecord::new(&name(2 * n), state(Some(1), 1, format), format)),
            )
            .await?
            .ok_or("old")?;
        let mut updates: Vec<_> = (0..64)
            .step_by(3)
            .map(|n| {
                update(
                    &name(2 * n),
                    Some(state(Some(1), 1, format)),
                    if n % 2 == 0 {
                        None
                    } else {
                        Some(oid(2, format))
                    },
                )
            })
            .collect();
        updates.extend(
            (0..64)
                .step_by(5)
                .map(|n| update(&name(2 * n + 1), None, Some(oid(3, format)))),
        );
        updates.reverse();
        let changed = plan(updates);
        let current = index
            .prepare(Some(old.clone()), operation(2), &changed)
            .await?
            .root();
        for meta in listed(&*objects).await? {
            assert!(meta.size <= 512 << 10);
        }
        let mut expected: std::collections::BTreeMap<_, _> = (0..64)
            .map(|n| (name(2 * n), state(Some(1), 1, format)))
            .collect();
        for update in &changed.updates {
            expected.insert(
                update.name.clone(),
                RefExpectation {
                    oid: update.new_oid,
                    version: if update.expected.is_some() { 2 } else { 1 },
                },
            );
        }
        assert_eq!(expected.len(), 77);
        let mut cursor = index.cursor(Some(current), None, false)?;
        for (name, expected) in expected {
            let actual = cursor.next().await?.ok_or("long-name inventory")?;
            assert_eq!(actual.name(), name);
            assert_eq!(actual.state(), &expected);
        }
        assert!(cursor.next().await?.is_none());
        assert_eq!(
            index.read(Some(old), &name(0)).await?,
            Some(state(Some(1), 1, format))
        );
    }
    Ok(())
}

#[tokio::test]
async fn repeated_left_edge_inserts_keep_the_tree_dense() -> Result {
    let format = ObjectFormat::Sha1;
    let (index, _, _) = index(format);
    let mut root = index
        .tree
        .build_sorted(
            operation(1),
            (0..512).map(|n| {
                RefStateRecord::new(
                    &format!("refs/heads/m/{n:05}"),
                    state(Some(1), 1, format),
                    format,
                )
            }),
        )
        .await?
        .ok_or("base")?;
    for (step, n) in (0..32).rev().enumerate() {
        root = index
            .prepare(
                Some(root),
                operation(2 + step as u64),
                &plan(vec![update(
                    &format!("refs/heads/a/{n:05}"),
                    None,
                    Some(oid(2, format)),
                )]),
            )
            .await?
            .root();
    }
    assert_eq!((root.record_count, root.object_count), (544, 544));
    index.clear_cache()?;
    let before = index.stats().loaded_nodes;
    let mut cursor = index.cursor(Some(root), None, false)?;
    let mut count = 0;
    while cursor.next().await?.is_some() {
        count += 1;
    }
    assert_eq!(count, 544);
    let reads = index.stats().loaded_nodes - before;
    assert!(
        reads <= 11,
        "repeated prefix insertions left too many underfilled nodes: {reads}"
    );
    Ok(())
}

#[tokio::test]
async fn repeated_prefix_inserts_balance_internal_tail_groups() -> Result {
    let format = ObjectFormat::Sha1;
    let (index, _, _) = index(format);
    let mut root = index
        .tree
        .build_sorted(
            operation(1),
            (0..16_384).map(|n| {
                RefStateRecord::new(
                    &format!("refs/heads/m/{n:05}"),
                    state(Some(1), 1, format),
                    format,
                )
            }),
        )
        .await?
        .ok_or("base")?;
    for (step, n) in (0..640).rev().enumerate() {
        root = index
            .prepare(
                Some(root),
                operation(2 + step as u64),
                &plan(vec![update(
                    &format!("refs/heads/a/{n:05}"),
                    None,
                    Some(oid(2, format)),
                )]),
            )
            .await?
            .root();
    }
    assert_eq!((root.record_count, root.height), (17_024, 2));
    index.clear_cache()?;
    let before = index.stats().loaded_nodes;
    let mut cursor = index.cursor(Some(root), None, false)?;
    let mut count = 0;
    while cursor.next().await?.is_some() {
        count += 1;
    }
    assert_eq!(count, 17_024);
    let reads = index.stats().loaded_nodes - before;
    assert!(
        reads <= 145,
        "internal split tails fragmented the tree: {reads}"
    );
    Ok(())
}
