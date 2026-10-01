use super::*;
use object_store::{ObjectStore, ObjectStoreExt, memory::InMemory};

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;
fn oid(n: u64, format: ObjectFormat) -> ObjectId {
    let mut bytes = vec![0; format.bytes()];
    bytes[..8].copy_from_slice(&n.to_be_bytes());
    bytes.try_into().unwrap()
}
// These fixtures test the descriptor index, not native object verification or
// publication. Their directory files are intentionally not populated.
fn run(n: u64, format: ObjectFormat) -> StoredRun {
    let digest = *blake3::hash(&n.to_be_bytes()).as_bytes();
    let artifact = ArtifactDescriptor {
        size: 16 << 10,
        digest,
        manifest_digest: [3; 32],
    };
    StoredRun {
        run: RunDescriptor {
            repository: [1; 16],
            operation: [2; 16],
            format,
            object_count: 2,
            first_oid: oid(3 * n + 1, format),
            last_oid: oid(3 * n + 2, format),
            inventory_digest: [4; 32],
            size: artifact.size,
            digest,
        },
        artifact,
    }
}
fn index(format: ObjectFormat) -> (RangeIndex, Arc<dyn ObjectStore>) {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    (
        RangeIndex::new(
            Arc::new(ArtifactStore::new(Arc::clone(&store), [1; 16])),
            format,
        ),
        store,
    )
}

#[tokio::test]
async fn incremental_split_and_removal_preserve_old_roots_for_both_formats() -> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let (index, _store) = index(format);
        let mut root = None;
        let count = FANOUT + 20;
        for n in 0..count {
            root = Some(index.insert(root, [5; 16], run(n as u64, format)).await?);
        }
        let old = root.ok_or("root")?;
        assert_eq!(old.height, 1);
        assert_eq!(old.record_count, count as u64);
        assert_eq!(old.object_count, 2 * count as u64);
        assert_eq!(index.insert(root, [6; 16], run(0, format)).await?, old);
        let mut cursor = index.cursor(root, None)?;
        for n in 0..count {
            assert_eq!(cursor.next().await?, Some(run(n as u64, format)));
        }
        assert!(cursor.next().await?.is_none());
        let missing = run(127, format);
        root = index.remove(root, [7; 16], missing).await?;
        assert!(index.find(root, missing.run.first_oid).await?.is_none());
        assert_eq!(
            index.find(Some(old), missing.run.first_oid).await?,
            Some(missing)
        );
        assert!(matches!(
            index.remove(root, [7; 16], missing).await,
            Err(IndexError::Stale)
        ));
        let mut stale = run(0, format);
        stale.artifact.manifest_digest[0] ^= 1;
        assert!(matches!(
            index.remove(root, [7; 16], stale).await,
            Err(IndexError::Stale)
        ));
        for n in 0..count {
            if n != 127 {
                root = index.remove(root, [8; 16], run(n as u64, format)).await?;
            }
        }
        assert!(root.is_none());
        assert_eq!(
            index
                .find(Some(old), run(200, format).run.first_oid)
                .await?,
            Some(run(200, format))
        );
        assert!(index.cache.lock().unwrap().len() <= CACHE_NODES);
    }
    Ok(())
}

#[tokio::test]
async fn overlap_detection_includes_ranges_enclosing_existing_runs() -> Result {
    let format = ObjectFormat::Sha256;
    let (index, _store) = index(format);
    let root = Some(index.insert(None, [5; 16], run(20, format)).await?);
    let mut enclosing = run(0, format);
    enclosing.run.first_oid = oid(1, format);
    enclosing.run.last_oid = oid(100, format);
    assert!(index.find(root, enclosing.run.first_oid).await?.is_none());
    assert!(index.find(root, enclosing.run.last_oid).await?.is_none());
    assert!(matches!(
        index.insert(root, [6; 16], enclosing).await,
        Err(IndexError::RangeOverlap)
    ));
    let mut touching = run(19, format);
    touching.run.last_oid = run(20, format).run.first_oid;
    assert!(matches!(
        index.insert(root, [6; 16], touching).await,
        Err(IndexError::RangeOverlap)
    ));
    let before = index.insert(root, [6; 16], run(0, format)).await?;
    let after = index.insert(Some(before), [6; 16], run(40, format)).await?;
    assert_eq!(after.record_count, 3);
    Ok(())
}

#[tokio::test]
async fn cold_point_lookup_reads_only_one_bounded_path_and_cursor_seeks() -> Result {
    let format = ObjectFormat::Sha1;
    let (index, _store) = index(format);
    let runs = (0..16).map(|n| run(n, format)).collect::<Vec<_>>();
    let mut level = Vec::new();
    for chunk in runs.chunks(2) {
        level.push(
            index
                .persist([5; 16], 0, Contents::Runs(chunk.to_vec()))
                .await?,
        );
    }
    let mut height = 0;
    while level.len() > 1 {
        height += 1;
        let mut next = Vec::new();
        for chunk in level.chunks(2) {
            next.push(
                index
                    .persist([5; 16], height, Contents::Children(chunk.to_vec()))
                    .await?,
            );
        }
        level = next;
    }
    let root = level[0];
    assert_eq!(root.height, 3);
    index.clear_cache()?;
    let before = index.stats();
    assert_eq!(
        index.find(Some(root), runs[10].run.first_oid).await?,
        Some(runs[10])
    );
    let after = index.stats();
    assert_eq!(
        after.loaded_nodes - before.loaded_nodes,
        u64::from(root.height) + 1
    );
    assert_eq!(
        index.find(Some(root), runs[10].run.last_oid).await?,
        Some(runs[10])
    );
    assert_eq!(index.stats().loaded_nodes, after.loaded_nodes);
    assert!(index.stats().cache_hits > after.cache_hits);
    assert!(index.find(Some(root), oid(33, format)).await?.is_none());
    let mut cursor = index.cursor(Some(root), Some(runs[8].run.first_oid))?;
    for expected in runs.iter().skip(9) {
        assert_eq!(cursor.next().await?, Some(*expected));
    }
    assert!(cursor.next().await?.is_none());
    let mut cursor = index.cursor(Some(root), Some(root.last_key))?;
    assert!(cursor.next().await?.is_none());
    Ok(())
}

#[test]
fn node_codec_rejects_bad_count_ranges_height_framing_and_large_input() -> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let node = Node {
            repository: [1; 16],
            operation: [5; 16],
            format,
            height: 0,
            contents: Contents::Runs(vec![run(0, format), run(1, format)]),
        };
        let bytes = node.encode()?;
        let decoded = Node::<StoredRun>::decode(&bytes)?;
        assert_eq!(decoded.encode()?, bytes);
        for length in [0, 1, bytes.len() - 1] {
            assert!(Node::<StoredRun>::decode(&bytes[..length]).is_err());
        }
        let mut trailing = bytes.clone();
        trailing.push(0);
        assert!(Node::<StoredRun>::decode(&trailing).is_err());
        assert!(Node::<StoredRun>::decode(&vec![0; NODE_BYTES as usize + 1]).is_err());
        let count_at = 4 + b"canopy.range-index.v1\0".len() + 4 + 16 + 4 + 16 + 1 + 1;
        for count in [0_u32, FANOUT as u32 + 1, u32::MAX] {
            let mut bad = bytes.clone();
            bad[count_at..count_at + 4].copy_from_slice(&count.to_be_bytes());
            assert!(Node::<StoredRun>::decode(&bad).is_err());
        }
        let mut reversed = node.clone();
        reversed.contents = Contents::Runs(vec![run(1, format), run(0, format)]);
        assert!(reversed.encode().is_err());
        let mut overlap = node.clone();
        overlap.contents = Contents::Runs(vec![run(0, format), run(0, format)]);
        assert!(overlap.encode().is_err());
        let mut height = node.clone();
        height.height = MAX_HEIGHT + 1;
        assert!(height.encode().is_err());
        let many = Node {
            contents: Contents::Runs((0..FANOUT).map(|n| run(n as u64, format)).collect()),
            ..node
        };
        assert!(many.encode()?.len() <= NODE_BYTES as usize);
    }
    Ok(())
}

#[tokio::test]
async fn authenticated_node_bytes_and_reference_summaries_are_checked_before_lookup() -> Result {
    let format = ObjectFormat::Sha256;
    let (index, store) = index(format);
    let reference = index.insert(None, [5; 16], run(0, format)).await?;
    let mut forged = reference;
    forged.object_count += 1;
    assert!(matches!(
        index.find(Some(forged), run(0, format).run.first_oid).await,
        Err(IndexError::Integrity)
    ));
    let path = index
        .store
        .path(reference.key(), reference.artifact.digest)?;
    store
        .put(
            &canopy_object_storage::external::part(&path, 0),
            bytes::Bytes::from(vec![0; reference.artifact.size as usize]).into(),
        )
        .await?;
    index.clear_cache()?;
    assert!(
        index
            .find(Some(reference), run(0, format).run.first_oid)
            .await
            .is_err()
    );
    let mut cursor = index.cursor(Some(reference), None)?;
    assert!(cursor.next().await.is_err());
    assert!(matches!(cursor.next().await, Err(IndexError::Integrity)));
    Ok(())
}

#[tokio::test]
async fn repository_operation_and_object_format_binding_cannot_be_forged() -> Result {
    let format = ObjectFormat::Sha256;
    let (index, _store) = index(format);
    let mut foreign = run(0, format);
    foreign.run.repository = [9; 16];
    assert!(matches!(
        index.insert(None, [5; 16], foreign).await,
        Err(IndexError::Integrity)
    ));
    assert!(matches!(
        index
            .insert(None, [5; 16], run(0, ObjectFormat::Sha1))
            .await,
        Err(IndexError::Integrity)
    ));
    let node = Node {
        repository: [9; 16],
        operation: [5; 16],
        format,
        height: 0,
        contents: Contents::Runs(vec![foreign]),
    };
    let bytes = node.encode()?;
    let digest = *blake3::hash(&bytes).as_bytes();
    let key = ArtifactKey {
        operation: [5; 16],
        binding_digest: digest,
        kind: ArtifactKind::CatalogNode,
    };
    let artifact = index
        .store
        .put(key, bytes.len() as u64, digest, &mut bytes.as_slice())
        .await?;
    let reference = node.reference(artifact)?;
    assert!(matches!(
        index.find(Some(reference), foreign.run.first_oid).await,
        Err(IndexError::Integrity)
    ));
    Ok(())
}

#[test]
fn source_nodes_use_typed_keys_separate_domains_and_bounded_leaf_codecs() -> Result {
    use crate::packs::sources::{SOURCE_FANOUT, SourceRecord, tests::source};
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let node = Node::<SourceRecord> {
            repository: [1; 16],
            operation: [5; 16],
            format,
            height: 0,
            contents: Contents::Runs(
                (0..SOURCE_FANOUT)
                    .map(|n| source(n as u64, format))
                    .collect(),
            ),
        };
        let bytes = node.encode()?;
        assert!(bytes.len() <= NODE_BYTES as usize);
        assert_eq!(Node::<SourceRecord>::decode(&bytes)?.encode()?, bytes);
        assert!(Node::<StoredRun>::decode(&bytes).is_err());
        let directory = Node::<StoredRun> {
            repository: [1; 16],
            operation: [5; 16],
            format,
            height: 0,
            contents: Contents::Runs(vec![run(0, format)]),
        };
        assert!(Node::<SourceRecord>::decode(&directory.encode()?).is_err());
        for length in [0, 1, bytes.len() - 1] {
            assert!(Node::<SourceRecord>::decode(&bytes[..length]).is_err());
        }
        let mut trailing = bytes.clone();
        trailing.push(0);
        assert!(Node::<SourceRecord>::decode(&trailing).is_err());
        let count_at = 4 + SourceRecord::DOMAIN.len() + 4 + 16 + 4 + 16 + 1 + 1;
        for count in [0_u32, SOURCE_FANOUT as u32 + 1, u32::MAX] {
            let mut bad = bytes.clone();
            bad[count_at..count_at + 4].copy_from_slice(&count.to_be_bytes());
            assert!(Node::<SourceRecord>::decode(&bad).is_err());
        }
        assert!(Node::<SourceRecord>::decode(&vec![0; NODE_BYTES as usize + 1]).is_err());
        let mut unsorted = node.clone();
        let Contents::Runs(records) = &mut unsorted.contents else {
            unreachable!()
        };
        records.reverse();
        assert!(unsorted.encode().is_err());
        for width in [0_usize, 47, 49, 64] {
            let mut encoder = BoundedEncoder::new(128)?;
            encoder.write_bytes(&vec![0; width])?;
            let bytes = encoder.finish();
            let mut decoder = BoundedDecoder::new(&bytes, 128)?;
            assert!(SegmentKey::decode(&mut decoder, format).is_err());
        }
    }
    Ok(())
}
