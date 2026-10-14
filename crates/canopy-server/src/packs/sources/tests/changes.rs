//! Coverage transferred from the retired SQL hydration sequence to the actual
//! source descriptor cursor used by native write-base construction.
use super::*;
use crate::packs::directory::index::IndexRecord;
use cellule_runtime::codec::BoundedEncoder;

async fn collect(
    index: &SourceIndex,
    before: Option<SourceRoot>,
    after: Option<SourceRoot>,
) -> Result<Vec<SourceRecord>> {
    let mut cursor = index.changes(before, after, None)?;
    let mut records = Vec::new();
    loop {
        let page = cursor.page(7, 64 << 10).await?;
        if page.is_empty() {
            break;
        }
        records.extend(page);
    }
    Ok(records)
}

#[tokio::test]
async fn lower_keys_are_found_and_later_publications_are_excluded() -> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let (store, _) = store();
        let index = SourceIndex::new(store, format);
        let before = index
            .build_sorted([5; 16], (200..460).map(|n| Ok(source(n, format))))
            .await?;
        let selected = Some(index.insert(before, [6; 16], source(1, format)).await?);
        let future = Some(index.insert(selected, [7; 16], source(0, format)).await?);
        assert_eq!(
            collect(&index, before, selected).await?,
            [source(1, format)]
        );
        assert_eq!(
            collect(&index, selected, future).await?,
            [source(0, format)]
        );
        assert_eq!(collect(&index, selected, selected).await?, []);
        assert_eq!(collect(&index, future, before).await?, []); // removals need no new native input
    }
    Ok(())
}

#[tokio::test]
async fn byte_limited_pages_and_restart_advance_only_over_returned_prefix() -> Result {
    let format = ObjectFormat::Sha256;
    let (store, _) = store();
    let index = SourceIndex::new(store, format);
    let root = index
        .build_sorted([5; 16], (0..270).map(|n| Ok(source(n, format))))
        .await?;
    let mut encoder = BoundedEncoder::new(64 << 10)?;
    source(0, format).encode_record(&mut encoder)?;
    let size = encoder.finish().len();
    let mut cursor = index.changes(None, root, None)?;
    let mut returned = Vec::new();
    for n in 0..270 {
        let page = cursor.page(128, size * 2 - 1).await?;
        assert_eq!(page, [source(n, format)]);
        // A stateless restart cannot skip the prefetched but excluded descriptor.
        let mut retry = index.changes(None, root, Some(page[0].key()))?;
        let next = retry.page(1, size).await?;
        assert_eq!(
            next,
            if n == 269 {
                vec![]
            } else {
                vec![source(n + 1, format)]
            }
        );
        returned.extend(page);
    }
    assert!(cursor.page(128, size).await?.is_empty());
    assert_eq!(returned.len(), 270);
    let mut cursor = index.changes(None, root, None)?;
    assert!(matches!(
        cursor.page(1, size - 1).await,
        Err(IndexError::Limit)
    ));
    assert!(matches!(
        cursor.page(1, size).await,
        Err(IndexError::Integrity)
    ));
    Ok(())
}

#[tokio::test]
async fn duplicate_failed_update_deletion_and_replacement_preserve_changes() -> Result {
    let format = ObjectFormat::Sha1;
    let (store, _) = store();
    let index = SourceIndex::new(store, format);
    let before = Some(index.insert(None, [5; 16], source(10, format)).await?);
    assert_eq!(
        Some(index.insert(before, [6; 16], source(10, format)).await?),
        before
    );
    let mut changed = source(10, format);
    changed.index.manifest_digest[0] ^= 1;
    assert!(index.insert(before, [6; 16], changed).await.is_err());
    assert!(collect(&index, before, before).await?.is_empty());
    let removed = index.remove(before, [7; 16], source(10, format)).await?;
    let after = Some(index.insert(removed, [8; 16], source(1, format)).await?);
    assert_eq!(collect(&index, before, after).await?, [source(1, format)]);
    let replaced = Some(
        index
            .replace(before.ok_or("root")?, [9; 16], source(10, format), changed)
            .await?,
    );
    assert_eq!(collect(&index, before, replaced).await?, [changed]);
    assert_eq!(
        index.find(before, source(10, format).key()).await?,
        Some(source(10, format))
    );
    Ok(())
}

#[tokio::test]
async fn small_increment_skips_unchanged_subtrees_after_ten_thousand_sources() -> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let (store, _) = store();
        let index = SourceIndex::new(store, format);
        let before = index
            .build_sorted([5; 16], (100..10_100).map(|n| Ok(source(n, format))))
            .await?;
        let mut after = before;
        for n in [0, 50, 20_000] {
            after = Some(index.insert(after, [6; 16], source(n, format)).await?);
        }
        index.clear_cache()?;
        let initial = index.stats();
        assert_eq!(
            collect(&index, before, after).await?,
            [
                source(0, format),
                source(50, format),
                source(20_000, format)
            ]
        );
        assert!(index.stats().loaded_nodes - initial.loaded_nodes < 24);
    }
    Ok(())
}

#[tokio::test]
async fn different_tree_shapes_and_rewrites_match_independent_key_difference() -> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let (store, _) = store();
        let index = SourceIndex::new(store, format);
        for count in [1, 127, 128, 129, 300] {
            let old: Vec<_> = (0..count).map(|n| source(n * 3, format)).collect();
            let mut new: Vec<_> = old
                .iter()
                .enumerate()
                .filter(|(n, _)| n % 5 != 0)
                .map(|(n, r)| {
                    let mut r = *r;
                    if n % 7 == 0 {
                        r.index.manifest_digest[0] ^= 1;
                    }
                    r
                })
                .collect();
            new.extend(
                (0..count)
                    .filter(|n| n % 4 == 0)
                    .map(|n| source(n * 3 + 1, format)),
            );
            new.sort_by_key(|r| r.key());
            let before = index
                .build_sorted([5; 16], old.iter().copied().map(Ok))
                .await?;
            let after = index
                .build_sorted([6; 16], new.iter().copied().map(Ok))
                .await?;
            let expected: Vec<_> = new.iter().copied().filter(|r| !old.contains(r)).collect();
            assert_eq!(collect(&index, before, after).await?, expected);
        }
    }
    Ok(())
}
