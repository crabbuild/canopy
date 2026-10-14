use super::*;
use crate::ObjectFormat;
use canopy_object_storage::artifact::ArtifactDescriptor;
type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

fn id(format: ObjectFormat, n: u64) -> ObjectId {
    let mut bytes = vec![0; format.bytes()];
    bytes[format.bytes() - 8..].copy_from_slice(&n.to_be_bytes());
    ObjectId::try_from(bytes).unwrap()
}
fn spool(budget: &DiskBudget, maximum: u64) -> Result<GraphSpool> {
    Ok(GraphSpool::new(
        Arc::new(tempfile::TempDir::new()?),
        budget.clone(),
        maximum,
        16,
        Arc::new(()),
    )?)
}

#[test]
fn frontier_is_indexed_paged_deduplicated_and_only_complete_members_are_visible() -> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let budget = DiskBudget::new(3 << 20);
        let mut s = spool(&budget, 1 << 20)?;
        let path = s.path().to_owned();
        let ids: Vec<_> = (1..=2500)
            .map(|n| (id(format, n), Some(ObjectKind::Blob)))
            .collect();
        for page in ids.chunks(512).rev() {
            s.add(page)?;
            s.add(page)?;
        }
        assert!(budget.used() > growth::INITIAL_BYTES * 3);
        assert!(budget.used() < budget.capacity());
        let plan: String = s.db.query_row(
            "EXPLAIN QUERY PLAN SELECT oid,kind FROM nodes WHERE done=0 ORDER BY oid LIMIT 128",
            [],
            |r| r.get(3),
        )?;
        assert!(plan.contains("pending"), "{plan}");
        assert!(!plan.contains("TEMP"), "{plan}");
        let mut completed = 0;
        loop {
            let page = s.pending()?;
            if page.is_empty() {
                break;
            }
            assert!(page.len() <= 128);
            assert!(page.windows(2).all(|p| p[0].0 < p[1].0));
            assert_eq!(page[0].0, id(format, completed + 1));
            let keys: Vec<_> = page.iter().map(|(id, _)| *id).collect();
            assert!(s.contains(&keys)?.iter().all(|present| !present));
            s.done(&page)?;
            assert!(s.contains(&keys)?.iter().all(|present| *present));
            completed += page.len() as u64;
        }
        assert_eq!(completed, 2500);
        assert_eq!(s.counts()?, (2500, 0));
        assert_eq!(
            s.contains(&[id(format, 2), id(format, 2501), id(format, 1)])?,
            [true, false, true]
        );
        {
            use rusqlite::StatementStatus;
            let mut lookup = s.db.prepare_cached("SELECT done FROM nodes WHERE oid=?1")?;
            lookup.reset_status(StatementStatus::VmStep);
            assert!(lookup.query_row([id(format, 1).as_ref()], |r| r.get::<_, bool>(0))?);
            assert!(lookup.get_status(StatementStatus::VmStep) < 50);
            assert_eq!(lookup.get_status(StatementStatus::FullscanStep), 0);
        }
        drop(s);
        assert!(!path.exists());
        assert_eq!(budget.used(), 0);
    }
    Ok(())
}

#[test]
fn kind_conflict_and_incomplete_done_roll_back_the_entire_batch() -> Result {
    let budget = DiskBudget::new(3 << 20);
    let mut s = spool(&budget, 1 << 20)?;
    let (a, b, c) = (
        id(ObjectFormat::Sha256, 1),
        id(ObjectFormat::Sha256, 2),
        id(ObjectFormat::Sha256, 3),
    );
    s.add(&[(a, None), (b, Some(ObjectKind::Tree))])?;
    assert!(matches!(
        s.add(&[(c, Some(ObjectKind::Blob)), (b, Some(ObjectKind::Commit))]),
        Err(MetadataError::Integrity)
    ));
    assert_eq!(s.pending()?, [(a, None), (b, Some(ObjectKind::Tree))]);
    assert!(matches!(
        s.done(&[(b, Some(ObjectKind::Tree)), (a, None)]),
        Err(MetadataError::Integrity)
    ));
    assert_eq!(s.contains(&[a, b, c])?, [false, false, false]);
    s.add(&[(a, Some(ObjectKind::Commit))])?;
    s.done(&[(a, None), (b, None)])?;
    assert_eq!(s.counts()?, (2, 0));
    Ok(())
}

#[test]
fn failed_growth_retains_prior_membership_without_a_partial_frontier() -> Result {
    let budget = DiskBudget::new(growth::INITIAL_BYTES * 3);
    let mut s = spool(&budget, 1 << 20)?;
    let initial = id(ObjectFormat::Sha256, 1);
    s.add(&[(initial, Some(ObjectKind::Commit))])?;
    s.done(&[(initial, None)])?;
    let mut accepted = 0;
    loop {
        let page: Vec<_> = (0..512)
            .map(|n| {
                (
                    id(ObjectFormat::Sha256, accepted + n + 2),
                    Some(ObjectKind::Blob),
                )
            })
            .collect();
        match s.add(&page) {
            Ok(()) => accepted += 512,
            Err(MetadataError::Budget(_)) => {
                let count: u64 =
                    s.db.query_row("SELECT count(*) FROM nodes", [], |r| r.get(0))?;
                assert_eq!(count, accepted + 1);
                assert!(s.db.is_autocommit());
                assert_eq!(
                    s.contains(&[initial, page[0].0, page[511].0])?,
                    [true, false, false]
                );
                break;
            }
            Err(e) => return Err(e.into()),
        }
        assert!(accepted < 10_000);
    }
    assert_eq!(budget.used(), growth::INITIAL_BYTES * 3);
    drop(s);
    assert_eq!(budget.used(), 0);
    let denied = DiskBudget::new(growth::INITIAL_BYTES * 3 - 1);
    assert!(spool(&denied, 1 << 20).is_err());
    assert_eq!(denied.used(), 0);
    Ok(())
}

#[test]
fn physical_input_dedup_ignores_namespace_but_rejects_conflicting_bindings() -> Result {
    let budget = DiskBudget::new(3 << 20);
    let mut s = spool(&budget, 1 << 20)?;
    let p = NativePackDescriptor {
        repository: [1; 16],
        operation: [2; 16],
        format: ObjectFormat::Sha1,
        git_checksum: id(ObjectFormat::Sha1, 1),
        object_count: 1,
        pack: ArtifactDescriptor {
            size: 100,
            digest: [3; 32],
            manifest_digest: [4; 32],
        },
        index: ArtifactDescriptor {
            size: 1100,
            digest: [5; 32],
            manifest_digest: [6; 32],
        },
    };
    assert!(!s.pack_seen(p)?);
    s.imported(p)?;
    let mut other = p;
    other.operation = [7; 16];
    other.pack.manifest_digest = [8; 32];
    assert!(s.pack_seen(other)?);
    for mutation in 0..5 {
        let mut conflict = other;
        match mutation {
            0 => conflict.pack.digest[0] ^= 1,
            1 => conflict.index.digest[0] ^= 1,
            2 => conflict.pack.size += 1,
            3 => conflict.index.size += 1,
            _ => conflict.object_count += 1,
        }
        assert!(matches!(
            s.pack_seen(conflict),
            Err(MetadataError::IdentityConflict)
        ));
    }
    assert_eq!(s.counts()?, (0, 1));
    Ok(())
}
