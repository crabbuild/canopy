use super::*;

pub(super) fn oid(format: ObjectFormat, n: u64) -> ObjectId {
    let mut bytes = vec![0; format.bytes()];
    let start = bytes.len() - 8;
    bytes[start..].copy_from_slice(&n.to_be_bytes());
    ObjectId::try_from(bytes.as_slice()).unwrap()
}
pub(super) fn synthetic(
    format: ObjectFormat,
    n: u64,
    kind: ObjectKind,
    edges: &[(u64, ObjectKind)],
) -> (ObjectHeader, Vec<TypedEdge>) {
    let edges: Vec<_> = edges
        .iter()
        .map(|(n, k)| TypedEdge {
            child: oid(format, *n),
            expected_kind: *k,
        })
        .collect();
    let object = CanonicalObject {
        oid: oid(format, n),
        kind,
        size: n,
        digest: *blake3::hash(&n.to_le_bytes()).as_bytes(),
    };
    (header(object, &edges), edges)
}
pub(super) fn insert(spool: &mut Spool, objects: &[(ObjectHeader, Vec<TypedEdge>)]) -> Result {
    // Private synthetic graph fixtures bypass physical verification to exercise
    // cycles and type faults. No production constructor can accept these rows.
    for page in objects.chunks(PAGE_OBJECTS) {
        let tx = spool.connection.transaction()?;
        for (h, edges) in page {
            tx.execute("INSERT INTO objects(oid,kind,size,digest,edge_count,edge_digest) VALUES(?1,?2,?3,?4,?5,?6)",params![h.object.oid.as_ref(),h.object.kind.git_name(),h.object.size as i64,h.object.digest.as_slice(),h.edge_count as i64,h.edge_digest.as_slice()])?;
            for e in edges {
                tx.execute(
                    "INSERT INTO object_edges VALUES(?1,?2,?3)",
                    params![
                        h.object.oid.as_ref(),
                        e.child.as_ref(),
                        e.expected_kind.git_name()
                    ],
                )?;
            }
        }
        tx.commit()?;
    }
    Ok(())
}
fn spool(root: &Path, budget: DiskBudget, ctx: ClosureContext) -> Result<Spool> {
    Ok(Spool::new(
        root,
        budget,
        ctx,
        limits(),
        Arc::new(AtomicBool::new(false)),
    )?)
}

#[test]
fn deep_chain_wide_fanout_and_many_ready_leaves_do_not_require_a_heap_graph() -> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let root = tempfile::TempDir::new()?;
        let budget = DiskBudget::new(64 << 20);
        let mut spool = spool(root.path(), budget.clone(), context(format))?;
        let leaf = synthetic(format, 1, ObjectKind::Blob, &[]);
        insert(&mut spool, &[leaf])?;
        // 10,000-deep chain, then 1,200 parents of a common leaf.
        for page in (2..10_002).collect::<Vec<_>>().chunks(PAGE_OBJECTS) {
            let nodes: Vec<_> = page
                .iter()
                .map(|n| {
                    synthetic(
                        format,
                        *n,
                        ObjectKind::Tag,
                        &[(
                            *n - 1,
                            if *n == 2 {
                                ObjectKind::Blob
                            } else {
                                ObjectKind::Tag
                            },
                        )],
                    )
                })
                .collect();
            insert(&mut spool, &nodes)?;
        }
        let fanout: Vec<_> = (10_002..11_202)
            .map(|n| synthetic(format, n, ObjectKind::Tree, &[(1, ObjectKind::Blob)]))
            .collect();
        insert(&mut spool, &fanout)?;
        let leaves: Vec<_> = (11_202..12_402)
            .map(|n| synthetic(format, n, ObjectKind::Blob, &[]))
            .collect();
        insert(&mut spool, &leaves)?;
        let edges: Vec<_> = (11_202..12_402).map(|n| (n, ObjectKind::Blob)).collect();
        insert(
            &mut spool,
            &[synthetic(format, 12_402, ObjectKind::Tree, &edges)],
        )?;
        spool.certify_graph()?;
        let proof = spool.witness()?;
        assert_eq!((proof.object_count(), proof.edge_count()), (12_402, 12_400));
        assert_eq!(
            spool.connection.query_row(
                "SELECT count(*) FROM objects WHERE done=1 AND pending=0",
                [],
                |r| r.get::<_, u64>(0)
            )?,
            12_402
        );
        // Critical paged queries must use their index and avoid a temp sort.
        for (sql, index) in [
            (
                "SELECT oid FROM objects WHERE pending=0 AND done=0 ORDER BY oid LIMIT 512",
                "ready_objects",
            ),
            (
                "SELECT oid FROM lookups WHERE resolved=0 ORDER BY oid LIMIT 512",
                "unresolved_lookups",
            ),
            (
                "SELECT parent FROM object_edges WHERE child=x'00' AND parent>x'00' ORDER BY parent LIMIT 512",
                "edge_child",
            ),
        ] {
            let rows: Vec<String> = spool
                .connection
                .prepare(&format!("EXPLAIN QUERY PLAN {sql}"))?
                .query_map([], |r| r.get(3))?
                .collect::<rusqlite::Result<_>>()?;
            assert!(rows.iter().any(|r| r.contains(index)), "{rows:?}");
            assert!(!rows.iter().any(|r| r.contains("TEMP B-TREE")), "{rows:?}");
        }
        drop(spool);
        assert_eq!(budget.used(), 0);
        assert_eq!(std::fs::read_dir(root.path())?.count(), 0);
    }
    Ok(())
}

#[test]
fn cycles_missing_children_and_wrong_types_cannot_certify_even_with_base_overlaps() -> Result {
    let format = ObjectFormat::Sha256;
    for case in 0..4 {
        let root = tempfile::TempDir::new()?;
        let budget = DiskBudget::new(64 << 20);
        let mut ctx = context(format);
        ctx.base = Some(base(ctx));
        let mut spool = spool(root.path(), budget.clone(), ctx)?;
        let nodes = match case {
            0 => vec![
                synthetic(format, 1, ObjectKind::Tag, &[(1, ObjectKind::Tag)]),
                synthetic(format, 3, ObjectKind::Blob, &[]),
            ],
            1 => vec![
                synthetic(format, 1, ObjectKind::Tag, &[(2, ObjectKind::Tag)]),
                synthetic(format, 2, ObjectKind::Tag, &[(1, ObjectKind::Tag)]),
                synthetic(format, 3, ObjectKind::Blob, &[]),
            ],
            2 => vec![synthetic(
                format,
                1,
                ObjectKind::Tree,
                &[(2, ObjectKind::Blob)],
            )],
            _ => vec![
                synthetic(format, 1, ObjectKind::Tree, &[(2, ObjectKind::Blob)]),
                synthetic(format, 2, ObjectKind::Tree, &[]),
            ],
        };
        insert(&mut spool, &nodes)?;
        spool.prepare_lookups()?;
        let ids = spool.lookup_page()?;
        let objects = ids
            .iter()
            .map(|id| {
                nodes
                    .iter()
                    .find(|(h, _)| h.object.oid == *id)
                    .map(|(h, _)| BaseObject {
                        header: *h,
                        certified: true,
                    })
            })
            .collect();
        spool.apply_base(
            &ids,
            BaseBatch {
                base: ctx.base.unwrap(),
                objects,
            },
        )?;
        let result = spool.certify_graph();
        match case {
            0 | 1 => assert!(matches!(result, Err(ClosureError::Cycle))),
            2 => assert!(matches!(result,Err(ClosureError::Missing(id)) if id==oid(format,2))),
            _ => assert!(
                matches!(result,Err(ClosureError::Kind {oid:id,expected:ObjectKind::Blob,actual:ObjectKind::Tree}) if id==oid(format,2))
            ),
        }
        drop(spool);
        assert_eq!(budget.used(), 0);
    }
    Ok(())
}

#[test]
fn base_batches_bind_generation_order_shape_and_the_complete_canonical_header() -> Result {
    let format = ObjectFormat::Sha256;
    for case in 0..10 {
        let root = tempfile::TempDir::new()?;
        let budget = DiskBudget::new(64 << 20);
        let mut ctx = context(format);
        ctx.base = Some(base(ctx));
        let mut spool = spool(root.path(), budget.clone(), ctx)?;
        let nodes = vec![
            synthetic(format, 1, ObjectKind::Blob, &[]),
            synthetic(format, 2, ObjectKind::Tree, &[(1, ObjectKind::Blob)]),
        ];
        insert(&mut spool, &nodes)?;
        spool.prepare_lookups()?;
        let ids = spool.lookup_page()?;
        assert_eq!(ids.len(), 2); // Shared children are looked up only once.
        let mut batch = BaseBatch {
            base: ctx.base.unwrap(),
            objects: nodes
                .iter()
                .map(|(h, _)| {
                    Some(BaseObject {
                        header: *h,
                        certified: true,
                    })
                })
                .collect(),
        };
        match case {
            0 => batch.base.generation += 1,
            1 => {
                batch.objects.pop();
            }
            2 => batch.objects.reverse(),
            3 => batch.objects[0].as_mut().unwrap().header.object.digest[0] ^= 1,
            4 => batch.objects[0].as_mut().unwrap().header.object.size += 1,
            5 => batch.objects[1].as_mut().unwrap().header.edge_count += 1,
            6 => batch.objects[1].as_mut().unwrap().header.edge_digest[0] ^= 1,
            7 => batch.objects[1].as_mut().unwrap().header.object.kind = ObjectKind::Tag,
            8 => batch.objects[0].as_mut().unwrap().certified = false,
            _ => batch.base.catalog.artifact.digest[0] ^= 1,
        }
        let result = spool.apply_base(&ids, batch);
        match case {
            3..=7 => assert!(matches!(
                result,
                Err(ClosureError::Metadata(MetadataError::IdentityConflict))
            )),
            8 => assert!(matches!(result, Err(ClosureError::Uncertified(_)))),
            _ => assert!(matches!(result, Err(ClosureError::Integrity))),
        }
        assert_eq!(spool.lookup_page()?, ids); // Failed batch is atomic.
        assert_eq!(
            spool
                .connection
                .query_row("SELECT count(*) FROM base_objects", [], |r| r
                    .get::<_, u64>(0))?,
            0
        );
        drop(spool);
        assert_eq!(budget.used(), 0);
    }
    Ok(())
}
