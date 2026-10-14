use super::*;

#[test]
fn dependency_type_checks_cover_kind_and_require_a_committed_certificate()
-> Result<(), Box<dyn std::error::Error>> {
    use cellule_ltx::rusqlite::{Connection, params, params_from_iter, types::Value};
    let db = Connection::open_in_memory()?;
    db.execute_batch(crate::SCHEMA)?;
    db.execute("INSERT INTO git_packs(digest, format, bytes, index_digest, index_bytes) VALUES (zeroblob(32), 'sha1', 1, zeroblob(32), 1)", [])?;
    let ids = [Oid::Sha1([1; 20]), Oid::Sha1([2; 20]), Oid::Sha1([3; 20])];
    for (oid, kind) in ids.iter().zip(["blob", "tree", "commit"]) {
        db.execute("INSERT INTO objects(oid, kind, size, digest, pack, graph_valid, graph_count, graph_digest) VALUES (?1, ?2, 0, zeroblob(32), zeroblob(32), 1, 0, zeroblob(32))", params![oid.as_ref(), kind])?;
    }
    for oid in &ids[..2] {
        db.execute("INSERT INTO object_closure VALUES (?1)", [oid.as_ref()])?;
    }
    let batch = certified_kind_query(&ids);
    let query = &batch.statements[0];
    let values: Vec<_> = query
        .parameters
        .iter()
        .map(|value| match value {
            SqlValue::Blob(value) => Value::Blob(value.clone()),
            _ => unreachable!(),
        })
        .collect();
    let plans: Vec<String> = db
        .prepare(&format!("EXPLAIN QUERY PLAN {}", query.sql))?
        .query_map(params_from_iter(&values), |row| row.get(3))?
        .collect::<Result<_, _>>()?;
    assert!(
        plans
            .iter()
            .any(|plan| plan.contains("COVERING INDEX objects_by_oid_kind")),
        "{plans:?}"
    );
    let rows: Vec<(Vec<u8>, String)> = db
        .prepare(&query.sql)?
        .query_map(params_from_iter(&values), |row| {
            Ok((row.get(0)?, row.get(1)?))
        })?
        .collect::<Result<_, _>>()?;
    assert_eq!(
        rows,
        vec![
            (ids[0].to_vec(), "blob".into()),
            (ids[1].to_vec(), "tree".into())
        ]
    );
    Ok(())
}

#[test]
fn certificate_codec_rejects_empty_oversized_and_malformed_batches() {
    for count in [0, MAX_CERTIFICATES + 1] {
        let mut encoder = BoundedEncoder::new(1 << 20).unwrap();
        encoder.write_count(count).unwrap();
        let input = encoder.finish();
        assert!(
            CertificateBatch::decode(&mut BoundedDecoder::new(&input, 1 << 20).unwrap()).is_err()
        );
    }
    let mut encoder = BoundedEncoder::new(1 << 20).unwrap();
    encoder.write_count(1).unwrap();
    encoder.write_bytes(&[0; 19]).unwrap();
    let input = encoder.finish();
    assert!(CertificateBatch::decode(&mut BoundedDecoder::new(&input, 1 << 20).unwrap()).is_err());
    let batch = CertificateBatch(vec![crate::ObjectId::Sha1([1; 20]); MAX_CERTIFICATES]);
    let mut encoder = BoundedEncoder::new(1 << 20).unwrap();
    batch.encode(&mut encoder).unwrap();
    let input = encoder.finish();
    let mut decoder = BoundedDecoder::new(&input, 1 << 20).unwrap();
    assert_eq!(CertificateBatch::decode(&mut decoder).unwrap().0, batch.0);
    decoder.finish().unwrap();
}

#[test]
fn repeated_edges_share_one_proof_but_different_types_do_not() {
    let mut body = Vec::new();
    for (mode, name) in [
        ("100644", "a"),
        ("100755", "b"),
        ("40000", "c"),
        ("160000", "d"),
    ] {
        body.extend(format!("{mode} {name}\0").as_bytes());
        body.extend([1; 20]);
    }
    assert_eq!(
        edges(crate::ObjectFormat::Sha1, ObjectKind::Tree, &body),
        Some(vec![
            (crate::ObjectId::Sha1([1; 20]), Some(ObjectKind::Blob)),
            (crate::ObjectId::Sha1([1; 20]), Some(ObjectKind::Tree))
        ])
    );
}
