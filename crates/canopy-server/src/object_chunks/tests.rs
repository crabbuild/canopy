use super::*;

fn results(row: Vec<SqlValue>) -> [SqlResultSet; 1] {
    [SqlResultSet {
        columns: Vec::new(),
        rows: vec![row],
        rows_affected: 0,
    }]
}

#[test]
fn snapshot_verification_counts_the_upload_only_once() {
    let bytes = vec![17; CHUNK_BYTES * 3 + 1];
    let oid = object_id(crate::ObjectFormat::Sha1, ObjectKind::Commit, &bytes);
    let mut chunks = Chunks::new(
        oid,
        ObjectKind::Commit,
        [7; 16],
        bytes.len() as u64,
        *blake3::hash(&bytes).as_bytes(),
    )
    .unwrap();
    for (part, body) in bytes.chunks(CHUNK_BYTES).enumerate() {
        let query = chunks.snapshot_query();
        assert_eq!(query.statements[0].sql.contains("COUNT(*)"), part == 0);
        // Separate async reads have no shared snapshot: keep their count check.
        assert!(chunks.query().statements[0].sql.contains("COUNT(*)"));
        let mut row = vec![SqlValue::Blob(body.to_vec())];
        if part == 0 {
            row.push(SqlValue::Integer(bytes.len().div_ceil(CHUNK_BYTES) as i64));
        }
        assert!(chunks.append_snapshot(&results(row)));
    }
    assert!(chunks.complete());
    assert_eq!(chunks.finish(), Some(bytes));
}

#[test]
fn snapshot_verification_rejects_extra_missing_and_malformed_chunks() {
    let bytes = vec![17; INLINE_OBJECT_LIMIT + 1];
    let oid = object_id(crate::ObjectFormat::Sha1, ObjectKind::Commit, &bytes);
    let mut chunks = Chunks::new(
        oid,
        ObjectKind::Commit,
        [7; 16],
        bytes.len() as u64,
        *blake3::hash(&bytes).as_bytes(),
    )
    .unwrap();
    let first = bytes[..CHUNK_BYTES].to_vec();
    assert!(!chunks.append_snapshot(&results(vec![
        SqlValue::Blob(first.clone()),
        SqlValue::Integer(3)
    ])));
    assert_eq!(chunks.part, 0);
    assert!(chunks.append_snapshot(&results(vec![SqlValue::Blob(first), SqlValue::Integer(2)])));
    assert!(!chunks.append_snapshot(&[]));
    assert!(!chunks.append_snapshot(&results(vec![SqlValue::Blob(vec![17, 17])])));
    assert!(!chunks.append_snapshot(&results(vec![SqlValue::Integer(17)])));
    assert_eq!(chunks.part, 1);
    assert!(chunks.append_snapshot(&results(vec![SqlValue::Blob(
        bytes[CHUNK_BYTES..].to_vec()
    )])));
    assert_eq!(chunks.finish(), Some(bytes));
}

#[test]
fn snapshot_verification_keeps_both_digest_checks() {
    let bytes = vec![17; INLINE_OBJECT_LIMIT + 1];
    for format in [crate::ObjectFormat::Sha1, crate::ObjectFormat::Sha256] {
        for corrupt_oid in [false, true] {
            let oid = object_id(
                format,
                ObjectKind::Commit,
                if corrupt_oid {
                    b"wrong identity"
                } else {
                    &bytes
                },
            );
            let mut digest = *blake3::hash(&bytes).as_bytes();
            if !corrupt_oid {
                digest[0] ^= 1;
            }
            let mut chunks =
                Chunks::new(oid, ObjectKind::Commit, [7; 16], bytes.len() as u64, digest).unwrap();
            assert!(chunks.append_snapshot(&results(vec![
                SqlValue::Blob(bytes[..CHUNK_BYTES].to_vec()),
                SqlValue::Integer(2)
            ])));
            assert!(chunks.append_snapshot(&results(vec![SqlValue::Blob(
                bytes[CHUNK_BYTES..].to_vec()
            )])));
            assert!(chunks.complete());
            assert!(chunks.finish().is_none());
        }
    }
}
