use super::*;
use crate::{INLINE_OBJECT_LIMIT, object_id};
use cellule_ltx::rusqlite::{Connection, StatementStatus, params};

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

fn database() -> Result<Connection> {
    let db = Connection::open_in_memory()?;
    db.execute_batch(crate::SCHEMA)?;
    Ok(db)
}

fn insert(db: &Connection, body: &[u8]) -> Result<crate::ObjectId> {
    let oid = object_id(crate::ObjectFormat::Sha1, ObjectKind::Blob, body);
    db.execute("INSERT INTO git_packs (digest, format, bytes, index_digest, index_bytes) VALUES (zeroblob(32), 'sha1', 1, zeroblob(32), 1) ON CONFLICT DO NOTHING", [])?;
    db.execute("INSERT INTO objects (oid, kind, size, digest, pack, graph_valid, graph_count, graph_digest) VALUES (?1, 'blob', ?2, ?3, zeroblob(32), 1, 0, ?4) ON CONFLICT(oid) DO NOTHING",
        params![oid.as_ref(), body.len() as i64, blake3::hash(body).as_bytes().as_slice(), blake3::hash(b"").as_bytes().as_slice()])?;
    Ok(oid)
}

fn high_water(db: &Connection) -> Result<i64> {
    Ok(db.query_row(
        "SELECT COALESCE(MAX(sequence), 0) FROM objects",
        [],
        |row| row.get(0),
    )?)
}

fn page(db: &Connection, after: i64, high: i64) -> Result<(Vec<crate::ObjectId>, i64)> {
    let mut statement = db.prepare(CHANGED_HEADERS)?;
    let rows = statement
        .query_map(params![after, high, MAX_OBJECTS as i64], |row| {
            Ok(vec![
                SqlValue::Integer(row.get(0)?),
                SqlValue::Blob(row.get(1)?),
                SqlValue::Integer(row.get(2)?),
            ])
        })?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    // An indexed range must neither walk the old history nor sort it anew.
    assert_eq!(statement.get_status(StatementStatus::FullscanStep), 0);
    assert_eq!(statement.get_status(StatementStatus::Sort), 0);
    Ok(decode_headers(&rows)?)
}

#[test]
fn insertion_cursor_finds_lower_oids_and_excludes_later_publications() -> Result {
    let db = database()?;
    let mut bodies: Vec<_> = (0..260)
        .map(|n| format!("cursor-{n}").into_bytes())
        .collect();
    bodies.sort_by_key(|body| {
        std::cmp::Reverse(object_id(crate::ObjectFormat::Sha1, ObjectKind::Blob, body))
    });
    let mut expected = Vec::new();
    for body in &bodies[..259] {
        expected.push(insert(&db, body)?);
    }
    let high = high_water(&db)?;
    let late = insert(&db, &bodies[259])?;
    assert!(late < *expected.last().ok_or("missing object")?);
    let mut after = 0;
    let mut actual = Vec::new();
    while after < high {
        let (ids, through) = page(&db, after, high)?;
        assert!(!ids.is_empty());
        assert!(ids.len() <= MAX_OBJECTS);
        actual.extend(ids);
        after = through;
    }
    assert_eq!(actual, expected);
    assert!(page(&db, after, high)?.0.is_empty());
    assert_eq!(page(&db, after, high_water(&db)?)?.0, vec![late]);
    Ok(())
}

#[test]
fn metadata_page_is_independent_of_body_sizes() -> Result {
    let db = database()?;
    let first = insert(&db, &vec![1; INLINE_OBJECT_LIMIT / 2])?;
    let second = insert(&db, &vec![2; INLINE_OBJECT_LIMIT])?;
    let third = insert(&db, b"third")?;
    let high = high_water(&db)?;
    let (ids, after) = page(&db, 0, high)?;
    assert_eq!(ids, vec![first, second, third]);
    assert_eq!(after, high);
    Ok(())
}

#[test]
fn duplicate_rollback_and_deletion_do_not_hide_subsequent_inserts() -> Result {
    let db = database()?;
    let original = insert(&db, b"original")?;
    let after = high_water(&db)?;
    insert(&db, b"original")?;
    assert_eq!(high_water(&db)?, after);
    db.execute_batch("SAVEPOINT failed_batch")?;
    insert(&db, b"rolled back")?;
    db.execute_batch("ROLLBACK TO failed_batch; RELEASE failed_batch")?;
    assert_eq!(high_water(&db)?, after);
    db.execute("DELETE FROM objects WHERE oid = ?1", [original.as_ref()])?;
    // The product has no collector yet. Never reusing a committed cursor also
    // protects a future fenced collection from hiding newly inserted rows.
    let next = insert(&db, b"after deletion")?;
    assert!(high_water(&db)? > after);
    assert_eq!(page(&db, after, high_water(&db)?)?.0, vec![next]);
    Ok(())
}

#[test]
fn small_increment_uses_bounded_sql_work_after_large_history() -> Result {
    let db = database()?;
    db.execute_batch("BEGIN")?;
    for n in 0..10_000 {
        insert(&db, format!("history-{n}").as_bytes())?;
    }
    db.execute_batch("COMMIT")?;
    let after = high_water(&db)?;
    let mut expected = Vec::new();
    for body in [b"one".as_slice(), b"two", b"three"] {
        expected.push(insert(&db, body)?);
    }
    assert_eq!(page(&db, after, high_water(&db)?)?.0, expected);
    Ok(())
}
