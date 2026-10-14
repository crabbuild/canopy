//! Admission for growing private SQLite files. Immutable artifacts keep their
//! existing format; only disposable construction-time capacity changes.
use super::{AdmittedFile, DiskBudget, DiskReservation, MetadataError};
use rusqlite::{Connection, Transaction};
use std::error::Error;

pub(in crate::packs) const INITIAL_BYTES: u64 = 64 << 10;
const PAGE_BYTES: u64 = 4096;
const SCRATCH_FACTOR: u64 = 3;

pub(in crate::packs) fn reserve(
    budget: &DiskBudget,
    maximum: u64,
) -> Result<DiskReservation, MetadataError> {
    Ok(budget.try_reserve(maximum.min(INITIAL_BYTES) * SCRATCH_FACTOR)?)
}

pub(in crate::packs) fn configure(
    connection: &Connection,
    file: &mut AdmittedFile,
) -> Result<(), MetadataError> {
    let pages = file.reservation().bytes() / SCRATCH_FACTOR / PAGE_BYTES;
    connection.pragma_update(None, "max_page_count", pages)?;
    let actual: u64 = connection.pragma_query_value(None, "max_page_count", |r| r.get(0))?;
    if actual != pages {
        return Err(MetadataError::Integrity);
    }
    Ok(())
}

/// The body must have no externally visible effects. Return cursors, digests,
/// and counters as the transaction's result and adopt them only after success.
/// A retry replays the complete body after SQLite has rolled it back. Resources
/// backing replayable inputs must remain owned throughout all attempts.
pub(in crate::packs) fn transaction<T, E>(
    connection: &mut Connection,
    file: &mut AdmittedFile,
    maximum: u64,
    mut body: impl FnMut(&Transaction<'_>) -> Result<T, E>,
) -> Result<T, E>
where
    E: Error + From<MetadataError> + 'static,
{
    loop {
        let result = (|| {
            let tx = connection.transaction().map_err(MetadataError::from)?;
            let value = body(&tx)?;
            tx.commit().map_err(MetadataError::from)?;
            Ok(value)
        })();
        let error = match result {
            Ok(value) => return Ok(value),
            Err(error) => error,
        };
        if !disk_full(&error) {
            return Err(error);
        }
        // Drop/automatic rollback may have failed. Never replay a body while a
        // previous transaction remains live, even if more disk becomes free.
        if !connection.is_autocommit() {
            return Err(MetadataError::Integrity.into());
        }
        let current: u64 = connection
            .pragma_query_value(None, "max_page_count", |r| r.get(0))
            .map_err(MetadataError::from)?;
        let bytes = current
            .checked_mul(PAGE_BYTES)
            .ok_or(MetadataError::Limit)?;
        if bytes >= maximum {
            return Err(MetadataError::Limit.into());
        }
        let next = bytes
            .checked_mul(2)
            .ok_or(MetadataError::Limit)?
            .min(maximum);
        // Admit database + rollback journal + conservative SQLite overhead
        // before allowing SQLite to allocate any additional pages. A failed
        // admission leaves both the old cap and its reservation unchanged.
        file.reservation()
            .resize(
                next.checked_mul(SCRATCH_FACTOR)
                    .ok_or(MetadataError::Limit)?,
            )
            .map_err(MetadataError::from)?;
        connection
            .pragma_update(None, "max_page_count", next / PAGE_BYTES)
            .map_err(MetadataError::from)?;
        let actual: u64 = connection
            .pragma_query_value(None, "max_page_count", |r| r.get(0))
            .map_err(MetadataError::from)?;
        if actual != next / PAGE_BYTES {
            return Err(MetadataError::Integrity.into());
        }
    }
}

fn disk_full(error: &(dyn Error + 'static)) -> bool {
    let mut current = Some(error);
    while let Some(error) = current {
        if let Some(sql) = error.downcast_ref::<rusqlite::Error>() {
            return sql.sqlite_error_code() == Some(rusqlite::ErrorCode::DiskFull);
        }
        current = error.source();
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    type Result<T = ()> = std::result::Result<T, Box<dyn Error>>;

    fn scratch(budget: &DiskBudget, maximum: u64) -> Result<(Connection, AdmittedFile)> {
        let mut file =
            AdmittedFile::new(tempfile::NamedTempFile::new()?, reserve(budget, maximum)?);
        let mut db = Connection::open(file.file().path())?;
        db.execute_batch("PRAGMA page_size=4096; PRAGMA journal_mode=DELETE; PRAGMA mmap_size=0;")?;
        configure(&db, &mut file)?;
        transaction(&mut db, &mut file, maximum, |tx| {
            tx.execute_batch("CREATE TABLE records(id INTEGER PRIMARY KEY, body BLOB NOT NULL)")?;
            Ok::<_, MetadataError>(())
        })?;
        Ok((db, file))
    }

    #[test]
    fn growth_replays_only_rolled_back_rows_and_reserves_before_pages() -> Result {
        let maximum = 1 << 20;
        let budget = DiskBudget::new(maximum * SCRATCH_FACTOR);
        let (mut db, mut file) = scratch(&budget, maximum)?;
        assert_eq!(budget.used(), INITIAL_BYTES * SCRATCH_FACTOR);
        let path = file.file().path().to_owned();
        let mut attempts = 0;
        let count = transaction(&mut db, &mut file, maximum, |tx| {
            attempts += 1;
            // A capacity failure after a successful prefix must not survive.
            assert_eq!(
                tx.query_row("SELECT count(*) FROM records", [], |r| r.get::<_, u64>(0))?,
                0
            );
            for id in 0..100 {
                tx.execute("INSERT INTO records VALUES(?1,zeroblob(4096))", [id])?;
                let mut journal = path.as_os_str().to_owned();
                journal.push("-journal");
                let actual = std::fs::metadata(&path)?.len()
                    + std::fs::metadata(std::path::Path::new(&journal))
                        .map(|m| m.len())
                        .unwrap_or(0);
                assert!(actual <= budget.used());
            }
            Ok::<_, MetadataError>(100)
        })?;
        assert!(attempts >= 3);
        assert_eq!(count, 100);
        assert_eq!(
            db.query_row("SELECT count(*) FROM records", [], |r| r.get::<_, u64>(0))?,
            100
        );
        let cap: u64 = db.pragma_query_value(None, "max_page_count", |r| r.get(0))?;
        assert_eq!(budget.used(), cap * PAGE_BYTES * SCRATCH_FACTOR);
        assert!(budget.used() < maximum * SCRATCH_FACTOR);
        drop(db);
        drop(file);
        assert_eq!(budget.used(), 0);
        assert!(!path.exists());
        Ok(())
    }

    #[test]
    fn denied_growth_keeps_cap_credit_and_prior_committed_rows() -> Result {
        let maximum = 1 << 20;
        let budget = DiskBudget::new(INITIAL_BYTES * SCRATCH_FACTOR);
        let (mut db, mut file) = scratch(&budget, maximum)?;
        db.execute("INSERT INTO records VALUES(-1,x'01')", [])?;
        let error = transaction(&mut db, &mut file, maximum, |tx| {
            for id in 0..100 {
                tx.execute("INSERT INTO records VALUES(?1,zeroblob(4096))", [id])?;
            }
            Ok::<_, MetadataError>(())
        })
        .unwrap_err();
        assert!(matches!(error, MetadataError::Budget(_)));
        assert!(db.is_autocommit());
        assert_eq!(budget.used(), INITIAL_BYTES * SCRATCH_FACTOR);
        assert_eq!(
            db.pragma_query_value(None, "max_page_count", |r| r.get::<_, u64>(0))?,
            INITIAL_BYTES / PAGE_BYTES
        );
        assert_eq!(
            db.query_row("SELECT count(*) FROM records WHERE id=-1", [], |r| r
                .get::<_, u64>(0))?,
            1
        );
        assert_eq!(
            db.query_row("SELECT count(*) FROM records WHERE id>=0", [], |r| r
                .get::<_, u64>(0))?,
            0
        );
        drop(db);
        drop(file);
        assert_eq!(budget.used(), 0);
        Ok(())
    }

    #[test]
    fn configured_ceiling_and_domain_errors_never_escape_or_retry() -> Result {
        // A non-power-of-two ceiling proves the last growth is clipped exactly.
        let maximum = 96 << 10;
        let budget = DiskBudget::new(maximum * SCRATCH_FACTOR);
        let (mut db, mut file) = scratch(&budget, maximum)?;
        let mut attempts = 0;
        let error = transaction(&mut db, &mut file, maximum, |tx| {
            attempts += 1;
            for id in 0..100 {
                tx.execute("INSERT INTO records VALUES(?1,zeroblob(4096))", [id])?;
            }
            Ok::<_, MetadataError>(())
        })
        .unwrap_err();
        assert!(matches!(error, MetadataError::Limit));
        assert_eq!(attempts, 2);
        assert_eq!(budget.used(), maximum * SCRATCH_FACTOR);
        assert_eq!(
            db.query_row("SELECT count(*) FROM records", [], |r| r.get::<_, u64>(0))?,
            0
        );
        attempts = 0;
        let error = transaction(&mut db, &mut file, maximum, |tx| {
            attempts += 1;
            tx.execute("INSERT INTO records VALUES(1,x'01')", [])?;
            Err::<(), _>(MetadataError::IdentityConflict)
        })
        .unwrap_err();
        assert!(matches!(error, MetadataError::IdentityConflict));
        assert_eq!(attempts, 1);
        assert_eq!(
            db.query_row("SELECT count(*) FROM records", [], |r| r.get::<_, u64>(0))?,
            0
        );
        drop(db);
        drop(file);
        assert_eq!(budget.used(), 0);
        Ok(())
    }
}
