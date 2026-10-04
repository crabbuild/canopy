//! Disposable, admitted graph frontier and source deduplication. No repository
//! SQL rows, history-sized Rust sets, or authority are stored here.
use crate::git_objects::ReadOwner;
use crate::packs::{
    metadata::{AdmittedFile, MetadataError, growth},
    sources::NativePackDescriptor,
};
use crate::{ObjectId, ObjectKind};
use cellule_ltx::DiskBudget;
use rusqlite::{Connection, OptionalExtension, params};
use std::sync::Arc;

type PackBinding = (Vec<u8>, Vec<u8>, u64, u64, u32);

pub(in crate::packs) struct GraphSpool {
    db: Connection,
    file: AdmittedFile,
    maximum: u64,
    // Drops after SQLite, journal and file cleanup, including blocking jobs.
    _owner: ReadOwner,
}
impl GraphSpool {
    pub(in crate::packs) fn new(
        root: Arc<tempfile::TempDir>,
        budget: DiskBudget,
        maximum: u64,
        cache_kib: u32,
        owner: ReadOwner,
    ) -> Result<Self, MetadataError> {
        let reservation = growth::reserve(&budget, maximum)?;
        let mut file = AdmittedFile::new(
            tempfile::Builder::new()
                .prefix("canopy-graph-")
                .tempfile_in(root.path())?,
            reservation,
        );
        file.retain_workspace(root);
        let mut db = Connection::open(file.file().path())?;
        db.execute_batch("PRAGMA page_size=4096; PRAGMA journal_mode=DELETE; PRAGMA synchronous=FULL; PRAGMA foreign_keys=ON; PRAGMA trusted_schema=OFF; PRAGMA mmap_size=0; PRAGMA temp_store=MEMORY;")?;
        db.pragma_update(None, "cache_size", -(i64::from(cache_kib)))?;
        growth::configure(&db, &mut file)?;
        growth::transaction(&mut db, &mut file, maximum, |tx| {
            tx.execute_batch("CREATE TABLE nodes(oid BLOB PRIMARY KEY,kind TEXT,done INTEGER NOT NULL DEFAULT 0 CHECK(done IN (0,1))) WITHOUT ROWID; CREATE INDEX pending ON nodes(done,oid); CREATE TABLE packs(checksum BLOB PRIMARY KEY,pack BLOB NOT NULL,idx BLOB NOT NULL,pack_bytes INTEGER NOT NULL,idx_bytes INTEGER NOT NULL,objects INTEGER NOT NULL) WITHOUT ROWID;")?;
            Ok::<_, MetadataError>(())
        })?;
        Ok(Self {
            db,
            file,
            maximum,
            _owner: owner,
        })
    }
    pub(in crate::packs) fn add(
        &mut self,
        ids: &[(ObjectId, Option<ObjectKind>)],
    ) -> Result<(), MetadataError> {
        if ids.len() > 512 {
            return Err(MetadataError::Limit);
        }
        growth::transaction(&mut self.db, &mut self.file, self.maximum, |tx| {
            let mut insert = tx.prepare_cached(
                "INSERT INTO nodes(oid,kind) VALUES(?1,?2) ON CONFLICT DO NOTHING",
            )?;
            let mut read = tx.prepare_cached("SELECT kind FROM nodes WHERE oid=?1")?;
            let mut update =
                tx.prepare_cached("UPDATE nodes SET kind=?2 WHERE oid=?1 AND kind IS NULL")?;
            for (id, kind) in ids {
                insert.execute(params![id.as_ref(), kind.map(ObjectKind::git_name)])?;
                if let Some(kind) = kind {
                    let existing: Option<String> = read.query_row([id.as_ref()], |r| r.get(0))?;
                    if existing
                        .as_deref()
                        .is_some_and(|old| old != kind.git_name())
                    {
                        return Err(MetadataError::Integrity);
                    }
                    update.execute(params![id.as_ref(), kind.git_name()])?;
                }
            }
            Ok::<_, MetadataError>(())
        })
    }
    pub(in crate::packs) fn pending(
        &self,
    ) -> Result<Vec<(ObjectId, Option<ObjectKind>)>, MetadataError> {
        let mut query = self
            .db
            .prepare_cached("SELECT oid,kind FROM nodes WHERE done=0 ORDER BY oid LIMIT 128")?;
        query
            .query_map([], |row| {
                let id: Vec<u8> = row.get(0)?;
                let name: Option<String> = row.get(1)?;
                let kind = name
                    .as_deref()
                    .map(|name| match name {
                        "blob" => Ok(ObjectKind::Blob),
                        "tree" => Ok(ObjectKind::Tree),
                        "commit" => Ok(ObjectKind::Commit),
                        "tag" => Ok(ObjectKind::Tag),
                        _ => Err(rusqlite::Error::InvalidQuery),
                    })
                    .transpose()?;
                Ok((
                    ObjectId::try_from(id).map_err(|_| rusqlite::Error::InvalidQuery)?,
                    kind,
                ))
            })?
            .collect::<Result<_, _>>()
            .map_err(Into::into)
    }
    pub(in crate::packs) fn done(
        &mut self,
        ids: &[(ObjectId, Option<ObjectKind>)],
    ) -> Result<(), MetadataError> {
        growth::transaction(&mut self.db, &mut self.file, self.maximum, |tx| {
            let mut update =
                tx.prepare_cached("UPDATE nodes SET done=1 WHERE oid=?1 AND kind IS NOT NULL")?;
            for (id, _) in ids {
                if update.execute([id.as_ref()])? != 1 {
                    return Err(MetadataError::Integrity);
                }
            }
            Ok::<_, MetadataError>(())
        })
    }
    pub(in crate::packs) fn pack_seen(
        &self,
        p: NativePackDescriptor,
    ) -> Result<bool, MetadataError> {
        let old: Option<PackBinding> = self
            .db
            .query_row(
                "SELECT pack,idx,pack_bytes,idx_bytes,objects FROM packs WHERE checksum=?1",
                [p.git_checksum.as_ref()],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?)),
            )
            .optional()?;
        let Some((pack, idx, pack_bytes, idx_bytes, objects)) = old else {
            return Ok(false);
        };
        if pack != p.pack.digest
            || idx != p.index.digest
            || pack_bytes != p.pack.size
            || idx_bytes != p.index.size
            || objects != p.object_count
        {
            return Err(MetadataError::IdentityConflict);
        }
        Ok(true)
    }
    pub(in crate::packs) fn imported(
        &mut self,
        p: NativePackDescriptor,
    ) -> Result<(), MetadataError> {
        growth::transaction(&mut self.db, &mut self.file, self.maximum, |tx| {
            tx.execute(
                "INSERT INTO packs VALUES(?1,?2,?3,?4,?5,?6)",
                params![
                    p.git_checksum.as_ref(),
                    p.pack.digest.as_slice(),
                    p.index.digest.as_slice(),
                    p.pack.size,
                    p.index.size,
                    p.object_count
                ],
            )?;
            Ok::<_, MetadataError>(())
        })
    }
    pub(in crate::packs) fn contains(&self, ids: &[ObjectId]) -> Result<Vec<bool>, MetadataError> {
        let mut q = self
            .db
            .prepare_cached("SELECT done FROM nodes WHERE oid=?1")?;
        ids.iter()
            .map(|id| {
                Ok(q.query_row([id.as_ref()], |r| r.get::<_, bool>(0))
                    .optional()?
                    .unwrap_or(false))
            })
            .collect()
    }
    #[cfg(test)]
    fn counts(&self) -> Result<(u64, u64), MetadataError> {
        Ok((
            self.db
                .query_row("SELECT count(*) FROM nodes WHERE done=1", [], |r| r.get(0))?,
            self.db
                .query_row("SELECT count(*) FROM packs", [], |r| r.get(0))?,
        ))
    }
    #[cfg(test)]
    fn path(&self) -> &std::path::Path {
        self.file.file().path()
    }
}

#[cfg(test)]
mod tests;
