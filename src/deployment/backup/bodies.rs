use super::*;
use crate::{
    large_blob::{LargeBlobReference, LargeBlobStore, blob_path},
    lfs::{LfsObject, lfs_path, verify_lfs_object},
};
use crab_cell_runtime::{NodeLeaseGuard, cell::catalog::CatalogRole};
use crab_ltx::rusqlite::{Connection, OpenFlags, OptionalExtension, params};
use object_store::{ObjectStore, prefix::PrefixStore};

#[derive(Clone, Copy)]
enum BodyKind {
    Git,
    Lfs,
}
enum Reference {
    Git(LargeBlobReference),
    Lfs(LfsObject),
}

impl Deployment {
    pub(super) async fn verify_bodies(
        &self,
        pin: &BackupPin,
        host: &Host,
        scratch: &std::path::Path,
        source: Option<&Path>,
        guard: &NodeLeaseGuard,
    ) -> BackupResult<u64> {
        if pin.control_count() > MAX_CELLS as u64 {
            return Err(BackupError::Invalid("backup exceeds Cell limit"));
        }
        let controls = self.pins(host.clone())?.verify(pin).await?;
        let catalog = CellCatalog::new(self.layout.clone(), self.identity.tenant());
        let destination_store: Arc<dyn ObjectStore> = Arc::new(PrefixStore::new(
            self.layout.store().inner().clone(),
            self.prefix.clone(),
        ));
        let source_store: Option<Arc<dyn ObjectStore>> = source.map(|root| {
            Arc::new(PrefixStore::new(
                self.layout.store().inner().clone(),
                root.clone(),
            )) as Arc<dyn ObjectStore>
        });
        let mut count = 0_u64;
        for control in controls {
            guard.check()?;
            let proof = catalog
                .lookup(control.cell)
                .await?
                .ok_or(BackupError::Invalid("backup Cell catalog entry is absent"))?;
            let entry = proof.entry();
            if entry.namespace() != REPOSITORIES {
                continue;
            }
            if entry.role() != CatalogRole::Sql
                || !self.registry.is_current_cell(
                    entry.namespace(),
                    entry.role(),
                    control.code,
                    control.schema,
                )
            {
                return Err(BackupError::Invalid("backup repository schema differs"));
            }
            let Some(root) = control.ltx_root() else {
                continue;
            };
            let replica = CellReplica::new(
                self.layout.clone(),
                *control.cell.as_bytes(),
                *control.incarnation.as_bytes(),
                crate::replica_limits(REPOSITORY_DATABASE_LIMIT_BYTES, 64 * 1024 * 1024),
            )?
            .with_host(host.clone());
            let directory = scratch.join(hex::encode(control.cell.as_bytes()));
            tokio::fs::create_dir_all(&directory).await?;
            let database = directory.join("snapshot.sqlite");
            let verified = replica.open_root(&root).await?;
            let database_bytes =
                u64::from(verified.page_size()) * u64::from(verified.database_pages());
            let _disk = host.local_disk_budget().try_reserve(database_bytes)?;
            verified.restore(&database).await?;
            let repository_id = read_identity(database.clone()).await?;
            if let Some(id) = repository_id {
                let target = crate::repository_target(
                    self.identity.tenant(),
                    self.identity.application(),
                    id,
                )?;
                if target.cell_id() != control.cell || target.partition() != entry.partition() {
                    return Err(BackupError::Invalid(
                        "repository UUID differs from backup Cell",
                    ));
                }
            }
            for kind in [BodyKind::Git, BodyKind::Lfs] {
                let mut cursor = Vec::new();
                loop {
                    let page = read_page(database.clone(), kind, cursor).await?;
                    if page.is_empty() {
                        break;
                    }
                    cursor = match page.last().ok_or(BackupError::Invalid("empty page"))? {
                        Reference::Git(value) => value.oid.to_vec(),
                        Reference::Lfs(value) => value.sha256.to_vec(),
                    };
                    for reference in page {
                        // A crash may leave a provisioned Cell before owner initialization.
                        // That Cell can be empty, but external bytes require a durable UUID.
                        let repository_id = repository_id.ok_or(BackupError::Invalid(
                            "external body has no repository identity",
                        ))?;
                        guard.check()?;
                        let path = match &reference {
                            Reference::Git(value) => blob_path(repository_id, &value.sha256),
                            Reference::Lfs(value) => lfs_path(repository_id, &value.sha256),
                        };
                        if let (Some(source), Some(source_store)) = (source, &source_store) {
                            verify(source_store.clone(), repository_id, &reference).await?;
                            let size = match &reference {
                                Reference::Git(value) => value.size,
                                Reference::Lfs(value) => value.size,
                            };
                            crate::external::copy_parts(
                                self.layout.store().inner().as_ref(),
                                &source.parts().chain(path.parts()).collect(),
                                &self.prefix.parts().chain(path.parts()).collect(),
                                size,
                            )
                            .await?;
                            match self
                                .layout
                                .store()
                                .copy_if_not_exists(
                                    &source.parts().chain(path.parts()).collect(),
                                    &self.prefix.parts().chain(path.parts()).collect(),
                                )
                                .await
                            {
                                Ok(()) | Err(StorageError::StateConflict { .. }) => {}
                                Err(error) => return Err(error.into()),
                            }
                        }
                        verify(destination_store.clone(), repository_id, &reference).await?;
                        count = count
                            .checked_add(1)
                            .ok_or(BackupError::Invalid("external object count overflow"))?;
                    }
                }
            }
            tokio::fs::remove_dir_all(directory).await?;
        }
        Ok(count)
    }
}

async fn read_identity(database: PathBuf) -> BackupResult<Option<[u8; 16]>> {
    tokio::task::spawn_blocking(move || {
        let connection = Connection::open_with_flags(
            database,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )?;
        let id: Option<Vec<u8>> = connection
            .query_row(
                "SELECT repository_id FROM repository_identity WHERE singleton = 1",
                [],
                |row| row.get(0),
            )
            .optional()?;
        id.map(|id| {
            id.try_into()
                .map_err(|_| BackupError::Invalid("invalid repository UUID"))
        })
        .transpose()
    })
    .await?
}

async fn verify(
    store: Arc<dyn ObjectStore>,
    repository_id: [u8; 16],
    reference: &Reference,
) -> BackupResult<()> {
    match reference {
        Reference::Git(value) => {
            LargeBlobStore::new(store, repository_id)
                .verify(value)
                .await?;
        }
        Reference::Lfs(value) => {
            verify_lfs_object(store, repository_id, *value, None).await?;
        }
    }
    Ok(())
}

async fn read_page(
    database: PathBuf,
    kind: BodyKind,
    cursor: Vec<u8>,
) -> BackupResult<Vec<Reference>> {
    tokio::task::spawn_blocking(move || {
        let connection = Connection::open_with_flags(database, OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX)?;
        let sql = match kind {
            BodyKind::Git => "SELECT oid, size, digest, external_sha256 FROM objects WHERE storage = 'external' AND oid > ?1 ORDER BY oid LIMIT 256",
            BodyKind::Lfs => "SELECT sha256, size, digest, sha256 FROM lfs_objects WHERE sha256 > ?1 ORDER BY sha256 LIMIT 256",
        };
        let mut statement = connection.prepare(sql)?;
        let mut rows = statement.query(params![cursor])?;
        let mut page = Vec::new();
        while let Some(row) = rows.next()? {
            let oid: Vec<u8> = row.get(0)?;
            let size: i64 = row.get(1)?;
            let digest: Vec<u8> = row.get(2)?;
            let sha256: Vec<u8> = row.get(3)?;
            let size = u64::try_from(size).map_err(|_| BackupError::Invalid("invalid body size"))?;
            let blake3 = digest.try_into().map_err(|_| BackupError::Invalid("invalid body digest"))?;
            let sha256 = sha256.try_into().map_err(|_| BackupError::Invalid("invalid body SHA-256"))?;
            page.push(match kind {
                BodyKind::Git => Reference::Git(LargeBlobReference { oid: oid.try_into().map_err(|_| BackupError::Invalid("invalid Git OID"))?, size, blake3, sha256 }),
                BodyKind::Lfs => Reference::Lfs(LfsObject { sha256, size, blake3 }),
            });
        }
        Ok(page)
    }).await?
}
