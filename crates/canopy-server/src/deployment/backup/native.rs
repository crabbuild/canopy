//! Pinned native roots, keyset-paged SQL inventory and admitted disk deduplication.
use super::*;
use crate::{
    ObjectFormat,
    packs::{
        backup::{ArtifactVisitor, Inventory},
        directory::index::WalkResult,
    },
};
use canopy_object_storage::artifact::{ArtifactDescriptor, ArtifactKey, ArtifactStore};
use cellule_ltx::{
    DiskReservation,
    rusqlite::{Connection, OpenFlags, OptionalExtension, params, params_from_iter, types::Value},
};
use cellule_runtime::NodeLeaseGuard;
use object_store::ObjectStore;
use std::sync::Mutex;

pub(super) struct Identity {
    pub repository: [u8; 16],
    format: ObjectFormat,
    seed: [u8; 32],
}
pub(super) async fn identity(database: PathBuf) -> BackupResult<Option<Identity>> {
    tokio::task::spawn_blocking(move || {
        let c = Connection::open_with_flags(database,OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX)?;
        let row = c.query_row("SELECT repository_id,object_format,push_cert_seed FROM repository_identity WHERE singleton=1",[],|row| Ok((row.get::<_,Vec<u8>>(0)?,row.get::<_,String>(1)?,row.get::<_,Vec<u8>>(2)?))).optional()?;
        row.map(|(repository,format,seed)| Ok(Identity {
            repository: repository.try_into().map_err(|_| BackupError::Invalid("invalid repository UUID"))?,
            format: match format.as_str() { "sha1" => ObjectFormat::Sha1, "sha256" => ObjectFormat::Sha256, _ => return Err(BackupError::Invalid("invalid repository format")) },
            seed: seed.try_into().map_err(|_| BackupError::Invalid("invalid repository seed"))?,
        })).transpose()
    }).await?
}
struct Purpose {
    table: &'static str,
    keys: &'static str,
    fields: &'static [&'static str],
    predicate: &'static str,
    cursor: Vec<Value>,
}
fn purposes() -> Vec<Purpose> {
    [
        (
            "catalog_generations",
            "generation",
            &["catalog", "refs"][..],
            "1",
            vec![Value::Integer(-1)],
        ),
        (
            "pushes",
            "id",
            &["response_root"][..],
            "response_root IS NOT NULL",
            vec![Value::Blob(vec![])],
        ),
        (
            "pull_merges",
            "id",
            &["publication"][..],
            "1",
            vec![Value::Blob(vec![])],
        ),
        (
            "merge_candidates",
            "id",
            &["native_publication"][..],
            "native_publication IS NOT NULL",
            vec![Value::Blob(vec![])],
        ),
        (
            "catalog_head_updates",
            "id",
            &["fact"][..],
            "1",
            vec![Value::Blob(vec![])],
        ),
        (
            "catalog_initialization",
            "singleton",
            &["result"][..],
            "1",
            vec![Value::Integer(0)],
        ),
        (
            "catalog_leases",
            "incarnation,admission_sequence",
            &[
                "input_checkpoint",
                "attestation",
                "recovery",
                "recovery_phase",
            ][..],
            "1",
            vec![Value::Blob(vec![]), Value::Integer(0)],
        ),
        (
            "catalog_recovery_receipts",
            "incarnation,admission_sequence",
            &["recovery", "recovery_phase", "recovery_release"][..],
            "1",
            vec![Value::Blob(vec![]), Value::Integer(0)],
        ),
    ]
    .into_iter()
    .map(|(table, keys, fields, predicate, cursor)| Purpose {
        table,
        keys,
        fields,
        predicate,
        cursor,
    })
    .collect()
}
struct Row {
    key: Vec<Value>,
    columns: Vec<Option<Vec<u8>>>,
}
async fn page(database: PathBuf, purpose: &Purpose) -> BackupResult<Vec<Row>> {
    // All identifiers and predicates above are compile-time schema declarations.
    let n = purpose.cursor.len();
    let parameters = (1..=n)
        .map(|i| format!("?{i}"))
        .collect::<Vec<_>>()
        .join(",");
    let sql = format!(
        "SELECT {},{} FROM {} WHERE {} AND ({}) > ({}) ORDER BY {} LIMIT 32",
        purpose.keys,
        purpose.fields.join(","),
        purpose.table,
        purpose.predicate,
        purpose.keys,
        parameters,
        purpose.keys
    );
    let cursor = purpose.cursor.clone();
    let fields = purpose.fields.len();
    tokio::task::spawn_blocking(move || {
        let c = Connection::open_with_flags(
            database,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )?;
        let mut stmt = c.prepare(&sql)?;
        let mut rows = stmt.query(params_from_iter(cursor))?;
        let mut page = Vec::new();
        while let Some(row) = rows.next()? {
            let key = (0..n)
                .map(|i| row.get(i))
                .collect::<std::result::Result<Vec<Value>, _>>()?;
            let columns = (n..n + fields)
                .map(|i| row.get(i))
                .collect::<std::result::Result<Vec<Option<Vec<u8>>>, _>>()?;
            page.push(Row { key, columns });
        }
        Ok(page)
    })
    .await?
}

#[expect(
    clippy::too_many_arguments,
    reason = "one pinned copy carries source and destination capabilities"
)]
pub(super) async fn verify(
    deployment: &Deployment,
    host: &Host,
    guard: &NodeLeaseGuard,
    database: &std::path::Path,
    directory: &std::path::Path,
    identity: Option<Identity>,
    source: Option<&Path>,
    source_store: Option<Arc<dyn ObjectStore>>,
    destination: Arc<dyn ObjectStore>,
) -> BackupResult<u64> {
    let mut purposes = purposes();
    let Some(identity) = identity else {
        for purpose in &purposes {
            if page(database.into(), purpose)
                .await?
                .iter()
                .any(|row| row.columns.iter().any(Option::is_some))
            {
                return Err(BackupError::Invalid(
                    "native roots have no repository identity",
                ));
            }
        }
        return Ok(0);
    };
    let target = crate::repository_target(
        deployment.identity.tenant(),
        deployment.identity.application(),
        identity.repository,
    )?;
    let destination = Arc::new(ArtifactStore::new(destination, identity.repository));
    let store = source_store
        .map(|store| Arc::new(ArtifactStore::new(store, identity.repository)))
        .unwrap_or_else(|| destination.clone());
    let disk = host.local_disk_budget().try_reserve(128 * 1024)?;
    let dedup_root = tempfile::Builder::new()
        .prefix("native-backup-")
        .tempdir_in(directory)?;
    let dedup = dedup_root.path().join("artifacts.sqlite");
    let c = tokio::task::spawn_blocking(move || -> BackupResult<Connection> {
        let c = Connection::open(dedup)?;
        c.execute_batch("PRAGMA journal_mode=OFF; PRAGMA synchronous=OFF; PRAGMA cache_size=-256; CREATE TABLE retained(path TEXT PRIMARY KEY,size INTEGER NOT NULL,digest BLOB NOT NULL,manifest BLOB NOT NULL) WITHOUT ROWID;")?;
        Ok(c)
    }).await??;
    let mut copier = Copier {
        deployment: deployment.clone(),
        source: source.cloned(),
        store: store.clone(),
        destination,
        guard: guard.clone(),
        dedup: Arc::new(Dedup {
            connection: Mutex::new(c),
            _root: dedup_root,
            disk,
        }),
        count: 0,
    };
    let mut inventory =
        Inventory::new(store, identity.format, target, &mut copier).map_err(BackupError::Native)?;
    for (kind, purpose) in purposes.iter_mut().enumerate() {
        loop {
            guard.check()?;
            let rows = page(database.into(), purpose).await?;
            if rows.is_empty() {
                break;
            }
            purpose.cursor = rows
                .last()
                .ok_or(BackupError::Invalid("empty native page"))?
                .key
                .clone();
            for row in rows {
                crate::packs::publication::backup::row(
                    kind as u8,
                    &row.columns,
                    &identity.seed,
                    &mut inventory,
                )
                .await
                .map_err(BackupError::Native)?;
            }
        }
    }
    Ok(copier.count)
}
struct Copier {
    deployment: Deployment,
    source: Option<Path>,
    store: Arc<ArtifactStore>,
    destination: Arc<ArtifactStore>,
    guard: NodeLeaseGuard,
    dedup: Arc<Dedup>,
    count: u64,
}
// Field drop order closes SQLite, removes its files, then returns admission.
// Blocking lookups retain this same owner even if their caller is canceled.
struct Dedup {
    connection: Mutex<Connection>,
    _root: tempfile::TempDir,
    disk: DiskReservation,
}
async fn checked(
    store: &ArtifactStore,
    key: ArtifactKey,
    value: ArtifactDescriptor,
) -> WalkResult<()> {
    let mut reader = store.read(key, value).await?;
    while reader.next().await?.is_some() {}
    Ok(())
}
#[async_trait::async_trait]
impl ArtifactVisitor for Copier {
    async fn artifact(&mut self, key: ArtifactKey, value: ArtifactDescriptor) -> WalkResult<bool> {
        self.guard.check()?;
        let path = self.store.path(key, value.digest)?;
        let dedup = self.dedup.clone();
        let name = path.to_string();
        let retained = tokio::task::spawn_blocking(move || -> WalkResult<bool> {
            let c = dedup
                .connection
                .lock()
                .map_err(|_| BackupError::Invalid("backup dedup poisoned"))?;
            let old = c
                .query_row(
                    "SELECT size,digest,manifest FROM retained WHERE path=?1",
                    params![name],
                    |row| {
                        Ok((
                            row.get::<_, u64>(0)?,
                            row.get::<_, Vec<u8>>(1)?,
                            row.get::<_, Vec<u8>>(2)?,
                        ))
                    },
                )
                .optional()?;
            if let Some((size, digest, manifest)) = old {
                if size != value.size || digest != value.digest || manifest != value.manifest_digest
                {
                    return Err(
                        BackupError::Invalid("conflicting native artifact descriptors").into(),
                    );
                }
                return Ok(true);
            }
            Ok(false)
        })
        .await??;
        if retained {
            return Ok(false);
        }
        if let Some(source) = &self.source {
            checked(&self.store, key, value).await?;
            let from = source.parts().chain(path.parts()).collect();
            let to = self.deployment.prefix.parts().chain(path.parts()).collect();
            crate::external::copy_parts(
                self.deployment.layout.store().inner().as_ref(),
                &from,
                &to,
                value.size,
            )
            .await?;
            match self
                .deployment
                .layout
                .store()
                .copy_if_not_exists(&from, &to)
                .await
            {
                Ok(()) | Err(StorageError::StateConflict { .. }) => {}
                Err(error) => return Err(error.into()),
            }
        }
        checked(&self.destination, key, value).await?;
        // Reserve before the insert. 4 KiB per physical artifact conservatively
        // covers its bounded key/descriptor and B-tree page overhead.
        self.dedup.disk.try_grow(4096)?;
        let dedup = self.dedup.clone();
        tokio::task::spawn_blocking(move || -> WalkResult<()> {
            dedup
                .connection
                .lock()
                .map_err(|_| BackupError::Invalid("backup dedup poisoned"))?
                .execute(
                    "INSERT INTO retained VALUES(?1,?2,?3,?4)",
                    params![
                        path.to_string(),
                        value.size,
                        value.digest.as_slice(),
                        value.manifest_digest.as_slice()
                    ],
                )?;
            Ok(())
        })
        .await??;
        self.count = self
            .count
            .checked_add(1)
            .ok_or(BackupError::Invalid("native artifact count overflow"))?;
        Ok(true)
    }
}
