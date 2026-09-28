//! Consistent published Cell snapshots with independently copied external bodies.

use super::*;
use crate::{
    REPOSITORIES, REPOSITORY_DATABASE_LIMIT_BYTES,
    large_blob::LargeBlobError,
    lfs::LfsError,
    server::{ServerError, unix_now_ms},
};
use cellule_ltx::{CellReplica, DiskBudget, Host};
use cellule_runtime::{
    control::Control, recovery::backup::BackupPin, recovery::backup::BackupPinStore,
    recovery::backup::PinnedCatalogShard,
};
use cellule_store::StorageError;
use std::path::PathBuf;

mod bodies;
mod enrollment;

const MAX_CELLS: usize = 100_000;

#[derive(Debug, thiserror::Error)]
pub enum BackupError {
    #[error("backup runtime operation failed")]
    Runtime(#[from] Error),
    #[error("backup worker failed")]
    Server(#[from] ServerError),
    #[error("backup replica operation failed")]
    Ltx(#[from] cellule_ltx::LtxError),
    #[error("backup storage operation failed")]
    Store(#[from] StorageError),
    #[error("backup external part copy failed")]
    External(#[from] object_store::Error),
    #[error("backup Git blob verification failed")]
    Blob(#[from] LargeBlobError),
    #[error("backup LFS verification failed")]
    Lfs(#[from] LfsError),
    #[error("backup SQLite read failed")]
    Sql(#[from] cellule_ltx::rusqlite::Error),
    #[error("backup local I/O failed")]
    Io(#[from] std::io::Error),
    #[error("backup task failed")]
    Task(#[from] tokio::task::JoinError),
    #[error("backup rejected: {0}")]
    Invalid(&'static str),
}

type BackupResult<T> = std::result::Result<T, BackupError>;

/// Receipt for a fully verified backup copy or restored destination.
#[derive(Serialize)]
pub struct BackupReport {
    pub pin: String,
    pub root: String,
    pub cells: u64,
    pub external_objects: u64,
}

impl Deployment {
    /// Copies a stable published snapshot and its external bytes to an isolated prefix.
    /// The destination uses the same provider; changing state during capture is an error.
    pub async fn create_backup(
        &self,
        id: RequestId,
        destination: Path,
        config: WorkerConfig,
    ) -> BackupResult<BackupReport> {
        disjoint(&self.prefix, &destination)?;
        let deployment = self.clone();
        tokio::spawn(async move {
            let worker = deployment.clone();
            worker
                .enrolled_backup(None, config, |host, scratch, guard| async move {
                    let pins = deployment.pins(host.clone())?;
                    let pin = match pins.load(id).await? {
                        Some(pin) => pin,
                        None => deployment.capture(&pins, id).await?,
                    };
                    if pin.control_count() > MAX_CELLS as u64 {
                        return Err(BackupError::Invalid("backup exceeds Cell limit"));
                    }
                    guard.check()?;
                    let purpose = root::RootPurpose::Backup {
                        source: deployment.prefix.to_string(),
                        pin: pin_id(id),
                        complete: false,
                    };
                    let claim =
                        root::reserve(deployment.layout.store(), &destination, purpose).await?;
                    if !claim.complete() {
                        pins.restore(&pin, destination.clone()).await?;
                    }
                    let copied = deployment.at(destination.clone())?;
                    let external_objects = copied
                        .verify_bodies(&pin, &host, &scratch, Some(&deployment.prefix), &guard)
                        .await?;
                    guard.check()?;
                    pins.verify(&pin).await?;
                    claim
                        .finish(deployment.layout.store(), &destination)
                        .await?;
                    Ok(report(&pin, &destination, external_objects))
                })
                .await
        })
        .await?
    }

    /// Verifies a completed backup without reading its original deployment prefix.
    pub async fn verify_backup(
        &self,
        id: RequestId,
        backup: Path,
        config: WorkerConfig,
    ) -> BackupResult<BackupReport> {
        let deployment = self.at(backup)?;
        tokio::spawn(async move {
            let worker = deployment.clone();
            worker
                .enrolled_backup(Some(id), config, |host, scratch, guard| async move {
                    let pin = deployment.required_pin(&host, id).await?;
                    let count = deployment
                        .verify_bodies(&pin, &host, &scratch, None, &guard)
                        .await?;
                    guard.check()?;
                    Ok(report(&pin, &deployment.prefix, count))
                })
                .await
        })
        .await?
    }

    /// Installs a verified backup at a reserved destination before allowing serving.
    pub async fn restore_backup(
        &self,
        id: RequestId,
        backup: Path,
        destination: Path,
        config: WorkerConfig,
    ) -> BackupResult<BackupReport> {
        disjoint(&backup, &destination)?;
        disjoint(&self.prefix, &destination)?;
        let deployment = self.at(backup)?;
        tokio::spawn(async move {
            let worker = deployment.clone();
            worker
                .enrolled_backup(Some(id), config, |host, scratch, guard| async move {
                    let pin = deployment.required_pin(&host, id).await?;
                    let purpose = root::RootPurpose::Restore {
                        source: deployment.prefix.to_string(),
                        pin: pin_id(id),
                        complete: false,
                    };
                    let claim =
                        root::reserve(deployment.layout.store(), &destination, purpose).await?;
                    if !claim.complete() {
                        deployment
                            .pins(host.clone())?
                            .restore(&pin, destination.clone())
                            .await?;
                    }
                    let restored = deployment.at(destination.clone())?;
                    let count = restored
                        .verify_bodies(&pin, &host, &scratch, Some(&deployment.prefix), &guard)
                        .await?;
                    guard.check()?;
                    claim
                        .finish(deployment.layout.store(), &destination)
                        .await?;
                    Ok(report(&pin, &destination, count))
                })
                .await
        })
        .await?
    }

    fn at(&self, prefix: Path) -> Result<Self> {
        Self::new(
            self.layout.store().clone(),
            prefix,
            self.identity,
            self.nodes.fleet(),
            self.image_digest,
            self.registry.clone(),
        )
    }

    fn pins(&self, host: Host) -> Result<BackupPinStore> {
        BackupPinStore::new(
            self.layout.clone(),
            self.identity,
            crate::replica_limits(REPOSITORY_DATABASE_LIMIT_BYTES, 64 * 1024 * 1024),
            host,
        )
    }

    async fn required_pin(&self, host: &Host, id: RequestId) -> BackupResult<BackupPin> {
        let pin = self
            .pins(host.clone())?
            .load(id)
            .await?
            .ok_or(BackupError::Invalid("backup pin is absent"))?;
        if pin.control_count() > MAX_CELLS as u64 {
            return Err(BackupError::Invalid("backup exceeds Cell limit"));
        }
        self.pins(host.clone())?.verify(&pin).await?;
        Ok(pin)
    }

    async fn capture(&self, pins: &BackupPinStore, id: RequestId) -> BackupResult<BackupPin> {
        self.require_ready().await?;
        let release = self.record().await?;
        let catalog = CellCatalog::new(self.layout.clone(), self.identity.tenant());
        let authority = CellAuthority::new(self.layout.clone());
        let mut shards = Vec::with_capacity(256);
        let mut controls = Vec::<Control>::new();
        for shard in 0..=u8::MAX {
            let mut scan = catalog.scan_shard(shard).await?;
            shards.push(PinnedCatalogShard {
                shard,
                revision: scan.revision(),
                pages: scan.page_digests().to_vec(),
            });
            while let Some(page) = scan.next_page().await? {
                for proof in page.entries() {
                    if controls.len() == MAX_CELLS {
                        return Err(BackupError::Invalid("backup exceeds Cell limit"));
                    }
                    let control = authority
                        .load(proof.entry().cell())
                        .await?
                        .ok_or(BackupError::Invalid("catalog Cell is not published"))?;
                    if control.value().recovery.is_some()
                        || (control.value().state != ControlState::Tombstoned
                            && control.value().root.is_none())
                    {
                        return Err(BackupError::Invalid("Cell recovery is incomplete"));
                    }
                    controls.push(control.value().clone());
                }
            }
        }
        // Every object must remain unchanged across both complete reads. Their
        // stability intervals overlap between the passes, yielding one global cut.
        for shard in &shards {
            let scan = catalog.scan_shard(shard.shard).await?;
            if scan.revision() != shard.revision || scan.page_digests() != shard.pages {
                return Err(BackupError::Invalid(
                    "catalog changed during backup capture; retry",
                ));
            }
        }
        for control in &controls {
            if authority
                .load(control.cell)
                .await?
                .as_ref()
                .map(|c| c.value())
                != Some(control)
            {
                return Err(BackupError::Invalid(
                    "Cell changed during backup capture; retry",
                ));
            }
        }
        if self.record().await? != release {
            return Err(BackupError::Invalid(
                "release changed during backup capture",
            ));
        }
        Ok(pins.create(id, unix_now_ms()?, shards, controls).await?)
    }
}

fn pin_id(id: RequestId) -> String {
    uuid::Uuid::from_bytes(*id.as_bytes()).to_string()
}
fn report(pin: &BackupPin, root: &Path, external_objects: u64) -> BackupReport {
    BackupReport {
        pin: pin_id(pin.id()),
        root: root.to_string(),
        cells: pin.control_count(),
        external_objects,
    }
}
fn disjoint(left: &Path, right: &Path) -> BackupResult<()> {
    if left.as_ref().is_empty()
        || right.as_ref().is_empty()
        || left == right
        || left.as_ref().starts_with(&format!("{right}/"))
        || right.as_ref().starts_with(&format!("{left}/"))
    {
        return Err(BackupError::Invalid(
            "prefixes must be nonempty and disjoint",
        ));
    }
    Ok(())
}
