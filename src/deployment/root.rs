//! Atomic ownership of a deployment, backup, or restore destination prefix.

use super::*;
use bytes::Bytes;
use cellule_store::{ETag, StorageError};
use serde::Deserialize;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(super) enum RootPurpose {
    Service,
    Backup {
        source: String,
        pin: String,
        complete: bool,
    },
    Restore {
        source: String,
        pin: String,
        complete: bool,
    },
}

impl RootPurpose {
    fn same_operation(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Service, Self::Service) => true,
            (
                Self::Backup {
                    source: left,
                    pin: a,
                    ..
                },
                Self::Backup {
                    source: right,
                    pin: b,
                    ..
                },
            )
            | (
                Self::Restore {
                    source: left,
                    pin: a,
                    ..
                },
                Self::Restore {
                    source: right,
                    pin: b,
                    ..
                },
            ) => left == right && a == b,
            _ => false,
        }
    }

    pub(super) fn complete(&self) -> bool {
        matches!(
            self,
            Self::Backup { complete: true, .. } | Self::Restore { complete: true, .. }
        )
    }

    fn permits_service(&self) -> bool {
        matches!(self, Self::Service | Self::Restore { complete: true, .. })
    }
}

pub(super) struct RootClaim {
    purpose: RootPurpose,
    token: ETag,
}

fn path(root: &Path) -> Path {
    root.clone().join("canopy-root-v1.json")
}

pub(super) async fn load(store: &Store, root: &Path) -> Result<Option<RootClaim>> {
    match store.get_with_etag_bounded(&path(root), 4096).await {
        Ok((bytes, token)) => Ok(Some(RootClaim {
            purpose: serde_json::from_slice(&bytes)?,
            token,
        })),
        Err(StorageError::NotFound { .. }) => Ok(None),
        Err(error) => Err(error.into()),
    }
}

pub(super) async fn reserve(store: &Store, root: &Path, purpose: RootPurpose) -> Result<RootClaim> {
    if load(store, root).await?.is_none()
        && !matches!(purpose, RootPurpose::Service)
        && ApplicationIdentityStore::new(store.clone(), root.clone())
            .load()
            .await?
            .is_some()
    {
        return Err(Error::Backup(
            "destination already contains an application identity",
        ));
    }
    let body = Bytes::from(serde_json::to_vec(&purpose)?);
    if body.len() > 4096 {
        return Err(Error::Backup("root reservation exceeds size limit"));
    }
    match store.create_strict_with_etag(&path(root), body).await {
        Ok(token) => Ok(RootClaim { purpose, token }),
        Err(error) => match load(store, root).await? {
            Some(existing) if purpose.same_operation(&existing.purpose) => Ok(existing),
            Some(_) => Err(Error::Backup(
                "destination prefix is reserved for another purpose",
            )),
            None => Err(error.into()),
        },
    }
}

impl RootClaim {
    pub(super) fn complete(&self) -> bool {
        self.purpose.complete()
    }

    pub(super) async fn finish(self, store: &Store, root: &Path) -> Result<()> {
        if self.complete() {
            return Ok(());
        }
        let mut next = self.purpose;
        match &mut next {
            RootPurpose::Backup { complete, .. } | RootPurpose::Restore { complete, .. } => {
                *complete = true
            }
            RootPurpose::Service => return Err(Error::Backup("service root cannot finish a copy")),
        }
        match store
            .update(
                &path(root),
                Bytes::from(serde_json::to_vec(&next)?),
                self.token,
            )
            .await
        {
            Ok(_) => Ok(()),
            Err(error) => match load(store, root).await? {
                Some(current) if current.purpose == next => Ok(()),
                _ => Err(error.into()),
            },
        }
    }
}

impl Deployment {
    pub(super) async fn claim_service_root(&self) -> Result<()> {
        let claim = match load(self.layout.store(), &self.prefix).await? {
            Some(claim) => claim,
            None => reserve(self.layout.store(), &self.prefix, RootPurpose::Service).await?,
        };
        if !claim.purpose.permits_service() {
            return Err(Error::Backup(
                "backup or unfinished restore prefix cannot serve",
            ));
        }
        Ok(())
    }

    pub(super) async fn require_service_root(&self) -> Result<()> {
        if !load(self.layout.store(), &self.prefix)
            .await?
            .is_some_and(|claim| claim.purpose.permits_service())
        {
            return Err(Error::Backup(
                "deployment prefix is not available for serving",
            ));
        }
        Ok(())
    }
}

pub(super) async fn require_backup(store: &Store, root: &Path, id: RequestId) -> Result<()> {
    let expected = uuid::Uuid::from_bytes(*id.as_bytes()).to_string();
    if !load(store, root).await?.is_some_and(|claim| {
        matches!(claim.purpose,
        RootPurpose::Backup { pin, complete: true, .. } if pin == expected)
    }) {
        return Err(Error::Backup(
            "completed backup reservation is absent or differs",
        ));
    }
    Ok(())
}
