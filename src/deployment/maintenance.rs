//! Persistent exclusion for offline administration, including interrupted sweeps.

use super::*;
use bytes::Bytes;
use cellule_store::{ETag, StorageError};
use serde::Deserialize;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum Action {
    Collect,
    Recover,
    End,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Worker {
    node: String,
    key: String,
    workspace: String,
}

impl Worker {
    pub(super) fn new(config: &WorkerConfig, workspace: &std::path::Path) -> Result<Self> {
        Ok(Self {
            node: hex::encode(config.node.as_bytes()),
            key: hex::encode(config.signing_key.verifying_key().as_bytes()),
            workspace: workspace
                .to_str()
                .ok_or(Error::Retention("maintenance workspace is not UTF-8"))?
                .into(),
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
enum State {
    Idle,
    Active {
        operation: String,
        action: Action,
        worker: Option<Worker>,
        run: String,
    },
}

pub(super) struct Claim {
    path: Path,
    state: State,
    token: ETag,
}

fn path(deployment: &Deployment) -> Path {
    deployment.prefix.clone().join("canopy-maintenance-v1.json")
}

async fn load(deployment: &Deployment) -> Result<Option<(State, ETag)>> {
    match deployment
        .layout
        .store()
        .get_with_etag_bounded(&path(deployment), 4096)
        .await
    {
        Ok((body, token)) => Ok(Some((serde_json::from_slice(&body)?, token))),
        Err(StorageError::NotFound { .. }) => Ok(None),
        Err(error) => Err(error.into()),
    }
}

pub(super) async fn require_idle(deployment: &Deployment) -> Result<()> {
    if load(deployment)
        .await?
        .is_some_and(|(state, _)| state != State::Idle)
    {
        return Err(Error::Retention(
            "an offline maintenance worker has not settled",
        ));
    }
    Ok(())
}

/// Worker adoption is permitted only while holding the original local workspace
/// lock. There is deliberately no timeout takeover of a collector: a paused
/// delete must never overlap reopened admission, even after a node lease expires.
pub(super) async fn acquire(
    deployment: &Deployment,
    operation: RequestId,
    action: Action,
    worker: Option<Worker>,
) -> Result<Claim> {
    let operation = hex::encode(operation.as_bytes());
    let previous = load(deployment).await?;
    if let Some((
        State::Active {
            operation: old_operation,
            action: old_action,
            worker: old_worker,
            ..
        },
        _,
    )) = &previous
        && (*old_operation != operation || *old_action != action || *old_worker != worker)
    {
        return Err(Error::Retention(
            "another offline maintenance worker owns the deployment",
        ));
    }
    // End is an idempotent release CAS. Collect/recover callers prove the
    // old process closed by acquiring its same canonical workspace first.
    let state = State::Active {
        operation,
        action,
        worker,
        run: uuid::Uuid::new_v4().to_string(),
    };
    let bytes = serde_json::to_vec(&state)?;
    if bytes.len() > 4096 {
        return Err(Error::Retention("maintenance claim exceeds size limit"));
    }
    let path = path(deployment);
    let token = match previous {
        Some((_, token)) => {
            deployment
                .layout
                .store()
                .update(&path, Bytes::from(bytes), token)
                .await?
        }
        None => {
            deployment
                .layout
                .store()
                .create_strict_with_etag(&path, Bytes::from(bytes))
                .await?
        }
    };
    Ok(Claim { path, state, token })
}

impl Claim {
    pub(super) async fn require_current(&self, deployment: &Deployment) -> Result<()> {
        if !load(deployment)
            .await?
            .is_some_and(|(state, token)| state == self.state && token == self.token)
        {
            return Err(Error::Retention("maintenance worker claim changed"));
        }
        Ok(())
    }

    pub(super) async fn finish(self, deployment: &Deployment) -> Result<()> {
        deployment
            .layout
            .store()
            .update(
                &self.path,
                Bytes::from(serde_json::to_vec(&State::Idle)?),
                self.token,
            )
            .await?;
        Ok(())
    }
}
