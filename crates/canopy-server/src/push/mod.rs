//! Durable request identity and replayable Git responses.

use crate::RepositoryCell;
use crate::access::READ_ACCESS;
use cellule_runtime::primitives::sql::{SqlBatch, SqlStatement, SqlValue};
use std::error::Error as StdError;

/// One completed push's authenticated, durable audit annotation.
#[derive(Debug, serde::Serialize)]
pub struct PushReceipt {
    pub id: String,
    pub actor: String,
    pub options: Vec<String>,
    pub certificate: Option<PushCertificateReceipt>,
}

pub(crate) fn valid_options(options: &[String]) -> bool {
    options.len() <= 16
        && options.iter().all(|option| {
            option.strip_prefix("canopy.note=").is_some_and(|note| {
                !note.is_empty()
                    && option.len() <= 1024
                    && option.bytes().all(|byte| (0x20..=0x7e).contains(&byte))
            })
        })
}

mod certificate;
pub use certificate::{PushCertificateReceipt, VerifiedPushCertificate};
pub(crate) mod report;

pub(crate) const CHUNK_BYTES: usize = 512 * 1024;
pub(crate) const MAX_RESPONSE_BYTES: usize = 64 * 1024 * 1024;

#[derive(Debug, thiserror::Error)]
pub enum PushError {
    #[error("push Cell operation failed")]
    Cell(#[source] Box<dyn StdError + Send + Sync>),
    #[error("push ID is already bound to another request or account")]
    Conflict,
    #[error("stored push response is incomplete or corrupt")]
    InvalidResponse,
    #[error("push ref plan is outside supported bounds")]
    InvalidPlan,
    #[error("push response headers cannot be encoded")]
    Headers(#[from] serde_json::Error),
    #[error("system clock cannot create a push mutation identity")]
    Clock,
}

fn cell(error: impl StdError + Send + Sync + 'static) -> PushError {
    PushError::Cell(Box::new(error))
}

impl RepositoryCell {
    /// Reads completed push options for its author or current repository owner.
    pub async fn push_receipt(
        &self,
        actor: &str,
        id: [u8; 16],
    ) -> Result<Option<PushReceipt>, PushError> {
        crate::directory::validate_component(actor).map_err(cell)?;
        let result = self.sql.query(None, SqlBatch { statements: vec![SqlStatement {
            sql: format!("SELECT p.actor, p.options, c.digest, c.signer, c.key, c.recorded_at_ms, p.response_root FROM pushes p LEFT JOIN push_certificates c ON c.push_id = p.id WHERE p.id = ?2 AND p.response_id IS NOT NULL AND (p.actor = ?1 OR EXISTS (SELECT 1 FROM repository_identity WHERE owner = ?1)) AND ({READ_ACCESS})"),
            parameters: vec![SqlValue::Text(actor.into()), SqlValue::Blob(id.to_vec())],
        }]}).await.map_err(cell)?;
        let set = result.output.first().ok_or(PushError::InvalidResponse)?;
        let Some(row) = set.rows.first() else {
            return Ok(None);
        };
        let [
            SqlValue::Text(author),
            SqlValue::Text(options),
            digest,
            signer,
            key,
            recorded_at_ms,
            root,
        ] = row.as_slice()
        else {
            return Err(PushError::InvalidResponse);
        };
        let options: Vec<String> = match root {
            SqlValue::Blob(bytes) => self
                .staging_coordinator()
                .map_err(cell)?
                .completed_options(bytes)
                .await
                .map_err(cell)?,
            SqlValue::Null => {
                serde_json::from_str(options).map_err(|_| PushError::InvalidResponse)?
            }
            _ => return Err(PushError::InvalidResponse),
        };
        if !valid_options(&options) {
            return Err(PushError::InvalidResponse);
        }
        let certificate = match (digest, signer, key, recorded_at_ms) {
            (SqlValue::Null, SqlValue::Null, SqlValue::Null, SqlValue::Null) => None,
            (
                SqlValue::Blob(digest),
                SqlValue::Text(signer),
                SqlValue::Text(key),
                SqlValue::Integer(recorded_at_ms),
            ) if digest.len() == 32 && *recorded_at_ms >= 0 => Some(PushCertificateReceipt {
                sha256: hex::encode(digest),
                signer: signer.clone(),
                key: key.clone(),
                recorded_at_ms: *recorded_at_ms,
            }),
            _ => return Err(PushError::InvalidResponse),
        };
        Ok(Some(PushReceipt {
            id: uuid::Uuid::from_bytes(id).to_string(),
            actor: author.clone(),
            options,
            certificate,
        }))
    }
}
