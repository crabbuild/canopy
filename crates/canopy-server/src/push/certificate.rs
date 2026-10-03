use super::*;
use sha2::{Digest as _, Sha256};

/// Metadata for a verified push certificate retained with its push.
#[derive(Debug, serde::Serialize)]
pub struct PushCertificateReceipt {
    pub sha256: String,
    pub signer: String,
    pub key: String,
    pub recorded_at_ms: i64,
}

/// Native-verified signed push. Its construction is confined to the gateway.
pub struct VerifiedPushCertificate {
    pub(crate) target: cellule_runtime::CellTarget,
    pub(crate) request_digest: [u8; 32],
    pub(crate) body: Vec<u8>,
    pub(crate) signer: String,
    pub(crate) key: String,
}

pub(super) struct CertificateMeta {
    pub digest: [u8; 32],
    pub size: i64,
    pub signer: String,
    pub key: String,
}

impl RepositoryCell {
    pub(crate) async fn push_certificate_seed(&self) -> Result<[u8; 32], PushError> {
        let result = self
            .sql
            .query(
                None,
                SqlBatch {
                    statements: vec![SqlStatement {
                        sql: "SELECT push_cert_seed FROM repository_identity WHERE singleton = 1"
                            .into(),
                        parameters: vec![],
                    }],
                },
            )
            .await
            .map_err(cell)?;
        let Some([SqlValue::Blob(seed)]) = result
            .output
            .first()
            .and_then(|set| set.rows.first())
            .map(Vec::as_slice)
        else {
            return Err(PushError::InvalidResponse);
        };
        seed.as_slice()
            .try_into()
            .map_err(|_| PushError::InvalidResponse)
    }

    pub(super) async fn stage_push_certificate(
        &self,
        push_id: [u8; 16],
        certificate: &VerifiedPushCertificate,
    ) -> Result<CertificateMeta, InvocationError<bool>> {
        let size = i64::try_from(certificate.body.len())
            .ok()
            .filter(|size| *size > 0)
            .ok_or(InvocationError::NotStarted(Error::Command(
                "invalid push certificate",
            )))?;
        if certificate.signer.is_empty() || certificate.key.is_empty() {
            return Err(InvocationError::NotStarted(Error::Command(
                "invalid push certificate identity",
            )));
        }
        for (part, body) in certificate.body.chunks(CHUNK_BYTES).enumerate() {
            self.sql.batch(identity().map_err(|_| InvocationError::NotStarted(Error::Command("push mutation clock failed")))?, SqlBatch { statements: vec![SqlStatement {
                sql: "INSERT INTO push_certificate_chunks (push_id, part, body) VALUES (?1, ?2, ?3) ON CONFLICT(push_id, part) DO UPDATE SET body = excluded.body".into(),
                parameters: vec![SqlValue::Blob(push_id.to_vec()), SqlValue::Integer(part as i64), SqlValue::Blob(body.to_vec())],
            }]}).await.map_err(|source| InvocationError::NotStarted(Error::Facility {
                name: "push certificate staging",
                source: Box::new(source),
            }))?;
        }
        Ok(CertificateMeta {
            digest: Sha256::digest(&certificate.body).into(),
            size,
            signer: certificate.signer.clone(),
            key: certificate.key.clone(),
        })
    }
}

pub(super) fn certificate_complete(
    context: &CommandContext<'_, '_>,
    push_id: [u8; 16],
    certificate: &CertificateMeta,
) -> cellule_runtime::Result<bool> {
    if certificate.size <= 0 || certificate.signer.is_empty() || certificate.key.is_empty() {
        return Ok(false);
    }
    let parts = (certificate.size - 1) / CHUNK_BYTES as i64 + 1;
    let totals = context.sql(&SqlBatch { statements: vec![SqlStatement {
        sql: "SELECT count(*), coalesce(sum(length(body)), 0) FROM push_certificate_chunks WHERE push_id = ?1".into(),
        parameters: vec![SqlValue::Blob(push_id.to_vec())],
    }]})?;
    if !matches!(
        totals.first().and_then(|set| set.rows.first()).map(Vec::as_slice),
        Some([SqlValue::Integer(count), SqlValue::Integer(size)]) if *count == parts && *size == certificate.size
    ) {
        return Ok(false);
    }
    let mut digest = Sha256::new();
    let mut size = 0_i64;
    for part in 0..parts {
        let result = context.sql(&SqlBatch {
            statements: vec![SqlStatement {
                sql: "SELECT body FROM push_certificate_chunks WHERE push_id = ?1 AND part = ?2"
                    .into(),
                parameters: vec![SqlValue::Blob(push_id.to_vec()), SqlValue::Integer(part)],
            }],
        })?;
        let Some([SqlValue::Blob(body)]) = result
            .first()
            .and_then(|set| set.rows.first())
            .map(Vec::as_slice)
        else {
            return Ok(false);
        };
        size = size
            .checked_add(body.len() as i64)
            .ok_or(Error::Command("push certificate size overflow"))?;
        if size > certificate.size {
            return Ok(false);
        }
        digest.update(body);
    }
    Ok(size == certificate.size && digest.finalize().as_slice() == certificate.digest)
}
