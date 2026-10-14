use super::*;

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
}
