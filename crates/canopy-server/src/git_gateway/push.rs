mod native;
use super::*;

impl GitGateway {
    async fn verified_certificate(
        &self,
        cached: &CachedRepository,
        commands: &branch_policy::PushCommands,
        actor: &str,
        request_digest: [u8; 32],
    ) -> Result<Option<crate::push::VerifiedPushCertificate>, GatewayError> {
        let Some(body) = commands.certificate() else {
            return Ok(None);
        };
        let path = cached
            .backend
            .git_dir()
            .join("hooks/canopy-push-certificate");
        let receipt = tokio::fs::read_to_string(path)
            .await
            .map_err(|_| GatewayError::Certificate("Canopy signed push verification failed"))?;
        let mut lines = receipt.lines();
        let (Some(oid), Some(signer), Some(key), None) =
            (lines.next(), lines.next(), lines.next(), lines.next())
        else {
            return Err(GatewayError::Certificate(
                "Canopy signed push verification failed",
            ));
        };
        let oid = crate::ObjectId::from_hex(oid).map_err(|_| {
            GatewayError::Certificate("Canopy signed push certificate ID is invalid")
        })?;
        if oid.format() != self.repository.object_format()
            || oid != crate::object_id(oid.format(), ObjectKind::Blob, body)
            || signer != actor
        {
            return Err(GatewayError::Certificate(
                "Canopy signed push certificate does not match verified bytes",
            ));
        }
        let directory = self
            .signer_directory
            .as_ref()
            .ok_or(GatewayError::Certificate(
                "Canopy signed push signer is unavailable",
            ))?;
        let signers = directory
            .push_signers(actor)
            .await
            .map_err(|error| GatewayError::Cell(Box::new(error)))?;
        if !signers.iter().any(|signer| signer.fingerprint() == key) {
            return Err(GatewayError::Certificate(
                "Canopy signed push key is no longer authorized",
            ));
        }
        Ok(Some(crate::push::VerifiedPushCertificate {
            target: self.repository.target.clone(),
            request_digest,
            body: body.to_vec(),
            signer: signer.into(),
            key: key.into(),
        }))
    }
}
