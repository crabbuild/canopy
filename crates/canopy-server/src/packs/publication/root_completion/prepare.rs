use super::*;

impl PreparedCatalog {
    /// Reopen only registered native custody; certify its exact successful plan
    /// and precompute both terminal refusals before entering the final Cell
    /// command. This does not publish, select an outcome or acknowledge a push.
    pub async fn root_push_completion(
        &self,
        guard: &PreparedRefPolicyGuard,
        directory: &Path,
        budget: DiskBudget,
        limits: MetadataLimits,
        signers: Option<&DirectoryCell>,
    ) -> Result<RootPushCompletion, RootCompletionPreparationError> {
        let (_, deadline) = self.base.live_lease()?;
        // This factory composes native reopening, conditional ref rewriting
        // and artifact freezing. Keep its large inner future off every
        // caller's stack when several preparations are composed together.
        timeout_at(
            deadline,
            Box::pin(async {
                let (checkpoint, _, _, _) = self.base.session.push_checkpoint().await?;
                let mut checkpoint_bytes = BoundedEncoder::new(CERTIFICATE_BYTES)?;
                checkpoint.encode(&mut checkpoint_bytes)?;
                let digest = *blake3::hash(&checkpoint_bytes.finish()).as_bytes();
                if self
                    .input_checkpoint_digest
                    .is_some_and(|expected| expected != digest)
                    || self.input_checkpoint_digest.is_none()
                        && (self.input_count() != 0 || checkpoint.root()?.is_some())
                {
                    return Err(RootCompletionPreparationError::Context);
                }
                inputs::verify_digest(&self.base, digest).await?;
                let native = checkpoint
                    .native_result()?
                    .ok_or(RootCompletionPreparationError::Context)?;
                let store = self.base.indexes().store();
                let request = self
                    .base
                    .session
                    .reopen_native_result(&store, directory, &budget, signers)
                    .await?;
                let plan = request
                    .plan
                    .ok_or(RootCompletionPreparationError::Context)?;
                let mut proof = self
                    .guarded_ref_snapshot(guard, plan, directory, budget, limits)
                    .await?;
                let ref_generation = proof.snapshot.read(&store).await?.generation;
                let record = native.read(&store).await?;
                let outcomes = outcome::freeze(
                    &store,
                    self.token().artifact_operation,
                    native,
                    (record.operation, record.response),
                    request.response,
                    request.certificate,
                    ref_generation,
                )
                .await?;
                // Freeze may outlast an ACL/check/config change or checkpoint
                // update. Recheck both authorities after every artifact is durable.
                inputs::verify_digest(&self.base, digest).await?;
                ref_policy::ensure_ready(self, proof.guard).await?;
                let mut data = certificate::CertificateData::from_prepared(self);
                data.input_checkpoint_digest = Some(digest);
                data.refs_digest = Some(ref_policy::root_binding(proof.guard, proof.snapshot)?);
                data.completion_digest = Some(outcomes.binding()?);
                proof.certificate = attestation::issue_data_certificate(&self.base, data).await?;
                let value = RootPushCompletion { proof, outcomes };
                value.encode(&mut BoundedEncoder::new(ROOT_COMPLETION_BYTES)?)?;
                self.ensure_live()?;
                Ok(value)
            }),
        )
        .await
        .map_err(|_| PreparationBaseError::Inactive)?
    }
}
