//! Private bridge from live SQL checkpoint custody to an exact native witness.
use super::*;
use crate::packs::closure::{ClosureContext, ClosureError};

pub(in crate::packs) struct RetainedNativeInput {
    native: NativePackDescriptor,
    context: ClosureContext,
    session: PreparationSession,
    digest: [u8; 32],
}
impl RetainedNativeInput {
    pub(in crate::packs::publication) async fn open(
        base: &PreparationBaseResolver,
        native: NativePackDescriptor,
    ) -> Result<Self, InputCheckpointError> {
        let (lease, _) = base.live_lease()?;
        if native.repository != lease.token.repository || native.format != lease.format {
            return Err(PreparationBaseError::Context.into());
        }
        let proof = read_checkpoint(base).await?;
        let inputs: Inputs = proof.0.data()?;
        let indexes = base.indexes();
        let key = crate::packs::directory::SegmentKey {
            operation: native.operation,
            digest: native.pack.digest,
        };
        if indexes.inputs().find(inputs.root, key).await? != Some(native) {
            return Err(PreparationBaseError::Context.into());
        }
        let digest = *blake3::hash(&proof.bytes()?).as_bytes();
        verify_digest(base, digest).await?;
        Ok(Self {
            native,
            context: base.context(),
            session: base.session.clone(),
            digest,
        })
    }
    pub(in crate::packs::publication) fn digest(&self) -> [u8; 32] {
        self.digest
    }
    pub(in crate::packs) fn authorize(
        &self,
        context: ClosureContext,
        native: NativePackDescriptor,
    ) -> Result<(), ClosureError> {
        if self.context != context || self.native != native || self.session.live_lease().is_err() {
            return Err(ClosureError::Integrity);
        }
        Ok(())
    }
}
async fn read_checkpoint(
    base: &PreparationBaseResolver,
) -> Result<NativeInputCertificate, InputCheckpointError> {
    let (lease, _) = base.live_lease()?;
    let (client, target, check) = base.capability();
    let proof = client
        .query::<CheckStagedInputs>(target, None, check.clone())
        .await
        .map_err(|e| InputCheckpointError::Retained(Box::new(e)))?
        .output
        .ok_or(PreparationBaseError::Inactive)?;
    let inputs: Inputs = proof.0.data()?;
    if proof.scoped_check(target)? != *check || inputs.format != lease.format {
        return Err(PreparationBaseError::Context.into());
    }
    Ok(proof)
}
pub(in crate::packs::publication) async fn verify_digest(
    base: &PreparationBaseResolver,
    digest: [u8; 32],
) -> Result<(), InputCheckpointError> {
    let proof = read_checkpoint(base).await?;
    if *blake3::hash(&proof.bytes()?).as_bytes() != digest {
        return Err(PreparationBaseError::Context.into());
    }
    let (client, target, check) = base.capability();
    let live = client
        .query::<CheckPreparation>(target, None, check.clone())
        .await
        .map_err(|e| InputCheckpointError::BoundCustody(Box::new(e)))?
        .output
        .ok_or(PreparationBaseError::Inactive)?;
    if live.token != base.context_token()
        || live.base != base.retention_floor()
        || live.format != base.context().format
    {
        return Err(PreparationBaseError::Context.into());
    }
    base.live_lease()?;
    Ok(())
}
