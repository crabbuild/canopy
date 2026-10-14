//! Symbolic HEAD changes share the original private owner and exact registered dispatch.
use super::*;
use canopy_object_storage::artifact::ArtifactStore;

#[must_use]
pub struct ReadyNativeHead {
    owner: Arc<PreparedCatalog>,
    command: PreparedCommand<PublishNativeHead>,
}
impl PreparedCatalog {
    pub async fn ready_native_head(
        self: &Arc<Self>,
        identity: MutationIdentity,
        request: HeadRequest,
    ) -> Result<ReadyNativeHead, NativeHeadPreparationError> {
        let proof = self.native_head_proof(request).await?;
        self.ensure_live()?;
        let (client, target, _) = self.base.capability();
        let command = client
            .prepare_command::<PublishNativeHead>(target, identity, proof)
            .await
            .map_err(|e| NativeHeadPreparationError::Command(Box::new(e)))?;
        self.ensure_live()?;
        Ok(ReadyNativeHead {
            owner: self.clone(),
            command,
        })
    }
}
impl ReadyNativeHead {
    pub async fn persist_recovery(
        &self,
        store: &ArtifactStore,
        identity: MutationIdentity,
    ) -> Result<RegisteredRootRecovery, RootRecoveryError> {
        super::super::recovery::persist(
            &self.owner.base.session,
            &self.command,
            super::super::recovery::Kind::Head,
            store,
            identity,
            0,
        )
        .await
    }
    pub fn bind_recovery(
        self,
        registered: RegisteredRootRecovery,
        store: &ArtifactStore,
    ) -> Result<ReadyBoundRecovery, Box<RecoveryBindingFailure<Self>>> {
        if !registered.matches_original(
            super::super::recovery::Kind::Head,
            self.command.evidence(),
            None,
            &self.owner.base.session,
            store,
        ) {
            return Err(Box::new(RecoveryBindingFailure {
                original: self,
                registered,
            }));
        }
        Ok(ReadyBoundRecovery::new(
            PushPreparation::Catalog(self.owner),
            None,
            false,
            registered,
            store,
        ))
    }
}
