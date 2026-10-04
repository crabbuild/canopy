//! Initialization reuses exact registered recovery and original live custody.
use super::*;
use canopy_object_storage::artifact::ArtifactStore;

/// Private empty-catalog proof and exact original SDK command. Registration
/// precedes every dispatch; unknown registration cannot authorize execution.
#[must_use]
pub struct ReadyInitialization {
    owner: Arc<PreparedCatalog>,
    command: PreparedCommand<InitializeCatalogRefs>,
}
impl PreparedCatalog {
    pub async fn ready_initialization(
        self: &Arc<Self>,
        identity: MutationIdentity,
    ) -> Result<ReadyInitialization, InitializationPreparationError> {
        let proof = self.empty_ref_initialization().await?;
        let (client, target, _) = self.base.capability();
        self.ensure_live()?;
        let command = client
            .prepare_command::<InitializeCatalogRefs>(target, identity, proof)
            .await
            .map_err(|error| InitializationPreparationError::Command(Box::new(error)))?;
        self.ensure_live()?;
        Ok(ReadyInitialization {
            owner: Arc::clone(self),
            command,
        })
    }
}
impl ReadyInitialization {
    pub async fn persist_recovery(
        &self,
        store: &ArtifactStore,
        identity: MutationIdentity,
    ) -> Result<RegisteredRootRecovery, RootRecoveryError> {
        super::super::recovery::persist(
            &self.owner.base.session,
            &self.command,
            super::super::recovery::Kind::Initialization,
            store,
            identity,
            0,
        )
        .await
    }
    /// Reuses the account-fair publication queue and its exact live-session
    /// binding. A decoded recovery record cannot substitute for this owner.
    pub fn bind_recovery(
        self,
        registered: RegisteredRootRecovery,
        store: &ArtifactStore,
    ) -> Result<ReadyBoundRecovery, Box<RecoveryBindingFailure<Self>>> {
        if !self.matches(&registered, store) {
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
    fn matches(&self, registered: &RegisteredRootRecovery, store: &ArtifactStore) -> bool {
        registered.matches_original(
            super::super::recovery::Kind::Initialization,
            self.command.evidence(),
            None,
            &self.owner.base.session,
            store,
        )
    }
    /// Repository startup already owns its bounded transition admission. Keep
    /// this exact original session through its registered command and all I/O.
    pub(crate) async fn complete(
        self,
        registered: &RegisteredRootRecovery,
        store: &ArtifactStore,
    ) -> Result<Committed<InitializationReply>, PublicationError> {
        if !self.matches(registered, store) {
            return Err(PublicationError::Recovery {
                evidence: Box::new(self.command.evidence().clone()),
                source: Box::new(RootRecoveryError::Context),
            });
        }
        let client = self.owner.base.capability().0;
        let result = registered
            .dispatch_initialization(
                client,
                store,
                &self.owner.base.session.authority,
                Some(&self.owner.base.session),
            )
            .await;
        drop(self.owner);
        result
    }
}
