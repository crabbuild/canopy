//! Exact policy-page registration shares foreground admission and custody.
use super::*;

#[derive(Debug, thiserror::Error)]
pub enum RefPolicyReadyError {
    #[error("ref policy page preparation failed")]
    Preparation(#[source] Box<RefPolicyPreparationError>),
    #[error("ref policy page preparation inactive")]
    Base(#[from] PreparationBaseError),
    #[error("ref policy page encoding failed")]
    Codec(#[from] CodecError),
    #[error("ref policy page command preparation failed")]
    Command(#[source] Box<InvocationError<RefPolicyReply>>),
}

/// Retain both original intent and verified inputs through an ambiguous page.
/// A regenerated intent/page/MutationIdentity is not exact recovery.
#[must_use]
pub struct ReadyRefPolicyPage {
    pub(super) prepared: Arc<PreparedCatalog>,
    intent: Arc<RefPolicyPreparation>,
    command: PreparedCommand<RegisterRefPolicyPage>,
    refusal: Option<Arc<ReadyRootPush>>,
    #[cfg(test)]
    refusal_fault: u8,
}
impl RefPolicyPreparation {
    pub async fn ready_page(
        self: &Arc<Self>,
        prepared: &Arc<PreparedCatalog>,
        identity: MutationIdentity,
        start: usize,
    ) -> Result<ReadyRefPolicyPage, RefPolicyReadyError> {
        let input = self
            .page(prepared, start)
            .await
            .map_err(|error| RefPolicyReadyError::Preparation(Box::new(error)))?;
        input.encode(&mut BoundedEncoder::new(REF_POLICY_PAGE_BYTES)?)?;
        prepared.ensure_live()?;
        let (client, target, _) = prepared.base.capability();
        let command = client
            .prepare_command::<RegisterRefPolicyPage>(target, identity, input)
            .await
            .map_err(|error| RefPolicyReadyError::Command(Box::new(error)))?;
        prepared.ensure_live()?;
        Ok(ReadyRefPolicyPage {
            prepared: prepared.clone(),
            intent: self.clone(),
            command,
            refusal: None,
            #[cfg(test)]
            refusal_fault: 0,
        })
    }
}
/// Refused composition returns both original private commands unchanged.
pub struct RefPolicyRefusalFailure {
    pub page: ReadyRefPolicyPage,
    pub refusal: Arc<ReadyRootPush>,
}
impl std::fmt::Debug for RefPolicyRefusalFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RefPolicyRefusalFailure")
            .finish_non_exhaustive()
    }
}
impl std::fmt::Display for RefPolicyRefusalFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("policy refusal requires the same session and a refusal-only command")
    }
}
impl std::error::Error for RefPolicyRefusalFailure {}
impl ReadyRefPolicyPage {
    /// Convert only after this exact original page/refusal bundle is registered.
    /// The shared session and intent survive admission failure and uncertainty.
    pub fn bind_recovery(
        self,
        registered: RegisteredRootRecovery,
        store: &canopy_object_storage::artifact::ArtifactStore,
    ) -> Result<ReadyBoundRecovery, Box<RecoveryBindingFailure<Self>>> {
        if !registered.matches_original(
            super::super::recovery::Kind::Policy,
            self.command.evidence(),
            self.refusal
                .as_ref()
                .and_then(|ready| ready.refusal_command())
                .map(|command| command.evidence()),
            &self.prepared.base.session,
            store,
        ) {
            return Err(Box::new(RecoveryBindingFailure {
                original: self,
                registered,
            }));
        }
        let ready = ReadyBoundRecovery::new(
            PushPreparation::Catalog(self.prepared),
            Some(self.intent),
            false,
            registered,
            store,
        );
        #[cfg(test)]
        ready.ready.refusal_fault_for_test(self.refusal_fault);
        Ok(ready)
    }
    /// Register both original SDK identities before page submission. A known
    /// settled predecessor is required before this attempt can advance its pin.
    pub async fn persist_recovery(
        &self,
        store: &canopy_object_storage::artifact::ArtifactStore,
        identity: MutationIdentity,
        previous: Option<&RegisteredRootRecovery>,
    ) -> Result<RegisteredRootRecovery, RootRecoveryError> {
        let refusal = self
            .refusal
            .as_ref()
            .and_then(|value| value.refusal_command())
            .ok_or(RootRecoveryError::Context)?;
        // Artifact uploads retain sizeable nested futures. Keep that state off
        // the caller's stack, including when used by an owned native worker.
        Box::pin(super::super::recovery::persist_full(
            &self.prepared.base.session,
            &self.command,
            super::super::recovery::Kind::Policy,
            Some(refusal),
            previous,
            store,
            identity,
            0,
        ))
        .await
    }
    /// Arm a pre-frozen terminal refusal before admission. Both exact commands
    /// occupy one operation in the existing fair queue and remain charged until
    /// a known page success or terminal refusal. Share one Arc across successful
    /// pages so a large report is frozen once. No new owner or input is minted.
    pub fn with_refusal(
        mut self,
        refusal: impl Into<Arc<ReadyRootPush>>,
    ) -> Result<Self, Box<RefPolicyRefusalFailure>> {
        let refusal = refusal.into();
        let source = refusal.owner.session();
        let session = &self.prepared.base.session;
        if self.refusal.is_some()
            || !refusal.refusal
            || source.target != session.target
            || source.check != session.check
            || source.ceiling != session.ceiling
            || !Arc::ptr_eq(&source.deadline, &session.deadline)
            || !Arc::ptr_eq(&source.fenced, &session.fenced)
        {
            return Err(Box::new(RefPolicyRefusalFailure {
                page: self,
                refusal,
            }));
        }
        self.refusal = Some(refusal);
        Ok(self)
    }
    #[cfg(test)]
    pub(in crate::packs::publication) fn refusal_fault_for_test(&mut self, fault: u8) {
        self.refusal_fault = fault;
    }
    #[cfg(test)]
    pub(in crate::packs::publication) fn command_for_test(
        &self,
    ) -> &PreparedCommand<RegisterRefPolicyPage> {
        &self.command
    }
    #[cfg(test)]
    pub(in crate::packs::publication) fn evidence_for_test(
        &self,
    ) -> cellule_runtime::PendingMutation {
        self.command.evidence().clone()
    }
}
