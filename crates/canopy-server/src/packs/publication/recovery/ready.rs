//! Restored work retains the same foreground/account bounds as live work.
use super::*;
#[derive(Clone)]
#[must_use]
pub struct ReadyRootRecovery {
    recovery: RegisteredRootRecovery,
    client: CellClient,
    store: ArtifactStore,
}
pub(in crate::packs::publication) const fn reservation() -> u64 {
    // Two root bodies (loaded input and transport), plus two bounded headers.
    2 * (ROOT_COMPLETION_BYTES as u64 + ROOT_BYTES as u64)
}
impl RegisteredRootRecovery {
    /// Submit through the existing PublicationCoordinator. This capability has
    /// no native work or original session; known results resolve before fresh
    /// custody, while authoritative absence restores exactly the saved bytes.
    pub fn ready(
        self,
        client: CellClient,
        store: ArtifactStore,
    ) -> Result<ReadyRootRecovery, RootRecoveryError> {
        target_matches(self.evidence().target(), &store, &self.record.check)?;
        Ok(ReadyRootRecovery {
            recovery: self,
            client,
            store,
        })
    }
}
impl ReadyRootRecovery {
    pub(in crate::packs::publication) fn capability(
        &self,
    ) -> (&CellClient, &CellTarget, &LeaseCheck) {
        (
            &self.client,
            self.recovery.evidence().target(),
            &self.recovery.record.check,
        )
    }
    pub(in crate::packs::publication) fn pending(&self) -> PublicationError {
        PublicationError::RootPush(InvocationError::Pending(Box::new(
            self.recovery.evidence().clone(),
        )))
    }
    pub(in crate::packs::publication) async fn dispatch(
        self,
        fault: u8,
    ) -> Result<PublicationOutcome, PublicationError> {
        if fault == 1 {
            return Err(self.pending());
        }
        let outcome = self.recovery.dispatch(&self.client, &self.store).await;
        if fault == 2 {
            return Err(self.pending());
        }
        assert_ne!(fault, 3, "injected durable root panic after execution");
        outcome
            .map(PublicationOutcome::RootPush)
            .map_err(PublicationError::RootPush)
    }
}
