//! Restored work retains the same foreground/account bounds as live work.
use super::*;
#[derive(Clone)]
#[must_use]
pub struct ReadyRootRecovery {
    // The immutable headers/certificate are shared by dispatch and observers;
    // cloning a queue entry must not copy the complete recovery bundle.
    recovery: std::sync::Arc<RegisteredRootRecovery>,
    client: CellClient,
    store: ArtifactStore,
    refusing: std::sync::Arc<std::sync::atomic::AtomicBool>,
    #[cfg(test)]
    refusal_fault: std::sync::Arc<std::sync::atomic::AtomicU8>,
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
        Ok(ReadyRootRecovery::from_verified(self, client, store))
    }
}
impl ReadyRootRecovery {
    #[cfg(test)]
    pub(in crate::packs::publication) fn refusal_fault_for_test(&self, fault: u8) {
        self.refusal_fault
            .store(fault, std::sync::atomic::Ordering::Release);
    }
    #[cfg(test)]
    pub(in crate::packs::publication) fn evidence_for_test(&self) -> PendingMutation {
        self.recovery.evidence().clone()
    }
    /// Only after the original factory or public recovery entry validates the
    /// exact repository/artifact context. This does not bind a live lifecycle.
    pub(in crate::packs::publication) fn from_verified(
        recovery: RegisteredRootRecovery,
        client: CellClient,
        store: ArtifactStore,
    ) -> Self {
        Self {
            recovery: std::sync::Arc::new(recovery),
            client,
            store,
            refusing: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            #[cfg(test)]
            refusal_fault: std::sync::Arc::new(std::sync::atomic::AtomicU8::new(0)),
        }
    }
    pub(in crate::packs::publication) fn matches_registered(
        &self,
        registered: &RegisteredRootRecovery,
    ) -> bool {
        // Advancing the durable head cannot replace the original queued
        // command. A later head for this exact attempt merely wakes its cold
        // owner; dispatch authenticates the original predecessor journal.
        self.recovery.record.check == registered.record.check
            && self.recovery.record.tenant == registered.record.tenant
            && self.recovery.record.application == registered.record.application
            && (self.recovery.certificate == registered.certificate
                || self.recovery.record.step < registered.record.step)
    }
    pub(in crate::packs::publication) fn is_policy_page(&self) -> bool {
        self.recovery.record.kind == Kind::Policy
    }
    pub(in crate::packs::publication) fn reservation(&self) -> u64 {
        2 * (u64::from(self.recovery.record.kind.body_limit())
            + u64::from(ROOT_BYTES)
            + if self.recovery.record.refusal.is_some() {
                u64::from(ROOT_COMPLETION_BYTES)
            } else {
                0
            })
    }
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
        if self.refusing.load(std::sync::atomic::Ordering::Acquire)
            && let Some(saved) = &self.recovery.bundle.refusal
        {
            PublicationError::RootPush(InvocationError::Pending(Box::new(
                saved.snapshot.evidence().clone(),
            )))
        } else if self.recovery.record.kind == Kind::Initialization {
            PublicationError::Initialization(InvocationError::Pending(Box::new(
                self.recovery.evidence().clone(),
            )))
        } else if self.recovery.record.kind == Kind::Policy {
            PublicationError::PolicyPage(InvocationError::Pending(Box::new(
                self.recovery.evidence().clone(),
            )))
        } else {
            PublicationError::RootPush(InvocationError::Pending(Box::new(
                self.recovery.evidence().clone(),
            )))
        }
    }
    pub(in crate::packs::publication) async fn dispatch(
        self,
        fault: u8,
    ) -> Result<PublicationOutcome, PublicationError> {
        self.dispatch_bound(fault, None).await
    }
    pub(in crate::packs::publication) async fn dispatch_bound(
        self,
        fault: u8,
        original: Option<&PreparationSession>,
    ) -> Result<PublicationOutcome, PublicationError> {
        if fault == 1 {
            return Err(self.pending());
        }
        let outcome = match original {
            Some(original) => {
                self.recovery
                    .dispatch_bound(
                        &self.client,
                        &self.store,
                        &self.refusing,
                        Some(original),
                        #[cfg(test)]
                        Some(&self.refusal_fault),
                    )
                    .await
            }
            None => {
                self.recovery
                    .dispatch_any(&self.client, &self.store, &self.refusing)
                    .await
            }
        };
        if fault == 2 {
            return Err(self.pending());
        }
        assert_ne!(fault, 3, "injected durable root panic after execution");
        outcome
    }
}
