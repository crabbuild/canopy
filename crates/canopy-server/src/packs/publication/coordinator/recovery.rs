//! Registered recovery keeps the original live producer's custody and intent.
use super::*;
use canopy_object_storage::artifact::ArtifactStore;

/// Refused binding returns both original capabilities without minting authority.
pub struct RecoveryBindingFailure<R> {
    pub original: R,
    pub registered: RegisteredRootRecovery,
}
impl<R> std::fmt::Debug for RecoveryBindingFailure<R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RecoveryBindingFailure")
            .finish_non_exhaustive()
    }
}
impl<R> std::fmt::Display for RecoveryBindingFailure<R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("registered recovery differs from the original factory output")
    }
}
impl<R> std::error::Error for RecoveryBindingFailure<R> {}

/// Only the original ready factory output can bind this live capability. A
/// restored record alone has no shared lifecycle fence, clock or intent owner.
#[derive(Clone)]
#[must_use]
pub struct ReadyBoundRecovery {
    pub(super) owner: PushPreparation,
    intent: Option<Arc<RefPolicyPreparation>>,
    pub(super) ready: ReadyRootRecovery,
    pub(super) refusal: bool,
}
impl ReadyBoundRecovery {
    #[cfg(test)]
    pub(in crate::packs::publication) fn evidence_for_test(
        &self,
    ) -> cellule_runtime::PendingMutation {
        self.ready.evidence_for_test()
    }
    pub(super) fn new(
        owner: PushPreparation,
        intent: Option<Arc<RefPolicyPreparation>>,
        refusal: bool,
        registered: RegisteredRootRecovery,
        store: &ArtifactStore,
    ) -> Self {
        let client = owner.capability().0.clone();
        Self {
            owner,
            intent,
            ready: ReadyRootRecovery::from_verified(registered, client, store.clone()),
            refusal,
        }
    }
    pub(super) async fn dispatch(self, fault: u8) -> DispatchResult {
        let result = self
            .ready
            .dispatch_bound(fault, Some(self.owner.session()))
            .await;
        // Keep original custody and original policy intent alive through all I/O.
        // The retained queue copy keeps them after an uncertain dispatch.
        drop(self.intent);
        drop(self.owner);
        result
    }
}
