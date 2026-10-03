//! Exact policy-page registration shares foreground admission and custody.
use super::*;

pub(super) const RESERVATION: u64 = 2 * REF_POLICY_PAGE_BYTES as u64;

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
        })
    }
}
impl ReadyRefPolicyPage {
    pub(super) fn dispatch_copy(&self) -> Self {
        Self {
            prepared: self.prepared.clone(),
            intent: self.intent.clone(),
            command: self.command.clone(),
        }
    }
    pub(super) fn pending(&self) -> PublicationError {
        PublicationError::PolicyPage(InvocationError::Pending(Box::new(
            self.command.evidence().clone(),
        )))
    }
    #[cfg(test)]
    pub(in crate::packs::publication) fn evidence_for_test(
        &self,
    ) -> cellule_runtime::PendingMutation {
        self.command.evidence().clone()
    }
    pub(super) async fn dispatch(self, recover: bool, fault: u8) -> DispatchResult {
        let client = self.prepared.base.capability().0.clone();
        super::super::exact::invoke_guarded(&client, self.command, recover, 512, fault, move || {
            self.prepared
                .ensure_live()
                .map_err(|_| Error::Command("inactive ref policy preparation"))
        })
        .await
        .map(PublicationOutcome::PolicyPage)
        .map_err(PublicationError::PolicyPage)
    }
}
