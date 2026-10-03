//! Exact bound input checkpoint dispatch reuses publication admission and recovery.
use super::*;

pub(super) const INPUT_RESERVATION: u64 = 8192;
const INPUT_INLINE_BYTES: u32 = 4096;

#[derive(Debug, thiserror::Error)]
pub enum NativeInputReadyError {
    #[error("bound input preparation inactive")]
    Base(#[from] PreparationBaseError),
    #[error("bound input checkpoint encoding failed")]
    Codec(#[from] CodecError),
    #[error("bound input checkpoint command preparation failed")]
    Command(#[source] Box<InvocationError<StagingReply>>),
}
#[must_use]
pub struct ReadyNativeInputs {
    pub(super) session: Arc<PreparationSession>,
    pub(super) command: PreparedCommand<RegisterStagedInputs>,
    pub(super) digest: [u8; 32],
}
/// Original durable outcome plus a fresh custody observation. Neither the
/// recorded lease nor a successful observation grants future authority; every
/// proof factory still checks the shared session and final commands recheck SQL.
#[derive(Clone, Debug)]
pub struct RegisteredNativeInputs {
    pub registration: Committed<StagingReply>,
    pub custody: Result<(), Arc<InputCheckpointError>>,
}
impl PreparationSession {
    /// Prepare, but do not execute, the exact adopted-input checkpoint command.
    /// Submit through the repository's existing PublicationCoordinator. Failed
    /// admission returns the same command, identity and session for later use.
    pub async fn ready_inputs(
        self: &Arc<Self>,
        identity: MutationIdentity,
        proof: NativeInputCertificate,
    ) -> Result<ReadyNativeInputs, NativeInputReadyError> {
        self.live_lease()?;
        let digest = proof.bound_digest(self)?;
        proof.encode(&mut BoundedEncoder::new(INPUT_INLINE_BYTES)?)?;
        let command = self
            .client
            .prepare_command::<RegisterStagedInputs>(&self.target, identity, proof)
            .await
            .map_err(|e| NativeInputReadyError::Command(Box::new(e)))?;
        self.live_lease()?;
        Ok(ReadyNativeInputs {
            session: self.clone(),
            command,
            digest,
        })
    }
}
impl ReadyNativeInputs {
    pub(super) async fn dispatch(self, recover: bool, fault: u8) -> DispatchResult {
        let result = super::super::exact::invoke(
            &self.session.client,
            self.command,
            recover,
            INPUT_INLINE_BYTES,
            fault,
        )
        .await;
        match result {
            Ok(registration) => {
                // Keep the known original outcome across fresh probes. Failure
                // revokes local use, never converts committed recovery to denial.
                let matched = matches!(&registration.output, StagingReply::Granted(lease)
                    if lease.token == self.session.lease.token && lease.format == self.session.lease.format);
                let custody = if matched {
                    super::super::inputs::observe_bound_registration(
                        &self.session,
                        self.digest,
                        registration.receipt,
                    )
                    .await
                } else {
                    Err(PreparationBaseError::Context.into())
                };
                if custody.is_err() {
                    self.session.fence();
                }
                Ok(PublicationOutcome::Inputs(RegisteredNativeInputs {
                    registration,
                    custody: custody.map_err(Arc::new),
                }))
            }
            Err(error) => {
                if !matches!(
                    &error,
                    InvocationError::Pending(_) | InvocationError::InvalidPublishedResult { .. }
                ) {
                    self.session.fence();
                }
                Err(PublicationError::Inputs(error))
            }
        }
    }
}
