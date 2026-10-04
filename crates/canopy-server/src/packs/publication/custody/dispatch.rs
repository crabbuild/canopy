//! Service-owned original registration and custody command, never a fresh retry.
use super::*;
use cellule_runtime::PreparedCommand;

// Retained and dispatch owners each hold intent + registrar body (four copies).
// Registrar transport and bounded query decode add two more intent ceilings.
// Original execution bodies and phase/reply decoding have independent ceilings.
pub(in crate::packs::publication) const RESERVATION: u64 =
    6 * INTENT_BYTES as u64 + 2 * INPUT_BYTES as u64 + 2 * 1024;

#[derive(Clone)]
pub(in crate::packs::publication) struct OwnedCustody {
    prepared: PreparedCustody,
    registration: Option<PreparedCommand<RegisterCustodyIntent>>,
}
impl OwnedCustody {
    pub(in crate::packs::publication) async fn prepare(
        client: &CellClient,
        target: &CellTarget,
        action: CustodyAction,
        identity: MutationIdentity,
    ) -> Result<Self, CustodyError> {
        let prepared = PreparedCustody::prepare(client, target, action, identity).await?;
        let registration = client
            .prepare_command::<RegisterCustodyIntent>(
                target,
                crate::server::mutation_identity()
                    .map_err(|error| CustodyError::Clock(Box::new(error)))?,
                prepared.intent.clone(),
            )
            .await
            .map_err(|error| CustodyError::Registration(Box::new(error)))?;
        Ok(Self {
            prepared,
            registration: Some(registration),
        })
    }
    pub(in crate::packs::publication) async fn restore(
        client: &CellClient,
        target: &CellTarget,
        operation: [u8; 16],
    ) -> Result<Self, CustodyError> {
        let registered = load(client, target, operation, None)
            .await?
            .ok_or(CustodyError::Context)?;
        Ok(Self {
            prepared: PreparedCustody {
                intent: registered.intent,
            },
            registration: None,
        })
    }
    pub(in crate::packs::publication) fn action(&self) -> Result<CustodyAction, CodecError> {
        Ok(self.prepared.intent.request()?.action)
    }
    pub(in crate::packs::publication) fn evidence(&self) -> &PendingMutation {
        self.prepared.evidence()
    }
    #[cfg(test)]
    pub(in crate::packs::publication) fn registration_evidence(&self) -> Option<&PendingMutation> {
        self.registration.as_ref().map(PreparedCommand::evidence)
    }
    async fn persist(
        &self,
        client: &CellClient,
        recover: bool,
        fault: u8,
    ) -> Result<RegisteredCustody, CustodyError> {
        let header = self.prepared.intent.header()?;
        let target = self.evidence().target();
        if let Some(saved) = load(client, target, header.operation, Some(header.step)).await? {
            return if saved.intent == self.prepared.intent {
                Ok(saved)
            } else {
                Err(CustodyError::Context)
            };
        }
        let registration = self.registration.as_ref().ok_or(CustodyError::Context)?;
        let registration_fault = match fault {
            4 => 1,
            5 => 2,
            6 => 3,
            _ => 0,
        };
        let result = super::super::exact::invoke(
            client,
            registration.clone(),
            recover,
            4096,
            registration_fault,
        )
        .await;
        // Injected lost registrar replies remain uncertain until the observer
        // requests recovery; ordinary network loss can use a durable pointer.
        if registration_fault != 0 {
            result.map_err(|error| CustodyError::Registration(Box::new(error)))?;
        } else if let Some(saved) =
            load(client, target, header.operation, Some(header.step)).await?
        {
            return if saved.intent == self.prepared.intent {
                Ok(saved)
            } else {
                Err(CustodyError::Context)
            };
        } else {
            result.map_err(|error| CustodyError::Registration(Box::new(error)))?;
        }
        Err(CustodyError::Context)
    }
    pub(in crate::packs::publication) async fn invoke(
        &self,
        client: &CellClient,
        recover: bool,
        fault: u8,
        before_execute: impl FnOnce() -> Result<(), Error> + Send,
    ) -> Result<Result<Committed<CustodyReply>, InvocationError<CustodyReply>>, CustodyError> {
        let saved = self.persist(client, recover, fault).await?;
        let execution_fault = if fault <= 3 { fault } else { 0 };
        if execution_fault == 1 {
            return Ok(Err(InvocationError::Pending(Box::new(
                self.evidence().clone(),
            ))));
        }
        let result = saved.recover_guarded(client, before_execute).await;
        if execution_fault == 2 {
            return Ok(Err(InvocationError::Pending(Box::new(
                self.evidence().clone(),
            ))));
        }
        assert_ne!(
            execution_fault, 3,
            "injected custody command panic after execution"
        );
        Ok(result)
    }
}
