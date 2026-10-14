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
/// A read probe retains no original command body or registrar. Its exact
/// fingerprint prevents a later logical successor from hiding this ordinal.
#[derive(Clone)]
pub(in crate::packs::publication) struct CustodyStopProbe {
    target: CellTarget,
    operation: [u8; 16],
    purpose: CustodyPurpose,
    step: u32,
    digest: [u8; 32],
    evidence: PendingMutation,
}
impl CustodyStopProbe {
    pub(in crate::packs::publication) fn evidence(&self) -> &PendingMutation {
        &self.evidence
    }
    pub(in crate::packs::publication) async fn observed(
        &self,
        client: &CellClient,
    ) -> Result<bool, CustodyError> {
        let Some(saved) = load(
            client,
            &self.target,
            CustodyKey {
                purpose: self.purpose,
                operation: self.operation,
            },
            Some(self.step),
        )
        .await?
        else {
            return Ok(false);
        };
        if *blake3::hash(&saved.intent.encoded()?).as_bytes() != self.digest
            || saved.evidence() != &self.evidence
        {
            return Err(CustodyError::Context);
        }
        Ok(saved.stop_fact().is_some())
    }
}
impl OwnedCustody {
    /// Observe only this exact accepted ordinal. This must never execute an
    /// absent original or substitute the latest renewal's receipt.
    pub(in crate::packs::publication) async fn serving_grant(
        &self,
        client: &CellClient,
    ) -> Result<ServingLease, CustodyError> {
        let header = self.prepared.intent.header()?;
        if !matches!(self.action()?, CustodyAction::AcquireServing(_)) {
            return Err(CustodyError::Context);
        }
        let saved = load(
            client,
            self.evidence().target(),
            header.key(),
            Some(header.step),
        )
        .await?
        .ok_or(CustodyError::Context)?;
        if saved.intent != self.prepared.intent || saved.stopped.is_some() {
            return Err(CustodyError::Context);
        }
        let phase = saved.phase.ok_or(CustodyError::Context)?;
        let committed = phase.committed::<CustodyReply>(self.evidence())?;
        match committed.output {
            CustodyReply::Serving(ServingReply::Granted(lease))
                if !phase.rejected()
                    && lease.token.repository == header.repository
                    && lease.token.reader == header.operation
                    && lease.token.admission_sequence == committed.receipt.commit_sequence =>
            {
                Ok(*lease)
            }
            _ => Err(CustodyError::Context),
        }
    }
    pub(in crate::packs::publication) fn stop_probe(
        &self,
    ) -> Result<CustodyStopProbe, CustodyError> {
        let header = self.prepared.intent.header()?;
        Ok(CustodyStopProbe {
            target: self.evidence().target().clone(),
            operation: header.operation,
            purpose: header.purpose,
            step: header.step,
            digest: *blake3::hash(&self.prepared.intent.encoded()?).as_bytes(),
            evidence: self.evidence().clone(),
        })
    }
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
        Self::restore_for(client, target, CustodyPurpose::Creating, operation).await
    }
    pub(in crate::packs::publication) async fn restore_for(
        client: &CellClient,
        target: &CellTarget,
        purpose: CustodyPurpose,
        operation: [u8; 16],
    ) -> Result<Self, CustodyError> {
        let registered = load(client, target, CustodyKey { purpose, operation }, None)
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
        if let Some(saved) = load(client, target, header.key(), Some(header.step)).await? {
            return if saved.intent == self.prepared.intent {
                if let Some(fact) = saved.stop_fact() {
                    return Err(CustodyError::Stopped(Box::new(fact)));
                }
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
        } else if let Some(saved) = load(client, target, header.key(), Some(header.step)).await? {
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
