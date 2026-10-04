//! Retire an expired original without inventing its execution result or receipt.
use super::*;
use cellule_runtime::{PreparedCommand, Receipt};

const DOMAIN: &[u8] = b"canopy.custody-retirement.v1\0";
#[derive(Clone, Debug, PartialEq, Eq)]
struct StopData {
    tenant: [u8; 16],
    application: [u8; 16],
    operation: [u8; 16],
    step: u32,
    intent_digest: [u8; 32],
    owner: OwnerFence,
}
impl WireValue for StopData {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        if self.operation == [0; 16] || self.step > MAX_STEPS || self.owner.epoch == 0 {
            return Err(CodecError::Invalid("custody retirement context"));
        }
        e.write_bytes(DOMAIN)?;
        e.write_bytes(&self.tenant)?;
        e.write_bytes(&self.application)?;
        e.write_bytes(&self.operation)?;
        e.write_u32(self.step)?;
        e.write_bytes(&self.intent_digest)?;
        e.write_bytes(self.owner.incarnation.as_bytes())?;
        e.write_u64(self.owner.epoch)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        if d.read_bytes()? != DOMAIN {
            return Err(CodecError::Invalid("custody retirement purpose"));
        }
        let value = Self {
            tenant: crate::packs::directory::index::codec::fixed(d)?,
            application: crate::packs::directory::index::codec::fixed(d)?,
            operation: crate::packs::directory::index::codec::fixed(d)?,
            step: d.read_u32()?,
            intent_digest: crate::packs::directory::index::codec::fixed(d)?,
            owner: OwnerFence {
                incarnation: IncarnationId::from_bytes(
                    crate::packs::directory::index::codec::fixed(d)?,
                ),
                epoch: d.read_u64()?,
            },
        };
        value.encode(&mut BoundedEncoder::new(512)?)?;
        Ok(value)
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CustodyStopInput {
    certificate: CertificateEnvelope,
}
impl WireValue for CustodyStopInput {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        self.certificate.data::<StopData>()?;
        self.certificate.encode(e)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        let value = Self {
            certificate: CertificateEnvelope::decode(d)?,
        };
        value.encode(&mut BoundedEncoder::new(1024)?)?;
        Ok(value)
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CustodyStopReply {
    Stopped,
    Settled,
    Denied(PreparationDenial),
}
impl WireValue for CustodyStopReply {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        match self {
            Self::Stopped => e.write_u8(0),
            Self::Settled => e.write_u8(1),
            Self::Denied(reason) => {
                e.write_u8(2)?;
                PreparationReply::Denied(*reason).encode(e)
            }
        }
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        match d.read_u8()? {
            0 => Ok(Self::Stopped),
            1 => Ok(Self::Settled),
            2 => match PreparationReply::decode(d)? {
                PreparationReply::Denied(reason) => Ok(Self::Denied(reason)),
                _ => Err(CodecError::Invalid("grant in custody retirement denial")),
            },
            _ => Err(CodecError::Invalid("custody retirement reply")),
        }
    }
}
/// Logical closure of an original, never that original's SDK outcome.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CustodyStopFact {
    pub receipt: Receipt,
    pub owner: OwnerFence,
    pub stopped_at_ms: i64,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct StopRecord {
    data: StopData,
    stamp: Stamp,
    stopped_at_ms: i64,
    result: Recorded,
}
impl WireValue for StopRecord {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        if self.stopped_at_ms < 0
            || self.result.rejected()
            || self.result.decode_reply::<CustodyStopReply>()? != CustodyStopReply::Stopped
        {
            return Err(CodecError::Invalid("custody retirement result"));
        }
        self.data.encode(e)?;
        self.stamp.encode(e)?;
        e.write_i64(self.stopped_at_ms)?;
        self.result.encode(e)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        let value = Self {
            data: StopData::decode(d)?,
            stamp: Stamp::decode(d)?,
            stopped_at_ms: d.read_i64()?,
            result: Recorded::decode(d)?,
        };
        value.encode(&mut BoundedEncoder::new(960)?)?;
        Ok(value)
    }
}
impl StopRecord {
    pub(super) fn fact(&self, target: &CellTarget) -> CustodyStopFact {
        CustodyStopFact {
            receipt: Receipt {
                cell: target.cell_id(),
                incarnation: self.data.owner.incarnation,
                commit_sequence: self.result.sequence(),
            },
            owner: self.data.owner,
            stopped_at_ms: self.stopped_at_ms,
        }
    }
}
pub(super) fn record(
    value: &SqlValue,
    intent: &CustodyIntent,
    seed: &[u8; 32],
) -> cellule_runtime::Result<Option<StopRecord>> {
    let bytes = match value {
        SqlValue::Null => return Ok(None),
        SqlValue::Blob(bytes) => bytes,
        _ => return Err(Error::Command("custody retirement row")),
    };
    let certificate: CertificateEnvelope = decode(bytes, 1024)?;
    if !certificate.authenticated(seed) {
        return Err(Error::Command("custody retirement authentication"));
    }
    let value: StopRecord = certificate.data()?;
    let header = intent.header()?;
    if value.data.tenant != header.tenant
        || value.data.application != header.application
        || value.data.operation != header.operation
        || value.data.step != header.step
        || value.data.intent_digest != *blake3::hash(&intent.encoded()?).as_bytes()
        || value.stopped_at_ms < intent.snapshot.evidence().identity().expires_at_ms
    {
        return Err(Error::Command("custody retirement binding"));
    }
    Ok(Some(value))
}

pub struct StopCustodyIntent;
impl Command for StopCustodyIntent {
    const MODULE: &'static str = RepositoryModule::NAME;
    const ID: u32 = 43;
    const CODEC_VERSION: u32 = 1;
    type Input = CustodyStopInput;
    type Output = CustodyStopReply;
    fn execute(
        context: &mut CommandContext<'_, '_>,
        input: Self::Input,
    ) -> cellule_runtime::Result<CommandResult<Self::Output>> {
        let deny = |reason| Ok(CommandResult::Rejected(CustodyStopReply::Denied(reason)));
        let data: StopData = input.certificate.data()?;
        let sets = context.sql(&SqlBatch {
            statements: vec![
                row_statement(data.operation, Some(data.step)),
                seed_statement(),
            ],
        })?;
        let seed = super::super::attestation::seed(&sets[1..])?;
        if !input.certificate.authenticated(&seed)
            || data.tenant != *context.target().tenant().as_bytes()
            || data.application != *context.target().application().as_bytes()
        {
            return deny(PreparationDenial::Unauthorized);
        }
        let Some(saved) = from_sets(&sets, context.target(), data.operation)? else {
            return deny(PreparationDenial::Missing);
        };
        if data.intent_digest != *blake3::hash(&saved.intent.encoded()?).as_bytes() {
            return deny(PreparationDenial::Conflict);
        }
        // These are logical observations, not grants, and precede current owner
        // checks. Neither an accepted original nor an existing stop is rewritten.
        if saved.settled() {
            return Ok(CommandResult::Success(CustodyStopReply::Settled));
        }
        if saved.stopped.is_some() {
            return Ok(CommandResult::Success(CustodyStopReply::Stopped));
        }
        if data.owner != context.owner_fence() {
            return deny(PreparationDenial::Stale);
        }
        let stopped_at_ms = now(context.now_ms())?;
        if saved.evidence().identity().expires_at_ms > stopped_at_ms {
            return deny(PreparationDenial::Conflict);
        }
        let evidence = context
            .mutation_evidence()
            .ok_or(Error::Command("custody retirement evidence absent"))?;
        let result = Recorded::new(
            context.sequence(),
            false,
            encode(&CustodyStopReply::Stopped, 128)?,
        )?;
        let record = StopRecord {
            data,
            stamp: Stamp::of(&evidence),
            stopped_at_ms,
            result,
        };
        let bytes = encode(&CertificateEnvelope::seal(&record, &seed)?, 1024)?;
        super::super::publish::changed(context.sql(&statement(
            "UPDATE catalog_custody_commands SET stopped=?1 WHERE operation=?2 AND step=?3 AND intent=?4 AND phase IS NULL AND stopped IS NULL",
            vec![SqlValue::Blob(bytes),blob(record.data.operation), number(u64::from(record.data.step))?,SqlValue::Blob(saved.intent.encoded()?)],
        ))?)?;
        Ok(CommandResult::Success(CustodyStopReply::Stopped))
    }
}

#[derive(Clone, Debug)]
pub struct CustodyStopOutcome {
    pub original: PendingMutation,
    pub invocation: PendingMutation,
    pub stop: Option<CustodyStopFact>,
    /// None when a different first-writer stop proves logical closure. This
    /// deliberately makes no execution claim about this invocation identity.
    pub committed: Option<Committed<CustodyStopReply>>,
}
#[derive(Clone)]
#[must_use]
pub struct ReadyCustodyStop {
    client: CellClient,
    target: CellTarget,
    request: BeginRequest,
    step: u32,
    intent_digest: [u8; 32],
    original: PendingMutation,
    command: PreparedCommand<StopCustodyIntent>,
}
impl RegisteredCustody {
    pub async fn ready_stop(
        &self,
        client: CellClient,
        identity: MutationIdentity,
        authority: &PreparationAuthority,
    ) -> Result<ReadyCustodyStop, CustodyError> {
        let target = self.evidence().target().clone();
        let header = self.intent.header()?;
        let current = load(&client, &target, header.operation, Some(header.step))
            .await?
            .ok_or(CustodyError::Context)?;
        if current.intent != self.intent {
            return Err(CustodyError::Context);
        }
        if let Some(fact) = current.stop_fact() {
            return Err(CustodyError::Stopped(Box::new(fact)));
        }
        if current.settled() {
            return Err(CustodyError::Context);
        }
        let owner = authority
            .observe(&target)
            .await
            .map_err(|error| CustodyError::Owner(Box::new(error)))?;
        let seed = super::super::attestation::seed(
            &SqlCell::<RepositoryModule>::new(client.clone(), target.clone())?
                .query(
                    None,
                    statement(
                        "SELECT push_cert_seed FROM repository_identity WHERE singleton=1",
                        vec![],
                    ),
                )
                .await
                .map_err(|error| CustodyError::Query(Box::new(error)))?
                .output,
        )?;
        let intent_digest = *blake3::hash(&self.intent.encoded()?).as_bytes();
        let data = StopData {
            tenant: header.tenant,
            application: header.application,
            operation: header.operation,
            step: header.step,
            intent_digest,
            owner,
        };
        let input = CustodyStopInput {
            certificate: CertificateEnvelope::seal(&data, &seed)?,
        };
        input.encode(&mut BoundedEncoder::new(1024)?)?;
        let command = client
            .prepare_command::<StopCustodyIntent>(&target, identity, input)
            .await
            .map_err(|error| CustodyError::StopPreparation(Box::new(error)))?;
        authority
            .check(&target, owner)
            .await
            .map_err(|error| CustodyError::Owner(Box::new(error)))?;
        Ok(ReadyCustodyStop {
            client,
            target,
            request: BeginRequest {
                repository: header.repository,
                operation: header.operation,
                request_digest: header.request_digest,
                actor: header.actor,
                lease_ms: DEFAULT_LEASE_MS,
            },
            step: header.step,
            intent_digest,
            original: self.evidence().clone(),
            command,
        })
    }
}
impl ReadyCustodyStop {
    pub fn evidence(&self) -> &PendingMutation {
        self.command.evidence()
    }
    pub fn original(&self) -> &PendingMutation {
        &self.original
    }
    pub(in crate::packs::publication) fn context(
        &self,
    ) -> (&CellClient, &CellTarget, BeginRequest) {
        (&self.client, &self.target, self.request.clone())
    }
    pub(in crate::packs::publication) fn pending(&self) -> PublicationError {
        PublicationError::CustodyStop(InvocationError::Pending(Box::new(self.evidence().clone())))
    }
    async fn recorded(&self) -> Result<Option<StopRecord>, CustodyError> {
        let saved = load(
            &self.client,
            &self.target,
            self.request.operation,
            Some(self.step),
        )
        .await?
        .ok_or(CustodyError::Context)?;
        if *blake3::hash(&saved.intent.encoded()?).as_bytes() != self.intent_digest {
            return Err(CustodyError::Context);
        }
        Ok(saved.stopped)
    }
    /// The repository's admitted, cancellation-owned cold transition may retire
    /// its one initialization head without starting a separate discovery task.
    /// This does not authorize native work or discard the original's evidence.
    pub(crate) async fn complete_tracked(self) -> Result<CustodyStopOutcome, PublicationError> {
        self.complete_exact(false, 0).await
    }
    pub(in crate::packs::publication) async fn dispatch(
        self,
        recover: bool,
        fault: u8,
    ) -> Result<PublicationOutcome, PublicationError> {
        self.complete_exact(recover, fault)
            .await
            .map(|outcome| PublicationOutcome::CustodyStop(Box::new(outcome)))
    }
    async fn complete_exact(
        self,
        recover: bool,
        fault: u8,
    ) -> Result<CustodyStopOutcome, PublicationError> {
        let saved = self
            .recorded()
            .await
            .map_err(|source| PublicationError::Custody {
                evidence: Box::new(self.evidence().clone()),
                source: Box::new(source),
            })?;
        let invocation = self.evidence().clone();
        let (stop, committed) = if let Some(saved) = saved {
            let committed = if saved.stamp == Stamp::of(&invocation)
                && saved.data.owner.incarnation == invocation.incarnation()
            {
                Some(saved.result.committed(&invocation).map_err(|source| {
                    PublicationError::Custody {
                        evidence: Box::new(invocation.clone()),
                        source: Box::new(source.into()),
                    }
                })?)
            } else {
                None
            };
            (Some(saved.fact(&self.target)), committed)
        } else {
            let committed = super::super::exact::invoke(
                &self.client,
                self.command.clone(),
                recover,
                128,
                fault,
            )
            .await
            .map_err(PublicationError::CustodyStop)?;
            let saved = self
                .recorded()
                .await
                .map_err(|source| PublicationError::Custody {
                    evidence: Box::new(invocation.clone()),
                    source: Box::new(source),
                })?;
            if committed.output == CustodyStopReply::Stopped && saved.is_none() {
                return Err(self.pending());
            }
            (saved.map(|value| value.fact(&self.target)), Some(committed))
        };
        Ok(CustodyStopOutcome {
            original: self.original,
            invocation,
            stop,
            committed,
        })
    }
    #[cfg(test)]
    pub(in crate::packs::publication) fn input_for_test(
        &self,
    ) -> Result<CustodyStopInput, CodecError> {
        decode(self.command.input_bytes(), 1024)
    }
    #[cfg(test)]
    pub(in crate::packs::publication) fn command_for_test(
        &self,
    ) -> PreparedCommand<StopCustodyIntent> {
        self.command.clone()
    }
}

#[cfg(test)]
impl CustodyStopInput {
    pub(in crate::packs::publication) fn tamper_for_test(mut self) -> Self {
        let last = self.certificate.body.len() - 1;
        self.certificate.body[last] ^= 2;
        self
    }
}
