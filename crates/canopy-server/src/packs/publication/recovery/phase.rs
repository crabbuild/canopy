//! Domain transition and original result share the executing Cell transaction.
use super::*;

const PHASE_BYTES: u32 = 2048;
const PHASE_DOMAIN: &[u8] = b"canopy.publication-phase.v1\0";
const FRAME_DOMAIN: &[u8] = b"canopy.settled-publication-frame.v1\0";

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct Recorded {
    sequence: u64,
    rejected: bool,
    result: Vec<u8>,
}
impl Recorded {
    pub(super) fn new(sequence: u64, rejected: bool, result: Vec<u8>) -> Result<Self, CodecError> {
        let value = Self {
            sequence,
            rejected,
            result,
        };
        value.encode(&mut BoundedEncoder::new(1024)?)?;
        Ok(value)
    }
    pub(super) fn rejected(&self) -> bool {
        self.rejected
    }
    pub(super) fn decode_reply<T: WireValue>(&self) -> Result<T, CodecError> {
        let mut d = BoundedDecoder::new(&self.result, 512)?;
        let result = T::decode(&mut d)?;
        d.finish()?;
        Ok(result)
    }
    pub(super) fn committed<T: WireValue>(
        &self,
        evidence: &PendingMutation,
    ) -> Result<Committed<T>, CodecError> {
        Ok(Committed {
            output: self.decode_reply()?,
            receipt: cellule_runtime::Receipt {
                cell: evidence.target().cell_id(),
                incarnation: evidence.incarnation(),
                commit_sequence: self.sequence,
            },
        })
    }
}
impl WireValue for Recorded {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        if self.sequence == 0
            || self.sequence > i64::MAX as u64
            || self.result.is_empty()
            || self.result.len() > 512
        {
            return Err(CodecError::Invalid("invalid recorded publication result"));
        }
        e.write_u64(self.sequence)?;
        e.write_bool(self.rejected)?;
        e.write_bytes(&self.result)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        let value = Self {
            sequence: d.read_u64()?,
            rejected: d.read_bool()?,
            result: d.read_bytes()?.to_vec(),
        };
        value.encode(&mut BoundedEncoder::new(1024)?)?;
        Ok(value)
    }
}
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(super) struct Journal {
    pub(super) primary: Option<Recorded>,
    pub(super) refusal: Option<Recorded>,
}
impl Journal {
    fn validate(&self, record: &Record) -> Result<(), CodecError> {
        if let Some(primary) = &self.primary {
            let denied = if record.kind == Kind::Policy {
                matches!(
                    primary.decode_reply::<RefPolicyReply>()?,
                    RefPolicyReply::Denied(_)
                )
            } else {
                matches!(
                    primary.decode_reply::<RootCompletionReply>()?,
                    RootCompletionReply::Denied(_)
                )
            };
            if denied != primary.rejected {
                return Err(CodecError::Invalid("publication result status differs"));
            }
        }
        if let Some(refusal) = &self.refusal
            && (record.refusal.is_none()
                || !self.refused(record)?
                || self
                    .primary
                    .as_ref()
                    .is_none_or(|primary| primary.sequence >= refusal.sequence)
                || matches!(
                    refusal.decode_reply::<RootCompletionReply>()?,
                    RootCompletionReply::Denied(_)
                ) != refusal.rejected)
        {
            return Err(CodecError::Invalid("invalid durable refusal phase"));
        }
        Ok(())
    }
    pub(super) fn refused(&self, record: &Record) -> Result<bool, CodecError> {
        if record.kind != Kind::Policy {
            return Ok(false);
        }
        Ok(
            match self
                .primary
                .as_ref()
                .map(Recorded::decode_reply::<RefPolicyReply>)
                .transpose()?
            {
                Some(RefPolicyReply::Denied(_)) => true,
                Some(RefPolicyReply::Registered(value)) => !value.valid,
                None => false,
            },
        )
    }
    pub(super) fn may_advance(&self, record: &Record) -> Result<bool, CodecError> {
        self.validate(record)?;
        if self.refusal.is_some() {
            return Ok(false);
        }
        let Some(primary) = &self.primary else {
            return Ok(false);
        };
        Ok(if record.kind == Kind::Policy {
            matches!(primary.decode_reply::<RefPolicyReply>()?, RefPolicyReply::Registered(value) if value.valid)
        } else {
            matches!(
                primary.decode_reply::<RootCompletionReply>()?,
                RootCompletionReply::Denied(_)
            )
        })
    }
}
impl WireValue for Journal {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        if self.primary.is_none() && self.refusal.is_some() {
            return Err(CodecError::Invalid("refusal without page result"));
        }
        e.write_bytes(PHASE_DOMAIN)?;
        e.write_bool(self.primary.is_some())?;
        if let Some(value) = &self.primary {
            value.encode(e)?;
        }
        e.write_bool(self.refusal.is_some())?;
        if let Some(value) = &self.refusal {
            value.encode(e)?;
        }
        Ok(())
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        if d.read_bytes()? != PHASE_DOMAIN {
            return Err(CodecError::Invalid("publication phase purpose"));
        }
        let primary = if d.read_bool()? {
            Some(Recorded::decode(d)?)
        } else {
            None
        };
        let refusal = if d.read_bool()? {
            Some(Recorded::decode(d)?)
        } else {
            None
        };
        let value = Self { primary, refusal };
        value.encode(&mut BoundedEncoder::new(PHASE_BYTES)?)?;
        Ok(value)
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct Frame {
    pub(super) certificate: RootRecoveryCertificate,
    pub(super) journal: Journal,
}
impl WireValue for Frame {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        if !self
            .journal
            .may_advance(&self.certificate.0.data::<Record>()?)?
        {
            return Err(CodecError::Invalid("unsettled predecessor frame"));
        }
        e.write_bytes(FRAME_DOMAIN)?;
        self.certificate.encode(e)?;
        self.journal.encode(e)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        if d.read_bytes()? != FRAME_DOMAIN {
            return Err(CodecError::Invalid("publication predecessor purpose"));
        }
        let value = Self {
            certificate: RootRecoveryCertificate::decode(d)?,
            journal: Journal::decode(d)?,
        };
        value.encode(&mut BoundedEncoder::new(ROOT_BYTES)?)?;
        Ok(value)
    }
}
pub(super) fn journal(bytes: &SqlValue, record: &Record) -> cellule_runtime::Result<Journal> {
    let value = match bytes {
        SqlValue::Null => Journal::default(),
        SqlValue::Blob(bytes) => {
            let mut d = BoundedDecoder::new(bytes, PHASE_BYTES)?;
            let value = Journal::decode(&mut d)?;
            d.finish()?;
            value
        }
        _ => return Err(Error::Command("invalid publication phase row")),
    };
    value.validate(record)?;
    Ok(value)
}
pub(super) fn certificate(bytes: &[u8]) -> Result<RootRecoveryCertificate, CodecError> {
    let mut d = BoundedDecoder::new(bytes, CERTIFICATE_BYTES)?;
    let value = RootRecoveryCertificate::decode(&mut d)?;
    d.finish()?;
    Ok(value)
}
pub(in crate::packs::publication) fn execute<T: WireValue>(
    context: &mut CommandContext<'_, '_>,
    check: &LeaseCheck,
    kind: Kind,
    denied: impl FnOnce(PreparationDenial) -> T,
    action: impl FnOnce(&mut CommandContext<'_, '_>) -> cellule_runtime::Result<CommandResult<T>>,
) -> cellule_runtime::Result<CommandResult<T>> {
    let sets = context.sql(&statement("SELECT recovery,recovery_phase,recovery_phase_revision FROM catalog_leases WHERE incarnation=?1 AND admission_sequence=?2", vec![blob(check.token.owner.incarnation.as_bytes()), number(check.token.attempt)?]))?;
    let (bytes, saved, revision) = match rows(&sets)?.first().map(Vec::as_slice) {
        None | Some([SqlValue::Null, SqlValue::Null, SqlValue::Integer(0)]) => {
            // Existing direct preparation callers are converted at the
            // production cutover. Registered commands use the atomic path.
            return action(context);
        }
        Some([SqlValue::Blob(bytes), saved, revision]) => (bytes, saved, revision),
        _ => return Err(Error::Command("invalid publication phase pin")),
    };
    let certificate = certificate(bytes)?;
    let record = certificate.0.data::<Record>()?;
    let seed = super::super::attestation::seed(&context.sql(&statement(
        "SELECT push_cert_seed FROM repository_identity WHERE singleton=1",
        vec![],
    ))?)?;
    let Some(evidence) = context.mutation_evidence() else {
        return Err(Error::Command(
            "publication phase lacks admitted mutation evidence",
        ));
    };
    if !certificate.0.authenticated(&seed)
        || record.check != *check
        || record.tenant != *context.target().tenant().as_bytes()
        || record.application != *context.target().application().as_bytes()
        || evidence.incarnation() != check.token.owner.incarnation
    {
        return Ok(CommandResult::Rejected(denied(PreparationDenial::Conflict)));
    }
    let mut journal = journal(saved, &record)?;
    let revision = unsigned(revision)?;
    let stamp = Stamp::of(&evidence);
    let refusal = if kind == record.kind && stamp == record.primary {
        false
    } else if kind == Kind::Outcome && record.kind == Kind::Policy && record.refusal == Some(stamp)
    {
        // Do not consume a pre-frozen refusal identity before a known page
        // refusal. An execution error leaves the SDK ledger absent.
        if !journal.refused(&record)? {
            return Err(Error::Command("policy refusal phase is not known"));
        }
        true
    } else {
        return Ok(CommandResult::Rejected(denied(PreparationDenial::Conflict)));
    };
    if if refusal {
        journal.refusal.is_some()
    } else {
        journal.primary.is_some()
    } {
        return Err(Error::Command(
            "recorded publication requires original receipt recovery",
        ));
    }
    if revision != u64::from(refusal) {
        return Err(Error::Command("publication phase revision differs"));
    }
    let (rejected, output) = match action(context)? {
        CommandResult::Success(output) => (false, output),
        CommandResult::Rejected(output) => (true, output),
    };
    let mut e = BoundedEncoder::new(512)?;
    output.encode(&mut e)?;
    let value = Recorded {
        sequence: context.sequence(),
        rejected,
        result: e.finish(),
    };
    if refusal {
        journal.refusal = Some(value);
    } else {
        journal.primary = Some(value);
    }
    journal.validate(&record)?;
    let mut e = BoundedEncoder::new(PHASE_BYTES)?;
    journal.encode(&mut e)?;
    super::super::publish::changed(context.sql(&statement("UPDATE catalog_leases SET recovery_phase=?1,recovery_phase_revision=?2 WHERE incarnation=?3 AND admission_sequence=?4 AND recovery=?5 AND recovery_phase_revision=?6", vec![blob(e.finish()), number(revision+1)?, blob(check.token.owner.incarnation.as_bytes()), number(check.token.attempt)?, blob(bytes), number(revision)?]))?)?;
    // Denials above every domain's first write become a committed phase result.
    // Any later SQL/encoding error rolls back both domain and phase changes.
    Ok(CommandResult::Success(output))
}
pub(in crate::packs::publication) fn normalize_root(
    result: Result<Committed<RootCompletionReply>, InvocationError<RootCompletionReply>>,
) -> Result<Committed<RootCompletionReply>, InvocationError<RootCompletionReply>> {
    match result {
        Ok(value) if matches!(value.output, RootCompletionReply::Denied(_)) => {
            Err(InvocationError::Rejected(Box::new(value)))
        }
        other => other,
    }
}
impl RegisteredRootRecovery {
    pub(super) async fn current_journal(
        &self,
        client: &CellClient,
        store: Option<&ArtifactStore>,
    ) -> Result<Journal, RootRecoveryError> {
        let sql =
            SqlCell::<RepositoryModule>::new(client.clone(), self.evidence().target().clone())?;
        let result = sql.query(None, SqlBatch { statements: vec![
            SqlStatement { sql: "SELECT recovery,recovery_phase FROM catalog_leases WHERE incarnation=?1 AND admission_sequence=?2 UNION ALL SELECT recovery,recovery_phase FROM pushes WHERE id=?3 AND recovery IS NOT NULL AND NOT EXISTS(SELECT 1 FROM catalog_leases WHERE incarnation=?1 AND admission_sequence=?2)".into(), parameters: vec![blob(self.token().owner.incarnation.as_bytes()), number(self.token().attempt)?, blob(self.token().operation)] },
            SqlStatement { sql: "SELECT push_cert_seed FROM repository_identity WHERE singleton=1".into(), parameters: vec![] },
        ] }).await.map_err(|error| RootRecoveryError::Query(Box::new(error)))?;
        let Some([SqlValue::Blob(bytes), saved]) = rows(&result.output)?.first().map(Vec::as_slice)
        else {
            return Err(RootRecoveryError::Context);
        };
        let seed = super::super::attestation::seed(
            result.output.get(1..).ok_or(RootRecoveryError::Context)?,
        )?;
        let current = certificate(bytes)?;
        if !current.0.authenticated(&seed) {
            return Err(RootRecoveryError::Context);
        }
        if current == self.certificate {
            return Ok(journal(saved, &self.record)?);
        }
        // Settled frames retain original page replies/receipts after the head
        // advances. Walk one bounded frame at a time, with strictly decreasing
        // authenticated steps; never materialize the history as a vector.
        let store = store.ok_or(RootRecoveryError::Context)?;
        let mut record = current.0.data::<Record>()?;
        while record.step > self.record.step {
            if record.check != self.record.check
                || record.tenant != self.record.tenant
                || record.application != self.record.application
            {
                return Err(RootRecoveryError::Context);
            }
            let root = record.previous.ok_or(RootRecoveryError::Context)?;
            let frame = root.read::<Frame>(store, ROOT_BYTES).await?;
            if !frame.certificate.0.authenticated(&seed) {
                return Err(RootRecoveryError::Context);
            }
            let previous = frame.certificate.0.data::<Record>()?;
            if previous.step + 1 != record.step || previous.check != self.record.check {
                return Err(RootRecoveryError::Context);
            }
            if frame.certificate == self.certificate {
                return Ok(frame.journal);
            }
            record = previous;
        }
        Err(RootRecoveryError::Context)
    }
    pub(super) async fn settled_frame(
        &self,
        client: &CellClient,
        store: &ArtifactStore,
    ) -> Result<StoredInputRoot, RootRecoveryError> {
        let journal = self.current_journal(client, None).await?;
        if !journal.may_advance(&self.record)? {
            return Err(RootRecoveryError::Context);
        }
        let frame = Frame {
            certificate: self.certificate.clone(),
            journal,
        };
        Ok(
            StoredInputRoot::upload(store, self.token().artifact_operation, &frame, ROOT_BYTES)
                .await?,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn recorded<T: WireValue>(
        sequence: u64,
        rejected: bool,
        reply: T,
    ) -> Result<Recorded, CodecError> {
        let mut e = BoundedEncoder::new(512)?;
        reply.encode(&mut e)?;
        Ok(Recorded {
            sequence,
            rejected,
            result: e.finish(),
        })
    }
    fn policy_record() -> Record {
        let mut value = super::super::tests::record();
        value.kind = Kind::Policy;
        let mut refusal = value.primary;
        refusal.digest[0] ^= 1;
        value.refusal = Some(refusal);
        value
    }
    fn page(valid: bool) -> RefPolicyReply {
        RefPolicyReply::Registered(super::super::super::RefPolicyProgress {
            next: 128,
            total: 257,
            valid,
        })
    }
    #[test]
    fn predecessor_requires_a_known_successful_page_and_preserves_original_result()
    -> Result<(), CodecError> {
        let record = policy_record();
        assert!(!Journal::default().may_advance(&record)?);
        for (reply, rejected, advances) in [
            (page(true), false, true),
            (page(false), false, false),
            (
                RefPolicyReply::Denied(PreparationDenial::Conflict),
                true,
                false,
            ),
        ] {
            let journal = Journal {
                primary: Some(recorded(17, rejected, reply)?),
                refusal: None,
            };
            assert_eq!(journal.may_advance(&record)?, advances);
            assert_eq!(journal.refused(&record)?, !advances);
            let frame = Frame {
                certificate: RootRecoveryCertificate(CertificateEnvelope::seal(&record, &[9; 32])?),
                journal,
            };
            let mut e = BoundedEncoder::new(ROOT_BYTES)?;
            if !advances {
                assert!(frame.encode(&mut e).is_err());
                continue;
            }
            frame.encode(&mut e)?;
            let bytes = e.finish();
            let mut d = BoundedDecoder::new(&bytes, ROOT_BYTES)?;
            assert_eq!(Frame::decode(&mut d)?, frame);
            d.finish()?;
            for end in 0..bytes.len() {
                assert!(
                    Frame::decode(&mut BoundedDecoder::new(&bytes[..end], ROOT_BYTES)?).is_err()
                );
            }
            let mut wrong_purpose = BoundedEncoder::new(ROOT_BYTES)?;
            frame.journal.encode(&mut wrong_purpose)?;
            assert!(
                Frame::decode(&mut BoundedDecoder::new(
                    &wrong_purpose.finish(),
                    ROOT_BYTES
                )?)
                .is_err()
            );
        }
        Ok(())
    }
    #[test]
    fn refusal_requires_a_known_negative_page_and_later_original_sequence() -> Result<(), CodecError>
    {
        let record = policy_record();
        let refusal = recorded(
            18,
            true,
            RootCompletionReply::Denied(PreparationDenial::Stale),
        )?;
        for primary in [None, Some(recorded(17, false, page(true))?)] {
            assert!(
                Journal {
                    primary,
                    refusal: Some(refusal.clone())
                }
                .validate(&record)
                .is_err()
            );
        }
        for reply in [
            page(false),
            RefPolicyReply::Denied(PreparationDenial::Unauthorized),
        ] {
            let primary = recorded(17, matches!(reply, RefPolicyReply::Denied(_)), reply)?;
            let mut journal = Journal {
                primary: Some(primary),
                refusal: Some(refusal.clone()),
            };
            journal.validate(&record)?;
            assert!(!journal.may_advance(&record)?);
            for sequence in [0, 16, 17] {
                journal.refusal.as_mut().unwrap().sequence = sequence;
                assert!(journal.validate(&record).is_err());
            }
            journal.refusal.as_mut().unwrap().sequence = 18;
            journal.refusal.as_mut().unwrap().rejected = false;
            assert!(journal.validate(&record).is_err());
        }
        let malformed = Journal {
            primary: Some(recorded(17, true, page(true))?),
            refusal: None,
        };
        assert!(malformed.validate(&record).is_err());
        for sequence in [0, i64::MAX as u64 + 1] {
            assert!(
                recorded(sequence, false, page(true))?
                    .encode(&mut BoundedEncoder::new(PHASE_BYTES)?)
                    .is_err()
            );
        }
        Ok(())
    }
}
