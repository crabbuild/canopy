//! Original initial-admission knowledge is independent of live input custody.
//! The bounded first receipt shares the logical push row and receipt codec.
use super::{
    certificate::CertificateEnvelope,
    recovery::{Stamp, phase::Recorded},
    sql::*,
    *,
};
use cellule_runtime::{
    CellClient, CellTarget, Committed, InvocationError, MutationIdentity, PendingMutation,
    primitives::sql::SqlCell,
};

const DOMAIN: &[u8] = b"canopy.initial-staging-receipt.v1\0";
#[derive(Debug, thiserror::Error)]
pub enum StagingReceiptError {
    #[error("initial staging receipt binding differs")]
    Context,
    #[error("initial staging receipt encoding failed")]
    Codec(#[from] CodecError),
    #[error("initial staging receipt capability failed")]
    Capability(#[from] Error),
    #[error("initial staging receipt query failed")]
    Query(#[source] Box<InvocationError<Vec<SqlResultSet>>>),
}
#[derive(Clone, Debug, PartialEq, Eq)]
struct Record {
    tenant: [u8; 16],
    application: [u8; 16],
    request: BeginRequest,
    stamp: Stamp,
    result: Recorded,
}
impl Record {
    fn lease(&self) -> Result<StagingLease, CodecError> {
        let StagingReply::Granted(lease) = self.result.decode_reply()? else {
            return Err(CodecError::Invalid(
                "initial staging receipt is not a grant",
            ));
        };
        if self.result.rejected()
            || lease.token.repository != self.request.repository
            || lease.token.operation != self.request.operation
            || lease.token.request_digest != self.request.request_digest
            || lease.token.attempt != self.result.sequence()
            || lease.observed_at_ms < 0
            || lease.expires_at_ms <= lease.observed_at_ms
            || lease.expires_at_ms - lease.observed_at_ms != self.request.lease_ms as i64
        {
            return Err(CodecError::Invalid(
                "initial staging receipt result differs",
            ));
        }
        Ok(*lease)
    }
}
impl WireValue for Record {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        self.lease()?;
        e.write_bytes(DOMAIN)?;
        e.write_bytes(&self.tenant)?;
        e.write_bytes(&self.application)?;
        self.request.encode(e)?;
        self.stamp.encode(e)?;
        self.result.encode(e)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        if d.read_bytes()? != DOMAIN {
            return Err(CodecError::Invalid("initial staging receipt purpose"));
        }
        let value = Self {
            tenant: crate::packs::directory::index::codec::fixed(d)?,
            application: crate::packs::directory::index::codec::fixed(d)?,
            request: BeginRequest::decode(d)?,
            stamp: Stamp::decode(d)?,
            result: Recorded::decode(d)?,
        };
        value.lease()?;
        Ok(value)
    }
}
/// Trusted service knowledge of the first accepted Begin. It grants no upload,
/// write or response permission. A restarted caller must explicitly Claim.
#[derive(Clone)]
pub struct StagingAdmission {
    target: CellTarget,
    record: Record,
}
impl StagingAdmission {
    pub async fn load(
        client: &CellClient,
        target: &CellTarget,
        operation: [u8; 16],
    ) -> Result<Option<Self>, StagingReceiptError> {
        let sql = SqlCell::<RepositoryModule>::new(client.clone(), target.clone())?;
        let result = sql.query(None, SqlBatch { statements: vec![
            SqlStatement { sql: "SELECT actor,request_digest,initial_staging FROM pushes WHERE id=?1".into(), parameters: vec![blob(operation)] },
            SqlStatement { sql: "SELECT push_cert_seed FROM repository_identity WHERE singleton=1".into(), parameters: vec![] },
        ] }).await.map_err(|e| StagingReceiptError::Query(Box::new(e)))?;
        let Some([SqlValue::Text(actor), digest, value]) =
            rows(&result.output)?.first().map(Vec::as_slice)
        else {
            return Ok(None);
        };
        let bytes = match value {
            SqlValue::Null => return Ok(None),
            SqlValue::Blob(bytes) => bytes,
            _ => return Err(StagingReceiptError::Context),
        };
        let mut d = BoundedDecoder::new(bytes, CERTIFICATE_BYTES)?;
        let envelope = CertificateEnvelope::decode(&mut d)?;
        d.finish()?;
        let seed = attestation::seed(result.output.get(1..).ok_or(StagingReceiptError::Context)?)?;
        if !envelope.authenticated(&seed) {
            return Err(StagingReceiptError::Context);
        }
        let record: Record = envelope.data()?;
        if record.request.actor != *actor
            || record.request.request_digest != fixed::<32>(digest)?
            || record.request.operation != operation
            || record.tenant != *target.tenant().as_bytes()
            || record.application != *target.application().as_bytes()
            || crate::repository_target(
                target.tenant(),
                target.application(),
                record.request.repository,
            )? != *target
        {
            return Err(StagingReceiptError::Context);
        }
        Ok(Some(Self {
            target: target.clone(),
            record,
        }))
    }
    pub fn lease(&self) -> StagingLease {
        self.record
            .lease()
            .expect("authenticated initial staging receipt")
    }
    pub fn receipt(&self) -> cellule_runtime::Receipt {
        cellule_runtime::Receipt {
            cell: self.target.cell_id(),
            incarnation: self.lease().token.owner.incarnation,
            commit_sequence: self.record.result.sequence(),
        }
    }
    pub async fn ready_claim(
        &self,
        client: CellClient,
        lease_ms: u64,
        identity: MutationIdentity,
    ) -> Result<ReadyStaging, StagingError> {
        ReadyStaging::claim(
            client,
            self.target.clone(),
            LeaseRequest {
                check: LeaseCheck {
                    token: self.lease().token,
                    actor: self.record.request.actor.clone(),
                },
                lease_ms,
            },
            identity,
        )
        .await
    }
    pub(super) fn original(
        &self,
        evidence: &PendingMutation,
    ) -> Result<Option<Committed<StagingReply>>, StagingReceiptError> {
        if self.record.stamp != Stamp::of(evidence) {
            return Ok(None);
        }
        if evidence.target() != &self.target
            || evidence.incarnation() != self.lease().token.owner.incarnation
        {
            return Err(StagingReceiptError::Context);
        }
        Ok(Some(self.record.result.committed(evidence)?))
    }
}
/// Called after admission writes. An encoding/SQL failure aborts admission and
/// its SDK receipt together. This never replaces an earlier logical receipt.
pub(super) fn save(
    context: &CommandContext<'_, '_>,
    request: &BeginRequest,
    lease: StagingLease,
) -> cellule_runtime::Result<()> {
    let evidence = context
        .mutation_evidence()
        .ok_or(Error::Command("initial staging evidence missing"))?;
    let mut output = BoundedEncoder::new(512)?;
    StagingReply::Granted(Box::new(lease)).encode(&mut output)?;
    let record = Record {
        tenant: *evidence.target().tenant().as_bytes(),
        application: *evidence.target().application().as_bytes(),
        request: request.clone(),
        stamp: Stamp::of(&evidence),
        result: Recorded::new(context.sequence(), false, output.finish())?,
    };
    let seed = attestation::seed(&context.sql(&statement(
        "SELECT push_cert_seed FROM repository_identity WHERE singleton=1",
        vec![],
    ))?)?;
    let envelope = CertificateEnvelope::seal(&record, &seed)?;
    let mut encoded = BoundedEncoder::new(CERTIFICATE_BYTES)?;
    envelope.encode(&mut encoded)?;
    let existing = context.sql(&statement(
        "SELECT actor,request_digest,initial_staging FROM pushes WHERE id=?1",
        vec![blob(request.operation)],
    ))?;
    let saved = if let Some(row) = rows(&existing)?.first() {
        let [SqlValue::Text(actor), digest, original] = row.as_slice() else {
            return Err(Error::Command("invalid initial staging push row"));
        };
        if *actor != request.actor || fixed::<32>(digest)? != request.request_digest {
            return Err(Error::Command("initial staging push identity differs"));
        }
        if *original != SqlValue::Null {
            return Ok(());
        }
        statement(
            "UPDATE pushes SET initial_staging=?1 WHERE id=?2 AND initial_staging IS NULL AND response_id IS NULL",
            vec![blob(encoded.finish()), blob(request.operation)],
        )
    } else {
        statement(
            "INSERT INTO pushes(id,actor,request_digest,initial_staging) VALUES(?1,?2,?3,?4)",
            vec![
                blob(request.operation),
                SqlValue::Text(request.actor.clone()),
                blob(request.request_digest),
                blob(encoded.finish()),
            ],
        )
    };
    publish::changed(context.sql(&saved)?)?;
    Ok(())
}

/// Only original authenticated admission knowledge can recreate a missing
/// staging operation. Completed logical outcomes still refuse recreation.
pub(super) fn restart_matches(
    context: &CommandContext<'_, '_>,
    check: &LeaseCheck,
) -> cellule_runtime::Result<bool> {
    let sets = context.sql(&statement(
        "SELECT actor,request_digest,initial_staging FROM pushes WHERE id=?1",
        vec![blob(check.token.operation)],
    ))?;
    let Some([SqlValue::Text(actor), digest, SqlValue::Blob(bytes)]) =
        rows(&sets)?.first().map(Vec::as_slice)
    else {
        return Ok(false);
    };
    if *actor != check.actor || fixed::<32>(digest)? != check.token.request_digest {
        return Ok(false);
    }
    let seed = attestation::seed(&context.sql(&statement(
        "SELECT push_cert_seed FROM repository_identity WHERE singleton=1",
        vec![],
    ))?)?;
    let mut d = BoundedDecoder::new(bytes, CERTIFICATE_BYTES)?;
    let envelope = CertificateEnvelope::decode(&mut d)?;
    d.finish()?;
    if !envelope.authenticated(&seed) {
        return Err(Error::Command(
            "initial staging receipt authentication failed",
        ));
    }
    let record: Record = envelope.data()?;
    let evidence = context
        .mutation_evidence()
        .ok_or(Error::Command("staging Claim evidence missing"))?;
    Ok(record.request.actor == check.actor
        && record.lease()?.token == check.token
        && record.tenant == *evidence.target().tenant().as_bytes()
        && record.application == *evidence.target().application().as_bytes())
}
