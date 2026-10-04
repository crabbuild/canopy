//! Bounded first-admission knowledge shared by staging and preparation.
//! These authenticated records grant no current custody or product access.
use super::{
    certificate::CertificateEnvelope,
    recovery::{Stamp, phase::Recorded},
    sql::*,
    *,
};
use cellule_runtime::{
    CellClient, CellTarget, Committed, InvocationError, PendingMutation, Receipt,
    primitives::sql::SqlCell,
};
use std::marker::PhantomData;

#[derive(Debug, thiserror::Error)]
pub enum AdmissionReceiptError {
    #[error("initial admission receipt binding differs")]
    Context,
    #[error("initial admission receipt encoding failed")]
    Codec(#[from] CodecError),
    #[error("initial admission receipt capability failed")]
    Capability(#[from] Error),
    #[error("initial admission receipt query failed")]
    Query(#[source] Box<InvocationError<Vec<SqlResultSet>>>),
}

/// Implemented only by the two private admission kinds. A column or purpose
/// cannot be chosen by an external caller.
pub(super) trait Admission: Clone + Send + 'static {
    const DOMAIN: &'static [u8];
    const COLUMN: &'static str;
    type Lease: Clone;
    type Reply: WireValue;
    fn grant(lease: Self::Lease) -> Self::Reply;
    fn lease(result: &Recorded, request: &BeginRequest) -> Result<Self::Lease, CodecError>;
    fn token(lease: &Self::Lease) -> PreparationToken;
}

#[derive(Clone)]
struct Record<A: Admission> {
    tenant: [u8; 16],
    application: [u8; 16],
    request: BeginRequest,
    stamp: Stamp,
    result: Recorded,
    kind: PhantomData<A>,
}
impl<A: Admission> WireValue for Record<A> {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        A::lease(&self.result, &self.request)?;
        e.write_bytes(A::DOMAIN)?;
        e.write_bytes(&self.tenant)?;
        e.write_bytes(&self.application)?;
        self.request.encode(e)?;
        self.stamp.encode(e)?;
        self.result.encode(e)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        if d.read_bytes()? != A::DOMAIN {
            return Err(CodecError::Invalid("initial admission receipt purpose"));
        }
        let value = Self {
            tenant: crate::packs::directory::index::codec::fixed(d)?,
            application: crate::packs::directory::index::codec::fixed(d)?,
            request: BeginRequest::decode(d)?,
            stamp: Stamp::decode(d)?,
            result: Recorded::decode(d)?,
            kind: PhantomData,
        };
        A::lease(&value.result, &value.request)?;
        Ok(value)
    }
}

fn decode<A: Admission>(bytes: &[u8], seed: &[u8; 32]) -> Result<Record<A>, AdmissionReceiptError> {
    let mut d = BoundedDecoder::new(bytes, CERTIFICATE_BYTES)?;
    let envelope = CertificateEnvelope::decode(&mut d)?;
    d.finish()?;
    if !envelope.authenticated(seed) {
        return Err(AdmissionReceiptError::Context);
    }
    Ok(envelope.data()?)
}

#[derive(Clone)]
pub(super) struct InitialAdmission<A: Admission> {
    target: CellTarget,
    record: Record<A>,
}
impl<A: Admission> InitialAdmission<A> {
    pub(super) async fn load(
        client: &CellClient,
        target: &CellTarget,
        operation: [u8; 16],
    ) -> Result<Option<Self>, AdmissionReceiptError> {
        let sql = SqlCell::<RepositoryModule>::new(client.clone(), target.clone())?;
        let result = sql
            .query(
                None,
                SqlBatch {
                    statements: vec![
                        SqlStatement {
                            sql: format!(
                                "SELECT actor,request_digest,{} FROM pushes WHERE id=?1",
                                A::COLUMN
                            ),
                            parameters: vec![blob(operation)],
                        },
                        SqlStatement {
                            sql: "SELECT push_cert_seed FROM repository_identity WHERE singleton=1"
                                .into(),
                            parameters: vec![],
                        },
                    ],
                },
            )
            .await
            .map_err(|e| AdmissionReceiptError::Query(Box::new(e)))?;
        let row = rows(&result.output)?.first().map(Vec::as_slice);
        let Some([SqlValue::Text(actor), digest, value]) = row else {
            return if row.is_none() {
                Ok(None)
            } else {
                Err(AdmissionReceiptError::Context)
            };
        };
        let bytes = match value {
            SqlValue::Null => return Ok(None),
            SqlValue::Blob(bytes) => bytes,
            _ => return Err(AdmissionReceiptError::Context),
        };
        let seed = attestation::seed(
            result
                .output
                .get(1..)
                .ok_or(AdmissionReceiptError::Context)?,
        )?;
        let record: Record<A> = decode(bytes, &seed)?;
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
            return Err(AdmissionReceiptError::Context);
        }
        Ok(Some(Self {
            target: target.clone(),
            record,
        }))
    }
    pub(super) fn request(&self) -> &BeginRequest {
        &self.record.request
    }
    pub(super) fn target(&self) -> &CellTarget {
        &self.target
    }
    pub(super) fn lease(&self) -> A::Lease {
        A::lease(&self.record.result, &self.record.request)
            .expect("authenticated initial admission")
    }
    pub(super) fn receipt(&self) -> Receipt {
        Receipt {
            cell: self.target.cell_id(),
            incarnation: A::token(&self.lease()).owner.incarnation,
            // A Begin observing an already-bound attempt has its own receipt.
            // The attempt sequence is not necessarily this command's sequence.
            commit_sequence: self.record.result.sequence(),
        }
    }
    pub(super) fn original(
        &self,
        evidence: &PendingMutation,
    ) -> Result<Option<Committed<A::Reply>>, AdmissionReceiptError> {
        if self.record.stamp != Stamp::of(evidence) {
            return Ok(None);
        }
        if evidence.target() != &self.target || evidence.incarnation() != self.receipt().incarnation
        {
            return Err(AdmissionReceiptError::Context);
        }
        Ok(Some(self.record.result.committed(evidence)?))
    }
}

/// The receipt is written after domain admission in the same SDK transaction.
/// Failure rolls back namespace allocation, custody and SDK knowledge together.
pub(super) fn save<A: Admission>(
    context: &CommandContext<'_, '_>,
    request: &BeginRequest,
    lease: A::Lease,
) -> cellule_runtime::Result<()> {
    let evidence = context
        .mutation_evidence()
        .ok_or(Error::Command("initial admission evidence missing"))?;
    if A::token(&lease).owner.incarnation != evidence.incarnation() {
        return Err(Error::Command("initial admission incarnation differs"));
    }
    let mut output = BoundedEncoder::new(512)?;
    A::grant(lease).encode(&mut output)?;
    let record = Record::<A> {
        tenant: *evidence.target().tenant().as_bytes(),
        application: *evidence.target().application().as_bytes(),
        request: request.clone(),
        stamp: Stamp::of(&evidence),
        result: Recorded::new(context.sequence(), false, output.finish())?,
        kind: PhantomData,
    };
    let seed = attestation::seed(&context.sql(&statement(
        "SELECT push_cert_seed FROM repository_identity WHERE singleton=1",
        vec![],
    ))?)?;
    let envelope = CertificateEnvelope::seal(&record, &seed)?;
    let mut encoded = BoundedEncoder::new(CERTIFICATE_BYTES)?;
    envelope.encode(&mut encoded)?;
    let existing = context.sql(&statement(
        &format!(
            "SELECT actor,request_digest,{} FROM pushes WHERE id=?1",
            A::COLUMN
        ),
        vec![blob(request.operation)],
    ))?;
    let saved = if let Some(row) = rows(&existing)?.first() {
        let [SqlValue::Text(actor), digest, original] = row.as_slice() else {
            return Err(Error::Command("invalid initial admission request row"));
        };
        if *actor != request.actor || fixed::<32>(digest)? != request.request_digest {
            return Err(Error::Command("initial admission request identity differs"));
        }
        if *original != SqlValue::Null {
            return Ok(());
        }
        statement(
            &format!(
                "UPDATE pushes SET {0}=?1 WHERE id=?2 AND {0} IS NULL AND response_id IS NULL",
                A::COLUMN
            ),
            vec![blob(encoded.finish()), blob(request.operation)],
        )
    } else {
        statement(
            &format!(
                "INSERT INTO pushes(id,actor,request_digest,{}) VALUES(?1,?2,?3,?4)",
                A::COLUMN
            ),
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

pub(super) fn restart_matches<A: Admission>(
    context: &CommandContext<'_, '_>,
    check: &LeaseCheck,
) -> cellule_runtime::Result<bool> {
    let sets = context.sql(&statement(
        &format!(
            "SELECT actor,request_digest,{} FROM pushes WHERE id=?1",
            A::COLUMN
        ),
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
    let record: Record<A> = decode(bytes, &seed)
        .map_err(|_| Error::Command("initial admission receipt authentication failed"))?;
    let evidence = context
        .mutation_evidence()
        .ok_or(Error::Command("Claim evidence missing"))?;
    Ok(record.request.actor == check.actor
        && A::token(&A::lease(&record.result, &record.request)?) == check.token
        && record.tenant == *evidence.target().tenant().as_bytes()
        && record.application == *evidence.target().application().as_bytes())
}
