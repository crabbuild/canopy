//! Ref-free native outcomes reuse the admitted session, bounded certificate
//! carrier and exact completion command; they cannot publish catalog or refs.
use super::*;
use super::{
    certificate::CertificateEnvelope,
    commands::{authorized, fact},
    completion::{load_response, payload_binding},
    sql::*,
};
use crate::packs::directory::index::codec::fixed as wire_fixed;
use cellule_runtime::{Committed, MutationIdentity, primitives::sql::SqlCell};
use tokio::time::timeout_at;

const DOMAIN: &[u8] = b"canopy.push-outcome.v1\0";
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OutcomeCertificate(pub(super) CertificateEnvelope);
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct OutcomeData {
    tenant: [u8; 16],
    application: [u8; 16],
    pub(super) check: LeaseCheck,
    pub(super) format: ObjectFormat,
    pub(super) floor: GenerationFact,
    pub(super) digest: [u8; 32],
}
impl OutcomeData {
    fn validate(&self) -> Result<(), CodecError> {
        self.floor.validate()?;
        if self.floor.catalog.is_some_and(|catalog| {
            catalog.repository != self.check.token.repository || catalog.format != self.format
        }) {
            return Err(CodecError::Invalid("outcome retention context"));
        }
        Ok(())
    }
}
impl WireValue for OutcomeData {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        self.validate()?;
        e.write_bytes(DOMAIN)?;
        e.write_bytes(&self.tenant)?;
        e.write_bytes(&self.application)?;
        self.check.encode(e)?;
        e.write_u8(self.format.bytes() as u8)?;
        self.floor.encode(e)?;
        e.write_bytes(&self.digest)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        if d.read_bytes()? != DOMAIN {
            return Err(CodecError::Invalid("outcome certificate domain"));
        }
        let value = Self {
            tenant: wire_fixed(d)?,
            application: wire_fixed(d)?,
            check: LeaseCheck::decode(d)?,
            format: match d.read_u8()? {
                20 => ObjectFormat::Sha1,
                32 => ObjectFormat::Sha256,
                _ => return Err(CodecError::Invalid("outcome format")),
            },
            floor: GenerationFact::decode(d)?,
            digest: wire_fixed(d)?,
        };
        value.validate()?;
        Ok(value)
    }
}
impl WireValue for OutcomeCertificate {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        self.0.data::<OutcomeData>()?;
        self.0.encode(e)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        let value = Self(CertificateEnvelope::decode(d)?);
        value.0.data::<OutcomeData>()?;
        Ok(value)
    }
}
impl PreparationSession {
    /// The session is authoritative but grants no catalog/ref proof. This path
    /// takes no artifact loader, disk budget, native worker or scratch root.
    pub async fn push_outcome(
        &self,
        request: PushCompletionRequest,
    ) -> Result<CatalogPushCompletion, PushCompletionProofError> {
        let (_, deadline) = self.live_lease()?;
        timeout_at(deadline, async {
            if request.plan.is_some() {
                return Err(CodecError::Invalid("outcome-only completion has a ref plan").into());
            }
            let signed = super::completion::signed_annotation(self, request.certificate)?;
            crate::push::report::publication_matches(&request.response, None)?;
            let response_id = uuid::Uuid::new_v4().into_bytes();
            let digest = payload_binding(
                None,
                &response_id,
                &request.response,
                &request.options,
                signed.as_ref(),
            )?;
            let proof = self.issue_outcome_certificate(digest).await?;
            self.live_lease()?;
            Ok(CatalogPushCompletion {
                proof: CompletionCatalogProof::OutcomeOnly(proof),
                response_id,
                response: request.response,
                options: request.options,
                signed,
            })
        })
        .await
        .map_err(|_| PreparationBaseError::Inactive)?
    }
    /// Shared private issuance; callers first certify their specific payload.
    pub(super) async fn issue_outcome_certificate(
        &self,
        digest: [u8; 32],
    ) -> Result<OutcomeCertificate, PushCompletionProofError> {
        let observed = self
            .client
            .query::<CheckPreparation>(&self.target, None, self.check.clone())
            .await
            .map_err(|error| PreparationBaseError::Query(Box::new(error)))?;
        let live = observed.output.ok_or(PreparationBaseError::Inactive)?;
        if live.token != self.lease.token
            || live.base != self.lease.base
            || live.format != self.lease.format
        {
            return Err(PreparationBaseError::Context.into());
        }
        let sql = SqlCell::<RepositoryModule>::new(self.client.clone(), self.target.clone())
            .map_err(CatalogAttestationError::from)?;
        let seed = super::attestation::seed(&sql.query(Some(observed.receipt), statement("SELECT push_cert_seed FROM repository_identity WHERE singleton=1 AND repository_id=?1 AND object_format=?2", vec![blob(live.token.repository), SqlValue::Text(live.format.as_str().into())])).await.map_err(|error| CatalogAttestationError::Query(Box::new(error)))?.output).map_err(CatalogAttestationError::from)?;
        let data = OutcomeData {
            tenant: *self.target.tenant().as_bytes(),
            application: *self.target.application().as_bytes(),
            check: self.check.clone(),
            format: live.format,
            floor: live.base,
            digest,
        };
        let proof = OutcomeCertificate(CertificateEnvelope::seal(&data, &seed)?);
        self.live_lease()?;
        Ok(proof)
    }
    /// Admitted final commands must settle or retain exact uncertainty. A local
    /// lease timeout must never turn a possibly durable response into refusal.
    pub async fn complete_outcome(
        &self,
        identity: MutationIdentity,
        request: PushCompletionRequest,
    ) -> Result<Committed<CatalogCompletionReply>, PushCompletionProofError> {
        let input = self.push_outcome(request).await?;
        self.live_lease()?;
        self.client
            .command::<CompleteCatalogPush>(&self.target, identity, input)
            .await
            .map_err(|error| PushCompletionProofError::Command(Box::new(error)))
    }
    pub async fn completed_push_response(
        &self,
        completed: &Committed<CatalogCompletionReply>,
    ) -> Result<crate::git_http::GitHttpResponse, CatalogPushResponseError> {
        let CatalogCompletionReply::Completed(output) = completed.output else {
            return Err(CatalogPushResponseError::Invalid);
        };
        let request = BeginRequest {
            repository: self.check.token.repository,
            operation: self.check.token.operation,
            request_digest: self.check.token.request_digest,
            actor: self.check.actor.clone(),
            lease_ms: DEFAULT_LEASE_MS,
        };
        load_response(
            &self.client,
            &self.target,
            &request,
            completed.receipt,
            output,
        )
        .await
    }
}
pub(super) fn authenticate(
    context: &CommandContext<'_, '_>,
    certificate: &OutcomeCertificate,
    digest: [u8; 32],
) -> cellule_runtime::Result<Option<(OutcomeData, [u8; 32])>> {
    let data = certificate.0.data::<OutcomeData>()?;
    if data.tenant != *context.target().tenant().as_bytes()
        || data.application != *context.target().application().as_bytes()
        || context.target()
            != &crate::repository_target(
                context.target().tenant(),
                context.target().application(),
                data.check.token.repository,
            )?
    {
        return Ok(None);
    }
    let seed = super::attestation::seed(&context.sql(&statement("SELECT push_cert_seed FROM repository_identity WHERE singleton=1 AND repository_id=?1 AND object_format=?2", vec![blob(data.check.token.repository), SqlValue::Text(data.format.as_str().into())]))?)?;
    if !certificate.0.authenticated(&seed) || data.digest != digest {
        return Ok(None);
    }
    Ok(Some((data, seed)))
}
pub(super) fn current_authority(
    context: &CommandContext<'_, '_>,
    data: &OutcomeData,
    generation: Option<u64>,
) -> cellule_runtime::Result<bool> {
    Ok(authorized(
        context,
        data.check.token.repository,
        &data.check.actor,
        TokenScope::Write,
    )? == Some(data.format)
        && generation == Some(data.floor.generation)
        && fact(
            context,
            data.check.token.repository,
            data.format,
            generation,
        )? == data.floor)
}

pub(super) async fn backup_graph(
    value: &OutcomeCertificate,
    seed: &[u8; 32],
    inventory: &mut crate::packs::backup::Inventory<'_>,
) -> crate::packs::directory::index::WalkResult<()> {
    if !value.0.authenticated(seed) {
        return Err(CodecError::Invalid("backup outcome MAC").into());
    }
    let data: OutcomeData = value.0.data()?;
    super::backup::context(
        data.tenant,
        data.application,
        data.check.token.repository,
        inventory,
    )?;
    if data.format != inventory.format() {
        return Err(CodecError::Invalid("backup outcome format").into());
    }
    super::backup::fact(data.floor, inventory).await
}
