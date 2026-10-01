//! Exact native outcome and catalog/ref publication in one durable command.
//! Decoded payloads are untrusted; the privately prepared factory signs their
//! complete binding. Large payloads still need the immutable-root transport.
use super::*;
use super::{
    commands::{check_pin, load, matched},
    publish::{authenticate, changed},
    sql::*,
};
use crate::{
    PushPlan,
    git_http::GitHttpResponse,
    push::{CHUNK_BYTES, MAX_RESPONSE_BYTES, VerifiedPushCertificate},
};
use cellule_ltx::DiskBudget;
use cellule_runtime::{
    CellClient, CellTarget, Committed, InvocationError, MutationIdentity, Receipt,
    primitives::sql::SqlCell,
};
use sha2::{Digest as _, Sha256};
use std::path::Path;
use tokio::time::timeout_at;

const JSON_BYTES: usize = 64 << 10;

/// Input from the native preparation service. Signed bytes can only arrive via
/// the gateway's opaque native-verified witness, not a decoded annotation DTO.
pub struct PushCompletionRequest {
    pub plan: Option<PushPlan>,
    pub response: GitHttpResponse,
    pub options: Vec<String>,
    pub certificate: Option<VerifiedPushCertificate>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CompletionCatalogProof {
    Refs(RefPublicationProof),
    OutcomeOnly(CatalogCertificate),
}
impl CompletionCatalogProof {
    fn certificate(&self) -> &CatalogCertificate {
        match self {
            Self::Refs(proof) => &proof.certificate,
            Self::OutcomeOnly(certificate) => certificate,
        }
    }
    fn refs_digest(&self) -> Result<Option<[u8; 32]>, CodecError> {
        match self {
            Self::Refs(proof) => Ok(Some(super::ref_proof::binding(
                &proof.plan,
                &proof.ancestry,
            )?)),
            Self::OutcomeOnly(_) => Ok(None),
        }
    }
}
/// Transport annotation only. Raw bytes cannot be converted into a trusted
/// native witness; edits invalidate the completion certificate.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SignedPushAnnotation {
    pub body: Vec<u8>,
    pub signer: String,
    pub key: String,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CatalogPushCompletion {
    pub proof: CompletionCatalogProof,
    pub response_id: [u8; 16],
    pub response: GitHttpResponse,
    pub options: Vec<String>,
    pub signed: Option<SignedPushAnnotation>,
}
#[derive(Debug, thiserror::Error)]
pub enum PushCompletionProofError {
    #[error("push completion ref proof failed")]
    Refs(#[from] RefProofError),
    #[error("push completion lease is inactive")]
    Base(#[from] PreparationBaseError),
    #[error("push completion attestation failed")]
    Attestation(#[from] CatalogAttestationError),
    #[error("push completion payload is invalid")]
    Codec(#[from] CodecError),
    #[error("push completion command failed")]
    Command(#[source] Box<InvocationError<CatalogCompletionReply>>),
    #[error("native report does not match the completion plan")]
    Report(#[from] crate::push::PushError),
}
impl PreparedCatalog {
    /// The command is the acknowledgement boundary. Do not put a local lease
    /// timeout around it: cancellation after admission can be an uncertain
    /// outcome, which must retain the logical ID and resolve through replay.
    pub async fn complete_push(
        &self,
        identity: MutationIdentity,
        request: PushCompletionRequest,
        root: &Path,
        budget: DiskBudget,
        limits: crate::packs::metadata::MetadataLimits,
    ) -> Result<Committed<CatalogCompletionReply>, PushCompletionProofError> {
        let input = Box::pin(self.push_completion(request, root, budget, limits)).await?;
        self.ensure_live()?;
        let (client, target, _) = self.base.capability();
        Box::pin(client.command::<CompleteCatalogPush>(target, identity, input))
            .await
            .map_err(|error| PushCompletionProofError::Command(Box::new(error)))
    }

    pub async fn push_completion(
        &self,
        request: PushCompletionRequest,
        root: &Path,
        budget: DiskBudget,
        limits: crate::packs::metadata::MetadataLimits,
    ) -> Result<CatalogPushCompletion, PushCompletionProofError> {
        let (_, deadline) = self.base.live_lease()?;
        timeout_at(deadline, async {
            if request.certificate.as_ref().is_some_and(|certificate| {
                certificate.target != *self.base.capability().1
                    || certificate.request_digest != self.token().request_digest
                    || certificate.signer != self.base.capability().2.actor
            }) {
                return Err(CodecError::Invalid("signed push witness context differs").into());
            }
            crate::push::report::publication_matches(&request.response, request.plan.as_ref())?;
            let checked = match request.plan {
                Some(plan) => Some(self.ref_evidence(plan, root, budget, limits).await?),
                None => None,
            };
            let signed = request.certificate.map(|certificate| SignedPushAnnotation {
                body: certificate.body,
                signer: certificate.signer,
                key: certificate.key,
            });
            if signed
                .as_ref()
                .is_some_and(|certificate| certificate.signer != self.base.capability().2.actor)
            {
                return Err(CodecError::Invalid("signed push actor differs").into());
            }
            let response_id = uuid::Uuid::new_v4().into_bytes();
            let refs_digest = checked
                .as_ref()
                .map(|(plan, bits)| super::ref_proof::binding(plan, bits))
                .transpose()?;
            let binding = payload_binding(
                checked.as_ref().map(|(plan, _)| plan),
                &response_id,
                &request.response,
                &request.options,
                signed.as_ref(),
            )?;
            let certificate = self.issue_certificate(refs_digest, Some(binding)).await?;
            let proof = match checked {
                Some((plan, ancestry)) => CompletionCatalogProof::Refs(RefPublicationProof {
                    plan,
                    ancestry,
                    certificate,
                }),
                None => CompletionCatalogProof::OutcomeOnly(certificate),
            };
            let input = CatalogPushCompletion {
                proof,
                response_id,
                response: request.response,
                options: request.options,
                signed,
            };
            self.ensure_live()?;
            Ok(input)
        })
        .await
        .map_err(|_| PreparationBaseError::Inactive)?
    }
}

#[derive(Debug, thiserror::Error)]
pub enum CatalogPushResponseError {
    #[error("completed push response is incomplete, corrupt or foreign")]
    Invalid,
    #[error("completed push lookup was denied: {0:?}")]
    Denied(PreparationDenial),
    #[error("completed push lookup failed")]
    Lookup(#[source] Box<InvocationError<Option<CatalogCompletionReply>>>),
    #[error("completed push response SQL capability failed")]
    Capability(#[from] Error),
    #[error("completed push response read failed")]
    Query(#[source] Box<InvocationError<Vec<SqlResultSet>>>),
}
impl PreparedCatalog {
    /// Read the exact stored wire outcome at the command's receipt, including
    /// refusals. The logical identity and actor must match this private session.
    /// This intentionally does not invoke the legacy rejection rewriter.
    pub async fn completed_push_response(
        &self,
        completed: &Committed<CatalogCompletionReply>,
    ) -> Result<GitHttpResponse, CatalogPushResponseError> {
        let CatalogCompletionReply::Completed(output) = completed.output else {
            return Err(CatalogPushResponseError::Invalid);
        };
        let (client, target, check) = self.base.capability();
        let request = BeginRequest {
            repository: check.token.repository,
            operation: check.token.operation,
            request_digest: check.token.request_digest,
            actor: check.actor.clone(),
            lease_ms: DEFAULT_LEASE_MS,
        };
        load_response(client, target, &request, completed.receipt, output).await
    }
}
/// Current authorized lookup and exact replay after reconnect/restart. It does
/// not allocate a preparation, namespace or durable command. Owner queries are
/// FIFO behind preceding publication; every subsequent read carries that floor.
pub async fn replay_push_response(
    client: &CellClient,
    target: &CellTarget,
    request: BeginRequest,
    minimum: Option<Receipt>,
) -> Result<Option<GitHttpResponse>, CatalogPushResponseError> {
    if target
        != &crate::repository_target(target.tenant(), target.application(), request.repository)?
    {
        return Err(CatalogPushResponseError::Invalid);
    }
    let found = client
        .query::<CheckCompletedPush>(target, minimum, request.clone())
        .await
        .map_err(|error| CatalogPushResponseError::Lookup(Box::new(error)))?;
    match found.output {
        None => Ok(None),
        Some(CatalogCompletionReply::Denied(reason)) => {
            Err(CatalogPushResponseError::Denied(reason))
        }
        Some(CatalogCompletionReply::Completed(output)) => Ok(Some(
            load_response(client, target, &request, found.receipt, output).await?,
        )),
    }
}
pub(super) async fn load_response(
    client: &CellClient,
    target: &CellTarget,
    request: &BeginRequest,
    minimum: Receipt,
    output: CompletedCatalogPush,
) -> Result<GitHttpResponse, CatalogPushResponseError> {
    let sql = SqlCell::<RepositoryModule>::new(client.clone(), target.clone())?;
    let result = sql.query(Some(minimum), statement(
            "SELECT r.status,r.headers,r.size,r.digest,count(c.part),coalesce(sum(length(c.body)),0),coalesce(min(c.part),0),coalesce(max(c.part),-1) FROM pushes p JOIN push_responses r ON p.response_id=r.id AND r.push_id=p.id LEFT JOIN push_response_chunks c ON c.response_id=r.id WHERE p.id=?1 AND p.actor=?2 AND p.request_digest=?3 AND p.response_id=?4 AND p.completion_digest IS NOT NULL GROUP BY r.id",
            vec![blob(request.operation),SqlValue::Text(request.actor.clone()),blob(request.request_digest),blob(output.response_id)],
        )).await.map_err(|error|CatalogPushResponseError::Query(Box::new(error)))?;
    let Some(
        [
            SqlValue::Integer(status),
            SqlValue::Text(headers),
            size,
            digest,
            count,
            total,
            first,
            last,
        ],
    ) = rows(&result.output)?.first().map(Vec::as_slice)
    else {
        return Err(CatalogPushResponseError::Invalid);
    };
    let size = usize::try_from(unsigned(size)?).map_err(|_| CatalogPushResponseError::Invalid)?;
    let parts = size.div_ceil(CHUNK_BYTES);
    if size > MAX_RESPONSE_BYTES
        || unsigned(count)? != parts as u64
        || unsigned(total)? != size as u64
        || *first != SqlValue::Integer(0)
        || *last != SqlValue::Integer(parts as i64 - 1)
    {
        return Err(CatalogPushResponseError::Invalid);
    }
    let headers: Vec<(String, String)> =
        serde_json::from_str(headers).map_err(|_| CatalogPushResponseError::Invalid)?;
    let digest = fixed::<32>(digest)?;
    let mut body = Vec::with_capacity(size);
    for part in 0..parts {
        let result = sql
            .query(
                Some(minimum),
                statement(
                    "SELECT body FROM push_response_chunks WHERE response_id=?1 AND part=?2",
                    vec![blob(output.response_id), number(part as u64)?],
                ),
            )
            .await
            .map_err(|error| CatalogPushResponseError::Query(Box::new(error)))?;
        let Some([SqlValue::Blob(bytes)]) = rows(&result.output)?.first().map(Vec::as_slice) else {
            return Err(CatalogPushResponseError::Invalid);
        };
        if bytes.len() != (size - body.len()).min(CHUNK_BYTES) {
            return Err(CatalogPushResponseError::Invalid);
        }
        body.extend_from_slice(bytes);
    }
    if body.len() != size || blake3::hash(&body).as_bytes() != &digest {
        return Err(CatalogPushResponseError::Invalid);
    }
    Ok(GitHttpResponse {
        status: u16::try_from(*status).map_err(|_| CatalogPushResponseError::Invalid)?,
        headers,
        body,
    })
}

fn json<T: serde::Serialize>(value: &T) -> Result<Vec<u8>, CodecError> {
    let bytes = serde_json::to_vec(value).map_err(|_| CodecError::Invalid("completion JSON"))?;
    if bytes.len() > JSON_BYTES {
        return Err(CodecError::Invalid("completion JSON size"));
    }
    Ok(bytes)
}
fn validate_payload(
    response: &GitHttpResponse,
    options: &[String],
    signed: Option<&SignedPushAnnotation>,
) -> Result<(), CodecError> {
    if !(100..=599).contains(&response.status)
        || response.body.len() > MAX_RESPONSE_BYTES
        || !crate::push::valid_options(options)
        || response.headers.iter().any(|(name, value)| {
            axum::http::HeaderName::from_bytes(name.as_bytes()).is_err()
                || axum::http::HeaderValue::from_str(value).is_err()
                || (name.eq_ignore_ascii_case("Content-Length")
                    && value.parse::<usize>().ok() != Some(response.body.len()))
        })
        || signed.as_ref().is_some_and(|signed| {
            signed.body.is_empty()
                || signed.body.len() > MAX_RESPONSE_BYTES
                || validate_component(&signed.signer).is_err()
                || signed.key.is_empty()
                || signed.key.len() > 4096
                || signed.key.chars().any(char::is_control)
        })
    {
        return Err(CodecError::Invalid("invalid push completion payload"));
    }
    json(&response.headers)?;
    json(&options)?;
    Ok(())
}
fn payload_binding(
    plan: Option<&PushPlan>,
    response_id: &[u8; 16],
    response: &GitHttpResponse,
    options: &[String],
    signed: Option<&SignedPushAnnotation>,
) -> Result<[u8; 32], CodecError> {
    validate_payload(response, options, signed)?;
    let mut hash = blake3::Hasher::new();
    hash.update(b"canopy.push-completion.v1\0");
    hash.update(&[u8::from(plan.is_some())]);
    if let Some(plan) = plan {
        hash.update(&super::ref_proof::plan_digest(plan)?);
    }
    fn bytes(hash: &mut blake3::Hasher, bytes: &[u8]) -> Result<(), CodecError> {
        let mut size = BoundedEncoder::new(4)?;
        size.write_count(bytes.len())?;
        hash.update(&size.finish());
        hash.update(bytes);
        Ok(())
    }
    bytes(&mut hash, response_id)?;
    hash.update(&u32::from(response.status).to_le_bytes());
    bytes(&mut hash, &json(&response.headers)?)?;
    bytes(&mut hash, &response.body)?;
    bytes(&mut hash, &json(&options)?)?;
    hash.update(&[u8::from(signed.is_some())]);
    if let Some(signed) = signed {
        bytes(&mut hash, signed.signer.as_bytes())?;
        bytes(&mut hash, signed.key.as_bytes())?;
        bytes(&mut hash, &signed.body)?;
    }
    Ok(*hash.finalize().as_bytes())
}
impl CatalogPushCompletion {
    fn validate(&self) -> Result<(), CodecError> {
        validate_payload(&self.response, &self.options, self.signed.as_ref())
    }
    fn binding(&self) -> Result<[u8; 32], CodecError> {
        let plan = match &self.proof {
            CompletionCatalogProof::Refs(proof) => Some(&proof.plan),
            CompletionCatalogProof::OutcomeOnly(_) => None,
        };
        payload_binding(
            plan,
            &self.response_id,
            &self.response,
            &self.options,
            self.signed.as_ref(),
        )
    }
}
impl WireValue for CatalogPushCompletion {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        self.validate()?;
        match &self.proof {
            CompletionCatalogProof::Refs(proof) => {
                e.write_u8(0)?;
                proof.encode(e)?;
            }
            CompletionCatalogProof::OutcomeOnly(certificate) => {
                e.write_u8(1)?;
                certificate.encode(e)?;
            }
        }
        e.write_bytes(&self.response_id)?;
        e.write_u32(u32::from(self.response.status))?;
        e.write_bytes(&json(&self.response.headers)?)?;
        e.write_bytes(&self.response.body)?;
        e.write_bytes(&json(&self.options)?)?;
        e.write_bool(self.signed.is_some())?;
        if let Some(signed) = &self.signed {
            e.write_text(&signed.signer)?;
            e.write_text(&signed.key)?;
            e.write_bytes(&signed.body)?;
        }
        Ok(())
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        let proof = match d.read_u8()? {
            0 => CompletionCatalogProof::Refs(RefPublicationProof::decode(d)?),
            1 => CompletionCatalogProof::OutcomeOnly(CatalogCertificate::decode(d)?),
            _ => return Err(CodecError::Invalid("completion proof kind")),
        };
        let response_id = crate::packs::directory::index::codec::fixed(d)?;
        let status =
            u16::try_from(d.read_u32()?).map_err(|_| CodecError::Invalid("completion status"))?;
        fn decode_json(bytes: &[u8]) -> Result<&[u8], CodecError> {
            if bytes.len() > JSON_BYTES {
                Err(CodecError::Invalid("completion JSON size"))
            } else {
                Ok(bytes)
            }
        }
        let headers = serde_json::from_slice(decode_json(d.read_bytes()?)?)
            .map_err(|_| CodecError::Invalid("completion headers"))?;
        let body = d.read_bytes()?;
        if body.len() > MAX_RESPONSE_BYTES {
            return Err(CodecError::Invalid("completion body size"));
        }
        let body = body.to_vec();
        let options = serde_json::from_slice(decode_json(d.read_bytes()?)?)
            .map_err(|_| CodecError::Invalid("completion options"))?;
        let signed = if d.read_bool()? {
            let signer = d.read_text()?.to_owned();
            let key = d.read_text()?.to_owned();
            let body = d.read_bytes()?;
            if body.len() > MAX_RESPONSE_BYTES {
                return Err(CodecError::Invalid("signed push size"));
            }
            Some(SignedPushAnnotation {
                signer,
                key,
                body: body.to_vec(),
            })
        } else {
            None
        };
        let value = Self {
            proof,
            response_id,
            response: GitHttpResponse {
                status,
                headers,
                body,
            },
            options,
            signed,
        };
        value.validate()?;
        Ok(value)
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CompletedCatalogPush {
    pub response_id: [u8; 16],
    pub rejected: bool,
    pub publication: Option<PublishedRefs>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CatalogCompletionReply {
    Completed(CompletedCatalogPush),
    Denied(PreparationDenial),
}
impl WireValue for CatalogCompletionReply {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        match self {
            Self::Denied(reason) => PreparationReply::Denied(*reason).encode(e),
            Self::Completed(value) => {
                if value.rejected && value.publication.is_some() {
                    return Err(CodecError::Invalid("rejected push published refs"));
                }
                e.write_u8(0)?;
                e.write_bytes(&value.response_id)?;
                e.write_bool(value.rejected)?;
                e.write_bool(value.publication.is_some())?;
                if let Some(publication) = value.publication {
                    publication.encode(e)?;
                }
                Ok(())
            }
        }
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        let value = match d.read_u8()? {
            0 => Self::Completed(CompletedCatalogPush {
                response_id: crate::packs::directory::index::codec::fixed(d)?,
                rejected: d.read_bool()?,
                publication: if d.read_bool()? {
                    Some(PublishedRefs::decode(d)?)
                } else {
                    None
                },
            }),
            1 => Self::Denied(PreparationDenial::Unauthorized),
            2 => Self::Denied(PreparationDenial::Conflict),
            3 => Self::Denied(PreparationDenial::Stale),
            4 => Self::Denied(PreparationDenial::Expired),
            5 => Self::Denied(PreparationDenial::Capacity),
            6 => Self::Denied(PreparationDenial::Missing),
            _ => return Err(CodecError::Invalid("completion reply")),
        };
        value.encode(&mut BoundedEncoder::new(128)?)?;
        Ok(value)
    }
}
fn denied(reason: PreparationDenial) -> CommandResult<CatalogCompletionReply> {
    CommandResult::Rejected(CatalogCompletionReply::Denied(reason))
}
/// Read-only preflight, using the same logical identity as Begin. A completed
/// request must return its saved response before repeating native preparation.
/// lease_ms is encoded by the reused BeginRequest but grants no lease here.
pub struct CheckCompletedPush;
impl Query for CheckCompletedPush {
    const MODULE: &'static str = RepositoryModule::NAME;
    const ID: u32 = 20;
    const CODEC_VERSION: u32 = 1;
    type Input = BeginRequest;
    type Output = Option<CatalogCompletionReply>;
    fn execute(
        context: &mut QueryContext<'_>,
        input: BeginRequest,
    ) -> cellule_runtime::Result<Self::Output> {
        validate_component(&input.actor)?;
        if !decode_access(&context.sql(&SqlBatch {
            statements: vec![access_statement(&input.actor)],
        })?)?
        .is_some_and(|role| role >= TokenScope::Read)
            || identity(
                &context.sql(&statement(IDENTITY, vec![]))?,
                input.repository,
            )?
            .is_none()
        {
            return Ok(Some(CatalogCompletionReply::Denied(
                PreparationDenial::Unauthorized,
            )));
        }
        let saved = context.sql(&statement(
            "SELECT actor,request_digest,response_id,rejected,publication FROM pushes WHERE id=?1",
            vec![blob(input.operation)],
        ))?;
        let Some(row) = rows(&saved)?.first() else {
            return Ok(None);
        };
        let [
            SqlValue::Text(actor),
            digest,
            response,
            rejected,
            publication,
        ] = row.as_slice()
        else {
            return Err(Error::Command("invalid completed push lookup"));
        };
        if *actor != input.actor || fixed::<32>(digest)? != input.request_digest {
            return Ok(Some(CatalogCompletionReply::Denied(
                PreparationDenial::Conflict,
            )));
        }
        let response_id = match response {
            SqlValue::Blob(response) => fixed::<16>(&SqlValue::Blob(response.clone()))?,
            SqlValue::Null if *publication == SqlValue::Null => return Ok(None),
            _ => {
                return Ok(Some(CatalogCompletionReply::Denied(
                    PreparationDenial::Conflict,
                )));
            }
        };
        let SqlValue::Integer(rejected) = rejected else {
            return Err(Error::Command("invalid saved push refusal"));
        };
        let publication = match publication {
            SqlValue::Null => None,
            SqlValue::Blob(bytes) => {
                let mut d = BoundedDecoder::new(bytes, 128)?;
                let value = PublishedRefs::decode(&mut d)?;
                d.finish()?;
                Some(value)
            }
            _ => return Err(Error::Command("invalid saved publication outcome")),
        };
        Ok(Some(CatalogCompletionReply::Completed(
            CompletedCatalogPush {
                response_id,
                rejected: *rejected == 1,
                publication,
            },
        )))
    }
}
pub struct CompleteCatalogPush;
impl Command for CompleteCatalogPush {
    const MODULE: &'static str = RepositoryModule::NAME;
    const ID: u32 = 19;
    const CODEC_VERSION: u32 = 1;
    type Input = CatalogPushCompletion;
    type Output = CatalogCompletionReply;
    fn execute(
        context: &mut CommandContext<'_, '_>,
        input: Self::Input,
    ) -> cellule_runtime::Result<CommandResult<Self::Output>> {
        let completion_digest = input.binding()?;
        let Some((data, key)) = authenticate(
            context,
            input.proof.certificate(),
            input.proof.refs_digest()?,
            Some(completion_digest),
        )?
        else {
            return Ok(denied(PreparationDenial::Unauthorized));
        };
        if input
            .signed
            .as_ref()
            .is_some_and(|certificate| certificate.signer != data.actor)
        {
            return Ok(denied(PreparationDenial::Unauthorized));
        }
        let saved = context.sql(&statement("SELECT actor,request_digest,response_id,completion_digest,rejected,publication FROM pushes WHERE id=?1", vec![blob(data.token.operation)]))?;
        let mut pending = false;
        if let Some(row) = rows(&saved)?.first() {
            let [
                SqlValue::Text(actor),
                digest,
                response,
                completion,
                rejected,
                publication,
            ] = row.as_slice()
            else {
                return Err(Error::Command("invalid completed push identity"));
            };
            if *actor != data.actor || fixed::<32>(digest)? != data.token.request_digest {
                return Ok(denied(PreparationDenial::Conflict));
            }
            match response {
                SqlValue::Blob(response) => {
                    if fixed::<32>(completion)? != completion_digest {
                        return Ok(denied(PreparationDenial::Conflict));
                    }
                    let publication = match publication {
                        SqlValue::Null => None,
                        SqlValue::Blob(bytes) => {
                            let mut d = BoundedDecoder::new(bytes, 128)?;
                            let value = PublishedRefs::decode(&mut d)?;
                            d.finish()?;
                            Some(value)
                        }
                        _ => return Err(Error::Command("invalid saved push publication")),
                    };
                    let SqlValue::Integer(rejected) = rejected else {
                        return Err(Error::Command("invalid saved push refusal"));
                    };
                    return Ok(CommandResult::Success(CatalogCompletionReply::Completed(
                        CompletedCatalogPush {
                            response_id: fixed::<16>(&SqlValue::Blob(response.clone()))?,
                            rejected: *rejected == 1,
                            publication,
                        },
                    )));
                }
                SqlValue::Null if *publication == SqlValue::Null => pending = true,
                _ => return Ok(denied(PreparationDenial::Conflict)),
            }
        }
        if data.token.owner != context.owner_fence() {
            return Ok(denied(PreparationDenial::Stale));
        }
        let Some(row) = load(context, data.token)? else {
            return Ok(denied(PreparationDenial::Missing));
        };
        if !matched(
            &row,
            &LeaseCheck {
                token: data.token,
                actor: data.actor.clone(),
            },
        ) {
            return Ok(denied(PreparationDenial::Stale));
        }
        check_pin(context, &row)?;
        if row.expires <= now(context.now_ms())? {
            return Ok(denied(PreparationDenial::Expired));
        }
        let signed_digest: Option<[u8; 32]> = input
            .signed
            .as_ref()
            .map(|certificate| Sha256::digest(&certificate.body).into());
        let replay = if let Some(digest) = signed_digest {
            !rows(&context.sql(&statement(
                "SELECT push_id FROM push_certificates WHERE digest=?1",
                vec![blob(digest)],
            ))?)?
            .is_empty()
        } else {
            false
        };
        // A moving catalog requires reconciliation, not a permanent client ng.
        if !replay
            && matches!(input.proof, CompletionCatalogProof::Refs(_))
            && (!super::publish::retention_matches(
                context,
                &data,
                row.generation,
                data.catalog.format,
            )? || super::commands::fact(
                context,
                data.token.repository,
                data.catalog.format,
                None,
            )? != data.base)
        {
            return Ok(denied(PreparationDenial::Conflict));
        }
        let mut publication = None;
        let mut rejected = replay;
        if !replay && let CompletionCatalogProof::Refs(proof) = &input.proof {
            match super::publish::publish_authenticated(context, proof, data.clone(), key)? {
                CommandResult::Success(PublicationReply::Published(value)) => {
                    publication = Some(value);
                    pending = true;
                }
                CommandResult::Rejected(PublicationReply::Denied(
                    PreparationDenial::Conflict | PreparationDenial::Unauthorized,
                )) => rejected = true,
                CommandResult::Rejected(PublicationReply::Denied(reason)) => {
                    return Ok(denied(reason));
                }
                _ => return Err(Error::Command("invalid catalog publication decision")),
            }
        }
        let reason = if replay {
            "Canopy signed push certificate was already used"
        } else {
            crate::push::report::REJECTED
        };
        let response = if rejected {
            match crate::push::report::rejected_report(&input.response, reason) {
                Ok(response) => std::borrow::Cow::Owned(response),
                Err(_) if matches!(input.proof, CompletionCatalogProof::OutcomeOnly(_)) => {
                    std::borrow::Cow::Borrowed(&input.response)
                }
                Err(_) => return Err(Error::Command("cannot encode push refusal")),
            }
        } else {
            std::borrow::Cow::Borrowed(&input.response)
        };
        if publication.is_none() && row.expires <= now(context.now_ms())? {
            return Ok(denied(PreparationDenial::Expired));
        }
        // No rejection below this point. Any failure rolls back refs, catalog,
        // signed-certificate ownership, chunks and completed response together.
        if !pending {
            changed(context.sql(&statement(
                "INSERT INTO pushes(id,actor,request_digest) VALUES(?1,?2,?3)",
                vec![
                    blob(data.token.operation),
                    SqlValue::Text(data.actor.clone()),
                    blob(data.token.request_digest),
                ],
            ))?)?;
        }
        if let Some(certificate) = &input.signed
            && !replay
        {
            changed(context.sql(&statement("INSERT INTO push_certificates(digest,push_id,actor,signer,key,size,recorded_at_ms) VALUES(?1,?2,?3,?4,?5,?6,?7)", vec![blob(signed_digest.ok_or(Error::Command("missing signed push digest"))?),blob(data.token.operation),SqlValue::Text(data.actor.clone()),SqlValue::Text(certificate.signer.clone()),SqlValue::Text(certificate.key.clone()),number(certificate.body.len() as u64)?,SqlValue::Integer(context.now_ms())]))?)?;
            for (part, body) in certificate.body.chunks(CHUNK_BYTES).enumerate() {
                changed(context.sql(&statement(
                    "INSERT INTO push_certificate_chunks(push_id,part,body) VALUES(?1,?2,?3)",
                    vec![blob(data.token.operation), number(part as u64)?, blob(body)],
                ))?)?;
            }
        }
        changed(context.sql(&statement("INSERT INTO push_responses(id,push_id,status,headers,size,digest) VALUES(?1,?2,?3,?4,?5,?6)", vec![blob(input.response_id),blob(data.token.operation),SqlValue::Integer(i64::from(response.status)),SqlValue::Text(serde_json::to_string(&response.headers).map_err(|_| Error::Command("invalid push response headers"))?),number(response.body.len() as u64)?,blob(blake3::hash(&response.body).as_bytes())]))?)?;
        for (part, body) in response.body.chunks(CHUNK_BYTES).enumerate() {
            changed(context.sql(&statement(
                "INSERT INTO push_response_chunks(response_id,part,body) VALUES(?1,?2,?3)",
                vec![blob(input.response_id), number(part as u64)?, blob(body)],
            ))?)?;
        }
        changed(context.sql(&statement("UPDATE pushes SET response_id=?1,rejected=?2,options=?3,rejection_reason=?4,completion_digest=?5 WHERE id=?6 AND response_id IS NULL", vec![blob(input.response_id),SqlValue::Integer(i64::from(rejected)),SqlValue::Text(serde_json::to_string(&input.options).map_err(|_| Error::Command("invalid push options"))?),if rejected {SqlValue::Text(reason.into())} else {SqlValue::Null},blob(completion_digest),blob(data.token.operation)]))?)?;
        if publication.is_none() {
            changed(context.sql(&statement(
                "DELETE FROM catalog_operations WHERE id=?1",
                vec![blob(data.token.operation)],
            ))?)?;
        }
        Ok(CommandResult::Success(CatalogCompletionReply::Completed(
            CompletedCatalogPush {
                response_id: input.response_id,
                rejected,
                publication,
            },
        )))
    }
}
