//! Durable request identity and replayable Git responses.

use std::{
    error::Error as StdError,
    time::{SystemTime, UNIX_EPOCH},
};

use cellule_runtime::{
    CellModule, Command, Error, InvocationError, MutationIdentity, codec::BoundedDecoder,
    codec::BoundedEncoder, codec::CodecError, codec::WireValue, identity::RequestId,
    primitives::sql::SqlBatch, primitives::sql::SqlStatement, primitives::sql::SqlValue,
    registry::CommandContext, registry::CommandResult,
};

use crate::access::READ_ACCESS;
use crate::{
    PushPlan, RepositoryCell, RepositoryModule, git_http::GitHttpResponse, refs::apply_refs,
};

/// One completed push's authenticated, durable audit annotation.
#[derive(Debug, serde::Serialize)]
pub struct PushReceipt {
    pub id: String,
    pub actor: String,
    pub options: Vec<String>,
    pub certificate: Option<PushCertificateReceipt>,
}

pub(crate) fn valid_options(options: &[String]) -> bool {
    options.len() <= 16
        && options.iter().all(|option| {
            option.strip_prefix("canopy.note=").is_some_and(|note| {
                !note.is_empty()
                    && option.len() <= 1024
                    && option.bytes().all(|byte| (0x20..=0x7e).contains(&byte))
            })
        })
}

mod certificate;
mod plan;
pub use certificate::PushCertificateReceipt;
pub(crate) use certificate::VerifiedPushCertificate;
use certificate::{CertificateMeta, certificate_complete};
pub(crate) mod report;
use plan::StagedPlan;

const CHUNK_BYTES: usize = 512 * 1024;
const MAX_RESPONSE_BYTES: usize = 64 * 1024 * 1024;

#[derive(Debug, thiserror::Error)]
pub enum PushError {
    #[error("push Cell operation failed")]
    Cell(#[source] Box<dyn StdError + Send + Sync>),
    #[error("push ID is already bound to another request or account")]
    Conflict,
    #[error("stored push response is incomplete or corrupt")]
    InvalidResponse,
    #[error("push ref plan is outside supported bounds")]
    InvalidPlan,
    #[error("push response headers cannot be encoded")]
    Headers(#[from] serde_json::Error),
    #[error("system clock cannot create a push mutation identity")]
    Clock,
}

fn cell(error: impl StdError + Send + Sync + 'static) -> PushError {
    PushError::Cell(Box::new(error))
}

impl RepositoryCell {
    /// Reads completed push options for its author or current repository owner.
    pub async fn push_receipt(
        &self,
        actor: &str,
        id: [u8; 16],
    ) -> Result<Option<PushReceipt>, PushError> {
        crate::directory::validate_component(actor).map_err(cell)?;
        let result = self.sql.query(None, SqlBatch { statements: vec![SqlStatement {
            sql: format!("SELECT p.actor, p.options, c.digest, c.signer, c.key, c.recorded_at_ms FROM pushes p LEFT JOIN push_certificates c ON c.push_id = p.id WHERE p.id = ?2 AND p.response_id IS NOT NULL AND (p.actor = ?1 OR EXISTS (SELECT 1 FROM repository_identity WHERE owner = ?1)) AND ({READ_ACCESS})"),
            parameters: vec![SqlValue::Text(actor.into()), SqlValue::Blob(id.to_vec())],
        }]}).await.map_err(cell)?;
        let set = result.output.first().ok_or(PushError::InvalidResponse)?;
        let Some(row) = set.rows.first() else {
            return Ok(None);
        };
        let [
            SqlValue::Text(author),
            SqlValue::Text(options),
            digest,
            signer,
            key,
            recorded_at_ms,
        ] = row.as_slice()
        else {
            return Err(PushError::InvalidResponse);
        };
        let options: Vec<String> =
            serde_json::from_str(options).map_err(|_| PushError::InvalidResponse)?;
        if !valid_options(&options) {
            return Err(PushError::InvalidResponse);
        }
        let certificate = match (digest, signer, key, recorded_at_ms) {
            (SqlValue::Null, SqlValue::Null, SqlValue::Null, SqlValue::Null) => None,
            (
                SqlValue::Blob(digest),
                SqlValue::Text(signer),
                SqlValue::Text(key),
                SqlValue::Integer(recorded_at_ms),
            ) if digest.len() == 32 && *recorded_at_ms >= 0 => Some(PushCertificateReceipt {
                sha256: hex::encode(digest),
                signer: signer.clone(),
                key: key.clone(),
                recorded_at_ms: *recorded_at_ms,
            }),
            _ => return Err(PushError::InvalidResponse),
        };
        Ok(Some(PushReceipt {
            id: uuid::Uuid::from_bytes(id).to_string(),
            actor: author.clone(),
            options,
            certificate,
        }))
    }

    pub(crate) async fn completed_response(
        &self,
        push_id: [u8; 16],
    ) -> Result<GitHttpResponse, PushError> {
        let result = self
            .sql
            .query(
                None,
                SqlBatch {
                    statements: vec![SqlStatement {
                        sql: "SELECT response_id, rejected, rejection_reason FROM pushes WHERE id = ?1".into(),
                        parameters: vec![SqlValue::Blob(push_id.to_vec())],
                    }],
                },
            )
            .await
            .map_err(cell)?;
        let Some([SqlValue::Blob(id), SqlValue::Integer(rejected), reason]) = result
            .output
            .first()
            .and_then(|set| set.rows.first())
            .map(Vec::as_slice)
        else {
            return Err(PushError::InvalidResponse);
        };
        let response = self
            .push_response(
                id.as_slice()
                    .try_into()
                    .map_err(|_| PushError::InvalidResponse)?,
            )
            .await?;
        match (rejected, reason) {
            (0, SqlValue::Null) => Ok(response),
            (1, SqlValue::Null) => report::rejected_report(&response, report::REJECTED),
            (1, SqlValue::Text(reason)) => report::rejected_report(&response, reason),
            _ => Err(PushError::InvalidResponse),
        }
    }

    pub(crate) async fn begin_push(
        &self,
        id: [u8; 16],
        actor: &str,
        digest: [u8; 32],
    ) -> Result<bool, PushError> {
        let result = self.sql.batch(identity()?, SqlBatch { statements: vec![
            SqlStatement {
                sql: "INSERT INTO pushes (id, actor, request_digest) VALUES (?1, ?2, ?3) ON CONFLICT(id) DO NOTHING".into(),
                parameters: vec![SqlValue::Blob(id.to_vec()), SqlValue::Text(actor.into()), SqlValue::Blob(digest.to_vec())],
            },
            SqlStatement {
                sql: "SELECT actor, request_digest, response_id FROM pushes WHERE id = ?1".into(),
                parameters: vec![SqlValue::Blob(id.to_vec())],
            },
        ]}).await.map_err(cell)?;
        let row = result
            .output
            .get(1)
            .and_then(|set| set.rows.first())
            .ok_or(PushError::InvalidResponse)?;
        let [
            SqlValue::Text(stored_actor),
            SqlValue::Blob(stored_digest),
            response,
        ] = row.as_slice()
        else {
            return Err(PushError::InvalidResponse);
        };
        if stored_actor != actor || stored_digest.as_slice() != digest {
            return Err(PushError::Conflict);
        }
        match response {
            SqlValue::Null => Ok(false),
            SqlValue::Blob(id) if id.len() == 16 => Ok(true),
            _ => Err(PushError::InvalidResponse),
        }
    }

    pub(crate) async fn stage_push_response(
        &self,
        push_id: [u8; 16],
        response: &GitHttpResponse,
    ) -> Result<[u8; 16], PushError> {
        if response.body.len() > MAX_RESPONSE_BYTES {
            return Err(PushError::InvalidResponse);
        }
        let id = uuid::Uuid::new_v4().into_bytes();
        let headers = serde_json::to_string(&response.headers)?;
        if headers.len() > 64 * 1024 {
            return Err(PushError::InvalidResponse);
        }
        let mut statements = vec![SqlStatement {
            sql: "INSERT INTO push_responses (id, push_id, status, headers, size, digest) VALUES (?1, ?2, ?3, ?4, ?5, ?6)".into(),
            parameters: vec![SqlValue::Blob(id.to_vec()), SqlValue::Blob(push_id.to_vec()), SqlValue::Integer(i64::from(response.status)), SqlValue::Text(headers), SqlValue::Integer(response.body.len() as i64), SqlValue::Blob(blake3::hash(&response.body).as_bytes().to_vec())],
        }];
        for (part, body) in response.body.chunks(CHUNK_BYTES).enumerate() {
            statements.push(SqlStatement {
                sql:
                    "INSERT INTO push_response_chunks (response_id, part, body) VALUES (?1, ?2, ?3)"
                        .into(),
                parameters: vec![
                    SqlValue::Blob(id.to_vec()),
                    SqlValue::Integer(part as i64),
                    SqlValue::Blob(body.to_vec()),
                ],
            });
            self.sql
                .batch(
                    identity()?,
                    SqlBatch {
                        statements: std::mem::take(&mut statements),
                    },
                )
                .await
                .map_err(cell)?;
        }
        if !statements.is_empty() {
            self.sql
                .batch(identity()?, SqlBatch { statements })
                .await
                .map_err(cell)?;
        }
        Ok(id)
    }

    async fn push_response(&self, id: [u8; 16]) -> Result<GitHttpResponse, PushError> {
        let result =
            self.sql
                .query(
                    None,
                    SqlBatch {
                        statements: vec![SqlStatement {
            sql: "SELECT status, headers, size, digest FROM push_responses WHERE id = ?1".into(),
            parameters: vec![SqlValue::Blob(id.to_vec())],
        }],
                    },
                )
                .await
                .map_err(cell)?;
        let row = result
            .output
            .first()
            .and_then(|set| set.rows.first())
            .ok_or(PushError::InvalidResponse)?;
        let [
            SqlValue::Integer(status),
            SqlValue::Text(headers),
            SqlValue::Integer(size),
            SqlValue::Blob(digest),
        ] = row.as_slice()
        else {
            return Err(PushError::InvalidResponse);
        };
        let size = usize::try_from(*size).map_err(|_| PushError::InvalidResponse)?;
        if size > MAX_RESPONSE_BYTES {
            return Err(PushError::InvalidResponse);
        }
        let mut body = Vec::with_capacity(size);
        for part in 0..size.div_ceil(CHUNK_BYTES) {
            let result = self.sql.query(None, SqlBatch { statements: vec![SqlStatement {
                sql: "SELECT body FROM push_response_chunks WHERE response_id = ?1 AND part = ?2".into(),
                parameters: vec![SqlValue::Blob(id.to_vec()), SqlValue::Integer(part as i64)],
            }]}).await.map_err(cell)?;
            let row = result
                .output
                .first()
                .and_then(|set| set.rows.first())
                .ok_or(PushError::InvalidResponse)?;
            let [SqlValue::Blob(chunk)] = row.as_slice() else {
                return Err(PushError::InvalidResponse);
            };
            if body.len() + chunk.len() > size {
                return Err(PushError::InvalidResponse);
            }
            body.extend_from_slice(chunk);
        }
        if body.len() != size || blake3::hash(&body).as_bytes().as_slice() != digest {
            return Err(PushError::InvalidResponse);
        }
        Ok(GitHttpResponse {
            status: u16::try_from(*status).map_err(|_| PushError::InvalidResponse)?,
            headers: serde_json::from_str(headers)?,
            body,
        })
    }

    pub(crate) async fn complete_push(
        &self,
        input: PushCompletion,
    ) -> Result<cellule_runtime::Committed<bool>, InvocationError<bool>> {
        if !valid_options(&input.options) {
            return Err(InvocationError::NotStarted(Error::Command(
                "invalid push options",
            )));
        }
        let certificate = if let Some(certificate) = &input.certificate {
            Some(self.stage_push_certificate(input.id, certificate).await?)
        } else {
            None
        };
        if let Some(plan) = &input.plan {
            self.prepare_graph(plan).await?;
            self.prepare_branch_proofs(plan).await?;
        }
        let plan = match &input.plan {
            Some(plan) => {
                if plan.actor != input.actor {
                    return Err(InvocationError::NotStarted(Error::Command(
                        "push plan actor mismatch",
                    )));
                }
                Some(
                    self.stage_push_plan(input.response_id, plan)
                        .await
                        .map_err(|source| {
                            InvocationError::NotStarted(Error::Facility {
                                name: "push ref staging",
                                source: Box::new(source),
                            })
                        })?,
                )
            }
            None => None,
        };
        let input = CompletePushInput {
            id: input.id,
            actor: input.actor,
            digest: input.digest,
            response_id: input.response_id,
            options: input.options,
            plan,
            certificate,
        };
        let identity = identity().map_err(|_| {
            InvocationError::NotStarted(Error::Command("push mutation clock failed"))
        })?;
        self.application
            .command::<CompletePush>(&self.target, identity, input)
            .await
    }
}

pub(crate) struct PushCompletion {
    pub id: [u8; 16],
    pub actor: String,
    pub digest: [u8; 32],
    pub response_id: [u8; 16],
    pub options: Vec<String>,
    pub plan: Option<PushPlan>,
    pub certificate: Option<VerifiedPushCertificate>,
}

pub(crate) struct CompletePushInput {
    id: [u8; 16],
    actor: String,
    digest: [u8; 32],
    response_id: [u8; 16],
    options: Vec<String>,
    plan: Option<StagedPlan>,
    certificate: Option<CertificateMeta>,
}

impl WireValue for CompletePushInput {
    fn encode(&self, encoder: &mut BoundedEncoder) -> Result<(), CodecError> {
        encoder.write_bytes(&self.id)?;
        encoder.write_text(&self.actor)?;
        encoder.write_bytes(&self.digest)?;
        encoder.write_bytes(&self.response_id)?;
        encoder.write_bytes(
            &serde_json::to_vec(&self.options).map_err(|_| CodecError::Invalid("push options"))?,
        )?;
        encoder.write_bool(self.plan.is_some())?;
        if let Some(plan) = &self.plan {
            plan.encode(encoder)?;
        }
        encoder.write_bool(self.certificate.is_some())?;
        if let Some(certificate) = &self.certificate {
            encoder.write_bytes(&certificate.digest)?;
            encoder.write_u64(certificate.size as u64)?;
            encoder.write_text(&certificate.signer)?;
            encoder.write_text(&certificate.key)?;
        }
        Ok(())
    }
    fn decode(decoder: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        Ok(Self {
            id: fixed(decoder)?,
            actor: decoder.read_text()?.into(),
            digest: fixed(decoder)?,
            response_id: fixed(decoder)?,
            options: serde_json::from_slice(decoder.read_bytes()?)
                .map_err(|_| CodecError::Invalid("push options"))?,
            plan: if decoder.read_bool()? {
                Some(StagedPlan::decode(decoder)?)
            } else {
                None
            },
            certificate: if decoder.read_bool()? {
                Some(CertificateMeta {
                    digest: fixed(decoder)?,
                    size: i64::try_from(decoder.read_u64()?)
                        .map_err(|_| CodecError::Invalid("invalid certificate size"))?,
                    signer: decoder.read_text()?.into(),
                    key: decoder.read_text()?.into(),
                })
            } else {
                None
            },
        })
    }
}

fn fixed<const N: usize>(decoder: &mut BoundedDecoder<'_>) -> Result<[u8; N], CodecError> {
    decoder
        .read_bytes()?
        .try_into()
        .map_err(|_| CodecError::Invalid("invalid push identity length"))
}

pub(crate) struct CompletePush;

impl Command for CompletePush {
    const MODULE: &'static str = RepositoryModule::NAME;
    const ID: u32 = 4;
    const CODEC_VERSION: u32 = 6;
    type Input = CompletePushInput;
    type Output = bool;

    fn execute(
        context: &mut CommandContext<'_, '_>,
        input: Self::Input,
    ) -> cellule_runtime::Result<CommandResult<bool>> {
        if !valid_options(&input.options) {
            return Ok(CommandResult::Rejected(false));
        }
        let result = context.sql(&SqlBatch { statements: vec![SqlStatement {
            sql: "SELECT response_id FROM pushes WHERE id = ?1 AND actor = ?2 AND request_digest = ?3".into(),
            parameters: vec![SqlValue::Blob(input.id.to_vec()), SqlValue::Text(input.actor.clone()), SqlValue::Blob(input.digest.to_vec())],
        }]})?;
        match result
            .first()
            .and_then(|set| set.rows.first())
            .map(Vec::as_slice)
        {
            Some([SqlValue::Blob(_)]) => return Ok(CommandResult::Success(true)),
            Some([SqlValue::Null]) => {}
            _ => return Ok(CommandResult::Rejected(false)),
        }
        if !response_complete(context, input.id, input.response_id)? {
            return Ok(CommandResult::Rejected(false));
        }
        // The final Cell decision must bind the signed principal to the push actor.
        if let Some(certificate) = &input.certificate
            && (certificate.signer != input.actor
                || !certificate_complete(context, input.id, certificate)?)
        {
            return Ok(CommandResult::Rejected(false));
        }
        let replay = if let Some(certificate) = &input.certificate {
            let rows = context.sql(&SqlBatch {
                statements: vec![SqlStatement {
                    sql: "SELECT push_id FROM push_certificates WHERE digest = ?1".into(),
                    parameters: vec![SqlValue::Blob(certificate.digest.to_vec())],
                }],
            })?;
            !rows
                .first()
                .ok_or(Error::Command("missing certificate replay result"))?
                .rows
                .is_empty()
        } else {
            false
        };
        let rejected = if replay {
            true
        } else if let Some(plan) = &input.plan {
            let plan = plan.load(context, input.response_id, &input.actor)?;
            // apply_refs returns false only before any writes. A policy/CAS/ACL
            // refusal records rejection without publishing refs; SQL failures
            // still roll back the whole completion transaction.
            !apply_refs(context, &plan, None)?
        } else {
            // Native errors and no-op pushes mutate no refs. Record their
            // bound outcome even if write permission was revoked after admission.
            false
        };
        if let Some(certificate) = &input.certificate
            && !replay
        {
            // Keep the exact signed bytes with the decision. A second push ID
            // cannot publish the same certificate, even after ref ABA or takeover.
            context.sql(&SqlBatch { statements: vec![SqlStatement {
                sql: "INSERT INTO push_certificates (digest, push_id, actor, signer, key, size, recorded_at_ms) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)".into(),
                parameters: vec![SqlValue::Blob(certificate.digest.to_vec()), SqlValue::Blob(input.id.to_vec()), SqlValue::Text(input.actor.clone()), SqlValue::Text(certificate.signer.clone()), SqlValue::Text(certificate.key.clone()), SqlValue::Integer(certificate.size), SqlValue::Integer(context.now_ms())],
            }]})?;
        }
        // Publishing the response in the ref transaction makes a lost HTTP reply replayable.
        // Staged chunks from interrupted attempts are never returned as completed outcomes.
        context.sql(&SqlBatch {
            statements: vec![SqlStatement {
                sql: "UPDATE pushes SET response_id = ?1, rejected = ?3, options = ?4, rejection_reason = ?5 WHERE id = ?2 AND response_id IS NULL"
                    .into(),
                parameters: vec![
                    SqlValue::Blob(input.response_id.to_vec()),
                    SqlValue::Blob(input.id.to_vec()),
                    SqlValue::Integer(i64::from(rejected)),
                    SqlValue::Text(serde_json::to_string(&input.options).map_err(|_| Error::Command("invalid push options"))?),
                    if replay { SqlValue::Text("Canopy signed push certificate was already used".into()) } else { SqlValue::Null },
                ],
            }],
        })?;
        // Replays need only the saved response. Reclaim the plan atomically
        // with publication so rollback keeps every staged chunk available.
        context.sql(&SqlBatch {
            statements: vec![SqlStatement {
                sql: "DELETE FROM push_plan_chunks WHERE response_id = ?1".into(),
                parameters: vec![SqlValue::Blob(input.response_id.to_vec())],
            }],
        })?;
        if replay {
            context.sql(&SqlBatch {
                statements: vec![SqlStatement {
                    sql: "DELETE FROM push_certificate_chunks WHERE push_id = ?1".into(),
                    parameters: vec![SqlValue::Blob(input.id.to_vec())],
                }],
            })?;
        }
        Ok(CommandResult::Success(true))
    }
}

fn response_complete(
    context: &CommandContext<'_, '_>,
    push_id: [u8; 16],
    response_id: [u8; 16],
) -> cellule_runtime::Result<bool> {
    let result = context.sql(&SqlBatch { statements: vec![SqlStatement {
        sql: "SELECT r.size, count(c.part), coalesce(sum(length(c.body)), 0), coalesce(min(c.part), 0), coalesce(max(c.part), -1) FROM push_responses r LEFT JOIN push_response_chunks c ON c.response_id = r.id WHERE r.id = ?1 AND r.push_id = ?2 GROUP BY r.id".into(),
        parameters: vec![SqlValue::Blob(response_id.to_vec()), SqlValue::Blob(push_id.to_vec())],
    }]})?;
    let Some(
        [
            SqlValue::Integer(size),
            SqlValue::Integer(count),
            SqlValue::Integer(total),
            SqlValue::Integer(first),
            SqlValue::Integer(last),
        ],
    ) = result
        .first()
        .and_then(|set| set.rows.first())
        .map(Vec::as_slice)
    else {
        return Ok(false);
    };
    let expected = (*size + CHUNK_BYTES as i64 - 1) / CHUNK_BYTES as i64;
    Ok(*count == expected && size == total && *first == 0 && *last == expected - 1)
}

fn identity() -> Result<MutationIdentity, PushError> {
    let now = i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| PushError::Clock)?
            .as_millis(),
    )
    .map_err(|_| PushError::Clock)?;
    Ok(MutationIdentity {
        request_id: RequestId::from_bytes(uuid::Uuid::new_v4().into_bytes()),
        issued_at_ms: now,
        expires_at_ms: now.checked_add(60_000).ok_or(PushError::Clock)?,
    })
}
