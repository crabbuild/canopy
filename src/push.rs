//! Durable request identity and replayable Git responses.

use std::{
    error::Error as StdError,
    time::{SystemTime, UNIX_EPOCH},
};

use cellule_runtime::{
    BoundedDecoder, BoundedEncoder, CellModule, CodecError, Command, CommandContext, CommandResult,
    Error, InvocationError, MutationIdentity, RequestId, SqlBatch, SqlStatement, SqlValue,
    WireValue,
};

use crate::{
    PushPlan, RepositoryCell, RepositoryModule,
    access::{access_statement, decode_access},
    directory::TokenScope,
    git_http::GitHttpResponse,
    refs::apply_push,
};

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
    #[error("push response headers cannot be encoded")]
    Headers(#[from] serde_json::Error),
    #[error("system clock cannot create a push mutation identity")]
    Clock,
}

fn cell(error: impl StdError + Send + Sync + 'static) -> PushError {
    PushError::Cell(Box::new(error))
}

impl RepositoryCell {
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
                        sql: "SELECT response_id FROM pushes WHERE id = ?1".into(),
                        parameters: vec![SqlValue::Blob(push_id.to_vec())],
                    }],
                },
            )
            .await
            .map_err(cell)?;
        let Some([SqlValue::Blob(id)]) = result
            .output
            .first()
            .and_then(|set| set.rows.first())
            .map(Vec::as_slice)
        else {
            return Err(PushError::InvalidResponse);
        };
        self.push_response(
            id.as_slice()
                .try_into()
                .map_err(|_| PushError::InvalidResponse)?,
        )
        .await
    }

    pub(crate) async fn begin_push(
        &self,
        id: [u8; 16],
        actor: &str,
        digest: [u8; 32],
    ) -> Result<Option<[u8; 16]>, PushError> {
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
            SqlValue::Null => Ok(None),
            SqlValue::Blob(id) => Ok(Some(
                id.as_slice()
                    .try_into()
                    .map_err(|_| PushError::InvalidResponse)?,
            )),
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

    pub(crate) async fn push_response(&self, id: [u8; 16]) -> Result<GitHttpResponse, PushError> {
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
        if let Some(plan) = &input.plan {
            self.prepare_graph(plan).await?;
            self.prepare_branch_proofs(plan).await?;
        }
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
    pub plan: Option<PushPlan>,
}

impl WireValue for PushCompletion {
    fn encode(&self, encoder: &mut BoundedEncoder) -> Result<(), CodecError> {
        encoder.write_bytes(&self.id)?;
        encoder.write_text(&self.actor)?;
        encoder.write_bytes(&self.digest)?;
        encoder.write_bytes(&self.response_id)?;
        encoder.write_bool(self.plan.is_some())?;
        if let Some(plan) = &self.plan {
            plan.encode(encoder)?;
        }
        Ok(())
    }
    fn decode(decoder: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        Ok(Self {
            id: fixed(decoder)?,
            actor: decoder.read_text()?.into(),
            digest: fixed(decoder)?,
            response_id: fixed(decoder)?,
            plan: if decoder.read_bool()? {
                Some(PushPlan::decode(decoder)?)
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
    const CODEC_VERSION: u32 = 1;
    type Input = PushCompletion;
    type Output = bool;

    fn execute(
        context: &mut CommandContext<'_, '_>,
        input: Self::Input,
    ) -> cellule_runtime::Result<CommandResult<bool>> {
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
        let result = context.sql(&SqlBatch { statements: vec![SqlStatement {
            sql: "SELECT r.size, count(c.part), coalesce(sum(length(c.body)), 0), coalesce(min(c.part), 0), coalesce(max(c.part), -1) FROM push_responses r LEFT JOIN push_response_chunks c ON c.response_id = r.id WHERE r.id = ?1 AND r.push_id = ?2 GROUP BY r.id".into(),
            parameters: vec![SqlValue::Blob(input.response_id.to_vec()), SqlValue::Blob(input.id.to_vec())],
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
            return Ok(CommandResult::Rejected(false));
        };
        let expected_count = (*size + CHUNK_BYTES as i64 - 1) / CHUNK_BYTES as i64;
        if *count != expected_count || size != total || *first != 0 || *last != expected_count - 1 {
            return Ok(CommandResult::Rejected(false));
        }
        let allowed = match &input.plan {
            Some(plan) => plan.actor == input.actor && apply_push(context, plan)?,
            None => decode_access(&context.sql(&SqlBatch {
                statements: vec![access_statement(&input.actor)],
            })?)?
            .is_some_and(|role| role >= TokenScope::Write),
        };
        if !allowed {
            return Ok(CommandResult::Rejected(false));
        }
        // Publishing the response in the ref transaction makes a lost HTTP reply replayable.
        // Staged chunks from interrupted attempts are never returned as completed outcomes.
        context.sql(&SqlBatch {
            statements: vec![SqlStatement {
                sql: "UPDATE pushes SET response_id = ?1 WHERE id = ?2 AND response_id IS NULL"
                    .into(),
                parameters: vec![
                    SqlValue::Blob(input.response_id.to_vec()),
                    SqlValue::Blob(input.id.to_vec()),
                ],
            }],
        })?;
        Ok(CommandResult::Success(true))
    }
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
