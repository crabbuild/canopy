//! Durable native merge candidates, separate from branch publication.

use crate::ReadIdentity;

pub(crate) mod command;
use super::merge::{MergeStrategy, oid, policy_state, policy_statement};
use super::*;
use crate::ObjectKind;
use cellule_runtime::{BoundedDecoder, BoundedEncoder, CodecError, CommandContext, WireValue};

/// Immutable preparation intent bound to one pull revision and creator.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CandidateRequest {
    pub id: String,
    pub revision: PullRevision,
    pub strategy: MergeStrategy,
    pub message: String,
}
/// Durable preparation state; only Ready supplies a publishable commit.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
pub enum CandidateResult {
    Pending,
    Ready { oid: String, tree_oid: String },
    Conflicted { paths_base64: Vec<String> },
    Unrelated,
}
/// A frozen candidate intent, creation time and first completed native result.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub struct MergeCandidate {
    #[serde(flatten)]
    pub request: CandidateRequest,
    pub number: i64,
    pub actor: String,
    pub created_at_ms: i64,
    pub result: CandidateResult,
}
impl MergeCandidate {
    /// Returns the immutable Git ref advertised once preparation succeeds.
    pub fn fetch_ref(&self) -> String {
        format!("refs/canopy/merge-candidates/{}", self.request.id)
    }
}
#[derive(Debug, Deserialize, Serialize)]
pub(crate) enum CandidateOutcome {
    Applied(Box<MergeCandidate>),
    NotFound,
    Forbidden,
    Conflict,
}
impl WireValue for CandidateOutcome {
    fn encode(&self, out: &mut BoundedEncoder) -> Result<(), CodecError> {
        out.write_bytes(
            &serde_json::to_vec(self).map_err(|_| CodecError::Invalid("candidate outcome"))?,
        )
    }
    fn decode(input: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        serde_json::from_slice(input.read_bytes()?)
            .map_err(|_| CodecError::Invalid("candidate outcome"))
    }
}
pub(crate) fn valid_request(request: &CandidateRequest) -> bool {
    super::merge::valid_request(&super::merge::MergeRequest {
        id: request.id.clone(),
        revision: request.revision.clone(),
        strategy: request.strategy.clone(),
        candidate_id: Some(request.id.clone()),
    }) && request.strategy != MergeStrategy::FastForward
        && valid_body(&request.message)
        && !request.message.trim().is_empty()
}
pub(crate) fn valid_result(result: &CandidateResult) -> bool {
    use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
    if serde_json::to_vec(result).map_or(true, |bytes| bytes.len() > 256 * 1024) {
        return false;
    }
    match result {
        CandidateResult::Pending => false,
        CandidateResult::Ready { oid, tree_oid } => {
            parse_oid(oid).is_some() && parse_oid(tree_oid).is_some()
        }
        CandidateResult::Unrelated => true,
        CandidateResult::Conflicted { paths_base64 } => paths_base64.iter().all(|path| {
            URL_SAFE_NO_PAD.decode(path).ok().is_some_and(|bytes| {
                !bytes.is_empty() && !bytes.contains(&0) && URL_SAFE_NO_PAD.encode(bytes) == *path
            })
        }),
    }
}

impl RepositoryCell {
    /// Reads an immutable candidate intent and its durable preparation result for a member.
    pub async fn merge_candidate<'a>(
        &self,
        actor: impl Into<ReadIdentity<'a>>,
        number: i64,
        id: &str,
    ) -> Result<Observed<Option<MergeCandidate>>, Invocation> {
        let actor = actor.into();
        actor.validate().map_err(Invocation::NotStarted)?;
        let id = uuid::Uuid::parse_str(id).map_err(|_| invalid("invalid candidate UUID"))?;
        let result=self.sql.query(None,SqlBatch {statements:vec![SqlStatement {
            sql:format!("SELECT binding, pull_number, actor, request, created_ms, result FROM merge_candidates WHERE id = ?3 AND pull_number = ?2 AND ({ACCESS})"),
            parameters:vec![actor.parameter(),SqlValue::Integer(number),SqlValue::Blob(id.as_bytes().to_vec())],
        }]}).await?;
        Ok(Observed {
            output: decode(&result.output)
                .map_err(Invocation::NotStarted)?
                .map(|(_, candidate)| candidate),
            receipt: result.receipt,
        })
    }
    pub(crate) async fn candidate_action(
        &self,
        identity: MutationIdentity,
        action: command::CandidateAction,
    ) -> Result<Committed<CandidateOutcome>, InvocationError<CandidateOutcome>> {
        self.application
            .command::<command::PrepareCandidate>(&self.target, identity, action)
            .await
    }
}
fn query(id: &str) -> cellule_runtime::Result<SqlBatch> {
    let id = uuid::Uuid::parse_str(id).map_err(|_| Error::Command("invalid candidate UUID"))?;
    Ok(SqlBatch {statements:vec![SqlStatement {sql:"SELECT binding, pull_number, actor, request, created_ms, result FROM merge_candidates WHERE id = ?1".into(),parameters:vec![SqlValue::Blob(id.as_bytes().to_vec())]}]})
}
fn decode(sets: &[SqlResultSet]) -> cellule_runtime::Result<Option<(Vec<u8>, MergeCandidate)>> {
    let set = sets
        .first()
        .ok_or(Error::Command("missing candidate result"))?;
    let Some(row) = set.rows.first() else {
        return Ok(None);
    };
    let [
        SqlValue::Blob(binding),
        SqlValue::Integer(number),
        SqlValue::Text(actor),
        SqlValue::Text(request),
        SqlValue::Integer(created),
        SqlValue::Text(result),
    ] = row.as_slice()
    else {
        return Err(Error::Command("invalid candidate row"));
    };
    Ok(Some((
        binding.clone(),
        MergeCandidate {
            request: serde_json::from_str(request).map_err(|source| Error::Facility {
                name: "stored candidate intent",
                source: Box::new(source),
            })?,
            number: *number,
            actor: actor.clone(),
            created_at_ms: *created,
            result: serde_json::from_str(result).map_err(|source| Error::Facility {
                name: "stored candidate result",
                source: Box::new(source),
            })?,
        },
    )))
}

pub(crate) fn commit_body(candidate: &MergeCandidate, tree_oid: &str) -> Vec<u8> {
    let revision = &candidate.request.revision;
    let parents = match candidate.request.strategy {
        MergeStrategy::MergeCommit => format!(
            "parent {}\nparent {}\n",
            revision.base_oid, revision.source_oid
        ),
        MergeStrategy::Squash => format!("parent {}\n", revision.base_oid),
        MergeStrategy::FastForward => String::new(),
    };
    let actor = &candidate.actor;
    let time = candidate.created_at_ms / 1000;
    let message = &candidate.request.message;
    format!("tree {tree_oid}\n{parents}author {actor} <{actor}@users.canopy.invalid> {time} +0000\ncommitter {actor} <{actor}@users.canopy.invalid> {time} +0000\n\n{message}{}",if message.ends_with('\n') {""} else {"\n"}).into_bytes()
}

pub(crate) fn publication_oid(
    context: &CommandContext<'_, '_>,
    number: i64,
    request: &merge::MergeRequest,
) -> cellule_runtime::Result<Option<[u8; 20]>> {
    let Some(id) = request.candidate_id.as_deref() else {
        return Ok(None);
    };
    let Some((_, candidate)) = decode(&context.sql(&query(id)?)?)? else {
        return Ok(None);
    };
    if candidate.number != number
        || candidate.request.revision != request.revision
        || candidate.request.strategy != request.strategy
    {
        return Ok(None);
    }
    let CandidateResult::Ready {
        oid: commit,
        tree_oid,
    } = &candidate.result
    else {
        return Ok(None);
    };
    if !certified(context, &candidate, commit, tree_oid)? {
        return Ok(None);
    }
    Ok(Some(oid(commit)?))
}
fn certified(
    context: &CommandContext<'_, '_>,
    candidate: &MergeCandidate,
    commit: &str,
    tree: &str,
) -> cellule_runtime::Result<bool> {
    // The trusted native worker owns tree-merging semantics. The transaction
    // verifies exact parent order, author, timestamp, message and graph closure.
    let body = commit_body(candidate, tree);
    let commit = oid(commit)?;
    if crate::object_id(ObjectKind::Commit, &body) != commit {
        return Ok(false);
    }
    let rows=context.sql(&SqlBatch {statements:vec![SqlStatement {
        sql:"SELECT o.body FROM objects o JOIN object_closure c ON c.oid = o.oid WHERE o.oid = ?1 AND o.kind = 'commit' AND o.storage = 'inline'".into(),parameters:vec![SqlValue::Blob(commit.to_vec())],
    }]})?;
    Ok(
        matches!(rows.first().and_then(|set|set.rows.first()).map(Vec::as_slice),Some([SqlValue::Blob(stored)]) if *stored == body),
    )
}
