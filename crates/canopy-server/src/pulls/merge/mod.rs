//! Revision-bound review requirements and atomic merge publication.

use crate::ReadIdentity;

pub(crate) mod command;
use super::*;
use crate::{RefExpectation, RefUpdate, directory::TokenScope};

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MergeStrategy {
    FastForward,
    MergeCommit,
    Squash,
    Rebase,
}
/// Retry identity and exact reviewed branches for a selected merge strategy.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MergeRequest {
    pub id: String,
    pub revision: PullRevision,
    pub strategy: MergeStrategy,
    pub candidate_id: Option<String>,
}
/// Durable merge result, replayed unchanged for an exact application request ID.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub struct MergeRecord {
    pub id: String,
    pub number: i64,
    pub oid: String,
    pub merged_at_ms: i64,
    pub revision: PullRevision,
}
pub(crate) fn record(row: &[SqlValue]) -> cellule_runtime::Result<MergeRecord> {
    let [
        SqlValue::Blob(id),
        SqlValue::Integer(number),
        SqlValue::Blob(oid),
        SqlValue::Integer(at),
        revision @ ..,
    ] = row
    else {
        return Err(Error::Command("invalid merge record"));
    };
    if !matches!(oid.len(), 20 | 32) || *number < 1 || *at < 0 {
        return Err(Error::Command("invalid merge result"));
    }
    Ok(MergeRecord {
        id: record_id(id)?,
        number: *number,
        oid: hex::encode(oid),
        merged_at_ms: *at,
        revision: stored_revision(revision)?,
    })
}
/// Current review requirements; this does not assert Git mergeability or check success.
#[derive(Debug, Serialize)]
pub struct ReviewPolicy {
    pub revision: Option<PullRevision>,
    pub ready: bool,
    pub rule_version: i64,
    pub require_pull_request: bool,
    pub required_approvals: i64,
    pub approvals: i64,
    pub changes_requested: bool,
    pub reviews_satisfied: bool,
}
/// The final transaction's domain outcome; only Applied moves refs.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum MergeOutcome {
    Applied { merge: MergeRecord },
    NotFound,
    Forbidden,
    Conflict,
    ReviewsRequired,
    NotFastForward,
    BranchPolicy,
}

// Only the merge command constructs this after checking the exact pull and
// current review policy. It authorizes one ref update, never an arbitrary push.
pub(crate) struct ReviewedMerge {
    update: RefUpdate,
}
impl ReviewedMerge {
    pub(crate) fn update(&self) -> &RefUpdate {
        &self.update
    }
    pub(crate) fn authorizes(&self, update: &RefUpdate) -> bool {
        self.update == *update
    }
}
/// Called only inside the native publisher's authenticated final transaction.
/// This constructs the same exact-update capability as the original merge
/// command; client facts alone cannot satisfy the current review predicate.
pub(crate) fn reviewed_native_update(
    context: &cellule_runtime::registry::CommandContext<'_, '_>,
    input: &command::MergeInput,
    selection: &crate::packs::publication::RefSelection,
    generated: Option<crate::ObjectId>,
) -> cellule_runtime::Result<Result<ReviewedMerge, MergeOutcome>> {
    let statement =
        super::native::with_refs(policy_statement(&input.actor, input.number), selection);
    let Some(state) = policy_state(&context.sql(&SqlBatch {
        statements: vec![statement],
    })?)?
    else {
        return Ok(Err(MergeOutcome::NotFound));
    };
    if !state.policy.ready || state.policy.revision.as_ref() != Some(&input.request.revision) {
        return Ok(Err(MergeOutcome::Conflict));
    }
    if !state.policy.reviews_satisfied {
        return Ok(Err(MergeOutcome::ReviewsRequired));
    }
    Ok(Ok(ReviewedMerge {
        update: RefUpdate {
            name: state.base,
            expected: Some(RefExpectation {
                oid: Some(oid(&input.request.revision.base_oid)?),
                version: input.request.revision.base_version,
            }),
            new_oid: Some(generated.unwrap_or(oid(&input.request.revision.source_oid)?)),
        },
    }))
}
pub(super) struct ReviewState {
    pub(super) policy: ReviewPolicy,
    base: String,
    writable: bool,
}

pub(crate) fn valid_request(request: &MergeRequest) -> bool {
    uuid::Uuid::parse_str(&request.id).ok().is_some_and(|id| {
        id.to_string() == request.id && validate_repository_id(id.into_bytes()).is_ok()
    }) && (1..i64::MAX).contains(&request.revision.pull_version)
        && (1..i64::MAX).contains(&request.revision.source_version)
        && (1..i64::MAX).contains(&request.revision.base_version)
        && parse_oid(&request.revision.source_oid).is_some()
        && parse_oid(&request.revision.base_oid).is_some()
        && match request.strategy {
            MergeStrategy::FastForward => request.candidate_id.is_none(),
            MergeStrategy::MergeCommit | MergeStrategy::Squash | MergeStrategy::Rebase => {
                request.candidate_id.as_ref().is_some_and(|id| {
                    uuid::Uuid::parse_str(id).ok().is_some_and(|value| {
                        value.to_string() == *id
                            && validate_repository_id(value.into_bytes()).is_ok()
                    })
                })
            }
        }
}

impl RepositoryCell {
    /// Reads current review requirements against authenticated native ref facts.
    pub async fn pull_review_policy<'a>(
        &self,
        actor: impl Into<ReadIdentity<'a>>,
        number: i64,
    ) -> Result<Observed<Option<ReviewPolicy>>, super::native::NativePullError> {
        let result = self.pull_review_state(actor.into(), number).await?;
        Ok(Observed {
            output: result.output.map(|state| state.policy),
            receipt: result.receipt,
        })
    }
    async fn pull_review_state(
        &self,
        actor: ReadIdentity<'_>,
        number: i64,
    ) -> Result<Observed<Option<ReviewState>>, super::native::NativePullError> {
        if number < 1 {
            return Err(Error::Command("invalid pull number").into());
        }
        let result = self
            .native_pull_rows(actor, super::native::ReadKind::ReviewPolicy(number))
            .await?;
        Ok(Observed {
            output: result
                .output
                .as_deref()
                .map(policy_state)
                .transpose()?
                .flatten(),
            receipt: result.receipt,
        })
    }
    /// Prepares ancestry facts and atomically publishes an exact reviewed merge.
    ///
    /// Current write authority, reviews, checks and ref versions are rechecked at
    /// publication. Exact request retries preserve the original merge record.
    pub async fn merge_pull(
        &self,
        identity: MutationIdentity,
        actor: &str,
        number: i64,
        request: MergeRequest,
    ) -> Result<Committed<MergeOutcome>, InvocationError<MergeOutcome>> {
        if !valid_request(&request) || number < 1 || validate_component(actor).is_err() {
            return Err(InvocationError::NotStarted(Error::Command(
                "invalid merge request",
            )));
        }
        let state = self
            .pull_review_state(ReadIdentity::Account(actor), number)
            .await
            .map_err(preparation)?
            .output;
        if state.is_some_and(|state| {
            state.writable
                && state.policy.ready
                && state.policy.reviews_satisfied
                && state.policy.revision.as_ref() == Some(&request.revision)
        }) {
            let base = oid(&request.revision.base_oid).map_err(InvocationError::NotStarted)?;
            let source = match &request.candidate_id {
                None => {
                    Some(oid(&request.revision.source_oid).map_err(InvocationError::NotStarted)?)
                }
                Some(id) => self
                    .merge_candidate(actor, number, id)
                    .await
                    .map_err(preparation)?
                    .output
                    .filter(|candidate| {
                        candidate.request.revision == request.revision
                            && candidate.request.strategy == request.strategy
                    })
                    .and_then(|candidate| match candidate.result {
                        super::candidates::CandidateResult::Ready { oid: commit, .. } => {
                            parse_oid(&commit).and_then(|bytes| bytes.try_into().ok())
                        }
                        _ => None,
                    }),
            };
            if let Some(source) = source {
                self.prepare_ancestry(base, source)
                    .await
                    .map_err(preparation)?;
            }
        }
        self.application
            .command::<command::MergePull>(
                &self.target,
                identity,
                command::MergeInput {
                    actor: actor.into(),
                    number,
                    request,
                    issued_at_ms: identity.issued_at_ms,
                },
            )
            .await
    }
}
fn preparation(
    error: impl std::error::Error + Send + Sync + 'static,
) -> InvocationError<MergeOutcome> {
    InvocationError::NotStarted(Error::Facility {
        name: "merge preparation",
        source: Box::new(error),
    })
}
pub(crate) fn oid(text: &str) -> cellule_runtime::Result<crate::ObjectId> {
    parse_oid(text)
        .and_then(|value| value.try_into().ok())
        .ok_or(Error::Command("invalid merge object ID"))
}
pub(crate) fn policy_statement<'a>(
    actor: impl Into<ReadIdentity<'a>>,
    number: i64,
) -> SqlStatement {
    let actor = actor.into();
    // Heads bound this aggregation to one decision per reviewer. Historical
    // retries and comments cannot increase the count or restore old decisions.
    SqlStatement {
        sql: format!(
            "SELECT p.version, p.state, p.draft, p.source_ref, s.oid, s.version, p.base_ref, b.oid, b.version, coalesce(q.version,0), coalesce(q.enabled = 1 AND q.require_pull_request = 1,0), CASE WHEN q.enabled = 1 THEN q.required_approvals ELSE 0 END, (SELECT count(*) FROM pull_review_heads h JOIN pull_reviews r ON r.number = h.review_number WHERE h.pull_number = p.number AND r.kind = 'approve' AND ({APPLICABLE})), EXISTS (SELECT 1 FROM pull_review_heads h JOIN pull_reviews r ON r.number = h.review_number WHERE h.pull_number = p.number AND r.kind = 'request_changes' AND ({APPLICABLE})), ({WRITE}) FROM {JOINS} LEFT JOIN branch_rules q ON q.reference = p.base_ref WHERE p.number = ?2 AND ({ACCESS})"
        ),
        parameters: vec![actor.parameter(), SqlValue::Integer(number)],
    }
}
pub(super) fn policy_state(sets: &[SqlResultSet]) -> cellule_runtime::Result<Option<ReviewState>> {
    let set = sets
        .first()
        .ok_or(Error::Command("missing review policy result"))?;
    let Some(row) = set.rows.first() else {
        return Ok(None);
    };
    let [
        SqlValue::Integer(version),
        SqlValue::Text(state),
        SqlValue::Integer(draft),
        SqlValue::Text(_source),
        source_oid,
        SqlValue::Integer(source_version),
        SqlValue::Text(base),
        base_oid,
        SqlValue::Integer(base_version),
        SqlValue::Integer(rule_version),
        SqlValue::Integer(required),
        SqlValue::Integer(needed),
        SqlValue::Integer(approvals),
        SqlValue::Integer(changes),
        SqlValue::Integer(write),
    ] = row.as_slice()
    else {
        return Err(Error::Command("invalid review policy result"));
    };
    let source_oid = optional_oid(source_oid)?;
    let base_oid = optional_oid(base_oid)?;
    let ready = state == "open"
        && *draft == 0
        && source_oid.is_some()
        && base_oid.is_some()
        && source_oid != base_oid;
    let revision = source_oid
        .zip(base_oid)
        .map(|(source_oid, base_oid)| PullRevision {
            pull_version: *version,
            source_oid,
            source_version: *source_version,
            base_oid,
            base_version: *base_version,
        });
    Ok(Some(ReviewState {
        base: base.clone(),
        writable: *write == 1,
        policy: ReviewPolicy {
            revision,
            ready,
            rule_version: *rule_version,
            require_pull_request: *required == 1,
            required_approvals: *needed,
            approvals: *approvals,
            changes_requested: *changes == 1,
            reviews_satisfied: *required == 0 || (*approvals >= *needed && *changes == 0),
        },
    }))
}

pub(crate) fn candidate_ready(
    sets: &[SqlResultSet],
    revision: &super::PullRevision,
) -> cellule_runtime::Result<Option<bool>> {
    Ok(policy_state(sets)?
        .map(|state| state.policy.ready && state.policy.revision.as_ref() == Some(revision)))
}
