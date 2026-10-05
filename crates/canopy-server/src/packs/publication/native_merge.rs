//! Private native ref preparation and one owner-fenced reviewed-merge transaction.
//! Transport DTOs grant no authority; the final receiver authenticates every
//! proposed fact before evaluating current editorial and branch predicates.
use super::*;
use super::{
    commands::{check_pin, fact, load, matched},
    publish::{authenticate, changed, checkpoint, retention_matches},
    ref_observation::RefFact,
    sql::*,
};
use crate::{
    PushPlan,
    packs::{
        metadata::MetadataLimits,
        ref_state::{RefSnapshotError, RefStateError, RefStateIndex},
    },
    pulls::merge::{
        MergeOutcome, MergeRecord, MergeStrategy,
        command::{MergeInput, request_binding},
    },
};
use cellule_ltx::DiskBudget;
use cellule_runtime::{InvocationError, primitives::sql::SqlCell};
use std::path::Path;
use tokio::time::timeout_at;

pub(super) mod audit;

pub const NATIVE_MERGE_BYTES: u32 = 256 << 10;

#[derive(Clone, Debug)]
struct Transition {
    plan: PushPlan,
    ancestry: Vec<u8>,
    refs: Option<RefStateSnapshotRoot>,
    ref_generation: Option<u64>,
    audit: Option<crate::packs::input_artifact::StoredInputRoot>,
}

/// Exact request, native ref facts and conditional snapshot. Only a privately
/// constructed PreparedCatalog can issue its MAC. No serving-only observation
/// or client-selected root can create write authority.
#[derive(Clone, Debug)]
pub struct NativeMergeProof {
    certificate: CatalogCertificate,
    input: MergeInput,
    selection: RefSelection,
    transition: Option<Transition>,
}

#[derive(Debug, thiserror::Error)]
pub enum NativeMergePreparationError {
    #[error("native merge audit root failed")]
    Root(#[from] crate::packs::InputRootError),
    #[error("native merge preparation is inactive")]
    Base(#[from] PreparationBaseError),
    #[error("native merge encoding failed")]
    Codec(#[from] CodecError),
    #[error("native merge metadata capability failed")]
    Capability(#[from] Error),
    #[error("native merge editorial query failed")]
    Query(#[source] Box<InvocationError<Vec<SqlResultSet>>>),
    #[error("native merge refs failed")]
    Refs(#[from] RefStateError),
    #[error("native merge snapshot failed")]
    Snapshot(#[from] RefSnapshotError),
    #[error("native merge transition failed")]
    Transition(#[from] RefSnapshotPreparationError),
    #[error("native merge ancestry failed")]
    Ancestry(#[from] RefProofError),
    #[error("native merge attestation failed")]
    Attestation(#[from] CatalogAttestationError),
    #[error("native merge context or strategy differs")]
    Context,
    #[error("native merge command preparation failed")]
    Command(#[source] Box<InvocationError<MergeOutcome>>),
}

impl NativeMergeProof {
    fn shape(&self) -> Result<(), CodecError> {
        let data = self.certificate.data()?;
        self.input.encode(&mut BoundedEncoder::new(4096)?)?;
        if data.compaction
            || data.input_count != 0
            || data.input_checkpoint_digest.is_some()
            || data.base.refs.is_none()
            || data.actor != self.input.actor
            || self.selection.repository != data.token.repository
            || self.selection.actor.as_deref() != Some(&self.input.actor)
            || self.selection.proof.is_some()
            || self.selection.facts.len() > 2
            || self.input.request.strategy != MergeStrategy::FastForward
        {
            return Err(CodecError::Invalid("invalid native merge scope"));
        }
        self.selection
            .encode(&mut BoundedEncoder::new(NATIVE_MERGE_BYTES)?)?;
        if self.selection.facts.iter().any(|f| {
            f.state
                .as_ref()
                .and_then(|s| s.oid)
                .is_some_and(|o| o.format() != data.catalog.format)
        }) {
            return Err(CodecError::Invalid("native merge ref format"));
        }
        if let Some(t) = &self.transition {
            if let Some(audit) = t.audit {
                audit.validate(crate::packs::input_artifact::INPUT_ROOT_BYTES)?;
            }
            super::ref_proof::shape(&t.plan, data.catalog.format)
                .map_err(|_| CodecError::Invalid("native merge plan"))?;
            super::ref_proof::binding(&t.plan, &t.ancestry)?;
            if t.plan.actor != self.input.actor
                || t.plan.updates.len() != 1
                || t.plan.updates[0].new_oid.is_none()
                || t.plan.updates[0]
                    .expected
                    .as_ref()
                    .and_then(|s| s.oid)
                    .is_none()
                || t.refs
                    .is_some_and(|r| r.operation() != data.token.artifact_operation)
                || t.audit
                    .is_some_and(|r| r.operation != data.token.artifact_operation)
                || t.ref_generation.is_some() != t.refs.is_some()
                || t.ref_generation
                    .is_some_and(|g| g == 0 || g > i64::MAX as u64)
                || t.audit.is_some() != t.refs.is_some()
                || t.refs.is_some() != super::ref_proof::proven(&t.ancestry, 0)
            {
                return Err(CodecError::Invalid("invalid native merge transition"));
            }
        }
        Ok(())
    }
    fn binding(&self) -> Result<[u8; 32], CodecError> {
        Self::payload_binding(&self.input, &self.selection, &self.transition)
    }
    fn payload_binding(
        input: &MergeInput,
        selection: &RefSelection,
        transition: &Option<Transition>,
    ) -> Result<[u8; 32], CodecError> {
        let mut e = BoundedEncoder::new(4096)?;
        input.encode(&mut e)?;
        let request = *blake3::hash(&e.finish()).as_bytes();
        let mut h = blake3::Hasher::new();
        h.update(b"canopy.native-reviewed-merge.v1\0");
        h.update(&selection.binding(request)?);
        h.update(&[u8::from(transition.is_some())]);
        if let Some(t) = transition {
            h.update(&super::ref_proof::binding(&t.plan, &t.ancestry)?);
            let mut e = BoundedEncoder::new(256)?;
            t.refs.encode(&mut e)?;
            t.ref_generation.encode(&mut e)?;
            t.audit.encode(&mut e)?;
            h.update(&e.finish());
        }
        Ok(*h.finalize().as_bytes())
    }
}
impl WireValue for NativeMergeProof {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        self.shape()?;
        self.certificate.encode(e)?;
        self.input.encode(e)?;
        self.selection.encode(e)?;
        e.write_bool(self.transition.is_some())?;
        if let Some(t) = &self.transition {
            t.plan.encode(e)?;
            e.write_bytes(&t.ancestry)?;
            t.refs.encode(e)?;
            t.ref_generation.encode(e)?;
            t.audit.encode(e)?;
        }
        Ok(())
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        let value = Self {
            certificate: CatalogCertificate::decode(d)?,
            input: MergeInput::decode(d)?,
            selection: RefSelection::decode(d)?,
            transition: if d.read_bool()? {
                Some(Transition {
                    plan: PushPlan::decode(d)?,
                    ancestry: d.read_bytes()?.to_vec(),
                    refs: Option::<RefStateSnapshotRoot>::decode(d)?,
                    ref_generation: Option::<u64>::decode(d)?,
                    audit: Option::<crate::packs::input_artifact::StoredInputRoot>::decode(d)?,
                })
            } else {
                None
            },
        };
        value.shape()?;
        Ok(value)
    }
}
impl PreparedCatalog {
    pub(crate) async fn native_merge_proof(
        &self,
        input: MergeInput,
        directory: &Path,
        budget: DiskBudget,
        limits: MetadataLimits,
    ) -> Result<NativeMergeProof, NativeMergePreparationError> {
        let (_, deadline) = self.base.live_lease()?;
        timeout_at(
            deadline,
            Box::pin(async {
                input.encode(&mut BoundedEncoder::new(4096)?)?;
                if self.input_count() != 0
                    || self.input_checkpoint_digest.is_some()
                    || input.actor != self.base.capability().2.actor
                    || input.request.strategy != MergeStrategy::FastForward
                {
                    return Err(NativeMergePreparationError::Context);
                }
                let (client, target, _) = self.base.capability();
                let sql = SqlCell::<RepositoryModule>::new(client.clone(), target.clone())?;
                let selected = sql
                    .query(
                        None,
                        statement(
                            "SELECT source_ref,base_ref FROM pull_requests WHERE number=?1",
                            vec![SqlValue::Integer(input.number)],
                        ),
                    )
                    .await
                    .map_err(|e| NativeMergePreparationError::Query(Box::new(e)))?;
                let store = self.base.indexes().store();
                let snapshot = self
                    .base()
                    .refs
                    .ok_or(NativeMergePreparationError::Context)?
                    .read(&store)
                    .await?;
                if snapshot.repository != self.token().repository
                    || snapshot.format != self.catalog().format
                {
                    return Err(NativeMergePreparationError::Context);
                }
                let refs = RefStateIndex::new(store, snapshot.format);
                let mut selection = RefSelection {
                    repository: self.token().repository,
                    actor: Some(input.actor.clone()),
                    facts: Vec::new(),
                    proof: None,
                };
                let names = match rows(&selected.output)?.first().map(Vec::as_slice) {
                    Some([SqlValue::Text(source), SqlValue::Text(base)]) => {
                        Some((source.clone(), base.clone()))
                    }
                    None => None,
                    _ => return Err(NativeMergePreparationError::Context),
                };
                let mut transition = None;
                if let Some((source, base)) = names {
                    let source_state = refs.read(snapshot.root.clone(), &source).await?;
                    let base_state = refs.read(snapshot.root, &base).await?;
                    selection.facts.push(RefFact {
                        name: source.clone(),
                        state: source_state.clone(),
                    });
                    if source != base {
                        selection.facts.push(RefFact {
                            name: base.clone(),
                            state: base_state.clone(),
                        });
                    }
                    selection.facts.sort_by(|a, b| a.name.cmp(&b.name));
                    if let (Some(source_oid), Some(base_state)) =
                        (source_state.and_then(|s| s.oid), base_state)
                        && base_state.oid.is_some()
                        && base_state.oid != Some(source_oid)
                    {
                        let plan = PushPlan {
                            actor: input.actor.clone(),
                            updates: vec![crate::RefUpdate {
                                name: base,
                                expected: Some(base_state),
                                new_oid: Some(source_oid),
                            }],
                        };
                        let (plan, ancestry) = self
                            .required_ref_evidence(plan, directory, budget, limits)
                            .await?;
                        let proposed = if super::ref_proof::proven(&ancestry, 0) {
                            Some(self.prepare_ref_snapshot(&plan).await?.snapshot())
                        } else {
                            None
                        };
                        let (audit, ref_generation) = match proposed {
                            Some(refs) => {
                                let (audit, generation) =
                                    audit::prepare(self, &input, &plan.updates[0].name, refs)
                                        .await?;
                                (Some(audit), Some(generation))
                            }
                            None => (None, None),
                        };
                        transition = Some(Transition {
                            plan,
                            ancestry,
                            refs: proposed,
                            ref_generation,
                            audit,
                        });
                    }
                }
                let binding = NativeMergeProof::payload_binding(&input, &selection, &transition)?;
                let proof = NativeMergeProof {
                    certificate: self.issue_certificate(Some(binding), None).await?,
                    input,
                    selection,
                    transition,
                };
                proof.encode(&mut BoundedEncoder::new(NATIVE_MERGE_BYTES)?)?;
                self.ensure_live()?;
                Ok(proof)
            }),
        )
        .await
        .map_err(|_| PreparationBaseError::Inactive)?
    }
}

pub struct PublishReviewedMerge;
impl Command for PublishReviewedMerge {
    const MODULE: &'static str = RepositoryModule::NAME;
    const ID: u32 = 9;
    const CODEC_VERSION: u32 = 7;
    type Input = NativeMergeProof;
    type Output = MergeOutcome;
    fn execute(
        context: &mut CommandContext<'_, '_>,
        input: Self::Input,
    ) -> cellule_runtime::Result<CommandResult<Self::Output>> {
        let data = input.certificate.data()?;
        let check = LeaseCheck {
            token: data.token,
            actor: data.actor,
        };
        recovery::execute(context, &check, recovery::Kind::Merge, |context| {
            publish(context, input)
        })
    }
}
fn denied(outcome: MergeOutcome) -> CommandResult<MergeOutcome> {
    CommandResult::Rejected(outcome)
}

fn publish(
    context: &mut CommandContext<'_, '_>,
    proof: NativeMergeProof,
) -> cellule_runtime::Result<CommandResult<MergeOutcome>> {
    proof.shape()?;
    let Some((data, key)) =
        authenticate(context, &proof.certificate, Some(proof.binding()?), None)?
    else {
        return Ok(denied(MergeOutcome::Conflict));
    };
    let check = LeaseCheck {
        token: data.token,
        actor: data.actor.clone(),
    };
    let result = publish_authenticated(context, proof, data, key)?;
    // Every authenticated result is terminal for this exact command. Close its
    // own binding atomically with the recovery phase, including denials and
    // application UUID replays. Never close a successor or an unauthenticated
    // proposal. The independent pin remains until typed terminal retirement.
    if check.token.owner == context.owner_fence()
        && let Some(row) = load(context, check.token)?
        && matched(&row, &check)
    {
        check_pin(context, &row)?;
        changed(context.sql(&statement(
            "DELETE FROM catalog_operations WHERE id=?1",
            vec![blob(check.token.operation)],
        ))?)?;
    }
    Ok(result)
}
fn publish_authenticated(
    context: &mut CommandContext<'_, '_>,
    proof: NativeMergeProof,
    data: super::certificate::CertificateData,
    key: [u8; 32],
) -> cellule_runtime::Result<CommandResult<MergeOutcome>> {
    let input = proof.input;
    let role = crate::access::decode_access(&context.sql(&SqlBatch {
        statements: vec![crate::access::access_statement(&input.actor)],
    })?)?;
    let Some(role) = role else {
        return Ok(denied(MergeOutcome::NotFound));
    };
    if role < TokenScope::Write {
        return Ok(denied(MergeOutcome::Forbidden));
    }
    let id = uuid::Uuid::parse_str(&input.request.id)
        .map_err(|_| Error::Command("invalid merge UUID"))?;
    let binding = request_binding(&input);
    let previous = context.sql(&statement(
        "SELECT binding,id,pull_number,oid,merged_ms,pull_version,source_oid,source_version,base_oid,base_version FROM pull_merges WHERE id=?1",
        vec![blob(id.as_bytes())],
    ))?;
    if let Some(row) = rows(&previous)?.first() {
        let Some(SqlValue::Blob(old)) = row.first() else {
            return Err(Error::Command("invalid merge binding"));
        };
        if *old != binding {
            return Ok(denied(MergeOutcome::Conflict));
        }
        return Ok(CommandResult::Success(MergeOutcome::Applied {
            merge: crate::pulls::merge::record(&row[1..])?,
        }));
    }
    if data.token.owner != context.owner_fence() {
        return Ok(denied(MergeOutcome::Conflict));
    }
    let Some(row) = load(context, data.token)? else {
        return Ok(denied(MergeOutcome::Conflict));
    };
    if !matched(
        &row,
        &LeaseCheck {
            token: data.token,
            actor: data.actor.clone(),
        },
    ) || row.expires <= now(context.now_ms())?
    {
        return Ok(denied(MergeOutcome::Conflict));
    }
    check_pin(context, &row)?;
    if !retention_matches(context, &data, row.generation, data.catalog.format)?
        || fact(context, data.token.repository, data.catalog.format, None)? != data.base
    {
        return Ok(denied(MergeOutcome::Conflict));
    }
    let reviewed =
        match crate::pulls::merge::reviewed_native_update(context, &input, &proof.selection)? {
            Ok(value) => value,
            Err(outcome) => return Ok(denied(outcome)),
        };
    let Some(transition) = proof.transition else {
        return Ok(denied(MergeOutcome::Conflict));
    };
    let update = reviewed.update();
    if !reviewed.authorizes(&transition.plan.updates[0]) {
        return Ok(denied(MergeOutcome::Conflict));
    }
    if !super::ref_proof::proven(&transition.ancestry, 0) {
        return Ok(denied(MergeOutcome::NotFastForward));
    }
    let Some(refs) = transition.refs else {
        return Ok(denied(MergeOutcome::Conflict));
    };
    let Some(ref_generation) = transition.ref_generation else {
        return Ok(denied(MergeOutcome::Conflict));
    };
    let Some(audit) = transition.audit else {
        return Ok(denied(MergeOutcome::Conflict));
    };
    let mut encoded_audit = BoundedEncoder::new(128)?;
    audit.encode(&mut encoded_audit)?;
    let encoded_audit = encoded_audit.finish();
    let policy = context.sql(&SqlBatch {
        statements: vec![crate::branch_rules::policy_statement_with_ancestry(
            update, true,
        )],
    })?;
    if crate::branch_rules::decode_policy(&policy)?
        .is_some_and(|p| !p.allows_reviewed(update, &reviewed))
    {
        return Ok(denied(MergeOutcome::BranchPolicy));
    }
    let count = context.sql(&statement(
        "SELECT count(*) FROM (SELECT generation FROM catalog_generations LIMIT ?1)",
        vec![number(MAX_RETAINED_GENERATIONS)?],
    ))?;
    let Some([count]) = rows(&count)?.first().map(Vec::as_slice) else {
        return Err(Error::Command("missing merge generation count"));
    };
    if unsigned(count)? >= MAX_RETAINED_GENERATIONS {
        return Ok(denied(MergeOutcome::Conflict));
    }
    let Some(missing) = checkpoint(context, &data, &key)? else {
        return Ok(denied(MergeOutcome::Conflict));
    };
    let bytes = proof.certificate.bytes()?;
    let digest = *blake3::hash(&bytes).as_bytes();
    let generation = data
        .base
        .generation
        .checked_add(1)
        .filter(|g| *g <= i64::MAX as u64)
        .ok_or(Error::Command("merge generation exhausted"))?;
    let source = update
        .new_oid
        .ok_or(Error::Command("missing merge source"))?;
    let base = update
        .expected
        .as_ref()
        .and_then(|s| s.oid)
        .ok_or(Error::Command("missing merge base"))?;
    let result = MergeOutcome::Applied {
        merge: MergeRecord {
            id: input.request.id.clone(),
            number: input.number,
            oid: hex::encode(source),
            merged_at_ms: input.issued_at_ms,
            revision: input.request.revision.clone(),
        },
    };
    result.encode(&mut BoundedEncoder::new(512)?)?;
    let mut catalog = BoundedEncoder::new(256)?;
    data.catalog.encode(&mut catalog)?;
    let mut encoded_refs = BoundedEncoder::new(128)?;
    refs.encode(&mut encoded_refs)?;
    if row.expires <= now(context.now_ms())? {
        return Ok(denied(MergeOutcome::Conflict));
    }
    // Every later error rolls back roots, pull/UUID state, checkpoint, operation
    // consumption and the exact recovery journal together. No later rejection.
    if missing {
        changed(context.sql(&statement("UPDATE catalog_operations SET attestation=?1,attestation_digest=?2 WHERE id=?3 AND attestation IS NULL", vec![blob(&bytes),blob(digest),blob(data.token.operation)]))?)?;
        changed(context.sql(&statement("UPDATE catalog_leases SET attestation=?1,attestation_digest=?2 WHERE incarnation=?3 AND admission_sequence=?4 AND attestation IS NULL", vec![blob(&bytes),blob(digest),blob(data.token.owner.incarnation.as_bytes()),number(data.token.attempt)?]))?)?;
    }
    changed(context.sql(&statement(
        "INSERT INTO catalog_generations(generation,catalog,certificate,refs) VALUES(?1,?2,?3,?4)",
        vec![
            number(generation)?,
            blob(catalog.finish()),
            blob(digest),
            blob(encoded_refs.finish()),
        ],
    ))?)?;
    changed(context.sql(&statement(
        "UPDATE catalog_state SET generation=?1 WHERE singleton=1 AND generation=?2",
        vec![number(generation)?, number(data.base.generation)?],
    ))?)?;
    changed(context.sql(&statement(
        "UPDATE ref_generation SET generation=?1 WHERE singleton=1",
        vec![number(ref_generation)?],
    ))?)?;
    changed(context.sql(&statement("UPDATE pull_requests SET state='merged',version=version+1,updated_ms=max(updated_ms,?2) WHERE number=?1 AND state='open' AND version=?3 AND version<9223372036854775807", vec![SqlValue::Integer(input.number),SqlValue::Integer(input.issued_at_ms),SqlValue::Integer(input.request.revision.pull_version)]))?)?;
    changed(context.sql(&statement("INSERT INTO pull_merges(id,binding,pull_number,oid,merged_ms,pull_version,source_oid,source_version,base_oid,base_version,publication) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11)", vec![blob(id.as_bytes()),blob(binding),SqlValue::Integer(input.number),blob(source),SqlValue::Integer(input.issued_at_ms),SqlValue::Integer(input.request.revision.pull_version),blob(crate::pulls::merge::oid(&input.request.revision.source_oid)?),SqlValue::Integer(input.request.revision.source_version),blob(base),SqlValue::Integer(input.request.revision.base_version),blob(encoded_audit)]))?)?;
    changed(context.sql(&statement(
        "DELETE FROM catalog_operations WHERE id=?1",
        vec![blob(data.token.operation)],
    ))?)?;
    Ok(CommandResult::Success(result))
}
