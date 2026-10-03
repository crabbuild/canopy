//! Ref-free immutable outcomes reuse the session certificate and result row.
//! Registered native custody may contain unverified packs: this command can
//! select an exact response but can never make those objects or refs visible.
use super::super::{
    commands::{authorized, check_pin, fact, load, matched},
    sql::*,
};
use super::*;
use crate::packs::directory::index::codec::fixed as wire_fixed;
const DOMAIN: &[u8] = b"canopy.root-outcome-completion.v2\0";
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RootOutcomeCompletion {
    pub proof: OutcomeCertificate,
    pub input_checkpoint_digest: [u8; 32],
    pub outcomes: RootPushOutcomes,
    /// Authenticated selection constraint: a publishing plan can only refuse.
    pub refusal: bool,
}
impl RootOutcomeCompletion {
    fn binding(
        checkpoint: [u8; 32],
        outcomes: &RootPushOutcomes,
        refusal: bool,
    ) -> Result<[u8; 32], CodecError> {
        let mut e = BoundedEncoder::new(ROOT_COMPLETION_BYTES)?;
        outcomes.encode(&mut e)?;
        let mut h = blake3::Hasher::new();
        h.update(DOMAIN);
        h.update(&checkpoint);
        h.update(&[u8::from(refusal)]);
        h.update(&e.finish());
        Ok(*h.finalize().as_bytes())
    }
    fn shape(&self) -> Result<(), CodecError> {
        let data = self.proof.0.data::<super::super::outcome::OutcomeData>()?;
        if self.outcomes.ref_generation != 0
            || self.input_checkpoint_digest == [0; 32]
            || data.digest
                != Self::binding(self.input_checkpoint_digest, &self.outcomes, self.refusal)?
            || [
                self.outcomes.native,
                self.outcomes.rejected,
                self.outcomes.replayed,
            ]
            .iter()
            .any(|root| root.operation() != data.check.token.artifact_operation)
        {
            return Err(CodecError::Invalid("invalid ref-free root completion"));
        }
        Ok(())
    }
}
impl WireValue for RootOutcomeCompletion {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        self.shape()?;
        e.write_bytes(DOMAIN)?;
        self.proof.encode(e)?;
        e.write_bytes(&self.input_checkpoint_digest)?;
        e.write_bool(self.refusal)?;
        self.outcomes.encode(e)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        if d.read_bytes()? != DOMAIN {
            return Err(CodecError::Invalid("ref-free root completion domain"));
        }
        let value = Self {
            proof: OutcomeCertificate::decode(d)?,
            input_checkpoint_digest: wire_fixed(d)?,
            refusal: d.read_bool()?,
            outcomes: RootPushOutcomes::decode(d)?,
        };
        value.shape()?;
        Ok(value)
    }
}
impl PreparationSession {
    /// Derive only from current registered native custody. This path requires
    /// no catalog reader, physical verifier, ref tree or policy pages.
    pub async fn root_outcome_completion(
        &self,
        store: &ArtifactStore,
        directory: &Path,
        budget: DiskBudget,
        signers: Option<&DirectoryCell>,
    ) -> Result<RootOutcomeCompletion, RootCompletionPreparationError> {
        self.prepare_root_outcome(store, directory, budget, signers, false)
            .await
    }
    /// Freeze a terminal refusal from registered native custody, including a
    /// publishing plan. It cannot select native success or expose any objects.
    pub async fn root_refusal_completion(
        &self,
        store: &ArtifactStore,
        directory: &Path,
        budget: DiskBudget,
        signers: Option<&DirectoryCell>,
    ) -> Result<RootOutcomeCompletion, RootCompletionPreparationError> {
        self.prepare_root_outcome(store, directory, budget, signers, true)
            .await
    }
    async fn prepare_root_outcome(
        &self,
        store: &ArtifactStore,
        directory: &Path,
        budget: DiskBudget,
        signers: Option<&DirectoryCell>,
        refusal: bool,
    ) -> Result<RootOutcomeCompletion, RootCompletionPreparationError> {
        let (_, deadline) = self.live_lease()?;
        timeout_at(
            deadline,
            Box::pin(async {
                let (checkpoint, _, _, _) = self.push_checkpoint().await?;
                let mut encoded = BoundedEncoder::new(CERTIFICATE_BYTES)?;
                checkpoint.encode(&mut encoded)?;
                let digest = *blake3::hash(&encoded.finish()).as_bytes();
                let native = checkpoint
                    .native_result()?
                    .ok_or(RootCompletionPreparationError::Context)?;
                let record = native.read(store).await?;
                // Refuse even an empty Some(plan) before materializing its frames.
                // A publishing native result needs the catalog/ref factory instead.
                if !refusal && record.has_plan() {
                    return Err(RootCompletionPreparationError::Context);
                }
                let request = self
                    .reopen_native_result(store, directory, &budget, signers)
                    .await?;
                if !refusal && request.plan.is_some() {
                    return Err(RootCompletionPreparationError::Context);
                }
                crate::push::report::publication_matches(&request.response, request.plan.as_ref())?;
                let outcomes = outcome::freeze(
                    store,
                    self.check.token.artifact_operation,
                    native,
                    (record.operation, record.response),
                    request,
                    0,
                )
                .await?;
                let (current, _, _, _) = self.push_checkpoint().await?;
                if current != checkpoint {
                    return Err(RootCompletionPreparationError::Context);
                }
                let proof = self
                    .issue_outcome_certificate(RootOutcomeCompletion::binding(
                        digest, &outcomes, refusal,
                    )?)
                    .await
                    .map_err(|error| RootCompletionPreparationError::Session(Box::new(error)))?;
                let value = RootOutcomeCompletion {
                    proof,
                    input_checkpoint_digest: digest,
                    outcomes,
                    refusal,
                };
                value.encode(&mut BoundedEncoder::new(ROOT_COMPLETION_BYTES)?)?;
                self.live_lease()?;
                Ok(value)
            }),
        )
        .await
        .map_err(|_| PreparationBaseError::Inactive)?
    }
}
pub struct CompleteRootOutcome;
impl Command for CompleteRootOutcome {
    const MODULE: &'static str = RepositoryModule::NAME;
    const ID: u32 = 38;
    const CODEC_VERSION: u32 = 2;
    type Input = RootOutcomeCompletion;
    type Output = RootCompletionReply;
    fn execute(
        context: &mut CommandContext<'_, '_>,
        input: Self::Input,
    ) -> cellule_runtime::Result<CommandResult<Self::Output>> {
        let data = input.proof.0.data::<super::super::outcome::OutcomeData>()?;
        let check = data.check;
        super::super::recovery::execute(
            context,
            &check,
            super::super::recovery::Kind::Outcome,
            RootCompletionReply::Denied,
            |context| Self::domain(context, input),
        )
    }
}
impl CompleteRootOutcome {
    fn domain(
        context: &mut CommandContext<'_, '_>,
        input: RootOutcomeCompletion,
    ) -> cellule_runtime::Result<CommandResult<RootCompletionReply>> {
        input.shape()?;
        let binding = RootOutcomeCompletion::binding(
            input.input_checkpoint_digest,
            &input.outcomes,
            input.refusal,
        )?;
        let Some((data, _)) = super::super::outcome::authenticate(context, &input.proof, binding)?
        else {
            return Ok(denied(PreparationDenial::Unauthorized));
        };
        let token = data.check.token;
        // Logical replay is independent of fresh ownership, write permission,
        // input-pin expiry and moving roots, but must match its exact binding.
        let saved = context.sql(&statement(read::SAVED, vec![blob(token.operation)]))?;
        if !rows(&saved)?.is_empty() {
            let Some(value) = read::saved(&saved, &data.check.actor, token.request_digest)? else {
                return Ok(denied(PreparationDenial::Conflict));
            };
            if fixed::<32>(&rows(&saved)?[0][3])? != binding {
                return Ok(denied(PreparationDenial::Conflict));
            }
            return Ok(CommandResult::Success(RootCompletionReply::Completed(
                Box::new(value),
            )));
        }
        if token.owner != context.owner_fence() {
            return Ok(denied(PreparationDenial::Stale));
        }
        let Some(row) = load(context, token)? else {
            return Ok(denied(PreparationDenial::Missing));
        };
        if !matched(&row, &data.check) {
            return Ok(denied(PreparationDenial::Stale));
        }
        if row.expires <= now(context.now_ms())? {
            return Ok(denied(PreparationDenial::Expired));
        }
        check_pin(context, &row)?;
        if identity(
            &context.sql(&statement(IDENTITY, vec![]))?,
            token.repository,
        )? != Some(data.format)
        {
            return Ok(denied(PreparationDenial::Unauthorized));
        }
        if row.generation != Some(data.floor.generation)
            || fact(context, token.repository, data.format, row.generation)? != data.floor
            || !inputs::checkpoint_matches(
                context,
                &data.check,
                data.format,
                input.input_checkpoint_digest,
            )?
        {
            return Ok(denied(PreparationDenial::Conflict));
        }
        let replayed = if let Some(signed) = &input.outcomes.signed {
            !rows(&context.sql(&statement(
                "SELECT 1 FROM push_certificates WHERE digest=?1",
                vec![blob(signed.digest)],
            ))?)?
            .is_empty()
        } else {
            false
        };
        let permitted = authorized(
            context,
            token.repository,
            &data.check.actor,
            TokenScope::Write,
        )? == Some(data.format);
        let selection = if replayed {
            result::Selection::Replayed
        } else if permitted && !input.refusal {
            result::Selection::Native
        } else {
            result::Selection::Rejected
        };
        let terminal = result::PreparedResult::new(
            &data.check,
            &input.outcomes,
            binding,
            None,
            selection,
            None,
            context.now_ms(),
        )?;
        if row.expires <= now(context.now_ms())? {
            return Ok(denied(PreparationDenial::Expired));
        }
        // No catalog/refs/attestation/body writes and no denial after this point.
        Ok(CommandResult::Success(terminal.save(context)?))
    }
}
fn denied(reason: PreparationDenial) -> CommandResult<RootCompletionReply> {
    CommandResult::Rejected(RootCompletionReply::Denied(reason))
}
