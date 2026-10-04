//! First-writer registration and domain result commit in the Repository Cell.
use super::*;

pub struct RegisterCustodyIntent;
impl Command for RegisterCustodyIntent {
    const MODULE: &'static str = RepositoryModule::NAME;
    const ID: u32 = 41;
    const CODEC_VERSION: u32 = 1;
    type Input = CustodyIntent;
    type Output = RootRecoveryReply;
    fn execute(
        context: &mut CommandContext<'_, '_>,
        intent: Self::Input,
    ) -> cellule_runtime::Result<CommandResult<Self::Output>> {
        let deny = |reason| Ok(CommandResult::Rejected(RootRecoveryReply::Denied(reason)));
        let seed = super::super::attestation::seed(&context.sql(&SqlBatch {
            statements: vec![seed_statement()],
        })?)?;
        let header = intent.validate(context.target(), &seed)?;
        let request = intent.request()?;
        let bytes = intent.encoded()?;
        let previous = context.sql(&SqlBatch {
            statements: vec![row_statement(header.operation, None), seed_statement()],
        })?;
        let previous = from_sets(&previous, context.target(), header.operation)?;
        if let Some(previous) = &previous {
            if previous.intent == intent {
                // Exact knowledge is idempotent even after permission/owner loss.
                return Ok(CommandResult::Success(RootRecoveryReply::Registered));
            }
            let old = previous.intent.header()?;
            if old.step >= header.step
                || !previous.closed()
                || old.step.checked_add(1) != Some(header.step)
                || header.previous != Some(*blake3::hash(&previous.intent.encoded()?).as_bytes())
                || old.actor != header.actor
                || old.request_digest != header.request_digest
                || old.repository != header.repository
            {
                return deny(PreparationDenial::Conflict);
            }
        } else if header.step != 0 || !request.action.begin() {
            return deny(PreparationDenial::Missing);
        }
        if header.incarnation != context.owner_fence().incarnation {
            return deny(PreparationDenial::Stale);
        }
        if intent.snapshot.evidence().identity().expires_at_ms <= now(context.now_ms())? {
            return deny(PreparationDenial::Expired);
        }
        if super::super::commands::authorized(
            context,
            header.repository,
            &header.actor,
            TokenScope::Write,
        )?
        .is_none()
        {
            return deny(PreparationDenial::Unauthorized);
        }
        let pending = context.sql(&statement(
            "SELECT count(*) FROM (SELECT operation FROM catalog_custody_commands WHERE phase IS NULL AND stopped IS NULL LIMIT ?1)",
            vec![number(MAX_OPERATIONS)?],
        ))?;
        let Some([SqlValue::Integer(pending)]) = rows(&pending)?.first().map(Vec::as_slice) else {
            return Err(Error::Command("custody pending count absent"));
        };
        if *pending >= MAX_OPERATIONS as i64 {
            return deny(PreparationDenial::Capacity);
        }
        super::super::publish::changed(context.sql(&statement(
            "INSERT INTO catalog_custody_commands(operation,step,incarnation,request_id,intent,phase) VALUES(?1,?2,?3,?4,?5,NULL)",
            vec![blob(header.operation),number(u64::from(header.step))?,blob(header.incarnation.as_bytes()),
                blob(intent.snapshot.evidence().identity().request_id.as_bytes()), SqlValue::Blob(bytes)],
        ))?)?;
        Ok(CommandResult::Success(RootRecoveryReply::Registered))
    }
}

/// The only receiver of the new custody protocol. Domain methods reuse the
/// existing allocator, pins, authorization and phase-specific validation.
pub struct ExecuteCustody;
impl Command for ExecuteCustody {
    const MODULE: &'static str = RepositoryModule::NAME;
    const ID: u32 = 42;
    const CODEC_VERSION: u32 = 1;
    type Input = CustodyRequest;
    type Output = CustodyReply;
    fn execute(
        context: &mut CommandContext<'_, '_>,
        request: Self::Input,
    ) -> cellule_runtime::Result<CommandResult<Self::Output>> {
        let (_, operation, _, _) = request.action.identity();
        let sets = context.sql(&SqlBatch {
            statements: vec![
                row_statement(operation, Some(request.step)),
                seed_statement(),
            ],
        })?;
        let saved = from_sets(&sets, context.target(), operation)?
            .ok_or(Error::Command("custody command is not registered"))?;
        let header = saved.intent.header()?;
        let evidence = context
            .mutation_evidence()
            .ok_or(Error::Command("custody command lacks mutation evidence"))?;
        if header.stamp != Stamp::of(&evidence)
            || header.incarnation != evidence.incarnation()
            || saved.intent.request()? != request
            || saved.closed()
        {
            return Err(Error::Command(
                "custody command differs from its original intent",
            ));
        }
        let output = match request.action.clone() {
            CustodyAction::BeginPreparation(input) => {
                prep(BeginPreparation::execute(context, input)?)
            }
            CustodyAction::ClaimPreparation(input) => {
                prep(ClaimPreparation::execute(context, input)?)
            }
            CustodyAction::RenewPreparation(input) => {
                prep(RenewPreparation::execute(context, input)?)
            }
            CustodyAction::BeginStaging(input) => stage(BeginStaging::execute(context, input)?),
            CustodyAction::ClaimStaging(input) => stage(ClaimStaging::execute(context, input)?),
            CustodyAction::RenewStaging(input) => stage(RenewStaging::execute(context, input)?),
            CustodyAction::BindStaging(input) => prep(BindStaging::execute(context, input)?),
        };
        let phase = Recorded::new(context.sequence(), output.rejected(), encode(&output, 512)?)?;
        codec::validate_phase(&phase, &request)?;
        let token = match &output {
            CustodyReply::Preparation(PreparationReply::Granted(lease)) => Some(lease.token),
            CustodyReply::Staging(StagingReply::Granted(lease)) => Some(lease.token),
            _ => None,
        };
        let grant_incarnation = token.map_or(SqlValue::Null, |token| {
            blob(token.owner.incarnation.as_bytes())
        });
        let grant_attempt = token
            .map(|token| number(token.attempt))
            .transpose()?
            .unwrap_or(SqlValue::Null);
        super::super::publish::changed(context.sql(&statement(
            "UPDATE catalog_custody_commands SET phase=?1,granted_incarnation=?5,granted_attempt=?6 WHERE operation=?2 AND step=?3 AND intent=?4 AND phase IS NULL AND stopped IS NULL",
            vec![SqlValue::Blob(encode(&phase, 1024)?),blob(operation),number(u64::from(request.step))?,SqlValue::Blob(saved.intent.encoded()?),grant_incarnation,grant_attempt],
        ))?)?;
        // Trusted denials commit the original phase alongside SDK acceptance;
        // the private service normalizes them back to Rejected at its boundary.
        Ok(CommandResult::Success(output))
    }
}
fn prep(result: CommandResult<PreparationReply>) -> CustodyReply {
    CustodyReply::Preparation(match result {
        CommandResult::Success(r) | CommandResult::Rejected(r) => r,
    })
}
fn stage(result: CommandResult<StagingReply>) -> CustodyReply {
    CustodyReply::Staging(match result {
        CommandResult::Success(r) | CommandResult::Rejected(r) => r,
    })
}
