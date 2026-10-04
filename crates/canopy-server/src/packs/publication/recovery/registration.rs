//! First-writer durable registration, never a ref publication or ACK.
use super::super::commands::{authorized, check_pin, load, matched};
use super::*;
pub struct RegisterRootRecovery;
impl Command for RegisterRootRecovery {
    const MODULE: &'static str = RepositoryModule::NAME;
    const ID: u32 = 39;
    const CODEC_VERSION: u32 = 4;
    type Input = RootRecoveryCertificate;
    type Output = RootRecoveryReply;
    fn execute(
        context: &mut CommandContext<'_, '_>,
        certificate: Self::Input,
    ) -> cellule_runtime::Result<CommandResult<Self::Output>> {
        let record = certificate.0.data::<Record>()?;
        let check = &record.check;
        let deny = |reason| Ok(CommandResult::Rejected(RootRecoveryReply::Denied(reason)));
        if record.tenant != *context.target().tenant().as_bytes()
            || record.application != *context.target().application().as_bytes()
            || check.token.owner != context.owner_fence()
        {
            return deny(PreparationDenial::Stale);
        }
        let permitted = authorized(
            context,
            check.token.repository,
            &check.actor,
            TokenScope::Write,
        )?
        .is_some();
        let Some(row) = load(context, check.token)? else {
            return deny(PreparationDenial::Missing);
        };
        if !matched(&row, check) {
            return deny(PreparationDenial::Stale);
        }
        if row.expires <= now(context.now_ms())? {
            return deny(PreparationDenial::Expired);
        }
        check_pin(context, &row)?;
        let seed = super::super::attestation::seed(&context.sql(&statement(
            "SELECT push_cert_seed FROM repository_identity WHERE singleton=1",
            vec![],
        ))?)?;
        if !certificate.0.authenticated(&seed) {
            return deny(PreparationDenial::Unauthorized);
        }
        let mut e = BoundedEncoder::new(CERTIFICATE_BYTES)?;
        certificate.encode(&mut e)?;
        let bytes = e.finish();
        let old = context.sql(&statement(
            "SELECT recovery,recovery_phase FROM catalog_leases WHERE incarnation=?1 AND admission_sequence=?2",
            vec![
                blob(check.token.owner.incarnation.as_bytes()),
                number(check.token.attempt)?,
            ],
        ))?;
        match rows(&old)?.first().map(Vec::as_slice) {
            Some([SqlValue::Blob(existing), _]) if *existing == bytes => {
                if !permitted {
                    return deny(PreparationDenial::Unauthorized);
                }
            }
            Some([SqlValue::Blob(existing), saved]) => {
                let old = phase::certificate(existing)?;
                let old_record = old.0.data::<Record>()?;
                if !old.0.authenticated(&seed) || old_record.check != *check {
                    return deny(PreparationDenial::Conflict);
                }
                // The original policy bundle already authorized this exact
                // refusal. Advancing to it cannot publish refs or objects and
                // must remain possible after current Write access is revoked.
                let frozen_refusal = old_record.kind == Kind::Policy
                    && record.kind == Kind::Outcome
                    && old_record.refusal == Some(record.primary);
                if !permitted && !frozen_refusal {
                    return deny(PreparationDenial::Unauthorized);
                }
                if record.step != old_record.step + 1 {
                    return deny(PreparationDenial::Conflict);
                }
                let journal = phase::journal(saved, &old_record)?;
                if !journal.may_advance(&old_record)? {
                    return deny(PreparationDenial::Conflict);
                }
                let Some(previous) = record.previous else {
                    return deny(PreparationDenial::Conflict);
                };
                let mut e = BoundedEncoder::new(ROOT_BYTES)?;
                phase::Frame {
                    certificate: old,
                    journal,
                }
                .encode(&mut e)?;
                let frame = e.finish();
                if previous.artifact.size != frame.len() as u64
                    || previous.artifact.digest != *blake3::hash(&frame).as_bytes()
                {
                    return deny(PreparationDenial::Conflict);
                }
                super::super::publish::changed(context.sql(&statement("UPDATE catalog_leases SET recovery=?1,recovery_phase=NULL,recovery_phase_revision=0 WHERE incarnation=?2 AND admission_sequence=?3 AND recovery=?4 AND recovery_phase IS ?5", vec![blob(bytes),blob(check.token.owner.incarnation.as_bytes()),number(check.token.attempt)?,blob(existing),saved.clone()]))?)?;
            }
            Some([SqlValue::Null, SqlValue::Null]) => {
                if !permitted {
                    return deny(PreparationDenial::Unauthorized);
                }
                if record.previous.is_some() {
                    return deny(PreparationDenial::Missing);
                }
                super::super::publish::changed(context.sql(&statement("UPDATE catalog_leases SET recovery=?1 WHERE incarnation=?2 AND admission_sequence=?3 AND recovery IS NULL", vec![blob(bytes), blob(check.token.owner.incarnation.as_bytes()), number(check.token.attempt)?]))?)?;
            }
            _ => return Err(Error::Command("root recovery pin is absent")),
        }
        Ok(CommandResult::Success(RootRecoveryReply::Registered))
    }
}
