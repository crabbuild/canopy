//! First-writer durable registration, never a ref publication or ACK.
use super::super::commands::{authorized, check_pin, load, matched};
use super::*;
pub struct RegisterRootRecovery;
impl Command for RegisterRootRecovery {
    const MODULE: &'static str = RepositoryModule::NAME;
    const ID: u32 = 39;
    const CODEC_VERSION: u32 = 1;
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
        if authorized(
            context,
            check.token.repository,
            &check.actor,
            TokenScope::Write,
        )?
        .is_none()
        {
            return deny(PreparationDenial::Unauthorized);
        }
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
            "SELECT recovery FROM catalog_leases WHERE incarnation=?1 AND admission_sequence=?2",
            vec![
                blob(check.token.owner.incarnation.as_bytes()),
                number(check.token.attempt)?,
            ],
        ))?;
        match rows(&old)?.first().map(Vec::as_slice) {
            Some([SqlValue::Blob(existing)]) if *existing == bytes => {}
            Some([SqlValue::Blob(_)]) => return deny(PreparationDenial::Conflict),
            Some([SqlValue::Null]) => {
                super::super::publish::changed(context.sql(&statement("UPDATE catalog_leases SET recovery=?1 WHERE incarnation=?2 AND admission_sequence=?3 AND recovery IS NULL", vec![blob(bytes), blob(check.token.owner.incarnation.as_bytes()), number(check.token.attempt)?]))?)?;
            }
            _ => return Err(Error::Command("root recovery pin is absent")),
        }
        Ok(CommandResult::Success(RootRecoveryReply::Registered))
    }
}
