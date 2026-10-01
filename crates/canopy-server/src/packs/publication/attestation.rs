//! Trusted service issuance and bounded durable registration. No ref or current
//! catalog is changed here; the later final publisher must authenticate this
//! certificate, whether carried inline or recovered from a checkpoint.
use super::*;
use super::{
    certificate::CertificateData,
    commands::{authorized, check_pin, fact, load, matched},
    sql::*,
};
use cellule_runtime::{Committed, InvocationError, MutationIdentity, primitives::sql::SqlCell};
use tokio::time::timeout_at;

#[derive(Debug, thiserror::Error)]
pub enum CatalogAttestationError {
    #[error("catalog attestation base is inactive")]
    Base(#[from] PreparationBaseError),
    #[error("catalog attestation SQL capability failed")]
    Capability(#[from] Error),
    #[error("catalog attestation issuer query failed")]
    Query(#[source] Box<InvocationError<Vec<SqlResultSet>>>),
    #[error("catalog attestation registration failed")]
    Command(#[source] Box<InvocationError<AttestationOutcome>>),
    #[error("catalog attestation encoding failed")]
    Codec(#[from] CodecError),
}
impl PreparedCatalog {
    /// Uses the already trusted application SQL capability, as signed-push
    /// nonce issuance does. Never expose the repository seed or raw SQL surface
    /// to product clients. Raw descriptors cannot call this factory.
    pub async fn certificate(&self) -> Result<CatalogCertificate, CatalogAttestationError> {
        let (_, deadline) = self.base.live_lease()?;
        timeout_at(deadline, self.issue_certificate(None, None))
            .await
            .map_err(|_| PreparationBaseError::Inactive)?
    }
    /// Optional durable checkpoint. The final publisher may instead carry the
    /// certificate inline and persist its facts with refs in the same command.
    pub async fn attest(
        &self,
        identity: MutationIdentity,
    ) -> Result<Committed<AttestationOutcome>, CatalogAttestationError> {
        let (_, deadline) = self.base.live_lease()?;
        timeout_at(deadline, self.attest_inner(identity))
            .await
            .map_err(|_| PreparationBaseError::Inactive)?
    }
    async fn attest_inner(
        &self,
        identity: MutationIdentity,
    ) -> Result<Committed<AttestationOutcome>, CatalogAttestationError> {
        let certificate = self.issue_certificate(None, None).await?;
        self.ensure_live()?;
        let (client, target, _) = self.base.capability();
        client
            .command::<RegisterCatalogAttestation>(target, identity, certificate)
            .await
            .map_err(|error| CatalogAttestationError::Command(Box::new(error)))
    }
    pub(super) async fn issue_certificate(
        &self,
        refs_digest: Option<[u8; 32]>,
        completion_digest: Option<[u8; 32]>,
    ) -> Result<CatalogCertificate, CatalogAttestationError> {
        let mut data = CertificateData::from_prepared(self);
        data.refs_digest = refs_digest;
        data.completion_digest = completion_digest;
        issue_data_certificate(&self.base, data).await
    }
}
/// Shared trusted issuer. Only private verified preparation factories construct
/// these facts; decoded descriptors cannot invoke it from a product surface.
pub(super) async fn issue_data_certificate(
    base: &PreparationBaseResolver,
    data: CertificateData,
) -> Result<CatalogCertificate, CatalogAttestationError> {
    let (client, target, check) = base.capability();
    let live = client
        .query::<CheckPreparation>(target, None, check.clone())
        .await
        .map_err(|error| PreparationBaseError::Query(Box::new(error)))?
        .output
        .ok_or(PreparationBaseError::Inactive)?;
    if live.token != base.context_token()
        || live.base != base.retention_floor()
        || live.format != data.catalog.format
    {
        return Err(PreparationBaseError::Context.into());
    }
    let sql = SqlCell::<RepositoryModule>::new(client.clone(), target.clone())?;
    let mut statements = vec![
        SqlStatement { sql: "SELECT push_cert_seed FROM repository_identity WHERE singleton=1 AND repository_id=?1 AND object_format=?2".into(), parameters:vec![blob(live.token.repository),SqlValue::Text(live.format.as_str().into())] },
        SqlStatement { sql: GENERATION.into(), parameters:vec![number(base.generation_fact().generation)?] },
    ];
    if data.compaction {
        statements.push(access_statement(&check.actor));
    }
    let result = sql
        .query(None, SqlBatch { statements })
        .await
        .map_err(|error| CatalogAttestationError::Query(Box::new(error)))?;
    if data.compaction
        && !decode_access(
            result
                .output
                .get(2..)
                .ok_or(Error::Command("missing compaction issuer authority"))?,
        )?
        .is_some_and(|role| role >= TokenScope::Admin)
    {
        return Err(PreparationBaseError::Inactive.into());
    }
    let seed = seed(&result.output)?;
    if generation(
        result
            .output
            .get(1..)
            .ok_or(Error::Command("missing selected generation"))?,
        live.token.repository,
        live.format,
    )? != base.generation_fact()
    {
        return Err(PreparationBaseError::Context.into());
    }
    let certificate = CatalogCertificate::seal(&data, &seed)?;
    base.live_lease()?;
    Ok(certificate)
}

pub(super) fn seed(sets: &[SqlResultSet]) -> cellule_runtime::Result<[u8; 32]> {
    let Some([value]) = rows(sets)?.first().map(Vec::as_slice) else {
        return Err(Error::Command("catalog issuer secret is absent"));
    };
    fixed(value)
}
fn deny(reason: PreparationDenial) -> CommandResult<AttestationOutcome> {
    CommandResult::Rejected(AttestationOutcome::Denied(reason))
}
pub struct RegisterCatalogAttestation;
impl Command for RegisterCatalogAttestation {
    const MODULE: &'static str = RepositoryModule::NAME;
    const ID: u32 = 17;
    const CODEC_VERSION: u32 = 1;
    type Input = CatalogCertificate;
    type Output = AttestationOutcome;
    fn execute(
        context: &mut CommandContext<'_, '_>,
        certificate: CatalogCertificate,
    ) -> cellule_runtime::Result<CommandResult<AttestationOutcome>> {
        let data = certificate.data()?;
        if data.tenant != *context.target().tenant().as_bytes()
            || data.application != *context.target().application().as_bytes()
            || data.token.owner != context.owner_fence()
        {
            return Ok(deny(PreparationDenial::Stale));
        }
        let Some(format) = authorized(
            context,
            data.token.repository,
            &data.actor,
            if data.compaction {
                TokenScope::Admin
            } else {
                TokenScope::Write
            },
        )?
        else {
            return Ok(deny(PreparationDenial::Unauthorized));
        };
        let Some(row) = load(context, data.token)? else {
            return Ok(deny(PreparationDenial::Missing));
        };
        if !matched(
            &row,
            &LeaseCheck {
                token: data.token,
                actor: data.actor.clone(),
            },
        ) {
            return Ok(deny(PreparationDenial::Stale));
        }
        if row.expires <= now(context.now_ms())? {
            return Ok(deny(PreparationDenial::Expired));
        }
        check_pin(context, &row)?;
        if format != data.catalog.format
            || !super::publish::retention_matches(context, &data, row.generation, format)?
            || fact(
                context,
                data.token.repository,
                format,
                Some(data.base.generation),
            )? != data.base
        {
            return Ok(deny(PreparationDenial::Conflict));
        }
        let key = seed(&context.sql(&statement(
            "SELECT push_cert_seed FROM repository_identity WHERE singleton=1",
            vec![],
        ))?)?;
        if !certificate.authenticated(&key) {
            return Ok(deny(PreparationDenial::Unauthorized));
        }
        let bytes = certificate.bytes()?;
        let digest = *blake3::hash(&bytes).as_bytes();
        let previous = context.sql(&statement(
            "SELECT attestation,attestation_digest FROM catalog_operations WHERE id=?1",
            vec![blob(data.token.operation)],
        ))?;
        let pin_previous = context.sql(&statement(
            "SELECT attestation,attestation_digest FROM catalog_leases WHERE incarnation=?1 AND admission_sequence=?2",
            vec![blob(data.token.owner.incarnation.as_bytes()),number(data.token.attempt)?],
        ))?;
        if rows(&previous)? != rows(&pin_previous)? {
            return Err(Error::Command(
                "catalog attestation differs from its retention pin",
            ));
        }
        match rows(&previous)?.first().map(Vec::as_slice) {
            Some([SqlValue::Null, SqlValue::Null]) => {
                let result = context.sql(&statement("UPDATE catalog_operations SET attestation=?1,attestation_digest=?2 WHERE id=?3 AND attestation IS NULL",vec![blob(&bytes),blob(digest),blob(data.token.operation)]))?;
                if result.first().is_none_or(|set| set.rows_affected != 1) {
                    return Err(Error::Command(
                        "catalog attestation registration changed no rows",
                    ));
                }
                let pinned = context.sql(&statement(
                    "UPDATE catalog_leases SET attestation=?1,attestation_digest=?2 WHERE incarnation=?3 AND admission_sequence=?4 AND attestation IS NULL",
                    vec![blob(&bytes),blob(digest),blob(data.token.owner.incarnation.as_bytes()),number(data.token.attempt)?],
                ))?;
                if pinned.first().is_none_or(|set| set.rows_affected != 1) {
                    return Err(Error::Command(
                        "catalog attestation retention changed no rows",
                    ));
                }
            }
            Some([SqlValue::Blob(stored), stored_digest]) => {
                if *stored != bytes || fixed::<32>(stored_digest)? != digest {
                    return Ok(deny(PreparationDenial::Conflict));
                }
            }
            _ => return Err(Error::Command("invalid stored catalog attestation")),
        }
        Ok(CommandResult::Success(AttestationOutcome::Registered(
            RegisteredCatalog {
                token: data.token,
                certificate_digest: digest,
            },
        )))
    }
}
