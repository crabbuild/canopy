//! Private issuance follows certified header lookup under tracked physical ownership.
use super::super::super::commit_membership::{CommitMembership, MembershipData};
use super::super::super::{attestation, sql};
use super::*;

impl ServingPin {
    pub(in crate::packs::publication::serving) async fn commit_membership(
        &self,
        actor: Option<String>,
        oid: crate::ObjectId,
    ) -> Result<Option<CommitMembership>, ServingReadError> {
        if oid.is_zero() || oid.format() != self.inner.lease.format {
            return Ok(None);
        }
        self.read_owned(actor.clone(), move |inner, deadline, _permit| async move {
            let reader = inner.catalog().await?;
            let header = reader
                .headers(&[oid], &*inner.context.files, &*inner.context.files)
                .await?
                .pop()
                .flatten();
            if header.is_none_or(|h| h.object.kind != crate::ObjectKind::Commit) {
                return Ok(None);
            }
            if Instant::now() >= deadline {
                return Err(ServingReadError::Inactive);
            }
            let capability = SqlCell::<RepositoryModule>::new(
                inner.context.client.clone(),
                inner.context.target.clone(),
            )?;
            let result = capability
                .query(None, SqlBatch {
                    statements: vec![
                        SqlStatement {
                            sql: "SELECT push_cert_seed FROM repository_identity WHERE singleton=1 AND repository_id=?1 AND object_format=?2".into(),
                            parameters: vec![
                                sql::blob(inner.lease.token.repository),
                                SqlValue::Text(oid.format().as_str().into()),
                            ],
                        },
                        SqlStatement {
                            sql: sql::GENERATION.into(),
                            parameters: vec![sql::number(inner.lease.token.generation)?],
                        },
                    ],
                })
                .await
                .map_err(|e| ServingReadError::Proof(Box::new(e)))?;
            let generation = sql::generation(
                result.output.get(1..).ok_or(ServingReadError::Context)?,
                inner.lease.token.repository,
                oid.format(),
            )?;
            if generation != inner.lease.fact {
                return Err(ServingReadError::Context);
            }
            let seed = attestation::seed(&result.output)?;
            Ok(Some(CommitMembership::seal(MembershipData {
                tenant: *inner.context.target.tenant().as_bytes(),
                application: *inner.context.target.application().as_bytes(),
                token: inner.lease.token,
                fact: inner.lease.fact,
                actor,
                oid,
            }, &seed)?))
        }).await
    }
}
