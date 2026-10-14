//! Issuance reads immutable ref facts under the existing tracked physical owner.
use super::super::super::ref_observation::{
    ObservationData, RefFact, RefObservation, RefSelection,
};
use super::super::super::{attestation, sql};
use super::*;

impl ServingPin {
    pub(in crate::packs::publication::serving) async fn ref_selection(
        &self,
        actor: Option<String>,
        request: [u8; 32],
        names: &[String],
    ) -> Result<RefSelection, ServingReadError> {
        if names.len() > 128
            || names.iter().map(String::len).sum::<usize>() > 512 << 10
            || names.windows(2).any(|p| p[0] >= p[1])
            || names.iter().any(|n| {
                !crate::refs::valid_ref_name(n) || n.len() > crate::packs::ref_state::MAX_NAME_BYTES
            })
        {
            return Err(ServingReadError::Context);
        }
        let names = names.to_vec();
        self.read_owned(actor.clone(), move |inner, deadline, _permit| async move {
            let snapshot = inner.ref_snapshot().await?;
            let mut facts = Vec::with_capacity(names.len());
            for name in names {
                if Instant::now() >= deadline { return Err(ServingReadError::Inactive) }
                let state = inner.context.indexes.refs().read(snapshot.root.clone(), &name).await?;
                facts.push(RefFact { name, state });
            }
            let mut selection = RefSelection { repository: inner.lease.token.repository, actor: actor.clone(), facts, proof: None };
            let binding = selection.binding(request)?;
            let capability = SqlCell::<RepositoryModule>::new(inner.context.client.clone(), inner.context.target.clone())?;
            let result = capability.query(None, SqlBatch { statements: vec![
                SqlStatement {
                    sql: "SELECT push_cert_seed FROM repository_identity WHERE singleton=1 AND repository_id=?1 AND object_format=?2".into(),
                    parameters: vec![sql::blob(inner.lease.token.repository), SqlValue::Text(inner.lease.format.as_str().into())],
                },
                SqlStatement { sql: sql::GENERATION.into(), parameters: vec![sql::number(inner.lease.token.generation)?] },
            ]}).await.map_err(|e| ServingReadError::Proof(Box::new(e)))?;
            if sql::generation(result.output.get(1..).ok_or(ServingReadError::Context)?, inner.lease.token.repository, inner.lease.format)? != inner.lease.fact {
                return Err(ServingReadError::Context)
            }
            let seed = attestation::seed(&result.output)?;
            selection.proof = Some(RefObservation::seal(ObservationData {
                tenant: *inner.context.target.tenant().as_bytes(), application: *inner.context.target.application().as_bytes(),
                token: inner.lease.token, fact: inner.lease.fact, actor, binding,
            }, &seed)?);
            if Instant::now() >= deadline { return Err(ServingReadError::Inactive) }
            Ok(selection)
        }).await
    }
}
