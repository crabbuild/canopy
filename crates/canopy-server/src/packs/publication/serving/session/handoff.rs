//! Accepted original knowledge retains a root; fresh observations authorize I/O.
use super::*;
use crate::packs::publication::{custody::OwnedCustody, sql};

impl ServingPin {
    pub(in crate::packs::publication::serving) async fn retain_original(
        context: ServingContext,
        original: Arc<OwnedCustody>,
    ) -> Result<Self, ServingReadError> {
        // Cleanup remains possible after read admission closes. The trusted
        // administrator's ordinary bounded node/account slot owns this probe.
        let permit = context
            .budget
            .inner
            .admission
            .acquire(ReadIdentity::Account(&context.administrator))
            .await?;
        let tasks = context.budget.inner.tasks.clone();
        tasks.spawn(async move {
            let _permit = permit;
            if original.evidence().target() != &context.target {
                return Err(ServingReadError::Context);
            }
            let lease = original.serving_grant(&context.client).await
                .map_err(|error| ServingReadError::Custody(Box::new(error)))?;
            if lease.format != context.indexes.sources().format() {
                return Err(ServingReadError::Context);
            }
            context.authority.check(&context.target, lease.token.owner).await?;
            let exclusive = super::super::ownership::reserve(&context.target, lease.token)?;
            let sql = SqlCell::<RepositoryModule>::new(context.client.clone(), context.target.clone())?;
            let mut batch = sql::statement(
                "SELECT incarnation,admission_sequence,owner_epoch,generation FROM catalog_serving_pins WHERE reader=?1",
                vec![SqlValue::Blob(lease.token.reader.to_vec())],
            );
            batch.statements.extend(sql::statement(
                sql::GENERATION,
                vec![sql::number(lease.token.generation)?],
            ).statements);
            let sets = sql.query(None, batch).await
                .map_err(|error| ServingReadError::Proof(Box::new(error)))?;
            use sql::{fixed, generation, rows, unsigned};
            let (retained, generations) = sets.output.split_first().ok_or(ServingReadError::Context)?;
            let Some([incarnation, sequence, epoch, retained_generation]) = rows(std::slice::from_ref(retained))?.first().map(Vec::as_slice) else {
                return Err(ServingReadError::Inactive);
            };
            if fixed::<16>(incarnation)? != *lease.token.owner.incarnation.as_bytes()
                || unsigned(sequence)? != lease.token.admission_sequence
                || u64::from_be_bytes(fixed(epoch)?) != lease.token.owner.epoch
                || unsigned(retained_generation)? != lease.token.generation
                || generation(generations, lease.token.repository, lease.format)? != lease.fact
            {
                return Err(ServingReadError::Context);
            }
            context.authority.check(&context.target, lease.token.owner).await?;
            Ok(Self { inner: Arc::new(Inner {
                _exclusive: exclusive,
                context,
                lease,
                state: Mutex::new(Workers::default()),
                changed: Notify::new(),
                reader: tokio::sync::Mutex::new(None),
                refs: tokio::sync::OnceCell::new(),
                release: tokio::sync::Mutex::new(None),
            }) })
        }).await?
    }
}
