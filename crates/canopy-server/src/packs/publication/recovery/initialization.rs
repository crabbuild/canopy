//! Exact initialization discovery, receipts and command restoration.
use super::*;

impl RegisteredRootRecovery {
    /// Discover the current or successfully closed initialization pin, using
    /// exact indexed operation/lease and immutable outcome bindings. Historical metadata grants knowledge, not Write.
    pub async fn load_initialization(
        client: &CellClient,
        target: &CellTarget,
        store: &ArtifactStore,
        input: &BeginRequest,
    ) -> Result<Option<Self>, RootRecoveryError> {
        if crate::repository_target(target.tenant(), target.application(), input.repository)?
            != *target
            || store.repository() != input.repository
        {
            return Err(RootRecoveryError::Context);
        }
        let sql = SqlCell::<RepositoryModule>::new(client.clone(), target.clone())?;
        let observed = sql.query(None, statement(
            "SELECT o.incarnation,o.admission_sequence FROM catalog_operations o JOIN catalog_leases l ON l.incarnation=o.incarnation AND l.admission_sequence=o.admission_sequence WHERE o.id=?1 AND o.actor=?2 AND o.request_digest=?3 AND o.generation=0 AND l.recovery IS NOT NULL UNION ALL SELECT l.incarnation,l.admission_sequence FROM catalog_initialization i JOIN catalog_leases l ON l.incarnation=i.incarnation AND l.admission_sequence=i.admission_sequence WHERE i.id=?1 AND i.actor=?2 AND i.request_digest=?3 AND l.generation=0 AND l.recovery IS NOT NULL",
            vec![blob(input.operation),SqlValue::Text(input.actor.clone()),blob(input.request_digest)]
        )).await.map_err(|error| RootRecoveryError::Query(Box::new(error)))?;
        if rows(&observed.output)?.len() > 1 {
            return Err(RootRecoveryError::Context);
        }
        let Some([incarnation, sequence]) = rows(&observed.output)?.first().map(Vec::as_slice)
        else {
            if rows(&observed.output)?.is_empty() {
                return Ok(None);
            }
            return Err(RootRecoveryError::Context);
        };
        let loaded = Self::load_pin(
            client,
            target,
            store,
            IncarnationId::from_bytes(fixed(incarnation)?),
            unsigned(sequence)?,
            None,
        )
        .await?
        .ok_or(RootRecoveryError::Context)?;
        if loaded.record.kind != Kind::Initialization
            || loaded.record.check.actor != input.actor
            || loaded.token().operation != input.operation
            || loaded.token().request_digest != input.request_digest
        {
            return Err(RootRecoveryError::Context);
        }
        Ok(Some(loaded))
    }
    /// Original durable phase knowledge wins before body reads and fresh
    /// custody. Only authoritative SDK absence can execute the saved bytes.
    pub async fn recover_initialization(
        &self,
        client: &CellClient,
        store: &ArtifactStore,
        authority: &PreparationAuthority,
    ) -> Result<Committed<InitializationReply>, PublicationError> {
        self.dispatch_initialization(client, store, authority, None)
            .await
    }
    pub(in crate::packs::publication) async fn dispatch_initialization(
        &self,
        client: &CellClient,
        store: &ArtifactStore,
        authority: &PreparationAuthority,
        original: Option<&PreparationSession>,
    ) -> Result<Committed<InitializationReply>, PublicationError> {
        if self.record.kind != Kind::Initialization {
            return Err(PublicationError::Recovery {
                evidence: Box::new(self.evidence().clone()),
                source: Box::new(RootRecoveryError::Context),
            });
        }
        let result = self
            .dispatch_command::<InitializeCatalogRefs>(client, store, authority, false, original)
            .await
            .map_err(|error| {
                error.publication(self.evidence(), PublicationError::Initialization)
            })?;
        if matches!(result.output, InitializationReply::Denied(_)) {
            return Err(PublicationError::Initialization(InvocationError::Rejected(
                Box::new(result),
            )));
        }
        Ok(result)
    }
}
