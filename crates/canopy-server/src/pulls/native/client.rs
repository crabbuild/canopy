use super::*;
impl RepositoryCell {
    async fn pull_ref_selection(
        &self,
        actor: &str,
        request: [u8; 32],
        names: &[String],
    ) -> Result<
        (
            Option<crate::packs::publication::ServingSnapshot>,
            RefSelection,
        ),
        NativePullError,
    > {
        let access = self
            .sql
            .query(
                None,
                SqlBatch {
                    statements: vec![reads::access(ReadIdentity::Account(actor))],
                },
            )
            .await
            .map_err(|e| NativePullError::Metadata(Box::new(e)))?;
        // A known denied caller still reaches the final typed command so its
        // domain refusal has an original durable receipt. No proof is issued.
        if !reads::allowed(&access.output)? {
            return Ok((
                None,
                RefSelection {
                    repository: self.id,
                    actor: Some(actor.into()),
                    facts: Vec::new(),
                    proof: None,
                },
            ));
        }
        let snapshot = self.serving_snapshot(ReadIdentity::Account(actor)).await?;
        let selection = snapshot.ref_selection(request, names).await?;
        Ok((Some(snapshot), selection))
    }

    pub(in crate::pulls) async fn native_create_pull(
        &self,
        identity: MutationIdentity,
        actor: &str,
        input: NewPull<'_>,
    ) -> Result<Committed<PullChange>, NativePullError> {
        validate_component(actor)?;
        validate_repository_id(input.id)?;
        if !valid_new(&input) {
            return Err(Error::Command("invalid pull creation").into());
        }
        let data = CreateData {
            id: input.id,
            title: input.title.into(),
            body: input.body.into(),
            draft: input.draft,
            source_ref: input.source_ref.into(),
            source_oid: input.source_oid.into(),
            base_ref: input.base_ref.into(),
            base_oid: input.base_oid.into(),
        };
        let mut names = vec![data.source_ref.clone(), data.base_ref.clone()];
        names.sort();
        let (_snapshot, selection) = self
            .pull_ref_selection(actor, data.digest()?, &names)
            .await?;
        let result = self
            .application
            .command::<CreateNativePull>(&self.target, identity, CreateRequest { selection, data })
            .await;
        match result {
            Ok(v) => Ok(v),
            Err(InvocationError::Rejected(v)) => Ok(*v),
            Err(e) => Err(NativePullError::Command(Box::new(e))),
        }
    }
    pub(in crate::pulls) async fn native_review_pull(
        &self,
        identity: MutationIdentity,
        actor: &str,
        number: i64,
        input: NewReview<'_>,
    ) -> Result<Committed<PullChange>, NativePullError> {
        validate_component(actor)?;
        validate_repository_id(input.id)?;
        if number < 1 || !valid_review(&input) {
            return Err(Error::Command("invalid pull review").into());
        }
        let data = ReviewData {
            number,
            id: input.id,
            revision: input.revision.clone(),
            kind: input.kind,
            body: input.body.into(),
        };
        let selected = self
            .sql
            .query(
                None,
                SqlBatch {
                    statements: vec![reads::selector(
                        ReadIdentity::Account(actor),
                        &ReadKind::Detail(number),
                    )],
                },
            )
            .await
            .map_err(|e| NativePullError::Metadata(Box::new(e)))?;
        let rows = &selected
            .output
            .first()
            .ok_or(Error::Command("review selection missing"))?
            .rows;
        let names = reads::selected_names(rows)?;
        let (_snapshot, selection) = self
            .pull_ref_selection(actor, data.digest()?, &names)
            .await?;
        let result = self
            .application
            .command::<ReviewNativePull>(&self.target, identity, ReviewRequest { selection, data })
            .await;
        match result {
            Ok(v) => Ok(v),
            Err(InvocationError::Rejected(v)) => Ok(*v),
            Err(e) => Err(NativePullError::Command(Box::new(e))),
        }
    }
}
