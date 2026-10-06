use super::*;

impl RepositoryCell {
    /// Creates or changes a context for the owner at the expected policy version.
    ///
    /// Names remain reserved when disabled. Enabling requires a current repository
    /// member as reporter. Every change invalidates prior policy-version results.
    pub async fn set_check_context(
        &self,
        identity: MutationIdentity,
        actor: &str,
        name: &str,
        input: CheckContextEdit<'_>,
    ) -> Result<Committed<CheckChange>, Invocation> {
        for name in [actor, name, input.reporter] {
            validate_component(name).map_err(Invocation::NotStarted)?;
        }
        if !(0..i64::MAX).contains(&input.expected_version) {
            return Err(Invocation::NotStarted(Error::Command(
                "invalid check context version",
            )));
        }
        let parameters = vec![
            SqlValue::Text(actor.into()),
            SqlValue::Text(name.into()),
            SqlValue::Integer(input.expected_version),
            SqlValue::Text(input.reporter.into()),
            SqlValue::Integer(i64::from(input.enabled)),
        ];
        let decision = format!(
            "CASE WHEN NOT ({OWNER}) THEN 'forbidden' WHEN ?5 = 1 AND NOT EXISTS (SELECT 1 FROM repository_identity WHERE owner = ?4) AND NOT EXISTS (SELECT 1 FROM repository_members WHERE account = ?4) THEN 'missing' WHEN coalesce((SELECT version FROM check_contexts WHERE name = ?2), 0) != ?3 THEN 'conflict' ELSE 'applied' END"
        );
        self.check_change(identity, vec![
            SqlStatement { sql: format!("SELECT {decision}"), parameters: parameters.clone() },
            SqlStatement { sql: format!("INSERT INTO check_contexts (name, reporter, enabled, version) SELECT ?2, ?4, ?5, ?3 + 1 WHERE ({decision}) = 'applied' ON CONFLICT(name) DO UPDATE SET reporter = excluded.reporter, enabled = excluded.enabled, version = excluded.version"), parameters },
        ]).await
    }

    /// Starts one queued attempt for the context's configured reporter.
    ///
    /// The commit must already be stored. A retry preserves the original attempt
    /// and never makes it newer than a subsequently created run.
    pub async fn start_check(
        &self,
        identity: MutationIdentity,
        actor: &str,
        input: NewCheck<'_>,
    ) -> Result<Committed<CheckChange>, NativeCheckError> {
        for value in [actor, input.context] {
            validate_component(value)?;
        }
        validate_repository_id(input.id)?;
        if input.context_version < 1 {
            return Err(Error::Command("invalid check context version").into());
        }
        let (_snapshot, selection) = self
            .check_selection(ReadIdentity::Account(actor), input.oid)
            .await?;
        let result = self
            .application
            .command::<native::StartCommitCheck>(
                &self.target,
                identity,
                native::CheckStart {
                    selection,
                    id: input.id,
                    context: input.context.into(),
                    context_version: input.context_version,
                },
            )
            .await;
        // A recorded policy rejection is a domain outcome with its original
        // receipt. Pending/transport failures must never be converted to one.
        match result {
            Ok(value) => Ok(value),
            Err(InvocationError::Rejected(value)) => Ok(*value),
            Err(error) => Err(NativeCheckError::Start(Box::new(error))),
        }
    }

    /// Advances an active attempt for its still-configured reporter and policy version.
    ///
    /// Terminal states are immutable; reruns require a new attempt UUID.
    pub async fn update_check(
        &self,
        identity: MutationIdentity,
        actor: &str,
        id: [u8; 16],
        input: CheckEdit<'_>,
    ) -> Result<Committed<CheckChange>, Invocation> {
        validate_component(actor).map_err(Invocation::NotStarted)?;
        if !(1..i64::MAX).contains(&input.expected_version)
            || input.state == CheckState::Queued
            || !valid_summary(input.summary)
        {
            return Err(Invocation::NotStarted(Error::Command(
                "invalid check update",
            )));
        }
        let mut parameters = vec![
            SqlValue::Text(actor.into()),
            SqlValue::Blob(id.to_vec()),
            SqlValue::Integer(input.expected_version),
        ];
        let decision = format!(
            "CASE WHEN NOT ({ACCESS}) OR NOT EXISTS (SELECT 1 FROM check_runs WHERE id = ?2) THEN 'missing' WHEN NOT EXISTS (SELECT 1 FROM check_runs WHERE id = ?2 AND reporter = ?1) THEN 'forbidden' WHEN NOT EXISTS (SELECT 1 FROM check_runs r JOIN check_contexts c ON c.name = r.context WHERE r.id = ?2 AND r.version = ?3 AND r.state IN ('queued', 'in_progress') AND c.enabled = 1 AND c.version = r.context_version AND c.reporter = ?1) THEN 'conflict' ELSE 'applied' END"
        );
        let check = SqlStatement {
            sql: format!("SELECT {decision}"),
            parameters: parameters.clone(),
        };
        parameters.extend([
            SqlValue::Text(input.state.as_str().into()),
            SqlValue::Text(input.summary.into()),
            SqlValue::Integer(identity.issued_at_ms),
        ]);
        self.check_change(identity, vec![check,
            SqlStatement { sql: format!("UPDATE check_runs SET state = ?4, summary = ?5, version = version + 1, updated_ms = max(updated_ms, ?6) WHERE id = ?2 AND ({decision}) = 'applied'"), parameters },
        ]).await
    }

    async fn check_change(
        &self,
        identity: MutationIdentity,
        statements: Vec<SqlStatement>,
    ) -> Result<Committed<CheckChange>, Invocation> {
        // Capture authority/version before mutation so advancing a version cannot
        // change the reported outcome; the runtime records the whole batch atomically.
        let result = self.sql.batch(identity, SqlBatch { statements }).await?;
        let output = match result
            .output
            .first()
            .and_then(|set| set.rows.first())
            .map(Vec::as_slice)
        {
            Some([SqlValue::Text(value)]) => match value.as_str() {
                "applied" => Some(CheckChange::Applied),
                "missing" => Some(CheckChange::NotFound),
                "forbidden" => Some(CheckChange::Forbidden),
                "conflict" => Some(CheckChange::Conflict),
                _ => None,
            },
            _ => None,
        }
        .ok_or_else(|| Invocation::InvalidPublishedResult {
            receipt: result.receipt,
            source: Box::new(Error::Command("invalid check mutation outcome")),
        })?;
        Ok(Committed {
            output,
            receipt: result.receipt,
        })
    }
}
