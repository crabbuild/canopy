use super::*;

impl RepositoryCell {
    /// Creates an issue once per UUID and original author/content binding.
    ///
    /// Rechecks repository read access at publication. Exact retries return the
    /// existing number even after edits; reusing the UUID for other input conflicts.
    pub async fn create_issue(
        &self,
        identity: MutationIdentity,
        actor: &str,
        input: NewIssue<'_>,
    ) -> Result<Committed<IssueChange>, Invocation> {
        if !valid_issue_text(input.title, input.body) {
            return Err(Invocation::NotStarted(Error::Command("invalid issue text")));
        }
        validate_repository_id(input.id).map_err(Invocation::NotStarted)?;
        let mut parameters = actor_parameters(actor).map_err(Invocation::NotStarted)?;
        parameters.extend([
            SqlValue::Blob(input.id.to_vec()),
            SqlValue::Blob(binding(&[actor, input.title, input.body])),
        ]);
        let decision = format!(
            "CASE WHEN NOT ({READ_ACCESS}) THEN 'missing' WHEN EXISTS (SELECT 1 FROM issues WHERE id = ?2 AND creation_digest != ?3) THEN 'conflict' ELSE 'applied' END"
        );
        let check = SqlStatement {
            sql: format!("SELECT {decision}"),
            parameters: parameters.clone(),
        };
        parameters.extend([
            SqlValue::Text(input.title.into()),
            SqlValue::Text(input.body.into()),
            SqlValue::Integer(identity.issued_at_ms),
        ]);
        self.issue_change(identity, vec![
            check,
            SqlStatement {
                sql: format!("INSERT INTO issues (id, creation_digest, author, title, body, state, version, created_ms, updated_ms) SELECT ?2, ?3, ?1, ?4, ?5, 'open', 1, ?6, ?6 WHERE ({decision}) = 'applied' AND NOT EXISTS (SELECT 1 FROM issues WHERE id = ?2)"),
                parameters,
            },
            SqlStatement { sql: "SELECT number FROM issues WHERE id = ?1".into(), parameters: vec![SqlValue::Blob(input.id.to_vec())] },
        ]).await
    }

    /// Replaces issue content/state for its author or a repository writer.
    ///
    /// Membership and the expected version are checked in the update transaction.
    pub async fn edit_issue(
        &self,
        identity: MutationIdentity,
        actor: &str,
        number: i64,
        input: IssueEdit<'_>,
    ) -> Result<Committed<IssueChange>, Invocation> {
        if number < 1
            || !(1..i64::MAX).contains(&input.expected_version)
            || !valid_issue_text(input.title, input.body)
        {
            return Err(Invocation::NotStarted(Error::Command(
                "invalid issue update",
            )));
        }
        let mut parameters = actor_parameters(actor).map_err(Invocation::NotStarted)?;
        parameters.extend([
            SqlValue::Integer(number),
            SqlValue::Integer(input.expected_version),
        ]);
        let decision = format!(
            "CASE WHEN NOT ({READ_ACCESS}) OR NOT EXISTS (SELECT 1 FROM issues WHERE number = ?2) THEN 'missing' WHEN NOT ({WRITE_ACCESS}) AND NOT EXISTS (SELECT 1 FROM issues WHERE number = ?2 AND author = ?1) THEN 'forbidden' WHEN NOT EXISTS (SELECT 1 FROM issues WHERE number = ?2 AND version = ?3) THEN 'conflict' ELSE 'applied' END"
        );
        let check = SqlStatement {
            sql: format!("SELECT {decision}"),
            parameters: parameters.clone(),
        };
        parameters.extend([
            SqlValue::Text(input.title.into()),
            SqlValue::Text(input.body.into()),
            SqlValue::Text(input.state.as_str().into()),
            SqlValue::Integer(identity.issued_at_ms),
        ]);
        self.issue_change(identity, vec![
            check,
            SqlStatement {
                sql: format!("UPDATE issues SET title = ?4, body = ?5, state = ?6, version = version + 1, updated_ms = max(updated_ms, ?7) WHERE number = ?2 AND ({decision}) = 'applied'"),
                parameters,
            },
            SqlStatement { sql: "SELECT number FROM issues WHERE number = ?1".into(), parameters: vec![SqlValue::Integer(number)] },
        ]).await
    }

    /// Adds a comment once per UUID bound to its parent, author and original body.
    ///
    /// Both open and closed issues accept comments from current repository readers.
    pub async fn create_issue_comment(
        &self,
        identity: MutationIdentity,
        actor: &str,
        number: i64,
        input: NewComment<'_>,
    ) -> Result<Committed<IssueChange>, Invocation> {
        if number < 1 || input.body.trim().is_empty() || !valid_body(input.body) {
            return Err(Invocation::NotStarted(Error::Command(
                "invalid issue comment",
            )));
        }
        validate_repository_id(input.id).map_err(Invocation::NotStarted)?;
        let mut parameters = actor_parameters(actor).map_err(Invocation::NotStarted)?;
        parameters.extend([
            SqlValue::Integer(number),
            SqlValue::Blob(input.id.to_vec()),
            SqlValue::Blob(binding(&[actor, input.body])),
        ]);
        let decision = format!(
            "CASE WHEN NOT ({READ_ACCESS}) OR NOT EXISTS (SELECT 1 FROM issues WHERE number = ?2) THEN 'missing' WHEN EXISTS (SELECT 1 FROM issue_comments WHERE id = ?3 AND (issue_number != ?2 OR creation_digest != ?4)) THEN 'conflict' ELSE 'applied' END"
        );
        let check = SqlStatement {
            sql: format!("SELECT {decision}"),
            parameters: parameters.clone(),
        };
        parameters.extend([
            SqlValue::Text(input.body.into()),
            SqlValue::Integer(identity.issued_at_ms),
        ]);
        self.issue_change(identity, vec![
            check,
            SqlStatement {
                sql: format!("INSERT INTO issue_comments (issue_number, id, creation_digest, author, body, version, created_ms, updated_ms) SELECT ?2, ?3, ?4, ?1, ?5, 1, ?6, ?6 WHERE ({decision}) = 'applied' AND NOT EXISTS (SELECT 1 FROM issue_comments WHERE id = ?3)"),
                parameters,
            },
            SqlStatement { sql: "SELECT number FROM issue_comments WHERE id = ?1".into(), parameters: vec![SqlValue::Blob(input.id.to_vec())] },
        ]).await
    }

    /// Replaces a comment for its author or a repository writer at one version.
    pub async fn edit_issue_comment(
        &self,
        identity: MutationIdentity,
        actor: &str,
        issue: i64,
        number: i64,
        input: CommentEdit<'_>,
    ) -> Result<Committed<IssueChange>, Invocation> {
        if issue < 1
            || number < 1
            || !(1..i64::MAX).contains(&input.expected_version)
            || input.body.trim().is_empty()
            || !valid_body(input.body)
        {
            return Err(Invocation::NotStarted(Error::Command(
                "invalid comment update",
            )));
        }
        let mut parameters = actor_parameters(actor).map_err(Invocation::NotStarted)?;
        parameters.extend([
            SqlValue::Integer(issue),
            SqlValue::Integer(number),
            SqlValue::Integer(input.expected_version),
        ]);
        let decision = format!(
            "CASE WHEN NOT ({READ_ACCESS}) OR NOT EXISTS (SELECT 1 FROM issue_comments WHERE number = ?3 AND issue_number = ?2) THEN 'missing' WHEN NOT ({WRITE_ACCESS}) AND NOT EXISTS (SELECT 1 FROM issue_comments WHERE number = ?3 AND author = ?1) THEN 'forbidden' WHEN NOT EXISTS (SELECT 1 FROM issue_comments WHERE number = ?3 AND version = ?4) THEN 'conflict' ELSE 'applied' END"
        );
        let check = SqlStatement {
            sql: format!("SELECT {decision}"),
            parameters: parameters.clone(),
        };
        parameters.extend([
            SqlValue::Text(input.body.into()),
            SqlValue::Integer(identity.issued_at_ms),
        ]);
        self.issue_change(identity, vec![
            check,
            SqlStatement {
                sql: format!("UPDATE issue_comments SET body = ?5, version = version + 1, updated_ms = max(updated_ms, ?6) WHERE number = ?3 AND issue_number = ?2 AND ({decision}) = 'applied'"),
                parameters,
            },
            SqlStatement { sql: "SELECT number FROM issue_comments WHERE number = ?1".into(), parameters: vec![SqlValue::Integer(number)] },
        ]).await
    }

    async fn issue_change(
        &self,
        identity: MutationIdentity,
        statements: Vec<SqlStatement>,
    ) -> Result<Committed<IssueChange>, Invocation> {
        // Save the pre-mutation decision with the result. Rechecking a version
        // after advancing it would misreport a successful edit as a conflict.
        let committed = self.sql.batch(identity, SqlBatch { statements }).await?;
        let output = decode_change(&committed.output).map_err(|source| {
            Invocation::InvalidPublishedResult {
                receipt: committed.receipt,
                source: Box::new(source),
            }
        })?;
        Ok(Committed {
            output,
            receipt: committed.receipt,
        })
    }
}

fn binding(fields: &[&str]) -> Vec<u8> {
    let mut hash = blake3::Hasher::new();
    hash.update(b"canopy-issue-create-v1");
    for field in fields {
        hash.update(&(field.len() as u64).to_le_bytes());
        hash.update(field.as_bytes());
    }
    hash.finalize().as_bytes().to_vec()
}

fn decode_change(sets: &[SqlResultSet]) -> cellule_runtime::Result<IssueChange> {
    match sets
        .first()
        .and_then(|set| set.rows.first())
        .map(Vec::as_slice)
    {
        Some([SqlValue::Text(value)]) if value == "missing" => Ok(IssueChange::NotFound),
        Some([SqlValue::Text(value)]) if value == "forbidden" => Ok(IssueChange::Forbidden),
        Some([SqlValue::Text(value)]) if value == "conflict" => Ok(IssueChange::Conflict),
        Some([SqlValue::Text(value)]) if value == "applied" => {
            match sets
                .get(2)
                .and_then(|set| set.rows.first())
                .map(Vec::as_slice)
            {
                Some([SqlValue::Integer(number)]) if *number > 0 => {
                    Ok(IssueChange::Applied(*number))
                }
                _ => Err(Error::Command("missing issue mutation result")),
            }
        }
        _ => Err(Error::Command("invalid issue mutation outcome")),
    }
}
