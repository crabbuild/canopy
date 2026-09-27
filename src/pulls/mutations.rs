use super::*;

impl RepositoryCell {
    /// Opens a pull against current stored branch tips, once per UUID/payload binding.
    ///
    /// Current members may propose changes. Exact retries preserve later edits;
    /// a new UUID with stale or missing source/base tips conflicts.
    pub async fn create_pull(
        &self,
        identity: MutationIdentity,
        actor: &str,
        input: NewPull<'_>,
    ) -> Result<Committed<PullChange>, Invocation> {
        validate_component(actor).map_err(Invocation::NotStarted)?;
        validate_repository_id(input.id).map_err(Invocation::NotStarted)?;
        if !valid_new(&input) {
            return Err(invalid("invalid pull creation"));
        }
        let mut parameters = vec![
            SqlValue::Text(actor.into()),
            SqlValue::Blob(input.id.to_vec()),
            SqlValue::Blob(binding(&[
                actor,
                input.title,
                input.body,
                if input.draft { "draft" } else { "ready" },
                input.source_ref,
                input.source_oid,
                input.base_ref,
                input.base_oid,
            ])),
            SqlValue::Text(input.source_ref.into()),
            SqlValue::Blob(
                parse_oid(input.source_oid).ok_or_else(|| invalid("invalid source OID"))?,
            ),
            SqlValue::Text(input.base_ref.into()),
            SqlValue::Blob(parse_oid(input.base_oid).ok_or_else(|| invalid("invalid base OID"))?),
        ];
        let decision = format!(
            "CASE WHEN NOT ({ACCESS}) THEN 'missing' WHEN EXISTS (SELECT 1 FROM pull_requests WHERE id = ?2 AND creation_digest != ?3) THEN 'conflict' WHEN EXISTS (SELECT 1 FROM pull_requests WHERE id = ?2) THEN 'applied' WHEN NOT EXISTS (SELECT 1 FROM refs WHERE name = ?4 AND oid = ?5) OR NOT EXISTS (SELECT 1 FROM refs WHERE name = ?6 AND oid = ?7) OR ?5 = ?7 THEN 'conflict' ELSE 'applied' END"
        );
        let check = SqlStatement {
            sql: format!("SELECT {decision}"),
            parameters: parameters.clone(),
        };
        parameters.extend([
            SqlValue::Text(input.title.into()),
            SqlValue::Text(input.body.into()),
            SqlValue::Integer(i64::from(input.draft)),
            SqlValue::Integer(identity.issued_at_ms),
        ]);
        self.pull_change(identity, vec![check,
            SqlStatement { sql: format!("INSERT INTO pull_requests (id, creation_digest, author, title, body, state, draft, version, source_ref, initial_source_oid, base_ref, initial_base_oid, created_ms, updated_ms) SELECT ?2, ?3, ?1, ?8, ?9, 'open', ?10, 1, ?4, ?5, ?6, ?7, ?11, ?11 WHERE ({decision}) = 'applied' AND NOT EXISTS (SELECT 1 FROM pull_requests WHERE id = ?2)"), parameters },
            SqlStatement { sql: "SELECT number FROM pull_requests WHERE id = ?1".into(), parameters: vec![SqlValue::Blob(input.id.to_vec())] },
        ]).await
    }
    /// Replaces editorial fields for the author or a current repository writer.
    ///
    /// Ref names are immutable. Every accepted edit advances the pull version,
    /// making earlier approvals inapplicable until the new revision is reviewed.
    pub async fn edit_pull(
        &self,
        identity: MutationIdentity,
        actor: &str,
        number: i64,
        input: PullEdit<'_>,
    ) -> Result<Committed<PullChange>, Invocation> {
        validate_component(actor).map_err(Invocation::NotStarted)?;
        if number < 1
            || input.state == PullState::Merged
            || !(1..i64::MAX).contains(&input.expected_version)
            || !valid_issue_text(input.title, input.body)
        {
            return Err(invalid("invalid pull edit"));
        }
        let mut parameters = vec![
            SqlValue::Text(actor.into()),
            SqlValue::Integer(number),
            SqlValue::Integer(input.expected_version),
        ];
        let decision = format!(
            "CASE WHEN NOT ({ACCESS}) OR NOT EXISTS (SELECT 1 FROM pull_requests WHERE number = ?2) THEN 'missing' WHEN NOT ({WRITE}) AND NOT EXISTS (SELECT 1 FROM pull_requests WHERE number = ?2 AND author = ?1) THEN 'forbidden' WHEN NOT EXISTS (SELECT 1 FROM pull_requests WHERE number = ?2 AND version = ?3 AND state != 'merged') THEN 'conflict' ELSE 'applied' END"
        );
        let check = SqlStatement {
            sql: format!("SELECT {decision}"),
            parameters: parameters.clone(),
        };
        parameters.extend([
            SqlValue::Text(input.title.into()),
            SqlValue::Text(input.body.into()),
            SqlValue::Text(input.state.as_str().into()),
            SqlValue::Integer(i64::from(input.draft)),
            SqlValue::Integer(identity.issued_at_ms),
        ]);
        self.pull_change(identity, vec![check,
            SqlStatement { sql: format!("UPDATE pull_requests SET title = ?4, body = ?5, state = ?6, draft = ?7, version = version + 1, updated_ms = max(updated_ms, ?8) WHERE number = ?2 AND ({decision}) = 'applied'"), parameters },
            SqlStatement { sql: "SELECT number FROM pull_requests WHERE number = ?1".into(), parameters: vec![SqlValue::Integer(number)] },
        ]).await
    }
    /// Appends an immutable review at the exact current pull/ref revision.
    ///
    /// Decisions require a current writer other than the pull author. Members
    /// may comment. Exact retries retain the original number and review order.
    pub async fn review_pull(
        &self,
        identity: MutationIdentity,
        actor: &str,
        number: i64,
        input: NewReview<'_>,
    ) -> Result<Committed<PullChange>, Invocation> {
        validate_component(actor).map_err(Invocation::NotStarted)?;
        validate_repository_id(input.id).map_err(Invocation::NotStarted)?;
        if number < 1 || !valid_review(&input) {
            return Err(invalid("invalid pull review"));
        }
        let revision = input.revision;
        let mut parameters = vec![
            SqlValue::Text(actor.into()),
            SqlValue::Integer(number),
            SqlValue::Blob(input.id.to_vec()),
            SqlValue::Blob(binding(&[
                actor,
                input.kind.as_str(),
                input.body,
                &revision.pull_version.to_string(),
                &revision.source_oid,
                &revision.source_version.to_string(),
                &revision.base_oid,
                &revision.base_version.to_string(),
            ])),
            SqlValue::Text(input.kind.as_str().into()),
            SqlValue::Integer(revision.pull_version),
            SqlValue::Blob(
                parse_oid(&revision.source_oid).ok_or_else(|| invalid("invalid source OID"))?,
            ),
            SqlValue::Integer(revision.source_version),
            SqlValue::Blob(
                parse_oid(&revision.base_oid).ok_or_else(|| invalid("invalid base OID"))?,
            ),
            SqlValue::Integer(revision.base_version),
        ];
        // Retry identity precedes current revision eligibility, but never access.
        // A historical retry can return its review without creating a new decision.
        let decision = format!(
            "CASE WHEN NOT ({ACCESS}) OR NOT EXISTS (SELECT 1 FROM pull_requests WHERE number = ?2) THEN 'missing' WHEN EXISTS (SELECT 1 FROM pull_reviews WHERE id = ?3 AND (pull_number != ?2 OR creation_digest != ?4)) THEN 'conflict' WHEN EXISTS (SELECT 1 FROM pull_reviews WHERE id = ?3) THEN 'applied' WHEN ?5 != 'comment' AND (NOT ({WRITE}) OR EXISTS (SELECT 1 FROM pull_requests WHERE number = ?2 AND author = ?1)) THEN 'forbidden' WHEN NOT EXISTS (SELECT 1 FROM {JOINS} WHERE p.number = ?2 AND p.version = ?6 AND p.state = 'open' AND (?5 = 'comment' OR p.draft = 0) AND s.oid = ?7 AND s.version = ?8 AND b.oid = ?9 AND b.version = ?10 AND s.oid != b.oid) THEN 'conflict' ELSE 'applied' END"
        );
        let check = SqlStatement {
            sql: format!("SELECT {decision}"),
            parameters: parameters.clone(),
        };
        parameters.extend([
            SqlValue::Text(input.body.into()),
            SqlValue::Integer(identity.issued_at_ms),
        ]);
        let review_binding = parameters[3].clone();
        self.pull_change(identity, vec![check,
            // Public commenters may have no grant history. Version zero cannot
            // authorize an approval: only the owner or a current writer qualifies.
            SqlStatement { sql: format!("INSERT INTO pull_reviews (id, creation_digest, pull_number, reviewer, membership_version, kind, body, pull_version, source_oid, source_version, base_oid, base_version, created_ms) SELECT ?3, ?4, ?2, ?1, CASE WHEN EXISTS (SELECT 1 FROM repository_identity WHERE owner = ?1) THEN 0 ELSE coalesce((SELECT version FROM membership_versions WHERE account = ?1), 0) END, ?5, ?11, ?6, ?7, ?8, ?9, ?10, ?12 WHERE ({decision}) = 'applied' AND NOT EXISTS (SELECT 1 FROM pull_reviews WHERE id = ?3)"), parameters },
            // Only an inserted or exact-bound decision can advance its reviewer's
            // head. Historical retries cannot replace a newer decision; comments
            // never enter this table.
            SqlStatement { sql: "INSERT INTO pull_review_heads (pull_number, reviewer, review_number) SELECT pull_number, reviewer, number FROM pull_reviews WHERE id = ?1 AND creation_digest = ?2 AND pull_number = ?3 AND kind != 'comment' AND (EXISTS (SELECT 1 FROM repository_identity WHERE owner = ?4) OR EXISTS (SELECT 1 FROM repository_members WHERE account = ?4)) ON CONFLICT(pull_number, reviewer) DO UPDATE SET review_number = excluded.review_number WHERE excluded.review_number > pull_review_heads.review_number".into(), parameters: vec![SqlValue::Blob(input.id.to_vec()), review_binding, SqlValue::Integer(number), SqlValue::Text(actor.into())] },
            SqlStatement { sql: "SELECT number FROM pull_reviews WHERE id = ?1".into(), parameters: vec![SqlValue::Blob(input.id.to_vec())] },
        ]).await
    }
    pub(super) async fn pull_change(
        &self,
        identity: MutationIdentity,
        statements: Vec<SqlStatement>,
    ) -> Result<Committed<PullChange>, Invocation> {
        let committed = self.sql.batch(identity, SqlBatch { statements }).await?;
        let output =
            change(&committed.output).map_err(|source| Invocation::InvalidPublishedResult {
                receipt: committed.receipt,
                source: Box::new(source),
            })?;
        Ok(Committed {
            output,
            receipt: committed.receipt,
        })
    }
}
pub(super) fn binding(fields: &[&str]) -> Vec<u8> {
    let mut hash = blake3::Hasher::new();
    hash.update(b"canopy-pull-record-v1");
    for field in fields {
        hash.update(&(field.len() as u64).to_le_bytes());
        hash.update(field.as_bytes());
    }
    hash.finalize().as_bytes().to_vec()
}
fn change(sets: &[SqlResultSet]) -> crab_cell_runtime::Result<PullChange> {
    match sets
        .first()
        .and_then(|set| set.rows.first())
        .map(Vec::as_slice)
    {
        Some([SqlValue::Text(value)]) if value == "missing" => Ok(PullChange::NotFound),
        Some([SqlValue::Text(value)]) if value == "forbidden" => Ok(PullChange::Forbidden),
        Some([SqlValue::Text(value)]) if value == "conflict" => Ok(PullChange::Conflict),
        Some([SqlValue::Text(value)]) if value == "applied" => match sets
            .last()
            .and_then(|set| set.rows.first())
            .map(Vec::as_slice)
        {
            Some([SqlValue::Integer(number)]) if *number > 0 => Ok(PullChange::Applied(*number)),
            _ => Err(Error::Command("missing pull mutation result")),
        },
        _ => Err(Error::Command("invalid pull mutation result")),
    }
}
