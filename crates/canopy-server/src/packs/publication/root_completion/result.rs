//! Pre-encode one immutable terminal selection before any transaction writes.
//! Both joint publication and ref-free completion use these same durable rows.
use super::super::{publish::changed, sql::*};
use super::*;
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Selection {
    Native,
    Rejected,
    Replayed,
}
pub(super) struct PreparedResult {
    result: RootCompletionReply,
    saved: SqlBatch,
    signed: Option<SqlBatch>,
    operation: [u8; 16],
}
impl PreparedResult {
    pub(super) fn new(
        check: &LeaseCheck,
        outcomes: &RootPushOutcomes,
        binding: [u8; 32],
        publication: Option<PublishedRefs>,
        selection: Selection,
        plan: Option<[u8; 32]>,
        now_ms: i64,
    ) -> cellule_runtime::Result<Self> {
        let rejected = selection != Selection::Native;
        let root = match selection {
            Selection::Native => outcomes.native,
            Selection::Rejected => outcomes.rejected,
            Selection::Replayed => outcomes.replayed,
        };
        let result = RootCompletionReply::Completed(Box::new(CompletedRootPush {
            completion: CompletedCatalogPush {
                response_id: outcomes.response_id,
                rejected,
                publication,
            },
            root,
        }));
        result.encode(&mut BoundedEncoder::new(512)?)?;
        let mut encoded_root = BoundedEncoder::new(128)?;
        root.encode(&mut encoded_root)?;
        let mut encoded_publication = BoundedEncoder::new(128)?;
        if let Some(value) = publication {
            value.encode(&mut encoded_publication)?;
        }
        let reason = match selection {
            Selection::Native => SqlValue::Null,
            Selection::Rejected => SqlValue::Text(crate::push::report::REJECTED.into()),
            Selection::Replayed => SqlValue::Text(REPLAYED.into()),
        };
        let saved = statement(
            "INSERT INTO pushes(id,actor,request_digest,response_id,completion_digest,rejected,rejection_reason,publication,publication_plan_digest,response_root) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)",
            vec![
                blob(check.token.operation),
                SqlValue::Text(check.actor.clone()),
                blob(check.token.request_digest),
                blob(outcomes.response_id),
                blob(binding),
                SqlValue::Integer(i64::from(rejected)),
                reason,
                if publication.is_some() {
                    blob(encoded_publication.finish())
                } else {
                    SqlValue::Null
                },
                plan.map_or(SqlValue::Null, blob),
                blob(encoded_root.finish()),
            ],
        );
        let signed = if selection != Selection::Replayed {
            outcomes.signed.as_ref().map(|signed| -> cellule_runtime::Result<SqlBatch> {
                Ok(statement("INSERT INTO push_certificates(digest,push_id,actor,signer,key,size,recorded_at_ms) VALUES(?1,?2,?3,?3,?4,?5,?6)", vec![blob(signed.digest), blob(check.token.operation), SqlValue::Text(check.actor.clone()), SqlValue::Text(signed.key.clone()), number(signed.size)?, SqlValue::Integer(now_ms)]))
            }).transpose()?
        } else {
            None
        };
        Ok(Self {
            result,
            saved,
            signed,
            operation: check.token.operation,
        })
    }
    pub(super) fn pending(mut self, pending: bool) -> Self {
        if pending {
            self.saved.statements[0].sql = "UPDATE pushes SET response_id=?4,completion_digest=?5,rejected=?6,rejection_reason=?7,publication=?8,publication_plan_digest=?9,response_root=?10 WHERE id=?1 AND actor=?2 AND request_digest=?3 AND response_id IS NULL AND response_root IS NULL AND publication IS NULL".into();
        }
        self
    }
    /// Every authority/capacity/encoding check must precede this call. A late
    /// SQL error aborts the entire caller's transaction; no denial follows writes.
    pub(super) fn save(
        self,
        context: &mut CommandContext<'_, '_>,
    ) -> cellule_runtime::Result<RootCompletionReply> {
        changed(context.sql(&self.saved)?)?;
        if let Some(signed) = self.signed {
            changed(context.sql(&signed)?)?;
        }
        changed(context.sql(&statement(
            "DELETE FROM catalog_operations WHERE id=?1",
            vec![blob(self.operation)],
        ))?)?;
        Ok(self.result)
    }
}
