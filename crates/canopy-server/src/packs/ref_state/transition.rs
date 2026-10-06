use super::*;
use std::collections::{BTreeMap, btree_map::IntoValues};

// A named streaming adapter keeps borrowed update lifetimes explicit across
// recursive await boundaries; no second plan/record inventory is allocated.
struct Records<'a> {
    updates: IntoValues<&'a str, &'a crate::RefUpdate>,
    format: ObjectFormat,
}
impl Iterator for Records<'_> {
    type Item = Result<RefStateRecord, IndexError>;
    fn next(&mut self) -> Option<Self::Item> {
        let update = self.updates.next()?;
        Some(RefStateRecord::new(
            &update.name,
            RefExpectation {
                oid: update.new_oid,
                version: update.expected.as_ref().map_or(1, |old| old.version + 1),
            },
            self.format,
        ))
    }
}

impl RefStateIndex {
    /// Validate the whole plan before writing any tree nodes. The selected
    /// root must come from certified current state before final publication.
    pub async fn prepare(
        &self,
        base: Option<RefStateRoot>,
        operation: [u8; 16],
        plan: &PushPlan,
    ) -> Result<RefTransition, RefStateError> {
        super::super::publication::codec::artifact_valid(operation)?;
        super::super::publication::ref_proof::shape(plan, self.format())?;
        self.prepare_checked(base, operation, plan).await
    }

    /// Private reserved-ref creation. This tree output grants no write authority;
    /// the generated publisher must authenticate the verified candidate scope.
    pub(crate) async fn prepare_candidate(
        &self,
        base: Option<RefStateRoot>,
        operation: [u8; 16],
        candidate: &crate::pulls::candidates::MergeCandidate,
    ) -> Result<RefTransition, RefStateError> {
        use crate::pulls::candidates::{CandidateResult, valid_request};
        let CandidateResult::Ready { oid, .. } = &candidate.result else {
            return Err(RefStateError::Changed);
        };
        if !valid_request(&candidate.request)
            || crate::directory::validate_component(&candidate.actor).is_err()
        {
            return Err(RefStateError::Changed);
        }
        let oid = crate::pulls::merge::oid(oid).map_err(|_| RefStateError::Changed)?;
        if oid.format() != self.format() || oid.is_zero() {
            return Err(RefStateError::Changed);
        }
        super::super::publication::codec::artifact_valid(operation)?;
        let plan = PushPlan {
            actor: candidate.actor.clone(),
            updates: vec![crate::RefUpdate {
                name: candidate.fetch_ref(),
                expected: None,
                new_oid: Some(oid),
            }],
        };
        self.prepare_checked(base, operation, &plan).await
    }

    async fn prepare_checked(
        &self,
        base: Option<RefStateRoot>,
        operation: [u8; 16],
        plan: &PushPlan,
    ) -> Result<RefTransition, RefStateError> {
        if let Some(root) = &base {
            self.tree.validate_root(root.clone()).await?;
        }
        let updates: BTreeMap<&str, _> = plan
            .updates
            .iter()
            .map(|update| (update.name.as_str(), update))
            .collect();
        for update in updates.values() {
            RefNameKey::new(&update.name)?;
            if self.read(base.clone(), &update.name).await? != update.expected {
                return Err(RefStateError::Changed);
            }
        }
        for update in updates.values().filter(|update| update.new_oid.is_some()) {
            let name = update.name.as_str();
            for (at, _) in name.match_indices('/').filter(|(at, _)| *at > 4) {
                let ancestor = &name[..at];
                let live = if let Some(update) = updates.get(ancestor) {
                    update.new_oid.is_some()
                } else {
                    self.read(base.clone(), ancestor)
                        .await?
                        .is_some_and(|state| state.oid.is_some())
                };
                if live {
                    return Err(RefStateError::Namespace);
                }
            }
            let prefix = format!("{name}/");
            let end = format!("{name}0");
            if updates
                .range(prefix.as_str()..end.as_str())
                .any(|(_, update)| update.new_oid.is_some())
            {
                return Err(RefStateError::Namespace);
            }
            if name.len() == MAX_NAME_BYTES {
                continue;
            }
            // The slash prefix cannot itself be a leaf. Seek directly to its
            // interval, avoiding unrelated names between `name` and `name/`.
            let mut cursor = self.cursor(base.clone(), Some(RefNameKey::new(&prefix)?), true)?;
            while let Some(existing) = cursor.next().await? {
                if existing.name() >= end.as_str() {
                    break;
                }
                if existing.name() < prefix.as_str() {
                    continue;
                }
                if updates
                    .get(existing.name())
                    .is_none_or(|update| update.new_oid.is_some())
                {
                    return Err(RefStateError::Namespace);
                }
            }
        }
        let plan_digest = super::super::publication::ref_proof::plan_digest(plan)?;
        let records = Records {
            updates: updates.into_values(),
            format: self.format(),
        };
        let root = self
            .tree
            .upsert_sorted(base.clone(), operation, records)
            .await?
            .ok_or(RefStateError::Changed)?;
        Ok(RefTransition {
            base,
            root,
            plan_digest,
        })
    }
}
