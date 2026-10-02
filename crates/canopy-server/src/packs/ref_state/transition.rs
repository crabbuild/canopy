use super::*;
use std::collections::BTreeMap;

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
        if let Some(root) = &base {
            self.tree.validate_root(root.clone()).await?;
        }
        let updates: BTreeMap<&str, _> = plan
            .updates
            .iter()
            .map(|update| (update.name.as_str(), update))
            .collect();
        for update in &plan.updates {
            RefNameKey::new(&update.name)?;
            if self.read(base.clone(), &update.name).await? != update.expected {
                return Err(RefStateError::Changed);
            }
        }
        for update in plan
            .updates
            .iter()
            .filter(|update| update.new_oid.is_some())
        {
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
        if base.is_none() {
            let records = updates.values().map(|update| {
                RefStateRecord::new(
                    &update.name,
                    RefExpectation {
                        oid: update.new_oid,
                        version: 1,
                    },
                    self.format(),
                )
            });
            let root = self
                .tree
                .build_sorted(operation, records)
                .await?
                .ok_or(RefStateError::Changed)?;
            return Ok(RefTransition {
                base,
                root,
                plan_digest,
            });
        }
        let mut root = base.clone();
        for update in &plan.updates {
            let state = RefExpectation {
                oid: update.new_oid,
                version: update.expected.as_ref().map_or(1, |old| old.version + 1),
            };
            let replacement = RefStateRecord::new(&update.name, state, self.format())?;
            root = Some(if let Some(old) = &update.expected {
                self.tree
                    .replace(
                        root.ok_or(RefStateError::Changed)?,
                        operation,
                        RefStateRecord::new(&update.name, old.clone(), self.format())?,
                        replacement,
                    )
                    .await?
            } else {
                self.tree.insert(root, operation, replacement).await?
            });
        }
        Ok(RefTransition {
            base,
            root: root.ok_or(RefStateError::Changed)?,
            plan_digest,
        })
    }
}
