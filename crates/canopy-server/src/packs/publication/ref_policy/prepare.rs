use super::*;

impl PreparedCatalog {
    /// Freeze original intent and catalog-verified evidence before minting
    /// pages. Caller roots, page lists and decoded certificates cannot enter.
    pub async fn ref_policy_preparation(
        &self,
        plan: PushPlan,
        root: &Path,
        budget: DiskBudget,
        limits: MetadataLimits,
    ) -> Result<RefPolicyPreparation, RefPolicyPreparationError> {
        let (_, deadline) = self.base.live_lease()?;
        timeout_at(deadline, async {
            if self.base().refs.is_none() {
                return Err(RefPolicyPreparationError::Context);
            }
            let (client, target, _) = self.base.capability();
            let sql = SqlCell::<RepositoryModule>::new(client.clone(), target.clone())?;
            let epoch = commands::epoch(
                &sql.query(
                    None,
                    SqlBatch {
                        statements: vec![SqlStatement {
                            sql: commands::EPOCH.into(),
                            parameters: vec![],
                        }],
                    },
                )
                .await
                .map_err(|error| RefPolicyPreparationError::Query(Box::new(error)))?
                .output,
            )?;
            let (plan, ancestry) = self.ref_evidence(plan, root, budget, limits).await?;
            let intent = RefPolicyIntent {
                id: *uuid::Uuid::new_v4().as_bytes(),
                epoch,
                updates: plan.updates.len() as u64,
                plan_digest: super::super::ref_proof::plan_digest(&plan)?,
                evidence_digest: super::super::ref_proof::binding(&plan, &ancestry)?,
            };
            self.ensure_live()?;
            Ok(RefPolicyPreparation {
                intent,
                token: self.token(),
                format: self.catalog().format,
                plan,
                ancestry,
            })
        })
        .await
        .map_err(|_| PreparationBaseError::Inactive)?
    }
    /// Conditional root certification only. The admitted final root/outcome
    /// command must still check this live guard, actual fence, ACL/pin and CAS.
    pub async fn guarded_ref_snapshot(
        &self,
        guard: &PreparedRefPolicyGuard,
        plan: PushPlan,
        root: &Path,
        budget: DiskBudget,
        limits: MetadataLimits,
    ) -> Result<RefRootPublicationProof, RefPolicyPreparationError> {
        let (_, deadline) = self.base.live_lease()?;
        timeout_at(deadline, async {
            if guard.token != self.token()
                || guard.actor != self.base.capability().2.actor
                || guard.format != self.catalog().format
                || guard.intent.plan_digest != super::super::ref_proof::plan_digest(&plan)?
            {
                return Err(RefPolicyPreparationError::Context);
            }
            let (plan, ancestry) = self.ref_evidence(plan, root, budget, limits).await?;
            if super::super::ref_proof::binding(&plan, &ancestry)? != guard.intent.evidence_digest {
                return Err(RefPolicyPreparationError::Context);
            }
            let snapshot = self.prepare_ref_snapshot(&plan).await?;
            if snapshot.base() != self.base() || snapshot.plan_digest() != guard.intent.plan_digest
            {
                return Err(RefPolicyPreparationError::Context);
            }
            ensure_ready(self, guard.intent).await?;
            let snapshot = snapshot.snapshot();
            let certificate = self
                .issue_certificate(Some(root_binding(guard.intent, snapshot)?), None)
                .await?;
            let value = RefRootPublicationProof {
                certificate,
                guard: guard.intent,
                snapshot,
            };
            value.shape()?;
            self.ensure_live()?;
            Ok(value)
        })
        .await
        .map_err(|_| PreparationBaseError::Inactive)?
    }
}
pub(in crate::packs::publication) async fn ensure_ready(
    prepared: &PreparedCatalog,
    intent: RefPolicyIntent,
) -> Result<(), RefPolicyPreparationError> {
    let (client, target, check) = prepared.base.capability();
    let progress = client
        .query::<CheckRefPolicyGuard>(
            target,
            None,
            RefPolicyLookup {
                check: check.clone(),
                intent,
            },
        )
        .await
        .map_err(|error| RefPolicyPreparationError::Guard(Box::new(error)))?
        .output;
    if !progress.is_some_and(|progress| progress.total == intent.updates && progress.ready()) {
        return Err(RefPolicyPreparationError::Context);
    }
    prepared.ensure_live()?;
    Ok(())
}
impl RefPolicyPreparation {
    pub fn intent(&self) -> RefPolicyIntent {
        self.intent
    }
    pub fn plan(&self) -> &PushPlan {
        &self.plan
    }
    pub fn into_plan(self) -> PushPlan {
        self.plan
    }
    fn matches(&self, prepared: &PreparedCatalog) -> bool {
        self.token == prepared.token()
            && self.format == prepared.catalog().format
            && self.plan.actor == prepared.base.capability().2.actor
    }
    /// At most 128 updates and 256 KiB, including certificate/framing. Byte
    /// boundaries may split ancestry bytes; copy only this bounded page.
    pub async fn page(
        &self,
        prepared: &PreparedCatalog,
        start: usize,
    ) -> Result<RefPolicyPage, RefPolicyPreparationError> {
        let (_, deadline) = prepared.base.live_lease()?;
        timeout_at(deadline, async {
            if !self.matches(prepared) || start >= self.plan.updates.len() {
                return Err(RefPolicyPreparationError::Context);
            }
            let mut used = CERTIFICATE_BYTES as usize + 512;
            let mut end = start;
            while end < self.plan.updates.len() && end - start < REF_POLICY_PAGE_UPDATES {
                let mut e = BoundedEncoder::new(REF_POLICY_PAGE_BYTES)?;
                crate::refs::encode_update(&self.plan.updates[end], &mut e)?;
                let n = e.finish().len();
                if used
                    .checked_add(n)
                    .is_none_or(|bytes| bytes > REF_POLICY_PAGE_BYTES as usize)
                {
                    break;
                }
                used += n;
                end += 1;
            }
            if end == start {
                return Err(CodecError::Limit.into());
            }
            let plan = PushPlan {
                actor: self.plan.actor.clone(),
                updates: self.plan.updates[start..end].to_vec(),
            };
            let mut ancestry = vec![0; plan.updates.len().div_ceil(8)];
            for i in 0..plan.updates.len() {
                if super::super::ref_proof::proven(&self.ancestry, start + i) {
                    ancestry[i / 8] |= 1 << (i % 8);
                }
            }
            let certificate = prepared
                .issue_certificate(
                    Some(page_payload_binding(
                        self.intent,
                        start as u64,
                        &plan,
                        &ancestry,
                    )?),
                    None,
                )
                .await?;
            let page = RefPolicyPage {
                intent: self.intent,
                offset: start as u64,
                proof: RefPublicationProof {
                    plan,
                    certificate,
                    ancestry,
                },
            };
            page.encode(&mut BoundedEncoder::new(REF_POLICY_PAGE_BYTES)?)?;
            prepared.ensure_live()?;
            Ok(page)
        })
        .await
        .map_err(|_| PreparationBaseError::Inactive)?
    }
    pub async fn ready(
        &self,
        prepared: &PreparedCatalog,
    ) -> Result<PreparedRefPolicyGuard, RefPolicyPreparationError> {
        let (_, deadline) = prepared.base.live_lease()?;
        timeout_at(deadline, async {
            if !self.matches(prepared) {
                return Err(RefPolicyPreparationError::Context);
            }
            ensure_ready(prepared, self.intent).await?;
            Ok(PreparedRefPolicyGuard {
                intent: self.intent,
                token: self.token,
                actor: self.plan.actor.clone(),
                format: self.format,
            })
        })
        .await
        .map_err(|_| PreparationBaseError::Inactive)?
    }
}
