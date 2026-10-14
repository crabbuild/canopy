use super::*;

struct ActivePack {
    partition: PhysicalPartition,
    digest: [u8; 32],
    native: crate::packs::sources::NativePackDescriptor,
    custody: Option<crate::packs::publication::RetainedNativeInput>,
}
pub struct ClosureVerifier {
    spool: Arc<Mutex<Spool>>,
    activity: crate::git_objects::ReadOwner,
    context: ClosureContext,
    canceled: Arc<AtomicBool>,
    active: Option<ActivePack>,
    failed: bool,
    completed: bool,
}
impl ClosureVerifier {
    #[cfg(test)]
    pub(super) fn test_ownership(&self) -> (std::sync::Weak<Mutex<Spool>>, Arc<AtomicBool>) {
        (Arc::downgrade(&self.spool), self.canceled.clone())
    }
    pub async fn new(
        root: &Path,
        budget: DiskBudget,
        context: ClosureContext,
        limits: MetadataLimits,
    ) -> Result<Self, ClosureError> {
        Self::new_inner(root, budget, context, limits, None, Arc::new(())).await
    }
    pub(in crate::packs) async fn new_in_workspace_owned(
        workspace: Arc<tempfile::TempDir>,
        budget: DiskBudget,
        context: ClosureContext,
        limits: MetadataLimits,
        activity: crate::git_objects::ReadOwner,
    ) -> Result<Self, ClosureError> {
        let root = workspace.path().to_owned();
        Self::new_inner(&root, budget, context, limits, Some(workspace), activity).await
    }
    async fn new_inner(
        root: &Path,
        budget: DiskBudget,
        context: ClosureContext,
        limits: MetadataLimits,
        workspace: Option<Arc<tempfile::TempDir>>,
        activity: crate::git_objects::ReadOwner,
    ) -> Result<Self, ClosureError> {
        let canceled = Arc::new(AtomicBool::new(false));
        let mut guard = CancelGuard::new(canceled.clone());
        let root = root.to_owned();
        let token = canceled.clone();
        let worker_activity = activity.clone();
        let spool = tokio::task::spawn_blocking(move || {
            let _activity = worker_activity;
            let mut spool = Spool::new(&root, budget, context, limits, token)?;
            if let Some(workspace) = workspace {
                spool._admitted.retain_workspace(workspace);
            }
            Ok::<_, ClosureError>(spool)
        })
        .await??;
        guard.complete();
        Ok(Self {
            spool: Arc::new(Mutex::new(spool)),
            activity,
            context,
            canceled,
            active: None,
            failed: false,
            completed: false,
        })
    }
    fn healthy(&self) -> Result<(), ClosureError> {
        if self.failed {
            Err(ClosureError::Integrity)
        } else if self.canceled.load(Ordering::Acquire) {
            Err(ClosureError::Canceled)
        } else {
            Ok(())
        }
    }
    /// Consume only a complete isolated physical witness. Copy its metadata
    /// shards sequentially; a missing or mismatched shard prevents finishing.
    pub fn begin_pack(&mut self, witness: PhysicalPackWitness) -> Result<(), ClosureError> {
        self.begin_inner(witness, None)
    }
    pub(in crate::packs) fn begin_retained_pack(
        &mut self,
        witness: PhysicalPackWitness,
        custody: crate::packs::publication::RetainedNativeInput,
    ) -> Result<(), ClosureError> {
        self.begin_inner(witness, Some(custody))
    }
    fn begin_inner(
        &mut self,
        witness: PhysicalPackWitness,
        custody: Option<crate::packs::publication::RetainedNativeInput>,
    ) -> Result<(), ClosureError> {
        self.healthy()?;
        self.failed = true;
        let native = witness.native();
        if let Some(custody) = &custody {
            custody.authorize(self.context, native)?;
        }
        if self.active.is_some()
            || native.repository != self.context.repository
            || (native.operation != self.context.operation && custody.is_none())
            || native.format != self.context.format
        {
            return Err(ClosureError::Integrity);
        }
        self.active = Some(ActivePack {
            partition: witness.partition(),
            digest: witness.metadata_digest(),
            native,
            custody,
        });
        self.failed = false;
        Ok(())
    }
    pub async fn add_segment(&mut self, segment: Arc<MetadataSegment>) -> Result<(), ClosureError> {
        self.healthy()?;
        self.failed = true;
        let mut guard = CancelGuard::new(self.canceled.clone());
        let active = self.active.as_mut().ok_or(ClosureError::Integrity)?;
        if let Some(custody) = &active.custody {
            custody.authorize(self.context, active.native)?;
        }
        active.partition.add(segment.descriptor())?;
        let operation = active.native.operation;
        let spool = self.spool.clone();
        let activity = self.activity.clone();
        tokio::task::spawn_blocking(move || {
            let _activity = activity;
            spool
                .lock()
                .map_err(|_| ClosureError::Integrity)?
                .copy_segment(&segment, operation)
        })
        .await??;
        self.failed = false;
        guard.complete();
        Ok(())
    }
    pub async fn finish_pack(&mut self) -> Result<(), ClosureError> {
        self.healthy()?;
        self.failed = true;
        let mut guard = CancelGuard::new(self.canceled.clone());
        let active = self.active.take().ok_or(ClosureError::Integrity)?;
        if let Some(custody) = &active.custody {
            custody.authorize(self.context, active.native)?;
        }
        active.partition.finish()?;
        let spool = self.spool.clone();
        let activity = self.activity.clone();
        tokio::task::spawn_blocking(move || {
            let _activity = activity;
            spool
                .lock()
                .map_err(|_| ClosureError::Integrity)?
                .input(active.digest)
        })
        .await??;
        self.failed = false;
        guard.complete();
        Ok(())
    }
    /// No base is permitted only for an empty published dependency catalog.
    /// External lookup status is conditional on the trusted resolver contract;
    /// final publication must recheck the bound catalog generation and fence.
    pub async fn finish(
        self,
        resolver: Option<&impl BaseResolver>,
    ) -> Result<ClosureWitness, ClosureError> {
        let (witness, _) = self.finish_retained(resolver).await?;
        Ok(witness)
    }
    pub(in crate::packs) async fn finish_retained(
        mut self,
        resolver: Option<&impl BaseResolver>,
    ) -> Result<(ClosureWitness, RetainedClosure), ClosureError> {
        self.healthy()?;
        if self.active.is_some() || self.context.base.is_some() != resolver.is_some() {
            return Err(ClosureError::Integrity);
        }
        let mut guard = CancelGuard::new(self.canceled.clone());
        let spool = self.spool.clone();
        let activity = self.activity.clone();
        tokio::task::spawn_blocking(move || {
            let _activity = activity;
            spool
                .lock()
                .map_err(|_| ClosureError::Integrity)?
                .prepare_lookups()
        })
        .await??;
        if let (Some(base), Some(resolver)) = (self.context.base, resolver) {
            loop {
                let spool = self.spool.clone();
                let activity = self.activity.clone();
                let ids = tokio::task::spawn_blocking(move || {
                    let _activity = activity;
                    spool
                        .lock()
                        .map_err(|_| ClosureError::Integrity)?
                        .lookup_page()
                })
                .await??;
                if ids.is_empty() {
                    break;
                }
                let batch = resolver.resolve(base, &ids).await?;
                let spool = self.spool.clone();
                let activity = self.activity.clone();
                tokio::task::spawn_blocking(move || {
                    let _activity = activity;
                    spool
                        .lock()
                        .map_err(|_| ClosureError::Integrity)?
                        .apply_base(&ids, batch)
                })
                .await??;
            }
        }
        let spool = self.spool.clone();
        let activity = self.activity.clone();
        let witness = tokio::task::spawn_blocking(move || {
            let _activity = activity;
            let mut spool = spool.lock().map_err(|_| ClosureError::Integrity)?;
            spool.certify_graph()?;
            spool.witness()
        })
        .await??;
        guard.complete();
        self.completed = true;
        Ok((
            witness,
            RetainedClosure {
                spool: Arc::clone(&self.spool),
                context: self.context,
                canceled: Arc::clone(&self.canceled),
            },
        ))
    }
}
impl Drop for ClosureVerifier {
    fn drop(&mut self) {
        if !self.completed {
            self.canceled.store(true, Ordering::Release);
        }
    }
}
