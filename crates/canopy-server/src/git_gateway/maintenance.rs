use super::*;
use std::sync::atomic::Ordering;

impl GitGateway {
    /// Best-effort maintenance has separate process admission and never owns a
    /// user transfer slot. A failed job leaves the previous generation serving.
    pub(crate) async fn maintain(&self) -> Result<(), GatewayError> {
        static JOBS: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(1);
        let Ok(_job) = JOBS.try_acquire() else {
            return Ok(());
        };
        let (old, through, generation) = {
            let Ok(objects) = self.objects.try_lock() else {
                return Ok(());
            };
            let Some(objects) = objects.as_ref() else {
                return Ok(());
            };
            if objects.cache.hydrating.load(Ordering::SeqCst) > 0 {
                return Ok(());
            }
            if objects.cache.loose_objects.load(Ordering::Relaxed) < 1024
                && objects.cache.pack_files.load(Ordering::Relaxed) < 8
            {
                return Ok(());
            }
            (
                Arc::clone(&objects.cache),
                objects.through,
                objects.cache.write_generation.load(Ordering::SeqCst),
            )
        };
        let started = std::time::Instant::now();
        let next = old
            .repacked(self.scratch_root.clone(), self.disk_budget.clone())
            .await?;
        // Same lock order as fetch_cache/build_cache. Foreground requests are
        // never locked out while pack-objects runs. Changed inventories retry.
        let mut refs = self.cache.lock().await;
        let mut objects = self.objects.lock().await;
        let Some(objects) = objects.as_mut() else {
            return Ok(());
        };
        if !Arc::ptr_eq(&objects.cache, &old)
            || objects.through != through
            || old.hydrating.load(Ordering::SeqCst) > 0
            || old.write_generation.load(Ordering::SeqCst) != generation
        {
            return Ok(());
        }
        tracing::info!(repository = %hex::encode(self.repository.repository_id()),
            index_entries = next.indexed_entries(), previous_bytes = old.bytes()?, packed_bytes = next.bytes()?,
            elapsed_seconds = started.elapsed().as_secs_f64(), "published background Git repack generation");
        self.pack_reader.replace(&old, Arc::clone(&next)).await;
        objects.cache = next;
        *refs = None;
        Ok(())
    }
}
