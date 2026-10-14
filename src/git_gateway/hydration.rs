use super::*;
use std::time::{Duration, Instant};

#[derive(Default)]
pub(super) struct Hydration {
    pub(super) objects: u64,
    pub(super) bytes: u64,
    pub(super) body_time: Duration,
    pub(super) cache_time: Duration,
}

impl GitGateway {
    pub(super) async fn hydrate(&self, shared: &mut CachedObjects) -> Result<(), GatewayError> {
        let started = Instant::now();
        let cache = &shared.cache;
        let cursor = &mut shared.through;
        let from_sequence = *cursor;
        // Bound this refresh even when other writers keep appending objects.
        // The read follows the chosen ref snapshot, whose objects are durable.
        let high_water = self
            .repository
            .object_high_water()
            .await
            .map_err(|error| GatewayError::Cell(Box::new(error)))?;
        let mut page_time = Duration::ZERO;
        let mut stats = Hydration::default();
        let mut scanned = 0_u64;
        while *cursor < high_water.output {
            let queried = Instant::now();
            let mut headers = self
                .repository
                .object_headers(*cursor, &high_water)
                .await
                .map_err(|error| GatewayError::Cell(Box::new(error)))?;
            if headers.objects.output.is_empty() {
                return Err(GatewayError::MalformedCache);
            }
            scanned += headers.objects.output.len() as u64;
            headers.objects.output = cache.missing_objects(headers.objects.output).await?;
            let page = self
                .repository
                .object_records(headers.objects)
                .await
                .map_err(|error| GatewayError::Cell(Box::new(error)))?
                .output;
            page_time += queried.elapsed();
            for object in page {
                self.cache_object(cache, object, &mut stats).await?;
            }
            // Failed or cancelled pages retain their previous cursor. Verified
            // files can be reused on retry, but no missing body is skipped.
            *cursor = headers.through;
        }
        tracing::debug!(
            repository = %hex::encode(self.repository.repository_id()),
            objects = stats.objects,
            scanned,
            from_sequence,
            through_sequence = *cursor,
            reused = scanned - stats.objects,
            bytes = stats.bytes,
            cache_bytes = cache.bytes()?,
            elapsed_seconds = started.elapsed().as_secs_f64(),
            page_seconds = page_time.as_secs_f64(),
            body_seconds = stats.body_time.as_secs_f64(),
            cache_seconds = stats.cache_time.as_secs_f64(),
            "hydrated Git cache"
        );
        Ok(())
    }

    pub(super) async fn cache_object(
        &self,
        cache: &Arc<GitCache>,
        object: StoredObject,
        stats: &mut Hydration,
    ) -> Result<Option<crate::ObjectId>, GatewayError> {
        let read = Instant::now();
        let ObjectStorage::Packed(reference) = object.storage else {
            return Err(GatewayError::MalformedCache);
        };
        let pack = self.repository.packs.load(&reference.pack).await?;
        let header = pack
            .data()?
            .header(object.oid)?
            .ok_or(GatewayError::MalformedCache)?;
        if header.kind != object.kind || header.size != reference.size {
            return Err(GatewayError::MalformedCache);
        }
        let target = if object.kind == ObjectKind::Tag {
            let edges = self
                .repository
                .dependencies(object.oid)
                .await
                .map_err(|error| GatewayError::Cell(Box::new(error)))?
                .ok_or(GatewayError::MalformedCache)?;
            match edges.as_slice() {
                [(target, _)] => Some(*target),
                _ => return Err(GatewayError::MalformedCache),
            }
        } else {
            None
        };
        stats.body_time += read.elapsed();
        stats.objects += 1;
        stats.bytes += header.size;
        let written = Instant::now();
        if header.size <= 4 * 1024 * 1024 {
            let oid = object.oid;
            let expected = reference.digest;
            let kind = object.kind;
            let body = tokio::task::spawn_blocking(move || {
                let (found, body) = pack
                    .data()?
                    .object(oid, 4 * 1024 * 1024)?
                    .ok_or(crate::git_pack::PackError::Integrity)?;
                if found != kind || blake3::hash(&body).as_bytes() != &expected {
                    return Err(crate::git_pack::PackError::Integrity);
                }
                Ok::<_, crate::git_pack::PackError>(body)
            })
            .await??;
            cache.store_object(object.oid, object.kind, body).await?;
        } else {
            cache
                .store_native_object(
                    pack,
                    *reference,
                    self.scratch_root.clone(),
                    self.disk_budget.clone(),
                )
                .await?;
        }
        stats.cache_time += written.elapsed();
        Ok(target)
    }
}
