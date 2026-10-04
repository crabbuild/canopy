use super::*;
use crate::blob::LargeBlobReference;
use std::time::{Duration, Instant};

#[derive(Default)]
pub(super) struct Hydration {
    pub(super) objects: u64,
    pub(super) bytes: u64,
    pub(super) body_time: Duration,
    pub(super) cache_time: Duration,
}

impl GitGateway {
    pub(super) async fn restore_packs(
        &self,
        shared: &mut CachedObjects,
    ) -> Result<(), GatewayError> {
        let mut after = Vec::new();
        loop {
            let page = self
                .repository
                .approved_packs(&after)
                .await
                .map_err(|error| GatewayError::Cell(Box::new(error)))?;
            if page.is_empty() {
                break;
            }
            for record in &page {
                self.pack_reader.install(&shared.cache, record).await?;
                shared.through = shared.through.max(record.covered_through);
            }
            after = page
                .last()
                .ok_or(GatewayError::MalformedCache)?
                .pack
                .sha256
                .to_vec();
        }
        Ok(())
    }

    pub(super) async fn hydrate(&self, shared: &mut CachedObjects) -> Result<(), GatewayError> {
        let started = Instant::now();
        let cache = &shared.cache;
        let _hydration = cache.hydration_guard();
        let _selection = cache.selection.lock().await;
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
        let body = match object.storage {
            ObjectStorage::Inline(body) => body,
            ObjectStorage::Packed { pack, size, blake3 } => {
                let record = self
                    .repository
                    .pack_record(pack)
                    .await
                    .map_err(|error| GatewayError::Cell(Box::new(error)))?;
                if record.approved {
                    self.pack_reader.install(cache, &record).await?;
                    return Ok(None);
                }
                let reader = self
                    .pack_reader
                    .native_reader(record, object.oid, size, blake3)
                    .await?;
                stats.body_time += read.elapsed();
                let written = Instant::now();
                cache.store_native_blob(reader).await?;
                stats.cache_time += written.elapsed();
                stats.objects += 1;
                stats.bytes += size;
                return Ok(None);
            }
            ObjectStorage::Chunked {
                upload,
                size,
                blake3,
            } => self
                .repository
                .chunked_body(object.oid, object.kind, upload, size, blake3)
                .await
                .map_err(|error| GatewayError::Cell(Box::new(error)))?,
            ObjectStorage::External {
                size,
                blake3,
                sha256,
            } => {
                let reference = LargeBlobReference {
                    oid: object.oid,
                    size,
                    blake3,
                    sha256,
                };
                let reader = self.large_blobs.read(&reference).await?;
                stats.body_time += read.elapsed();
                let written = Instant::now();
                cache.store_blob(reader).await?;
                stats.cache_time += written.elapsed();
                stats.objects += 1;
                stats.bytes += reference.size;
                return Ok(None);
            }
        };
        stats.body_time += read.elapsed();
        stats.objects += 1;
        stats.bytes += body.len() as u64;
        let target = (object.kind == ObjectKind::Tag)
            .then(|| {
                crate::graph::tag_edge(&body)
                    .map(|(oid, _)| oid)
                    .filter(|target| target.format() == object.oid.format())
            })
            .flatten();
        let written = Instant::now();
        cache.store_object(object.oid, object.kind, body).await?;
        stats.cache_time += written.elapsed();
        Ok(target)
    }
}
