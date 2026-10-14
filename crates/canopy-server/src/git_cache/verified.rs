//! Bounded canonical extraction into an unpublished loose-object file.
use super::*;
use crate::{
    git_objects::{BodySink, GitObjects, ObjectReadError, ReadOwner},
    packs::{catalog::NativeReadError, metadata::CanonicalObject},
};
use tokio::sync::OwnedMutexGuard;

struct Sink {
    encoder: Option<ZlibEncoder<CacheWriter>>,
    temporary: tempfile::TempPath,
    destination: PathBuf,
    cache: Arc<GitCache>,
    owner: ReadOwner,
    _write: OwnedMutexGuard<()>,
}
impl BodySink for Sink {
    async fn append(&mut self, bytes: bytes::Bytes) -> Result<(), ObjectReadError> {
        let mut encoder = self.encoder.take().ok_or(ObjectReadError::Malformed)?;
        let owner = self.owner.clone();
        self.encoder = Some(
            tokio::task::spawn_blocking(move || {
                let _owner = owner;
                encoder.write_all(&bytes)?;
                Ok::<_, ObjectReadError>(encoder)
            })
            .await??,
        );
        Ok(())
    }
}
impl GitCache {
    /// The input is an isolated admitted native reader. Only a complete frame
    /// matching the certified metadata and a successful child may publish it.
    pub(crate) async fn copy_native_owned(
        self: &Arc<Self>,
        input: PathBuf,
        expected: CanonicalObject,
        owner: ReadOwner,
    ) -> Result<(), NativeReadError> {
        let write = self.object_write_lock(expected.oid).lock_owned().await;
        let cache = self.clone();
        let held = owner.clone();
        let sink = tokio::task::spawn_blocking(move || {
            if expected.oid.format() != cache.object_format {
                return Err(CacheError::Io(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "object format mismatch",
                )));
            }
            if cache.object_present(expected.oid)? {
                return Ok(None);
            }
            let (mut encoder, temporary, destination) = cache.object_writer(expected.oid)?;
            encoder.write_all(
                format!("{} {}\0", expected.kind.git_name(), expected.size).as_bytes(),
            )?;
            Ok::<_, CacheError>(Some(Sink {
                encoder: Some(encoder),
                temporary,
                destination,
                cache,
                owner: held,
                _write: write,
            }))
        })
        .await??;
        let Some(mut sink) = sink else {
            return Ok(());
        };
        let mut objects = GitObjects::batch_owned(&input, &self.native, owner)?;
        objects.copy_verified(expected, &mut sink).await?;
        objects.finish().await?;
        tokio::task::spawn_blocking(move || {
            let encoder = sink
                .encoder
                .take()
                .ok_or_else(|| io::Error::other("incomplete object writer"))?;
            drop(encoder.finish()?);
            sink.temporary
                .persist_noclobber(&sink.destination)
                .map_err(|error| error.error)?;
            #[cfg(test)]
            sink.cache
                .loose_objects
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            sink.cache
                .write_generation
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok::<_, CacheError>(())
        })
        .await??;
        Ok(())
    }
}
