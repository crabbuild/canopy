//! Immutable, fixed-size parts for Git blob and LFS bodies.

use bytes::{Bytes, BytesMut};
use object_store::{
    GetOptions, MultipartUpload, ObjectMeta, ObjectStore, ObjectStoreExt, PutMode, PutOptions,
    path::Path,
};
use std::future::poll_fn;
use std::{future::Future, sync::Arc, time::Duration};

pub(crate) const PART_BYTES: usize = 8 * 1024 * 1024;
const MAGIC: &[u8; 8] = b"CANOPY01";

pub(crate) fn part(path: &Path, index: u64) -> Path {
    Path::from(format!("{path}.parts/{index:016x}"))
}

fn invalid() -> object_store::Error {
    object_store::Error::Generic {
        store: "Canopy external body",
        source: Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "invalid external body manifest or part",
        )),
    }
}

async fn timed<T>(
    future: impl Future<Output = object_store::Result<T>>,
) -> object_store::Result<T> {
    tokio::time::timeout(Duration::from_secs(120), future)
        .await
        .map_err(|error| object_store::Error::Generic {
            store: "Canopy external body",
            source: Box::new(error),
        })?
}

pub(crate) struct Upload {
    store: Arc<dyn ObjectStore>,
    stage: Path,
    parts: u64,
    active: Option<Box<dyn MultipartUpload>>,
}

impl Upload {
    pub(crate) async fn new(
        store: Arc<dyn ObjectStore>,
        stage: Path,
    ) -> object_store::Result<Self> {
        let active = timed(store.put_multipart(&part(&stage, 0))).await?;
        Ok(Self {
            store,
            stage,
            parts: 0,
            active: Some(active),
        })
    }

    pub(crate) async fn write(&mut self, bytes: Bytes) -> object_store::Result<()> {
        if bytes.len() > PART_BYTES {
            return Err(invalid());
        }
        if self.active.is_none() {
            self.active =
                Some(timed(self.store.put_multipart(&part(&self.stage, self.parts))).await?);
        }
        let upload = self.active.as_mut().ok_or_else(invalid)?;
        timed(upload.put_part(bytes.into())).await?;
        timed(upload.complete()).await?;
        self.active = None;
        self.parts += 1;
        Ok(())
    }

    pub(crate) async fn publish(&mut self, path: &Path, size: u64) -> object_store::Result<()> {
        if self.active.is_some() || self.parts != size.div_ceil(PART_BYTES as u64).max(1) {
            return Err(invalid());
        }
        copy_parts(self.store.as_ref(), &self.stage, path, size).await?;
        let mut manifest = MAGIC.to_vec();
        manifest.extend_from_slice(&size.to_le_bytes());
        match timed(self.store.put_opts(
            path,
            manifest.into(),
            PutOptions {
                mode: PutMode::Create,
                ..Default::default()
            },
        ))
        .await
        {
            Ok(_) | Err(object_store::Error::AlreadyExists { .. }) => Ok(()),
            Err(error) => Err(error),
        }
    }

    pub(crate) async fn cleanup(&mut self) -> object_store::Result<()> {
        let mut failure = None;
        if let Some(upload) = &mut self.active
            && let Err(error) = timed(upload.abort()).await
        {
            failure = Some(error);
        }
        // Include the active part: completion may have succeeded despite a lost reply.
        for index in 0..self.parts + u64::from(self.active.is_some()) {
            match timed(self.store.delete(&part(&self.stage, index))).await {
                Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
                Err(error) => {
                    failure = Some(error);
                }
            }
        }
        failure.map_or(Ok(()), Err)
    }
}

pub(crate) async fn copy_parts(
    store: &dyn ObjectStore,
    from: &Path,
    to: &Path,
    size: u64,
) -> object_store::Result<()> {
    for index in 0..size.div_ceil(PART_BYTES as u64).max(1) {
        match timed(store.copy_if_not_exists(&part(from, index), &part(to, index))).await {
            Ok(()) | Err(object_store::Error::AlreadyExists { .. }) => {}
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

async fn bounded(
    store: &dyn ObjectStore,
    path: &Path,
    options: GetOptions,
    size: usize,
) -> object_store::Result<(ObjectMeta, Bytes)> {
    timed(async {
        let result = store.get_opts(path, options).await?;
        if result.meta.size != size as u64 || result.range != (0..size as u64) {
            return Err(invalid());
        }
        let meta = result.meta.clone();
        let mut stream = result.into_stream();
        let mut bytes = BytesMut::with_capacity(size);
        while let Some(chunk) = poll_fn(|cx| stream.as_mut().poll_next(cx)).await {
            let chunk = chunk?;
            if chunk.len() > size - bytes.len() {
                return Err(invalid());
            }
            bytes.extend_from_slice(&chunk);
        }
        if bytes.len() != size {
            return Err(invalid());
        }
        Ok((meta, bytes.freeze()))
    })
    .await
}

pub(crate) async fn open(
    store: &dyn ObjectStore,
    path: &Path,
    size: u64,
) -> object_store::Result<ObjectMeta> {
    let (meta, bytes) = bounded(store, path, GetOptions::default(), 16).await?;
    if &bytes[..8] != MAGIC || bytes[8..] != size.to_le_bytes() {
        return Err(invalid());
    }
    if size == 0 {
        bounded(store, &part(path, 0), GetOptions::default(), 0).await?;
    }
    Ok(meta)
}

pub(crate) async fn read(
    store: &dyn ObjectStore,
    path: &Path,
    meta: &ObjectMeta,
    size: u64,
    offset: u64,
) -> object_store::Result<Bytes> {
    let length = (size - offset).min(PART_BYTES as u64) as usize;
    // Parts are create-only. The caller verifies whole-body hashes before
    // accepting the last part, so one final manifest check fences replacement
    // without an extra provider round trip for every earlier part.
    if offset + length as u64 == size {
        let (_, manifest) = bounded(
            store,
            path,
            GetOptions {
                if_match: meta.e_tag.clone(),
                version: meta.version.clone(),
                ..Default::default()
            },
            16,
        )
        .await?;
        if &manifest[..8] != MAGIC || manifest[8..] != size.to_le_bytes() {
            return Err(invalid());
        }
    }
    let (_, bytes) = bounded(
        store,
        &part(path, offset / PART_BYTES as u64),
        GetOptions::default(),
        length,
    )
    .await?;
    Ok(bytes)
}
