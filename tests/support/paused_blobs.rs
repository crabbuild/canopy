use futures_core::Stream;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, memory::InMemory,
    path::Path as StorePath,
};
use std::{
    fmt,
    pin::Pin,
    sync::atomic::{AtomicBool, Ordering},
};
use tokio::sync::Notify;

type StoreStream<T> = Pin<Box<dyn Stream<Item = object_store::Result<T>> + Send + 'static>>;

#[derive(Debug, Default)]
pub(crate) struct PausedBlobs {
    inner: InMemory,
    pub(crate) armed: AtomicBool,
    pub(crate) entered: Notify,
    pub(crate) proceed: Notify,
}

impl fmt::Display for PausedBlobs {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("publication-race-store")
    }
}

#[async_trait::async_trait]
impl ObjectStore for PausedBlobs {
    async fn put_opts(
        &self,
        path: &StorePath,
        payload: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.inner.put_opts(path, payload, options).await
    }
    async fn put_multipart_opts(
        &self,
        path: &StorePath,
        options: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        // Large-blob ingestion starts only after native receive-pack has
        // accepted the disposable refs, but before Cell ref publication.
        if self.armed.swap(false, Ordering::SeqCst) {
            self.entered.notify_one();
            self.proceed.notified().await;
        }
        self.inner.put_multipart_opts(path, options).await
    }
    async fn get_opts(
        &self,
        path: &StorePath,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.inner.get_opts(path, options).await
    }
    fn delete_stream(&self, paths: StoreStream<StorePath>) -> StoreStream<StorePath> {
        self.inner.delete_stream(paths)
    }
    fn list(&self, prefix: Option<&StorePath>) -> StoreStream<ObjectMeta> {
        self.inner.list(prefix)
    }
    async fn list_with_delimiter(
        &self,
        prefix: Option<&StorePath>,
    ) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }
    async fn copy_opts(
        &self,
        from: &StorePath,
        to: &StorePath,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}
