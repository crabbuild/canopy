use std::{collections::BTreeMap, fmt, pin::Pin, sync::Arc};

use crab_cell_runtime::{control::Control, control::ControlState};
use futures_core::Stream;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult, path::Path,
};
use serde::Serialize;
use tokio::sync::Mutex;

type StoreStream<T> = Pin<Box<dyn Stream<Item = object_store::Result<T>> + Send + 'static>>;

#[derive(Clone, Debug, Default, Serialize)]
pub struct Counts {
    pub get_started: u64,
    pub head_started: u64,
    pub put_started: u64,
    pub put_completed: u64,
    pub put_failed: u64,
    pub put_payload_bytes_completed: u64,
    pub conditional_control_updates: u64,
    pub unchanged_root_updates: u64,
    pub root_changes: u64,
    pub other_operations_started: u64,
    pub updated_cells: BTreeMap<String, u64>,
}

#[derive(Debug, Default)]
struct State {
    counts: Option<Counts>,
    controls: BTreeMap<String, Control>,
    puts_in_flight: u64,
}

#[derive(Debug)]
pub struct MeasuredStore {
    inner: Arc<dyn ObjectStore>,
    state: Mutex<State>,
}

impl MeasuredStore {
    pub fn new(inner: Arc<dyn ObjectStore>) -> Self {
        Self {
            inner,
            state: Mutex::new(State::default()),
        }
    }

    pub async fn begin(&self) -> (usize, u64) {
        let mut state = self.state.lock().await;
        state.counts = Some(Counts::default());
        (
            state
                .controls
                .values()
                .filter(|c| c.state == ControlState::Serving)
                .count(),
            state.puts_in_flight,
        )
    }

    pub async fn end(&self) -> (Option<Counts>, usize, u64) {
        let mut state = self.state.lock().await;
        (
            state.counts.take(),
            state
                .controls
                .values()
                .filter(|c| c.state == ControlState::Serving)
                .count(),
            state.puts_in_flight,
        )
    }

    async fn other(&self) {
        if let Some(counts) = &mut self.state.lock().await.counts {
            counts.other_operations_started += 1;
        }
    }
}

impl fmt::Display for MeasuredStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("idle-density-measured-store")
    }
}

#[async_trait::async_trait]
impl ObjectStore for MeasuredStore {
    async fn put_opts(
        &self,
        path: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        // The pinned Cell layout names its bounded authority records control.json.
        // Decode those records only; large immutable object bodies are not copied.
        let control = if path.filename() == Some("control.json") && payload.content_length() <= 8192
        {
            let bytes: Vec<u8> = payload
                .iter()
                .flat_map(|part| part.iter().copied())
                .collect();
            Control::decode(&bytes).ok()
        } else {
            None
        };
        let conditional = matches!(options.mode, PutMode::Update(_));
        let bytes = payload.content_length() as u64;
        {
            let mut state = self.state.lock().await;
            state.puts_in_flight += 1;
            if let Some(counts) = &mut state.counts {
                counts.put_started += 1;
            }
        }
        let result = self.inner.put_opts(path, payload, options).await;
        let mut state = self.state.lock().await;
        state.puts_in_flight -= 1;
        let previous = if result.is_ok() {
            control.as_ref().and_then(|c| {
                state
                    .controls
                    .insert(hex::encode(c.cell.as_bytes()), c.clone())
            })
        } else {
            None
        };
        if let Some(counts) = &mut state.counts {
            if result.is_err() {
                counts.put_failed += 1;
            } else {
                counts.put_completed += 1;
                counts.put_payload_bytes_completed += bytes;
                if let Some(control) = control.filter(|_| conditional) {
                    counts.conditional_control_updates += 1;
                    *counts
                        .updated_cells
                        .entry(hex::encode(control.cell.as_bytes()))
                        .or_default() += 1;
                    if let Some(previous) = previous {
                        if previous.root == control.root {
                            counts.unchanged_root_updates += 1;
                        } else {
                            counts.root_changes += 1;
                        }
                    }
                }
            }
        }
        result
    }

    async fn get_opts(&self, path: &Path, options: GetOptions) -> object_store::Result<GetResult> {
        if let Some(counts) = &mut self.state.lock().await.counts {
            if options.head {
                counts.head_started += 1;
            } else {
                counts.get_started += 1;
            }
        }
        self.inner.get_opts(path, options).await
    }

    async fn put_multipart_opts(
        &self,
        path: &Path,
        options: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.other().await;
        self.inner.put_multipart_opts(path, options).await
    }
    fn delete_stream(&self, paths: StoreStream<Path>) -> StoreStream<Path> {
        self.inner.delete_stream(paths)
    }
    fn list(&self, prefix: Option<&Path>) -> StoreStream<ObjectMeta> {
        self.inner.list(prefix)
    }
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.other().await;
        self.inner.list_with_delimiter(prefix).await
    }
    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.other().await;
        self.inner.copy_opts(from, to, options).await
    }
}
