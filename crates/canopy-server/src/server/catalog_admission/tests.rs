use std::{
    fmt,
    pin::Pin,
    sync::{Arc, Mutex},
    time::Duration,
};

use bytes::Bytes;
use cellule_app::CellApplication;
use cellule_runtime::{
    ApplicationId, CellModule, Digest, TenantId,
    cell::application::ApplicationIdentity,
    cell::catalog::{CatalogEntry, CellCatalog},
    identity::RequestId,
    ltx::CellStorageLayout,
};
use cellule_store::Store;
use futures_core::Stream;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload, PutResult, memory::InMemory,
    path::Path,
};
use tokio::{sync::Notify, time::timeout};

use super::*;
use crate::{
    CanopyApplication, build_descriptor,
    directory::{DirectoryModule, directory_target},
};

type TestResult<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;
type StoreStream<T> = Pin<Box<dyn Stream<Item = object_store::Result<T>> + Send + 'static>>;

// Fault injection belongs to this owned fixture, not a shared deployment.
#[derive(Debug, Default)]
struct PausedDescriptor {
    inner: InMemory,
    pause: Mutex<Option<Path>>,
    entered: Notify,
    proceed: Notify,
}

impl fmt::Display for PausedDescriptor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("catalog-admission-fixture")
    }
}

#[async_trait::async_trait]
impl ObjectStore for PausedDescriptor {
    async fn put_opts(
        &self,
        path: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.inner.put_opts(path, payload, options).await
    }
    async fn put_multipart_opts(
        &self,
        path: &Path,
        options: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(path, options).await
    }
    async fn get_opts(&self, path: &Path, options: GetOptions) -> object_store::Result<GetResult> {
        let pause = {
            let mut armed = self.pause.lock().unwrap();
            if armed.as_ref() == Some(path) {
                armed.take();
                true
            } else {
                false
            }
        };
        if pause {
            self.entered.notify_one();
            self.proceed.notified().await;
        }
        self.inner.get_opts(path, options).await
    }
    fn delete_stream(&self, paths: StoreStream<Path>) -> StoreStream<Path> {
        self.inner.delete_stream(paths)
    }
    fn list(&self, prefix: Option<&Path>) -> StoreStream<ObjectMeta> {
        self.inner.list(prefix)
    }
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }
    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

struct Fixture {
    store: Arc<PausedDescriptor>,
    layout: CellStorageLayout,
    registry: Arc<Registry>,
    target: CellTarget,
    catalog: CellCatalog,
    releases: ReleaseStore,
}

impl Fixture {
    fn new() -> TestResult<Self> {
        let store = Arc::new(PausedDescriptor::default());
        let tenant = TenantId::from_bytes([1; 16]);
        let application = ApplicationId::from_bytes([2; 16]);
        let layout = CellStorageLayout::new(
            Store::new(store.clone()),
            Path::from("admission"),
            *application.as_bytes(),
        );
        let registry = CanopyApplication::compile(build_descriptor(
            include_bytes!("../../../../../../Cargo.lock"),
            env!("CARGO_PKG_VERSION"),
        ))?
        .registry();
        let target = directory_target(tenant, application)?;
        let catalog = CellCatalog::new(layout.clone(), tenant);
        let releases = ReleaseStore::new(
            layout.clone(),
            ApplicationIdentity::new(tenant, application),
        )?;
        Ok(Self {
            store,
            layout,
            registry,
            target,
            catalog,
            releases,
        })
    }

    fn code(&self) -> Digest {
        self.registry.module_code(DirectoryModule::NAME).unwrap()
    }

    async fn prepare(&self) -> TestResult<(u64, RequestId)> {
        let revision = self
            .releases
            .load()
            .await?
            .map_or(0, |value| value.record().revision());
        let operation = RequestId::from_bytes(uuid::Uuid::new_v4().into_bytes());
        let prepared = self
            .releases
            .prepare(
                self.registry.release_bytes(),
                self.registry.release_digest(),
                revision,
                &format!("sha256:{}", "a".repeat(64)),
                operation,
            )
            .await?;
        Ok((prepared.revision(), operation))
    }

    async fn ready(&self) -> TestResult {
        let (revision, operation) = self.prepare().await?;
        let activating = self.releases.start_activation(revision, operation).await?;
        self.releases
            .complete_activation(activating.revision(), operation)
            .await?;
        Ok(())
    }

    async fn proof(
        &self,
        role: CatalogRole,
        code: Digest,
        schema: u32,
    ) -> TestResult<CatalogProof> {
        Ok(self
            .catalog
            .provision(CatalogEntry::new(&self.target, role, code, schema)?)
            .await?)
    }

    async fn admit(&self, proof: CatalogProof) -> cellule_runtime::Result<CatalogProof> {
        existing_sql_proof(&self.releases, &self.registry, &self.target, proof).await
    }
}

#[tokio::test]
async fn supported_current_and_retained_proofs_are_returned_unchanged() -> TestResult {
    let retained = Digest::from_bytes(
        hex::decode("f7254eda9d5d339566f45457502618ad13cbbf6e5a74595f5b3ce46653ea12f1")?
            .try_into()
            .map_err(|_| "invalid retained code")?,
    );
    for code in [None, Some(retained)] {
        let fixture = Fixture::new()?;
        fixture.ready().await?;
        let proof = fixture
            .proof(CatalogRole::Sql, code.unwrap_or(fixture.code()), 1)
            .await?;
        let entry = proof.entry().clone();
        let revision = proof.revision();
        let release = fixture.releases.load().await?.unwrap().record().clone();
        let admitted = fixture.admit(proof).await?;
        assert_eq!(admitted.entry(), &entry);
        assert_eq!(admitted.revision(), revision);
        assert_eq!(fixture.releases.load().await?.unwrap().record(), &release);
    }
    Ok(())
}

#[tokio::test]
async fn absent_and_non_ready_releases_close_existing_admission() -> TestResult {
    for state in [
        None,
        Some(ReleaseState::Prepared),
        Some(ReleaseState::Activating),
        Some(ReleaseState::Maintenance),
    ] {
        let fixture = Fixture::new()?;
        let proof = fixture.proof(CatalogRole::Sql, fixture.code(), 1).await?;
        if let Some(state) = state {
            let (revision, operation) = fixture.prepare().await?;
            match state {
                ReleaseState::Activating => {
                    fixture
                        .releases
                        .start_activation(revision, operation)
                        .await?;
                }
                ReleaseState::Maintenance => {
                    fixture
                        .releases
                        .start_maintenance(revision, operation)
                        .await?;
                }
                ReleaseState::Prepared => {}
                _ => unreachable!(),
            }
        }
        let before = fixture
            .releases
            .load()
            .await?
            .map(|value| value.record().clone());
        assert!(matches!(fixture.admit(proof).await, Err(Error::Release(_))));
        let after = fixture
            .releases
            .load()
            .await?
            .map(|value| value.record().clone());
        assert_eq!(before, after);
    }
    Ok(())
}

#[tokio::test]
async fn corrupted_selected_descriptor_closes_existing_admission() -> TestResult {
    let fixture = Fixture::new()?;
    fixture.ready().await?;
    let proof = fixture.proof(CatalogRole::Sql, fixture.code(), 1).await?;
    fixture
        .store
        .put(
            &fixture
                .layout
                .release_descriptor_path(fixture.registry.release_digest().as_bytes()),
            Bytes::from_static(b"corrupt").into(),
        )
        .await?;
    assert!(matches!(fixture.admit(proof).await, Err(Error::Release(_))));
    Ok(())
}

#[tokio::test]
async fn unsupported_catalog_identity_and_wrong_target_are_not_adopted() -> TestResult {
    for case in 0..4 {
        let fixture = Fixture::new()?;
        fixture.ready().await?;
        let role = if case == 0 {
            CatalogRole::Kv
        } else {
            CatalogRole::Sql
        };
        let code = if case == 1 {
            Digest::from_bytes([1; 32])
        } else {
            fixture.code()
        };
        let schema = if case == 2 { 2 } else { 1 };
        let proof = fixture.proof(role, code, schema).await?;
        let entry = proof.entry().clone();
        let other = directory_target(fixture.target.tenant(), ApplicationId::from_bytes([3; 16]))?;
        let target = if case == 3 { &other } else { &fixture.target };
        assert!(matches!(
            existing_sql_proof(&fixture.releases, &fixture.registry, target, proof).await,
            Err(Error::Release(
                "existing SQL catalog identity is unsupported"
            )),
        ));
        assert_eq!(
            fixture
                .catalog
                .lookup(fixture.target.cell_id())
                .await?
                .unwrap()
                .entry(),
            &entry
        );
    }
    Ok(())
}

#[tokio::test]
async fn release_round_trip_to_ready_during_admission_is_rejected() -> TestResult {
    let fixture = Fixture::new()?;
    fixture.ready().await?;
    let proof = fixture.proof(CatalogRole::Sql, fixture.code(), 1).await?;
    let before = fixture.releases.load().await?.unwrap().record().clone();
    *fixture.store.pause.lock().unwrap() = Some(
        fixture
            .layout
            .release_descriptor_path(fixture.registry.release_digest().as_bytes()),
    );
    let change = async {
        timeout(Duration::from_secs(5), fixture.store.entered.notified()).await?;
        // This fixture has no owners or Controls; it is not a live upgrade controller.
        let result = fixture.ready().await;
        fixture.store.proceed.notify_one();
        result
    };
    let (admission, changed) = tokio::join!(fixture.admit(proof), change);
    changed?;
    let after = fixture.releases.load().await?.unwrap().record().clone();
    assert_eq!(after.state(), ReleaseState::Ready);
    assert_eq!(before.current(), after.current());
    assert_eq!(before.desired(), after.desired());
    assert_ne!(before.revision(), after.revision());
    assert!(matches!(
        admission,
        Err(Error::Release(
            "release changed during existing Cell admission"
        ))
    ));
    Ok(())
}
