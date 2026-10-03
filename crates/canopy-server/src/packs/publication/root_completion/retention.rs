//! Typed closed-audit edges. Command/input bodies and unpublished native packs
//! are not permanent audit roots; active/unknown pins retain those separately.
use super::*;
use canopy_object_storage::artifact::{ArtifactKey, ArtifactKind};

pub(in crate::packs::publication) async fn closed_graph(
    store: &ArtifactStore,
    root: NativeOutcomeRoot,
    hash: &mut blake3::Hasher,
) -> Result<(), super::super::RootRecoveryError> {
    let record: OutcomeRecord = root.0.read(store, INPUT_ROOT_BYTES).await?;
    super::super::recovery::archive::descriptor(
        hash,
        root.0.operation,
        ArtifactKind::InputRoot,
        root.0.artifact,
    )?;
    let native = record.native.read(store).await?;
    super::super::recovery::archive::descriptor(
        hash,
        record.native.operation(),
        ArtifactKind::InputRoot,
        record.native.artifact(),
    )?;
    // Selected body can legitimately borrow a prior admitted namespace.
    verify(
        store,
        super::super::native_result::body_key(record.body_operation, record.response.body),
        record.response.body,
        hash,
    )
    .await?;
    for (key, body) in native.audit_bodies() {
        verify(store, key, body, hash).await?;
    }
    Ok(())
}
async fn verify(
    store: &ArtifactStore,
    key: ArtifactKey,
    body: ArtifactDescriptor,
    hash: &mut blake3::Hasher,
) -> Result<(), super::super::RootRecoveryError> {
    let mut reader = store.read(key, body).await?;
    while reader.next().await?.is_some() {}
    super::super::recovery::archive::descriptor(hash, key.operation, key.kind, body)
}
