//! Typed graph edges selected exclusively from a pinned repository database.
use super::*;
use crate::packs::{
    backup::{Inventory, decode},
    directory::index::WalkResult,
    wire_request::WireRequestRoot,
};

pub(super) fn context(
    tenant: [u8; 16],
    application: [u8; 16],
    repository: [u8; 16],
    inventory: &Inventory<'_>,
) -> WalkResult<()> {
    if tenant != *inventory.target().tenant().as_bytes()
        || application != *inventory.target().application().as_bytes()
        || repository != inventory.store().repository()
    {
        return Err(CodecError::Invalid("backup repository context").into());
    }
    Ok(())
}
pub(super) async fn fact(value: GenerationFact, inventory: &mut Inventory<'_>) -> WalkResult<()> {
    value.validate()?;
    if let Some(root) = value.catalog {
        inventory.catalog(root).await?;
    }
    if let Some(root) = value.refs {
        inventory.refs(root).await?;
    }
    Ok(())
}
pub(super) async fn catalog_certificate(
    value: &CatalogCertificate,
    seed: &[u8; 32],
    inventory: &mut Inventory<'_>,
) -> WalkResult<()> {
    if !value.authenticated(seed) {
        return Err(CodecError::Invalid("backup catalog MAC").into());
    }
    let data = value.data()?;
    context(
        data.tenant,
        data.application,
        data.token.repository,
        inventory,
    )?;
    fact(data.base, inventory).await?;
    inventory.catalog(data.catalog).await
}
pub(super) async fn wire(root: WireRequestRoot, inventory: &mut Inventory<'_>) -> WalkResult<()> {
    let record = root.read(&inventory.store()).await?;
    context(
        *record.tenant.as_bytes(),
        *record.application.as_bytes(),
        record.identity.repository,
        inventory,
    )?;
    if record.format != inventory.format() {
        return Err(CodecError::Invalid("backup wire format").into());
    }
    inventory.input(root.operation(), root.artifact()).await?;
    inventory.body(record.operation, record.request.body).await
}
async fn outcomes(value: RootPushOutcomes, inventory: &mut Inventory<'_>) -> WalkResult<()> {
    for root in [value.native, value.rejected, value.replayed] {
        root_completion::backup_graph(root, inventory).await?;
    }
    Ok(())
}
pub(super) async fn command(
    kind: recovery::Kind,
    bytes: &[u8],
    seed: &[u8; 32],
    inventory: &mut Inventory<'_>,
) -> WalkResult<()> {
    use recovery::Kind;
    match kind {
        Kind::Publish => {
            let value: RootPushCompletion = decode(bytes, ROOT_COMPLETION_BYTES)?;
            catalog_certificate(&value.proof.certificate, seed, inventory).await?;
            inventory.refs(value.proof.snapshot).await?;
            outcomes(value.outcomes, inventory).await?;
        }
        Kind::Outcome => {
            let value: RootOutcomeCompletion = decode(bytes, ROOT_COMPLETION_BYTES)?;
            outcome::backup_graph(&value.proof, seed, inventory).await?;
            outcomes(value.outcomes, inventory).await?;
        }
        Kind::Policy => {
            let value: RefPolicyPage = decode(bytes, REF_POLICY_PAGE_BYTES)?;
            catalog_certificate(&value.proof.certificate, seed, inventory).await?;
        }
        Kind::Initialization => {
            let value: InitialRefProof = decode(bytes, INITIALIZATION_BYTES)?;
            catalog_certificate(&value.certificate, seed, inventory).await?;
            inventory.refs(value.refs).await?;
        }
        Kind::Head => {
            let value: NativeHeadProof = decode(bytes, NATIVE_HEAD_BYTES)?;
            catalog_certificate(&value.certificate, seed, inventory).await?;
            if let Some(root) = value.refs {
                inventory.refs(root).await?;
            }
        }
        Kind::Candidate => {
            candidate_publication::backup_graph(
                &decode(bytes, NATIVE_CANDIDATE_BYTES)?,
                seed,
                inventory,
            )
            .await?
        }
        Kind::Merge => {
            native_merge::backup_graph(&decode(bytes, NATIVE_MERGE_BYTES)?, seed, inventory).await?
        }
    }
    Ok(())
}

/// Each row kind uses its declared codec; a blob cannot select its own purpose.
pub(crate) async fn row(
    kind: u8,
    columns: &[Option<Vec<u8>>],
    seed: &[u8; 32],
    inventory: &mut Inventory<'_>,
) -> WalkResult<()> {
    let at = |n: usize| {
        columns
            .get(n)
            .and_then(Option::as_deref)
            .ok_or(CodecError::Invalid("backup graph row"))
    };
    match kind {
        0 => {
            if let Some(bytes) = columns.first().and_then(Option::as_deref) {
                inventory.catalog(decode(bytes, 256)?).await?;
            }
            if let Some(bytes) = columns.get(1).and_then(Option::as_deref) {
                inventory.refs(decode(bytes, 128)?).await?;
            }
        }
        1 => root_completion::backup_graph(decode(at(0)?, 128)?, inventory).await?,
        2 => native_merge::audit::backup_graph(decode(at(0)?, 128)?, inventory).await?,
        3 => {
            let value: candidate_publication::audit::Selected = decode(at(0)?, 512)?;
            if let Some(root) = value.root {
                candidate_publication::audit::backup_graph(root, inventory).await?;
            }
        }
        4 => {
            let value: GenerationFact = decode(at(0)?, 512)?;
            value.validate()?;
            if let Some(root) = value.catalog {
                inventory.catalog_headers(root).await?;
            }
            if let Some(root) = value.refs {
                inventory.refs(root).await?;
            }
        }
        5 => fact(decode(at(0)?, 512)?, inventory).await?,
        6 => {
            if let Some(bytes) = columns.first().and_then(Option::as_deref) {
                inputs::backup_graph(&decode(bytes, 1024)?, seed, inventory).await?;
            }
            if let Some(bytes) = columns.get(1).and_then(Option::as_deref) {
                catalog_certificate(&decode(bytes, 1024)?, seed, inventory).await?;
            }
            if let Some(bytes) = columns.get(2).and_then(Option::as_deref) {
                recovery::backup::graph(
                    bytes,
                    columns.get(3).and_then(Option::as_deref),
                    None,
                    seed,
                    inventory,
                )
                .await?;
            }
        }
        7 => recovery::backup::graph(at(0)?, Some(at(1)?), Some(at(2)?), seed, inventory).await?,
        _ => return Err(CodecError::Invalid("backup graph purpose").into()),
    }
    Ok(())
}
