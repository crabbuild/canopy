//! Read-only admission of an existing immutable SQL catalog identity.

#[cfg(test)]
mod tests;

use cellule_runtime::{
    CellTarget, Error, Registry, Result,
    cell::catalog::{CatalogProof, CatalogRole},
    recovery::release::{ReleaseState, ReleaseStore},
};

pub(super) async fn existing_sql_proof(
    releases: &ReleaseStore,
    registry: &Registry,
    target: &CellTarget,
    proof: CatalogProof,
) -> Result<CatalogProof> {
    // New entries must still use ReleaseStore::provision and current code.
    // An existing entry pins its original code/schema forever; admission must
    // verify that identity instead of attempting to rewrite it during restore.
    let before = releases.load().await?.ok_or(Error::Release(
        "release is absent during existing Cell admission",
    ))?;
    let digest = registry.release_digest();
    let record = before.record();
    if record.state() != ReleaseState::Ready
        || record.current() != Some(digest)
        || record.desired() != Some(digest)
        || releases.descriptor(digest).await? != registry.release_bytes()
    {
        return Err(Error::Release(
            "existing Cell requires the exact ready release",
        ));
    }
    let entry = proof.entry();
    if entry.cell() != target.cell_id()
        || entry.namespace() != target.namespace()
        || entry.partition() != target.partition()
        || entry.role() != CatalogRole::Sql
        || !registry.supports_cell(
            entry.namespace(),
            entry.role(),
            entry.initial_code(),
            entry.initial_schema(),
        )
    {
        return Err(Error::Release(
            "existing SQL catalog identity is unsupported",
        ));
    }
    let after = releases.load().await?.ok_or(Error::Release(
        "release disappeared during existing Cell admission",
    ))?;
    if after.record() != record {
        return Err(Error::Release(
            "release changed during existing Cell admission",
        ));
    }
    Ok(proof)
}
