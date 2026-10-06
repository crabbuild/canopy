//! Permanent selected Ready roots; negative results need only the frozen editorial row.
use super::super::sql::*;
use super::*;
use crate::packs::{
    catalog::CatalogSnapshot,
    directory::index::IndexError,
    input_artifact::{INPUT_ROOT_BYTES, StoredInputRoot},
    ref_state::{RefSnapshotError, RefStateError, RefStateIndex},
};
use canopy_object_storage::artifact::{ArtifactKind, ArtifactStore};
const DOMAIN: &[u8] = b"canopy.generated-candidate-audit.v1\0";
pub(in crate::packs::publication) const SAVED: &str = "SELECT binding,pull_number,actor,request,created_ms,result,native_publication FROM merge_candidates WHERE id=?1";
#[derive(Debug, thiserror::Error)]
pub enum NativeCandidateAuditError {
    #[error("candidate audit codec failed")]
    Codec(#[from] CodecError),
    #[error("candidate audit root failed")]
    Root(#[from] crate::packs::InputRootError),
    #[error("candidate audit catalog failed")]
    Catalog(#[from] IndexError),
    #[error("candidate audit refs failed")]
    Refs(#[from] RefStateError),
    #[error("candidate audit snapshot failed")]
    Snapshot(#[from] RefSnapshotError),
    #[error("candidate audit context differs")]
    Context,
}
#[derive(Clone, Debug)]
pub(in crate::packs::publication) struct Selected {
    pub reply: CandidatePublicationReply,
    pub root: Option<StoredInputRoot>,
}
impl WireValue for Selected {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        self.reply.encode(e)?;
        self.root.encode(e)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        Ok(Self {
            reply: CandidatePublicationReply::decode(d)?,
            root: Option::decode(d)?,
        })
    }
}
pub(in crate::packs::publication) struct Audit {
    pub candidate: MergeCandidate,
    pub catalog: StoredCatalog,
    refs: RefStateSnapshotRoot,
    generation: u64,
    ref_generation: u64,
}
impl WireValue for Audit {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        if !matches!(self.candidate.result, CandidateResult::Ready { .. })
            || self.generation == 0
            || self.ref_generation == 0
            || self.ref_generation > self.generation
        {
            return Err(CodecError::Invalid("candidate audit scope"));
        }
        e.write_bytes(DOMAIN)?;
        e.write_bytes(&candidate_bytes(&self.candidate)?)?;
        self.catalog.encode(e)?;
        self.refs.encode(e)?;
        e.write_u64(self.generation)?;
        e.write_u64(self.ref_generation)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        if d.read_bytes()? != DOMAIN {
            return Err(CodecError::Invalid("candidate audit purpose"));
        }
        let value = Self {
            candidate: serde_json::from_slice(d.read_bytes()?)
                .map_err(|_| CodecError::Invalid("candidate audit value"))?,
            catalog: StoredCatalog::decode(d)?,
            refs: RefStateSnapshotRoot::decode(d)?,
            generation: d.read_u64()?,
            ref_generation: d.read_u64()?,
        };
        value.encode(&mut BoundedEncoder::new(INPUT_ROOT_BYTES)?)?;
        Ok(value)
    }
}
pub(super) async fn prepare(
    prepared: &PreparedCatalog,
    candidate: &MergeCandidate,
    refs: RefStateSnapshotRoot,
    ref_generation: u64,
) -> Result<StoredInputRoot, NativeCandidatePublicationError> {
    let value = Audit {
        candidate: candidate.clone(),
        catalog: prepared.catalog(),
        refs,
        generation: prepared.base().generation + 1,
        ref_generation,
    };
    Ok(StoredInputRoot::upload(
        &prepared.base.indexes().store(),
        prepared.token().artifact_operation,
        &value,
        INPUT_ROOT_BYTES,
    )
    .await?)
}
type CandidateRow = (Vec<u8>, MergeCandidate, Option<Selected>);

pub(super) fn row(sets: &[SqlResultSet]) -> Result<Option<CandidateRow>, Error> {
    let Some(row) = rows(sets)?.first() else {
        return Ok(None);
    };
    if row.len() != 7 {
        return Err(Error::Command("invalid candidate audit row"));
    }
    let (binding, candidate) = crate::pulls::candidates::decode(&[SqlResultSet {
        columns: vec![],
        rows: vec![row[..6].to_vec()],
        rows_affected: 0,
    }])?
    .ok_or(Error::Command("candidate audit row absent"))?;
    let selected = match &row[6] {
        SqlValue::Null => None,
        SqlValue::Blob(bytes) => {
            let mut d = BoundedDecoder::new(bytes, 512)?;
            let value = Selected::decode(&mut d)?;
            d.finish()?;
            Some(value)
        }
        _ => return Err(Error::Command("invalid candidate publication")),
    };
    Ok(Some((binding, candidate, selected)))
}
pub(in crate::packs::publication) fn statement(reply: &CandidatePublicationReply) -> SqlStatement {
    let id = match reply {
        CandidatePublicationReply::Applied { id, .. } => blob(id),
        _ => SqlValue::Null,
    };
    SqlStatement {
        sql: SAVED.into(),
        parameters: vec![id],
    }
}
pub(in crate::packs::publication) fn selected(
    sets: &[SqlResultSet],
    reply: &CandidatePublicationReply,
    actor: &str,
) -> Result<Option<Selected>, Error> {
    let CandidatePublicationReply::Applied {
        id,
        digest,
        publication,
    } = reply
    else {
        return Ok(None);
    };
    let Some((_, candidate, stored)) = row(sets)? else {
        return Ok(None);
    };
    if candidate.actor != actor
        || candidate.request.id != uuid::Uuid::from_bytes(*id).to_string()
        || result_digest(&candidate)? != *digest
    {
        return Ok(None);
    }
    let ready = matches!(candidate.result, CandidateResult::Ready { .. });
    if ready != publication.is_some() {
        return Err(Error::Command("candidate publication result differs"));
    }
    match stored {
        Some(value) if value.reply == *reply && ready == value.root.is_some() => Ok(Some(value)),
        None if !ready => Ok(Some(Selected {
            reply: reply.clone(),
            root: None,
        })),
        _ => Ok(None),
    }
}
pub(in crate::packs::publication) async fn closed_graph(
    store: &ArtifactStore,
    sets: &[SqlResultSet],
    reply: &CandidatePublicationReply,
    check: &LeaseCheck,
    hash: &mut blake3::Hasher,
) -> Result<(), RootRecoveryError> {
    if !reply.applied() {
        return Ok(());
    }
    let selected = selected(sets, reply, &check.actor)?.ok_or(RootRecoveryError::Context)?;
    let Some(root) = selected.root else {
        return Ok(());
    };
    let audit: Audit = root
        .read(store, INPUT_ROOT_BYTES)
        .await
        .map_err(NativeCandidateAuditError::from)?;
    let CandidatePublicationReply::Applied {
        id,
        digest,
        publication: Some(published),
    } = reply
    else {
        return Err(RootRecoveryError::Context);
    };
    if audit.candidate.actor != check.actor
        || audit.candidate.request.id != uuid::Uuid::from_bytes(*id).to_string()
        || result_digest(&audit.candidate)? != *digest
        || audit.catalog.repository != check.token.repository
        || audit.refs.operation() != root.operation
        || audit.generation != published.generation
        || audit.ref_generation != published.ref_generation
    {
        return Err(RootRecoveryError::Context);
    }
    let snapshot = CatalogSnapshot::download(store, audit.catalog)
        .await
        .map_err(NativeCandidateAuditError::from)?;
    crate::packs::directory::snapshot::DirectorySnapshot::download(store, snapshot.directory)
        .await
        .map_err(NativeCandidateAuditError::from)?;
    let refs = audit
        .refs
        .read(store)
        .await
        .map_err(NativeCandidateAuditError::from)?;
    if refs.repository != check.token.repository
        || refs.format != audit.catalog.format
        || refs.generation != audit.ref_generation
    {
        return Err(RootRecoveryError::Context);
    }
    let CandidateResult::Ready { oid, .. } = &audit.candidate.result else {
        return Err(RootRecoveryError::Context);
    };
    let expected = crate::RefExpectation {
        oid: Some(crate::pulls::merge::oid(oid)?),
        version: 1,
    };
    let found = RefStateIndex::new(std::sync::Arc::new(store.clone()), refs.format)
        .read(refs.root, &audit.candidate.fetch_ref())
        .await
        .map_err(NativeCandidateAuditError::from)?;
    if found != Some(expected) {
        return Err(RootRecoveryError::Context);
    }
    let descriptor = super::super::recovery::archive::descriptor;
    descriptor(hash, root.operation, ArtifactKind::InputRoot, root.artifact)?;
    descriptor(
        hash,
        audit.catalog.operation,
        ArtifactKind::CatalogNode,
        audit.catalog.artifact,
    )?;
    descriptor(
        hash,
        snapshot.directory.operation,
        ArtifactKind::CatalogNode,
        snapshot.directory.artifact,
    )?;
    descriptor(
        hash,
        audit.refs.operation(),
        ArtifactKind::InputRoot,
        audit.refs.artifact(),
    )?;
    Ok(())
}

/// Select only a completed native result matching the requested pull revision
/// and strategy. SQL bytes alone do not establish commit semantics: the private
/// merge factory separately verifies this candidate in its accepted catalog.
pub(in crate::packs::publication) fn ready_for_merge(
    sets: &[SqlResultSet],
    input: &crate::pulls::merge::command::MergeInput,
) -> Result<Option<(MergeCandidate, StoredInputRoot)>, Error> {
    let Some((binding, candidate, stored)) = row(sets)? else {
        return Ok(None);
    };
    if candidate.number != input.number
        || candidate.request.revision != input.request.revision
        || candidate.request.strategy != input.request.strategy
        || input.request.candidate_id.as_deref() != Some(&candidate.request.id)
        || !matches!(candidate.result, CandidateResult::Ready { .. })
        || binding != crate::pulls::candidates::intent_binding(&candidate)?
    {
        return Ok(None);
    }
    let Some(stored) = stored else {
        return Ok(None);
    };
    if selected(sets, &stored.reply, &candidate.actor)?.is_none() {
        return Ok(None);
    }
    Ok(stored.root.map(|root| (candidate, root)))
}
pub(in crate::packs::publication) async fn verify_ready(
    store: &ArtifactStore,
    candidate: &MergeCandidate,
    root: StoredInputRoot,
) -> Result<(), NativeCandidateAuditError> {
    let audit: Audit = root.read(store, INPUT_ROOT_BYTES).await?;
    if audit.candidate != *candidate
        || audit.catalog.repository != store.repository()
        || audit.refs.operation() != root.operation
    {
        return Err(NativeCandidateAuditError::Context);
    }
    Ok(())
}
